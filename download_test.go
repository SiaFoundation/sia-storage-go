package siastorage

import (
	"bytes"
	"errors"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// TestDownloadInflight asserts downloads release their inflight
// reservations.
func TestDownloadInflight(t *testing.T) {
	sdk, hosts := newTestSDK(t, 40, zaptest.NewLogger(t))
	defer sdk.Close()

	data := frand.Bytes(int(proto.SectorSize) * 10)
	obj := NewEmptyObject()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	hosts.waitInflightDrained(t)

	got, err := readAll(sdk.Download(obj))
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}
	hosts.waitInflightDrained(t)
}

func TestOutOfOrderDownload(t *testing.T) {
	sdk, hosts := newTestSDK(t, 30, zaptest.NewLogger(t))
	defer sdk.Close()

	slabSize := uint64(proto.SectorSize) * 10
	data := frand.Bytes(int(slabSize))
	obj := NewEmptyObject()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// decreasing per-sector delays make later chunks more likely to finish
	// first, which exercises ordered output.
	for _, slab := range obj.Slabs() {
		for _, sector := range slab.Sectors {
			hosts.SetSectorReadDelay(sector.Root, 500*time.Millisecond)
		}
	}

	got, err := readAll(sdk.Download(obj, WithDownloadInflight(40)))
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}
}

func TestChunkIter(t *testing.T) {
	makeSlab := func(length uint32) slabs.SlabSlice {
		return slabs.SlabSlice{
			EncryptionKey: frand.Entropy256(),
			MinShards:     10,
			Length:        length,
		}
	}

	check := func(t *testing.T, chunks []slabs.SlabSlice, length uint64) {
		t.Helper()
		var total uint64
		for i, c := range chunks {
			if uint64(c.Length) > chunkSize {
				t.Fatalf("chunk %d exceeds chunkSize: %d", i, c.Length)
			}
			if i > 0 && c.EncryptionKey == chunks[i-1].EncryptionKey {
				if c.Offset != chunks[i-1].Offset+chunks[i-1].Length {
					t.Fatalf("chunk %d not contiguous", i)
				}
			}
			total += uint64(c.Length)
		}
		if total != length {
			t.Fatalf("total chunk length %d != expected %d", total, length)
		}
	}

	tests := []struct {
		name   string
		slabs  []slabs.SlabSlice
		offset uint64
		length uint64
	}{
		{
			name:   "single slab full",
			slabs:  []slabs.SlabSlice{makeSlab(1 << 20)},
			offset: 0,
			length: 1 << 20,
		},
		{
			name:   "partial offset",
			slabs:  []slabs.SlabSlice{makeSlab(1 << 20)},
			offset: 100,
			length: chunkSize + 50,
		},
		{
			name:   "multiple slabs",
			slabs:  []slabs.SlabSlice{makeSlab(chunkSize * 2), makeSlab(chunkSize * 3)},
			offset: 0,
			length: chunkSize*2 + chunkSize*3,
		},
		{
			name:   "offset skips first slab",
			slabs:  []slabs.SlabSlice{makeSlab(1000), makeSlab(chunkSize * 2)},
			offset: 1000,
			length: chunkSize * 2,
		},
		{
			name:   "span across slabs",
			slabs:  []slabs.SlabSlice{makeSlab(chunkSize), makeSlab(chunkSize)},
			offset: chunkSize / 2,
			length: chunkSize,
		},
		{
			name:   "small request",
			slabs:  []slabs.SlabSlice{makeSlab(chunkSize * 4)},
			offset: 0,
			length: 100,
		},
		{
			name:   "zero length",
			slabs:  []slabs.SlabSlice{makeSlab(chunkSize)},
			offset: 0,
			length: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ci := newChunkIter(tt.slabs, tt.offset, tt.length)
			var chunks []slabs.SlabSlice
			objectOffset := tt.offset
			for c, ok := ci.next(); ok; c, ok = ci.next() {
				if c.objectOffset != objectOffset {
					t.Fatalf("expected object offset %d, got %d", objectOffset, c.objectOffset)
				}
				chunks = append(chunks, c.slab)
				objectOffset += uint64(c.slab.Length)
			}
			check(t, chunks, tt.length)
		})
	}
}

func TestChunkWriterBuffered(t *testing.T) {
	var output bytes.Buffer
	counted := &countWriter{w: &output}
	dataKey := frand.Entropy256()
	cw := newChunkWriter(counted, &dataKey)
	recovered := recoveredChunk{
		shards:   [][]byte{make([]byte, chunkSize)},
		writeLen: chunkSize,
	}

	if err := cw.writeChunk(chunkSlab{}, recovered); err != nil {
		t.Fatal(err)
	} else if output.Len() != chunkSize {
		t.Fatalf("expected %d output bytes, got %d", chunkSize, output.Len())
	} else if expected := chunkSize / downloadWriteBufferSize; counted.count != expected {
		t.Fatalf("expected %d buffered writes, got %d", expected, counted.count)
	}
}

func TestDownloadV0(t *testing.T) {
	sdk, _ := newTestSDK(t, 12, zaptest.NewLogger(t))
	defer sdk.Close()

	// build a legacy object by applying the v0 object-wide cipher
	// before sending the data through the unchanged shard upload layer
	const dataShards = 3
	slabSize := uint64(proto.SectorSize) * dataShards
	data := frand.Bytes(int(slabSize) + 4096)
	obj := NewEmptyObject()
	uo, enc, err := newUploadOption(WithRedundancy(dataShards, 9))
	if err != nil {
		t.Fatal(err)
	}
	slabKeys := &slabKeySource{}
	encrypted := encrypt((*[32]byte)(obj.dataKey), bytes.NewReader(data), 0)
	slabsCh := make(chan slabUpload, uo.maxConcurrentSlabs())
	go func() {
		defer close(slabsCh)
		sdk.uploadSlabs(t.Context(), slabsCh, encrypted, slabKeys, enc, uo)
	}()
	obj.slabs, err = collectSlabs(t.Context(), slabsCh, uo)
	if err != nil {
		t.Fatal(err)
	}
	for i := range obj.slabs {
		obj.slabs[i].Version = 0
	}

	tests := []struct {
		name   string
		offset uint64
		length uint64
	}{
		{"full object", 0, uint64(len(data))},
		{"within slab", 12345, 54321},
		{"across slabs", slabSize - 1000, 2000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readAll(sdk.Download(obj, WithDownloadRange(tt.offset, tt.length)))
			if err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(got, data[tt.offset:tt.offset+tt.length]) {
				t.Fatal("data mismatch")
			}
		})
	}
}

func TestDownloadUnsupportedSlabVersion(t *testing.T) {
	sdk, _ := newTestSDK(t, 12, zaptest.NewLogger(t))
	defer sdk.Close()

	data := frand.Bytes(4096)
	obj := NewEmptyObject()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data), WithRedundancy(3, 9)); err != nil {
		t.Fatal(err)
	}
	obj.slabs[0].Version = 2

	// the download must fail fast, before any sectors are fetched
	if _, err := sdk.Download(obj); !errors.Is(err, slabs.ErrUnsupportedSlabVersion) {
		t.Fatalf("expected ErrUnsupportedSlabVersion, got %v", err)
	}
}

func TestSlabRecovery(t *testing.T) {
	sdk, _ := newTestSDK(t, 30, zaptest.NewLogger(t))
	defer sdk.Close()

	slabSize := int(proto.SectorSize) * 10
	data := frand.Bytes(slabSize)
	obj := NewEmptyObject()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		offset uint64
		length uint64
	}{
		{"full slab", 0, uint64(slabSize)},
		{"first half", 0, uint64(slabSize / 2)},
		{"second half", uint64(slabSize / 2), uint64(slabSize / 2)},
		{"first 30 bytes", 0, 30},
		{"middle 30 bytes", uint64(slabSize/2 - 15), 30},
		{"last 30 bytes", uint64(slabSize - 30), 30},
		{"first 4KiB", 0, 4096},
		{"middle 4KiB", uint64(slabSize/2 - 2048), 4096},
		{"last 4KiB", uint64(slabSize - 4096), 4096},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readAll(sdk.Download(obj, WithDownloadRange(tt.offset, tt.length)))
			if err != nil {
				t.Fatalf("download failed: %v", err)
			}
			expected := data[tt.offset : tt.offset+tt.length]
			if !bytes.Equal(got, expected) {
				t.Fatalf("data mismatch: got %d bytes, expected %d", len(got), len(expected))
			}
		})
	}
}
