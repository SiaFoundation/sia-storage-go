package siastorage

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
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
			for c, _, ok := ci.next(); ok; c, _, ok = ci.next() {
				chunks = append(chunks, c)
			}
			check(t, chunks, tt.length)
		})
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

// joinSegments concatenates every segment yielded by a chunkSegments built from
// rc, returning the bytes and the first non-EOF error.
func joinSegments(rc recoveredChunk) ([]byte, error) {
	cs := newChunkSegments(rc)
	var out []byte
	for {
		seg, err := cs.next()
		if errors.Is(err, io.EOF) {
			return out, nil
		} else if err != nil {
			return out, err
		} else if len(seg) == 0 || len(seg) > proto.LeafSize {
			return out, errors.New("segment must be non-empty and within one leaf")
		}
		out = append(out, seg...)
	}
}

func TestChunkSegments(t *testing.T) {
	const dataShards = 4
	const parityShards = 1

	// 3.5 data shards worth of data, striped across the slab's data shards.
	data := frand.Bytes(proto.SectorSize * 7 / 2)
	slab, err := NewSlabReader(dataShards, parityShards).ReadSlab(bytes.NewReader(data))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	shards := slab.Shards[:dataShards]
	half := len(data) / 2

	// skip/writeLen should select arbitrary sub-ranges of the recovered data
	tests := []struct {
		name           string
		skip, writeLen int
		expected       []byte
	}{
		{"full", 0, len(data), data},
		{"first half", 0, half, data[:half]},
		{"second half", half, half, data[half:]},
		{"empty", 0, 0, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := joinSegments(recoveredChunk{shards: shards, skip: tt.skip, writeLen: tt.writeLen})
			if err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(got, tt.expected) {
				t.Fatalf("data mismatch: expected %d bytes, got %d", len(tt.expected), len(got))
			}
		})
	}

	// data shards too short to satisfy writeLen must surface ErrShortData
	t.Run("short data", func(t *testing.T) {
		short := [][]byte{frand.Bytes(proto.LeafSize), frand.Bytes(proto.LeafSize)}
		if _, err := joinSegments(recoveredChunk{shards: short, writeLen: 4 * proto.LeafSize}); !errors.Is(err, reedsolomon.ErrShortData) {
			t.Fatalf("expected ErrShortData, got %v", err)
		}
	})
}
