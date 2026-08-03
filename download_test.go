package siastorage

import (
	"bytes"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
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
			if uint64(c.Length) > maxChunkSize {
				t.Fatalf("chunk %d exceeds maxChunkSize: %d", i, c.Length)
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
			slabs:  []slabs.SlabSlice{makeSlab(maxChunkSize * 2)},
			offset: 100,
			length: maxChunkSize + 50,
		},
		{
			name:   "multiple slabs",
			slabs:  []slabs.SlabSlice{makeSlab(maxChunkSize * 2), makeSlab(maxChunkSize * 3)},
			offset: 0,
			length: maxChunkSize*2 + maxChunkSize*3,
		},
		{
			name:   "offset skips first slab",
			slabs:  []slabs.SlabSlice{makeSlab(1000), makeSlab(maxChunkSize * 2)},
			offset: 1000,
			length: maxChunkSize * 2,
		},
		{
			name:   "span across slabs",
			slabs:  []slabs.SlabSlice{makeSlab(maxChunkSize), makeSlab(maxChunkSize)},
			offset: maxChunkSize / 2,
			length: maxChunkSize,
		},
		{
			name:   "small request",
			slabs:  []slabs.SlabSlice{makeSlab(maxChunkSize * 4)},
			offset: 0,
			length: 100,
		},
		{
			name:   "zero length",
			slabs:  []slabs.SlabSlice{makeSlab(maxChunkSize)},
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

func TestDownloadRacing(t *testing.T) {
	// builds a slab where the minShards "fastest looking" hosts are actually
	// slow, so a quick download has to race the other hosts instead of waiting
	// on the slow ones
	setup := func(slowDelay time.Duration) (*SDK, Object, []byte) {
		t.Helper()
		sdk, hosts := newTestSDK(t, 60, zaptest.NewLogger(t))
		t.Cleanup(func() { sdk.Close() })

		// upload one slab of random data
		data := frand.Bytes(int(proto.SectorSize) * 10)
		obj := NewEmptyObject()
		if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}

		// make the minShards hosts look fastest so Prioritize favors them
		slab := obj.Slabs()[0]
		var slow []types.PublicKey
		for _, sector := range slab.Sectors[:slab.MinShards] {
			hosts.provider.AddReadSample(sector.HostKey, 1<<18, time.Microsecond) // 256 KiB
			slow = append(slow, sector.HostKey)
		}

		// now make them slow
		hosts.SetSlowHostKeys(slow, slowDelay)
		return sdk, obj, data
	}

	// download a bounded range. racing still has to beat the slow hosts to
	// recover it, but the smaller transfer keeps the timing margin robust when
	// the suite is under load.
	const raceLen = 1 << 20 // 1 MiB
	sdk, obj, data := setup(5 * time.Second)
	start := time.Now()
	got, err := readAll(sdk.Download(obj, WithDownloadRange(0, raceLen)))

	// assert racing recovered the data without waiting on the slow hosts
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, data[:raceLen]) {
		t.Fatal("data mismatch")
	} else if elapsed := time.Since(start); elapsed >= 4*time.Second {
		t.Fatal("racing should beat the slow hosts", elapsed)
	}

	// the reader is still on chunk 0 and this chunk sits a full window ahead, so
	// racing it now would steal bandwidth from data nobody is waiting for yet
	sdk, obj, _ = setup(1500 * time.Millisecond)
	chunk := obj.Slabs()[0]
	chunk.Offset, chunk.Length = 0, 1<<18 // 256 KiB
	popped := newChangeCounter(0)
	start = time.Now()

	// assert the chunk waited for the slow host instead of racing
	if _, err := sdk.downloadSlab(t.Context(), chunk, 0, raceWindow, popped, time.Minute, nil); err != nil {
		t.Fatal(err)
	} else if elapsed := time.Since(start); elapsed < 1200*time.Millisecond {
		t.Fatal("gated chunk should have waited, not raced", elapsed)
	}

	// bumping the read head at 200ms pulls this chunk into the window, so racing
	// should start right then instead of waiting out the 1500ms slow host
	sdk, obj, _ = setup(1500 * time.Millisecond)
	chunk = obj.Slabs()[0]
	chunk.Offset, chunk.Length = 0, 1<<18 // 256 KiB
	popped = newChangeCounter(0)
	time.AfterFunc(200*time.Millisecond, func() { popped.add(1) })
	start = time.Now()

	// assert racing started when the window opened, not after the slow delay
	if _, err := sdk.downloadSlab(t.Context(), chunk, 0, raceWindow, popped, time.Minute, nil); err != nil {
		t.Fatal(err)
	} else if elapsed := time.Since(start); elapsed < 190*time.Millisecond {
		t.Fatal("raced before the window opened", elapsed)
	} else if elapsed >= 1200*time.Millisecond {
		t.Fatal("did not race when the window opened", elapsed)
	}
}
