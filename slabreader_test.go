package siastorage

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
	"lukechampine.com/frand"
)

func TestStripedRead(t *testing.T) {
	const dataShards = 3
	const parityShards = 2
	const slabSize = proto4.SectorSize * dataShards

	testCases := []struct {
		// (data size, expected size)
		dataSize     int
		expectedSize int
	}{
		{100, 100},               // under
		{slabSize, slabSize},     // exact
		{2 * slabSize, slabSize}, // over
	}

	for _, tc := range testCases {
		data := frand.Bytes(tc.dataSize)

		reader := NewSlabReader(dataShards, parityShards)
		slab, err := reader.ReadSlab(bytes.NewReader(data))

		if tc.dataSize >= slabSize {
			if err != nil {
				t.Fatalf("data size %d: unexpected error: %v", tc.dataSize, err)
			}
		} else {
			if err != io.EOF {
				t.Fatalf("data size %d: expected io.EOF, got %v", tc.dataSize, err)
			}
		}

		if slab.Length != tc.expectedSize {
			t.Fatalf("data size %d: read mismatch: %d", tc.dataSize, slab.Length)
		} else if len(slab.Shards) != dataShards+parityShards {
			t.Fatalf("data size %d: shard count mismatch: %d", tc.dataSize, len(slab.Shards))
		}

		for i, chunk := range chunks(data[:slab.Length], proto4.LeafSize) {
			// pad it out with zeros
			var padded [proto4.LeafSize]byte
			copy(padded[:], chunk)

			index := i % dataShards
			offset := (i / dataShards) * proto4.LeafSize

			actual := slab.Shards[index][offset : offset+proto4.LeafSize]
			if !reflect.DeepEqual(actual, padded[:]) {
				t.Fatalf("data size %d: shard %d mismatch at offset %d", tc.dataSize, index, offset)
			}
		}
	}
}

func TestStripedReadWrite(t *testing.T) {
	const dataShards = 4
	const parityShards = 1

	coder, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		t.Fatal(err)
	}

	// 3.5 shards of data
	data := make([]byte, proto4.SectorSize*7/2)
	for i := range proto4.SectorSize {
		data[i] = 1
	}
	for i := range proto4.SectorSize {
		data[proto4.SectorSize+i] = 2
	}
	for i := range proto4.SectorSize {
		data[2*proto4.SectorSize+i] = 3
	}
	for i := range len(data) - 3*proto4.SectorSize {
		data[3*proto4.SectorSize+i] = 4
	}

	reader := NewSlabReader(dataShards, parityShards)
	slab, err := reader.ReadSlab(bytes.NewReader(data))
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	} else if slab.Length != len(data) {
		t.Fatalf("expected length %d, got %d", len(data), slab.Length)
	}

	// we expect 5 shards and the last one is an empty parity shard
	if len(slab.Shards) != 5 {
		t.Fatalf("expected 5 shards, got %d", len(slab.Shards))
	} else if slab.Length != proto4.SectorSize*7/2 {
		t.Fatalf("expected length %d, got %d", proto4.SectorSize*7/2, slab.Length)
	} else if !reflect.DeepEqual(slab.Shards[4], make([]byte, proto4.SectorSize)) {
		t.Fatal("parity shard should be empty")
	}

	for _, s := range slab.Shards[:4] {
		// every shard should be of SectorSize
		if len(s) != proto4.SectorSize {
			t.Fatalf("expected shard size %d, got %d", proto4.SectorSize, len(s))
		}

		quarter := proto4.SectorSize / 4

		// first quarter of every shard is 1s
		for _, b := range s[:quarter] {
			if b != 1 {
				t.Fatal("expected 1s in first quarter")
			}
		}

		// second quarter is 2s
		for _, b := range s[quarter : 2*quarter] {
			if b != 2 {
				t.Fatal("expected 2s in second quarter")
			}
		}

		// third quarter is 3s
		for _, b := range s[2*quarter : 3*quarter] {
			if b != 3 {
				t.Fatal("expected 3s in third quarter")
			}
		}

		// half of the fourth quarter is 4s
		for _, b := range s[3*quarter : proto4.SectorSize/8*7] {
			if b != 4 {
				t.Fatal("expected 4s in half of fourth quarter")
			}
		}

		// remainder is padded with 0s
		for _, b := range s[proto4.SectorSize/8*7:] {
			if b != 0 {
				t.Fatal("expected 0s in remainder")
			}
		}
	}

	// encoding the read shards should succeed without errors and cause the
	// parity shard to be filled
	if err := coder.Encode(slab.Shards); err != nil {
		t.Fatal(err)
	} else if reflect.DeepEqual(slab.Shards[4], make([]byte, proto4.SectorSize)) {
		t.Fatal("parity shard should be filled after encoding")
	}
}

// chunks splits data into segments of the given size.
func chunks(data []byte, size int) [][]byte {
	var result [][]byte
	for len(data) > 0 {
		end := min(size, len(data))
		result = append(result, data[:end])
		data = data[end:]
	}
	return result
}
