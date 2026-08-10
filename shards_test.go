package siastorage

import (
	"reflect"
	"testing"

	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
	"lukechampine.com/frand"
)

func TestSplitShards(t *testing.T) {
	const dataShards = 3
	const slabSize = proto4.SectorSize * dataShards

	// under and exact
	for _, dataSize := range []int{100, slabSize} {
		data := frand.Bytes(dataSize)

		shards := make([][]byte, dataShards)
		for i := range shards {
			shards[i] = make([]byte, proto4.SectorSize)
		}
		splitShards(shards, data)

		for i, chunk := range chunks(data, proto4.LeafSize) {
			// pad it out with zeros
			var padded [proto4.LeafSize]byte
			copy(padded[:], chunk)

			index := i % dataShards
			offset := (i / dataShards) * proto4.LeafSize

			actual := shards[index][offset : offset+proto4.LeafSize]
			if !reflect.DeepEqual(actual, padded[:]) {
				t.Fatalf("data size %d: shard %d mismatch at offset %d", dataSize, index, offset)
			}
		}
	}
}

func TestSplitJoinShards(t *testing.T) {
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

	shards := make([][]byte, dataShards+parityShards)
	for i := range shards {
		shards[i] = make([]byte, proto4.SectorSize)
	}
	splitShards(shards[:dataShards], data)

	// the last shard is an empty parity shard
	if !reflect.DeepEqual(shards[4], make([]byte, proto4.SectorSize)) {
		t.Fatal("parity shard should be empty")
	}

	for _, s := range shards[:4] {
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
	if err := coder.Encode(shards); err != nil {
		t.Fatal(err)
	} else if reflect.DeepEqual(shards[4], make([]byte, proto4.SectorSize)) {
		t.Fatal("parity shard should be filled after encoding")
	}

	// joining the shards back together should result in the original data
	joined := make([]byte, len(data))
	if err := stripedJoin(joined, shards[:dataShards], 0); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(joined, data) {
		t.Fatal("mismatch")
	}

	// join only the first half
	joined = make([]byte, len(data)/2)
	if err := stripedJoin(joined, shards[:dataShards], 0); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(joined, data[:len(data)/2]) {
		t.Fatal("mismatch")
	}

	// join only the second half
	joined = make([]byte, len(data)/2)
	if err := stripedJoin(joined, shards[:dataShards], len(data)/2); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(joined, data[len(data)/2:]) {
		t.Fatal("mismatch")
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
