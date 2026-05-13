package siastorage

import (
	"io"

	proto4 "go.sia.tech/core/rhp/v4"
)

// ReadSlab is the result of reading a full or partial slab from a SlabReader.
type ReadSlab struct {
	Length int
	Shards [][]byte
}

// SlabReader is a streaming reader that interleaves incoming bytes across a
// slab's data shards as they become available. It yields a ReadSlab whenever
// a full slab has been accumulated or when the underlying reader reaches EOF.
type SlabReader struct {
	dataShards int
	shards     [][]byte
	length     int
}

// NewSlabReader creates a new SlabReader that interleaves data across
// dataShards shards. The returned shards slice always has dataShards +
// parityShards entries, with parity slots zeroed and ready for encoding.
func NewSlabReader(dataShards, parityShards int) *SlabReader {
	totalShards := dataShards + parityShards
	shards := make([][]byte, totalShards)
	for i := range shards {
		shards[i] = make([]byte, proto4.SectorSize)
	}
	return &SlabReader{
		dataShards: dataShards,
		shards:     shards,
	}
}

// OptimalDataSize returns the number of data bytes that fill exactly one slab.
func (sr *SlabReader) OptimalDataSize() int {
	return sr.dataShards * proto4.SectorSize
}

// ReadSlab reads data from r, interleaving it across the data shards in
// LeafSize segments. When a full slab is filled, the shards are returned
// and the internal buffers are reset for the next slab.
//
// On EOF, the partial slab is returned with err set to io.EOF. If no bytes
// were buffered at EOF, Length is 0. The caller should check Length > 0
// before using the slab.
func (sr *SlabReader) ReadSlab(r io.Reader) (ReadSlab, error) {
	optimal := sr.OptimalDataSize()

	for sr.length < optimal {
		// calculate current position in the interleaved layout
		stripeSize := proto4.LeafSize * sr.dataShards
		shardIndex := (sr.length % stripeSize) / proto4.LeafSize
		byteInSeg := sr.length % proto4.LeafSize
		segStart := (sr.length / stripeSize) * proto4.LeafSize

		segment := sr.shards[shardIndex][segStart+byteInSeg : segStart+proto4.LeafSize]
		n, err := r.Read(segment)
		sr.length += n

		if err == io.EOF {
			return sr.take(), io.EOF
		} else if err != nil {
			return ReadSlab{}, err
		} else if n == 0 {
			return sr.take(), io.EOF
		}
	}

	return sr.take(), nil
}

// take returns the current slab and resets the internal buffers.
func (sr *SlabReader) take() ReadSlab {
	result := ReadSlab{
		Length: sr.length,
		Shards: sr.shards,
	}

	// skip allocation if nothing was buffered
	if sr.length > 0 {
		sr.shards = make([][]byte, len(sr.shards))
		for i := range sr.shards {
			sr.shards[i] = make([]byte, proto4.SectorSize)
		}
	}
	sr.length = 0

	return result
}
