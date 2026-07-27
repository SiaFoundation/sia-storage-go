package siastorage

import (
	"errors"
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
	buf        []byte
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
	// read the whole slab into one contiguous buffer, so the source sees
	// large reads, then interleave it with plain copies
	if sr.buf == nil {
		sr.buf = make([]byte, sr.OptimalDataSize())
	}
	n, err := io.ReadFull(r, sr.buf)
	sr.length = n
	splitShards(sr.shards[:sr.dataShards], sr.buf[:n])

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return sr.take(), io.EOF
	} else if err != nil {
		return ReadSlab{}, err
	}
	return sr.take(), nil
}

// splitShards interleaves src across the data shards in LeafSize segments.
func splitShards(dataShards [][]byte, src []byte) {
	n := 0
	for off := 0; n < len(src); off += proto4.LeafSize {
		for _, shard := range dataShards {
			n += copy(shard[off:off+proto4.LeafSize], src[n:])
			if n == len(src) {
				return
			}
		}
	}
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
