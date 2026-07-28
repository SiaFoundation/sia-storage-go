package siastorage

import (
	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
)

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

// joinShards interleaves the striped data shards into dst, skipping the
// first 'skip' bytes of the recovered data and filling all of dst.
func joinShards(dst []byte, dataShards [][]byte, skip int) error {
	n := 0
	for off := 0; n < len(dst); off += proto4.LeafSize {
		for _, shard := range dataShards {
			if len(shard[off:]) < proto4.LeafSize {
				return reedsolomon.ErrShortData
			}
			shard = shard[off:][:proto4.LeafSize]
			if skip >= len(shard) {
				skip -= len(shard)
				continue
			} else if skip > 0 {
				shard = shard[skip:]
				skip = 0
			}
			n += copy(dst[n:], shard)
			if n == len(dst) {
				return nil
			}
		}
	}
	return nil
}
