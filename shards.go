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

// stripedJoin joins the striped data shards, writing them to dst. The first 'skip'
// bytes of the recovered data are skipped, and len(dst) bytes are written in
// total.
func stripedJoin(dst []byte, dataShards [][]byte, skip int) error {
	written, writeLen := 0, len(dst)
	for off := 0; writeLen > 0; off += proto4.LeafSize {
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
			if writeLen < len(shard) {
				shard = shard[:writeLen]
			}
			n := copy(dst[written:], shard)
			written += n
			writeLen -= n
		}
	}
	return nil
}
