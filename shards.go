package siastorage

import (
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
