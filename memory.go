package siastorage

import (
	"math"
	"runtime/debug"
	"sync"

	proto4 "go.sia.tech/core/rhp/v4"
)

const fallbackSystemMemory = 1 << 30 // 1 GiB

// defaultMemoryBudget returns roughly 10% of the memory available to the
// process, computed once. systemMemory is implemented per OS; Linux includes
// the process's cgroup limit. A configured Go memory limit further caps it.
var defaultMemoryBudget = sync.OnceValue(func() uint64 {
	total := systemMemory()
	if total == 0 {
		total = fallbackSystemMemory
	}

	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < math.MaxInt64 && uint64(limit) < total {
		total = uint64(limit)
	}
	return max(total/10, 1)
})

func defaultSlabsInMemory(totalShards int) int {
	slabSize := uint64(totalShards) * uint64(proto4.SectorSize)
	return max(int(defaultMemoryBudget()/slabSize), 1)
}

func defaultChunksInMemory() int {
	return max(int(defaultMemoryBudget()/maxChunkSize), 1)
}
