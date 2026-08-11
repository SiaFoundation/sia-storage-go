package siastorage

import "testing"

func TestDefaultMemoryBudget(t *testing.T) {
	if budget := defaultMemoryBudget(); budget == 0 {
		t.Fatal("default memory budget must be positive")
	} else if slabs := defaultSlabsInMemory(30); slabs < 1 {
		t.Fatal("must buffer at least one slab")
	} else if chunks := defaultChunksInMemory(); chunks < 1 {
		t.Fatal("must buffer at least one chunk")
	}
}
