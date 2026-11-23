package siastorage

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// TestBuilderCopySharesHandle proves a Builder copied by value shares the
// original's native handle, so closing both frees it once and either copy sees
// it closed.
func TestBuilderCopySharesHandle(t *testing.T) {
	b := testBuilder(t)
	c := *b
	if err := c.Close(); err != nil {
		t.Fatalf("close copy: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close original: %v", err)
	}
	if _, err := b.RequestConnection(context.Background()); !errors.Is(err, errClosed) {
		t.Fatalf("original after closing the copy: %v", err)
	}
}

// TestObjectCopyOutlivesOriginal proves an Object copied by value keeps the
// native object alive after the original is collected, rather than reading
// whatever reuses its memory.
func TestObjectCopyOutlivesOriginal(t *testing.T) {
	copies := make([]Object, 64)
	for i := range copies {
		copies[i] = *NewEmptyObject().WithMetadata([]byte{byte(i)})
	}
	// Cleanups run on their own goroutine after a cycle.
	for range 3 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	fill := make([]*Object, len(copies))
	for i := range fill {
		fill[i] = NewEmptyObject().WithMetadata([]byte("fill"))
	}

	for i := range copies {
		// Truncate clones the native object, so this reads its metadata back
		// out of native memory rather than from the Go side's cached copy.
		if got := copies[i].Truncate(0).Metadata(); !bytes.Equal(got, []byte{byte(i)}) {
			t.Fatalf("copy %d: native metadata is %q", i, got)
		}
	}
	runtime.KeepAlive(fill)
}
