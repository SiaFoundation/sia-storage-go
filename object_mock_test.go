//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

// TestObjectEmpty covers the accessors on a fresh handle, before an upload has
// given it slabs or timestamps.
func TestObjectEmpty(t *testing.T) {
	obj := NewObject()
	defer obj.Close()

	if obj.Size() != 0 {
		t.Fatalf("expected an empty object to have size 0, got %d", obj.Size())
	}
	if obj.EncodedSize() != 0 {
		t.Fatalf("expected encoded size 0, got %d", obj.EncodedSize())
	}
	if md := obj.Metadata(); md != nil {
		t.Fatalf("expected no metadata, got %d bytes", len(md))
	}
	// A new object is stamped on creation rather than left empty, so these are
	// real times even before an upload.
	if age := time.Since(obj.CreatedAt()); age < 0 || age > time.Minute {
		t.Fatalf("expected CreatedAt to be just now, got %v", obj.CreatedAt())
	}
	if obj.UpdatedAt().Before(obj.CreatedAt()) {
		t.Fatalf("UpdatedAt %v precedes CreatedAt %v", obj.UpdatedAt(), obj.CreatedAt())
	}
}

// TestObjectMetadataRoundTrip proves metadata survives the boundary at sizes
// either side of a single copy, and that clearing works.
func TestObjectMetadataRoundTrip(t *testing.T) {
	obj := NewObject()
	defer obj.Close()

	for _, size := range []int{1, 31, 32, 33, 4096} {
		want := bytes.Repeat([]byte{byte(size)}, size)
		obj.UpdateMetadata(want)
		got := obj.Metadata()
		if !bytes.Equal(got, want) {
			t.Fatalf("metadata of %d bytes came back as %d bytes", size, len(got))
		}
	}

	obj.UpdateMetadata(nil)
	if md := obj.Metadata(); md != nil {
		t.Fatalf("expected clearing metadata to leave none, got %d bytes", len(md))
	}
}

// TestObjectCloseIsIdempotent proves the cleanup and an explicit Close cannot
// both free the same pointer.
func TestObjectCloseIsIdempotent(t *testing.T) {
	obj := NewObject()
	if err := obj.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := obj.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestObjectIDIsContentAddressed proves ID comes from the slabs rather than
// being random, which is what lets a caller detect that two uploads produced
// the same object.
func TestObjectIDIsContentAddressed(t *testing.T) {
	a, b := NewObject(), NewObject()
	defer a.Close()
	defer b.Close()

	if a.ID() != b.ID() {
		t.Fatal("two empty objects should share an ID, so the ID is not derived from the slabs")
	}
	// Metadata is not part of the ID, only the slabs are.
	a.UpdateMetadata([]byte(`{"name":"a"}`))
	if a.ID() != b.ID() {
		t.Fatal("metadata must not change the object ID")
	}
	var zero types.Hash256
	if a.ID() == zero {
		t.Fatal("an empty object's ID should still be a real hash, not zero")
	}
}
