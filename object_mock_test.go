//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"context"
	"errors"
	"sync"
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

// TestHandleUseAfterClose proves every handle reports a zero value rather than
// reading memory Close already freed. Before the guard these went straight to
// the native side: Object.Size returned whatever the freed allocation happened
// to hold, which is harder to notice than a crash.
func TestHandleUseAfterClose(t *testing.T) {
	_, sdk := testSDK(t)
	ctx := context.Background()

	obj := NewObject()
	obj.UpdateMetadata([]byte("something"))
	if err := obj.Close(); err != nil {
		t.Fatalf("close object: %v", err)
	}
	if n := obj.Size(); n != 0 {
		t.Errorf("Size after Close = %d, want 0", n)
	}
	if n := obj.EncodedSize(); n != 0 {
		t.Errorf("EncodedSize after Close = %d, want 0", n)
	}
	if id := obj.ID(); id != (types.Hash256{}) {
		t.Errorf("ID after Close = %v, want the zero hash", id)
	}
	if !obj.CreatedAt().IsZero() {
		t.Error("CreatedAt after Close should be the zero time")
	}
	if md := obj.Metadata(); md != nil {
		t.Errorf("Metadata after Close = %q, want nil", md)
	}
	obj.UpdateMetadata([]byte("ignored")) // must not touch the freed handle

	// A closed object handed to the SDK is the same dangling read, so the
	// call has to refuse it rather than pass the pointer across.
	if err := sdk.PinObject(ctx, obj); !errors.Is(err, errClosed) {
		t.Errorf("PinObject with a closed object returned %v, want errClosed", err)
	}

	key, err := sdk.CreateSharingKey(ctx, "closed key", time.Time{})
	if err != nil {
		t.Fatalf("create sharing key: %v", err)
	}
	pub := key.PublicKey()
	if err := key.Close(); err != nil {
		t.Fatalf("close key: %v", err)
	}
	if got := key.PublicKey(); got == pub {
		t.Error("PublicKey after Close should be the zero key")
	}
	if seed := key.Export(); seed != ([32]byte{}) {
		t.Error("Export after Close should be the zero seed")
	}
	if err := sdk.RevokeSharingKey(ctx, key); !errors.Is(err, errClosed) {
		t.Errorf("RevokeSharingKey with a closed key returned %v, want errClosed", err)
	}
}

// TestSDKCloseDuringCall is the race the guard exists for: a shutdown path
// closing the SDK while another goroutine is mid-call. Without the lock, Close
// freed the handle underneath the call. It is run under -race, where an
// unsynchronised free shows up as a data race even when it does not crash.
func TestSDKCloseDuringCall(t *testing.T) {
	net := NewMockNetwork(10)
	defer net.Close()

	var seed [32]byte
	seed[0] = 42
	sdk, err := net.SDK(context.Background(), seed)
	if err != nil {
		t.Fatalf("mock sdk: %v", err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Either outcome is fine. Crashing or racing is not.
			if _, err := sdk.Account(context.Background()); err != nil && !errors.Is(err, errClosed) {
				return
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		sdk.Close()
	}()

	close(start)
	wg.Wait()

	// Close must have taken effect however the race landed.
	if _, err := sdk.Account(context.Background()); !errors.Is(err, errClosed) {
		t.Errorf("Account after Close returned %v, want errClosed", err)
	}
}

// TestObjectMetadataConcurrentAccess proves the metadata setter and reader are
// serialised against each other. sia_object_set_metadata takes a non-const
// handle and mutates it, so running it under a read lock alongside
// sia_object_metadata was a data race in Rust, and left Metadata able to size
// a buffer against one value and fill it from another.
//
// The race detector is what makes this test meaningful; it passes trivially
// without -race.
func TestObjectMetadataConcurrentAccess(t *testing.T) {
	obj := NewObject()
	defer obj.Close()

	values := [][]byte{
		bytes.Repeat([]byte("a"), 16),
		bytes.Repeat([]byte("b"), 512),
		bytes.Repeat([]byte("c"), 900),
	}
	obj.UpdateMetadata(values[0])

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 50 {
				obj.UpdateMetadata(values[(i+j)%len(values)])
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				// Whatever it returns must be one of the values in full, never
				// a buffer sized for one and filled from another.
				md := obj.Metadata()
				if md == nil {
					continue
				}
				ok := false
				for _, v := range values {
					if bytes.Equal(md, v) {
						ok = true
						break
					}
				}
				if !ok {
					t.Errorf("Metadata returned %d bytes matching no value written", len(md))
					return
				}
			}
		}()
	}
	wg.Wait()
}
