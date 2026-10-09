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
	obj := NewEmptyObject()

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
	obj := NewEmptyObject()

	for _, size := range []int{1, 31, 32, 33, 4096} {
		want := bytes.Repeat([]byte{byte(size)}, size)
		obj = obj.WithMetadata(want)
		got := obj.Metadata()
		if !bytes.Equal(got, want) {
			t.Fatalf("metadata of %d bytes came back as %d bytes", size, len(got))
		}
	}

	obj = obj.WithMetadata(nil)
	if md := obj.Metadata(); md != nil {
		t.Fatalf("expected clearing metadata to leave none, got %d bytes", len(md))
	}
}

// TestObjectIDIsContentAddressed proves ID comes from the slabs rather than
// being random, which is what lets a caller detect that two uploads produced
// the same object.
func TestObjectIDIsContentAddressed(t *testing.T) {
	a, b := NewEmptyObject(), NewEmptyObject()

	if a.ID() != b.ID() {
		t.Fatal("two empty objects should share an ID, so the ID is not derived from the slabs")
	}
	// Metadata is not part of the ID, only the slabs are.
	a = a.WithMetadata([]byte(`{"name":"a"}`))
	if a.ID() != b.ID() {
		t.Fatal("metadata must not change the object ID")
	}
	var zero types.Hash256
	if a.ID() == zero {
		t.Fatal("an empty object's ID should still be a real hash, not zero")
	}
}

// TestSharingKeyUseAfterClose proves a closed key reports a zero value rather
// than reading memory Close already freed. Before the guard these went straight
// to the native side, which is harder to notice than a crash.
func TestSharingKeyUseAfterClose(t *testing.T) {
	_, sdk := testSDK(t)
	ctx := context.Background()

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

// TestObjectConcurrentUse proves an object is safe to share between goroutines
// without a lock. Nothing can change one once it exists: WithMetadata returns a
// new object and leaves the receiver alone, and Metadata hands back a copy. The
// old handle needed a mutex because sia_object_set_metadata mutated it in
// place, which left Metadata able to size a buffer against one value and fill
// it from another.
//
// The race detector is what makes this test meaningful; it passes trivially
// without -race.
func TestObjectConcurrentUse(t *testing.T) {
	values := [][]byte{
		bytes.Repeat([]byte("a"), 16),
		bytes.Repeat([]byte("b"), 512),
		bytes.Repeat([]byte("c"), 900),
	}
	shared := NewEmptyObject().WithMetadata(values[0])

	var wg sync.WaitGroup
	// Derive new objects from the shared one while others read it.
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 50 {
				want := values[(i+j)%len(values)]
				derived := shared.WithMetadata(want)
				if !bytes.Equal(derived.Metadata(), want) {
					t.Errorf("WithMetadata produced %d bytes, want %d", len(derived.Metadata()), len(want))
					return
				}
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				// Deriving cannot disturb the object it was derived from.
				if md := shared.Metadata(); !bytes.Equal(md, values[0]) {
					t.Errorf("the shared object changed under a reader: %d bytes", len(md))
					return
				}
			}
		}()
	}
	wg.Wait()

	// Writing through a returned copy cannot reach the object either.
	md := shared.Metadata()
	md[0] ^= 0xff
	if bytes.Equal(shared.Metadata(), md) {
		t.Fatal("Metadata handed out the object's own buffer")
	}
}
