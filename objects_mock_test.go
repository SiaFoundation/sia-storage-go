//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

// uploadPinned uploads and then pins, which is what puts the object record at
// the indexer. The upload itself only pins the slabs.
func uploadPinned(t *testing.T, sdk *SDK, data []byte) *Object {
	t.Helper()
	obj := uploadPayload(t, sdk, data, UploadOptions{})
	if err := sdk.PinObject(context.Background(), obj); err != nil {
		t.Fatalf("pin: %v", err)
	}
	return obj
}

// TestObjectFetchByID proves an uploaded object can be found again from its ID
// alone, which is what a caller who persisted only the ID has to rely on.
func TestObjectFetchByID(t *testing.T) {
	_, sdk := transferSDK(t)
	want := payload(payloadSize)

	uploaded := uploadPinned(t, sdk, want)
	defer uploaded.Close()

	fetched, err := sdk.Object(context.Background(), uploaded.ID())
	if err != nil {
		t.Fatalf("fetch by id: %v", err)
	}
	defer fetched.Close()

	if fetched.ID() != uploaded.ID() {
		t.Fatal("the fetched object has a different ID")
	}
	if fetched.Size() != uploaded.Size() {
		t.Fatalf("fetched size %d, uploaded %d", fetched.Size(), uploaded.Size())
	}

	// The fetched handle has to carry usable keys, not just the right numbers,
	// so read it back through the download path.
	dl, err := sdk.Download(context.Background(), fetched, DownloadOptions{})
	if err != nil {
		t.Fatalf("download the fetched object: %v", err)
	}
	defer dl.Close()
	got, err := io.ReadAll(dl)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the object fetched by ID did not download to the same bytes")
	}
}

// TestObjectMetadataPersists proves metadata set locally reaches the indexer
// and comes back on a fresh fetch, which is the crossing a caller storing a
// filename depends on.
func TestObjectMetadataPersists(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	uploaded := uploadPinned(t, sdk, payload(1<<20))
	defer uploaded.Close()

	want := []byte(`{"filename":"holiday.jpg"}`)
	uploaded.UpdateMetadata(want)
	if err := sdk.UpdateObjectMetadata(ctx, uploaded); err != nil {
		t.Fatalf("update metadata: %v", err)
	}

	fetched, err := sdk.Object(ctx, uploaded.ID())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	defer fetched.Close()
	if got := fetched.Metadata(); !bytes.Equal(got, want) {
		t.Fatalf("metadata came back as %q", got)
	}
}

// TestObjectDelete proves a deleted object is gone from the indexer's view.
func TestObjectDelete(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	uploaded := uploadPinned(t, sdk, payload(1<<20))
	id := uploaded.ID()
	uploaded.Close()

	if err := sdk.DeleteObject(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if obj, err := sdk.Object(ctx, id); err == nil {
		obj.Close()
		t.Fatal("the object is still fetchable after being deleted")
	}
}

// TestObjectDeleteUnknown proves deleting something that was never there
// reports rather than pretending to succeed.
func TestObjectDeleteUnknown(t *testing.T) {
	_, sdk := transferSDK(t)

	var id types.Hash256
	id[0] = 0xAB
	if err := sdk.DeleteObject(context.Background(), id); err == nil {
		t.Fatal("deleting an unknown object reported success")
	}
}

// TestPruneSlabs proves the call reaches the indexer. Pruning after a delete is
// what actually releases the pinned storage.
func TestPruneSlabs(t *testing.T) {
	net, sdk := transferSDK(t)
	ctx := context.Background()

	uploaded := uploadPinned(t, sdk, payload(payloadSize))
	id := uploaded.ID()
	uploaded.Close()

	if net.PinnedSlabs() == 0 {
		t.Fatal("the upload pinned no slabs")
	}
	if err := sdk.DeleteObject(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := sdk.PruneSlabs(ctx); err != nil {
		t.Fatalf("prune: %v", err)
	}
}

// TestObjectShareURLRoundTrip proves a share URL resolves back to an object a
// holder can download, which is the whole point of handing one out.
func TestObjectShareURLRoundTrip(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()
	want := payload(payloadSize)

	uploaded := uploadPinned(t, sdk, want)
	defer uploaded.Close()

	url, err := sdk.ObjectShareURL(uploaded, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("share url: %v", err)
	}
	if url == "" {
		t.Fatal("share url is empty")
	}

	shared, err := sdk.ObjectFromShareURL(ctx, url)
	if err != nil {
		t.Fatalf("resolve share url: %v", err)
	}
	defer shared.Close()
	if shared.ID() != uploaded.ID() {
		t.Fatal("the shared URL resolved to a different object")
	}

	dl, err := sdk.Download(ctx, shared, DownloadOptions{})
	if err != nil {
		t.Fatalf("download the shared object: %v", err)
	}
	defer dl.Close()
	got, err := io.ReadAll(dl)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the shared object did not download to the same bytes")
	}
}

// TestObjectShareURLRequiresExpiry keeps the zero time from reaching the
// boundary, where it decodes as a timestamp the native side rejects with a
// message that says nothing about the real mistake.
func TestObjectShareURLRequiresExpiry(t *testing.T) {
	_, sdk := transferSDK(t)

	uploaded := uploadPayload(t, sdk, payload(1<<20), UploadOptions{})
	defer uploaded.Close()

	if _, err := sdk.ObjectShareURL(uploaded, time.Time{}); err == nil {
		t.Fatal("a zero expiration produced a share URL")
	}
}

// TestObjectFromBadShareURL proves a malformed URL is an error rather than a
// handle that fails later.
func TestObjectFromBadShareURL(t *testing.T) {
	_, sdk := transferSDK(t)

	obj, err := sdk.ObjectFromShareURL(context.Background(), "https://example.invalid/not-a-share")
	if err == nil {
		obj.Close()
		t.Fatal("a malformed share URL resolved to an object")
	}
	if errors.Is(err, errClosed) {
		t.Fatalf("unexpected error shape: %v", err)
	}
}

// TestSealedObjectRoundTrip proves an object survives being persisted as JSON
// and opened again, which is what a consumer storing objects in its own schema
// depends on.
func TestSealedObjectRoundTrip(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()
	want := payload(payloadSize)

	uploaded := uploadPinned(t, sdk, want)
	defer uploaded.Close()
	uploaded.UpdateMetadata([]byte(`{"filename":"sealed.bin"}`))

	sealed, err := sdk.SealObject(uploaded)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if !json.Valid(sealed) {
		t.Fatalf("the sealed form is not valid JSON: %s", sealed)
	}

	opened, err := sdk.ObjectFromSealed(sealed)
	if err != nil {
		t.Fatalf("open sealed: %v", err)
	}
	defer opened.Close()

	if opened.ID() != uploaded.ID() {
		t.Fatal("the opened object has a different ID")
	}
	if opened.Size() != uploaded.Size() {
		t.Fatalf("opened size %d, sealed %d", opened.Size(), uploaded.Size())
	}
	if got := opened.Metadata(); !bytes.Equal(got, []byte(`{"filename":"sealed.bin"}`)) {
		t.Fatalf("metadata did not survive sealing, got %q", got)
	}

	// The keys have to come back usable, not just the numbers.
	dl, err := sdk.Download(ctx, opened, DownloadOptions{})
	if err != nil {
		t.Fatalf("download the opened object: %v", err)
	}
	defer dl.Close()
	got, err := io.ReadAll(dl)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the object opened from sealed JSON did not download to the same bytes")
	}
}

// TestSealedObjectRejectsForeignKey proves the signature check is real, so a
// sealed object from another account fails to open rather than yielding a
// handle that reads nothing.
func TestSealedObjectRejectsForeignKey(t *testing.T) {
	net, sdk := transferSDK(t)

	uploaded := uploadPinned(t, sdk, payload(1<<20))
	defer uploaded.Close()
	sealed, err := sdk.SealObject(uploaded)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}

	var otherSeed [32]byte
	otherSeed[0] = 200
	other, err := net.SDK(context.Background(), otherSeed)
	if err != nil {
		t.Fatalf("second sdk: %v", err)
	}
	defer other.Close()

	if obj, err := other.ObjectFromSealed(sealed); err == nil {
		obj.Close()
		t.Fatal("a sealed object opened under a different app key")
	}
}

// TestSealedObjectRejectsGarbage proves a malformed document is an error rather
// than a panic crossing the boundary.
func TestSealedObjectRejectsGarbage(t *testing.T) {
	_, sdk := transferSDK(t)

	for _, bad := range [][]byte{nil, []byte(""), []byte("{"), []byte(`{"slabs":[]}`)} {
		if obj, err := sdk.ObjectFromSealed(bad); err == nil {
			obj.Close()
			t.Fatalf("%q opened as a sealed object", bad)
		}
	}
}
