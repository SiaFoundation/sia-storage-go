//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

// closeRecords releases the handles a listing minted, which the caller owns.
func closeRecords(recs []KeyRecord) {
	for _, r := range recs {
		if r.Key != nil {
			r.Key.Close()
		}
	}
}

// TestSharingKeyLifecycle walks the whole owner side path: create a key, attach
// an object, see it listed, detach it and revoke the key.
func TestSharingKeyLifecycle(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	obj := uploadPinned(t, sdk, payload(payloadSize))
	defer obj.Close()

	key, err := sdk.CreateSharingKey(ctx, "holiday album", time.Time{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer key.Close()

	rec, err := sdk.SharingKey(ctx, key)
	if err != nil {
		t.Fatalf("fetch key: %v", err)
	}
	if rec.Description != "holiday album" {
		t.Fatalf("description came back as %q", rec.Description)
	}
	if !rec.Stats.ExpiresAt.IsZero() {
		t.Fatalf("a key created without an expiry reports %v", rec.Stats.ExpiresAt)
	}
	if rec.Stats.CreatedAt.IsZero() {
		t.Fatal("createdAt is zero")
	}
	if rec.Stats.ObjectCount != 0 {
		t.Fatalf("a fresh key already has %d objects", rec.Stats.ObjectCount)
	}

	if err := sdk.ShareObject(ctx, key, obj); err != nil {
		t.Fatalf("share: %v", err)
	}

	shared, err := sdk.SharedObjects(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("list shared: %v", err)
	}
	defer func() {
		for _, o := range shared {
			o.Close()
		}
	}()
	if len(shared) != 1 {
		t.Fatalf("the key lists %d objects, want 1", len(shared))
	}
	if shared[0].ID() != obj.ID() {
		t.Fatal("the listed object is not the one that was shared")
	}

	if rec, err = sdk.SharingKey(ctx, key); err != nil {
		t.Fatalf("refetch key: %v", err)
	} else if rec.Stats.ObjectCount != 1 {
		t.Fatalf("stats report %d objects after sharing one", rec.Stats.ObjectCount)
	}

	if err := sdk.UnshareObject(ctx, key, obj.ID()); err != nil {
		t.Fatalf("unshare: %v", err)
	}
	if after, err := sdk.SharedObjects(ctx, key, 0, 0); err != nil {
		t.Fatalf("list after unshare: %v", err)
	} else if len(after) != 0 {
		for _, o := range after {
			o.Close()
		}
		t.Fatalf("the key still lists %d objects after detaching", len(after))
	}

	if err := sdk.RevokeSharingKey(ctx, key); err != nil {
		t.Fatalf("revoke: %v", err)
	}
}

// TestSharingKeySeedRoundTrip proves the seed is the whole credential, which is
// what makes handing one out equivalent to handing over the objects.
func TestSharingKeySeedRoundTrip(t *testing.T) {
	_, sdk := transferSDK(t)

	key, err := sdk.CreateSharingKey(context.Background(), "seed test", time.Time{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer key.Close()

	seed := key.Export()
	if seed == ([32]byte{}) {
		t.Fatal("the exported seed is all zeroes")
	}

	imported := ImportSharingKey(seed)
	defer imported.Close()
	if imported.PublicKey() != key.PublicKey() {
		t.Fatal("a key rebuilt from its seed has a different public key")
	}
	if imported.Export() != seed {
		t.Fatal("the reimported key exports a different seed")
	}
}

// TestSharingKeyGrantsReads is the point of the feature: a holder of the seed
// alone can read the bytes, without the owner's app key.
func TestSharingKeyGrantsReads(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()
	want := payload(payloadSize)

	obj := uploadPinned(t, sdk, want)
	defer obj.Close()

	key, err := sdk.CreateSharingKey(ctx, "reader", time.Time{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer key.Close()
	if err := sdk.ShareObject(ctx, key, obj); err != nil {
		t.Fatalf("share: %v", err)
	}

	// Come back to the object through nothing but the seed.
	recipient := ImportSharingKey(key.Export())
	defer recipient.Close()
	shared, err := sdk.SharedObjects(ctx, recipient, 0, 0)
	if err != nil {
		t.Fatalf("list as recipient: %v", err)
	}
	if len(shared) != 1 {
		t.Fatalf("the recipient sees %d objects, want 1", len(shared))
	}
	defer shared[0].Close()

	dl, err := sdk.Download(ctx, shared[0], DownloadOptions{})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	defer dl.Close()
	got, err := io.ReadAll(dl)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the shared object did not read back to the same bytes")
	}
}

// TestSharingKeysListing proves the account level listing returns the keys that
// were created, each with a handle of its own.
func TestSharingKeysListing(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	want := map[types.PublicKey]string{}
	for _, desc := range []string{"first", "second", "third"} {
		key, err := sdk.CreateSharingKey(ctx, desc, time.Time{})
		if err != nil {
			t.Fatalf("create %s: %v", desc, err)
		}
		want[key.PublicKey()] = desc
		key.Close()
	}

	recs, err := sdk.SharingKeys(ctx, 0, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer closeRecords(recs)
	if len(recs) < len(want) {
		t.Fatalf("listed %d keys, created %d", len(recs), len(want))
	}

	for _, r := range recs {
		if r.Key == nil {
			t.Fatal("a listed record has no key handle")
		}
		desc, ok := want[r.Key.PublicKey()]
		if !ok {
			continue
		}
		if r.Description != desc {
			t.Fatalf("key %v is described as %q, want %q", r.Key.PublicKey(), r.Description, desc)
		}
		delete(want, r.Key.PublicKey())
	}
	if len(want) != 0 {
		t.Fatalf("%d created keys did not appear in the listing", len(want))
	}
}

// TestSharingKeyExpiry proves an expiry reaches the indexer and comes back,
// rather than being silently dropped as the never expires case.
func TestSharingKeyExpiry(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	// Microseconds are the boundary's resolution, so compare at that.
	expires := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Microsecond)
	key, err := sdk.CreateSharingKey(ctx, "expiring", expires)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer key.Close()

	rec, err := sdk.SharingKey(ctx, key)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !rec.Stats.ExpiresAt.Equal(expires) {
		t.Fatalf("expiry came back as %v, want %v", rec.Stats.ExpiresAt, expires)
	}
}

// TestUnshareNotAttached proves the dedicated status code reaches the caller as
// a sentinel rather than as message text.
func TestUnshareNotAttached(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	key, err := sdk.CreateSharingKey(ctx, "empty", time.Time{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer key.Close()

	var id types.Hash256
	id[0] = 0xCD
	if err := sdk.UnshareObject(ctx, key, id); !errors.Is(err, ErrObjectNotAttached) {
		t.Fatalf("detaching an unattached object returned %v, want ErrObjectNotAttached", err)
	}
}

// TestSharingKeyCloseIsIdempotent proves the cleanup and an explicit Close
// cannot both free the same key.
func TestSharingKeyCloseIsIdempotent(t *testing.T) {
	var seed [32]byte
	seed[0] = 3
	key := ImportSharingKey(seed)
	if err := key.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := key.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
