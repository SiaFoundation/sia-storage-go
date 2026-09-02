package siastorage

import (
	"errors"
	"net/http"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

// seedSharedObject pins a slab and an object owned by appKey, spread over the
// given number of slices of that one slab, and returns the object.
func seedSharedObject(t *testing.T, mc *mockAppClient, appKey types.PrivateKey, slices int) slabs.SealedObject {
	t.Helper()

	params := slabs.SlabPinParams{
		MinShards: 2,
		Sectors: []slabs.PinnedSector{
			{Root: frand.Entropy256(), HostKey: frand.Entropy256()},
			{Root: frand.Entropy256(), HostKey: frand.Entropy256()},
			{Root: frand.Entropy256(), HostKey: frand.Entropy256()},
		},
	}
	if _, err := mc.PinSlabs(t.Context(), appKey, params); err != nil {
		t.Fatal(err)
	}

	obj := slabs.SealedObject{EncryptedDataKey: frand.Bytes(sharing.EncryptionKeySize)}
	for range slices {
		obj.Slabs = append(obj.Slabs, slabs.SlabSlice{
			EncryptionKey: params.EncryptionKey,
			MinShards:     params.MinShards,
			Sectors:       params.Sectors,
			Length:        100,
		})
	}
	obj.Sign(appKey)
	if err := mc.PinObject(t.Context(), appKey, obj); err != nil {
		t.Fatal(err)
	}
	return obj
}

// sealSharedObject builds the attachment request for obj under sharingKey the
// way the SDK will: a field copy of a sealed object, never separately re-signed.
func sealSharedObject(obj slabs.SealedObject, sharingKey types.PrivateKey, metadata bool) sharing.SharedObjectRequest {
	sealed := slabs.SealedObject{
		Slabs:            obj.Slabs,
		EncryptedDataKey: frand.Bytes(sharing.EncryptionKeySize),
	}
	if metadata {
		sealed.EncryptedMetadataKey = frand.Bytes(sharing.EncryptionKeySize)
		sealed.EncryptedMetadata = frand.Bytes(64)
	}
	sealed.Sign(sharingKey)
	return sharing.SharedObjectRequest{
		ObjectID:             sealed.ID(),
		EncryptedDataKey:     sealed.EncryptedDataKey,
		DataSignature:        sealed.DataSignature,
		EncryptedMetadataKey: sealed.EncryptedMetadataKey,
		EncryptedMetadata:    sealed.EncryptedMetadata,
		MetadataSignature:    sealed.MetadataSignature,
	}
}

// newSharingKeyRequest derives a sharing key from appKey and returns it with a
// signed request to create it.
func newSharingKeyRequest(appKey types.PrivateKey, description string, expiresAt *time.Time) (types.PrivateKey, sharing.KeyRequest) {
	nonce := sharing.Nonce(frand.Entropy256())
	sharingKey := sharing.DeriveSharingKey(appKey, nonce)
	req := sharing.KeyRequest{Nonce: nonce, Description: description, ExpiresAt: expiresAt}
	req.Sign(sharingKey)
	return sharingKey, req
}

// assertHTTPStatus asserts err carries the given status and matches the given
// sentinel, the way the SDK will classify indexer responses. Both halves matter:
// [app.HTTPError.Is] matches a sentinel by looking for its message inside the
// response body, so the body's wording is load-bearing and not merely
// descriptive.
func assertHTTPStatus(t *testing.T, err error, want int, sentinel error) {
	t.Helper()
	var httpErr *app.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected *app.HTTPError, got %v", err)
	} else if httpErr.StatusCode != want {
		t.Fatalf("expected status %d, got %d (%s)", want, httpErr.StatusCode, httpErr.Body)
	} else if !errors.Is(err, sentinel) {
		t.Fatalf("expected a body matching %q, got %q", sentinel, httpErr.Body)
	}
}

// TestMockSharingRules pins the rules mockAppClient enforces on the sharing
// routes against the indexer's real behavior. It covers the mock rather than the
// SDK because the owner and recipient tests that follow are only meaningful if
// the mock rejects what the indexer rejects.
func TestMockSharingRules(t *testing.T) {
	ctx := t.Context()
	appKey := types.GeneratePrivateKey()
	other := types.GeneratePrivateKey()
	mc := newMockAppClient(newMockHostStore(3))

	// Drive the mock's clock explicitly rather than sleeping: the expiry and
	// touched-timestamp assertions below otherwise depend on real elapsed time,
	// and a wider window would only make a stall rarer, not harmless.
	clock := time.Now()
	mc.clock = func() time.Time { return clock }

	sharingKey, req := newSharingKeyRequest(appKey, "photos", nil)
	key, err := mc.AddSharingKey(ctx, appKey, req)
	if err != nil {
		t.Fatal(err)
	} else if key.PublicKey != sharingKey.PublicKey() {
		t.Fatalf("expected public key %v, got %v", sharingKey.PublicKey(), key.PublicKey)
	} else if key.Account != appKey.PublicKey() {
		t.Fatalf("expected account %v, got %v", appKey.PublicKey(), key.Account)
	} else if key.ObjectCount != 0 {
		t.Fatalf("expected a new key to have no objects, got %d", key.ObjectCount)
	}

	// creating the same key again is a conflict, never an update
	_, err = mc.AddSharingKey(ctx, appKey, req)
	assertHTTPStatus(t, err, http.StatusConflict, sharing.ErrSharingKeyExists)

	// the nonce is unique too, so reusing it under a different public key is the
	// same conflict
	reusedNonce := sharing.KeyRequest{Nonce: req.Nonce, Description: "reused nonce"}
	reusedNonce.Sign(types.GeneratePrivateKey())
	_, err = mc.AddSharingKey(ctx, appKey, reusedNonce)
	assertHTTPStatus(t, err, http.StatusConflict, sharing.ErrSharingKeyExists)

	// validation runs before the signature is checked, so a zero nonce is
	// reported even though the request is also unsigned
	_, err = mc.AddSharingKey(ctx, appKey, sharing.KeyRequest{PublicKey: types.GeneratePrivateKey().PublicKey()})
	assertHTTPStatus(t, err, http.StatusBadRequest, sharing.ErrInvalidRequest)

	past := time.Now().Add(-time.Hour)
	_, expiredReq := newSharingKeyRequest(appKey, "already expired", &past)
	_, err = mc.AddSharingKey(ctx, appKey, expiredReq)
	assertHTTPStatus(t, err, http.StatusBadRequest, sharing.ErrInvalidRequest)

	tampered := req
	tampered.Nonce = sharing.Nonce(frand.Entropy256())
	tampered.PublicKey = types.GeneratePrivateKey().PublicKey()
	_, err = mc.AddSharingKey(ctx, appKey, tampered)
	assertHTTPStatus(t, err, http.StatusBadRequest, sharing.ErrInvalidRequest)

	// another account cannot see the key at all, and gets the same not-found it
	// would get for a key that never existed
	if _, err := mc.SharingKey(ctx, appKey, sharingKey.PublicKey()); err != nil {
		t.Fatal(err)
	}
	_, err = mc.SharingKey(ctx, other, sharingKey.PublicKey())
	assertHTTPStatus(t, err, http.StatusNotFound, sharing.ErrSharingKeyNotFound)

	if keys, err := mc.SharingKeys(ctx, appKey); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys, err := mc.SharingKeys(ctx, other); err != nil {
		t.Fatal(err)
	} else if len(keys) != 0 {
		t.Fatalf("expected another account to see no keys, got %d", len(keys))
	}

	obj := seedSharedObject(t, mc, appKey, 2)

	// an object this account has not pinned cannot be attached
	unpinned := sealSharedObject(slabs.SealedObject{Slabs: []slabs.SlabSlice{{MinShards: 1, Length: 1}}}, sharingKey, false)
	assertHTTPStatus(t, mc.AddSharedObject(ctx, appKey, sharingKey.PublicKey(), unpinned), http.StatusNotFound, slabs.ErrObjectNotFound)

	// the signatures are verified against the sharing key, so sealing under the
	// app key is rejected. The indexer never decrypts the sealed keys, so the
	// signature is the only check that catches a mixed-up key.
	assertHTTPStatus(t, mc.AddSharedObject(ctx, appKey, sharingKey.PublicKey(), sealSharedObject(obj, appKey, false)), http.StatusBadRequest, sharing.ErrInvalidRequest)

	// a malformed data key is rejected by validation, before the signature
	shortKey := sealSharedObject(obj, sharingKey, false)
	shortKey.EncryptedDataKey = shortKey.EncryptedDataKey[:8]
	assertHTTPStatus(t, mc.AddSharedObject(ctx, appKey, sharingKey.PublicKey(), shortKey), http.StatusBadRequest, sharing.ErrInvalidRequest)

	// a correctly signed request for a key the caller does not own reaches the
	// ownership check and is a not-found, which is what proves the ordering
	attachment := sealSharedObject(obj, sharingKey, true)
	assertHTTPStatus(t, mc.AddSharedObject(ctx, other, sharingKey.PublicKey(), attachment), http.StatusNotFound, sharing.ErrSharingKeyNotFound)

	if err := mc.AddSharedObject(ctx, appKey, sharingKey.PublicKey(), attachment); err != nil {
		t.Fatal(err)
	}

	// the key's totals follow the indexer's formulas: the logical size counts
	// every slice, while the storage figures count the slab once even though two
	// slices reference it
	key, err = mc.SharingKey(ctx, appKey, sharingKey.PublicKey())
	if err != nil {
		t.Fatal(err)
	} else if key.ObjectCount != 1 {
		t.Fatalf("expected 1 object, got %d", key.ObjectCount)
	} else if key.ObjectSize != 200 {
		t.Fatalf("expected size 200, got %d", key.ObjectSize)
	} else if want := uint64(2) * proto.SectorSize; key.PinnedData != want {
		t.Fatalf("expected pinned data %d, got %d", want, key.PinnedData)
	} else if want := uint64(3) * proto.SectorSize; key.PinnedSize != want {
		t.Fatalf("expected pinned size %d, got %d", want, key.PinnedSize)
	}
	attachedAt := key.UpdatedAt

	// The same encrypted data key attached under a second sharing key. The data
	// sig hash covers only the object ID and that key, so the request is validly
	// signed for the second key and reaches the store, where it collides on the
	// table-wide unique encrypted data key. Tampering with the key bytes instead
	// would be rejected by the signature check first.
	second := seedSharedObject(t, mc, appKey, 1)
	secondKey, secondReq := newSharingKeyRequest(appKey, "second key", nil)
	if _, err := mc.AddSharingKey(ctx, appKey, secondReq); err != nil {
		t.Fatal(err)
	}
	collision := slabs.SealedObject{Slabs: obj.Slabs, EncryptedDataKey: attachment.EncryptedDataKey}
	collision.Sign(secondKey)
	assertHTTPStatus(t, mc.AddSharedObject(ctx, appKey, secondKey.PublicKey(), sharing.SharedObjectRequest{
		ObjectID:          collision.ID(),
		EncryptedDataKey:  collision.EncryptedDataKey,
		DataSignature:     collision.DataSignature,
		MetadataSignature: collision.MetadataSignature,
	}), http.StatusConflict, sharing.ErrSharedObjectConflict)

	// the rejected attachment left the totals alone
	if key, err := mc.SharingKey(ctx, appKey, sharingKey.PublicKey()); err != nil {
		t.Fatal(err)
	} else if key.ObjectCount != 1 {
		t.Fatalf("a conflict changed the object count to %d", key.ObjectCount)
	}

	// re-attaching the same object to the same key overwrites the sealed keys
	// and signatures, and touches the key without changing its object count
	clock = clock.Add(time.Second)
	if err := mc.AddSharedObject(ctx, appKey, sharingKey.PublicKey(), sealSharedObject(obj, sharingKey, false)); err != nil {
		t.Fatal(err)
	}
	if key, err := mc.SharingKey(ctx, appKey, sharingKey.PublicKey()); err != nil {
		t.Fatal(err)
	} else if key.ObjectCount != 1 {
		t.Fatalf("re-attaching changed the object count to %d", key.ObjectCount)
	} else if !key.UpdatedAt.After(attachedAt) {
		t.Fatal("re-attaching did not touch the key")
	}

	// the listed object carries the owner's slabs and the attachment's re-sealed
	// keys, so it verifies under the sharing key rather than the app key
	objects, err := mc.SharingKeyObjects(ctx, appKey, sharingKey.PublicKey())
	if err != nil {
		t.Fatal(err)
	} else if len(objects) != 1 {
		t.Fatalf("expected 1 attached object, got %d", len(objects))
	} else if objects[0].ID() != obj.ID() {
		t.Fatalf("expected object %v, got %v", obj.ID(), objects[0].ID())
	} else if err := objects[0].VerifySignatures(sharingKey.PublicKey()); err != nil {
		t.Fatalf("attached object does not verify under the sharing key: %v", err)
	}
	_, err = mc.SharingKeyObjects(ctx, other, sharingKey.PublicKey())
	assertHTTPStatus(t, err, http.StatusNotFound, sharing.ErrSharingKeyNotFound)

	// paging matches the indexer, including the default limit that truncates a
	// longer list without erroring
	for range 120 {
		_, bulk := newSharingKeyRequest(appKey, "bulk", nil)
		if _, err := mc.AddSharingKey(ctx, appKey, bulk); err != nil {
			t.Fatal(err)
		}
	}
	if keys, err := mc.SharingKeys(ctx, appKey); err != nil {
		t.Fatal(err)
	} else if len(keys) != 100 {
		t.Fatalf("expected the default limit to truncate to 100, got %d", len(keys))
	}
	if keys, err := mc.SharingKeys(ctx, appKey, api.WithLimit(api.MaxLimit)); err != nil {
		t.Fatal(err)
	} else if len(keys) != 122 {
		t.Fatalf("expected 122 keys, got %d", len(keys))
	}
	if keys, err := mc.SharingKeys(ctx, appKey, api.WithOffset(121), api.WithLimit(api.MaxLimit)); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Fatalf("expected 1 key on the last page, got %d", len(keys))
	}
	_, err = mc.SharingKeys(ctx, appKey, api.WithLimit(api.MaxLimit+1))
	assertHTTPStatus(t, err, http.StatusBadRequest, api.ErrInvalidLimit)

	// paging is parsed before the key is looked up, so this outranks the
	// not-found another account would otherwise get
	_, err = mc.SharingKeyObjects(ctx, other, sharingKey.PublicKey(), api.WithLimit(api.MaxLimit+1))
	assertHTTPStatus(t, err, http.StatusBadRequest, api.ErrInvalidLimit)

	// detaching reports the same not-found for an object that was never
	// attached and for a key owned by someone else
	detachErr := mc.DeleteSharedObject(ctx, appKey, sharingKey.PublicKey(), second.ID())
	assertHTTPStatus(t, detachErr, http.StatusNotFound, sharing.ErrSharedObjectNotFound)
	assertHTTPStatus(t, mc.DeleteSharedObject(ctx, other, sharingKey.PublicKey(), obj.ID()), http.StatusNotFound, sharing.ErrSharedObjectNotFound)

	// Sentinel matching is a substring search over the body, and "shared object
	// not found" contains "object not found", so a detach failure also matches
	// the unrelated object sentinel. Anything mapping these errors has to key on
	// the route it called or on StatusCode, never on this pair of sentinels
	// alone.
	if !errors.Is(detachErr, slabs.ErrObjectNotFound) {
		t.Fatal("expected the shared object sentinel to subsume the object one; if this now fails, the overlap is gone and the mapping in UnshareObject can be simplified")
	}
	if err := mc.DeleteSharedObject(ctx, appKey, sharingKey.PublicKey(), obj.ID()); err != nil {
		t.Fatal(err)
	}
	if key, err := mc.SharingKey(ctx, appKey, sharingKey.PublicKey()); err != nil {
		t.Fatal(err)
	} else if key.ObjectCount != 0 || key.ObjectSize != 0 || key.PinnedData != 0 || key.PinnedSize != 0 {
		t.Fatalf("detaching left totals behind: %+v", key)
	}

	assertHTTPStatus(t, mc.DeleteSharingKey(ctx, other, sharingKey.PublicKey()), http.StatusNotFound, sharing.ErrSharingKeyNotFound)
	if err := mc.DeleteSharingKey(ctx, appKey, sharingKey.PublicKey()); err != nil {
		t.Fatal(err)
	}
	assertHTTPStatus(t, mc.DeleteSharingKey(ctx, appKey, sharingKey.PublicKey()), http.StatusNotFound, sharing.ErrSharingKeyNotFound)
	_, err = mc.SharingKey(ctx, appKey, sharingKey.PublicKey())
	assertHTTPStatus(t, err, http.StatusNotFound, sharing.ErrSharingKeyNotFound)

	// Deleting the object detaches it everywhere, the way
	// shared_objects.object_id ON DELETE CASCADE does.
	cascadeKey, cascadeReq := newSharingKeyRequest(appKey, "cascade", nil)
	if _, err := mc.AddSharingKey(ctx, appKey, cascadeReq); err != nil {
		t.Fatal(err)
	}
	cascadeObj := seedSharedObject(t, mc, appKey, 1)
	if err := mc.AddSharedObject(ctx, appKey, cascadeKey.PublicKey(), sealSharedObject(cascadeObj, cascadeKey, false)); err != nil {
		t.Fatal(err)
	}
	attached, err := mc.SharingKey(ctx, appKey, cascadeKey.PublicKey())
	if err != nil {
		t.Fatal(err)
	} else if attached.ObjectCount != 1 {
		t.Fatalf("expected the object to be attached, got a count of %d", attached.ObjectCount)
	}

	clock = clock.Add(time.Second)
	if err := mc.DeleteObject(ctx, appKey, cascadeObj.ID()); err != nil {
		t.Fatal(err)
	}
	if key, err := mc.SharingKey(ctx, appKey, cascadeKey.PublicKey()); err != nil {
		t.Fatal(err)
	} else if key.ObjectCount != 0 || key.ObjectSize != 0 || key.PinnedData != 0 || key.PinnedSize != 0 {
		t.Fatalf("deleting the object left totals behind: %+v", key)
	} else if !key.UpdatedAt.After(attached.UpdatedAt) {
		t.Fatal("the cascade did not touch the key, which the indexer's trigger does")
	}
	if objects, err := mc.SharingKeyObjects(ctx, appKey, cascadeKey.PublicKey()); err != nil {
		t.Fatal(err)
	} else if len(objects) != 0 {
		t.Fatalf("expected the attachment to be gone, got %d", len(objects))
	}
	assertHTTPStatus(t, mc.DeleteSharedObject(ctx, appKey, cascadeKey.PublicKey(), cascadeObj.ID()), http.StatusNotFound, sharing.ErrSharedObjectNotFound)

	// Expiry is asymmetric, and the SDK has to cope with it: the reads treat an
	// expired key as gone, but it stays deletable until the indexer's pruner
	// removes it.
	expiresAt := clock.Add(time.Hour)

	expiring, expiringReq := newSharingKeyRequest(appKey, "expiring", &expiresAt)
	if _, err := mc.AddSharingKey(ctx, appKey, expiringReq); err != nil {
		t.Fatal(err)
	}
	if _, err := mc.SharingKey(ctx, appKey, expiring.PublicKey()); err != nil {
		t.Fatal(err)
	}

	clock = expiresAt.Add(time.Second)
	_, err = mc.SharingKey(ctx, appKey, expiring.PublicKey())
	assertHTTPStatus(t, err, http.StatusNotFound, sharing.ErrSharingKeyNotFound)
	assertHTTPStatus(t, mc.AddSharedObject(ctx, appKey, expiring.PublicKey(), sealSharedObject(obj, expiring, false)), http.StatusNotFound, sharing.ErrSharingKeyNotFound)
	if err := mc.DeleteSharingKey(ctx, appKey, expiring.PublicKey()); err != nil {
		t.Fatalf("an expired key should still be deletable: %v", err)
	}
}
