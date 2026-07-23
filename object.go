package siastorage

import (
	"context"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/keys"
	"go.sia.tech/indexd/slabs"
	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/frand"
)

// A SealedObject is an object that has been locked with an app key.
// It can be safely serialized and shared, but cannot be used to access
// the underlying data until it has been unlocked with the app key.
type SealedObject struct {
	slabs.SealedObject
}

// Open decrypts the SealedObject using the given app key and returns an
// Object.
func (so *SealedObject) Open(appKey types.PrivateKey) (Object, error) {
	return objectFromSealedObject(so.SealedObject, appKey)
}

// An Object represents a collection of slabs that can be used to access
// encrypted data. The master key is used to encrypt/decrypt the data and
// metadata, and should be kept secret.
//
// It has no public fields to prevent accidental leakage of unencrypted data.
type Object struct {
	dataKey   []byte
	slabs     []slabs.SlabSlice
	metadata  json.RawMessage
	createdAt time.Time
	updatedAt time.Time
}

// ID returns the object's ID, which is a hash of its slabs.
func (o *Object) ID() types.Hash256 {
	return slabs.ObjectID(o.slabs)
}

// CreatedAt returns the time the object was created.
func (o *Object) CreatedAt() time.Time {
	return o.createdAt
}

// UpdatedAt returns the time the object was last updated.
func (o *Object) UpdatedAt() time.Time {
	return o.updatedAt
}

// Seal returns a SealedObject that can be safely serialized and shared.
func (o *Object) Seal(appKey types.PrivateKey) SealedObject {
	objectID := o.ID()

	seal := func(keyCipher cipher.AEAD, plaintext []byte) []byte {
		nonce := frand.Bytes(keyCipher.NonceSize())
		return keyCipher.Seal(nonce, nonce, plaintext, nil)
	}
	encryptedDataKey := seal(dataKeyCipher(appKey, objectID), o.dataKey)

	var encryptedMetaKey, encryptedMetadata []byte
	if len(o.metadata) > 0 {
		metaDataKey := frand.Bytes(32)
		encryptedMetaKey = seal(metadataKeyCipher(appKey, objectID), metaDataKey)
		encryptedMetadata = seal(metadataCipher(metaDataKey), o.metadata)
	}

	so := SealedObject{slabs.SealedObject{
		EncryptedDataKey:     encryptedDataKey,
		Slabs:                cloneSlabs(o.slabs),
		EncryptedMetadataKey: encryptedMetaKey,
		EncryptedMetadata:    encryptedMetadata,
		CreatedAt:            o.createdAt,
		UpdatedAt:            o.updatedAt,
	}}
	so.Sign(appKey)
	return so
}

// Size returns the total size of the object in bytes.
func (o *Object) Size() uint64 {
	var size uint64
	for _, ss := range o.slabs {
		size += uint64(ss.Length)
	}
	return size
}

// UnsafeDataKey returns the key used to encrypt the object's data.
//
// The data key alone decrypts the object's data. Never store it in plaintext
// and do not reuse it for new objects.
//
// Prefer sealing the object with [Object.Seal] instead.
func (o *Object) UnsafeDataKey() [32]byte {
	var key [32]byte
	copy(key[:], o.dataKey)
	return key
}

// Slabs returns a copy of the object's slabs.
func (o *Object) Slabs() []slabs.SlabSlice {
	return cloneSlabs(o.slabs)
}

// Metadata returns a copy of the object's metadata.
func (o *Object) Metadata() json.RawMessage {
	return slices.Clone(o.metadata)
}

// UpdateMetadata updates the object's metadata.
func (o *Object) UpdateMetadata(meta json.RawMessage) {
	o.metadata = slices.Clone(meta)
}

// NewEmptyObject creates a new Object to use in [Upload].
func NewEmptyObject() Object {
	now := time.Now()
	return Object{
		dataKey:   frand.Bytes(32),
		createdAt: now,
		updatedAt: now,
	}
}

// NewUnsafeObject creates an Object from a data key and slabs. It can be used
// together with [Object.DataKey] and [Object.Slabs] to reconstruct an object
// whose key and slabs were stored outside the indexer.
//
// This is useful for interoperability with systems such as IPFS or LBRY,
// where an object's components are persisted separately and the object must
// be reconstructed from them.
//
// Objects produced by [SDK.Upload] are guaranteed to be safe to reconstruct.
// Others, not so much. Here be dragons.
//
// Invariants:
//   - The data key must be the one that encrypted the slabs. A mismatched key
//     fails silently: downloads succeed but return garbage.
//   - Slab keys must never be reused. Reuse compromises encryption.
//   - Each slab's version must match the version it was encrypted with. A
//     mislabeled slab decrypts to garbage without error.
//   - Each slab's offset and length must match how the data was encrypted:
//     offset seeks the keystream, and slab order defines the object's byte
//     stream. Wrong values silently corrupt or reorder the data.
//
// The returned object has empty metadata and sets CreatedAt/UpdatedAt to time.Now.
func NewUnsafeObject(dataKey [32]byte, ss []slabs.SlabSlice) Object {
	now := time.Now()
	return Object{
		dataKey:   dataKey[:],
		slabs:     cloneSlabs(ss),
		createdAt: now,
		updatedAt: now,
	}
}

// ObjectEvent represents a change to an object. If the object was deleted,
// Deleted is true and Object is nil.
type ObjectEvent struct {
	Key       types.Hash256
	Deleted   bool
	UpdatedAt time.Time
	Object    *Object
}

// ObjectEvents returns object events from the indexer, starting from the
// given cursor, up to the given limit. Unlike ListObjects, it preserves
// deletion events.
func (s *SDK) ObjectEvents(ctx context.Context, cursor slabs.Cursor, limit int) ([]ObjectEvent, error) {
	raw, err := s.app.ListObjects(ctx, s.appKey, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list object events: %w", err)
	}
	events := make([]ObjectEvent, len(raw))
	for i, ev := range raw {
		events[i] = ObjectEvent{
			Key:       ev.Key,
			Deleted:   ev.Deleted,
			UpdatedAt: ev.UpdatedAt,
		}
		if ev.Object != nil {
			so := SealedObject{*ev.Object}
			obj, err := so.Open(s.appKey)
			if err != nil {
				return nil, fmt.Errorf("failed to unseal object: %w", err)
			}
			events[i].Object = &obj
		}
	}
	return events, nil
}

// Object retrieves the object with the given key.
func (s *SDK) Object(ctx context.Context, objectKey types.Hash256) (Object, error) {
	lo, err := s.app.Object(ctx, s.appKey, objectKey)
	if err != nil {
		return Object{}, fmt.Errorf("failed to get locked object: %w", err)
	}
	so := SealedObject{lo}
	return so.Open(s.appKey)
}

// CreateSharedObjectURL creates a URL that can be used to share the object
// until the given time. The URL contains the encryption key required to decrypt
// the object's data and metadata.
//
// Sharing the URL allows anyone with the URL to read the object's data
// and metadata. They will not be able to modify the object or access any other
// objects in the account.
func (s *SDK) CreateSharedObjectURL(ctx context.Context, objectKey types.Hash256, validUntil time.Time) (string, error) {
	obj, err := s.Object(ctx, objectKey)
	if err != nil {
		return "", fmt.Errorf("failed to get object: %w", err)
	}
	return s.app.CreateSharedObjectURL(ctx, s.appKey, obj.ID(), obj.dataKey, validUntil)
}

// cloneSlabs returns a deep copy of the given slabs, cloning each slab's
// sectors so the returned slabs share no backing arrays with the originals.
func cloneSlabs(ss []slabs.SlabSlice) []slabs.SlabSlice {
	cloned := slices.Clone(ss)
	for i := range cloned {
		cloned[i].Sectors = slices.Clone(cloned[i].Sectors)
	}
	return cloned
}

// dataKeyCipher derives the data key cipher from the app key and object ID.
func dataKeyCipher(appKey types.PrivateKey, objectID types.Hash256) cipher.AEAD {
	key := keys.Derive(appKey, objectID[:], []byte("dataKey"), 32)
	cipher, _ := chacha20poly1305.NewX(key)
	return cipher
}

// metadataKeyCipher derives the metadata key cipher from the app key and object ID.
func metadataKeyCipher(appKey types.PrivateKey, objectID types.Hash256) cipher.AEAD {
	key := keys.Derive(appKey, objectID[:], []byte("metadataKey"), 32)
	cipher, _ := chacha20poly1305.NewX(key)
	return cipher
}

// metadataCipher returns the cipher used to encrypt/decrypt metadata.
func metadataCipher(metadataKey []byte) cipher.AEAD {
	cipher, _ := chacha20poly1305.NewX(metadataKey)
	return cipher
}

func unlockEncryptedMetadata(metadataKey, encryptedMeta []byte) (json.RawMessage, error) {
	if len(encryptedMeta) == 0 {
		return nil, nil
	}
	metadataCipher := metadataCipher(metadataKey)
	if len(encryptedMeta) < metadataCipher.NonceSize() {
		return nil, fmt.Errorf("encrypted metadata too short")
	}
	nonce := encryptedMeta[:metadataCipher.NonceSize()]
	metadata, err := metadataCipher.Open(nil, nonce, encryptedMeta[metadataCipher.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to unlock metadata: %w", err)
	}
	return metadata, nil
}

// objectFromSealedObject unlocks a SealedObject using the given app key.
func objectFromSealedObject(so slabs.SealedObject, appKey types.PrivateKey) (Object, error) {
	obj := Object{
		slabs:     cloneSlabs(so.Slabs),
		createdAt: so.CreatedAt,
		updatedAt: so.UpdatedAt,
	}
	objectID := obj.ID()
	if so.ID() != objectID {
		return Object{}, fmt.Errorf("object ID mismatch")
	} else if err := so.VerifySignatures(appKey.PublicKey()); err != nil {
		return Object{}, err
	}

	decryptKey := func(keyCipher cipher.AEAD, encryptedKey []byte) ([]byte, error) {
		if len(encryptedKey) < keyCipher.NonceSize() {
			return nil, fmt.Errorf("encrypted key is too short")
		}
		nonce := encryptedKey[:keyCipher.NonceSize()]
		var err error
		key, err := keyCipher.Open(nil, nonce, encryptedKey[keyCipher.NonceSize():], nil)
		if err != nil {
			return nil, fmt.Errorf("failed to unlock key: %w", err)
		}
		return key, nil
	}
	var err error
	obj.dataKey, err = decryptKey(dataKeyCipher(appKey, objectID), so.EncryptedDataKey)
	if err != nil {
		return Object{}, fmt.Errorf("failed to unlock data key: %w", err)
	}
	if len(so.EncryptedMetadata) > 0 {
		metaDataKey, err := decryptKey(metadataKeyCipher(appKey, objectID), so.EncryptedMetadataKey)
		if err != nil {
			return Object{}, fmt.Errorf("failed to unlock metadata key: %w", err)
		}
		obj.metadata, err = unlockEncryptedMetadata(metaDataKey, so.EncryptedMetadata)
		if err != nil {
			return Object{}, fmt.Errorf("failed to unlock metadata: %w", err)
		}
	}
	return obj, nil
}
