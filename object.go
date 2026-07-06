package siastorage

import (
	"encoding/json"
	"time"

	"go.sia.tech/core/types"
	ffi "go.sia.tech/siastorage/sia_storage_ffi"
)

// A SealedObject is an object that has been locked with an app key. It can be
// safely serialized and shared, but cannot be used to access the underlying
// data until it has been unlocked with the app key.
type SealedObject struct {
	ffi.SealedObject
}

// Open decrypts the SealedObject using the given app key and returns an
// Object.
func (so *SealedObject) Open(appKey types.PrivateKey) (Object, error) {
	ffiKey, err := appKeyFromPrivate(appKey)
	if err != nil {
		return Object{}, err
	}
	inner, err := ffi.PinnedObjectOpen(ffiKey, so.SealedObject)
	if err != nil {
		return Object{}, err
	}
	return Object{inner: inner}, nil
}

// An Object represents a collection of slabs that can be used to access
// encrypted data. It is created with [NewEmptyObject] and populated by
// [SDK.Upload].
type Object struct {
	inner *ffi.PinnedObject
}

// NewEmptyObject creates a new Object to use in [SDK.Upload].
func NewEmptyObject() Object {
	return Object{inner: ffi.NewPinnedObject()}
}

// ID returns the object's ID, which is a hash of its slabs.
func (o *Object) ID() types.Hash256 {
	h, _ := parseHash(o.inner.Id())
	return h
}

// CreatedAt returns the time the object was created.
func (o *Object) CreatedAt() time.Time {
	return o.inner.CreatedAt()
}

// UpdatedAt returns the time the object was last updated.
func (o *Object) UpdatedAt() time.Time {
	return o.inner.UpdatedAt()
}

// Seal returns a SealedObject that can be safely serialized and shared.
func (o *Object) Seal(appKey types.PrivateKey) SealedObject {
	ffiKey, err := appKeyFromPrivate(appKey)
	if err != nil {
		// appKey seeds are always 32 bytes, so this cannot fail in practice.
		panic("siastorage: invalid app key: " + err.Error())
	}
	return SealedObject{o.inner.Seal(ffiKey)}
}

// Size returns the total size of the object in bytes.
func (o *Object) Size() uint64 {
	return o.inner.Size()
}

// EncodedSize returns the total size of the object after erasure coding.
func (o *Object) EncodedSize() uint64 {
	return o.inner.EncodedSize()
}

// Slabs returns the object's slabs.
//
// The native SDK returned []slabs.SlabSlice; this returns the equivalent FFI
// [Slab] type.
func (o *Object) Slabs() []Slab {
	return o.inner.Slabs()
}

// Metadata returns a copy of the object's metadata.
func (o *Object) Metadata() json.RawMessage {
	return json.RawMessage(o.inner.Metadata())
}

// UpdateMetadata updates the object's metadata.
func (o *Object) UpdateMetadata(meta json.RawMessage) {
	o.inner.UpdateMetadata([]byte(meta))
}

// ObjectEvent represents a change to an object. If the object was deleted,
// Deleted is true and Object is nil.
type ObjectEvent struct {
	Key       types.Hash256
	Deleted   bool
	UpdatedAt time.Time
	Object    *Object
}
