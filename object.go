package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// A SealedObject is an object locked with the account's app key. It is the one
// thing a caller sees inside rather than holds as a handle, because a consumer
// persisting objects into its own schema needs the fields.
type SealedObject = slabs.SealedObject

// An Object is a collection of slabs plus the keys needed to read them.
//
// It has no exported fields, because the data key it carries must not leak.
// Use the accessors.
type Object struct {
	// encoded is the whole object as the native side wrote it. The calls that
	// need one rebuild it, use it and release it before returning, so an
	// Object is an ordinary Go value: copy it, store it, share it between
	// goroutines, and let the collector have it when you are done.
	encoded []byte

	// Read once, when the object was built. Nothing can change an object, so
	// these never go stale: WithMetadata and Truncate return a new one rather
	// than altering this.
	id          types.Hash256
	size        uint64
	encodedSize uint64
	createdAt   time.Time
	updatedAt   time.Time
	metadata    []byte
}

// NewEmptyObject returns an empty object, ready to be given metadata and
// uploaded.
func NewEmptyObject() *Object {
	return wrapObject(C.sia_object_new())
}

// wrapObject reads everything out of a native object and releases it. The
// result owns no native memory.
func wrapObject(ptr *C.sia_object_t) *Object {
	if ptr == nil {
		return nil
	}
	defer C.sia_object_free(ptr)
	return readObject(ptr)
}

// readObject reads everything out of a native object, leaving it to the
// caller to release.
func readObject(ptr *C.sia_object_t) *Object {
	n := C.sia_object_encode(ptr, nil, 0)
	if n == 0 {
		return nil
	}
	buf := make([]byte, int(n))
	if C.sia_object_encode(ptr, (*C.uint8_t)(unsafe.Pointer(&buf[0])), n) != n {
		return nil
	}

	o := &Object{
		encoded:     buf,
		size:        uint64(C.sia_object_size(ptr)),
		encodedSize: uint64(C.sia_object_encoded_size(ptr)),
		createdAt:   unixMicro(int64(C.sia_object_created_at(ptr))),
		updatedAt:   unixMicro(int64(C.sia_object_updated_at(ptr))),
		metadata:    nativeMetadata(ptr),
	}
	C.sia_object_id(ptr, cBytes32((*[32]byte)(&o.id)))
	return o
}

// nativeMetadata copies an object's metadata out of a native handle.
func nativeMetadata(ptr *C.sia_object_t) []byte {
	n := C.sia_object_metadata(ptr, nil, 0)
	if n == 0 {
		return nil
	}
	buf := make([]byte, int(n))
	// Nothing is copied when the buffer is too small, which would otherwise
	// hand back a silently zero filled slice.
	if C.sia_object_metadata(ptr, (*C.uint8_t)(unsafe.Pointer(&buf[0])), n) != n {
		return nil
	}
	return buf
}

// native rebuilds the object on the native side for the duration of one call.
// The caller must release the result with C.sia_object_free.
func (o *Object) native() (*C.sia_object_t, error) {
	if o == nil || len(o.encoded) == 0 {
		return nil, errors.New("object is not usable")
	}
	var ptr *C.sia_object_t
	var cerr *C.char
	code := C.sia_object_decode((*C.uint8_t)(unsafe.Pointer(&o.encoded[0])),
		C.size_t(len(o.encoded)), &ptr, &cerr)
	if code != C.SIA_OK {
		return nil, goError(context.Background(), code, cerr)
	}
	return ptr, nil
}

// ID returns the object's identifier, which is a hash of its slabs. An empty
// object has a stable ID of its own, so two objects with the same contents
// share an ID.
func (o *Object) ID() types.Hash256 { return o.id }

// Size returns the length of the data the object holds.
func (o *Object) Size() uint64 { return o.size }

// EncodedSize returns the storage the object occupies once erasure coded,
// which is what the account is billed for.
func (o *Object) EncodedSize() uint64 { return o.encodedSize }

// CreatedAt returns when the indexer first saw the object, or the zero time
// for one that has never been pinned.
func (o *Object) CreatedAt() time.Time { return o.createdAt }

// UpdatedAt returns when the indexer last saw the object change, or the zero
// time for one that has never been pinned.
func (o *Object) UpdatedAt() time.Time { return o.updatedAt }

// Metadata returns the object's metadata, which is encrypted at rest and
// decrypted here. It returns nil when there is none.
//
// The result is a copy, so writing through it cannot change the object.
func (o *Object) Metadata() []byte {
	if len(o.metadata) == 0 {
		return nil
	}
	return bytes.Clone(o.metadata)
}

// WithMetadata returns a copy of the object carrying metadata in place of
// whatever it held. Passing nil clears it. The receiver is untouched, so an
// object never changes under a reader and is safe to share between goroutines.
//
// This only produces a local object. Whichever call next sends it to the
// indexer stores the metadata: [SDK.PinObject] for an object that is not
// pinned yet, or [SDK.UpdateObjectMetadata] for one that is.
func (o *Object) WithMetadata(metadata []byte) *Object {
	ptr, err := o.native()
	if err != nil {
		return nil
	}
	defer C.sia_object_free(ptr)

	if len(metadata) == 0 {
		C.sia_object_set_metadata(ptr, nil, 0)
	} else {
		C.sia_object_set_metadata(ptr,
			(*C.uint8_t)(unsafe.Pointer(&metadata[0])), C.size_t(len(metadata)))
	}
	// set_metadata mutated the handle we already hold, so read it back rather
	// than building another.
	return readObject(ptr)
}

// Truncate returns a copy of the object shortened to length bytes.
//
// The last retained slab is shortened and any slab past it is dropped. A length
// at or above the current size copies it unchanged. The receiver is untouched.
//
// This only rewrites the slab list. Pin the result with [SDK.PinObject] before
// the indexer knows about it.
func (o *Object) Truncate(length uint64) *Object {
	ptr, err := o.native()
	if err != nil {
		return nil
	}
	defer C.sia_object_free(ptr)
	return wrapObject(C.sia_object_truncate(ptr, C.uint64_t(length)))
}

// unixMicro converts the microsecond timestamps the C ABI uses, mapping zero to
// the zero time rather than to the epoch.
func unixMicro(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}

// Object fetches the object with the given ID from the indexer, decrypting the
// keys and metadata it carries.
func (s *SDK) Object(ctx context.Context, id types.Hash256) (*Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	var ptr *C.sia_object_t
	var cerr *C.char
	code := C.sia_sdk_object(s.ptr, cBytes32((*[32]byte)(&id)), tok, &ptr, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapObject(ptr), nil
}

// PinObject registers obj with the indexer, pinning any of its slabs that are
// not already pinned.
//
// An upload pins the slabs it writes but not the object itself, so until this
// is called the object exists only as a local handle and no lookup by ID, share
// URL or delete can find it.
func (s *SDK) PinObject(ctx context.Context, obj *Object) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errClosed
	}
	objPtr, err := obj.native()
	if err != nil {
		return err
	}
	defer C.sia_object_free(objPtr)

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_pin_object(s.ptr, objPtr, tok, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(obj)
	return goError(ctx, code, cerr)
}

// UpdateObjectMetadata persists the metadata currently on obj, which
// [Object.WithMetadata] only produces locally.
//
// It pins the object as a side effect, so it also serves to persist an object
// whose metadata is the only thing that changed.
//
// The indexer caps the encrypted form at 1 KiB, and encryption adds a 24 byte
// nonce and a 16 byte tag, so the usable budget is 984 bytes. Exceeding it
// fails here rather than at the point the metadata was set.
func (s *SDK) UpdateObjectMetadata(ctx context.Context, obj *Object) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errClosed
	}
	objPtr, err := obj.native()
	if err != nil {
		return err
	}
	defer C.sia_object_free(objPtr)

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_update_object_metadata(s.ptr, objPtr, tok, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(obj)
	return goError(ctx, code, cerr)
}

// DeleteObject removes the object from the indexer. The slabs it held survive
// until they are pruned, so an object sharing slabs with another is unaffected.
func (s *SDK) DeleteObject(ctx context.Context, id types.Hash256) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_delete_object(s.ptr, cBytes32((*[32]byte)(&id)), tok, &cerr)
	runtime.KeepAlive(s)
	return goError(ctx, code, cerr)
}

// PruneSlabs releases the slabs no remaining object references, which is what
// actually frees the pinned storage a deleted object was using. Only slabs
// pinned before the given time are released.
//
// A zero before leaves the cutoff to the indexer, which holds back anything
// pinned recently so an upload still in flight is not swept up. A caller that
// knows nothing else is uploading can pass the present to release everything.
func (s *SDK) PruneSlabs(ctx context.Context, before time.Time) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_prune_slabs(s.ptr,
		C.bool(!before.IsZero()), C.int64_t(before.UnixMicro()), tok, &cerr)
	runtime.KeepAlive(s)
	return goError(ctx, code, cerr)
}

// ObjectShareURL returns a URL granting read access to obj until validUntil.
// It is derived locally, so it reaches no indexer and cannot be revoked once
// handed out.
//
// The recipient resolves it with [SDK.ObjectFromShareURL], so they need an
// account of their own.
//
// validUntil must be a real time; there is no sentinel for an unexpiring URL.
func (s *SDK) ObjectShareURL(obj *Object, validUntil time.Time) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return "", errClosed
	}
	objPtr, err := obj.native()
	if err != nil {
		return "", err
	}
	defer C.sia_object_free(objPtr)

	if validUntil.IsZero() {
		return "", errors.New("share URL requires an expiration time")
	}
	var cURL, cerr *C.char
	code := C.sia_sdk_object_share_url(s.ptr, objPtr,
		C.int64_t(validUntil.UnixMicro()), &cURL, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(obj)
	if code != C.SIA_OK {
		return "", localError(code, cerr)
	}
	return goString(cURL), nil
}

// ObjectFromShareURL resolves a URL from [SDK.ObjectShareURL] into an object
// this SDK can download. The reads are paid for by this account.
func (s *SDK) ObjectFromShareURL(ctx context.Context, shareURL string) (*Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	cURL := C.CString(shareURL)
	defer C.free(unsafe.Pointer(cURL))

	tok, release := cancelToken(ctx)
	defer release()

	var ptr *C.sia_object_t
	var cerr *C.char
	code := C.sia_sdk_object_from_share_url(s.ptr, cURL, tok, &ptr, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapObject(ptr), nil
}

// SealObject seals obj with the data and metadata keys encrypted to the
// account's app key and signed by it. Store the result and hand it back to
// [SDK.ObjectFromSealed].
//
// It is derived locally and reaches no indexer.
func (s *SDK) SealObject(obj *Object) (SealedObject, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return SealedObject{}, errClosed
	}
	objPtr, err := obj.native()
	if err != nil {
		return SealedObject{}, err
	}
	defer C.sia_object_free(objPtr)

	var cJSON, cerr *C.char
	code := C.sia_object_seal_json(s.ptr, objPtr, &cJSON, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(obj)
	if code != C.SIA_OK {
		return SealedObject{}, localError(code, cerr)
	}

	// The native side speaks the API's JSON, which is what SealedObject is
	// declared to decode; a field either side renamed lands here as a zero.
	var sealed SealedObject
	if err := json.Unmarshal([]byte(goString(cJSON)), &sealed); err != nil {
		return SealedObject{}, fmt.Errorf("failed to decode sealed object: %w", err)
	}
	return sealed, nil
}

// ObjectFromSealed opens a sealed object from [SDK.SealObject], verifying its
// signatures against the account's app key.
//
// A sealed object produced under a different app key fails here rather than
// producing a handle that cannot read anything.
func (s *SDK) ObjectFromSealed(sealed SealedObject) (*Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	buf, err := json.Marshal(sealed)
	if err != nil {
		return nil, fmt.Errorf("failed to encode sealed object: %w", err)
	}

	cJSON := C.CString(string(buf))
	defer C.free(unsafe.Pointer(cJSON))

	var ptr *C.sia_object_t
	var cerr *C.char
	code := C.sia_object_from_sealed_json(s.ptr, cJSON, &ptr, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, localError(code, cerr)
	}
	return wrapObject(ptr), nil
}
