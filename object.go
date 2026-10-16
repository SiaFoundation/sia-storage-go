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
	"sync"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// A SealedObject is an object locked with the account's app key. Its fields are
// exposed, unlike those of [Object], because a consumer persisting objects into
// its own schema needs them.
type SealedObject = slabs.SealedObject

// An Object is a collection of slabs plus the keys needed to read them.
//
// It has no exported fields, because the data key it carries must not leak.
// Use the accessors, and Close when finished, since the handle is owned by the
// native side rather than by Go.
type Object struct {
	ptr     *C.sia_object_t
	cleanup runtime.Cleanup

	// mu guards ptr against Close. Every method that hands the handle to C
	// holds it for the read, so Close cannot free the handle underneath a
	// call already running on another goroutine.
	mu     sync.RWMutex
	closed bool

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

// wrapObject takes ownership of a native object and reads out everything the
// accessors answer from Go.
func wrapObject(ptr *C.sia_object_t) *Object {
	if ptr == nil {
		return nil
	}
	o := &Object{
		ptr:         ptr,
		size:        uint64(C.sia_object_size(ptr)),
		encodedSize: uint64(C.sia_object_encoded_size(ptr)),
		createdAt:   unixMicro(int64(C.sia_object_created_at(ptr))),
		updatedAt:   unixMicro(int64(C.sia_object_updated_at(ptr))),
		metadata:    nativeMetadata(ptr),
	}
	C.sia_object_id(ptr, cBytes32((*[32]byte)(&o.id)))
	o.cleanup = runtime.AddCleanup(o, func(p *C.sia_object_t) {
		C.sia_object_free(p)
	}, ptr)
	return o
}

// Close releases the object. It is safe to call more than once, and waits for
// any call already using the handle to return.
//
// The accessors keep answering afterwards, since they report values copied
// when the object was built. What stops working is whatever needs the native
// side: WithMetadata and Truncate return nil, and an SDK call fails.
func (o *Object) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	o.cleanup.Stop()
	C.sia_object_free(o.ptr)
	return nil
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
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return nil
	}
	var ptr *C.sia_object_t
	if len(metadata) == 0 {
		ptr = C.sia_object_with_metadata(o.ptr, nil, 0)
	} else {
		ptr = C.sia_object_with_metadata(o.ptr,
			(*C.uint8_t)(unsafe.Pointer(&metadata[0])), C.size_t(len(metadata)))
	}
	runtime.KeepAlive(o)
	return wrapObject(ptr)
}

// Truncate returns a copy of the object shortened to length bytes.
//
// The last retained slab is shortened and any slab past it is dropped. A length
// at or above the current size copies it unchanged. The receiver is untouched.
//
// This only rewrites the slab list. Pin the result with [SDK.PinObject] before
// the indexer knows about it.
func (o *Object) Truncate(length uint64) *Object {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return nil
	}
	ptr := C.sia_object_truncate(o.ptr, C.uint64_t(length))
	runtime.KeepAlive(o)
	return wrapObject(ptr)
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
// is called the object exists only locally and no lookup by ID, share URL or
// delete can find it.
func (s *SDK) PinObject(ctx context.Context, obj *Object) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if s.closed || obj.closed {
		return errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_pin_object(s.ptr, obj.ptr, tok, &cerr)
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
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if s.closed || obj.closed {
		return errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_update_object_metadata(s.ptr, obj.ptr, tok, &cerr)
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
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if s.closed || obj.closed {
		return "", errClosed
	}

	if validUntil.IsZero() {
		return "", errors.New("share URL requires an expiration time")
	}
	var cURL, cerr *C.char
	code := C.sia_sdk_object_share_url(s.ptr, obj.ptr,
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
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if s.closed || obj.closed {
		return SealedObject{}, errClosed
	}

	var cJSON, cerr *C.char
	code := C.sia_object_seal_json(s.ptr, obj.ptr, &cJSON, &cerr)
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
// producing an object that cannot read anything.
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
