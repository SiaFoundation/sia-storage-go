package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
)

// An Object is a collection of slabs plus the keys needed to read them.
//
// It has no exported fields, because the data key it carries must not leak. Use
// the accessors, and Close when finished, since the bytes behind the handle are
// owned by the native side rather than by Go.
type Object struct {
	ptr     *C.sia_object_t
	cleanup runtime.Cleanup

	// mu guards ptr against Close. Every method that hands the handle to C
	// holds it for the read, so Close cannot free the handle underneath a
	// call already running on another goroutine.
	mu     sync.RWMutex
	closed bool
}

// NewObject returns an empty object, ready to be given metadata and uploaded.
func NewObject() *Object {
	return wrapObject(C.sia_object_new())
}

func wrapObject(ptr *C.sia_object_t) *Object {
	o := &Object{ptr: ptr}
	o.cleanup = runtime.AddCleanup(o, func(p *C.sia_object_t) {
		C.sia_object_free(p)
	}, ptr)
	return o
}

// Close releases the object. It is safe to call more than once, and waits for
// any call already using the handle to return.
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

// ID returns the object's identifier, which is a hash of its slabs. An empty
// object has a stable ID of its own, so two objects with the same contents
// share an ID.
func (o *Object) ID() (id types.Hash256) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return
	}
	C.sia_object_id(o.ptr, cBytes32((*[32]byte)(&id)))
	runtime.KeepAlive(o)
	return
}

// Size returns the length of the data the object holds.
func (o *Object) Size() uint64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return 0
	}
	n := uint64(C.sia_object_size(o.ptr))
	runtime.KeepAlive(o)
	return n
}

// EncodedSize returns how many bytes the object occupies on the network, which
// is larger than Size by the redundancy the slabs were encoded with.
func (o *Object) EncodedSize() uint64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return 0
	}
	n := uint64(C.sia_object_encoded_size(o.ptr))
	runtime.KeepAlive(o)
	return n
}

// CreatedAt returns when the indexer first recorded the object. It is the zero
// time for an object that has not been uploaded.
func (o *Object) CreatedAt() time.Time {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return time.Time{}
	}
	us := int64(C.sia_object_created_at(o.ptr))
	runtime.KeepAlive(o)
	return unixMicro(us)
}

// UpdatedAt returns when the object last changed, which the indexer also bumps
// when it repairs a slab onto a different host.
func (o *Object) UpdatedAt() time.Time {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return time.Time{}
	}
	us := int64(C.sia_object_updated_at(o.ptr))
	runtime.KeepAlive(o)
	return unixMicro(us)
}

// Metadata returns the object's metadata, which is encrypted at rest and
// decrypted here. It returns nil when there is none.
func (o *Object) Metadata() []byte {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return nil
	}
	n := C.sia_object_metadata(o.ptr, nil, 0)
	if n == 0 {
		runtime.KeepAlive(o)
		return nil
	}
	buf := make([]byte, int(n))
	C.sia_object_metadata(o.ptr, (*C.uint8_t)(unsafe.Pointer(&buf[0])), n)
	runtime.KeepAlive(o)
	return buf
}

// UpdateMetadata replaces the object's metadata. Passing nil clears it.
//
// This only changes the local handle. Call SDK.UpdateObjectMetadata to persist
// it to the indexer.
func (o *Object) UpdateMetadata(metadata []byte) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return
	}
	if len(metadata) == 0 {
		C.sia_object_set_metadata(o.ptr, nil, 0)
		runtime.KeepAlive(o)
		return
	}
	C.sia_object_set_metadata(o.ptr,
		(*C.uint8_t)(unsafe.Pointer(&metadata[0])), C.size_t(len(metadata)))
	runtime.KeepAlive(o)
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
// [Object.UpdateMetadata] only changes locally.
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
// actually frees the pinned storage a deleted object was using.
func (s *SDK) PruneSlabs(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_prune_slabs(s.ptr, tok, &cerr)
	runtime.KeepAlive(s)
	return goError(ctx, code, cerr)
}

// ObjectShareURL returns a URL granting read access to obj until validUntil,
// without the recipient needing an account. It is derived locally, so it
// reaches no indexer and cannot be revoked once handed out.
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
		return "", goError(nil, code, cerr)
	}
	return goString(cURL), nil
}

// ObjectFromShareURL resolves a URL from [SDK.ObjectShareURL] into an object
// the holder can download, paid for by the account that shared it.
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

// SealObject encodes obj as the sealed JSON the indexer API exchanges, with the
// data and metadata keys encrypted to the account's app key and signed by it.
//
// This is the one type a caller sees inside rather than holds as a handle,
// because a consumer persisting objects into its own schema needs the fields.
// Store the result unchanged and hand it back to [SDK.ObjectFromSealed].
//
// It is derived locally and reaches no indexer.
func (s *SDK) SealObject(obj *Object) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if s.closed || obj.closed {
		return nil, errClosed
	}

	var cJSON, cerr *C.char
	code := C.sia_object_seal_json(s.ptr, obj.ptr, &cJSON, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(obj)
	if code != C.SIA_OK {
		return nil, goError(nil, code, cerr)
	}
	return []byte(goString(cJSON)), nil
}

// ObjectFromSealed decodes and opens sealed JSON from [SDK.SealObject],
// verifying its signatures against the account's app key.
//
// A sealed object produced under a different app key fails here rather than
// producing a handle that cannot read anything.
func (s *SDK) ObjectFromSealed(sealed []byte) (*Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	cJSON := C.CString(string(sealed))
	defer C.free(unsafe.Pointer(cJSON))

	var ptr *C.sia_object_t
	var cerr *C.char
	code := C.sia_object_from_sealed_json(s.ptr, cJSON, &ptr, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(nil, code, cerr)
	}
	return wrapObject(ptr), nil
}
