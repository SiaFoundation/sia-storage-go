package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"runtime"
	"sync/atomic"
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
	closed  atomic.Bool
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

// Close releases the object. It is safe to call more than once.
func (o *Object) Close() error {
	if o.closed.Swap(true) {
		return nil
	}
	o.cleanup.Stop()
	C.sia_object_free(o.ptr)
	return nil
}

// ID returns the object's identifier, which is a hash of its slabs. An empty
// object has a stable ID of its own, so two objects with the same contents
// share an ID.
func (o *Object) ID() (id types.Hash256) {
	C.sia_object_id(o.ptr, cBytes32((*[32]byte)(&id)))
	runtime.KeepAlive(o)
	return
}

// Size returns the length of the data the object holds.
func (o *Object) Size() uint64 {
	n := uint64(C.sia_object_size(o.ptr))
	runtime.KeepAlive(o)
	return n
}

// EncodedSize returns how many bytes the object occupies on the network, which
// is larger than Size by the redundancy the slabs were encoded with.
func (o *Object) EncodedSize() uint64 {
	n := uint64(C.sia_object_encoded_size(o.ptr))
	runtime.KeepAlive(o)
	return n
}

// CreatedAt returns when the indexer first recorded the object. It is the zero
// time for an object that has not been uploaded.
func (o *Object) CreatedAt() time.Time {
	us := int64(C.sia_object_created_at(o.ptr))
	runtime.KeepAlive(o)
	return unixMicro(us)
}

// UpdatedAt returns when the object last changed, which the indexer also bumps
// when it repairs a slab onto a different host.
func (o *Object) UpdatedAt() time.Time {
	us := int64(C.sia_object_updated_at(o.ptr))
	runtime.KeepAlive(o)
	return unixMicro(us)
}

// Metadata returns the object's metadata, which is encrypted at rest and
// decrypted here. It returns nil when there is none.
func (o *Object) Metadata() []byte {
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
