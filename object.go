package siastorage

/*
#include "sia_storage.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
)

// errNilObject is returned when a zero-value Object is used. Objects must be
// created with NewEmptyObject or returned by the SDK.
var errNilObject = errors.New("object is not initialized")

// objectHandle owns a Rust object and frees it once unreachable.
type objectHandle struct {
	ptr *C.sia_object_t
}

// An Object represents a collection of slabs that can be used to access
// encrypted data. The master key is used to encrypt/decrypt the data and
// metadata, and should be kept secret.
//
// It has no public fields to prevent accidental leakage of unencrypted data.
type Object struct {
	h *objectHandle
}

// ObjectEvent represents a change to an object. If the object was deleted,
// Deleted is true and Object is nil.
type ObjectEvent struct {
	Key       types.Hash256
	Deleted   bool
	UpdatedAt time.Time
	Object    *Object
}

// A Cursor is used to paginate through object events. During pagination,
// 'After' should be set to the 'UpdatedAt' value of the last event received
// and 'Key' to the 'Key' of the last event received.
type Cursor struct {
	After time.Time
	Key   types.Hash256
}

func newObjectHandle(ptr *C.sia_object_t) *objectHandle {
	h := &objectHandle{ptr: ptr}
	runtime.AddCleanup(h, func(p *C.sia_object_t) {
		C.sia_object_free(p)
	}, ptr)
	return h
}

func newObject(ptr *C.sia_object_t) Object {
	return Object{h: newObjectHandle(ptr)}
}

// ID returns the object's ID, which is a hash of its slabs.
func (o *Object) ID() (id types.Hash256) {
	if o.h == nil {
		return types.Hash256{}
	}
	C.sia_object_id(o.h.ptr, cBytes32((*[32]byte)(&id)))
	runtime.KeepAlive(o.h)
	return id
}

// Size returns the total size of the object in bytes.
func (o *Object) Size() uint64 {
	if o.h == nil {
		return 0
	}
	n := uint64(C.sia_object_size(o.h.ptr))
	runtime.KeepAlive(o.h)
	return n
}

// EncodedSize returns the total size of the object on the network after
// erasure coding.
func (o *Object) EncodedSize() uint64 {
	if o.h == nil {
		return 0
	}
	n := uint64(C.sia_object_encoded_size(o.h.ptr))
	runtime.KeepAlive(o.h)
	return n
}

// CreatedAt returns the time the object was created.
func (o *Object) CreatedAt() time.Time {
	if o.h == nil {
		return time.Time{}
	}
	t := time.UnixMicro(int64(C.sia_object_created_at(o.h.ptr))).UTC()
	runtime.KeepAlive(o.h)
	return t
}

// UpdatedAt returns the time the object was last updated.
func (o *Object) UpdatedAt() time.Time {
	if o.h == nil {
		return time.Time{}
	}
	t := time.UnixMicro(int64(C.sia_object_updated_at(o.h.ptr))).UTC()
	runtime.KeepAlive(o.h)
	return t
}

// Metadata returns a copy of the object's metadata.
func (o *Object) Metadata() json.RawMessage {
	if o.h == nil {
		return nil
	}
	defer runtime.KeepAlive(o.h)
	n := C.sia_object_metadata(o.h.ptr, nil, 0)
	if n == 0 {
		return nil
	}
	buf := make([]byte, n)
	C.sia_object_metadata(o.h.ptr, (*C.uint8_t)(unsafe.Pointer(&buf[0])), n)
	return buf
}

// UpdateMetadata updates the object's metadata.
func (o *Object) UpdateMetadata(meta json.RawMessage) {
	if o.h == nil {
		return
	}
	var ptr *C.uint8_t
	if len(meta) > 0 {
		ptr = (*C.uint8_t)(unsafe.Pointer(&meta[0]))
	}
	C.sia_object_set_metadata(o.h.ptr, ptr, C.size_t(len(meta)))
	runtime.KeepAlive(o.h)
}

// NewEmptyObject creates a new Object to use in [SDK.Upload].
func NewEmptyObject() Object {
	return newObject(C.sia_object_new())
}

// ObjectEvents returns object events from the indexer, starting from the
// given cursor, up to the given limit. It preserves deletion events.
func (s *SDK) ObjectEvents(ctx context.Context, cursor Cursor, limit int) ([]ObjectEvent, error) {
	ptr, unlock, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer unlock()

	tok, release := cancelToken(ctx)
	defer release()

	hasCursor := cursor != (Cursor{})
	var afterUS C.int64_t
	var afterID [32]byte
	if hasCursor {
		afterUS = C.int64_t(cursor.After.UnixMicro())
		afterID = cursor.Key
	}
	var limitC C.uint64_t
	if limit > 0 {
		limitC = C.uint64_t(limit)
	}

	var evs *C.sia_events_t
	var cerr *C.char
	code := C.sia_sdk_object_events(ptr, C.bool(hasCursor), afterUS, cBytes32(&afterID), limitC, tok, &evs, &cerr)
	if err := goError(ctx, code, cerr); err != nil {
		return nil, fmt.Errorf("failed to list object events: %w", err)
	}
	defer C.sia_events_free(evs)

	events := make([]ObjectEvent, C.sia_events_len(evs))
	for i := range events {
		var id [32]byte
		var deleted C.bool
		var updatedUS C.int64_t
		var obj *C.sia_object_t
		C.sia_events_at(evs, C.size_t(i), cBytes32(&id), &deleted, &updatedUS, &obj)
		events[i] = ObjectEvent{
			Key:       types.Hash256(id),
			Deleted:   bool(deleted),
			UpdatedAt: time.UnixMicro(int64(updatedUS)).UTC(),
		}
		if obj != nil {
			o := newObject(obj)
			events[i].Object = &o
		}
	}
	return events, nil
}

// Object retrieves the object with the given key.
func (s *SDK) Object(ctx context.Context, objectKey types.Hash256) (Object, error) {
	ptr, unlock, err := s.acquire()
	if err != nil {
		return Object{}, err
	}
	defer unlock()

	tok, release := cancelToken(ctx)
	defer release()

	key := [32]byte(objectKey)
	var obj *C.sia_object_t
	var cerr *C.char
	code := C.sia_sdk_object(ptr, cBytes32(&key), tok, &obj, &cerr)
	if err := goError(ctx, code, cerr); err != nil {
		return Object{}, fmt.Errorf("failed to get object: %w", err)
	}
	return newObject(obj), nil
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
		return "", err
	}

	ptr, unlock, err := s.acquire()
	if err != nil {
		return "", err
	}
	defer unlock()

	var urlC *C.char
	var cerr *C.char
	code := C.sia_sdk_share_object(ptr, obj.h.ptr, C.int64_t(validUntil.UnixMicro()), &urlC, &cerr)
	runtime.KeepAlive(obj.h)
	if err := goError(ctx, code, cerr); err != nil {
		return "", fmt.Errorf("failed to share object: %w", err)
	}
	return goString(urlC), nil
}
