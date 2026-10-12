package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
)

// A SharedSDK is the recipient half of a sharing key.
//
// Where [SDK] authenticates with an app key and an account, this authenticates
// with the sharing key itself, so holding the 32 byte seed is the whole of what
// it takes to use one: no account, no app registration, no approval step. It is
// read only, and can list, fetch and download the objects the key grants access
// to but not upload, pin or delete.
//
// Reads are paid for with account tokens drawn from the key owner's account,
// refreshed in the background before they expire.
type SharedSDK struct {
	ptr     *C.sia_shared_sdk_t
	cleanup runtime.Cleanup

	// mu guards ptr against Close, as on SDK.
	mu     sync.RWMutex
	closed bool
}

func wrapSharedSDK(ptr *C.sia_shared_sdk_t) *SharedSDK {
	s := &SharedSDK{ptr: ptr}
	s.cleanup = runtime.AddCleanup(s, func(p *C.sia_shared_sdk_t) {
		C.sia_shared_sdk_free(p)
	}, ptr)
	return s
}

// ConnectShared connects to indexerURL as the recipient of the sharing key
// derived from seed.
//
// Unlike [Builder.Connect] there is no registration or approval step, because
// the seed is the entire credential and how it reached the recipient is the
// caller's business.
func ConnectShared(ctx context.Context, indexerURL string, seed [32]byte) (*SharedSDK, error) {
	tok, release := cancelToken(ctx)
	defer release()

	cURL := C.CString(indexerURL)
	defer C.free(unsafe.Pointer(cURL))

	var ptr *C.sia_shared_sdk_t
	var cerr *C.char
	code := C.sia_shared_sdk_connect(cURL, cBytes32(&seed), tok, &ptr, &cerr)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapSharedSDK(ptr), nil
}

// Close releases the handle. It is safe to call more than once.
//
// A download started from this SDK keeps its own token refresh alive, so
// closing while one is still reading does not break it.
func (s *SharedSDK) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.cleanup.Stop()
	C.sia_shared_sdk_free(s.ptr)
	return nil
}

// Stats reports what the indexer currently holds for this sharing key.
func (s *SharedSDK) Stats(ctx context.Context) (KeyStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return KeyStats{}, errClosed
	}
	tok, release := cancelToken(ctx)
	defer release()

	var stats C.sia_key_stats_t
	var cerr *C.char
	code := C.sia_shared_sdk_stats(s.ptr, tok, &stats, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return KeyStats{}, goError(ctx, code, cerr)
	}
	return goKeyStats(stats), nil
}

// Object fetches and decrypts one object the key grants access to.
//
// The result is a handle the caller owns and must Close.
func (s *SharedSDK) Object(ctx context.Context, id types.Hash256) (*Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}
	tok, release := cancelToken(ctx)
	defer release()

	var ptr *C.sia_object_t
	var cerr *C.char
	code := C.sia_shared_sdk_object(s.ptr, cBytes32((*[32]byte)(&id)), tok, &ptr, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapObject(ptr), nil
}

// Objects lists and decrypts a page of the objects the key grants access to. A
// zero offset and limit take the indexer's defaults.
//
// Every returned object is a handle the caller owns and must Close.
func (s *SharedSDK) Objects(ctx context.Context, offset, limit int) ([]*Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}
	tok, release := cancelToken(ctx)
	defer release()

	var objs **C.sia_object_t
	var n C.size_t
	var cerr *C.char
	code := C.sia_shared_sdk_objects(s.ptr,
		pageArg(offset), pageArg(limit), tok, &objs, &n, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	if objs == nil || n == 0 {
		return nil, nil
	}
	// The array is ours to free; the objects in it are handed to the caller.
	defer C.sia_object_array_free(objs, n)

	out := make([]*Object, 0, int(n))
	for _, ptr := range unsafe.Slice(objs, int(n)) {
		out = append(out, wrapObject(ptr))
	}
	return out, nil
}

// An ObjectSummary is an object without its slab layout: enough to list it,
// not enough to download it. [SharedSDK.Object] fetches the whole object by ID
// for that.
type ObjectSummary struct {
	ID   types.Hash256
	Size uint64

	// Metadata is the object's metadata, decrypted here, or nil when there is
	// none. Each summary owns its copy.
	Metadata []byte

	CreatedAt time.Time
	UpdatedAt time.Time
}

// ObjectSummaries lists the same page [SharedSDK.Objects] would, without the
// slab layouts that make up most of an object's size. A zero offset and limit
// take the indexer's defaults.
//
// This is what a listing wants: the slabs are only needed to download, and a
// page showing names and sizes reads none of them.
func (s *SharedSDK) ObjectSummaries(ctx context.Context, offset, limit int) ([]ObjectSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}
	tok, release := cancelToken(ctx)
	defer release()

	var sums *C.sia_object_summaries_t
	var cerr *C.char
	code := C.sia_shared_sdk_object_summaries(s.ptr,
		pageArg(offset), pageArg(limit), tok, &sums, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	if sums == nil {
		return nil, nil
	}
	defer C.sia_object_summaries_free(sums)

	n := int(C.sia_object_summaries_len(sums))
	out := make([]ObjectSummary, 0, n)
	for i := range n {
		var c C.sia_object_summary_t
		if !bool(C.sia_object_summaries_at(sums, C.size_t(i), &c)) {
			return nil, fmt.Errorf("summary %d of %d is out of range", i, n)
		}
		out = append(out, goObjectSummary(c, summaryMetadata(sums, i)))
	}
	return out, nil
}

func goObjectSummary(c C.sia_object_summary_t, metadata []byte) ObjectSummary {
	sum := ObjectSummary{
		Size:      uint64(c.size),
		Metadata:  metadata,
		CreatedAt: unixMicro(int64(c.created_at_unix_us)),
		UpdatedAt: unixMicro(int64(c.updated_at_unix_us)),
	}
	for i, b := range c.id {
		sum.ID[i] = byte(b)
	}
	return sum
}

// summaryMetadata copies the metadata of the summary at i. It is variable
// length, so it does not travel in the flat struct.
func summaryMetadata(sums *C.sia_object_summaries_t, i int) []byte {
	n := C.sia_object_summaries_metadata(sums, C.size_t(i), nil, 0)
	if n == 0 {
		return nil
	}
	buf := make([]byte, int(n))
	// Nothing is copied when the buffer is too small, which would otherwise
	// hand back a silently zero filled slice.
	if C.sia_object_summaries_metadata(sums, C.size_t(i),
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), n) != n {
		return nil
	}
	return buf
}
