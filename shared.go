package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"runtime"
	"sync"
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
// Unlike [Connect] there is no registration or approval step, because the seed
// is the entire credential and how it reached the recipient is the caller's
// business.
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
func (s *SharedSDK) Objects(ctx context.Context, offset, limit uint64) ([]*Object, error) {
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
		C.uint64_t(offset), C.uint64_t(limit), tok, &objs, &n, &cerr)
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
