//go:build siastorage_mock

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
)

// A MockNetwork is a set of in-process hosts backed by memory. Real erasure
// coding, encryption and transfer pipelines run against it, so it exercises
// everything except the network itself.
//
// It is only available under the siastorage_mock build tag, because the entry
// points behind it exist only in an archive built with the mock cargo feature.
// Build that archive with `make testlib`.
type MockNetwork struct {
	ptr     *C.sia_mock_t
	cleanup runtime.Cleanup

	mu     sync.RWMutex
	closed bool
}

// NewMockNetwork starts numHosts in-process hosts.
func NewMockNetwork(numHosts int) *MockNetwork {
	ptr := C.sia_mock_new(C.size_t(numHosts))
	m := &MockNetwork{ptr: ptr}
	m.cleanup = runtime.AddCleanup(m, func(p *C.sia_mock_t) {
		C.sia_mock_free(p)
	}, ptr)
	return m
}

// Close releases the hosts. It is safe to call more than once.
func (m *MockNetwork) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	m.cleanup.Stop()
	C.sia_mock_free(m.ptr)
	return nil
}

// SDK returns a connection to the mock network for the given 32 byte app key
// seed. The result is an ordinary SDK, so every other method drives the mock
// without knowing about it.
func (m *MockNetwork) SDK(ctx context.Context, appKeySeed [32]byte) (*SDK, error) {
	tok, release := cancelToken(ctx)
	defer release()

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, errClosed
	}

	var ptr *C.sia_sdk_t
	var cerr *C.char
	code := C.sia_mock_sdk(m.ptr, cBytes32(&appKeySeed), tok, &ptr, &cerr)
	runtime.KeepAlive(m)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapSDK(ptr), nil
}

// ClearSectors drops every sector the hosts hold, so a download of an object
// already uploaded fails the way it would if the hosts had lost the data.
func (m *MockNetwork) ClearSectors() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return
	}
	C.sia_mock_clear_sectors(m.ptr)
	runtime.KeepAlive(m)
}

// PinnedSlabs reports how many slabs the mock indexer has pinned.
func (m *MockNetwork) PinnedSlabs() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return 0
	}
	n := int(C.sia_mock_pinned_slabs(m.ptr))
	runtime.KeepAlive(m)
	return n
}
