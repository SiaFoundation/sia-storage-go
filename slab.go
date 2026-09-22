package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"

	"go.sia.tech/core/types"
)

// A Sector is one erasure coded piece of a slab, held by one host.
type Sector struct {
	Root    types.Hash256   `json:"root"`
	HostKey types.PublicKey `json:"hostKey"`
}

// A PinnedSlab is what the indexer holds for one slab of an object's data.
//
// EncryptionKey is the slab's data key. Anyone with it and the sector roots can
// recover the slab's contents, so treat a PinnedSlab as secret.
type PinnedSlab struct {
	Version       uint8         `json:"version"`
	ID            types.Hash256 `json:"id"`
	EncryptionKey []byte        `json:"encryptionKey"`
	MinShards     uint8         `json:"minShards"`
	Sectors       []Sector      `json:"sectors"`
}

// Slab retrieves one pinned slab from the indexer by its id.
//
// Get an id from [Object.SlabID]. A slab id is derived from the slab's contents
// rather than stored, so there is no other way to name one.
func (s *SDK) Slab(ctx context.Context, id types.Hash256) (PinnedSlab, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return PinnedSlab{}, errClosed
	}
	tok, release := cancelToken(ctx)
	defer release()

	var cJSON, cerr *C.char
	code := C.sia_sdk_slab(s.ptr, cBytes32((*[32]byte)(&id)), tok, &cJSON, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return PinnedSlab{}, goError(ctx, code, cerr)
	}
	// goString owns the free, so cJSON must not be released here.
	var slab PinnedSlab
	if err := json.Unmarshal([]byte(goString(cJSON)), &slab); err != nil {
		return PinnedSlab{}, fmt.Errorf("decoding slab: %w", err)
	}
	return slab, nil
}

// SlabCount reports how many slabs the object's data is spread across.
func (o *Object) SlabCount() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return 0
	}
	n := int(C.sia_object_slab_count(o.ptr))
	runtime.KeepAlive(o)
	return n
}

// SlabID returns the id of the object's ith slab, which is what [SDK.Slab]
// takes. It reports false when i is out of range.
func (o *Object) SlabID(i int) (id types.Hash256, ok bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return types.Hash256{}, false
	}
	ok = bool(C.sia_object_slab_id_at(o.ptr, C.size_t(i), cBytes32((*[32]byte)(&id))))
	runtime.KeepAlive(o)
	if !ok {
		return types.Hash256{}, false
	}
	return id, true
}

// SlabIDs returns the ids of every slab the object references.
func (o *Object) SlabIDs() []types.Hash256 {
	n := o.SlabCount()
	if n == 0 {
		return nil
	}
	ids := make([]types.Hash256, 0, n)
	for i := range n {
		if id, ok := o.SlabID(i); ok {
			ids = append(ids, id)
		}
	}
	return ids
}
