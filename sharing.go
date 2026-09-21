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
	"sync/atomic"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
)

// A SharingKey grants read only access to the objects attached to it, with no
// account, app registration or approval needed by whoever holds it. Reads are
// paid for by the account that created the key.
//
// The 32 byte seed is the entire credential, so [SharingKey.Export] hands over
// access to every object on the key and cannot be taken back for a copy already
// made. [SDK.RevokeSharingKey] detaches everything at once, which is the only
// way to undo it.
type SharingKey struct {
	ptr     *C.sia_sharing_key_t
	cleanup runtime.Cleanup
	closed  atomic.Bool
}

// ImportSharingKey rebuilds a key from a seed handed out by its owner.
func ImportSharingKey(seed [32]byte) *SharingKey {
	return wrapSharingKey(C.sia_sharing_key_import(cBytes32(&seed)))
}

func wrapSharingKey(ptr *C.sia_sharing_key_t) *SharingKey {
	k := &SharingKey{ptr: ptr}
	k.cleanup = runtime.AddCleanup(k, func(p *C.sia_sharing_key_t) {
		C.sia_sharing_key_free(p)
	}, ptr)
	return k
}

// Close releases the key. It is safe to call more than once.
func (k *SharingKey) Close() error {
	if k.closed.Swap(true) {
		return nil
	}
	k.cleanup.Stop()
	C.sia_sharing_key_free(k.ptr)
	return nil
}

// Export returns the seed, which is the whole credential. Treat it as a secret
// and hand it out only over a channel you would send a password over.
func (k *SharingKey) Export() (seed [32]byte) {
	C.sia_sharing_key_export(k.ptr, cBytes32(&seed))
	runtime.KeepAlive(k)
	return
}

// PublicKey returns the half the indexer identifies the key by. Safe to log.
func (k *SharingKey) PublicKey() (pk types.PublicKey) {
	C.sia_sharing_key_public_key(k.ptr, cBytes32((*[32]byte)(&pk)))
	runtime.KeepAlive(k)
	return
}

// KeyStats is the indexer's snapshot of what a sharing key grants access to.
type KeyStats struct {
	ObjectCount uint64
	ObjectSize  uint64

	// PinnedData is the size before redundancy and PinnedSize what the objects
	// actually occupy on the network.
	PinnedData uint64
	PinnedSize uint64

	CreatedAt time.Time

	// ExpiresAt is the zero time when the key never expires.
	ExpiresAt time.Time
}

// A KeyRecord is the indexer's view of one sharing key.
type KeyRecord struct {
	// Key is set by [SDK.SharingKeys], which mints a handle for each record
	// that the caller must Close. It is nil from [SDK.SharingKey], where the
	// caller already holds the handle it asked about.
	Key *SharingKey

	Description string
	Stats       KeyStats
}

func goKeyStats(s C.sia_key_stats_t) KeyStats {
	stats := KeyStats{
		ObjectCount: uint64(s.object_count),
		ObjectSize:  uint64(s.object_size),
		PinnedData:  uint64(s.pinned_data),
		PinnedSize:  uint64(s.pinned_size),
		CreatedAt:   unixMicro(int64(s.created_at_unix_us)),
	}
	if bool(s.has_expiry) {
		stats.ExpiresAt = unixMicro(int64(s.expires_at_unix_us))
	}
	return stats
}

// CreateSharingKey registers a new key with the indexer. A zero expiresAt
// creates a key that never expires.
//
// The returned key is the only copy of its seed, so export it before closing
// it if the point was to hand it out.
func (s *SDK) CreateSharingKey(ctx context.Context, description string, expiresAt time.Time) (*SharingKey, error) {
	cDesc := C.CString(description)
	defer C.free(unsafe.Pointer(cDesc))

	tok, release := cancelToken(ctx)
	defer release()

	var ptr *C.sia_sharing_key_t
	var cerr *C.char
	code := C.sia_sdk_create_sharing_key(s.ptr, cDesc,
		C.bool(!expiresAt.IsZero()), C.int64_t(expiresAt.UnixMicro()), tok, &ptr, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapSharingKey(ptr), nil
}

// SharingKey fetches the indexer's record for key. The returned record's Key is
// nil, since the caller already holds the handle.
func (s *SDK) SharingKey(ctx context.Context, key *SharingKey) (KeyRecord, error) {
	tok, release := cancelToken(ctx)
	defer release()

	var cDesc, cerr *C.char
	var stats C.sia_key_stats_t
	code := C.sia_sdk_sharing_key(s.ptr, key.ptr, tok, &cDesc, &stats, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(key)
	if code != C.SIA_OK {
		return KeyRecord{}, goError(ctx, code, cerr)
	}
	return KeyRecord{Description: goString(cDesc), Stats: goKeyStats(stats)}, nil
}

// SharingKeys lists the account's sharing keys. A zero offset and limit take
// the indexer's defaults.
//
// Every returned record carries a handle the caller owns and must Close.
func (s *SDK) SharingKeys(ctx context.Context, offset, limit uint64) ([]KeyRecord, error) {
	tok, release := cancelToken(ctx)
	defer release()

	var recs *C.sia_key_records_t
	var cerr *C.char
	code := C.sia_sdk_sharing_keys(s.ptr, C.uint64_t(offset), C.uint64_t(limit), tok, &recs, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	defer C.sia_key_records_free(recs)

	out := make([]KeyRecord, 0, int(C.sia_key_records_len(recs)))
	for i := C.size_t(0); ; i++ {
		var ptr *C.sia_sharing_key_t
		var cDesc *C.char
		var stats C.sia_key_stats_t
		if !bool(C.sia_key_records_at(recs, i, &ptr, &cDesc, &stats)) {
			break
		}
		out = append(out, KeyRecord{
			Key:         wrapSharingKey(ptr),
			Description: goString(cDesc),
			Stats:       goKeyStats(stats),
		})
	}
	return out, nil
}

// ShareObject attaches obj to key, so anyone holding the key's seed can read
// it. The object must already be pinned.
func (s *SDK) ShareObject(ctx context.Context, key *SharingKey, obj *Object) error {
	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_share_object(s.ptr, key.ptr, obj.ptr, tok, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(key)
	runtime.KeepAlive(obj)
	return goError(ctx, code, cerr)
}

// SharedObjects lists the objects attached to key. A zero offset and limit take
// the indexer's defaults.
//
// Every returned object is a handle the caller owns and must Close.
func (s *SDK) SharedObjects(ctx context.Context, key *SharingKey, offset, limit uint64) ([]*Object, error) {
	tok, release := cancelToken(ctx)
	defer release()

	var objs **C.sia_object_t
	var n C.size_t
	var cerr *C.char
	code := C.sia_sdk_shared_objects(s.ptr, key.ptr,
		C.uint64_t(offset), C.uint64_t(limit), tok, &objs, &n, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(key)
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

// UnshareObject detaches one object from key, returning
// [ErrObjectNotAttached] when it was not attached in the first place.
func (s *SDK) UnshareObject(ctx context.Context, key *SharingKey, id types.Hash256) error {
	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_unshare_object(s.ptr, key.ptr, cBytes32((*[32]byte)(&id)), tok, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(key)
	return goError(ctx, code, cerr)
}

// RevokeSharingKey detaches every object from key at once, which is the only
// way to withdraw a seed already handed out.
//
// Downloads already in flight can keep reading from hosts for up to five more
// minutes, because the hosts were paid for those reads before the revocation.
func (s *SDK) RevokeSharingKey(ctx context.Context, key *SharingKey) error {
	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_revoke_sharing_key(s.ptr, key.ptr, tok, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(key)
	return goError(ctx, code, cerr)
}
