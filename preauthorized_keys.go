package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
)

// A PreAuthorizedKey lets an app connect with [Builder.ConnectPreAuthorized],
// skipping the interactive approval step, once an indexer has registered it
// from a request signed by [PreAuthorizedKey.SignRequest].
//
// The 32 byte seed is the entire credential. Anyone holding it can connect
// apps as the user for as many uses as the indexer granted.
type PreAuthorizedKey struct {
	ptr     *C.sia_pre_authorized_key_t
	cleanup runtime.Cleanup

	// mu orders the reads of ptr against the construction that filled it, as
	// on every other handle in this package.
	mu sync.RWMutex
}

// A PreAuthorizedKeyRequest is the policy an indexer attaches to a
// pre-authorized key.
type PreAuthorizedKeyRequest struct {
	// ConnectKey is the connect key that apps using this key connect under.
	ConnectKey string
	// Expiration is sent with microsecond precision.
	Expiration time.Time
	// TotalUses caps how many connections the key authorizes.
	TotalUses uint32
	// AllowedAppID restricts the key to one app. Nil allows any app.
	AllowedAppID *types.Hash256
}

// NewPreAuthorizedKey generates a random pre-authorized key.
func NewPreAuthorizedKey() *PreAuthorizedKey {
	return wrapPreAuthorizedKey(C.sia_pre_authorized_key_new())
}

// ImportPreAuthorizedKey rebuilds a key from a seed returned by
// [PreAuthorizedKey.Export].
func ImportPreAuthorizedKey(seed [32]byte) *PreAuthorizedKey {
	return wrapPreAuthorizedKey(C.sia_pre_authorized_key_import(cBytes32(&seed)))
}

func wrapPreAuthorizedKey(ptr *C.sia_pre_authorized_key_t) *PreAuthorizedKey {
	k := &PreAuthorizedKey{ptr: ptr}
	k.cleanup = runtime.AddCleanup(k, func(p *C.sia_pre_authorized_key_t) {
		C.sia_pre_authorized_key_free(p)
	}, ptr)
	return k
}

// Export returns the seed, which is the whole credential. Store it no longer
// than it takes to hand to the app that will connect with it.
func (k *PreAuthorizedKey) Export() (seed [32]byte) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	C.sia_pre_authorized_key_export(k.ptr, cBytes32(&seed))
	runtime.KeepAlive(k)
	return
}

// PublicKey returns the half the indexer identifies the key by. Safe to log.
func (k *PreAuthorizedKey) PublicKey() (pk types.PublicKey) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	C.sia_pre_authorized_key_public_key(k.ptr, cBytes32((*[32]byte)(&pk)))
	runtime.KeepAlive(k)
	return
}

// Sign signs message with the key.
func (k *PreAuthorizedKey) Sign(message []byte) (sig types.Signature) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	// The status is ignored because it only reports a nil handle or a nil
	// message with a nonzero length, and neither can be passed from here.
	C.sia_pre_authorized_key_sign(k.ptr, cMessage(message), C.uintptr_t(len(message)),
		(*C.uint8_t)(unsafe.Pointer(&sig[0])), nil)
	runtime.KeepAlive(k)
	return
}

// VerifySignature reports whether sig is the key's signature of message.
func (k *PreAuthorizedKey) VerifySignature(message []byte, sig types.Signature) bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	ok := C.sia_pre_authorized_key_verify_signature(k.ptr, cMessage(message),
		C.uintptr_t(len(message)), (*C.uint8_t)(unsafe.Pointer(&sig[0])))
	runtime.KeepAlive(k)
	return bool(ok)
}

// SignRequest signs req for an indexer to register the key, ready to submit
// with indexd's admin client.
//
// The returned request carries the expiration that was signed, which drops
// anything below a microsecond, so submit it rather than rebuilding one from
// req.
func (k *PreAuthorizedKey) SignRequest(req PreAuthorizedKeyRequest) (accounts.PreAuthorizedKeyRequest, error) {
	cConnectKey := C.CString(req.ConnectKey)
	defer C.free(unsafe.Pointer(cConnectKey))

	creq := C.sia_pre_authorized_key_request_t{
		connect_key:        cConnectKey,
		expiration_unix_us: C.int64_t(req.Expiration.UnixMicro()),
		total_uses:         C.uint32_t(req.TotalUses),
	}
	// Copied to C memory because cgo refuses a struct holding a pointer into
	// Go memory.
	if req.AllowedAppID != nil {
		appID := C.CBytes(req.AllowedAppID[:])
		defer C.free(appID)
		creq.allowed_app_id = (*C.uint8_t)(appID)
	}

	k.mu.RLock()
	defer k.mu.RUnlock()
	var cJSON, cerr *C.char
	code := C.sia_pre_authorized_key_sign_request_json(k.ptr, &creq, &cJSON, &cerr)
	runtime.KeepAlive(k)
	if code != C.SIA_OK {
		return accounts.PreAuthorizedKeyRequest{}, localError(code, cerr)
	}

	var signed accounts.PreAuthorizedKeyRequest
	if err := json.Unmarshal([]byte(goString(cJSON)), &signed); err != nil {
		return accounts.PreAuthorizedKeyRequest{}, fmt.Errorf("failed to decode signed request: %w", err)
	}
	return signed, nil
}

// cMessage points at the first byte of message, or is nil for an empty one,
// which the C ABI accepts with a zero length.
func cMessage(message []byte) *C.uint8_t {
	if len(message) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&message[0]))
}
