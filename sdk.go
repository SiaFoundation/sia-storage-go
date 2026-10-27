package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
)

// AppMetadata identifies an application to the indexer.
type AppMetadata struct {
	// AppID is derived into the account's encryption keys. Generate it once
	// with GenerateAppID and store it, because changing it makes data written
	// under the previous value unreachable.
	AppID       types.Hash256
	Name        string
	Description string
	ServiceURL  string

	// LogoURL and CallbackURL are optional and sent as null when empty.
	LogoURL     string
	CallbackURL string
}

// appMetadataJSON is the wire form sia_builder_new parses. The field names are
// the contract with the Rust side and do not all follow one convention.
type appMetadataJSON struct {
	AppID       types.Hash256 `json:"appID"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	ServiceURL  string        `json:"serviceURL"`
	LogoURL     *string       `json:"logoURL"`
	CallbackURL *string       `json:"callbackURL"`
}

// GenerateAppID returns a random application identifier.
func GenerateAppID() (id types.Hash256) {
	// crypto/rand.Read is documented never to fail.
	rand.Read(id[:])
	return
}

// An App is the indexer's record of a registered application.
type App struct {
	ID          types.Hash256 `json:"id"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	LogoURL     *string       `json:"logoURL"`
	ServiceURL  *string       `json:"serviceURL"`
}

// An Account is the indexer's view of the account behind an app key.
//
// The JSON matches the indexer's own type, so a caller needing that can decode
// into it instead. [SealedObject] is the indexer's type directly.
type Account struct {
	AccountKey types.PublicKey `json:"accountKey"`

	// MaxPinnedData is the account's pinning limit, PinnedData how much of it
	// is used, and RemainingStorage what is left after both that limit and the
	// current quota. PinnedSize is the encoded size, which is what actually
	// sits on the network.
	MaxPinnedData    uint64 `json:"maxPinnedData"`
	RemainingStorage uint64 `json:"remainingStorage"`
	PinnedData       uint64 `json:"pinnedData"`
	PinnedSize       uint64 `json:"pinnedSize"`

	// Ready is false while the indexer is still processing a registration. An
	// account becomes ready once it has propagated on the network.
	Ready bool `json:"ready"`

	App      App       `json:"app"`
	LastUsed time.Time `json:"lastUsed"`
}

// A Builder authorizes an app key against an indexer and produces an SDK.
// Close it once the SDK has been obtained.
type Builder struct {
	ptr     *C.sia_builder_t
	cleanup runtime.Cleanup

	// mu guards ptr against Close. Readers hold it for the whole FFI call, so
	// Close waits for anything in flight rather than freeing underneath it.
	// A blocking call therefore delays Close, which is the trade this package
	// makes everywhere: a slow Close beats a use after free.
	mu     sync.RWMutex
	closed bool
}

// NewBuilder prepares a connection to the indexer at indexerURL.
func NewBuilder(indexerURL string, metadata AppMetadata) (*Builder, error) {
	meta := appMetadataJSON{
		AppID:       metadata.AppID,
		Name:        metadata.Name,
		Description: metadata.Description,
		ServiceURL:  metadata.ServiceURL,
	}
	if metadata.LogoURL != "" {
		meta.LogoURL = &metadata.LogoURL
	}
	if metadata.CallbackURL != "" {
		meta.CallbackURL = &metadata.CallbackURL
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("encode app metadata: %w", err)
	}

	cURL := C.CString(indexerURL)
	defer C.free(unsafe.Pointer(cURL))
	cMeta := C.CString(string(metaJSON))
	defer C.free(unsafe.Pointer(cMeta))

	var ptr *C.sia_builder_t
	var cerr *C.char
	if code := C.sia_builder_new(cURL, cMeta, &ptr, &cerr); code != C.SIA_OK {
		return nil, localError(code, cerr)
	}
	return wrapBuilder(ptr), nil
}

func wrapBuilder(ptr *C.sia_builder_t) *Builder {
	b := &Builder{ptr: ptr}
	// The cleanup is a backstop for a caller who never calls Close. It must not
	// capture b, or b would never become unreachable.
	//
	// Because it does not, b is unreachable from the moment a method loads
	// b.ptr, so every method that hands the handle to C has to keep b alive
	// until that call returns. Without it the cleanup can free the handle
	// while the call is still running, and for the blocking calls that window
	// is a whole network round trip.
	b.cleanup = runtime.AddCleanup(b, func(p *C.sia_builder_t) {
		C.sia_builder_free(p)
	}, ptr)
	return b
}

// Close releases the builder. It is safe to call more than once.
func (b *Builder) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.cleanup.Stop()
	C.sia_builder_free(b.ptr)
	return nil
}

// RequestConnection asks the indexer to open an approval request and returns
// the URL the user has to visit to approve it.
func (b *Builder) RequestConnection(ctx context.Context) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return "", errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	var cURL, cerr *C.char
	code := C.sia_builder_request_connection(b.ptr, tok, &cURL, &cerr)
	runtime.KeepAlive(b)
	if code != C.SIA_OK {
		return "", goError(ctx, code, cerr)
	}
	return goString(cURL), nil
}

// WaitForApproval blocks until the request is resolved, returning
// ErrUserRejected if the user declined and ErrRequestExpired if the approval
// window closed first.
func (b *Builder) WaitForApproval(ctx context.Context) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_builder_wait_for_approval(b.ptr, tok, &cerr)
	runtime.KeepAlive(b)
	return goError(ctx, code, cerr)
}

// Reconnecting reports whether the connect key the user approved with already
// has an account for this application. A connect key may hold accounts under
// more than one recovery phrase, so this does not say whether a particular
// phrase is the one already registered; MatchesExistingAppKey answers that.
//
// Only valid once the connection is approved.
func (b *Builder) Reconnecting() (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return false, errClosed
	}

	var out C.bool
	var cerr *C.char
	code := C.sia_builder_reconnecting(b.ptr, &out, &cerr)
	runtime.KeepAlive(b)
	if code != C.SIA_OK {
		return false, localError(code, cerr)
	}
	return bool(out), nil
}

// MatchesExistingAppKey reports whether mnemonic derives the app key the
// indexer already knows, so a caller can tell a reconnect from a new
// registration before calling Register, which cannot be undone. It registers
// nothing and leaves the builder as it was.
//
// Only valid once the connection is approved.
func (b *Builder) MatchesExistingAppKey(ctx context.Context, mnemonic string) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return false, errClosed
	}

	cMnemonic := C.CString(mnemonic)
	defer C.free(unsafe.Pointer(cMnemonic))

	tok, release := cancelToken(ctx)
	defer release()

	var out C.bool
	var cerr *C.char
	code := C.sia_builder_matches_existing_app_key(b.ptr, cMnemonic, tok, &out, &cerr)
	runtime.KeepAlive(b)
	if code != C.SIA_OK {
		return false, goError(ctx, code, cerr)
	}
	return bool(out), nil
}

// Register derives an app key from mnemonic and registers it with the indexer.
func (b *Builder) Register(ctx context.Context, mnemonic string) (*SDK, error) {
	// The write lock, not the read lock: registering consumes the builder on
	// the native side, which refuses while another call still holds it.
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, errClosed
	}

	cMnemonic := C.CString(mnemonic)
	defer C.free(unsafe.Pointer(cMnemonic))

	tok, release := cancelToken(ctx)
	defer release()

	var ptr *C.sia_sdk_t
	var cerr *C.char
	code := C.sia_builder_register(b.ptr, cMnemonic, tok, &ptr, &cerr)
	runtime.KeepAlive(b)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapSDK(ptr), nil
}

// Connect authorizes an app key that the indexer already knows, returning
// ErrUnauthorized when it does not.
//
// Only the first 32 bytes are sent, which is the seed the rest of the key is
// derived from.
func (b *Builder) Connect(ctx context.Context, appKey types.PrivateKey) (*SDK, error) {
	if len(appKey) < 32 {
		return nil, errors.New("app key must be at least 32 bytes")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}
	tok, release := cancelToken(ctx)
	defer release()

	var ptr *C.sia_sdk_t
	var cerr *C.char
	code := C.sia_builder_connect(b.ptr, (*C.uint8_t)(unsafe.Pointer(&appKey[0])), tok, &ptr, &cerr)
	runtime.KeepAlive(b)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapSDK(ptr), nil
}

// ConnectPreAuthorized connects with a key the indexer has already authorized,
// doing request, approve and register in one call with nothing to wait for.
// The app key is derived from mnemonic, so a different phrase registers a new
// account rather than reconnecting to the existing one.
//
// Consumes the builder, cancellation included, since the key may already have
// been spent.
//
// Only the first 32 bytes of preAuthorizedKey are sent, which is the seed the
// rest of the key is derived from.
func (b *Builder) ConnectPreAuthorized(ctx context.Context, preAuthorizedKey types.PrivateKey, mnemonic string) (*SDK, error) {
	if len(preAuthorizedKey) < 32 {
		return nil, errors.New("pre-authorized key must be at least 32 bytes")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}

	cMnemonic := C.CString(mnemonic)
	defer C.free(unsafe.Pointer(cMnemonic))

	tok, release := cancelToken(ctx)
	defer release()

	var ptr *C.sia_sdk_t
	var cerr *C.char
	code := C.sia_builder_connect_pre_authorized(b.ptr,
		(*C.uint8_t)(unsafe.Pointer(&preAuthorizedKey[0])), cMnemonic, tok, &ptr, &cerr)
	runtime.KeepAlive(b)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return wrapSDK(ptr), nil
}

// An SDK is an authorized connection to an indexer.
type SDK struct {
	ptr     *C.sia_sdk_t
	cleanup runtime.Cleanup

	// mu guards ptr against Close. Readers hold it for the whole FFI call, so
	// Close waits for anything in flight rather than freeing underneath it.
	// A blocking call therefore delays Close, which is the trade this package
	// makes everywhere: a slow Close beats a use after free.
	mu     sync.RWMutex
	closed bool
}

func wrapSDK(ptr *C.sia_sdk_t) *SDK {
	s := &SDK{ptr: ptr}
	s.cleanup = runtime.AddCleanup(s, func(p *C.sia_sdk_t) {
		C.sia_sdk_free(p)
	}, ptr)
	return s
}

// Close releases the connection and its resources. It is safe to call more
// than once.
func (s *SDK) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.cleanup.Stop()
	C.sia_sdk_free(s.ptr)
	return nil
}

// AppKey returns the key this connection is authorized under, or a zeroed key
// once the SDK is closed
func (s *SDK) AppKey() types.PrivateKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return make(types.PrivateKey, 64)
	}
	var seed [32]byte
	C.sia_sdk_app_key(s.ptr, cBytes32(&seed))
	runtime.KeepAlive(s)
	return types.NewPrivateKeyFromSeed(seed[:])
}

// Account fetches the account record from the indexer.
func (s *SDK) Account(ctx context.Context) (Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Account{}, errClosed
	}
	tok, release := cancelToken(ctx)
	defer release()

	var cJSON, cerr *C.char
	code := C.sia_sdk_account(s.ptr, tok, &cJSON, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return Account{}, goError(ctx, code, cerr)
	}
	var a Account
	if err := json.Unmarshal([]byte(goString(cJSON)), &a); err != nil {
		return Account{}, fmt.Errorf("decode account: %w", err)
	}
	return a, nil
}
