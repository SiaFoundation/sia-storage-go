package siastorage

/*
#include <stdlib.h>
#include "sia_storage.h"
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
	"unsafe"

	"go.sia.tech/core/types"
)

const (
	builderStateInit = iota
	builderStateRequested
	builderStateApproved
	builderStateConsumed
)

var (
	// ErrBuilderConsumed is returned when a [Builder] method is called
	// after the builder has already created an SDK. A builder is
	// single-use; create a new one to start another connection.
	ErrBuilderConsumed = errors.New("builder already consumed")

	// ErrNoConnectionRequest is returned by [Builder.WaitForApproval]
	// when called before [Builder.RequestConnection].
	ErrNoConnectionRequest = errors.New("no connection request")

	// ErrRequestExpired is returned by [Builder.WaitForApproval]
	// when the connection request has expired.
	ErrRequestExpired = errors.New("connection request expired")

	// ErrNotApproved is returned by [Builder.Register] when called
	// before [Builder.WaitForApproval] has successfully completed.
	ErrNotApproved = errors.New("connection not approved")

	// ErrUnauthorized is returned by [Builder.SDK] when the supplied
	// app key is not authorized by the indexer.
	ErrUnauthorized = errors.New("app key is not authorized")

	// ErrUserRejected is returned by [Builder.WaitForApproval] when
	// the user rejects the connection request.
	ErrUserRejected = errors.New("user rejected the connection request")
)

type (
	// AppMetadata contains metadata about an application.
	// This metadata is provided during app registration
	// and is used to identify the application to users.
	AppMetadata struct {
		// ID is a unique identifier for an application.
		// It should be generated once and stay constant for
		// the lifetime of the app.
		//
		// Changing it will invalidate any existing app keys
		// and prevent access to associated data.
		//
		// It should be a randomly generated 32-byte value.
		// You can use GenerateAppID to create a new app ID.
		ID          types.Hash256
		Name        string
		Description string
		LogoURL     string
		ServiceURL  string
		CallbackURL string
	}
)

// A Builder helps connect an application to an indexer
// and initialize an SDK instance.
//
// A Builder is single-use: once it has created an SDK, any further calls
// on the same Builder return [ErrBuilderConsumed].
type Builder struct {
	indexerURL string
	metadata   AppMetadata

	mu    sync.Mutex
	state int
	ptr   *C.sia_builder_t
}

// handle lazily creates the underlying Rust builder.
func (b *Builder) handle() (*C.sia_builder_t, error) {
	if b.ptr != nil {
		return b.ptr, nil
	}

	optional := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	metaJSON, err := json.Marshal(map[string]any{
		"appID":       b.metadata.ID,
		"name":        b.metadata.Name,
		"description": b.metadata.Description,
		"serviceURL":  b.metadata.ServiceURL,
		"logoURL":     optional(b.metadata.LogoURL),
		"callbackURL": optional(b.metadata.CallbackURL),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode app metadata: %w", err)
	}

	curl := C.CString(b.indexerURL)
	cmeta := C.CString(string(metaJSON))
	defer C.free(unsafe.Pointer(curl))
	defer C.free(unsafe.Pointer(cmeta))

	var ptr *C.sia_builder_t
	var cerr *C.char
	code := C.sia_builder_new(curl, cmeta, &ptr, &cerr)
	if err := goError(nil, code, cerr); err != nil {
		return nil, fmt.Errorf("failed to create builder: %w", err)
	}
	b.ptr = ptr
	runtime.AddCleanup(b, func(p *C.sia_builder_t) {
		C.sia_builder_free(p)
	}, ptr)
	return ptr, nil
}

// RequestConnection sends a request to connect an application to the indexer.
//
// It returns a response URL that the user must visit to approve the request.
// The app should display the response URL to the user.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance.
func (b *Builder) RequestConnection(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == builderStateConsumed {
		return "", ErrBuilderConsumed
	}
	ptr, err := b.handle()
	if err != nil {
		return "", err
	}

	tok, release := cancelToken(ctx)
	defer release()

	var urlC, cerr *C.char
	code := C.sia_builder_request_connection(ptr, tok, &urlC, &cerr)
	if err := goError(ctx, code, cerr); err != nil {
		return "", fmt.Errorf("failed to request app connection: %w", err)
	}
	b.state = builderStateRequested
	return goString(urlC), nil
}

// WaitForApproval waits for the user to approve the app connection request.
// The user must visit the response URL returned by [Builder.RequestConnection]
// to approve the request. It blocks until the request is approved, denied, or
// the context is cancelled.
//
// It returns [ErrUserRejected] if the user denied the request,
// [ErrRequestExpired] if the request expired before it was approved, and
// [ErrBuilderConsumed] if the builder has already created an SDK instance.
// Callers can branch on these using [errors.Is].
func (b *Builder) WaitForApproval(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case builderStateConsumed:
		return ErrBuilderConsumed
	case builderStateInit, builderStateApproved:
		return ErrNoConnectionRequest
	}
	ptr, err := b.handle()
	if err != nil {
		return err
	}

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_builder_wait_for_approval(ptr, tok, &cerr)
	if err := goError(ctx, code, cerr); err != nil {
		// the underlying builder is consumed on failure; a new
		// connection attempt requires a new builder
		b.state = builderStateConsumed
		return err
	}
	b.state = builderStateApproved
	return nil
}

// Register derives an application key from a BIP-39 seed phrase and
// registers it with the indexer.
//
// This key should be stored securely by the application and never
// shared with anyone else. It can be regenerated using the same app
// ID, user account, and seed phrase.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance and [ErrNotApproved] if [Builder.WaitForApproval] has not yet
// returned successfully.
func (b *Builder) Register(ctx context.Context, mnemonic string) (*SDK, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case builderStateConsumed:
		return nil, ErrBuilderConsumed
	case builderStateInit, builderStateRequested:
		return nil, ErrNotApproved
	}
	ptr, err := b.handle()
	if err != nil {
		return nil, err
	}
	// the underlying builder is consumed by registration regardless of the
	// outcome
	b.state = builderStateConsumed

	tok, release := cancelToken(ctx)
	defer release()

	cmnemonic := C.CString(mnemonic)
	defer C.free(unsafe.Pointer(cmnemonic))

	var sdkPtr *C.sia_sdk_t
	var cerr *C.char
	code := C.sia_builder_register(ptr, cmnemonic, tok, &sdkPtr, &cerr)
	if err := goError(ctx, code, cerr); err != nil {
		return nil, fmt.Errorf("failed to register app: %w", err)
	}
	return newSDK(sdkPtr), nil
}

// SDK creates a new SDK instance using the given application key. If the
// key is not authorized, an error is returned.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance and [ErrUnauthorized] if the app key is not authorized by the
// indexer.
func (b *Builder) SDK(appKey types.PrivateKey, opts ...Option) (*SDK, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == builderStateConsumed {
		return nil, ErrBuilderConsumed
	}
	if len(appKey) != 64 {
		return nil, fmt.Errorf("invalid app key length: %d", len(appKey))
	}
	ptr, err := b.handle()
	if err != nil {
		return nil, err
	}

	var seed [32]byte
	copy(seed[:], appKey[:32])

	var sdkPtr *C.sia_sdk_t
	var cerr *C.char
	code := C.sia_builder_connect(ptr, cBytes32(&seed), nil, &sdkPtr, &cerr)
	if err := goError(nil, code, cerr); err != nil {
		if errors.Is(err, ErrUnauthorized) {
			return nil, ErrUnauthorized
		}
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	b.state = builderStateConsumed

	sdk := newSDK(sdkPtr)
	for _, opt := range opts {
		opt(sdk)
	}
	return sdk, nil
}

// NewBuilder creates a new Builder for connecting applications to the indexer.
//
// A builder can only be used to create a single SDK instance. Methods called
// on a builder that has already created an SDK return [ErrBuilderConsumed].
func NewBuilder(indexerURL string, metadata AppMetadata) *Builder {
	return &Builder{
		indexerURL: indexerURL,
		metadata:   metadata,
	}
}

// GenerateAppID generates a new random application ID.
func GenerateAppID() (id types.Hash256) {
	rand.Read(id[:])
	return id
}

// NewSeedPhrase generates a new seed phrase.
func NewSeedPhrase() string {
	return goString(C.sia_generate_recovery_phrase())
}
