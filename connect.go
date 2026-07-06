package siastorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"go.sia.tech/core/types"
	ffi "go.sia.tech/siastorage/sia_storage_ffi"
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

// AppMetadata contains metadata about an application. This metadata is
// provided during app registration and is used to identify the application
// to users.
type AppMetadata struct {
	// ID is a unique identifier for an application. It should be generated
	// once and stay constant for the lifetime of the app.
	//
	// Changing it will invalidate any existing app keys and prevent access
	// to associated data.
	//
	// It should be a randomly generated 32-byte value. You can use
	// GenerateAppID to create a new app ID.
	ID          types.Hash256
	Name        string
	Description string
	LogoURL     string
	ServiceURL  string
	CallbackURL string
}

func (m AppMetadata) toFFI() ffi.AppMetadata {
	meta := ffi.AppMetadata{
		Id:          m.ID[:],
		Name:        m.Name,
		Description: m.Description,
		ServiceUrl:  m.ServiceURL,
	}
	if m.LogoURL != "" {
		meta.LogoUrl = &m.LogoURL
	}
	if m.CallbackURL != "" {
		meta.CallbackUrl = &m.CallbackURL
	}
	return meta
}

// A Builder helps connect an application to an indexer and initialize an SDK
// instance.
//
// A Builder is single-use: once it has created an SDK, any further calls on
// the same Builder return [ErrBuilderConsumed].
type Builder struct {
	inner     *ffi.Builder
	initErr   error
	requested atomic.Bool
	consumed  atomic.Bool
}

// NewBuilder creates a new Builder for connecting applications to the indexer.
//
// A builder can only be used to create a single SDK instance. Methods called
// on a builder that has already created an SDK return [ErrBuilderConsumed].
func NewBuilder(indexerURL string, metadata AppMetadata) *Builder {
	inner, err := ffi.NewBuilder(indexerURL, metadata.toFFI())
	return &Builder{inner: inner, initErr: err}
}

func (b *Builder) checkConsumed() error {
	if b.initErr != nil {
		return b.initErr
	} else if b.consumed.Load() {
		return ErrBuilderConsumed
	}
	return nil
}

func (b *Builder) consume() error {
	if !b.consumed.CompareAndSwap(false, true) {
		return ErrBuilderConsumed
	}
	return nil
}

// RequestConnection sends a request to connect an application to the indexer.
//
// It returns a response URL that the user must visit to approve the request.
// The app should display the response URL to the user.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance.
func (b *Builder) RequestConnection(ctx context.Context) (string, error) {
	if err := b.checkConsumed(); err != nil {
		return "", err
	}
	url, err := runContext(ctx, func() (string, error) {
		if _, err := b.inner.RequestConnection(); err != nil {
			return "", err
		}
		return b.inner.ResponseUrl()
	})
	if err != nil {
		return "", err
	}
	b.requested.Store(true)
	return url, nil
}

// WaitForApproval waits for the user to approve the app connection request.
// The user must visit the response URL returned by [Builder.RequestConnection]
// to approve the request. It blocks until the request is approved, denied, or
// the context is cancelled.
//
// It returns [ErrUserRejected] if the user denied the request and
// [ErrBuilderConsumed] if the builder has already created an SDK instance.
// Callers can branch on these using [errors.Is].
func (b *Builder) WaitForApproval(ctx context.Context) error {
	if err := b.checkConsumed(); err != nil {
		return err
	} else if !b.requested.Load() {
		return ErrNoConnectionRequest
	}
	_, err := runContext(ctx, func() (struct{}, error) {
		_, err := b.inner.WaitForApproval()
		return struct{}{}, mapApprovalError(err)
	})
	return err
}

// Register derives an application key from a BIP-39 seed phrase and registers
// it with the indexer.
//
// This key should be stored securely by the application and never shared with
// anyone else. It can be regenerated using the same app ID, user account, and
// seed phrase.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance and [ErrNotApproved] if [Builder.WaitForApproval] has not yet
// returned successfully.
func (b *Builder) Register(ctx context.Context, mnemonic string) (*SDK, error) {
	if err := b.checkConsumed(); err != nil {
		return nil, err
	}
	inner, err := runContext(ctx, func() (*ffi.Sdk, error) {
		return b.inner.Register(mnemonic)
	})
	if err != nil {
		return nil, mapRegisterError(err)
	}
	if err := b.consume(); err != nil {
		return nil, err
	}
	return &SDK{inner: inner}, nil
}

// SDK creates a new SDK instance using the given application key. If the key
// is not authorized, an error is returned.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance and [ErrUnauthorized] if the app key is not authorized by the
// indexer.
func (b *Builder) SDK(appKey types.PrivateKey, opts ...Option) (*SDK, error) {
	if err := b.checkConsumed(); err != nil {
		return nil, err
	}
	ffiKey, err := appKeyFromPrivate(appKey)
	if err != nil {
		return nil, fmt.Errorf("invalid app key: %w", err)
	}
	inner, err := b.inner.Connected(ffiKey)
	if err != nil {
		return nil, err
	} else if inner == nil {
		return nil, ErrUnauthorized
	}
	if err := b.consume(); err != nil {
		return nil, err
	}
	sdk := &SDK{inner: *inner}
	for _, opt := range opts {
		opt(sdk)
	}
	sdk.applyLogger()
	return sdk, nil
}

// mapApprovalError maps a flat FFI BuilderError to the sentinel errors
// documented on [Builder.WaitForApproval].
func mapApprovalError(err error) error {
	if err == nil {
		return nil
	}
	switch msg := strings.ToLower(err.Error()); {
	case strings.Contains(msg, "reject"), strings.Contains(msg, "denied"):
		return ErrUserRejected
	case strings.Contains(msg, "expired"):
		return ErrRequestExpired
	default:
		return err
	}
}

func mapRegisterError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "invalid state") {
		return ErrNotApproved
	}
	return err
}
