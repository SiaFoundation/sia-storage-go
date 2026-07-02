package siastorage

import (
	"errors"

	ffi "go.sia.tech/siastorage/sia_storage_ffi"
)

// ErrNotRegistered is returned by Builder.Connected when the app key is not
// registered with the indexer yet. Call Builder.RequestConnection to start
// the registration flow.
var ErrNotRegistered = errors.New("app key is not registered with the indexer")

// A Builder connects an application to an indexer and produces an SDK
// instance.
//
// Call Connected to attempt to connect with an existing app key. If that
// returns ErrNotRegistered, call RequestConnection to obtain a URL for the
// user to approve the connection, WaitForApproval to block until they do,
// and finally Register to register the app and obtain an SDK.
type Builder struct {
	inner *ffi.Builder
}

// NewBuilder creates a new SDK builder for the given indexer URL.
func NewBuilder(indexerURL string, meta AppMetadata) (*Builder, error) {
	inner, err := ffi.NewBuilder(indexerURL, meta)
	if err != nil {
		return nil, err
	}
	return &Builder{inner: inner}, nil
}

// Connected attempts to connect using the provided app key. If the app key
// is not registered with the indexer, ErrNotRegistered is returned and the
// caller should proceed with RequestConnection.
func (b *Builder) Connected(appKey *AppKey) (*SDK, error) {
	sdk, err := b.inner.Connected(appKey)
	if err != nil {
		return nil, err
	} else if sdk == nil {
		return nil, ErrNotRegistered
	}
	return &SDK{inner: *sdk}, nil
}

// RequestConnection requests connection approval for the application and
// returns the URL the user must visit to approve the connection request.
func (b *Builder) RequestConnection() (string, error) {
	if _, err := b.inner.RequestConnection(); err != nil {
		return "", err
	}
	return b.inner.ResponseUrl()
}

// WaitForApproval blocks until the user approves the connection request
// created by RequestConnection.
func (b *Builder) WaitForApproval() error {
	_, err := b.inner.WaitForApproval()
	return err
}

// Register registers the application with the indexer using the user's
// recovery phrase and returns an SDK instance.
func (b *Builder) Register(mnemonic string) (*SDK, error) {
	sdk, err := b.inner.Register(mnemonic)
	if err != nil {
		return nil, err
	}
	return &SDK{inner: sdk}, nil
}
