// Package siastorage is a Go SDK for interacting with the Sia decentralized
// storage network through an indexer.
//
// The SDK is implemented in Rust (github.com/SiaFoundation/sia-sdk-rs) and
// exposed to Go via UniFFI-generated bindings. This package wraps the
// low-level generated bindings in sia_storage_ffi with an idiomatic Go API.
// Applications that need functionality not covered by the wrappers can use
// the sia_storage_ffi package directly.
package siastorage

import (
	"log/slog"

	ffi "go.sia.tech/siastorage/sia_storage_ffi"
)

// Types re-exported from the generated bindings. See the sia_storage_ffi
// package for their documentation.
type (
	// An AppKey is used to sign requests to the indexer. It is derived from a
	// BIP-39 recovery phrase and must be stored securely by the application.
	AppKey = ffi.AppKey
	// AppMetadata describes an application connecting to the indexer.
	AppMetadata = ffi.AppMetadata
	// An Object is an immutable, erasure-coded and client-side encrypted
	// piece of data pinned to an indexer.
	Object = ffi.PinnedObject
	// A SealedObject is an encrypted object for secure offline storage or
	// transmission. It can be opened with the app key using OpenObject.
	SealedObject = ffi.SealedObject
	// An ObjectEvent represents an object and whether it was deleted or not.
	ObjectEvent = ffi.ObjectEvent
	// An ObjectsCursor paginates through objects stored in the indexer.
	ObjectsCursor = ffi.ObjectsCursor
	// A Host is a storage provider on the Sia network.
	Host = ffi.Host
	// An Account is an account registered on the indexer.
	Account = ffi.Account
	// A Slab is a contiguous erasure-coded segment of a file.
	Slab = ffi.Slab
	// A PinnedSlab is a slab that has been pinned to the indexer.
	PinnedSlab = ffi.PinnedSlab
	// ShardProgress reports a successfully transferred shard.
	ShardProgress = ffi.ShardProgress
	// ProgressCallback receives ShardProgress updates during transfers.
	ProgressCallback = ffi.ProgressCallback
	// UploadOptions configures an upload operation.
	UploadOptions = ffi.UploadOptions
	// DownloadOptions configures a download operation.
	DownloadOptions = ffi.DownloadOptions
)

// NewAppKey imports an app key from a 32-byte seed previously returned by
// AppKey.Export.
func NewAppKey(key []byte) (*AppKey, error) {
	return ffi.NewAppKey(key)
}

// NewObject creates a new empty object to upload into.
func NewObject() *Object {
	return ffi.NewPinnedObject()
}

// OpenObject opens a sealed object using the app key that sealed it.
func OpenObject(appKey *AppKey, sealed SealedObject) (*Object, error) {
	return ffi.PinnedObjectOpen(appKey, sealed)
}

// GenerateRecoveryPhrase generates a new BIP-39 12-word recovery phrase.
func GenerateRecoveryPhrase() string {
	return ffi.GenerateRecoveryPhrase()
}

// ValidateRecoveryPhrase validates a BIP-39 recovery phrase.
func ValidateRecoveryPhrase(phrase string) error {
	return ffi.ValidateRecoveryPhrase(phrase)
}

// EncodedSize calculates the encoded size of data given the original size and
// erasure coding parameters.
func EncodedSize(size uint64, dataShards, parityShards uint8) uint64 {
	return ffi.EncodedSize(size, dataShards, parityShards)
}

// NewProgressCallback wraps fn so it can be set as the progress callback in
// UploadOptions or DownloadOptions.
func NewProgressCallback(fn func(ShardProgress)) *ProgressCallback {
	var cb ProgressCallback = progressFunc(fn)
	return &cb
}

type progressFunc func(ShardProgress)

func (fn progressFunc) Progress(p ShardProgress) { fn(p) }

// SetLogger routes the SDK's internal logging to the provided slog.Logger.
// Level is one of "trace", "debug", "info", "warn" or "error".
func SetLogger(log *slog.Logger, level string) {
	ffi.SetLogger(slogAdapter{log}, level)
}

type slogAdapter struct{ log *slog.Logger }

func (l slogAdapter) Debug(msg string) { l.log.Debug(msg) }
func (l slogAdapter) Info(msg string)  { l.log.Info(msg) }
func (l slogAdapter) Warn(msg string)  { l.log.Warn(msg) }
func (l slogAdapter) Error(msg string) { l.log.Error(msg) }
