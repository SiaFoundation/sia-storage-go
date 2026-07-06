// Package siastorage is a Go SDK for storing and retrieving data on the Sia
// decentralized storage network through an indexer.
//
// The SDK is implemented in Rust (github.com/SiaFoundation/sia-sdk-rs) and
// exposed to Go via UniFFI-generated bindings. This package wraps the
// low-level generated bindings in the sia_storage_ffi package with an
// idiomatic Go API that mirrors the previous native Go implementation.
// Applications that need functionality not covered by the wrappers can use
// the sia_storage_ffi package directly.
//
// Because the SDK is backed by a Rust library, some types that were provided
// by go.sia.tech/indexd in the native implementation (for example
// slabs.SlabSlice, slabs.Cursor and app.AccountResponse) are exposed here as
// their lightweight FFI equivalents to avoid re-introducing that dependency
// tree.
package siastorage

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"go.sia.tech/core/types"
	ffi "go.sia.tech/siastorage/sia_storage_ffi"
)

// Types re-exported from the generated bindings. See the sia_storage_ffi
// package for their documentation.
type (
	// A Slab is a contiguous erasure-coded segment of an object.
	Slab = ffi.Slab
	// A PinnedSlab is a slab that has been pinned to the indexer.
	PinnedSlab = ffi.PinnedSlab
	// A PinnedSector is a sector stored on a specific host.
	PinnedSector = ffi.PinnedSector
	// A Host is a storage provider on the Sia network.
	Host = ffi.Host
	// An Account holds account information for the current app key. It
	// replaces the app.AccountResponse returned by the native SDK.
	Account = ffi.Account
	// A Cursor paginates through objects stored in the indexer. It replaces
	// slabs.Cursor from the native SDK. The zero value requests the first
	// page of results.
	Cursor = ffi.ObjectsCursor
)

// A ShardProgress reports the result of a successfully completed shard upload
// or download.
type ShardProgress struct {
	HostKey    types.PublicKey
	SlabIndex  int
	ShardIndex int
	ShardSize  uint64
	Elapsed    time.Duration
}

// NewSeedPhrase generates a new BIP-39 seed phrase.
func NewSeedPhrase() string {
	return ffi.GenerateRecoveryPhrase()
}

// GenerateAppID generates a new random application ID.
func GenerateAppID() (id types.Hash256) {
	if _, err := rand.Read(id[:]); err != nil {
		panic(err) // crypto/rand should never fail
	}
	return id
}

// EncodedSize calculates the encoded size of data given the original size and
// erasure coding parameters.
func EncodedSize(size uint64, dataShards, parityShards uint8) uint64 {
	return ffi.EncodedSize(size, dataShards, parityShards)
}

// --- conversion helpers between core/types and the FFI string encodings ---

func hashToString(h types.Hash256) string {
	return hex.EncodeToString(h[:])
}

func parseHash(s string) (types.Hash256, error) {
	var h types.Hash256
	if err := h.UnmarshalText([]byte(s)); err != nil {
		return types.Hash256{}, err
	}
	return h, nil
}

func parsePublicKey(s string) (types.PublicKey, error) {
	var pk types.PublicKey
	if err := pk.UnmarshalText([]byte(s)); err != nil {
		return types.PublicKey{}, err
	}
	return pk, nil
}

// appKeyFromPrivate converts a types.PrivateKey into the FFI AppKey. The app
// key seed is the first 32 bytes of the private key.
func appKeyFromPrivate(pk types.PrivateKey) (*ffi.AppKey, error) {
	return ffi.NewAppKey(pk[:32])
}

func privateFromAppKey(ak *ffi.AppKey) types.PrivateKey {
	return types.NewPrivateKeyFromSeed(ak.Export())
}
