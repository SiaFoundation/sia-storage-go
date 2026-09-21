// Package siastorage stores and retrieves data on the Sia network.
//
// Erasure coding, encryption, host selection and the RHP4 transport are not
// implemented here. They live in sia-sdk-rs and are reached through cgo, over
// the C ABI its sia_storage_cabi crate exports, so there is one implementation
// of that logic rather than two. This package is the Go surface over it.
//
// # Platforms
//
// Because the native code is linked from a prebuilt static archive, this
// package builds only where an archive is committed: darwin/arm64,
// darwin/amd64, linux/amd64, linux/arm64 and windows/amd64. CGO_ENABLED=0 and
// GOOS=js do not work at all, and the linux archives are gnu, so a musl based
// image needs its own.
//
// # Getting started
//
// An application authorizes once against an indexer and reuses the app key it
// receives. [NewBuilder] starts that flow; [Builder.Register] walks a new user
// through approval, and [Builder.Connect] reuses a key already authorized.
//
// The app key is derived from a BIP-39 recovery phrase, which
// [GenerateRecoveryPhrase] produces. Store it: the account cannot be reached
// without it.
//
// # Handles
//
// [SDK], [Object], [SharingKey], [Upload], [Download] and [PackedUpload] are
// handles onto memory the native side owns. Each has a Close that releases it,
// and every one is safe to call more than once. Close waits for any call still
// using the handle to return, so closing an SDK while a request is in flight
// blocks until that request finishes rather than freeing underneath it.
//
// Using a handle after Close is not undefined: accessors report a zero value
// and calls return an error.
//
// # Testing
//
// The siastorage_mock build tag swaps the host transport for an in-process
// network, so real erasure coding, encryption and the whole transfer pipeline
// run with only the network itself faked. It needs an archive built with the
// mock cargo feature, which `make testlib` produces and which is never
// committed or shipped. See [MockNetwork].
package siastorage
