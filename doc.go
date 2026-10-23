// Package siastorage stores and retrieves data on the Sia network.
//
// # Platforms
//
// This package links a prebuilt static archive, so it builds only where one is
// committed: darwin/arm64, darwin/amd64, linux/amd64, linux/arm64,
// windows/amd64 and windows/arm64. CGO_ENABLED=0 and GOOS=js do not work at
// all, and the linux archives are gnu, so a musl based image needs its own.
//
// # Getting started
//
// An application authorizes once and reuses the app key it receives.
//
// For a new account, from [NewBuilder]:
//
//   - [Builder.RequestConnection] returns a URL for the user to approve
//   - [Builder.WaitForApproval] blocks until they answer
//   - [Builder.Register] takes a phrase from [NewSeedPhrase], returns the SDK
//
// With a key already authorized, [Builder.Connect] replaces all three.
//
// Store the phrase: the account cannot be reached without it.
//
// # Handles
//
// [Builder], [SDK], [SharedSDK], [Download] and [PackedUpload] have a Close.
// It is safe to call more than once and blocks until any call still using the
// handle returns. Afterwards accessors report zero values and calls return an
// error.
package siastorage
