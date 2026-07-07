---
default: major
---

# Rewrite the SDK over the sia_storage Rust crate

The SDK is now a cgo binding over the `sia_storage` Rust crate instead of a
separate Go implementation. Erasure coding, encryption, host transport, and
the upload/download pipelines run in Rust; the Go API is preserved where the
crate has equivalent functionality.

Building now requires cgo. Prebuilt static libraries are committed under
`ffi/lib/` so `go get` continues to work on supported platforms without a
Rust toolchain.

API changes:

- `SealedObject`, `Object.Seal`, and `Object.Slabs` were removed; objects are
  sealed/opened inside the Rust core. Use `SDK.Object` to retrieve pinned
  objects and the new `Object.EncodedSize` for the on-network size.
- `WithUploadInflight`, `WithDownloadInflight`, and `WithDownloadHostTimeout`
  were replaced by `WithMaxBufferedSlabs` and `WithMaxBufferedChunks`,
  matching the Rust crate's memory-budget options.
- `Account` and `ObjectEvents` now use types defined by this package instead
  of `go.sia.tech/indexd` types; `PruneSlabs` no longer takes query options.
- `WithLogger` bridges the Rust core's process-wide logging to zap, so the
  most recently configured logger receives output from every SDK instance.
