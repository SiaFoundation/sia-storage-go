---
default: major
---

# Replace the native Go implementation with UniFFI bindings

The SDK is now derived from the Rust SDK (sia-sdk-rs) via UniFFI-generated
bindings, like the Kotlin, Python and Swift SDKs. The `siastorage` package
wraps the generated `sia_storage_ffi` package in an idiomatic Go API. Building
now requires a Rust toolchain and cgo; see the README for instructions.
