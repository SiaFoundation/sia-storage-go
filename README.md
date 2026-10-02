# Sia Storage SDK

The official Go SDK for storing and retrieving data on the Sia network.

For guides and additional resources, visit the [developer portal](https://devs.sia.storage). For detailed API documentation, see the [Godocs](https://pkg.go.dev/go.sia.tech/siastorage@v0.2.2-0.20261002235308-d2590c25520a).

## Requirements

This SDK is a binding, not an implementation. Erasure coding, encryption, host
selection and the RHP4 transport live in
[sia-sdk-rs](https://github.com/SiaFoundation/sia-sdk-rs) and are reached
through cgo, so there is one implementation of that logic rather than two.

That has consequences worth knowing before you depend on it:

- **cgo is required.** `CGO_ENABLED=0` and `GOOS=js` do not work.
- **Only six platforms are supported**, because each needs a prebuilt static
  archive committed to this repository: `darwin/arm64`, `darwin/amd64`,
  `linux/amd64`, `linux/arm64`, `windows/amd64` and `windows/arm64`.
- **The linux archives are gnu.** A musl based image, which is common for
  containers, needs an archive of its own.

Nothing else is needed: the archives are committed, so `go get` works with no
extra tooling.

## Usage

Every exported call has a runnable example in the
[Godocs](https://pkg.go.dev/go.sia.tech/siastorage@v0.2.2-0.20261002235308-d2590c25520a#pkg-examples), covering
connecting and registering, uploading and downloading, packed uploads, sharing
keys and share URLs, the object event feed and the tuning options.

`examples/demo` walks the whole surface end to end against a real indexer.

## Development

The archives under `ffi/lib/` are built only by the **Build FFI Libraries**
workflow, never locally: an archive compiled on a developer machine cannot be
traced back to a revision or reproduced by anyone else. `ffi/lib/PROVENANCE`
records which `sia-sdk-rs` revision and workflow run produced the current set,
and a CI check rejects any pull request that edits `ffi/lib/` by hand.

    make fetch-lib    # download this platform's archive from a workflow run
    make testlib      # build the mock archive, the one thing built locally
    make test         # both suites
    make lint

Tests behind the `siastorage_mock` build tag run against an in-process network
of hosts, with real erasure coding, encryption and transfer pipelines and only
the network itself faked. `examples/demo` walks the whole surface and is the
fastest way to see it work:

    make testlib
    go run -tags siastorage_mock ./examples/demo
