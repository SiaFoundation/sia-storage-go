# siastorage

Go bindings for [sia-sdk-rs](https://github.com/SiaFoundation/sia-sdk-rs), over
the C ABI its `sia_storage_cabi` crate exports.

This package contains no storage engine. Erasure coding, encryption, host
selection and the RHP4 transport all live in Rust and are reached through cgo,
so there is one implementation of that logic rather than two.

## Archives

The prebuilt static archives live in a separate module,
[siastorage-libs](https://github.com/SiaFoundation/siastorage-libs),
which `link.go` imports for effect. They are committed there rather than here
so that a fresh copy of every platform does not enter this repository's history
on each rebuild, while `go get` still works with no extra tooling.

To rebuild one from a local sia-sdk-rs checkout, which writes into the libs
module beside this one:

    make lib SIA_SDK_RS=../rs_sia_suite/sia-sdk-rs LIBS=../sia-storage-cabi-libs

Two guards are worth running before shipping anything:

    make check-header    # the vendored header still matches sia-sdk-rs
    make check-no-mock   # the archive was not built with the mock transport

## Platforms

`link.go` declares darwin/arm64, darwin/amd64, linux/amd64, linux/arm64 and
windows/amd64. Only a platform with a committed archive can link, so this is a
much smaller set than the 47 a pure Go module supports, and `CGO_ENABLED=0`
and `GOOS=js` do not work at all.

Note that the linux targets are gnu. A musl based image, which is common for
containers, needs its own archive.

## Running the demo

`examples/demo` walks the whole surface in the order an application would,
printing what each step exercised. It is the fastest way to see the bindings
work, and the fastest way to find out which entry point broke.

Run it from anywhere inside this repository. The uncommitted `go.work` carries
the `replace` for the unpublished libs module, so nothing builds without it.

### Against the mock

No indexer, no credentials and no Siacoin. An in process network of 40 hosts
runs real erasure coding, encryption and the whole transfer pipeline, with only
the network itself faked.

    make testlib SIA_SDK_RS=../rs_sia_suite/sia-sdk-rs
    go run -tags siastorage_mock ./examples/demo

The `make testlib` step matters. The mock archive is deliberately not committed,
and a stale one fails in ways that look like binding bugs, most memorably a 501
from every sharing call. Rebuild it after any change to the Rust.

### Against a real indexer

Built without the tag, the demo links the production archive and talks to an
indexer. This writes real data and spends real Siacoin.

    make lib SIA_SDK_RS=../rs_sia_suite/sia-sdk-rs LIBS=../sia-storage-cabi-libs
    go run ./examples/demo -indexer https://sia.storage

With no app key it walks the approval flow, printing a URL to approve in a
browser and then the app key to reconnect with later. Write down the recovery
phrase it generates, because the account is unreachable without it.

    go run ./examples/demo -indexer https://sia.storage -app-key <hex>

### Flags

| flag | what it does |
|---|---|
| `-indexer` | indexer URL; leave empty to use the mock |
| `-app-key` | hex app key to connect with, skipping approval |
| `-recovery-phrase` | phrase to register with, generated when empty |
| `-v` | route the Rust log output through zap |