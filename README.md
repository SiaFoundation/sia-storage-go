# Sia Storage SDK

The official Go SDK for storing and retrieving data on the Sia network.

The SDK is implemented in Rust ([sia-sdk-rs](https://github.com/SiaFoundation/sia-sdk-rs))
and exposed to Go through [UniFFI](https://mozilla.github.io/uniffi-rs/) bindings
generated with [uniffi-bindgen-go](https://github.com/NordSecurity/uniffi-bindgen-go),
the same approach used for the [Kotlin, Python and Swift SDKs](https://github.com/SiaFoundation/sia-storage-sdk).
The root `siastorage` package wraps the generated bindings in an idiomatic Go
API; the low-level bindings are available in the `sia_storage_ffi` package.

For guides and additional resources, visit the [developer portal](https://devs.sia.storage).
For detailed API documentation, see the [Godocs](https://pkg.go.dev/go.sia.tech/siastorage).

## Building

The SDK uses cgo to call into the Rust library, so a [Rust toolchain](https://rustup.rs)
is required in addition to Go:

```sh
./build.sh   # builds the Rust library into lib/
go test ./...
```

### Regenerating the bindings

The generated bindings in `sia_storage_ffi/` are checked in. To regenerate
them after bumping the `sia_storage_ffi` dependency in `Cargo.toml`, install
the bindings generator and run:

```sh
cargo install uniffi-bindgen-go --git https://github.com/NordSecurity/uniffi-bindgen-go --tag v0.7.0+v0.31.0
./build.sh bindings
```

The uniffi-bindgen-go version must match the `uniffi` version used by
`sia_storage_ffi` (currently 0.31.0).

## Connecting to the Indexer

Before uploading or downloading data, your application must connect to an
indexer. First, create a `Builder` with your application metadata, then walk
the user through the approval flow:

```go
builder := siastorage.NewBuilder("https://sia.storage", siastorage.AppMetadata{
	ID:          appID,                       // a persistent, randomly-generated 32-byte app ID
	Name:        "MyApp",                     // display name
	Description: "My first Sia application",  // short description
	ServiceURL:  "https://my.app",            // your application's homepage
})

// request a connection — the user must visit the returned URL to approve
responseURL, err := builder.RequestConnection(ctx)
if err != nil {
	log.Fatal("failed to request connection:", err)
}
fmt.Println("Approve the connection:", responseURL)

// block until the user approves or rejects
if err := builder.WaitForApproval(ctx); errors.Is(err, siastorage.ErrUserRejected) {
	log.Fatal("user denied the connection")
} else if err != nil {
	log.Fatal("failed to wait for approval:", err)
}

// derive an app key from a BIP-39 seed phrase and register it
mnemonic := siastorage.NewSeedPhrase() // generate once — store securely
sdk, err := builder.Register(ctx, mnemonic)
if err != nil {
	log.Fatal("failed to register:", err)
}
defer sdk.Close()
```

On subsequent launches, skip the approval flow and connect directly with the
previously derived app key:

```go
sdk, err := builder.SDK(appKey) // appKey is a types.PrivateKey
if errors.Is(err, siastorage.ErrUnauthorized) {
	// fall back to the approval flow above
} else if err != nil {
	log.Fatal("failed to connect:", err)
}
```

## Uploading and Downloading Data

Once connected, you can upload and download objects using the SDK:

```go
// upload
obj := siastorage.NewEmptyObject()
if err := sdk.Upload(ctx, &obj, file); err != nil {
	log.Fatal("failed to upload:", err)
}
if err := sdk.PinObject(ctx, obj); err != nil {
	log.Fatal("failed to pin object:", err)
}

// download
r, err := sdk.Download(obj)
if err != nil {
	log.Fatal("failed to start download:", err)
}
defer r.Close()
if _, err := io.Copy(dst, r); err != nil {
	log.Fatal("failed to download:", err)
}
```

See the [examples](examples/) directory for a complete, runnable example.
