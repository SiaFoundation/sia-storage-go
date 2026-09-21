# Sia Storage SDK

The official Go SDK for storing and retrieving data on the Sia network.

For guides and additional resources, visit the [developer portal](https://devs.sia.storage). For detailed API documentation, see the [Godocs](https://pkg.go.dev/go.sia.tech/siastorage).

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

## Connecting to the Indexer

Before uploading or downloading data, your application must connect to an
indexer. First, create a `Builder` with your application metadata, then walk
the user through the approval flow:

```go
builder, err := siastorage.NewBuilder("https://sia.storage", siastorage.AppMetadata{
	AppID:       appID,                       // a persistent, randomly-generated 32-byte app ID
	Name:        "MyApp",                     // display name
	Description: "My first Sia application",  // short description
	LogoURL:     "https://my.app/logo.png",   // optional, shown in the indexer UI
	ServiceURL:  "https://my.app",            // your application's homepage
})
if err != nil {
	log.Fatal("failed to create builder:", err)
}
defer builder.Close()

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

// derive an app key from a BIP-39 recovery phrase and register it
phrase := siastorage.GenerateRecoveryPhrase() // generate once — store securely
sdk, err := builder.Register(ctx, phrase)
if err != nil {
	log.Fatal("failed to register:", err)
}
defer sdk.Close()
```

`AppID` is derived into the account's encryption keys. Generate it once with
`GenerateAppID` and store it, because changing it makes everything written
under the previous value unreachable.

Once registered, reconnect with the app key instead of repeating the approval
flow:

```go
sdk, err := builder.Connect(ctx, appKey)
if errors.Is(err, siastorage.ErrUnauthorized) {
	log.Fatal("this app key is not authorized")
}
```

## Uploading and Downloading Data

`Upload` implements `io.Writer` and `Download` implements `io.ReadCloser`, so
`io.Copy` drives both.

```go
up, err := sdk.Upload(ctx, siastorage.NewObject(), siastorage.UploadOptions{})
if err != nil {
	log.Fatal("failed to start upload:", err)
}
defer up.Close()

if _, err := io.Copy(up, src); err != nil {
	log.Fatal("failed to write:", err)
}

// Finish signals EOF and waits for the transfer to complete
obj, err := up.Finish()
if err != nil {
	log.Fatal("failed to finish upload:", err)
}
defer obj.Close()

// an upload pins the slabs it wrote, but not the object record; until the
// object is pinned no lookup by ID, share URL or delete can find it
if err := sdk.PinObject(ctx, obj); err != nil {
	log.Fatal("failed to pin object:", err)
}
```

Downloading takes the object back, optionally a byte range of it:

```go
dl, err := sdk.Download(ctx, obj, siastorage.DownloadOptions{})
if err != nil {
	log.Fatal("failed to start download:", err)
}
defer dl.Close()

if _, err := io.Copy(dst, dl); errors.Is(err, siastorage.ErrNotEnoughShards) {
	log.Fatal("too many shards are gone to recover the object")
} else if err != nil {
	log.Fatal("failed to read:", err)
}
```

Both accept an `OnShard` callback that fires as each shard completes, which is
how you drive a progress bar. It runs on a goroutine the transfer owns rather
than on the thread that reported the shard, so it may block without stalling
the transfer, though events are dropped once it falls far enough behind.
`Dropped` reports how many were missed, and every event carries the running
total in `ShardProgress.Transferred`.

## Packed Uploads

Objects smaller than a slab waste the remainder of it. A packed upload fills
one slab with several objects:

```go
packed, err := sdk.PackedUpload(ctx, siastorage.UploadOptions{})
if err != nil {
	log.Fatal("failed to start packed upload:", err)
}
defer packed.Close()

for _, f := range files {
	if _, err := packed.Add(f); err != nil {
		log.Fatal("failed to add object:", err)
	}
}

// returns one object per successful Add, in order
objects, err := packed.Finalize()
if err != nil {
	log.Fatal("failed to finalize:", err)
}
```

`OptimalDataSize` reports the payload that fills a slab exactly, which is the
size to aim a batch at. `Remaining` and `Length` report progress toward it.

As with a plain upload, finalizing pins the slabs but not the object records,
so each still has to be pinned.

## Sharing Objects

A share URL grants read access to a single object until it expires. It is
derived locally, reaches no indexer, and cannot be revoked once handed out:

```go
url, err := sdk.ObjectShareURL(obj, time.Now().Add(24*time.Hour))
```

A sharing key grants access to as many objects as you attach to it, and unlike
a share URL it can be revoked:

```go
key, err := sdk.CreateSharingKey(ctx, "photos", time.Time{}) // zero time never expires
if err != nil {
	log.Fatal("failed to create sharing key:", err)
}
defer key.Close()

if err := sdk.ShareObject(ctx, key, obj); err != nil {
	log.Fatal("failed to share object:", err)
}

// the 32-byte seed is the entire credential — treat it as a password
seed := key.Export()
```

Whoever holds the seed reads the objects without an account, paid for by the
account that shared them. `RevokeSharingKey` detaches every object at once,
which is the only way to withdraw a seed already handed out. Downloads already
in flight can keep reading for up to five more minutes, because the hosts were
paid for those reads before the revocation.

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
