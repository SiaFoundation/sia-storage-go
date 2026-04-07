# Sia Storage SDK

The official Go SDK for storing and retrieving data on the Sia network.

For guides and additional resources, visit the [developer portal](https://devs.sia.storage). For detailed API documentation, see the [Godocs](https://pkg.go.dev/go.sia.tech/sia-storage).

## Connecting to the Indexer

Before uploading or downloading data, your application must connect to an
indexer. First, create a `Builder` with your application metadata, then walk
the user through the approval flow:

```go
builder := sdk.NewBuilder("https://sia.storage", sdk.AppMetadata{
	ID:          appID,                          // a persistent, randomly-generated 32-byte app ID
	Name:        "MyApp",                        // display name
	Description: "My first Sia application",     // short description
	LogoURL:     "https://my.app/logo.png",      // logo shown in the indexer UI
	ServiceURL:  "https://my.app",               // your application's homepage
})

// request a connection — the user must visit the returned URL to approve
responseURL, err := builder.RequestConnection(ctx)
if err != nil {
	log.Fatal("failed to request connection:", err)
}
fmt.Println("Approve the connection:", responseURL)

// block until the user approves or rejects
approved, err := builder.WaitForApproval(ctx)
if err != nil {
	log.Fatal("failed to wait for approval:", err)
} else if !approved {
	log.Fatal("user denied the connection")
}

// derive an app key from a BIP-39 seed phrase and register it
mnemonic := sdk.NewSeedPhrase() // generate once — store securely
client, err := builder.Register(ctx, mnemonic)
if err != nil {
	log.Fatal("failed to register:", err)
}
defer client.Close()
```

On subsequent launches, skip the approval flow and create the SDK directly
with the previously derived app key:

```go
builder := sdk.NewBuilder("https://sia.storage", sdk.AppMetadata{ID: appID})
client, err := builder.SDK(appKey)
if err != nil {
	log.Fatal("failed to create SDK:", err)
}
defer client.Close()
```

## Uploading and Downloading Data

Once connected, you can upload and download files using the SDK:

```go
// upload
obj := sdk.NewEmptyObject()
f, _ := os.Open("path/to/src.dat")
defer f.Close()

if err := client.Upload(ctx, &obj, f); err != nil {
	log.Fatal("upload failed:", err)
}

// pin the object so the indexer tracks it
if err := client.PinObject(ctx, obj); err != nil {
	log.Fatal("pin failed:", err)
}

// download
out, _ := os.Create("path/to/dst.dat")
defer out.Close()

if err := client.Download(ctx, out, obj); err != nil {
	log.Fatal("download failed:", err)
}
```

The `Object` returned by `Upload` contains the encryption key and slab
metadata required to download the data later. After uploading, call
`PinObject` to persist the object on the indexer. Applications should store
the sealed object (via `obj.Seal(appKey)`) so it can be reopened later.

## Packed Uploads

When uploading many small objects, `UploadPacked` combines them into shared
slabs to reduce overhead:

```go
packed, err := client.UploadPacked()
if err != nil {
	log.Fatal("failed to create packed upload:", err)
}
defer packed.Close()

for _, data := range smallObjects {
	if _, err := packed.Add(ctx, bytes.NewReader(data)); err != nil {
		log.Fatal("failed to add object:", err)
	}
}

objects, err := packed.Finalize(ctx)
if err != nil {
	log.Fatal("failed to finalize:", err)
}

// pin and download each object individually
for _, obj := range objects {
	client.PinObject(ctx, obj)
}
```

### Optimizing Packed Uploads

Use `Remaining()` and `Length()` to monitor padding and decide when to
finalize:

```go
const paddingTarget = 0.05

for len(uploads) > 0 {
	packed, err := client.UploadPacked()
	if err != nil {
		log.Fatal(err)
	}

	for len(uploads) > 0 {
		if _, err := packed.Add(ctx, bytes.NewReader(uploads[0])); err != nil {
			log.Fatal(err)
		}
		uploads = uploads[1:]

		if packed.Length() == 0 {
			continue
		}
		if float64(packed.Remaining())/float64(packed.Length()) <= paddingTarget {
			break
		}
	}

	objects, err := packed.Finalize(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, obj := range objects {
		client.PinObject(ctx, obj)
	}
	packed.Close()
}
```

### Limitations

- `PackedUpload` is not thread-safe; do not call `Add` concurrently
- Empty objects are not supported and will return `ErrEmptyObject`
- Once `Finalize` is called, subsequent `Add` calls return `ErrUploadFinalized`

## Sharing Objects

Objects can be shared via time-limited URLs:

```go
url, err := client.CreateSharedObjectURL(ctx, obj.ID(), time.Now().Add(24*time.Hour))
if err != nil {
	log.Fatal("failed to create shared URL:", err)
}

// anyone with the URL can download the object
var buf bytes.Buffer
if err := client.DownloadSharedObject(ctx, &buf, url); err != nil {
	log.Fatal("failed to download shared object:", err)
}
```
