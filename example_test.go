package siastorage_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/siastorage"
	"go.uber.org/zap"
)

// appID identifies the application and is derived into the account's
// encryption keys. Generate it once with [siastorage.GenerateAppID] and write
// the value into the source. It belongs to the application rather than to an
// install, and deriving from a different one makes everything written under
// the old value unreachable, so it is never read from the environment.
var appID = types.Hash256{
	0x39, 0x3f, 0x08, 0xaa, 0x18, 0x61, 0x17, 0x59,
	0xc0, 0x76, 0xbc, 0xee, 0x16, 0x52, 0x49, 0x07,
	0x5e, 0x71, 0x40, 0x83, 0xde, 0x8a, 0xf4, 0xa3,
	0x1a, 0x9f, 0xe2, 0x57, 0x6c, 0xd1, 0xbb, 0xc8,
}

// Connecting for the first time walks the user through approval, then derives
// an app key from a recovery phrase and registers it. Store both: the phrase
// is the only way back to the account, and the app key skips the approval
// flow next time.
func Example() {
	ctx := context.Background()

	builder, err := siastorage.NewBuilder("https://sia.storage", siastorage.AppMetadata{
		AppID:       appID,
		Name:        "MyApp",
		Description: "My first Sia application",
		ServiceURL:  "https://my.app",
	})
	if err != nil {
		log.Fatal("failed to create builder:", err)
	}
	defer builder.Close()

	responseURL, err := builder.RequestConnection(ctx)
	if err != nil {
		log.Fatal("failed to request connection:", err)
	}
	fmt.Println("Approve the connection:", responseURL)

	if err := builder.WaitForApproval(ctx); errors.Is(err, siastorage.ErrUserRejected) {
		log.Fatal("the user declined")
	} else if errors.Is(err, siastorage.ErrRequestExpired) {
		log.Fatal("the request expired before it was answered")
	} else if err != nil {
		log.Fatal("failed to wait for approval:", err)
	}

	phrase := siastorage.NewSeedPhrase()
	fmt.Println("Recovery phrase, store this somewhere safe:", phrase)

	sdk, err := builder.Register(ctx, phrase)
	if err != nil {
		log.Fatal("failed to register:", err)
	}
	defer sdk.Close()

	fmt.Printf("App key, store this too: %x\n", sdk.AppKey())
}

// An application holding a key that is already authorized skips the approval
// flow entirely.
func ExampleBuilder_Connect() {
	ctx := context.Background()

	// The app key Builder.Register generated on a previous run.
	raw, err := hex.DecodeString(os.Getenv("SIA_APP_KEY"))
	if err != nil {
		log.Fatal("failed to decode the stored app key:", err)
	}
	appKey := types.PrivateKey(raw)

	builder, err := siastorage.NewBuilder("https://sia.storage", siastorage.AppMetadata{
		AppID: appID,
		Name:  "MyApp",
	})
	if err != nil {
		log.Fatal("failed to create builder:", err)
	}
	defer builder.Close()

	sdk, err := builder.Connect(ctx, appKey)
	if errors.Is(err, siastorage.ErrUnauthorized) {
		log.Fatal("this app key is not authorized; register it first")
	} else if err != nil {
		log.Fatal("failed to connect:", err)
	}
	defer sdk.Close()

	account, err := sdk.Account(ctx)
	if err != nil {
		log.Fatal("failed to fetch the account:", err)
	}
	fmt.Printf("connected, %d of %d bytes pinned\n",
		account.PinnedData, account.MaxPinnedData)
}

// Upload implements io.Writer, so io.Copy drives it. Uploading pins the slabs
// it writes but not the object record, so the object has to be pinned before
// anything can look it up.
func ExampleSDK_Upload() {
	ctx := context.Background()
	var sdk *siastorage.SDK // from Builder.Connect or Builder.Register
	var src io.Reader       // the data to store

	up, err := sdk.Upload(ctx, siastorage.NewEmptyObject())
	if err != nil {
		log.Fatal("failed to start upload:", err)
	}
	defer up.Close()

	if _, err := io.Copy(up, src); err != nil {
		log.Fatal("failed to write:", err)
	}

	// Finish signals EOF and waits for the transfer to complete.
	obj, err := up.Finish()
	if err != nil {
		log.Fatal("failed to finish upload:", err)
	}

	if err := sdk.PinObject(ctx, obj); err != nil {
		log.Fatal("failed to pin object:", err)
	}

	fmt.Printf("stored %d bytes as %v\n", obj.Size(), obj.ID())
}

// WithUploadProgress reports each shard as it lands, which is what drives a
// progress bar. The callback runs on a goroutine the transfer owns, so it may
// block without stalling the upload, though events are dropped once it falls
// far enough behind.
func ExampleSDK_Upload_progress() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var src io.Reader

	up, err := sdk.Upload(ctx, siastorage.NewEmptyObject(),
		siastorage.WithUploadProgress(func(p siastorage.ShardProgress) {
			fmt.Printf("%d bytes transferred\n", p.Transferred)
		}))
	if err != nil {
		log.Fatal("failed to start upload:", err)
	}
	defer up.Close()

	if _, err := io.Copy(up, src); err != nil {
		log.Fatal("failed to write:", err)
	}
	if _, err := up.Finish(); err != nil {
		log.Fatal("failed to finish upload:", err)
	}

	if dropped := up.Dropped(); dropped > 0 {
		fmt.Printf("%d progress events were dropped\n", dropped)
	}
}

// Download implements io.ReadCloser. An object whose shards have decayed past
// the point of recovery reports ErrNotEnoughShards.
func ExampleSDK_Download() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var obj *siastorage.Object

	dl, err := sdk.Download(ctx, obj)
	if err != nil {
		log.Fatal("failed to start download:", err)
	}
	defer dl.Close()

	if _, err := io.Copy(os.Stdout, dl); errors.Is(err, siastorage.ErrNotEnoughShards) {
		log.Fatal("too many shards are gone to recover the object")
	} else if err != nil {
		log.Fatal("failed to read:", err)
	}
}

// WithDownloadProgress reports each shard as it lands, the same terms as
// WithUploadProgress. Transferred is the running total counted before an event
// can be dropped, so read it rather than summing ShardSize.
func ExampleSDK_Download_progress() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var obj *siastorage.Object

	dl, err := sdk.Download(ctx, obj,
		siastorage.WithDownloadProgress(func(p siastorage.ShardProgress) {
			fmt.Printf("%d of %d bytes\n", p.Transferred, obj.Size())
		}))
	if err != nil {
		log.Fatal("failed to start download:", err)
	}
	defer dl.Close()

	if _, err := io.Copy(os.Stdout, dl); err != nil {
		log.Fatal("failed to read:", err)
	}

	if dropped := dl.Dropped(); dropped > 0 {
		fmt.Printf("%d progress events were dropped\n", dropped)
	}
}

// Only the requested range is fetched, so a range read costs a fraction of
// the whole object.
func ExampleSDK_Download_byteRange() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var obj *siastorage.Object

	dl, err := sdk.Download(ctx, obj, siastorage.WithDownloadRange(0, 1<<20))
	if err != nil {
		log.Fatal("failed to start download:", err)
	}
	defer dl.Close()

	first, err := io.ReadAll(dl)
	if err != nil {
		log.Fatal("failed to read:", err)
	}
	fmt.Printf("read the first %d bytes\n", len(first))
}

// An object smaller than a slab wastes the rest of it. A packed upload fills
// one slab with several objects, and OptimalDataSize is the payload to aim a
// batch at.
func ExampleSDK_UploadPacked() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var files []io.Reader

	packed, err := sdk.UploadPacked(ctx)
	if err != nil {
		log.Fatal("failed to start packed upload:", err)
	}
	defer packed.Close()

	fmt.Printf("a full slab holds %d bytes\n", packed.OptimalDataSize())

	for _, f := range files {
		if _, err := packed.Add(f); err != nil {
			log.Fatal("failed to add object:", err)
		}
	}

	// One object per successful Add, in order.
	objects, err := packed.Finalize()
	if err != nil {
		log.Fatal("failed to finalize:", err)
	}

	// Finalizing pins the slabs but not the object records, so each still
	// has to be pinned.
	for _, obj := range objects {
		if err := sdk.PinObject(ctx, obj); err != nil {
			log.Fatal("failed to pin object:", err)
		}
	}
}

// A share URL grants read access to one object until it expires. It is derived
// locally, so it reaches no indexer and cannot be revoked once handed out.
func ExampleSDK_ObjectShareURL() {
	var sdk *siastorage.SDK
	var obj *siastorage.Object

	url, err := sdk.ObjectShareURL(obj, time.Now().Add(24*time.Hour))
	if err != nil {
		log.Fatal("failed to derive share URL:", err)
	}
	fmt.Println("any account holder given this URL can read the object for a day:", url)
}

// A sharing key grants access to as many objects as are attached to it, and
// unlike a share URL it can be revoked. The seed is the whole credential, so
// treat it as a password.
func ExampleSDK_CreateSharingKey() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var obj *siastorage.Object

	key, err := sdk.CreateSharingKey(ctx, "photos", time.Time{}) // a zero time never expires
	if err != nil {
		log.Fatal("failed to create sharing key:", err)
	}
	defer key.Close()

	if err := sdk.ShareObject(ctx, key, obj); err != nil {
		log.Fatal("failed to share object:", err)
	}

	seed := key.Export()
	fmt.Printf("hand this seed to the recipient: %x\n", seed)
}

// The recipient needs the indexer URL and the seed, and nothing else. No app
// key, no registration, no approval. The account that shared the objects pays
// for the reads.
func ExampleConnectShared() {
	ctx := context.Background()
	var seed [32]byte // handed over by whoever shared the objects

	shared, err := siastorage.ConnectShared(ctx, "https://sia.storage", seed)
	if err != nil {
		log.Fatal("failed to connect as the recipient:", err)
	}
	defer shared.Close()

	objects, err := shared.Objects(ctx, 0, 10)
	if err != nil {
		log.Fatal("failed to list the shared objects:", err)
	}

	for _, obj := range objects {
		fmt.Printf("%v, %d bytes\n", obj.ID(), obj.Size())
	}
}

// An object ID is all that has to be stored to find an object again. Fetching
// by ID decrypts the keys and metadata the record carries, so the result is a
// handle ready to download from.
func ExampleSDK_Object() {
	ctx := context.Background()
	var sdk *siastorage.SDK

	// Whatever was persisted after the upload was pinned.
	var id types.Hash256
	if _, err := hex.Decode(id[:], []byte("5343a814a71d09220f35c9ece59fa14684d825d7b2d30523a5f0921f227f4dc4")); err != nil {
		log.Fatal("failed to decode the object ID:", err)
	}

	obj, err := sdk.Object(ctx, id)
	if err != nil {
		log.Fatal("failed to fetch the object:", err)
	}

	fmt.Printf("%v is %d bytes, uploaded %v\n", obj.ID(), obj.Size(), obj.CreatedAt())
	fmt.Printf("it occupies %d bytes on the network after redundancy\n", obj.EncodedSize())
}

// Deleting an object removes the indexer's record of it, but the slabs it
// wrote stay pinned, and pinned data is what the account pays for. Pruning is
// what actually releases them, and it only releases slabs no remaining object
// references.
func ExampleSDK_DeleteObject() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var id types.Hash256 // the object to remove

	if err := sdk.DeleteObject(ctx, id); err != nil {
		log.Fatal("failed to delete the object:", err)
	}

	// A zero time leaves the cutoff to the indexer, which holds back anything
	// pinned recently so that an upload still in flight is not swept up. That
	// is the right choice for an application: slabs deleted now are released
	// once they age past the indexer's grace period.
	if err := sdk.PruneSlabs(ctx, time.Time{}); err != nil {
		log.Fatal("failed to prune slabs:", err)
	}

	// A caller that knows nothing else is uploading, such as a test or a
	// one-shot cleanup job, can prune right up to the present instead.
	if err := sdk.PruneSlabs(ctx, time.Now()); err != nil {
		log.Fatal("failed to prune slabs:", err)
	}
}

// A sealed object is the one thing this package hands back by value rather
// than as a handle, because an application keeping its own index needs the
// fields to persist. It is locked with the account's app key, so only the same
// account can open it again.
func ExampleSDK_SealObject() {
	var sdk *siastorage.SDK
	var obj *siastorage.Object

	sealed, err := sdk.SealObject(obj)
	if err != nil {
		log.Fatal("failed to seal the object:", err)
	}

	// Store this however the application stores anything else. It is already
	// encrypted, so a row in your own database is fine.
	row, err := json.Marshal(sealed)
	if err != nil {
		log.Fatal("failed to encode the sealed object:", err)
	}
	fmt.Printf("persisting %d bytes for object %v\n", len(row), sealed.ID())

	// Later, in another process, turn the stored form back into a handle.
	var restored siastorage.SealedObject
	if err := json.Unmarshal(row, &restored); err != nil {
		log.Fatal("failed to decode the sealed object:", err)
	}

	reopened, err := sdk.ObjectFromSealed(restored)
	if err != nil {
		log.Fatal("failed to open the sealed object:", err)
	}

	fmt.Printf("reopened %v, %d bytes\n", reopened.ID(), reopened.Size())
}

// A share URL carries everything needed to read one object until the URL
// expires. It is resolved through an SDK, so the holder needs an account of
// their own and pays for the reads themselves.
func ExampleSDK_ObjectFromShareURL() {
	ctx := context.Background()
	var sdk *siastorage.SDK

	// Minted by the owner with SDK.ObjectShareURL and handed over.
	shareURL := "sia://sia.storage/objects/e4eac7218c1caae41d51ea564bbf8c6fcc5dc2eed631a7b923e3c72d6cca2336/share?..."

	obj, err := sdk.ObjectFromShareURL(ctx, shareURL)
	if err != nil {
		log.Fatal("failed to resolve the share URL:", err)
	}

	dl, err := sdk.Download(ctx, obj)
	if err != nil {
		log.Fatal("failed to start download:", err)
	}
	defer dl.Close()

	if _, err := io.Copy(os.Stdout, dl); err != nil {
		log.Fatal("failed to read:", err)
	}
}

// The event feed is how an application keeping its own index stays in step
// without re-listing everything. Each page resumes directly after the event
// the cursor names, so store the cursor alongside the index and pass it back
// on the next poll.
func ExampleSDK_ObjectEvents() {
	ctx := context.Background()
	var sdk *siastorage.SDK

	// The zero cursor starts from the beginning. A real application would
	// load the cursor it saved after the previous run.
	var cursor siastorage.EventCursor

	for {
		events, err := sdk.ObjectEvents(ctx, cursor, 100)
		if err != nil {
			log.Fatal("failed to read the event feed:", err)
		}
		if len(events) == 0 {
			break // caught up
		}

		for _, event := range events {
			if event.Deleted {
				fmt.Printf("%v was deleted at %v\n", event.ID, event.UpdatedAt)
				continue
			}
			fmt.Printf("%v changed at %v, now %d bytes\n",
				event.ID, event.UpdatedAt, event.Object.Size())
		}

		// Take the cursor from the last event of the page rather than
		// building one from a timestamp: events sharing a timestamp are
		// ordered by ID, which is why the cursor carries both.
		cursor = events[len(events)-1].Cursor()
	}

	fmt.Println("save this cursor for the next poll:", cursor.After, cursor.AfterID)
}

// Metadata is held on the object handle, and whichever call next sends the
// handle to the indexer persists it. For an object that has not been pinned
// yet, that call is SDK.PinObject, so no separate metadata call is needed.
func ExampleObject_WithMetadata() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var src io.Reader

	up, err := sdk.Upload(ctx, siastorage.NewEmptyObject())
	if err != nil {
		log.Fatal("failed to start upload:", err)
	}
	defer up.Close()

	if _, err := io.Copy(up, src); err != nil {
		log.Fatal("failed to write:", err)
	}

	obj, err := up.Finish()
	if err != nil {
		log.Fatal("failed to finish upload:", err)
	}

	// Sets it on the handle only.
	obj = obj.WithMetadata([]byte(`{"filename":"holiday.jpg","tags":["2026"]}`))

	// Pinning seals the object, and the sealed form carries the metadata, so
	// this is what stores it.
	if err := sdk.PinObject(ctx, obj); err != nil {
		log.Fatal("failed to pin object:", err)
	}
}

// Changing the metadata of an object that is already pinned takes two steps,
// because Object.WithMetadata only produces a new object locally. This is the
// call that sends the change to the indexer.
func ExampleSDK_UpdateObjectMetadata() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var obj *siastorage.Object // already pinned

	fmt.Printf("metadata before: %s\n", obj.Metadata())

	obj = obj.WithMetadata([]byte(`{"filename":"holiday.jpg","tags":["2026","edited"]}`))

	if err := sdk.UpdateObjectMetadata(ctx, obj); err != nil {
		log.Fatal("failed to update the object metadata:", err)
	}

	fmt.Printf("metadata after: %s\n", obj.Metadata())
}

// Listing hosts is how an application sees what the indexer has to work with.
// The zero query applies no filters; a location sorts by proximity to a point,
// which is useful when latency matters more than breadth.
func ExampleSDK_Hosts() {
	ctx := context.Background()
	var sdk *siastorage.SDK

	all, err := sdk.Hosts(ctx, siastorage.HostQuery{})
	if err != nil {
		log.Fatal("failed to list hosts:", err)
	}
	fmt.Printf("the indexer knows %d hosts\n", len(all))

	// Sorted by distance from Amsterdam, nearest first.
	near, err := sdk.Hosts(ctx, siastorage.HostQuery{
		Location: &siastorage.GeoLocation{Latitude: 52.37, Longitude: 4.90},
		Limit:    10,
	})
	if err != nil {
		log.Fatal("failed to list hosts by location:", err)
	}

	for _, host := range near {
		fmt.Printf("%v in %s, good for upload: %t\n",
			host.PublicKey, host.CountryCode, host.GoodForUpload)
	}
}

// Truncate rewrites the slab list of a copy, leaving the original untouched.
// Both handles have to be closed, and the copy is not known to the indexer
// until it is pinned.
func ExampleObject_Truncate() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var obj *siastorage.Object

	// The first megabyte of the object, as a new object.
	prefix := obj.Truncate(1 << 20)
	if prefix == nil {
		log.Fatal("the object handle was already closed")
	}

	// This only rewrote the slab list. Nothing is stored until it is pinned.
	if err := sdk.PinObject(ctx, prefix); err != nil {
		log.Fatal("failed to pin the truncated copy:", err)
	}

	fmt.Printf("%d bytes truncated to %d\n", obj.Size(), prefix.Size())
}

// Redundancy is chosen per upload. More parity shards survive more host
// failures and cost proportionally more to store, since every shard is a
// sector on a different host.
func ExampleSDK_Upload_redundancy() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var src io.Reader

	// Ten data shards and twenty parity: any ten of the thirty recover the
	// data, at three times the stored size.
	up, err := sdk.Upload(ctx, siastorage.NewEmptyObject(),
		siastorage.WithRedundancy(10, 20))
	if err != nil {
		log.Fatal("failed to start upload:", err)
	}
	defer up.Close()

	if _, err := io.Copy(up, src); err != nil {
		log.Fatal("failed to write:", err)
	}

	obj, err := up.Finish()
	if err != nil {
		log.Fatal("failed to finish upload:", err)
	}

	fmt.Printf("%d bytes stored as %d bytes on the network\n",
		obj.Size(), obj.EncodedSize())
}

// A start offset overwrites part of an existing object in place rather than
// appending. Only the slabs covering the rewritten range are re-uploaded.
func ExampleSDK_Upload_startOffset() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var obj *siastorage.Object
	var patch io.Reader

	up, err := sdk.Upload(ctx, obj, siastorage.WithUploadStartOffset(4<<20))
	if err != nil {
		log.Fatal("failed to start upload:", err)
	}
	defer up.Close()

	if _, err := io.Copy(up, patch); err != nil {
		log.Fatal("failed to write:", err)
	}

	patched, err := up.Finish()
	if err != nil {
		log.Fatal("failed to finish upload:", err)
	}

	// The rewritten object is a new record until it is pinned.
	if err := sdk.PinObject(ctx, patched); err != nil {
		log.Fatal("failed to pin the patched object:", err)
	}
}

// Both transfers buffer ahead of the caller so the network stays busy while
// the application is slow to read or write. Raising the limit costs memory,
// roughly one slab or chunk each; lowering it bounds memory on a small device.
func ExampleSDK_Upload_buffering() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var src io.Reader

	// At most two slabs are held in memory ahead of the writer.
	up, err := sdk.Upload(ctx, siastorage.NewEmptyObject(),
		siastorage.WithUploadMaxBufferedSlabs(2))
	if err != nil {
		log.Fatal("failed to start upload:", err)
	}
	defer up.Close()

	if _, err := io.Copy(up, src); err != nil {
		log.Fatal("failed to write:", err)
	}

	obj, err := up.Finish()
	if err != nil {
		log.Fatal("failed to finish upload:", err)
	}

	// The download side has the same knob, counted in chunks rather than
	// slabs.
	dl, err := sdk.Download(ctx, obj,
		siastorage.WithDownloadMaxBufferedChunks(2))
	if err != nil {
		log.Fatal("failed to start download:", err)
	}
	defer dl.Close()

	if _, err := io.Copy(io.Discard, dl); err != nil {
		log.Fatal("failed to read:", err)
	}
}

// Listing the account's sharing keys returns the indexer's view of each one,
// including what it grants access to. Every record carries a key handle that
// has to be closed.
func ExampleSDK_SharingKeys() {
	ctx := context.Background()
	var sdk *siastorage.SDK

	records, err := sdk.SharingKeys(ctx, 0, 50)
	if err != nil {
		log.Fatal("failed to list sharing keys:", err)
	}

	for _, record := range records {
		fmt.Printf("%q grants %d object(s), %d bytes pinned\n",
			record.Description, record.Stats.ObjectCount, record.Stats.PinnedSize)

		if record.Stats.ExpiresAt.IsZero() {
			fmt.Println("  this key never expires")
		} else {
			fmt.Println("  expires", record.Stats.ExpiresAt)
		}

		record.Key.Close()
	}
}

// The indexer's record for a single key, which is how an owner checks what a
// key is currently granting without listing all of them.
func ExampleSDK_SharingKey() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var key *siastorage.SharingKey

	record, err := sdk.SharingKey(ctx, key)
	if err != nil {
		log.Fatal("failed to fetch the sharing key record:", err)
	}

	fmt.Printf("%q grants %d object(s), created %v\n",
		record.Description, record.Stats.ObjectCount, record.Stats.CreatedAt)
}

// An owner can list the objects attached to one of their keys, which is the
// same set the holder of the seed sees.
func ExampleSDK_SharedObjects() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var key *siastorage.SharingKey

	objects, err := sdk.SharedObjects(ctx, key, 0, 50)
	if err != nil {
		log.Fatal("failed to list the shared objects:", err)
	}

	for _, obj := range objects {
		fmt.Printf("%v, %d bytes\n", obj.ID(), obj.Size())
	}
}

// Detaching one object leaves the key and everything else attached to it in
// place. This is the narrow withdrawal; RevokeSharingKey is the broad one.
func ExampleSDK_UnshareObject() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var key *siastorage.SharingKey
	var id types.Hash256

	err := sdk.UnshareObject(ctx, key, id)
	if errors.Is(err, siastorage.ErrObjectNotAttached) {
		log.Fatal("that object was never attached to this key")
	} else if err != nil {
		log.Fatal("failed to detach the object:", err)
	}
}

// Revoking detaches every object at once, which is the only way to withdraw a
// seed that has already been handed out. Downloads already in flight can keep
// reading for a few more minutes, because the hosts were paid for those reads
// before the revocation landed.
func ExampleSDK_RevokeSharingKey() {
	ctx := context.Background()
	var sdk *siastorage.SDK
	var key *siastorage.SharingKey

	if err := sdk.RevokeSharingKey(ctx, key); err != nil {
		log.Fatal("failed to revoke the sharing key:", err)
	}

	// The handle is still valid locally; it simply grants nothing now.
	key.Close()
}

// The recipient downloads through the SharedSDK, not through an SDK. There is
// no account on this side, so every read is paid for by whoever shared it.
func ExampleSharedSDK_Download() {
	ctx := context.Background()
	var shared *siastorage.SharedSDK
	var obj *siastorage.Object // from SharedSDK.Objects or SharedSDK.Object

	dl, err := shared.Download(ctx, obj)
	if err != nil {
		log.Fatal("failed to start download:", err)
	}
	defer dl.Close()

	if _, err := io.Copy(os.Stdout, dl); err != nil {
		log.Fatal("failed to read:", err)
	}
}

// Stats tells the holder of a seed what it grants, which is the only view
// they have: there is no account behind a SharedSDK to query.
func ExampleSharedSDK_Stats() {
	ctx := context.Background()
	var shared *siastorage.SharedSDK

	stats, err := shared.Stats(ctx)
	if err != nil {
		log.Fatal("failed to fetch the key stats:", err)
	}

	fmt.Printf("%d object(s), %d bytes, %d bytes pinned\n",
		stats.ObjectCount, stats.ObjectSize, stats.PinnedSize)

	if stats.ExpiresAt.IsZero() {
		fmt.Println("this key never expires")
	} else {
		fmt.Println("access ends", stats.ExpiresAt)
	}
}

// The log sink is process wide rather than per SDK, because the native engine
// behind this package installs one global logger. Setting it twice replaces
// the first, and every SDK in the process logs through whichever was set last.
func ExampleSetLogger() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal("failed to build the logger:", err)
	}
	defer logger.Sync()

	siastorage.SetLogger(logger)
}
