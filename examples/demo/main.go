// Command demo drives the whole Go surface over the C ABI, in the order a real
// application would: connect, upload, inspect, share, list and clean up.
//
// It runs against an in process mock network by default, so it needs no
// indexer, no credentials and no Siacoin:
//
//	go run -tags siastorage_mock ./examples/demo
//
// Built without that tag it links the production archive and talks to a real
// indexer instead:
//
//	go run ./examples/demo -indexer https://sia.storage
//
// Every step prints what it exercised, so a failure names the entry point that
// broke rather than leaving a stack trace to decode.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"go.sia.tech/siastorage"
	"go.uber.org/zap"
)

const (
	// Large enough to span several 4 MiB sectors, so erasure coding actually
	// runs rather than the payload landing in a single shard.
	payloadSize = 9 << 20

	// Small enough that several share one slab, which is the point of a packed
	// upload.
	packedSize  = 32 << 10
	packedCount = 4
)

// connectOptions is what the two connect implementations share. The mock build
// ignores everything but the absence of an indexer URL.
type connectOptions struct {
	IndexerURL     string
	AppKeyHex      string
	RecoveryPhrase string
}

var step int

// stage prints a numbered heading so the output reads as a script.
func stage(format string, args ...any) {
	step++
	fmt.Printf("\n%2d. %s\n", step, fmt.Sprintf(format, args...))
}

func info(format string, args ...any) {
	fmt.Printf("    %s\n", fmt.Sprintf(format, args...))
}

func fail(what string, err error) {
	fmt.Printf("    FAILED: %s: %v\n", what, err)
	os.Exit(1)
}

func main() {
	var (
		indexer = flag.String("indexer", "", "indexer URL; empty runs against the mock")
		appKey  = flag.String("app-key", "", "hex app key to connect with, skipping approval")
		phrase  = flag.String("recovery-phrase", "", "recovery phrase to register with; generated when empty")
		verbose = flag.Bool("v", false, "log what the native side is doing")
	)
	flag.Parse()

	if *verbose {
		logger, err := zap.NewDevelopment()
		if err != nil {
			log.Fatalf("logger: %v", err)
		}
		defer logger.Sync()
		// Exercises sia_set_logger, which bridges the Rust log crate to zap.
		siastorage.SetLogger(logger)
	}

	ctx := context.Background()
	sdk, cleanup, err := connect(ctx, connectOptions{
		IndexerURL:     *indexer,
		AppKeyHex:      *appKey,
		RecoveryPhrase: *phrase,
	})
	if err != nil {
		fail("connect", err)
	}
	defer cleanup()
	defer sdk.Close()

	run(ctx, sdk)

	fmt.Printf("\nAll %d steps completed.\n", step)
}

func run(ctx context.Context, sdk *siastorage.SDK) {
	stage("Account")
	info("app key %v", sdk.AppKey().PublicKey())
	account, err := sdk.Account(ctx)
	if err != nil {
		fail("Account", err)
	}
	info("account %v, ready %t", account.AccountKey, account.Ready)
	info("pinned %s of %s, %s remaining",
		bytes4(account.PinnedData), bytes4(account.MaxPinnedData), bytes4(account.RemainingStorage))
	if account.App.Name != "" {
		info("registered as %q", account.App.Name)
	}

	// ---------------------------------------------------------------- upload
	stage("Upload %s, watching shard progress", bytes4(payloadSize))
	want := payload(payloadSize)
	var shards int
	var transferred uint64
	obj, err := upload(ctx, sdk, want, siastorage.UploadOptions{
		OnShard: func(p siastorage.ShardProgress) {
			shards++
			transferred = p.Transferred
		},
	})
	if err != nil {
		fail("upload", err)
	}
	defer obj.Close()
	info("%d shards reported, %s transferred", shards, bytes4(transferred))
	info("object %v", obj.ID())
	info("%s logical, %s encoded, %.2fx redundancy",
		bytes4(obj.Size()), bytes4(obj.EncodedSize()),
		float64(obj.EncodedSize())/float64(obj.Size()))
	info("created %s, updated %s",
		obj.CreatedAt().Format(time.RFC3339), obj.UpdatedAt().Format(time.RFC3339))

	// -------------------------------------------------------------- metadata
	stage("Attach metadata and persist it")
	meta := []byte(`{"filename":"demo.bin","kind":"synthetic"}`)
	obj.UpdateMetadata(meta)
	if err := sdk.UpdateObjectMetadata(ctx, obj); err != nil {
		fail("UpdateObjectMetadata", err)
	}
	info("%d bytes of metadata stored, of the 984 an object can hold", len(meta))

	// ------------------------------------------------------------------- pin
	stage("Pin the object so the indexer knows about it")
	if err := sdk.PinObject(ctx, obj); err != nil {
		fail("PinObject", err)
	}
	fetched, err := sdk.Object(ctx, obj.ID())
	if err != nil {
		fail("Object", err)
	}
	defer fetched.Close()
	info("fetched by ID alone, %s, metadata %s", bytes4(fetched.Size()), fetched.Metadata())

	// -------------------------------------------------------------- download
	stage("Download the whole object and verify it")
	got, err := download(ctx, sdk, fetched, siastorage.DownloadOptions{})
	if err != nil {
		fail("download", err)
	}
	if !bytes.Equal(got, want) {
		fail("verify", fmt.Errorf("%d bytes came back and they differ", len(got)))
	}
	sum := sha256.Sum256(got)
	info("%s verified, sha256 %x", bytes4(uint64(len(got))), sum[:8])

	stage("Download a byte range")
	const offset, length = 1 << 20, 64 << 10
	n := uint64(length)
	part, err := download(ctx, sdk, fetched, siastorage.DownloadOptions{Offset: offset, Length: &n})
	if err != nil {
		fail("range download", err)
	}
	if !bytes.Equal(part, want[offset:offset+length]) {
		fail("verify range", fmt.Errorf("the range does not match the source"))
	}
	info("bytes %d to %d match", offset, offset+length)

	// ----------------------------------------------------------------- hosts
	stage("List the hosts the indexer knows")
	hosts, err := sdk.Hosts(ctx, siastorage.HostQuery{})
	if err != nil {
		fail("Hosts", err)
	}
	if len(hosts) == 0 {
		fail("Hosts", fmt.Errorf("the indexer knows no hosts"))
	}
	info("%d host(s); the first is %v in %s, good for upload %t",
		len(hosts), hosts[0].PublicKey, hosts[0].CountryCode, hosts[0].GoodForUpload)
	if len(hosts[0].Addresses) > 0 {
		info("reachable at %s over %s", hosts[0].Addresses[0].Address, hosts[0].Addresses[0].Protocol)
	}
	paged, err := sdk.Hosts(ctx, siastorage.HostQuery{Offset: 1, Limit: 3})
	if err != nil {
		fail("Hosts paged", err)
	}
	info("offset 1 limit 3 returned %d", len(paged))
	near, err := sdk.Hosts(ctx, siastorage.HostQuery{
		Location: &siastorage.GeoLocation{Latitude: 52.37, Longitude: 4.90},
		Limit:    1,
	})
	if err != nil {
		fail("Hosts by location", err)
	}
	info("nearest to Amsterdam: %d host(s)", len(near))

	// ----------------------------------------------------------------- slabs
	stage("Inspect the slabs the object is built from")
	ids := fetched.SlabIDs()
	if len(ids) == 0 {
		fail("SlabIDs", fmt.Errorf("a %s object references no slabs", bytes4(fetched.Size())))
	}
	info("%d slab(s); an id is derived from a slab's contents, not stored", len(ids))
	slab, err := sdk.Slab(ctx, ids[0])
	if err != nil {
		fail("Slab", err)
	}
	info("slab %v: %d of %d sectors needed, version %d",
		slab.ID, slab.MinShards, len(slab.Sectors), slab.Version)
	if len(slab.Sectors) > 0 {
		info("first sector root %v on host %v", slab.Sectors[0].Root, slab.Sectors[0].HostKey)
	}
	if _, ok := fetched.SlabID(len(ids)); ok {
		fail("SlabID", fmt.Errorf("an index past the end reported success"))
	}
	info("an out of range index reports false rather than a zero id")

	// ---------------------------------------------------------------- sealed
	stage("Seal the object and open it again")
	sealed, err := sdk.SealObject(obj)
	if err != nil {
		fail("SealObject", err)
	}
	opened, err := sdk.ObjectFromSealed(sealed)
	if err != nil {
		fail("ObjectFromSealed", err)
	}
	defer opened.Close()
	info("%d slabs sealed, reopened to the same ID %t", len(sealed.Slabs), opened.ID() == obj.ID())

	// ------------------------------------------------------------- share URL
	stage("Mint a share URL and resolve it")
	url, err := sdk.ObjectShareURL(obj, time.Now().Add(24*time.Hour))
	if err != nil {
		fail("ObjectShareURL", err)
	}
	info("%s", truncate(url, 96))
	viaURL, err := sdk.ObjectFromShareURL(ctx, url)
	if err != nil {
		fail("ObjectFromShareURL", err)
	}
	defer viaURL.Close()
	info("resolved to the same object %t", viaURL.ID() == obj.ID())

	// ------------------------------------------------------------- overwrite
	stage("Overwrite a range in place")
	const patchAt = 2 << 20
	patch := bytes.Repeat([]byte{0xAB}, 128<<10)
	at := uint64(patchAt)
	overwritten, err := uploadInto(ctx, sdk, obj, patch, siastorage.UploadOptions{StartOffset: &at})
	if err != nil {
		fail("overwrite", err)
	}
	defer overwritten.Close()
	if overwritten.Size() != uint64(len(want)) {
		fail("overwrite", fmt.Errorf("the size changed to %d", overwritten.Size()))
	}
	if overwritten.ID() == obj.ID() {
		fail("overwrite", fmt.Errorf("the id did not change, though the slabs did"))
	}
	if err := sdk.PinObject(ctx, overwritten); err != nil {
		fail("PinObject overwritten", err)
	}
	expected := append([]byte(nil), want...)
	copy(expected[patchAt:], patch)
	after, err := download(ctx, sdk, overwritten, siastorage.DownloadOptions{})
	if err != nil {
		fail("download overwritten", err)
	}
	if !bytes.Equal(after, expected) {
		fail("verify overwrite", fmt.Errorf("the rewritten range or its surroundings differ"))
	}
	info("%s rewritten at offset %s, the rest untouched, same size",
		bytes4(uint64(len(patch))), bytes4(patchAt))
	info("the id moved to %v, since an id is derived from the slabs", overwritten.ID())

	past := overwritten.Size() + 1
	if _, err := uploadInto(ctx, sdk, overwritten, []byte("x"), siastorage.UploadOptions{StartOffset: &past}); !errors.Is(err, siastorage.ErrOutOfRange) {
		fail("overwrite past the end", fmt.Errorf("wanted ErrOutOfRange, got %v", err))
	}
	info("starting past the end reports ErrOutOfRange")

	if _, err := sdk.PackedUpload(ctx, siastorage.UploadOptions{StartOffset: &at}); !errors.Is(err, siastorage.ErrInvalidState) {
		fail("packed with a start offset", fmt.Errorf("wanted ErrInvalidState, got %v", err))
	}
	info("a packed upload refuses one, since it always appends")

	// -------------------------------------------------------------- truncate
	stage("Truncate a copy of the object")
	const cut = 1 << 20
	short := overwritten.Truncate(cut)
	if short == nil {
		fail("Truncate", fmt.Errorf("truncate returned nothing"))
	}
	defer short.Close()
	if short.Size() != cut {
		fail("Truncate", fmt.Errorf("size is %d, wanted %d", short.Size(), cut))
	}
	if overwritten.Size() != uint64(len(want)) {
		fail("Truncate", fmt.Errorf("the original was modified"))
	}
	if err := sdk.PinObject(ctx, short); err != nil {
		fail("PinObject truncated", err)
	}
	prefix, err := download(ctx, sdk, short, siastorage.DownloadOptions{})
	if err != nil {
		fail("download truncated", err)
	}
	if !bytes.Equal(prefix, expected[:cut]) {
		fail("verify truncate", fmt.Errorf("the truncated copy is not the original's prefix"))
	}
	info("%s copy verified as the prefix; the original is unchanged", bytes4(cut))

	// ---------------------------------------------------------- sharing keys
	stage("Create a sharing key and attach the object")
	key, err := sdk.CreateSharingKey(ctx, "demo key", time.Now().Add(7*24*time.Hour))
	if err != nil {
		fail("CreateSharingKey", err)
	}
	defer key.Close()
	info("public key %v", key.PublicKey())
	if err := sdk.ShareObject(ctx, key, obj); err != nil {
		fail("ShareObject", err)
	}
	record, err := sdk.SharingKey(ctx, key)
	if err != nil {
		fail("SharingKey", err)
	}
	info("%q holds %d object(s), %s pinned, expires %s",
		record.Description, record.Stats.ObjectCount,
		bytes4(record.Stats.PinnedSize), record.Stats.ExpiresAt.Format(time.RFC3339))

	keys, err := sdk.SharingKeys(ctx, 0, 0)
	if err != nil {
		fail("SharingKeys", err)
	}
	info("the account has %d sharing key(s)", len(keys))
	for _, r := range keys {
		r.Key.Close()
	}

	stage("List the key's objects as its owner")
	seed := key.Export()
	recipient := siastorage.ImportSharingKey(seed)
	defer recipient.Close()
	if recipient.PublicKey() != key.PublicKey() {
		fail("ImportSharingKey", fmt.Errorf("the reimported key differs"))
	}
	shared, err := sdk.SharedObjects(ctx, recipient, 0, 0)
	if err != nil {
		fail("SharedObjects", err)
	}
	defer func() {
		for _, o := range shared {
			o.Close()
		}
	}()
	if len(shared) == 0 {
		fail("SharedObjects", fmt.Errorf("the key lists nothing"))
	}
	info("the owner sees %d object(s) on the key", len(shared))

	// --------------------------------------------------------- the recipient
	// Everything below uses the seed alone. No app key, no account, no
	// approval: another process handed 32 bytes could do exactly this.
	stage("Connect as the recipient, holding only the seed")
	shard, err := connectShared(ctx, seed)
	if err != nil {
		fail("ConnectShared", err)
	}
	defer shard.Close()
	info("connected with no account of its own")

	stats, err := shard.Stats(ctx)
	if err != nil {
		fail("SharedSDK.Stats", err)
	}
	info("the key grants %d object(s), %s pinned", stats.ObjectCount, bytes4(stats.PinnedSize))

	theirs, err := shard.Objects(ctx, 0, 0)
	if err != nil {
		fail("SharedSDK.Objects", err)
	}
	defer func() {
		for _, o := range theirs {
			o.Close()
		}
	}()
	if len(theirs) == 0 {
		fail("SharedSDK.Objects", fmt.Errorf("the recipient lists nothing"))
	}
	info("the recipient lists %d object(s)", len(theirs))

	byID, err := shard.Object(ctx, theirs[0].ID())
	if err != nil {
		fail("SharedSDK.Object", err)
	}
	defer byID.Close()
	info("fetched %v by id, %s", byID.ID(), bytes4(byID.Size()))

	theirHosts, err := shard.Hosts(ctx, siastorage.HostQuery{Limit: 5})
	if err != nil {
		fail("SharedSDK.Hosts", err)
	}
	info("%d host(s) serve the key's objects", len(theirHosts))

	stage("Download as the recipient, then close the SDK mid transfer")
	dl, err := shard.Download(ctx, byID, siastorage.DownloadOptions{})
	if err != nil {
		fail("SharedSDK.Download", err)
	}
	// The download keeps its own token refresh alive, so dropping the handle
	// it came from must not break it.
	if err := shard.Close(); err != nil {
		fail("SharedSDK.Close", err)
	}
	viaKey, err := io.ReadAll(dl)
	dl.Close()
	if err != nil {
		fail("recipient download", err)
	}
	// The key holds the object as it was when attached, the original upload
	// rather than the overwritten copy.
	if !bytes.Equal(viaKey, want) {
		fail("verify recipient download", fmt.Errorf("the recipient read %d bytes and they differ", len(viaKey)))
	}
	info("%s read and verified after the SharedSDK was closed", bytes4(uint64(len(viaKey))))

	// -------------------------------------------------------- packed uploads
	stage("Pack %d objects of %s into shared slabs", packedCount, bytes4(packedSize))
	packed, err := sdk.PackedUpload(ctx, siastorage.UploadOptions{})
	if err != nil {
		fail("PackedUpload", err)
	}
	defer packed.Close()
	info("a full slab holds %s", bytes4(packed.OptimalDataSize()))
	small := make([][]byte, packedCount)
	for i := range small {
		small[i] = payload(packedSize)
		if _, err := packed.Add(bytes.NewReader(small[i])); err != nil {
			fail("PackedUpload.Add", err)
		}
	}
	info("%s packed, %s still free in the slab", bytes4(packed.Length()), bytes4(packed.Remaining()))
	packedObjs, err := packed.Finalize()
	if err != nil {
		fail("PackedUpload.Finalize", err)
	}
	defer func() {
		for _, o := range packedObjs {
			o.Close()
		}
	}()
	info("%d objects came back", len(packedObjs))
	// As with a plain upload, finalizing pins the slabs but not the object
	// records, so each still has to be pinned to exist at the indexer.
	for i, o := range packedObjs {
		if err := sdk.PinObject(ctx, o); err != nil {
			fail(fmt.Sprintf("PinObject packed %d", i), err)
		}
	}
	info("each pinned, so the indexer can find them")
	if len(packedObjs) > 0 {
		first, err := download(ctx, sdk, packedObjs[0], siastorage.DownloadOptions{})
		if err != nil {
			fail("packed download", err)
		}
		if !bytes.Equal(first, small[0]) {
			fail("verify packed", fmt.Errorf("the first packed object did not round trip"))
		}
		info("the first one verified against its source")
	}

	// ---------------------------------------------------------------- events
	stage("Page through the object event feed")
	var cursor siastorage.EventCursor
	var seen int
	for page := 0; page < 10; page++ {
		events, err := sdk.ObjectEvents(ctx, cursor, 50)
		if err != nil {
			fail("ObjectEvents", err)
		}
		if len(events) == 0 {
			siastorage.CloseObjects(events)
			break
		}
		for _, e := range events {
			seen++
			if e.Deleted {
				info("deleted %v at %s", e.ID, e.UpdatedAt.Format(time.RFC3339))
			}
		}
		cursor = events[len(events)-1].Cursor()
		siastorage.CloseObjects(events)
	}
	info("%d event(s) read, the feed is caught up", seen)

	// --------------------------------------------------------------- cleanup
	stage("Detach, revoke, delete and prune")
	if err := sdk.UnshareObject(ctx, key, obj.ID()); err != nil {
		fail("UnshareObject", err)
	}
	if err := sdk.RevokeSharingKey(ctx, key); err != nil {
		fail("RevokeSharingKey", err)
	}
	info("object detached and the key revoked")

	deleted := 0
	for _, o := range append([]*siastorage.Object{obj, overwritten, short}, packedObjs...) {
		if err := sdk.DeleteObject(ctx, o.ID()); err != nil {
			fail("DeleteObject", err)
		}
		deleted++
	}
	if err := sdk.PruneSlabs(ctx); err != nil {
		fail("PruneSlabs", err)
	}
	info("%d object(s) deleted and their slabs pruned", deleted)

	if _, err := sdk.Object(ctx, obj.ID()); err == nil {
		fail("cleanup", fmt.Errorf("the object is still fetchable after deletion"))
	}
	info("the object is gone from the indexer")

	report(sdk)
}

// upload streams data in through io.Copy and returns the finished object.
func upload(ctx context.Context, sdk *siastorage.SDK, data []byte, opts siastorage.UploadOptions) (*siastorage.Object, error) {
	up, err := sdk.Upload(ctx, siastorage.NewObject(), opts)
	if err != nil {
		return nil, err
	}
	defer up.Close()

	if _, err := io.Copy(up, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	obj, err := up.Finish()
	if err != nil {
		return nil, err
	}
	if dropped := up.Dropped(); dropped > 0 {
		info("%d progress events were dropped, the handler fell behind", dropped)
	}
	return obj, nil
}

// uploadInto streams data into an existing object, which is what an overwrite
// needs: the object carries the slabs the range is rewritten against.
func uploadInto(ctx context.Context, sdk *siastorage.SDK, obj *siastorage.Object, data []byte, opts siastorage.UploadOptions) (*siastorage.Object, error) {
	up, err := sdk.Upload(ctx, obj, opts)
	if err != nil {
		return nil, err
	}
	defer up.Close()

	if _, err := io.Copy(up, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return up.Finish()
}

// download reads an object out through io.ReadAll.
func download(ctx context.Context, sdk *siastorage.SDK, obj *siastorage.Object, opts siastorage.DownloadOptions) ([]byte, error) {
	dl, err := sdk.Download(ctx, obj, opts)
	if err != nil {
		return nil, err
	}
	defer dl.Close()
	return io.ReadAll(dl)
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// bytes4 formats a byte count at a readable scale.
func bytes4(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
