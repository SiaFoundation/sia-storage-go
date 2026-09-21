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

	// ---------------------------------------------------------------- sealed
	stage("Seal the object to JSON and open it again")
	sealed, err := sdk.SealObject(obj)
	if err != nil {
		fail("SealObject", err)
	}
	opened, err := sdk.ObjectFromSealed(sealed)
	if err != nil {
		fail("ObjectFromSealed", err)
	}
	defer opened.Close()
	info("%d bytes of sealed JSON, reopened to the same ID %t", len(sealed), opened.ID() == obj.ID())

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

	stage("Read the object as a recipient holding only the seed")
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
	viaKey, err := download(ctx, sdk, shared[0], siastorage.DownloadOptions{})
	if err != nil {
		fail("recipient download", err)
	}
	info("the recipient read %s from a 32 byte seed alone", bytes4(uint64(len(viaKey))))

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
	var cursor *siastorage.EventCursor
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
		next := events[len(events)-1].Cursor()
		cursor = &next
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
	for _, o := range append([]*siastorage.Object{obj}, packedObjs...) {
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
