// Command benchmark measures upload and download throughput through the C ABI,
// so the numbers can be compared against the native Go engine it replaces.
//
// The matching harness for the native engine lives in examples/benchmark-native
// and prints the same JSON, so a run of each can be merged and compared.
//
//	go run -tags siastorage_mock ./examples/benchmark -reps 3
//	go run ./examples/benchmark -indexer https://sia.storage -app-key <hex> -reps 5
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/siastorage"
)

// A Sample is one upload and download of the same payload. The native harness
// emits the same shape, so the two runs merge into one table.
type Sample struct {
	Engine       string `json:"engine"`
	Rep          int    `json:"rep"`
	PayloadBytes int64  `json:"payloadBytes"`

	UploadNS   int64 `json:"uploadNs"`
	TTFBNS     int64 `json:"ttfbNs"`
	DownloadNS int64 `json:"downloadNs"`

	EncodedBytes int64  `json:"encodedBytes"`
	PeakRSSBytes int64  `json:"peakRssBytes"`
	GoHeapBytes  int64  `json:"goHeapBytes"`
	Verified     bool   `json:"verified"`
	ObjectID     string `json:"objectID"`
}

func main() {
	var (
		indexer = flag.String("indexer", "", "indexer URL; empty runs against the mock")
		appKey  = flag.String("app-key", "", "hex app key to connect with")
		size    = flag.Int64("size", 256*1000*1000, "payload size in bytes")
		reps    = flag.Int("reps", 3, "how many upload and download cycles to run")
		keep    = flag.Bool("keep", false, "leave the uploaded objects on the network")
		hosts   = flag.Int("hosts", 90, "mock host pool size; ignored against a real indexer")
		slabs   = flag.Uint64("buffered-slabs", 0, "max buffered slabs; 0 uses the SDK default")
		chunks  = flag.Uint64("buffered-chunks", 0, "max buffered download chunks; 0 uses the SDK default")
	)
	flag.Parse()

	ctx := context.Background()
	sdk, cleanup, err := connect(ctx, *indexer, *appKey, *hosts)
	if err != nil {
		die("connect: %v", err)
	}
	defer cleanup()
	defer sdk.Close()

	// One payload for every rep, so the comparison is not measuring different
	// data. Generated once because generating 120 MiB is not free either.
	payload := makePayload(*size)
	want := sha256.Sum256(payload)

	enc := json.NewEncoder(os.Stdout)
	for rep := 1; rep <= *reps; rep++ {
		s, err := runOnce(ctx, sdk, payload, want, rep, *keep, *slabs, *chunks)
		if err != nil {
			die("rep %d: %v", rep, err)
		}
		if err := enc.Encode(s); err != nil {
			die("encode: %v", err)
		}
	}
}

func runOnce(ctx context.Context, sdk *siastorage.SDK, payload []byte, want [32]byte, rep int, keep bool, bufferedSlabs, bufferedChunks uint64) (Sample, error) {
	s := Sample{Engine: engineName, Rep: rep, PayloadBytes: int64(len(payload))}

	// ---- upload, measured from the first byte offered to the object returned
	start := time.Now()
	up, err := sdk.Upload(ctx, siastorage.NewObject(), siastorage.UploadOptions{MaxBufferedSlabs: bufferedSlabs})
	if err != nil {
		return s, fmt.Errorf("upload start: %w", err)
	}
	// 1 MiB, not io.Copy's 32 KiB default. This API is writer shaped where the
	// native engine's is reader shaped, so the copy loop is the harness rather
	// than the engine, and at 32 KiB it crosses the boundary 8192 times per
	// 256 MB and costs about 10% of upload throughput. 1 MiB also matches the
	// read size on the download side, so both directions are measured at the
	// same granularity.
	if _, err := io.CopyBuffer(up, bytes.NewReader(payload), make([]byte, 1<<20)); err != nil {
		up.Close()
		return s, fmt.Errorf("upload write: %w", err)
	}
	obj, err := up.Finish()
	up.Close()
	if err != nil {
		return s, fmt.Errorf("upload finish: %w", err)
	}
	s.UploadNS = time.Since(start).Nanoseconds()
	defer obj.Close()

	s.EncodedBytes = int64(obj.EncodedSize())
	s.ObjectID = obj.ID().String()

	if err := sdk.PinObject(ctx, obj); err != nil {
		return s, fmt.Errorf("pin: %w", err)
	}

	// ---- download, with time to first byte split out, since a decentralised
	// read pays a recovery cost before any data moves
	start = time.Now()
	dl, err := sdk.Download(ctx, obj, siastorage.DownloadOptions{MaxBufferedChunks: bufferedChunks})
	if err != nil {
		return s, fmt.Errorf("download start: %w", err)
	}
	defer dl.Close()

	// Read and discard, which is what the native engine's own benchmark does.
	// Accumulating the payload to hash it would put a 256 MB copy inside the
	// timed region, and at these rates that copy is a large share of the
	// measurement rather than a rounding error.
	buf := make([]byte, 1<<20)
	first := true
	for {
		n, rerr := dl.Read(buf)
		if n > 0 && first {
			s.TTFBNS = time.Since(start).Nanoseconds()
			first = false
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return s, fmt.Errorf("download read: %w", rerr)
		}
	}
	s.DownloadNS = time.Since(start).Nanoseconds()

	// Correctness is checked on a second pass, outside the timer, so the
	// benchmark never reports bytes it did not confirm.
	s.Verified, err = verify(ctx, sdk, obj, want)
	if err != nil {
		return s, fmt.Errorf("verify: %w", err)
	}

	s.PeakRSSBytes = peakRSS()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.GoHeapBytes = int64(ms.HeapAlloc)

	if !keep {
		if err := sdk.DeleteObject(ctx, obj.ID()); err != nil {
			return s, fmt.Errorf("delete: %w", err)
		}
		// Deleting the object only removes the record. Pruning is what releases
		// the slabs, and without it a long run accumulates every object it ever
		// wrote, which reads as a leak.
		if err := sdk.PruneSlabs(ctx); err != nil {
			return s, fmt.Errorf("prune: %w", err)
		}
	}
	return s, nil
}

// verify re-reads the object and checks it against the payload hash. It runs
// outside the timed region, so correctness costs nothing in the numbers.
func verify(ctx context.Context, sdk *siastorage.SDK, obj *siastorage.Object, want [32]byte) (bool, error) {
	dl, err := sdk.Download(ctx, obj, siastorage.DownloadOptions{})
	if err != nil {
		return false, err
	}
	defer dl.Close()
	h := sha256.New()
	if _, err := io.Copy(h, dl); err != nil {
		return false, err
	}
	return [32]byte(h.Sum(nil)) == want, nil
}

func makePayload(n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func parseAppKey(s string) (types.PrivateKey, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode app key: %w", err)
	}
	if len(raw) < 32 {
		return nil, fmt.Errorf("an app key is at least 32 bytes, got %d", len(raw))
	}
	return types.PrivateKey(raw), nil
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
