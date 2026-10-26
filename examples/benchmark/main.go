// Command benchmark measures upload and download throughput through the C ABI.
//
// A profile binds an app key to an indexer, so only the first run needs the
// approval flow. The config file is shared with the Rust benchmark in
// sia-sdk-rs, so a profile made by either tool works in the other.
//
//	go run ./examples/benchmark login
//	go run ./examples/benchmark run -n 5
//	go run -tags siastorage_mock ./examples/benchmark run -n 3
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"os"
	"runtime"
	"time"

	"go.sia.tech/siastorage"
)

// betweenReps runs after each rep when the build provides one. The mock
// network keeps every sector it is ever written, so without a hook to drop
// them the harness outgrows what it is measuring.
var betweenReps func()

// A Sample is one upload and download of the same payload.
type Sample struct {
	Rep          int   `json:"rep"`
	PayloadBytes int64 `json:"payloadBytes"`

	UploadNS   int64 `json:"uploadNs"`
	TTFBNS     int64 `json:"ttfbNs"`
	DownloadNS int64 `json:"downloadNs"`

	EncodedBytes int64  `json:"encodedBytes"`
	PeakRSSBytes int64  `json:"peakRssBytes"`
	GoHeapBytes  int64  `json:"goHeapBytes"`
	Verified     bool   `json:"verified"`
	ObjectID     string `json:"objectID"`
}

const (
	defaultIndexer = "https://sia.storage"
	defaultProfile = "default"
)

func usage() {
	fmt.Fprintln(os.Stderr, `benchmark measures Sia uploads and downloads

Usage:
  benchmark login    [-profile NAME] [-indexer URL] [-new]
  benchmark run      [-profile NAME] [-size BYTES] [-runs N] [-keep]
                     [-upload-max-buffered-slabs N]
                     [-download-max-buffered-chunks N] [-hosts N] [-v]
  benchmark profiles

A profile binds an app key to an indexer so a run needs no approval flow. The
config is shared with the Rust benchmark, so a profile made by either tool
works in the other.`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()

	switch os.Args[1] {
	case "login":
		fs := flag.NewFlagSet("login", flag.ExitOnError)
		profileName := fs.String("profile", defaultProfile, "profile to store the app key under")
		indexer := fs.String("indexer", defaultIndexer, "indexer URL to authorize against")
		newPhrase := fs.Bool("new", false, "generate a recovery phrase instead of prompting for one")
		_ = fs.Parse(os.Args[2:])
		if err := login(ctx, *profileName, *indexer, *newPhrase); err != nil {
			die("login: %v", err)
		}

	case "profiles":
		if err := listProfiles(); err != nil {
			die("profiles: %v", err)
		}

	case "run":
		fs := flag.NewFlagSet("run", flag.ExitOnError)
		profileName := fs.String("profile", defaultProfile, "profile to run against")
		// Names and defaults follow the Rust benchmark so the same command
		// drives either tool. -keep, -hosts and -v have no counterpart there.
		size := fs.Int64("size", 120*1024*1024, "size of the data to upload and download in bytes")
		fs.Int64Var(size, "s", 120*1024*1024, "shorthand for -size")
		runs := fs.Int("runs", 1, "number of times to run the benchmark")
		fs.IntVar(runs, "n", 1, "shorthand for -runs")
		slabs := fs.Uint64("upload-max-buffered-slabs", 0, "maximum number of slabs buffered in memory during upload; 0 uses the SDK default")
		chunks := fs.Uint64("download-max-buffered-chunks", 0, "maximum number of chunks buffered in memory during download; 0 uses the SDK default")
		keep := fs.Bool("keep", false, "leave the uploaded objects on the network")
		hosts := fs.Int("hosts", 90, "mock host pool size; ignored against a real indexer")
		verbose := fs.Bool("v", false, "log the engine's own decisions, including the inflight limit")
		_ = fs.Parse(os.Args[2:])

		if *verbose {
			// Debug records from the Rust side, stderr so stdout stays samples.
			cfg := zap.NewDevelopmentEncoderConfig()
			siastorage.SetLogger(zap.New(zapcore.NewCore(
				zapcore.NewConsoleEncoder(cfg), zapcore.Lock(os.Stderr), zap.DebugLevel)))
		}
		sdk, err := connect(ctx, *profileName, *hosts)
		if err != nil {
			die("connect: %v", err)
		}
		defer sdk.Close()

		// One payload for every rep, so the reps are not measuring different
		// data. Generated once because generating 120 MiB is not free either.
		payload := makePayload(*size)
		want := sha256.Sum256(payload)

		enc := json.NewEncoder(os.Stdout)
		for rep := 1; rep <= *runs; rep++ {
			s, err := runOnce(ctx, sdk, payload, want, rep, *keep, *slabs, *chunks)
			if err != nil {
				die("rep %d: %v", rep, err)
			}
			if err := enc.Encode(s); err != nil {
				die("encode: %v", err)
			}
			if betweenReps != nil {
				betweenReps()
			}
		}

	default:
		usage()
		os.Exit(2)
	}
}

func runOnce(ctx context.Context, sdk *siastorage.SDK, payload []byte, want [32]byte, rep int, keep bool, bufferedSlabs, bufferedChunks uint64) (Sample, error) {
	s := Sample{Rep: rep, PayloadBytes: int64(len(payload))}

	// ---- upload, measured from the first byte offered to the object returned
	start := time.Now()
	obj, err := sdk.Upload(ctx, siastorage.NewEmptyObject(), bytes.NewReader(payload),
		siastorage.WithUploadMaxBufferedSlabs(int(bufferedSlabs)))
	if err != nil {
		return s, fmt.Errorf("upload: %w", err)
	}
	s.UploadNS = time.Since(start).Nanoseconds()

	s.EncodedBytes = int64(obj.EncodedSize())
	s.ObjectID = obj.ID().String()

	if err := sdk.PinObject(ctx, obj); err != nil {
		return s, fmt.Errorf("pin: %w", err)
	}

	// ---- download, with time to first byte split out, since a decentralised
	// read pays a recovery cost before any data moves
	start = time.Now()
	dl, err := sdk.Download(ctx, obj, siastorage.WithDownloadMaxBufferedChunks(int(bufferedChunks)))
	if err != nil {
		return s, fmt.Errorf("download start: %w", err)
	}
	defer dl.Close()

	// Read and discard. Accumulating the payload to hash it would put a 256 MB
	// copy inside the timed region, and at these rates that copy is a large
	// share of the measurement rather than a rounding error.
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
		// wrote, which reads as a leak. The cutoff has to be the present, since
		// the slabs being released were pinned moments ago.
		if err := sdk.PruneSlabs(ctx, time.Now()); err != nil {
			return s, fmt.Errorf("prune: %w", err)
		}
	}
	return s, nil
}

// verify re-reads the object and checks it against the payload hash. It runs
// outside the timed region, so correctness costs nothing in the numbers.
func verify(ctx context.Context, sdk *siastorage.SDK, obj *siastorage.Object, want [32]byte) (bool, error) {
	dl, err := sdk.Download(ctx, obj)
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

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
