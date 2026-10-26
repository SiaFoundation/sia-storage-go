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

	"github.com/vbauerster/mpb/v8"
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

	MaxGapNS     int64  `json:"maxGapNs"`
	EncodedBytes int64  `json:"encodedBytes"`
	PeakRSSBytes int64  `json:"peakRssBytes"`
	GoHeapBytes  int64  `json:"goHeapBytes"`
	Verified     bool   `json:"verified"`
	ObjectID     string `json:"objectID"`
}

const (
	defaultIndexer = "https://sia.storage"
	defaultProfile = "default"

	// The SDK's default redundancy, used only to map encoded bytes back to an
	// approximate position in the payload for the upload bar, and to size the
	// warm up at a full slab.
	dataShards  = 10
	totalShards = 30
	slabBytes   = dataShards * (4 << 20)
)

func usage() {
	fmt.Fprintln(os.Stderr, `benchmark measures Sia uploads and downloads

Usage:
  benchmark login    [-profile NAME] [-indexer URL] [-new]
  benchmark run      [-profile NAME] [-size BYTES] [-runs N] [-warm] [-keep]
                     [-upload-max-buffered-slabs N] [-host-summary]
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
		hostSummary := fs.Bool("host-summary", false, "print a per-host breakdown of shards and throughput after each run")
		warm := fs.Bool("warm", false, "transfer a smaller object first to seed the pipeline's metrics")
		fs.BoolVar(warm, "w", false, "shorthand for -warm")
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

		opts := runOpts{
			bufferedSlabs:  *slabs,
			bufferedChunks: *chunks,
			keep:           *keep,
			hostSummary:    *hostSummary,
		}
		// Bars go to stderr so stdout stays a stream of samples. Suppressed
		// under -v, where the engine's own records would scribble over them.
		if !*verbose {
			bars := mpb.New(mpb.WithOutput(os.Stderr))
			opts.bars = bars
			defer bars.Wait()
		}

		if *warm {
			// A tenth of the payload, but never less than a full slab, which
			// is the smallest upload that exercises the whole pipeline.
			warmSize := max(*size/10, int64(slabBytes))
			warmPayload := makePayload(warmSize)
			warmOpts := opts
			warmOpts.hostSummary = false
			if _, err := runOnce(ctx, sdk, warmPayload, sha256.Sum256(warmPayload), 0, warmOpts); err != nil {
				die("warm up: %v", err)
			}
			if betweenReps != nil {
				betweenReps()
			}
		}

		enc := json.NewEncoder(os.Stdout)
		samples := make([]Sample, 0, *runs)
		for rep := 1; rep <= *runs; rep++ {
			s, err := runOnce(ctx, sdk, payload, want, rep, opts)
			if err != nil {
				die("run %d: %v", rep, err)
			}
			if err := enc.Encode(s); err != nil {
				die("encode: %v", err)
			}
			samples = append(samples, s)
			if betweenReps != nil {
				betweenReps()
			}
		}
		printRunSummary(samples)

	default:
		usage()
		os.Exit(2)
	}
}

// runOpts carries what a run needs beyond the payload itself.
type runOpts struct {
	bufferedSlabs  uint64
	bufferedChunks uint64
	keep           bool
	hostSummary    bool
	bars           *mpb.Progress // nil draws nothing
}

func runOnce(ctx context.Context, sdk *siastorage.SDK, payload []byte, want [32]byte, rep int, opts runOpts) (Sample, error) {
	s := Sample{Rep: rep, PayloadBytes: int64(len(payload))}

	var upHosts, downHosts *hostStats
	if opts.hostSummary {
		upHosts, downHosts = newHostStats(), newHostStats()
	}

	// ---- upload, measured from the first byte offered to the object returned
	start := time.Now()
	uploadOpts := []siastorage.UploadOption{
		siastorage.WithUploadMaxBufferedSlabs(int(opts.bufferedSlabs)),
	}
	var uploadBar *mpb.Bar
	if opts.bars != nil {
		uploadBar = newTransferBar(opts.bars, "upload", uint64(len(payload)), start)
	}
	if upHosts != nil || uploadBar != nil {
		uploadOpts = append(uploadOpts, siastorage.WithUploadProgress(func(p siastorage.ShardProgress) {
			if upHosts != nil {
				upHosts.record(p.HostKey.String(), p.ShardSize, p.Elapsed)
			}
			if uploadBar != nil {
				// Transferred counts encoded bytes, so scale it back to a
				// position in the payload the bar is measuring.
				uploadBar.SetCurrent(min(int64(p.Transferred)*dataShards/totalShards, int64(len(payload))))
			}
		}))
	}
	obj, err := sdk.Upload(ctx, siastorage.NewEmptyObject(), bytes.NewReader(payload), uploadOpts...)
	if err != nil {
		if uploadBar != nil {
			uploadBar.Abort(false)
		}
		return s, fmt.Errorf("upload: %w", err)
	}
	s.UploadNS = time.Since(start).Nanoseconds()
	if uploadBar != nil {
		uploadBar.SetCurrent(int64(len(payload)))
	}

	s.EncodedBytes = int64(obj.EncodedSize())
	s.ObjectID = obj.ID().String()

	if err := sdk.PinObject(ctx, obj); err != nil {
		return s, fmt.Errorf("pin: %w", err)
	}

	// ---- download, with time to first byte split out, since a decentralised
	// read pays a recovery cost before any data moves
	start = time.Now()
	downloadOpts := []siastorage.DownloadOption{
		siastorage.WithDownloadMaxBufferedChunks(int(opts.bufferedChunks)),
	}
	if downHosts != nil {
		downloadOpts = append(downloadOpts, siastorage.WithDownloadProgress(func(p siastorage.ShardProgress) {
			downHosts.record(p.HostKey.String(), p.ShardSize, p.Elapsed)
		}))
	}
	var downloadBar *mpb.Bar
	if opts.bars != nil {
		downloadBar = newTransferBar(opts.bars, "download", uint64(len(payload)), start)
	}
	dl, err := sdk.Download(ctx, obj, downloadOpts...)
	if err != nil {
		if downloadBar != nil {
			downloadBar.Abort(false)
		}
		return s, fmt.Errorf("download start: %w", err)
	}
	defer dl.Close()

	// Read and discard. Accumulating the payload to hash it would put a 256 MB
	// copy inside the timed region, and at these rates that copy is a large
	// share of the measurement rather than a rounding error.
	buf := make([]byte, 1<<20)
	first := true
	// The largest gap between successive reads, which is what a stall looks
	// like to a caller streaming the object.
	var prev time.Duration
	var maxGap time.Duration
	for {
		n, rerr := dl.Read(buf)
		elapsed := time.Since(start)
		if n > 0 {
			if first {
				s.TTFBNS = elapsed.Nanoseconds()
				first = false
			} else if gap := elapsed - prev; gap > maxGap {
				maxGap = gap
			}
			prev = elapsed
			if downloadBar != nil {
				downloadBar.IncrBy(n)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			if downloadBar != nil {
				downloadBar.Abort(false)
			}
			return s, fmt.Errorf("download read: %w", rerr)
		}
	}
	s.DownloadNS = time.Since(start).Nanoseconds()
	s.MaxGapNS = maxGap.Nanoseconds()

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

	if upHosts != nil {
		upHosts.printSummary("Upload")
		downHosts.printSummary("Download")
	}

	if !opts.keep {
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
