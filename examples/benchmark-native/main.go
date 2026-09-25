// Command benchmark-native is the control arm: the same measurement loop as
// examples/benchmark, run against the native Go engine this module replaces.
//
// It is a module of its own because both engines publish the same import path,
// so they cannot be linked into one binary. Build it with the workspace off,
// or the require below resolves to the cgo engine sitting beside it:
//
//	GOWORK=off go run ./examples/benchmark-native -indexer https://sia.storage -app-key <hex> -reps 5
//
// It prints the same JSON as the cgo arm, so a run of each merges into one
// table. There is no mock arm here, because the native engine's mock is test
// only and cannot be reached from a program.
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
	"syscall"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/siastorage"
)

// Sample matches the cgo arm's shape field for field.
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

var benchApp = siastorage.AppMetadata{
	ID:          mustAppID("3f9c2a815d47e6b09c8a1f2e3d4b5a6978869504132e7f8a9b0c1d2e3f405162"),
	Name:        "siastorage benchmark",
	Description: "Measures the native Go engine against the C ABI",
	ServiceURL:  "https://sia.tech",
}

func main() {
	var (
		indexer = flag.String("indexer", "", "indexer URL")
		appKey  = flag.String("app-key", "", "hex app key to connect with")
		size    = flag.Int64("size", 120<<20, "payload size in bytes")
		reps    = flag.Int("reps", 3, "how many upload and download cycles to run")
		keep    = flag.Bool("keep", false, "leave the uploaded objects on the network")
	)
	flag.Parse()

	if *indexer == "" || *appKey == "" {
		die("both -indexer and -app-key are required")
	}
	key, err := parseAppKey(*appKey)
	if err != nil {
		die("%v", err)
	}

	ctx := context.Background()
	sdk, err := siastorage.NewBuilder(*indexer, benchApp).SDK(key)
	if err != nil {
		die("connect: %v", err)
	}
	defer sdk.Close()

	payload := makePayload(*size)
	want := sha256.Sum256(payload)

	enc := json.NewEncoder(os.Stdout)
	for rep := 1; rep <= *reps; rep++ {
		s, err := runOnce(ctx, sdk, payload, want, rep, *keep)
		if err != nil {
			die("rep %d: %v", rep, err)
		}
		if err := enc.Encode(s); err != nil {
			die("encode: %v", err)
		}
	}
}

func runOnce(ctx context.Context, sdk *siastorage.SDK, payload []byte, want [32]byte, rep int, keep bool) (Sample, error) {
	s := Sample{Engine: "native", Rep: rep, PayloadBytes: int64(len(payload))}

	obj := siastorage.NewEmptyObject()

	start := time.Now()
	if err := sdk.Upload(ctx, &obj, bytes.NewReader(payload)); err != nil {
		return s, fmt.Errorf("upload: %w", err)
	}
	s.UploadNS = time.Since(start).Nanoseconds()
	s.ObjectID = obj.ID().String()
	s.EncodedBytes = encodedSize(&obj)

	if err := sdk.PinObject(ctx, obj); err != nil {
		return s, fmt.Errorf("pin: %w", err)
	}

	start = time.Now()
	dl, err := sdk.Download(obj)
	if err != nil {
		return s, fmt.Errorf("download start: %w", err)
	}
	defer dl.Close()

	got := make([]byte, 0, len(payload))
	buf := make([]byte, 1<<20)
	first := true
	for {
		n, rerr := dl.Read(buf)
		if n > 0 && first {
			s.TTFBNS = time.Since(start).Nanoseconds()
			first = false
		}
		got = append(got, buf[:n]...)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return s, fmt.Errorf("download read: %w", rerr)
		}
	}
	s.DownloadNS = time.Since(start).Nanoseconds()
	s.Verified = sha256.Sum256(got) == want

	s.PeakRSSBytes = peakRSS()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.GoHeapBytes = int64(ms.HeapAlloc)

	if !keep {
		if err := sdk.DeleteObject(ctx, obj.ID()); err != nil {
			return s, fmt.Errorf("delete: %w", err)
		}
	}
	return s, nil
}

// encodedSize sums what the slabs actually occupy, which the cgo arm reads
// straight off the object.
func encodedSize(obj *siastorage.Object) int64 {
	var total int64
	for _, s := range obj.Slabs() {
		total += int64(len(s.Sectors)) * (1 << 22)
	}
	return total
}

func peakRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return int64(ru.Maxrss)
	}
	return int64(ru.Maxrss) * 1024
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
	if len(raw) != 32 {
		return nil, fmt.Errorf("an app key seed is 32 bytes, got %d", len(raw))
	}
	// The same seed the cgo demo prints. The native engine wants the derived
	// ed25519 key, which is twice as long.
	return types.NewPrivateKeyFromSeed(raw), nil
}

func mustAppID(s string) types.Hash256 {
	var id types.Hash256
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != len(id) {
		panic("bad app id literal")
	}
	copy(id[:], raw)
	return id
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
