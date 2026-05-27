package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand/v2"
	"os"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	siastorage "go.sia.tech/siastorage"
)

const appIDHex = "5c0b1af28e6ac76395b2087ea987297b9c496f90d2ab3e3d3d07980ae4c43633"

type seededReader struct {
	src       *mrand.ChaCha8
	remaining uint64
}

func newSeededReader(seed [32]byte, size uint64) *seededReader {
	return &seededReader{src: mrand.NewChaCha8(seed), remaining: size}
}

func (r *seededReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if uint64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, _ := r.src.Read(p)
	r.remaining -= uint64(n)
	return n, nil
}

type seededVerifier struct {
	src       *mrand.ChaCha8
	size      uint64
	remaining uint64
	buf       []byte
	start     time.Time
	ttfb      time.Duration
	hasTTFB   bool
	prev      time.Duration
	hasPrev   bool
	gapMax    time.Duration
}

func newSeededVerifier(seed [32]byte, size uint64) *seededVerifier {
	return &seededVerifier{
		src:       mrand.NewChaCha8(seed),
		size:      size,
		remaining: size,
		start:     time.Now(),
	}
}

func (v *seededVerifier) Write(p []byte) (int, error) {
	now := time.Now()
	if !v.hasTTFB {
		v.ttfb = now.Sub(v.start)
		v.hasTTFB = true
	}
	elapsed := now.Sub(v.start)
	if v.hasPrev {
		if gap := elapsed - v.prev; gap > v.gapMax {
			v.gapMax = gap
		}
	}
	v.prev = elapsed
	v.hasPrev = true

	if uint64(len(p)) > v.remaining {
		return 0, fmt.Errorf("expected %d more bytes, got %d", v.remaining, len(p))
	}
	if cap(v.buf) < len(p) {
		v.buf = make([]byte, len(p))
	}
	expected := v.buf[:len(p)]
	_, _ = v.src.Read(expected)
	if !bytes.Equal(expected, p) {
		return 0, fmt.Errorf("data mismatch at byte %d", v.size-v.remaining)
	}
	v.remaining -= uint64(len(p))
	return len(p), nil
}

func formatBytes(b uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	v := float64(b)
	for i, u := range units {
		if v < 1024.0 || i == len(units)-1 {
			return fmt.Sprintf("%.2f %s", v, u)
		}
		v /= 1024.0
	}
	panic("unreachable")
}

func formatBitrate(b uint64, d time.Duration) string {
	bps := float64(b) * 8.0 / d.Seconds()
	units := []string{"bps", "Kbps", "Mbps", "Gbps", "Tbps"}
	v := bps
	for i, u := range units {
		if v < 1000.0 || i == len(units)-1 {
			return fmt.Sprintf("%.2f %s", v, u)
		}
		v /= 1000.0
	}
	panic("unreachable")
}

func encodedSize(obj siastorage.Object) uint64 {
	var n uint64
	for _, s := range obj.Slabs() {
		n += uint64(len(s.Sectors)) * proto4.SectorSize
	}
	return n
}

func main() {
	size := flag.Uint64("size", 120*1024*1024, "size of the data to upload and download in bytes")
	uploadMaxInflight := flag.Int("upload-max-inflight", 0, "maximum number of concurrent shard uploads (0 = SDK default)")
	downloadMaxInflight := flag.Int("download-max-inflight", 0, "maximum number of concurrent chunk downloads (0 = SDK default)")
	flag.Parse()

	var appID types.Hash256
	if err := appID.UnmarshalText([]byte(appIDHex)); err != nil {
		log.Fatal("failed to parse app ID:", err)
	}

	builder := siastorage.NewBuilder("https://sia.storage", siastorage.AppMetadata{
		ID:          appID,
		Name:        "Benchmark Example",
		Description: "Benchmarks upload and download performance of the siastorage SDK",
		ServiceURL:  "https://myexampleapp.com",
	})

	ctx := context.Background()

	responseURL, err := builder.RequestConnection(ctx)
	if err != nil {
		log.Fatal("failed to request connection:", err)
	}
	fmt.Println("Visit the following URL to authorize the application:", responseURL)

	if err := builder.WaitForApproval(ctx); err != nil {
		log.Fatal("failed to wait for approval:", err)
	}
	fmt.Println("Connection approved!")

	fmt.Println("Enter recovery phrase:")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		log.Fatal("failed to read recovery phrase")
	}
	phrase := scanner.Text()

	sdk, err := builder.Register(ctx, phrase)
	if err != nil {
		log.Fatal("failed to register app:", err)
	}
	defer sdk.Close()
	fmt.Println("App registered successfully!")

	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		log.Fatal("failed to generate seed:", err)
	}

	var uploadOpts []siastorage.UploadOption
	if *uploadMaxInflight > 0 {
		uploadOpts = append(uploadOpts, siastorage.WithUploadInflight(*uploadMaxInflight))
	}

	fmt.Println("Uploading random data...")
	obj := siastorage.NewEmptyObject()
	uploadStart := time.Now()
	if err := sdk.Upload(ctx, &obj, newSeededReader(seed, *size), uploadOpts...); err != nil {
		log.Fatal("failed to upload object:", err)
	}
	uploadDuration := time.Since(uploadStart)

	if err := sdk.PinObject(ctx, obj); err != nil {
		log.Fatal("failed to pin object:", err)
	}
	fmt.Println("Object pinned successfully!")

	var downloadOpts []siastorage.DownloadOption
	if *downloadMaxInflight > 0 {
		downloadOpts = append(downloadOpts, siastorage.WithDownloadInflight(*downloadMaxInflight))
	}

	fmt.Println("Downloading object...")
	verifier := newSeededVerifier(seed, *size)
	downloadStart := time.Now()
	rc, err := sdk.Download(obj, downloadOpts...)
	if err != nil {
		log.Fatal("failed to start download:", err)
	}
	defer rc.Close()
	if _, err := io.Copy(verifier, rc); err != nil {
		log.Fatal("failed to copy data:", err)
	}
	if verifier.remaining != 0 {
		log.Fatalf("expected %d more bytes", verifier.remaining)
	}
	downloadDuration := time.Since(downloadStart)

	encoded := encodedSize(obj)
	fmt.Printf(
		"Object uploaded ID: %s\tSize: %s\tEncoded: %s\tElapsed: %s\tThroughput: %s\tEncoded Throughput: %s\n",
		obj.ID(),
		formatBytes(obj.Size()),
		formatBytes(encoded),
		uploadDuration,
		formatBitrate(obj.Size(), uploadDuration),
		formatBitrate(encoded, uploadDuration),
	)
	fmt.Printf(
		"Object downloaded ID: %s\tSize: %s\tEncoded: %s\tElapsed: %s\tTTFB: %s\tThroughput: %s\tMax Write Latency: %s\n",
		obj.ID(),
		formatBytes(obj.Size()),
		formatBytes(encoded),
		downloadDuration,
		verifier.ttfb,
		formatBitrate(obj.Size(), downloadDuration),
		verifier.gapMax,
	)

	fmt.Println("Cleaning up...")
	if err := sdk.DeleteObject(ctx, obj.ID()); err != nil {
		log.Fatal("failed to delete object:", err)
	}
	if err := sdk.PruneSlabs(ctx); err != nil {
		log.Fatal("failed to prune slabs:", err)
	}
	fmt.Println("Object unpinned and slabs pruned.")
}
