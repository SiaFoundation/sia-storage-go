package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	siastorage "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/term"
)

const (
	appIDHex       = "5c0b1af28e6ac76395b2087ea987297b9c496f90d2ab3e3d3d07980ae4c43633"
	defaultIndexer = "https://sia.storage"
	defaultProfile = "default"

	// These mirror the SDK's default redundancy and are used only to map the
	// number of encoded bytes uploaded back to an approximate unencoded
	// position for the upload progress bar.
	benchDataShards  = 10
	benchTotalShards = 30
)

func appMetadata() (siastorage.AppMetadata, error) {
	var appID types.Hash256
	if err := appID.UnmarshalText([]byte(appIDHex)); err != nil {
		return siastorage.AppMetadata{}, fmt.Errorf("failed to parse app ID: %w", err)
	}
	return siastorage.AppMetadata{
		ID:          appID,
		Name:        "Benchmark",
		Description: "A simple upload and download benchmark for the SDK",
		ServiceURL:  "https://sia.tech",
	}, nil
}

// seededReader produces a deterministic stream of bytes from a seed.
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

// seededVerifier verifies a downloaded stream against the same seed used to
// produce it, while recording latency metrics and driving a progress bar.
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
	bar       *mpb.Bar
}

func newSeededVerifier(seed [32]byte, size uint64, bar *mpb.Bar) *seededVerifier {
	return &seededVerifier{
		src:       mrand.NewChaCha8(seed),
		size:      size,
		remaining: size,
		start:     time.Now(),
		bar:       bar,
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
	v.bar.IncrBy(len(p))
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
	if d <= 0 {
		return "0.00 bps"
	}
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

// newTransferBar adds an indicatif-style progress bar to the container. The
// rate is reported as a bitrate (Mbps) to match the benchmark's summary, and
// is frozen at completion so a finished bar doesn't decay while the next
// transfer runs.
func newTransferBar(p *mpb.Progress, name string, total uint64, start time.Time) *mpb.Bar {
	var frozen time.Duration
	return p.New(int64(total),
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding("-").Rbound("]"),
		mpb.BarWidth(40),
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%-8s ", name)),
			decor.CountersKibiByte("% .2f / % .2f", decor.WCSyncSpace),
		),
		mpb.AppendDecorators(
			decor.Any(func(s decor.Statistics) string {
				elapsed := time.Since(start)
				if s.Completed {
					if frozen == 0 {
						frozen = elapsed
					}
					elapsed = frozen
				}
				return formatBitrate(uint64(s.Current), elapsed)
			}, decor.WCSyncSpace),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO, decor.WCSyncSpace), "done"),
		),
	)
}

// hostStat accumulates per-host transfer totals.
type hostStat struct {
	shards  int
	bytes   uint64
	elapsed time.Duration // summed per-shard time; overcounts wall-clock as shards overlap
}

type hostStats struct {
	mu sync.Mutex
	m  map[string]*hostStat
}

func newHostStats() *hostStats {
	return &hostStats{m: make(map[string]*hostStat)}
}

func (h *hostStats) record(host string, bytes uint64, elapsed time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.m[host]
	if s == nil {
		s = &hostStat{}
		h.m[host] = s
	}
	s.shards++
	s.bytes += bytes
	s.elapsed += elapsed
}

func (h *hostStats) printSummary(label string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.m) == 0 {
		return
	}
	rate := func(s *hostStat) float64 {
		if s.elapsed <= 0 {
			return 0
		}
		return float64(s.bytes) / s.elapsed.Seconds()
	}
	type row struct {
		host string
		stat *hostStat
	}
	rows := make([]row, 0, len(h.m))
	var total uint64
	for host, s := range h.m {
		rows = append(rows, row{host, s})
		total += s.bytes
	}
	sort.Slice(rows, func(i, j int) bool { return rate(rows[i].stat) > rate(rows[j].stat) })
	fmt.Printf("\n%s per-host summary (%d hosts):\n", label, len(rows))
	for _, r := range rows {
		fmt.Printf("  %s  %4d shards  %11s  %s\n",
			r.host, r.stat.shards, formatBytes(r.stat.bytes), formatBitrate(r.stat.bytes, r.stat.elapsed))
	}
	fmt.Printf("  total %s across %d hosts\n", formatBytes(total), len(rows))
}

// --- profile config (shared with the Rust benchmark) ------------------------
//
// The on-disk location and format intentionally match the Rust benchmark in
// sia-sdk-rs, so a profile created by either tool works in the other:
//   - location: the `directories` crate's ProjectDirs("tech", "Sia", "sia-benchmark")
//   - format:   TOML, with a `[profiles.<name>]` table per profile
//   - app_key:  the 32-byte app-key seed, hex-encoded (matching AppKey::export)

type config struct {
	Profiles map[string]profile `toml:"profiles"`
}

type profile struct {
	Indexer string `toml:"indexer"`
	// AppKey is the 32-byte app-key seed, hex-encoded (matching AppKey::export).
	AppKey string `toml:"app_key"`
}

func configPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("could not determine config directory: %w", err)
	}
	// Mirror the `directories` crate's per-OS ProjectDirs config layout so the
	// path lines up with the Rust benchmark.
	var rel string
	switch runtime.GOOS {
	case "darwin":
		rel = filepath.Join("tech.Sia.sia-benchmark", "config.toml")
	case "windows":
		rel = filepath.Join("Sia", "sia-benchmark", "config", "config.toml")
	default: // linux and other XDG platforms
		rel = filepath.Join("sia-benchmark", "config.toml")
	}
	return filepath.Join(base, rel), nil
}

func loadConfig() (config, error) {
	path, err := configPath()
	if err != nil {
		return config{}, err
	}
	cfg := config{Profiles: map[string]profile{}}
	if _, err := toml.DecodeFile(path, &cfg); errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	} else if err != nil {
		return config{}, err
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]profile{}
	}
	return cfg, nil
}

func saveConfig(cfg config) (string, error) {
	path, err := configPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	if err := toml.NewEncoder(f).Encode(cfg); err != nil {
		return "", errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return path, nil
}

func readProfile(name string) (string, types.PrivateKey, error) {
	cfg, err := loadConfig()
	if err != nil {
		return "", nil, err
	}
	p, ok := cfg.Profiles[name]
	if !ok {
		return "", nil, fmt.Errorf("profile %q not found; run `benchmark login --profile %s` first", name, name)
	}
	seed, err := hex.DecodeString(p.AppKey)
	if err != nil {
		return "", nil, fmt.Errorf("profile %q has an invalid app key: %w", name, err)
	}
	if len(seed) != 32 {
		return "", nil, fmt.Errorf("profile %q has an invalid app key: expected 32 bytes, got %d", name, len(seed))
	}
	return p.Indexer, types.NewPrivateKeyFromSeed(seed), nil
}

func upsertProfile(name, indexer string, key types.PrivateKey) (string, error) {
	if len(key) < 32 {
		return "", fmt.Errorf("app key too short: %d bytes", len(key))
	}
	cfg, err := loadConfig()
	if err != nil {
		return "", err
	}
	cfg.Profiles[name] = profile{
		Indexer: strings.TrimSpace(indexer),
		// Store the 32-byte seed (matching the Rust AppKey::export format) so
		// the profile is interchangeable between the two benchmarks.
		AppKey: hex.EncodeToString(key[:32]),
	}
	return saveConfig(cfg)
}

// readPhrase reads the wallet recovery phrase from stdin. When stdin is a
// terminal it reads without echoing the secret; otherwise it reads a single
// line so the benchmark can still be driven non-interactively (e.g. piped in).
func readPhrase() (string, error) {
	fmt.Println("Enter recovery phrase:")
	if term.IsTerminal(int(os.Stdin.Fd())) {
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err != nil {
			return "", fmt.Errorf("failed to read recovery phrase: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	}

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("failed to read recovery phrase: %w", err)
		}
		return "", fmt.Errorf("failed to read recovery phrase: unexpected EOF on stdin")
	}
	return strings.TrimSpace(scanner.Text()), nil
}

// --- commands ----------------------------------------------------------------

func login(ctx context.Context, profileName, indexer string, newPhrase bool) error {
	meta, err := appMetadata()
	if err != nil {
		return err
	}
	builder := siastorage.NewBuilder(indexer, meta)

	responseURL, err := builder.RequestConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to request connection: %w", err)
	}
	fmt.Println("Visit the following URL to authorize the application:", responseURL)

	if err := builder.WaitForApproval(ctx); err != nil {
		return fmt.Errorf("failed to wait for approval: %w", err)
	}
	fmt.Println("Connection approved!")

	var phrase string
	if newPhrase {
		phrase = siastorage.NewSeedPhrase()
		fmt.Printf("Generated recovery phrase (write it down):\n  %s\n", phrase)
	} else {
		phrase, err = readPhrase()
		if err != nil {
			return err
		}
	}

	sdk, err := builder.Register(ctx, phrase)
	if err != nil {
		return fmt.Errorf("failed to register app: %w", err)
	}
	defer sdk.Close()

	path, err := upsertProfile(profileName, indexer, sdk.AppKey())
	if err != nil {
		return fmt.Errorf("failed to save profile: %w", err)
	}
	fmt.Printf("Profile %q saved to %s (indexer: %s)\n", profileName, path, indexer)
	return nil
}

func connect(ctx context.Context, profileName string, logger *zap.Logger) (*siastorage.SDK, error) {
	indexer, key, err := readProfile(profileName)
	if err != nil {
		return nil, err
	}
	meta, err := appMetadata()
	if err != nil {
		return nil, err
	}
	builder := siastorage.NewBuilder(indexer, meta)
	sdk, err := builder.SDK(key, siastorage.WithLogger(logger))
	if errors.Is(err, siastorage.ErrUnauthorized) {
		return nil, fmt.Errorf("app key for profile %q is not authorized; run `benchmark login --profile %s`", profileName, profileName)
	} else if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	account, err := sdk.Account(ctx)
	if err != nil {
		sdk.Close()
		return nil, fmt.Errorf("failed to fetch account: %w", err)
	}
	if !account.Ready {
		sdk.Close()
		return nil, errors.New("account is not ready yet — the indexer is still propagating registration on the network; try again shortly")
	}
	return sdk, nil
}

func listProfiles() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if len(cfg.Profiles) == 0 {
		fmt.Println("No profiles configured. Run `benchmark login` to create one.")
		return nil
	}
	names := make([]string, 0, len(cfg.Profiles))
	pad := 0
	for name := range cfg.Profiles {
		names = append(names, name)
		if len(name) > pad {
			pad = len(name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf("  %-*s  %s\n", pad, name, cfg.Profiles[name].Indexer)
	}
	return nil
}

func runBenchmark(ctx context.Context, sdk *siastorage.SDK, size uint64, uploadMaxBufferedSlabs, downloadMaxBufferedChunks int, hostSummary bool) error {
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return fmt.Errorf("failed to generate seed: %w", err)
	}

	progress := mpb.New(mpb.WithOutput(os.Stderr))

	// upload the data to the network
	uploadHosts := newHostStats()
	var encodedUploaded atomic.Uint64
	uploadStart := time.Now()
	uploadBar := newTransferBar(progress, "upload", size, uploadStart)
	uploadOpts := []siastorage.UploadOption{
		siastorage.WithUploadProgress(func(p siastorage.ShardProgress) {
			encoded := encodedUploaded.Add(p.ShardSize)
			// map encoded bytes back to an approximate unencoded position
			unencoded := encoded * benchDataShards / benchTotalShards
			if unencoded > size {
				unencoded = size
			}
			uploadBar.SetCurrent(int64(unencoded))
			uploadHosts.record(p.HostKey.String(), p.ShardSize, p.Elapsed)
		}),
	}
	if uploadMaxBufferedSlabs > 0 {
		uploadOpts = append(uploadOpts, siastorage.WithUploadBufferedSlabs(uploadMaxBufferedSlabs))
	}

	obj := siastorage.NewEmptyObject()
	if err := sdk.Upload(ctx, &obj, newSeededReader(seed, size), uploadOpts...); err != nil {
		uploadBar.Abort(false)
		progress.Wait()
		return fmt.Errorf("failed to upload object: %w", err)
	}
	uploadDuration := time.Since(uploadStart)
	uploadBar.SetCurrent(int64(size)) // ensure the bar completes

	if err := sdk.PinObject(ctx, obj); err != nil {
		// The upload succeeded but the slabs were never associated with an
		// object, so prune them best-effort to avoid leaving orphaned data.
		if pruneErr := sdk.PruneSlabs(ctx); pruneErr != nil {
			log.Printf("failed to prune slabs after pin failure: %v", pruneErr)
		}
		progress.Wait()
		return fmt.Errorf("failed to pin object: %w", err)
	}

	// Best-effort cleanup so a failure during download/verification doesn't
	// leave a pinned object and its slabs behind in the user's account. Both
	// steps run independently: PruneSlabs is safe and useful even if the
	// object delete fails, since it only removes unreferenced slabs.
	defer func() {
		if err := sdk.DeleteObject(ctx, obj.ID()); err != nil {
			log.Printf("failed to delete object: %v", err)
		}
		if err := sdk.PruneSlabs(ctx); err != nil {
			log.Printf("failed to prune slabs: %v", err)
		}
	}()

	// download and verify the data
	downloadHosts := newHostStats()
	downloadOpts := []siastorage.DownloadOption{
		siastorage.WithDownloadProgress(func(p siastorage.ShardProgress) {
			downloadHosts.record(p.HostKey.String(), p.ShardSize, p.Elapsed)
		}),
	}
	if downloadMaxBufferedChunks > 0 {
		downloadOpts = append(downloadOpts, siastorage.WithDownloadBufferedChunks(downloadMaxBufferedChunks))
	}

	downloadStart := time.Now()
	downloadBar := newTransferBar(progress, "download", size, downloadStart)
	verifier := newSeededVerifier(seed, size, downloadBar)
	rc, err := sdk.Download(obj, downloadOpts...)
	if err != nil {
		downloadBar.Abort(false)
		progress.Wait()
		return fmt.Errorf("failed to start download: %w", err)
	}
	defer rc.Close()
	if _, err := io.Copy(verifier, rc); err != nil {
		downloadBar.Abort(false)
		progress.Wait()
		return fmt.Errorf("failed to copy data: %w", err)
	}
	if verifier.remaining != 0 {
		downloadBar.Abort(false)
		progress.Wait()
		return fmt.Errorf("expected %d more bytes", verifier.remaining)
	}
	downloadDuration := time.Since(downloadStart)
	progress.Wait() // flush and stop rendering before printing the summary

	encoded := encodedSize(obj)
	fmt.Printf("\n%-15s%s\n", "Size:", formatBytes(obj.Size()))
	fmt.Printf("%-15s%s\n", "Encoded:", formatBytes(encoded))

	fmt.Println("\nUpload")
	fmt.Printf("  %-20s%s\n", "Elapsed:", uploadDuration)
	fmt.Printf("  %-20s%s\n", "Throughput:", formatBitrate(obj.Size(), uploadDuration))
	fmt.Printf("  %-20s%s\n", "Encoded Throughput:", formatBitrate(encoded, uploadDuration))

	fmt.Println("\nDownload")
	fmt.Printf("  %-20s%s\n", "Size:", formatBytes(obj.Size()))
	fmt.Printf("  %-20s%s\n", "Elapsed:", downloadDuration)
	fmt.Printf("  %-20s%s\n", "TTFB:", verifier.ttfb)
	fmt.Printf("  %-20s%s\n", "Throughput:", formatBitrate(obj.Size(), downloadDuration))
	fmt.Printf("  %-20s%s\n", "Max latency:", verifier.gapMax)

	if hostSummary {
		uploadHosts.printSummary("Upload")
		downloadHosts.printSummary("Download")
	}
	return nil
}

// newFileLogger writes SDK logs to a timestamped file so they don't interleave
// with the progress bars on stderr. The returned closer flushes the logger and
// closes the underlying file; callers should defer it.
func newFileLogger() (logger *zap.Logger, path string, closer func() error, err error) {
	path = fmt.Sprintf("benchmark-%s.log", time.Now().Format("20060102T150405"))
	f, err := os.Create(path)
	if err != nil {
		return nil, "", nil, err
	}
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(cfg), zapcore.AddSync(f), zap.InfoLevel)
	logger = zap.New(core)
	closer = func() error {
		// Sync flushes zap's buffers to the file; Close releases the fd.
		syncErr := logger.Sync()
		return errors.Join(syncErr, f.Close())
	}
	return logger, path, closer, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `benchmark — benchmark Sia uploads and downloads

Usage:
  benchmark login    [--profile NAME] [--indexer URL] [--new]
  benchmark run      [--profile NAME] [--size BYTES] [--upload-max-buffered-slabs N]
                     [--download-max-buffered-chunks N] [--host-summary]
  benchmark profiles

Each profile binds an app key to an indexer so subsequent runs can skip the
auth flow.`)
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
		newPhrase := fs.Bool("new", false, "generate a new recovery phrase instead of prompting for one")
		fs.Parse(os.Args[2:])
		if err := login(ctx, *profileName, *indexer, *newPhrase); err != nil {
			log.Fatal(err)
		}

	case "run":
		fs := flag.NewFlagSet("run", flag.ExitOnError)
		profileName := fs.String("profile", defaultProfile, "profile to use")
		size := fs.Uint64("size", 120*1024*1024, "size of the data to upload and download in bytes")
		uploadMaxBufferedSlabs := fs.Int("upload-max-buffered-slabs", 0, "maximum number of slabs buffered in memory during upload (0 = SDK default)")
		downloadMaxBufferedChunks := fs.Int("download-max-buffered-chunks", 0, "maximum number of chunks buffered in memory during download (0 = SDK default)")
		hostSummary := fs.Bool("host-summary", false, "print a per-host breakdown of shards and throughput after the run")
		fs.Parse(os.Args[2:])

		logger, logPath, closeLogger, err := newFileLogger()
		if err != nil {
			log.Fatal(err)
		}
		defer closeLogger()
		fmt.Fprintf(os.Stderr, "logging to %s\n", logPath)

		sdk, err := connect(ctx, *profileName, logger)
		if err != nil {
			log.Fatal(err)
		}
		defer sdk.Close()

		if err := runBenchmark(ctx, sdk, *size, *uploadMaxBufferedSlabs, *downloadMaxBufferedChunks, *hostSummary); err != nil {
			log.Fatal(err)
		}

	case "profiles":
		if err := listProfiles(); err != nil {
			log.Fatal(err)
		}

	default:
		usage()
		os.Exit(2)
	}
}
