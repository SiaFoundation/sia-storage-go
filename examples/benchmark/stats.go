package main

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// newTransferBar adds a progress bar to the container. The rate is reported as
// a bitrate to match the summary, and is frozen at completion so a finished bar
// does not decay while the next transfer runs.
func newTransferBar(p *mpb.Progress, name string, total uint64, start time.Time) *mpb.Bar {
	var frozen time.Duration
	return p.New(int64(total),
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding("-").Rbound("]"),
		mpb.BarWidth(40),
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%-9s ", name)),
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
	elapsed time.Duration // summed per-shard time; overcounts wall clock as shards overlap
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

// printSummary writes the per-host breakdown to stderr, so stdout stays a
// stream of samples.
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
	fmt.Fprintf(os.Stderr, "\n%s per-host summary (%d hosts):\n", label, len(rows))
	for _, r := range rows {
		fmt.Fprintf(os.Stderr, "  %s  %4d shards  %11s  %s\n",
			r.host, r.stat.shards, formatBytes(r.stat.bytes), formatBitrate(r.stat.bytes, r.stat.elapsed))
	}
	fmt.Fprintf(os.Stderr, "  total %s across %d hosts\n", formatBytes(total), len(rows))
}

// printRunSummary reports min, max and mean of each figure across the runs,
// matching what the Rust benchmark prints when more than one run is asked for.
func printRunSummary(samples []Sample) {
	if len(samples) < 2 {
		return
	}
	stat := func(label string, pick func(Sample) float64, format func(float64) string) {
		min, max, sum := pick(samples[0]), pick(samples[0]), 0.0
		for _, s := range samples {
			v := pick(s)
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
			sum += v
		}
		fmt.Fprintf(os.Stderr, "  %-15smin %12s  max %12s  avg %12s\n",
			label, format(min), format(max), format(sum/float64(len(samples))))
	}
	secs := func(v float64) string { return time.Duration(v).Round(time.Millisecond).String() }
	rate := func(v float64) string { return formatBitrate(uint64(v)/8, time.Second) }

	fmt.Fprintf(os.Stderr, "\nSummary (%d runs)\n", len(samples))
	fmt.Fprintln(os.Stderr, "Upload")
	stat("Elapsed:", func(s Sample) float64 { return float64(s.UploadNS) }, secs)
	stat("Rate:", func(s Sample) float64 { return bitsPerSecond(s.PayloadBytes, s.UploadNS) }, rate)
	stat("Encoded Rate:", func(s Sample) float64 { return bitsPerSecond(s.EncodedBytes, s.UploadNS) }, rate)
	fmt.Fprintln(os.Stderr, "Download")
	stat("Elapsed:", func(s Sample) float64 { return float64(s.DownloadNS) }, secs)
	stat("TTFB:", func(s Sample) float64 { return float64(s.TTFBNS) }, secs)
	stat("Rate:", func(s Sample) float64 { return bitsPerSecond(s.PayloadBytes, s.DownloadNS) }, rate)
	stat("Max latency:", func(s Sample) float64 { return float64(s.MaxGapNS) }, secs)
}

func bitsPerSecond(bytes, ns int64) float64 {
	if ns <= 0 {
		return 0
	}
	return float64(bytes) * 8.0 / (float64(ns) / float64(time.Second))
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
	units := []string{"bps", "Kbps", "Mbps", "Gbps", "Tbps"}
	v := float64(b) * 8.0 / d.Seconds()
	for i, u := range units {
		if v < 1000.0 || i == len(units)-1 {
			return fmt.Sprintf("%.2f %s", v, u)
		}
		v /= 1000.0
	}
	panic("unreachable")
}
