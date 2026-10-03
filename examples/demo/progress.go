package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// A progress renders one line that updates in place while a transfer runs, so
// a slow upload against a real indexer shows movement rather than sitting
// silent for minutes.
//
// The total is derived from the payload and the erasure coding the demo asks
// for, not reported by the SDK, so treat the percentage as the demo's own
// arithmetic. It is clamped rather than allowed to exceed 100%.
type progress struct {
	total   uint64
	started time.Time
	tty     bool

	// mu guards the rest. OnShard runs on a goroutine the transfer owns, and
	// nothing promises the calls are serialised.
	mu       sync.Mutex
	done     uint64
	lastDraw time.Time
	lastPct  int
}

// redrawInterval throttles the terminal writes. Shard events can arrive in
// bursts of thirty and redrawing on each one is just noise.
const redrawInterval = 100 * time.Millisecond

func newProgress(total uint64) *progress {
	return &progress{
		total:   total,
		started: time.Now(),
		tty:     isTerminal(),
		lastPct: -1,
	}
}

// isTerminal reports whether stdout is a terminal, so piped output does not
// fill up with carriage returns.
func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// update records the running total and redraws if enough time has passed.
func (p *progress) update(transferred uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done = transferred

	now := time.Now()
	if p.tty {
		if now.Sub(p.lastDraw) < redrawInterval {
			return
		}
		p.lastDraw = now
		fmt.Print("\r" + p.line())
		return
	}
	// Without a terminal, say something every 25% instead of every frame.
	pct := p.percent() / 25 * 25
	if pct > p.lastPct {
		p.lastPct = pct
		fmt.Printf("    %d%% %s\n", pct, bytes4(p.done))
	}
}

// finish clears the live line and leaves one summary behind.
func (p *progress) finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	elapsed := time.Since(p.started)
	if p.tty {
		// Erase to end of line rather than blanking a guessed width, which a
		// long eta or a GiB/s rate can overrun.
		fmt.Print("\r\x1b[K")
	}
	fmt.Printf("    %s in %s, %s/s\n",
		bytes4(p.done), elapsed.Round(time.Millisecond), bytes4(rate(p.done, elapsed)))
}

func (p *progress) percent() int {
	if p.total == 0 {
		return 0
	}
	pct := int(p.done * 100 / p.total)
	return min(pct, 100)
}

// line renders the bar itself. Caller holds mu.
func (p *progress) line() string {
	const width = 24
	pct := p.percent()
	filled := pct * width / 100
	bar := strings.Repeat("=", filled) + strings.Repeat(" ", width-filled)
	if filled > 0 && filled < width {
		bar = bar[:filled-1] + ">" + bar[filled:]
	}

	elapsed := time.Since(p.started)
	var eta string
	if p.done > 0 && p.done < p.total {
		remaining := time.Duration(float64(elapsed) * float64(p.total-p.done) / float64(p.done))
		eta = fmt.Sprintf("  eta %s", remaining.Round(time.Second))
	}
	return fmt.Sprintf("    [%s] %3d%%  %s / %s  %s/s%s",
		bar, pct, bytes4(p.done), bytes4(p.total), bytes4(rate(p.done, elapsed)), eta)
}

// rate is bytes per second, guarding the zero duration a fast start produces.
func rate(n uint64, d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64(float64(n) / d.Seconds())
}
