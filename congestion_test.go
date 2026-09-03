package siastorage

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

// congestionStep feeds completions of the given latency until the limit moves,
// returning the new limit.
func congestionStep(c *inflightController, seconds float64) int {
	start := c.currentLimit()
	for n := 0; c.currentLimit() == start && n < 100_000; n++ {
		c.record(c.sample(), time.Duration(seconds*float64(time.Second)), true)
	}
	return c.currentLimit()
}

// congestionStepFailing feeds one window of failed completions, returning the
// resulting limit. A window without a success always decides, so one window is
// always a step, whether or not it moves the limit.
func congestionStepFailing(c *inflightController) int {
	for range minWindow {
		c.record(c.sample(), time.Second, false)
	}
	return c.currentLimit()
}

// congestionStepSaturating steps with a latency that is flat below sat and
// grows in proportion to the limit above it, so goodput plateaus at sat.
func congestionStepSaturating(c *inflightController, sat int, base float64) int {
	seconds := base * max(float64(c.currentLimit())/float64(sat), 1)
	return congestionStep(c, seconds)
}

// congestionWindowsUntilProbe feeds windows of successes at the settled limit
// and returns how many it took for the controller to probe upward, giving up
// after twice the schedule.
func congestionWindowsUntilProbe(c *inflightController, seconds float64) int {
	start := c.currentLimit()
	var windows int
	for windows < 2*probeInterval && c.currentLimit() == start {
		c.mu.Lock()
		window := c.window()
		c.mu.Unlock()
		for range window {
			c.record(c.sample(), time.Duration(seconds*float64(time.Second)), true)
		}
		windows++
	}
	return windows
}

// waitParked waits for n callers to be parked on the limiter. Callers park
// asynchronously, so a goroutine that has started may not be waiting yet.
func waitParked(t testing.TB, l *inflightLimiter, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		parked := l.parked
		l.mu.Unlock()
		if parked == n {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("expected %d parked callers, got %d", n, parked)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestInflightController(t *testing.T) {
	t.Run("climbs while goodput rises", func(t *testing.T) {
		c := newInflightController(8, 2, 1000, 1, zaptest.NewLogger(t))
		for _, want := range []int{16, 32, 64, 128} {
			if got := congestionStep(c, 1); got != want {
				t.Fatalf("expected %d, got %d", want, got)
			}
		}
	})

	t.Run("settles at saturation", func(t *testing.T) {
		c := newInflightController(8, 2, 1000, 1, zaptest.NewLogger(t))
		// 128 probes one step past saturation, then it settles at the last
		// level that was still climbing
		for _, want := range []int{16, 32, 64, 128, 64} {
			if got := congestionStepSaturating(c, 64, 1); got != want {
				t.Fatalf("expected %d, got %d", want, got)
			}
		}
	})

	t.Run("steady state", func(t *testing.T) {
		c := newInflightController(8, 2, 1000, 1, zaptest.NewLogger(t))
		for range 5 {
			congestionStepSaturating(c, 64, 1)
		}
		if got := c.currentLimit(); got != 64 {
			t.Fatal("unexpected settled limit", got)
		}

		minLimit := c.currentLimit()
		for range 30 {
			minLimit = min(minLimit, congestionStepSaturating(c, 64, 1))
		}
		if minLimit < 64 {
			t.Fatal("steady state backed off below saturation", minLimit)
		} else if got := congestionStep(c, 1); got <= 64 {
			t.Fatal("steady state did not probe upward", got)
		}
	})

	t.Run("backs off", func(t *testing.T) {
		c := newInflightController(8, 2, 1000, 1, zaptest.NewLogger(t))
		for range 5 {
			congestionStepSaturating(c, 64, 1)
		}
		// 4x the latency at the same limit is a quarter of the goodput
		if got := congestionStep(c, 4); got >= 64 {
			t.Fatal("sustained goodput decline did not back off", got)
		}
	})

	t.Run("failures", func(t *testing.T) {
		c := newInflightController(8, 2, 1000, 1, zaptest.NewLogger(t))
		congestionStep(c, 1)

		start := c.currentLimit()
		for range minWindow * (confirmWindows + 1) {
			c.record(c.sample(), time.Second, false)
			if c.currentLimit() < start {
				return
			}
		}
		t.Fatal("sustained failures did not back off")
	})

	t.Run("failures without baseline", func(t *testing.T) {
		c := newInflightController(8, 2, 1000, 1, zaptest.NewLogger(t))
		// each window without a success halves the limit, then holds it at the
		// floor rather than probing upward on a goodput of zero
		for _, want := range []int{4, 2, 2, 2} {
			if got := congestionStepFailing(c); got != want {
				t.Fatalf("expected %d, got %d", want, got)
			}
		}
	})

	// a limit whose doubling times every operation out must not be climbed
	// straight back to on the next window, or every other window is a total
	// loss; it is retried on the usual probe schedule
	t.Run("failed probe settles", func(t *testing.T) {
		c := newInflightController(8, 2, 1000, 1, zaptest.NewLogger(t))
		// settle at 8: the doubling to 16 does not raise goodput
		for _, want := range []int{16, 8} {
			if got := congestionStepSaturating(c, 8, 1); got != want {
				t.Fatalf("expected %d, got %d", want, got)
			}
		}
		for range 3 {
			if got := congestionWindowsUntilProbe(c, 1); got != probeInterval {
				t.Fatalf("expected a probe after %d healthy windows, got %d", probeInterval, got)
			} else if got := congestionStepFailing(c); got != 8 {
				t.Fatalf("expected the failed probe to settle at 8, got %d", got)
			} else if c.probing {
				t.Fatal("still probing after a probe without a single success")
			}
		}
	})

	// a healthy window at the capacity leaves the limit where it is, so a window
	// without a success there has no lower level to return to and must halve
	t.Run("failure at capacity backs off", func(t *testing.T) {
		c := newInflightController(8, 2, 16, 1, zaptest.NewLogger(t))
		if got := congestionStep(c, 1); got != 16 {
			t.Fatalf("expected 16, got %d", got)
		}
		for range minWindow {
			c.record(c.sample(), time.Second, true)
		}
		if got := congestionStepFailing(c); got != 8 {
			t.Fatalf("expected 8, got %d", got)
		}
	})

	// an initial limit that fails outright is halved and held, not climbed back
	// to on the next window as a fresh start
	t.Run("failed start holds below", func(t *testing.T) {
		c := newInflightController(8, 2, 1000, 1, zaptest.NewLogger(t))
		if got := congestionStepFailing(c); got != 4 {
			t.Fatalf("expected 4, got %d", got)
		} else if c.probing {
			t.Fatal("still probing after the initial limit failed outright")
		} else if got := congestionWindowsUntilProbe(c, 1); got != probeInterval {
			t.Fatalf("expected a probe after %d healthy windows, got %d", probeInterval, got)
		} else if got := c.currentLimit(); got != 8 {
			t.Fatalf("expected the probe to try 8 again, got %d", got)
		}
	})

	t.Run("bounds", func(t *testing.T) {
		tests := []struct {
			name                           string
			initial, floor, capacity, want int
		}{
			{"initial above capacity", 8, 2, 4, 4},
			{"initial below floor", 1, 2, 100, 2},
			{"capacity below floor", 8, 2, 1, 1},
			{"zero capacity", 8, 2, 0, 1},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c := newInflightController(tt.initial, tt.floor, tt.capacity, 1, zaptest.NewLogger(t))
				if got := c.currentLimit(); got != tt.want {
					t.Fatalf("expected %d, got %d", tt.want, got)
				}
			})
		}
	})

	t.Run("discards old generation", func(t *testing.T) {
		c := newInflightController(8, 2, 100, 1, zaptest.NewLogger(t))
		stale := c.sample()
		if got := congestionStep(c, 1); got != 16 {
			t.Fatal("unexpected grown limit", got)
		}

		for range minWindow * 2 {
			c.record(stale, time.Hour, false)
		}
		if got := c.currentLimit(); got != 16 {
			t.Fatal("old-generation completions changed limit", got)
		}
	})
}

func TestInflightLimiter(t *testing.T) {
	t.Run("wakes waiter", func(t *testing.T) {
		limiter := newInflightLimiter(1, 1, 1, 1, zaptest.NewLogger(t))
		release, ok := limiter.acquire(t.Context())
		if !ok {
			t.Fatal("failed to acquire initial permit")
		}

		acquired := make(chan func(), 1)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		go func() {
			if release, ok := limiter.acquire(ctx); ok {
				acquired <- release
			}
		}()
		waitParked(t, limiter, 1)
		select {
		case second := <-acquired:
			second()
			t.Fatal("limiter exceeded its limit")
		default:
		}

		release()
		select {
		case second := <-acquired:
			second()
		case <-ctx.Done():
			t.Fatal("parked acquire missed permit release")
		}
	})

	// a single permit handed from one caller to the next must get all of them
	// through, leaving nobody parked behind a release they could have taken
	t.Run("wakes every waiter", func(t *testing.T) {
		limiter := newInflightLimiter(1, 1, 1, 1, zaptest.NewLogger(t))
		release, ok := limiter.acquire(t.Context())
		if !ok {
			t.Fatal("failed to acquire initial permit")
		}

		const waiters = 16
		done := make(chan struct{}, waiters)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		for range waiters {
			go func() {
				if release, ok := limiter.acquire(ctx); ok {
					release() // pass the permit along to the next in line
					done <- struct{}{}
				}
			}()
		}
		waitParked(t, limiter, waiters)

		release()
		for i := range waiters {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("only %d of %d waiters were woken", i, waiters)
			}
		}
	})

	// a limit increase must admit exactly the waiters the permits it opened can
	// hold, no more and no fewer
	t.Run("limit growth wakes waiters", func(t *testing.T) {
		limiter := newInflightLimiter(8, 2, 1000, 1, zaptest.NewLogger(t))

		// hold every permit the current limit allows
		for range limiter.controller.currentLimit() {
			if _, ok := limiter.tryAcquire(); !ok {
				t.Fatal("failed to fill the limiter")
			}
		}

		// park more callers than the next doubling can admit
		const waiters = 32
		admitted := make(chan struct{}, waiters)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		for range waiters {
			go func() {
				if _, ok := limiter.acquire(ctx); ok {
					admitted <- struct{}{} // hold the permit
				}
			}()
		}
		waitParked(t, limiter, waiters)

		// feed completions until the controller raises the limit
		before := limiter.controller.currentLimit()
		for n := 0; limiter.controller.currentLimit() == before; n++ {
			if n > 100_000 {
				t.Fatal("controller never raised the limit")
			}
			limiter.record(limiter.sample(), time.Second, true)
		}
		grew := limiter.controller.currentLimit() - before

		for i := range grew {
			select {
			case <-admitted:
			case <-time.After(time.Second):
				t.Fatalf("limit grew by %d but only %d waiters were woken", grew, i)
			}
		}
		// no caller beyond the new limit may be admitted
		waitParked(t, limiter, waiters-grew)
		select {
		case <-admitted:
			t.Fatal("limit growth admitted more callers than the permits it opened")
		default:
		}
	})

	// a caller that gives up must not swallow the wakeup a later waiter needs
	t.Run("cancelled waiter", func(t *testing.T) {
		limiter := newInflightLimiter(1, 1, 1, 1, zaptest.NewLogger(t))
		release, ok := limiter.tryAcquire()
		if !ok {
			t.Fatal("failed to acquire initial permit")
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, ok := limiter.acquire(ctx); ok {
			t.Fatal("cancelled waiter acquired a permit")
		}

		acquired := make(chan func(), 1)
		go func() {
			if release, ok := limiter.acquire(t.Context()); ok {
				acquired <- release
			}
		}()
		waitParked(t, limiter, 1)
		release()

		select {
		case release := <-acquired:
			release()
		case <-time.After(time.Second):
			t.Fatal("cancelled waiter consumed the next notification")
		}
	})

	// the backlog may run one batch ahead of the current limit, but no further
	t.Run("commit lookahead", func(t *testing.T) {
		limiter := newInflightLimiter(8, 2, 100, 1, zaptest.NewLogger(t)) // initial limit 8, capacity 100

		first, ok := limiter.commit(t.Context(), 10)
		if !ok {
			t.Fatal("failed to commit the first batch")
		} else if _, ok := limiter.commit(t.Context(), 10); !ok {
			t.Fatal("failed to commit the lookahead batch")
		}

		committed := make(chan *commitment, 1)
		go func() {
			c, _ := limiter.commit(t.Context(), 10)
			committed <- c
		}()
		waitParked(t, limiter, 1)
		select {
		case c := <-committed:
			c.releaseAll()
			t.Fatal("third batch passed the lookahead gate")
		default:
		}

		// dropping below limit + one batch lets the parked batch proceed
		for range 3 {
			first.releaseOne()
		}
		select {
		case <-committed:
		case <-time.After(time.Second):
			t.Fatal("parked batch did not resume once the backlog cleared")
		}
	})

	// the backlog never exceeds the capacity, even when the limit would allow it
	t.Run("commit capacity", func(t *testing.T) {
		limiter := newInflightLimiter(8, 2, 2, 1, zaptest.NewLogger(t)) // capacity of one two-unit batch

		first, ok := limiter.commit(t.Context(), 2)
		if !ok {
			t.Fatal("failed to commit the first batch")
		}

		committed := make(chan *commitment, 1)
		go func() {
			c, _ := limiter.commit(t.Context(), 2)
			committed <- c
		}()
		waitParked(t, limiter, 1)
		select {
		case c := <-committed:
			c.releaseAll()
			t.Fatal("batch committed beyond the capacity")
		default:
		}

		first.releaseAll()
		select {
		case <-committed:
		case <-time.After(time.Second):
			t.Fatal("parked batch did not resume once the capacity freed up")
		}
	})

	// batches are served in arrival order, so a small batch that would fit waits
	// behind a larger one that does not, rather than starving it
	t.Run("commit in order", func(t *testing.T) {
		limiter := newInflightLimiter(8, 2, 12, 1, zaptest.NewLogger(t)) // limit 8, capacity 12

		first, ok := limiter.commit(t.Context(), 8)
		if !ok {
			t.Fatal("failed to commit the first batch")
		}

		// park a batch that does not fit, then one behind it that would
		big := make(chan *commitment, 1)
		go func() {
			c, _ := limiter.commit(t.Context(), 5)
			big <- c
		}()
		waitParked(t, limiter, 1)

		small := make(chan *commitment, 1)
		go func() {
			c, _ := limiter.commit(t.Context(), 1)
			small <- c
		}()
		waitParked(t, limiter, 2)
		select {
		case <-small:
			t.Fatal("small batch committed ahead of the larger one in line")
		default:
		}

		first.releaseAll()
		for _, batch := range []chan *commitment{big, small} {
			select {
			case c := <-batch:
				if c == nil {
					t.Fatal("batch failed to commit")
				}
			case <-time.After(time.Second):
				t.Fatal("batch stayed parked once its turn came")
			}
		}
	})

	// a parked batch that gives up leaves the line, so the one behind it is not
	// stuck waiting for a turn that never comes
	t.Run("commit cancelled in line", func(t *testing.T) {
		limiter := newInflightLimiter(8, 2, 12, 1, zaptest.NewLogger(t)) // limit 8, capacity 12

		if _, ok := limiter.commit(t.Context(), 8); !ok {
			t.Fatal("failed to commit the first batch")
		}

		ctx, cancel := context.WithCancel(t.Context())
		big := make(chan *commitment, 1)
		go func() {
			c, _ := limiter.commit(ctx, 5)
			big <- c
		}()
		waitParked(t, limiter, 1)

		small := make(chan *commitment, 1)
		go func() {
			c, _ := limiter.commit(t.Context(), 1)
			small <- c
		}()
		waitParked(t, limiter, 2)

		cancel()
		if c := <-big; c != nil {
			t.Fatal("cancelled batch committed")
		}
		select {
		case c := <-small:
			if c == nil {
				t.Fatal("small batch failed to commit")
			}
		case <-time.After(time.Second):
			t.Fatal("small batch stayed parked behind a cancelled one")
		}
	})

	// a batch larger than the whole capacity is admitted alone rather than
	// parked forever, and nothing else commits until it is done, so oversized
	// batches take turns
	t.Run("commit larger than the capacity", func(t *testing.T) {
		limiter := newInflightLimiter(8, 2, 4, 1, zaptest.NewLogger(t)) // capacity 4

		held, ok := limiter.commit(t.Context(), 30)
		if !ok {
			t.Fatal("oversized batch was not admitted")
		}

		// another oversized batch and one that fits, in either order
		sizes := []int{30, 1}
		done := make(chan *commitment, len(sizes))
		for _, n := range sizes {
			go func() {
				c, _ := limiter.commit(t.Context(), n)
				done <- c
			}()
		}
		waitParked(t, limiter, len(sizes))

		for i := range sizes {
			held.releaseAll()
			select {
			case c := <-done:
				if c == nil {
					t.Fatalf("waiter %d failed to commit", i)
				}
				held = c
			case <-time.After(time.Second):
				t.Fatalf("waiter %d stayed parked after the batch ahead finished", i)
			}
			if len(done) != 0 {
				t.Fatalf("waiter %d committed alongside another batch", i)
			}
			waitParked(t, limiter, len(sizes)-i-1)
		}
		held.releaseAll()
	})

	t.Run("commit cancelled", func(t *testing.T) {
		limiter := newInflightLimiter(8, 2, 2, 1, zaptest.NewLogger(t))
		if _, ok := limiter.commit(t.Context(), 2); !ok {
			t.Fatal("failed to commit the first batch")
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, ok := limiter.commit(ctx, 2); ok {
			t.Fatal("cancelled commit succeeded")
		}
	})
}
