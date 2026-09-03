package siastorage

import (
	"context"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	// minWindow and maxWindow bound the number of completions gathered before
	// each decision. While probing the window is minimal so the limit reacts
	// quickly; in steady state it scales with the limit, clamped, so the goodput
	// estimate stays stable without going open-loop.
	minWindow = 16
	maxWindow = 1024

	// riseMargin is how much goodput must improve for a doubling to count as
	// progress. Once a doubling buys less, probing stops.
	riseMargin = 0.10

	// declineMargin is how far goodput must fall below its smoothed peak in
	// steady state to count as congestion rather than noise.
	declineMargin = 0.25

	// emaAlpha smooths the steady-state goodput baseline.
	emaAlpha = 0.20

	// confirmWindows is the number of consecutive adverse windows required
	// before acting, so a single noisy window can neither settle the limit low
	// nor back it off. A window without a single success is exempt: no goodput
	// at all is not noise.
	confirmWindows = 2

	// probeInterval is the number of healthy steady-state windows between
	// upward probes, so a settled limit climbs again when capacity frees up,
	// e.g. when a concurrent transfer finishes.
	probeInterval = 8

	// maxInflightLimit caps every limiter's capacity, keeping the doubling and
	// the window scaling well clear of overflow.
	maxInflightLimit = 1 << 20
)

// clampBufferBudget clamps a buffer budget so the capacity it converts to stays
// within maxInflightLimit. budget counts buffered slabs on the upload path and
// buffered chunks on the download path; permitsEach is how many permits one of
// them holds.
func clampBufferBudget(budget, permitsEach int) int {
	return min(budget, max(maxInflightLimit/max(permitsEach, 1), 1))
}

// samplePermit stamps an operation with the generation of the limit it was
// dispatched under. A completion carrying a superseded generation measured a
// different limit, so the controller discards it.
type samplePermit struct {
	generation uint64
}

// inflightController adapts a pipeline's inflight limit, doubling it while
// that raises goodput and backing off when goodput declines. Goodput is
// estimated once per window of completions; see record.
type inflightController struct {
	// immutable after construction, so they are read without the mutex
	floor    int
	capacity int
	scale    int
	log      *zap.Logger

	mu    sync.Mutex
	limit int
	// probing doubles the limit while goodput climbs; it holds once a doubling
	// stops paying off.
	probing     bool
	completions int
	successes   int
	elapsed     float64 // total latency of the current window, in seconds
	// prevGoodput is zero while there is no baseline: at startup and after a
	// back-off.
	prevGoodput float64
	prevLimit   int
	goodputEMA  float64
	strikes     int
	steadyRun   int
	// generation is bumped on every limit change and stamps sample permits.
	generation uint64
}

// newInflightController returns a controller starting at initial and adapting
// within [floor, capacity]. scale is the number of sampled operations each
// limited unit dispatches; it sizes the steady-state window.
func newInflightController(initial, floor, capacity, scale int, log *zap.Logger) *inflightController {
	capacity = min(max(capacity, 1), maxInflightLimit)
	floor = min(max(floor, 1), capacity)
	limit := min(max(initial, floor), capacity)
	return &inflightController{
		floor:    floor,
		capacity: capacity,
		scale:    min(max(scale, 1), maxWindow), // a window never exceeds maxWindow
		log:      log,

		limit:     limit,
		probing:   true,
		prevLimit: limit,
	}
}

func (c *inflightController) currentLimit() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.limit
}

// sample stamps an operation about to be dispatched. Hand the permit back to
// record when the operation completes.
func (c *inflightController) sample() samplePermit {
	c.mu.Lock()
	defer c.mu.Unlock()
	return samplePermit{generation: c.generation}
}

// window is the number of completions to gather before the next decision. The
// caller must hold c.mu.
func (c *inflightController) window() int {
	if c.probing {
		return minWindow
	}
	return min(max(c.limit*c.scale, minWindow), maxWindow)
}

// record reports a completed operation, stamped by sample at dispatch, and
// returns the resulting change in the limit.
func (c *inflightController) record(permit samplePermit, elapsed time.Duration, ok bool) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if permit.generation != c.generation {
		// dispatched under a superseded limit, so it says nothing about this one
		return 0
	}
	c.completions++
	if ok {
		c.successes++
	}
	c.elapsed += elapsed.Seconds()
	if c.completions < c.window() {
		return 0
	}

	old, successes, windowElapsed := c.limit, c.successes, c.elapsed
	c.completions, c.successes, c.elapsed = 0, 0, 0

	// no goodput at all, however fast the failures came back, so back off
	// without waiting out the strike count, and settle rather than probe back
	// up to the limit that just timed every operation out
	if successes == 0 {
		if c.probing && c.prevGoodput > 0 && c.prevLimit < old {
			// a failed probe returns to the level that was working
			c.settle(c.prevLimit)
		} else {
			// a new level, whose first healthy window seeds the baseline
			c.settle(old / 2)
			c.goodputEMA = 0
		}
		c.prevGoodput = 0
		return c.applyLimit(old, 0)
	}
	if windowElapsed <= 0 {
		// too fast to measure, so it says nothing either way
		return 0
	}

	// Little's law: throughput ≈ inflight / latency
	goodput := float64(successes) * float64(old) / windowElapsed

	var adverse bool
	if c.prevGoodput > 0 {
		if c.probing {
			// each doubling has to keep paying off
			adverse = goodput < c.prevGoodput*(1+riseMargin)
		} else {
			// once settled, only a drop below the smoothed peak at an
			// unchanged limit counts against it
			adverse = old == c.prevLimit && goodput < c.goodputEMA*(1-declineMargin)
		}
	}
	if adverse && c.strikes+1 < confirmWindows {
		c.strikes++
		return 0
	}
	c.strikes = 0

	doubled := min(old*2, c.capacity)
	switch {
	case !adverse && c.probing: // no baseline yet, or still paying off, so climb
		c.limit = doubled
		c.prevGoodput = goodput

	case !adverse: // settled and healthy
		if c.goodputEMA == 0 {
			// a window without a success cleared the baseline, so seed it
			c.goodputEMA = goodput
		} else {
			c.goodputEMA = emaAlpha*goodput + (1-emaAlpha)*c.goodputEMA
		}
		c.steadyRun++
		if c.steadyRun >= probeInterval && old < c.capacity {
			// probe upward in case capacity has freed up
			c.steadyRun = 0
			c.probing = true
			c.limit = doubled
		}
		c.prevGoodput = goodput

	case c.probing: // the last doubling did not pay off, settle at the level below it
		c.settle(c.prevLimit)
		c.goodputEMA = goodput
		c.prevGoodput = goodput

	default: // sustained decline at a settled limit, back off and probe again
		c.backOff(old)
	}
	return c.applyLimit(old, goodput)
}

// settle holds the limit at limit, within the bounds, and stops probing. The
// caller must hold c.mu.
func (c *inflightController) settle(limit int) {
	c.limit = min(max(limit, c.floor), c.capacity)
	c.strikes = 0
	c.probing = false
	c.steadyRun = 0
}

// backOff halves the limit, down to the floor, and clears the baseline so the
// controller probes upward again from there. The caller must hold c.mu.
func (c *inflightController) backOff(old int) {
	c.limit = max(old/2, c.floor)
	c.strikes = 0
	c.probing = true
	c.prevGoodput = 0
	c.steadyRun = 0
}

// applyLimit records the limit the decision replaced and reports the change it
// made. A change bumps the generation, retiring permits sampled under old. The
// caller must hold c.mu.
func (c *inflightController) applyLimit(old int, goodput float64) int {
	c.prevLimit = old
	delta := c.limit - old
	if delta != 0 {
		c.generation++
		c.log.Debug("adjusted inflight limit",
			zap.Int("from", old),
			zap.Int("to", c.limit),
			zap.Bool("probing", c.probing),
			zap.Float64("goodput", goodput))
	}
	return delta
}

// inflightLimiter admits operations at its controller's current limit. acquire
// issues one permit per operation about to be dispatched; commit takes them in
// bulk for work buffered ahead of dispatch. A permit is a shard upload on the
// upload path and a chunk recovery on the download path, not one of the
// per-host inflight reservations the host client tracks for scoring.
type inflightLimiter struct {
	controller *inflightController

	mu        sync.Mutex
	inflight  int // permits held by acquire and tryAcquire callers
	committed int // permits held by buffered work
	parked    int // callers blocked in wait

	pending []*commitment // callers in commit, in arrival order

	// changed closes whenever a permit is freed or the limit grows, waking
	// every parked caller to re-check, then a fresh channel takes its place.
	// Like [changeCounter], handing the channel out under mu is what stops a
	// waiter from missing a change between its check and its park. One channel
	// serves acquirers and committers alike; each re-runs only its own admit,
	// so a wakeup it cannot use costs it just the re-check.
	changed chan struct{}
}

func newInflightLimiter(initial, floor, capacity, scale int, log *zap.Logger) *inflightLimiter {
	return &inflightLimiter{
		controller: newInflightController(initial, floor, capacity, scale, log),
		changed:    make(chan struct{}),
	}
}

// notify wakes every parked caller. The caller must hold l.mu.
func (l *inflightLimiter) notify() {
	close(l.changed)
	l.changed = make(chan struct{})
}

// wait blocks until admit succeeds or ctx is done, reporting whether it
// succeeded. admit is called with l.mu held, so it sees a consistent view of
// the limiter and may take what it admits. notify must be called whenever
// admit's condition may have become true.
func (l *inflightLimiter) wait(ctx context.Context, admit func() bool) bool {
	for {
		l.mu.Lock()
		if admit() {
			l.mu.Unlock()
			return true
		}
		// read the wakeup channel while still holding mu, so a change landing
		// between the failed admit and the park cannot be missed
		changed := l.changed
		l.parked++
		l.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
		}

		l.mu.Lock()
		l.parked--
		l.mu.Unlock()
		if ctx.Err() != nil {
			return false
		}
	}
}

// acquire blocks until the limit leaves room for another operation. The
// returned release must be called exactly once, when the operation finishes.
func (l *inflightLimiter) acquire(ctx context.Context) (release func(), ok bool) {
	if !l.wait(ctx, l.admit) {
		return nil, false
	}
	return l.release, true
}

// tryAcquire acquires a permit only if the limit leaves room for one right now.
func (l *inflightLimiter) tryAcquire() (release func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.admit() {
		return nil, false
	}
	return l.release, true
}

// admit takes a permit if the limit allows. The caller must hold l.mu.
func (l *inflightLimiter) admit() bool {
	if l.inflight >= l.controller.currentLimit() {
		return false
	}
	l.inflight++
	return true
}

func (l *inflightLimiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inflight--
	l.notify()
}

// commit blocks until n more permits fit in the backlog, then takes them. The
// backlog may reach the current limit plus n and otherwise stays within the
// capacity. A batch larger than the whole capacity is admitted on its own
// rather than parked forever, since nothing it waits on could ever free enough.
// Batches are served in arrival order, so a stream of smaller batches cannot
// keep a larger one parked forever.
func (l *inflightLimiter) commit(ctx context.Context, n int) (*commitment, bool) {
	c := &commitment{limiter: l, remaining: n}
	l.mu.Lock()
	l.pending = append(l.pending, c)
	l.mu.Unlock()
	// leave the line whether admitted or not, so the next batch gets its turn
	defer func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.pending = slices.DeleteFunc(l.pending, func(p *commitment) bool { return p == c })
		l.notify()
	}()

	admit := func() bool {
		if l.pending[0] != c || l.committed >= l.controller.currentLimit()+n || l.committed+n > max(l.controller.capacity, n) {
			return false
		}
		l.committed += n
		return true
	}
	if !l.wait(ctx, admit) {
		return nil, false
	}
	return c, true
}

// sample stamps an operation about to be dispatched; see
// [inflightController.sample].
func (l *inflightLimiter) sample() samplePermit {
	return l.controller.sample()
}

// record feeds a completed operation to the controller, notifying waiters when
// the new limit opens permits.
func (l *inflightLimiter) record(permit samplePermit, elapsed time.Duration, ok bool) {
	delta := l.controller.record(permit, elapsed, ok)
	if delta <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// a larger limit can admit several waiters at once, so wake them all rather
	// than guess how many permits the delta opened
	l.notify()
}

// commitment holds the permits taken by [inflightLimiter.commit], one per
// encoded shard of a buffered slab. Permits are freed as the shards they cover
// finish uploading, so the backlog frees up incrementally.
type commitment struct {
	limiter   *inflightLimiter
	remaining int // guarded by limiter.mu
}

// releaseOne frees a single permit, or nothing if they have all been freed
// already.
func (c *commitment) releaseOne() {
	c.limiter.mu.Lock()
	defer c.limiter.mu.Unlock()
	c.free(1)
}

// releaseAll frees every permit still held.
func (c *commitment) releaseAll() {
	c.limiter.mu.Lock()
	defer c.limiter.mu.Unlock()
	c.free(c.remaining)
}

// free frees n permits, clamped to what is still held. The caller must hold
// limiter.mu.
func (c *commitment) free(n int) {
	if n = min(n, c.remaining); n == 0 {
		return
	}
	c.remaining -= n
	c.limiter.committed -= n
	c.limiter.notify()
}
