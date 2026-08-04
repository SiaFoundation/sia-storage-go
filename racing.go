package siastorage

import "sync"

const (
	// raceFactor sets how long we wait before racing. We only start a second
	// attempt once a host takes more than raceFactor times the normal time, so
	// only the real stragglers get raced.
	raceFactor = 1.5

	// raceWindow limits how far ahead of the read head a chunk may race. Racing
	// chunks the reader does not need yet would steal bandwidth from the ones it
	// is waiting on.
	raceWindow = 3
)

// changeCounter is a number that goroutines can read and wait on. Every change
// to the number closes a channel to wake up anyone waiting, then swaps in a
// fresh channel for the next change. snapshot hands back the number and that
// wakeup channel together while holding the lock, so a waiter can never miss a
// change that happens right after it reads the number.
type changeCounter struct {
	mu sync.Mutex
	v  int
	ch chan struct{}
}

func newChangeCounter(v int) *changeCounter {
	return &changeCounter{v: v, ch: make(chan struct{})}
}

// add applies delta and returns the new value, waking any waiters.
func (c *changeCounter) add(delta int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v += delta
	close(c.ch)
	c.ch = make(chan struct{})
	return c.v
}

func (c *changeCounter) load() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
}

// snapshot returns the current value plus the channel that closes on the next
// change.
func (c *changeCounter) snapshot() (int, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v, c.ch
}
