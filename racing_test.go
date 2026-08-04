package siastorage

import (
	"testing"
	"time"
)

func TestChangeCounter(t *testing.T) {
	c := newChangeCounter(0)

	if v := c.load(); v != 0 {
		t.Fatal("unexpected", v)
	}

	// snapshot returns the current value and a channel that closes on the
	// next change
	v, changed := c.snapshot()
	if v != 0 {
		t.Fatal("unexpected", v)
	}
	select {
	case <-changed:
		t.Fatal("channel closed before any mutation")
	default:
	}

	if v := c.add(1); v != 1 {
		t.Fatal("unexpected", v)
	}

	// the channel from the earlier snapshot must now be closed
	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("snapshot channel not closed after mutation")
	}

	if v := c.add(-1); v != 0 {
		t.Fatal("unexpected", v)
	} else if v := c.load(); v != 0 {
		t.Fatal("unexpected", v)
	}
}
