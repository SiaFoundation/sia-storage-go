package siastorage

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// cgo cannot be used directly from a _test.go file, so these tests reach the
// C ABI only through the wrappers in ffi.go. Anything the Go package does not
// wrap is unreachable from a test, which is why the wrappers come first.

// TestGenerateRecoveryPhrase proves the static library is linked into the test
// binary, that the Rust side runs, and that a heap-allocated string crosses the
// boundary and is freed by Go without tripping the allocator.
func TestGenerateRecoveryPhrase(t *testing.T) {
	seen := make(map[string]struct{})
	for range 32 {
		phrase := GenerateRecoveryPhrase()
		words := strings.Fields(phrase)
		if len(words) != 12 {
			t.Fatalf("expected a 12 word recovery phrase, got %d words (%q)", len(words), phrase)
		}
		seen[phrase] = struct{}{}
	}
	// A constant phrase would still satisfy the word count while meaning the
	// Rust side never reached its RNG.
	if len(seen) != 32 {
		t.Fatalf("expected 32 distinct phrases, got %d", len(seen))
	}
}

// TestCancelTokenLifecycle allocates, fires and frees the cancellation handle
// that every blocking FFI call takes, in the order ffi.go's callers use it.
func TestCancelTokenLifecycle(t *testing.T) {
	tok, cancel, release := newCancelToken()
	if tok == nil {
		t.Fatal("sia_cancel_new returned nil")
	}
	cancel()
	cancel() // cancelling twice must be harmless
	release()
}

// The progress sink is plain Go, so unlike the C entry points it can be driven
// directly. These tests cover each guarantee the ShardProgress doc comment
// makes, since callers rely on them to write ordinary handlers.

// TestProgressSinkDelivers proves every event reaches the handler in order, and
// that unregisterProgress waits for the queue to drain before returning.
func TestProgressSinkDelivers(t *testing.T) {
	const events = 256
	var got []int
	id := registerProgress(func(p ShardProgress) {
		got = append(got, p.ShardIndex)
	})
	s := lookupProgress(id)
	if s == nil {
		t.Fatal("registerProgress returned an unknown handle")
	}
	for i := range events {
		s.send(ShardProgress{ShardIndex: i})
	}
	if _, dropped := unregisterProgress(id); dropped != 0 {
		t.Fatalf("expected no drops from a handler that keeps up, got %d", dropped)
	}
	if len(got) != events {
		t.Fatalf("expected %d events, got %d", events, len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("event %d arrived out of order as %d", i, v)
		}
	}
}

// TestProgressSinkAbsorbsPanic proves a panicking handler costs its own event
// and nothing else. Without the recover the panic would unwind into Rust and
// take the process down.
func TestProgressSinkAbsorbsPanic(t *testing.T) {
	const events = 8
	delivered := 0
	id := registerProgress(func(p ShardProgress) {
		if p.ShardIndex == 0 {
			panic("a handler written by a caller")
		}
		delivered++
	})
	s := lookupProgress(id)
	for i := range events {
		s.send(ShardProgress{ShardIndex: i})
	}
	unregisterProgress(id)
	if delivered != events-1 {
		t.Fatalf("expected the %d events after the panic to survive, got %d", events-1, delivered)
	}
}

// TestProgressSinkDropsRatherThanBlocks proves a stalled handler cannot stall
// the trampoline, which is what keeps a slow caller from throttling a transfer.
// Every event must be either delivered or counted as dropped.
func TestProgressSinkDropsRatherThanBlocks(t *testing.T) {
	release := make(chan struct{})
	var handled atomic.Uint64
	id := registerProgress(func(ShardProgress) {
		<-release
		handled.Add(1)
	})
	s := lookupProgress(id)

	const sent = progressQueue * 4
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range sent {
			s.send(ShardProgress{ShardIndex: i})
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("send blocked while the handler was stalled")
	}

	close(release)
	_, dropped := unregisterProgress(id)
	if dropped == 0 {
		t.Fatal("expected the overflow to be counted as dropped")
	}
	if total := handled.Load() + dropped; total != sent {
		t.Fatalf("expected all %d events delivered or counted, got %d", sent, total)
	}
}

// TestProgressSinkTransferredIsExact proves the running total is the sum of
// every shard seen so far, so a handler that keeps up can drive progress
// straight from the last event it received.
func TestProgressSinkTransferredIsExact(t *testing.T) {
	const (
		shard  = 4 << 20
		events = 64
	)
	var last uint64
	id := registerProgress(func(p ShardProgress) { last = p.Transferred })
	s := lookupProgress(id)
	for range events {
		s.send(ShardProgress{ShardSize: shard})
	}
	transferred, dropped := unregisterProgress(id)
	if dropped != 0 {
		t.Fatalf("expected no drops, got %d", dropped)
	}
	if want := uint64(events) * shard; last != want || transferred != want {
		t.Fatalf("expected %d bytes from both the last event and the sink, got %d and %d",
			want, last, transferred)
	}
}

// TestProgressSinkTransferredSurvivesDrops proves the running total stays exact
// through heavy loss, which is why a handler must read Transferred rather than
// sum what it receives. It also pins the limit of that guarantee, namely that
// the last surviving event can fall short of the true total when the tail is
// dropped, so a completed transfer has to publish the sink's figure instead.
func TestProgressSinkTransferredSurvivesDrops(t *testing.T) {
	const (
		shard = 4 << 20
		sent  = progressQueue * 3
	)
	release := make(chan struct{})
	var seen []uint64
	id := registerProgress(func(p ShardProgress) {
		<-release
		seen = append(seen, p.Transferred)
	})
	s := lookupProgress(id)
	for range sent {
		s.send(ShardProgress{ShardSize: shard})
	}
	close(release)
	transferred, dropped := unregisterProgress(id)

	if dropped == 0 {
		t.Fatal("expected drops, the guarantee is untested without them")
	}
	want := uint64(sent) * shard
	if transferred != want {
		t.Fatalf("expected the sink to count every byte, wanted %d and got %d", want, transferred)
	}
	// Whatever survived must be a genuine running total, strictly increasing
	// and always a whole number of shards.
	for i, v := range seen {
		if v%shard != 0 {
			t.Fatalf("event %d reported %d bytes, which is not a whole number of shards", i, v)
		}
		if i > 0 && v <= seen[i-1] {
			t.Fatalf("event %d reported %d after %d, the total must only grow", i, v, seen[i-1])
		}
	}
	// The documented limit. Summing deliveries undercounts, and even the final
	// delivery may, which is why the authoritative number comes from the sink.
	var summed uint64
	for range seen {
		summed += shard
	}
	if summed >= want {
		t.Fatalf("expected summing deliveries to undercount %d, got %d", want, summed)
	}
	t.Logf("delivered %d of %d events, last event reported %.1f GiB of %.1f GiB actual",
		len(seen), sent, float64(seen[len(seen)-1])/(1<<30), float64(want)/(1<<30))
}

// TestProgressSinkIgnoresNilHandler proves the zero handle is inert. It is what
// the C side carries whenever a caller registers no handler at all.
func TestProgressSinkIgnoresNilHandler(t *testing.T) {
	if id := registerProgress(nil); id != 0 {
		t.Fatalf("expected the zero handle for a nil handler, got %d", id)
	}
	if lookupProgress(0) != nil {
		t.Fatal("the zero handle must not resolve to a sink")
	}
	if _, dropped := unregisterProgress(0); dropped != 0 {
		t.Fatalf("unregistering the zero handle must be harmless, got %d", dropped)
	}
}
