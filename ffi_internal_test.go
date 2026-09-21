package siastorage

import (
	"strings"
	"testing"
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
