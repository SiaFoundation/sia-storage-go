package siastorage

import (
	"errors"
	"strings"
	"testing"
)

// These tests reach the error mapping directly, which only works because
// mapError takes plain Go types. Through goError it would be unreachable: a
// _test.go file cannot import "C", so a cgo signature cannot be called from
// one.

// TestMapErrorStatusCodes pins every status the C ABI defines to the sentinel
// callers match with errors.Is. A code losing its sentinel turns a failure the
// caller could branch on into an opaque string.
func TestMapErrorStatusCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int32
		msg  string
		want error
	}{
		{"ok", statusOK, "", nil},
		{"cancelled", statusCancelled, "cancelled", errCancelled},
		{"unauthorized", statusUnauthorized, "nope", ErrUnauthorized},
		{"user rejected", statusUserRejected, "nope", ErrUserRejected},
		{"request expired", statusRequestExpired, "too late", ErrRequestExpired},
		{"not attached", statusObjectNotAttached, "nope", ErrObjectNotAttached},
		{"key mismatch", statusKeyMismatch, "nope", ErrKeyMismatch},
		{"invalid state", statusInvalidState, "upload already finished", ErrInvalidState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := mapError(tc.code, tc.msg)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("mapError(%d) = %v, which does not match its sentinel", tc.code, err)
			}
		})
	}
}

// TestMapErrorKeepsTheMessage proves a sentinel that carries context does not
// throw the native message away, since that message is usually the only thing
// saying which host or slab failed.
func TestMapErrorKeepsTheMessage(t *testing.T) {
	const msg = "upload already finished"
	err := mapError(statusInvalidState, msg)
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("lost the sentinel: %v", err)
	}
	if err.Error() != msg {
		t.Fatalf("expected the native message %q, got %q", msg, err.Error())
	}
}

// TestMapErrorNeverReturnsABlankError covers the case that produced a non-nil
// error printing as nothing: a status with no message, which the C ABI does
// for SIA_ERR_INVALID_HANDLE and could do for any future code.
func TestMapErrorNeverReturnsABlankError(t *testing.T) {
	for _, code := range []int32{statusInvalidHandle, 9999, -1} {
		err := mapError(code, "")
		if err == nil {
			t.Fatalf("code %d produced no error at all", code)
		}
		if strings.TrimSpace(err.Error()) == "" {
			t.Fatalf("code %d produced an error that prints as nothing", code)
		}
	}
}

// TestMessageSentinels covers the two failure modes the C ABI gives no status
// code of its own, so they are recovered from the message text. Nothing on
// either side of the boundary pins those strings, which makes this the only
// thing standing between a rewording in Rust and callers silently losing the
// ability to branch on the two errors they most need to.
func TestMessageSentinels(t *testing.T) {
	for _, tc := range []struct {
		msg  string
		want error
	}{
		{"not enough shards", ErrNotEnoughShards},
		{"slab 3: not enough shards to recover", ErrNotEnoughShards},
		{"no more hosts available", ErrNoMoreHosts},
		{"upload failed: no more hosts available after 4 attempts", ErrNoMoreHosts},
	} {
		err := mapError(1, tc.msg)
		if !errors.Is(err, tc.want) {
			t.Errorf("%q did not map to its sentinel, got %v", tc.msg, err)
		}
		if err.Error() != tc.msg {
			t.Errorf("expected the message preserved, got %q", err.Error())
		}
	}

	// An unrelated message must stay unrelated rather than being swept into
	// whichever sentinel happens to share a word.
	err := mapError(1, "host refused the connection")
	for _, s := range []error{ErrNotEnoughShards, ErrNoMoreHosts} {
		if errors.Is(err, s) {
			t.Errorf("an unrelated message matched %v", s)
		}
	}
}
