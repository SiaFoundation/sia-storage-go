package siastorage

import (
	"errors"
	"fmt"
	"strings"
)

// mapError turns a C ABI status code and its message into a Go error. It is
// deliberately free of cgo: a _test.go file cannot import "C", so anything
// reached only through a cgo signature cannot be tested at all. Keeping the
// mapping here means the sentinel behaviour callers rely on is covered without
// an archive, a Rust toolchain or the mock network.
func mapError(code int32, msg string) error {
	switch code {
	case statusOK:
		return nil
	case statusCancelled:
		return errCancelled
	case statusUnauthorized:
		return ErrUnauthorized
	case statusUserRejected:
		return ErrUserRejected
	case statusRequestExpired:
		return ErrRequestExpired
	case statusObjectNotAttached:
		return ErrObjectNotAttached
	case statusKeyMismatch:
		return ErrKeyMismatch
	case statusInvalidState:
		return &wrappedError{msg: msg, sentinel: ErrInvalidState}
	case statusInvalidHandle:
		// The C ABI returns this one without setting *err, so there is no
		// message to fall through to. It means a nil handle reached the
		// boundary, which is a bug in this package rather than a runtime
		// failure the caller can act on.
		return errors.New("invalid handle: a required handle was nil")
	}
	// A code with no message would otherwise become errors.New(""), a non-nil
	// error that prints as nothing.
	if msg == "" {
		return fmt.Errorf("sia storage error %d", code)
	}
	for _, m := range messageSentinels {
		if strings.Contains(msg, m.substring) {
			return &wrappedError{msg: msg, sentinel: m.sentinel}
		}
	}
	return errors.New(msg)
}

// messageSentinels recovers the two failure modes the C ABI does not give a
// status code of its own. Matching on message text is fragile: nothing on
// either side of the boundary pins these strings, so a rewording in Rust
// silently downgrades them to opaque errors. TestMessageSentinels is what
// makes that a test failure rather than a support ticket.
var messageSentinels = []struct {
	substring string
	sentinel  error
}{
	{"not enough shards", ErrNotEnoughShards},
	{"no more hosts available", ErrNoMoreHosts},
}
