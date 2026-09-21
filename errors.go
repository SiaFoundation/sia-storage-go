package siastorage

import (
	"errors"
	"fmt"
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
	case statusNotEnoughShards:
		return &wrappedError{msg: msg, sentinel: ErrNotEnoughShards}
	case statusNoMoreHosts:
		return &wrappedError{msg: msg, sentinel: ErrNoMoreHosts}
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
	return errors.New(msg)
}
