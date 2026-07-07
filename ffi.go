package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"go.uber.org/zap"
)

var (
	progressMu       sync.Mutex
	progressHandlers = make(map[uintptr]func(ShardProgress))
	progressNextID   uintptr

	globalLogger atomic.Pointer[zap.Logger]
	loggerOnce   sync.Once
)

// errCancelled is returned when an FFI call is interrupted by its
// cancellation token outside of a context (e.g. by closing a stream).
var errCancelled = errors.New("operation cancelled")

// registerProgress registers a shard progress callback and returns its
// handle. A zero handle (nil fn) is ignored by the trampoline.
func registerProgress(fn func(ShardProgress)) uintptr {
	if fn == nil {
		return 0
	}
	progressMu.Lock()
	defer progressMu.Unlock()
	progressNextID++
	progressHandlers[progressNextID] = fn
	return progressNextID
}

func unregisterProgress(id uintptr) {
	if id == 0 {
		return
	}
	progressMu.Lock()
	defer progressMu.Unlock()
	delete(progressHandlers, id)
}

func progressHandler(id uintptr) func(ShardProgress) {
	progressMu.Lock()
	defer progressMu.Unlock()
	return progressHandlers[id]
}

// setGlobalLogger routes the Rust SDK's process-wide log output to the given
// zap logger. The C-side hook is installed once; the target logger can be
// swapped at any time.
func setGlobalLogger(log *zap.Logger) {
	globalLogger.Store(log)
	loggerOnce.Do(func() {
		C.sia_set_logger(C.sia_go_log_cb(), 0, 4)
	})
}

// cancelTokenFunc creates a C cancellation token that fires when cancel is
// invoked. release frees the token; it must only be called once no FFI call
// is using it.
func newCancelToken() (tok *C.sia_cancel_t, cancel func(), release func()) {
	tok = C.sia_cancel_new()
	return tok, func() { C.sia_cancel_cancel(tok) }, func() { C.sia_cancel_free(tok) }
}

// cancelToken creates a C cancellation token wired to ctx. release must be
// called once the FFI call(s) using the token have returned.
func cancelToken(ctx context.Context) (tok *C.sia_cancel_t, release func()) {
	tok, cancel, free := newCancelToken()
	if ctx == nil || ctx.Done() == nil {
		return tok, free
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			cancel()
		case <-done:
		}
	}()
	return tok, func() {
		close(done)
		<-exited
		free()
	}
}

// goError converts an FFI status code and error message into a Go error,
// freeing the C message. ctx, when non-nil, supplies the cause for
// SIA_ERR_CANCELLED.
func goError(ctx context.Context, code C.int32_t, cerr *C.char) error {
	if code == C.SIA_OK {
		return nil
	}
	var msg string
	if cerr != nil {
		msg = C.GoString(cerr)
		C.sia_string_free(cerr)
	}
	switch code {
	case C.SIA_ERR_CANCELLED:
		if ctx != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
		}
		return errCancelled
	case C.SIA_ERR_UNAUTHORIZED:
		return ErrUnauthorized
	case C.SIA_ERR_USER_REJECTED:
		return ErrUserRejected
	case C.SIA_ERR_REQUEST_EXPIRED:
		return ErrRequestExpired
	}
	// preserve errors.Is compatibility for well-known failure modes
	if strings.Contains(msg, "not enough shards") {
		return &wrappedError{msg: msg, sentinel: ErrNotEnoughShards}
	}
	if strings.Contains(msg, "no more hosts available") {
		return &wrappedError{msg: msg, sentinel: ErrNoMoreHosts}
	}
	return errors.New(msg)
}

// wrappedError preserves the Rust error message while matching a sentinel
// with errors.Is.
type wrappedError struct {
	msg      string
	sentinel error
}

func (e *wrappedError) Error() string { return e.msg }
func (e *wrappedError) Unwrap() error { return e.sentinel }

func cBytes32(b *[32]byte) *C.uint8_t {
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

func goString(s *C.char) string {
	if s == nil {
		return ""
	}
	defer C.sia_string_free(s)
	return C.GoString(s)
}
