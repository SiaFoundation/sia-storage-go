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

// progressQueue bounds how many shard events may be waiting for a handler
// before the sink starts dropping them.
const progressQueue = 1024

var (
	progressSinks  sync.Map
	progressNextID atomic.Uintptr

	globalLogger atomic.Pointer[zap.Logger]
	loggerOnce   sync.Once
)

// errCancelled is returned when an FFI call is interrupted by its
// cancellation token outside of a context (e.g. by closing a stream).
var errCancelled = errors.New("operation cancelled")

// A progressSink separates the Rust runtime thread that reports a finished
// shard from the goroutine that runs the caller's handler.
type progressSink struct {
	ch      chan ShardProgress
	done    chan struct{}
	bytes   atomic.Uint64
	dropped atomic.Uint64

	mu     sync.RWMutex
	closed bool
}

// send stamps the running total onto the event and enqueues it without ever
// blocking. The read lock keeps a concurrent close from closing the channel
// underneath the send.
func (s *progressSink) send(p ShardProgress) {
	p.Transferred = s.bytes.Add(p.ShardSize)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	select {
	case s.ch <- p:
	default:
		s.dropped.Add(1)
	}
}

// close stops the sink and waits for the queue to drain, so once it returns the
// handler is guaranteed not to fire again. The returned total counts dropped
// events.
func (s *progressSink) close() (transferred, dropped uint64) {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	s.mu.Unlock()
	<-s.done
	return s.bytes.Load(), s.dropped.Load()
}

// registerProgress starts a sink delivering to fn on its own goroutine and
// returns the handle the C side carries as userdata. A nil fn registers nothing
// and yields the zero handle, which the trampoline ignores.
func registerProgress(fn func(ShardProgress)) uintptr {
	if fn == nil {
		return 0
	}
	s := &progressSink{
		ch:   make(chan ShardProgress, progressQueue),
		done: make(chan struct{}),
	}
	id := progressNextID.Add(1)
	progressSinks.Store(id, s)
	go func() {
		defer close(s.done)
		for p := range s.ch {
			deliver(fn, p)
		}
	}()
	return id
}

// deliver runs the caller's handler with a recover in place. A panic unwinding
// from Go into Rust would terminate the process, so a handler that panics costs
// its own event and nothing further.
func deliver(fn func(ShardProgress), p ShardProgress) {
	defer func() { _ = recover() }()
	fn(p)
}

// unregisterProgress closes the sink for id and waits for its queued events to
// reach the handler. It must be called only once the FFI call that can produce
// events has returned.
func unregisterProgress(id uintptr) (transferred, dropped uint64) {
	if id == 0 {
		return 0, 0
	}
	v, ok := progressSinks.LoadAndDelete(id)
	if !ok {
		return 0, 0
	}
	return v.(*progressSink).close()
}

// lookupProgress returns the sink registered for id, or nil once it has been
// unregistered.
func lookupProgress(id uintptr) *progressSink {
	v, ok := progressSinks.Load(id)
	if !ok {
		return nil
	}
	return v.(*progressSink)
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

// newCancelToken creates a C cancellation token that fires when cancel is
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
	// An already cancelled context has to fire the token before this returns.
	// Leaving it to the watcher goroutine is a race that a fast call wins, and
	// the call then runs as though it were never cancelled.
	select {
	case <-ctx.Done():
		cancel()
		return tok, free
	default:
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

// GenerateRecoveryPhrase returns a new 12 word BIP-39 recovery phrase, from
// which [Builder.Register] derives an app key.
func GenerateRecoveryPhrase() string {
	return goString(C.sia_generate_recovery_phrase())
}
