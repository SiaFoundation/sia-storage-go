package siastorage

import (
	"context"

	ffi "go.sia.tech/siastorage/sia_storage_ffi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// zapLogger adapts a *zap.Logger to the FFI Logger interface. The FFI layer
// emits at all levels; zap applies its own level filtering.
type zapLogger struct {
	log *zap.Logger
}

func (l zapLogger) Debug(msg string) { l.log.Debug(msg) }
func (l zapLogger) Info(msg string)  { l.log.Info(msg) }
func (l zapLogger) Warn(msg string)  { l.log.Warn(msg) }
func (l zapLogger) Error(msg string) { l.log.Error(msg) }

// setGlobalLogger routes the SDK's internal logging to log. Logging in the
// underlying Rust library is process-global, so the most recently configured
// logger wins across all SDK instances.
func setGlobalLogger(log *zap.Logger) {
	if log == nil || log.Core().Enabled(zapcore.InvalidLevel) {
		return
	}
	ffi.SetLogger(zapLogger{log}, "trace")
}

// runContext runs a blocking FFI call in a goroutine so the caller can
// observe context cancellation. The underlying FFI work is not itself
// cancellable and continues to completion in the background.
func runContext[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn()
		ch <- result{v, err}
	}()
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case r := <-ch:
		return r.v, r.err
	}
}
