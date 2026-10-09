package siastorage

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestRustLogLevel pins what verbosity the native side is asked for. Asking
// for more than the logger accepts means Rust formats records that are thrown
// away the moment they cross, on its own runtime threads.
func TestRustLogLevel(t *testing.T) {
	at := func(l zapcore.Level) *zap.Logger {
		return zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zapcore.AddSync(discard{}),
			l,
		))
	}

	for _, tc := range []struct {
		name string
		log  *zap.Logger
		want int32
	}{
		{"debug logger asks for debug", at(zapcore.DebugLevel), rustLevelDebug},
		{"info logger asks for info", at(zapcore.InfoLevel), rustLevelInfo},
		{"warn logger asks for warn", at(zapcore.WarnLevel), rustLevelWarn},
		{"error logger asks for error", at(zapcore.ErrorLevel), rustLevelError},
		{"nil logger still installs the hook", nil, rustLevelError},
		{"nop logger asks for the minimum", zap.NewNop(), rustLevelError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rustLogLevel(tc.log); got != tc.want {
				t.Fatalf("rustLogLevel = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestRustLogLevelNeverExceedsDebug guards the one level the bridge cannot
// represent: zap has nothing below debug, so a trace record would arrive only
// to be logged as a debug one.
func TestRustLogLevelNeverExceedsDebug(t *testing.T) {
	verbose := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(discard{}),
		zapcore.Level(-127), // below debug, as low as a core can go
	))
	if got := rustLogLevel(verbose); got != rustLevelDebug {
		t.Fatalf("rustLogLevel = %d, want it capped at debug (%d)", got, rustLevelDebug)
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
