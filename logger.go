package siastorage

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// The verbosity levels sia_set_logger accepts. The header defines them as
// 1=error 2=warn 3=info 4=debug 5=trace; anything the native side emits above
// the ceiling is never formatted, let alone handed across.
const (
	rustLevelError = 1
	rustLevelWarn  = 2
	rustLevelInfo  = 3
	rustLevelDebug = 4
)

// rustLogLevel picks the loudest level worth asking the native side for, which
// is the loudest one log will actually accept. Asking for more means Rust
// formats records that are discarded the moment they reach goLogMessage, and
// formatting happens on the Rust runtime's own threads.
//
// Trace is never requested. zap has no level below debug, so a trace record
// would arrive here only to be logged as a debug one, which is a lot of work
// for output that cannot be distinguished from what debug already gives.
//
// A nil logger still installs the hook at the error level rather than
// silencing it, because SetLogger can be called again with a real logger and
// the ceiling cannot be raised afterwards.
func rustLogLevel(log *zap.Logger) int32 {
	if log == nil {
		return rustLevelError
	}
	core := log.Core()
	switch {
	case core.Enabled(zapcore.DebugLevel):
		return rustLevelDebug
	case core.Enabled(zapcore.InfoLevel):
		return rustLevelInfo
	case core.Enabled(zapcore.WarnLevel):
		return rustLevelWarn
	default:
		return rustLevelError
	}
}
