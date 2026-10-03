//go:build !windows

package main

import (
	"runtime"
	"syscall"
)

// peakRSS is the interesting memory number for this comparison, because the
// Rust side allocates outside the Go heap and Go's own stats cannot see it.
func peakRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	// ru_maxrss is bytes on darwin and kilobytes on linux.
	if runtime.GOOS == "darwin" {
		return int64(ru.Maxrss)
	}
	return int64(ru.Maxrss) * 1024
}
