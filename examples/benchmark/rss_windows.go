//go:build windows

package main

// peakRSS has no Windows implementation. getrusage is POSIX, and the
// equivalent, GetProcessMemoryInfo, would mean taking a dependency on
// golang.org/x/sys purely so a benchmark can print one more field.
//
// Zero rather than a guess, so a reader can tell the number is absent rather
// than believing the process used no memory. Every other figure the benchmark
// reports is measured the same way on all platforms.
func peakRSS() int64 { return 0 }
