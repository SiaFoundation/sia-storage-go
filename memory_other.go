//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package siastorage

// systemMemory returns zero on platforms without a native implementation,
// causing defaultMemoryBudget to use its conservative fallback.
func systemMemory() uint64 { return 0 }
