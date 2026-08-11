//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package siastorage

import (
	"runtime"

	"golang.org/x/sys/unix"
)

func systemMemory() uint64 {
	var names []string
	switch runtime.GOOS {
	case "darwin":
		names = []string{"hw.memsize"}
	case "netbsd", "openbsd":
		names = []string{"hw.physmem64", "hw.physmem"}
	default: // DragonFly and FreeBSD
		names = []string{"hw.physmem", "hw.realmem"}
	}
	for _, name := range names {
		if total, err := unix.SysctlUint64(name); err == nil && total > 0 {
			return total
		}
	}
	return 0
}
