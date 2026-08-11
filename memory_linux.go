//go:build linux

package siastorage

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// cgroup hierarchy roots, as mounted by every mainstream distribution.
const (
	cgroupV2Root = "/sys/fs/cgroup"
	cgroupV1Root = "/sys/fs/cgroup/memory"
)

// systemMemory returns the physical memory of the machine, lowered to the
// cgroup limit when the process runs under a smaller one, e.g. in a container.
// Returns zero if physical memory cannot be read.
func systemMemory() uint64 {
	physical := physicalMemory()
	if physical == 0 {
		// an unlimited cgroup v1 reports a sentinel rather than an error, so a
		// limit is only usable with a physical reading to clamp it against
		return 0
	}
	// a limit above physical memory means unlimited, not a bigger machine
	if limit := cgroupMemoryLimit(); limit > 0 && limit < physical {
		return limit
	}
	return physical
}

func physicalMemory() uint64 {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0
	}
	unit := uint64(info.Unit)
	if unit == 0 {
		unit = 1
	}
	total := uint64(info.Totalram)
	if total > math.MaxUint64/unit {
		return math.MaxUint64
	}
	return total * unit
}

// cgroupMemoryLimit returns the lowest memory limit applying to the process, or
// zero if it is unlimited or cannot be determined. Both cgroup versions are
// checked at the hierarchy root, which is what a namespaced mount exposes, and
// at the process's own path, for a host that mounts the whole hierarchy.
func cgroupMemoryLimit() uint64 {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0
	}
	v2Path, v1Path := cgroupPaths(string(data))

	var limit uint64
	for _, filename := range []string{
		filepath.Join(cgroupV2Root, "memory.max"),
		cgroupFile(cgroupV2Root, v2Path, "memory.max"),
		filepath.Join(cgroupV1Root, "memory.limit_in_bytes"),
		cgroupFile(cgroupV1Root, v1Path, "memory.limit_in_bytes"),
	} {
		if l := readMemoryLimit(filename); l > 0 && (limit == 0 || l < limit) {
			limit = l
		}
	}
	return limit
}

// cgroupPaths parses /proc/self/cgroup into the process's path in the v2
// unified hierarchy and its path in the v1 memory hierarchy, either of which
// may be empty.
func cgroupPaths(data string) (v2, v1 string) {
	for line := range strings.Lines(data) {
		// hierarchy ID:controller list:path, where hierarchy 0 with an empty
		// controller list is the v2 unified hierarchy
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			v2 = parts[2]
		case commaListContains(parts[1], "memory"):
			v1 = parts[2]
		}
	}
	return v2, v1
}

// cgroupFile returns the path of a cgroup's control file within its hierarchy,
// or an empty string if the cgroup is not reachable there.
func cgroupFile(root, cgroupPath, name string) string {
	if cgroupPath == "" {
		return ""
	}
	filename := filepath.Join(root, cgroupPath, name)
	// a cgroup outside the mounted namespace resolves above the root
	if !strings.HasPrefix(filename, root+"/") {
		return ""
	}
	return filename
}

// readMemoryLimit reads a cgroup memory limit, returning zero if it is missing
// or unlimited. Only cgroup v2 spells unlimited as a word; v1 uses a sentinel
// that [systemMemory] clamps away.
func readMemoryLimit(filename string) uint64 {
	if filename == "" {
		return 0
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0
	}
	limit, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil { // includes cgroup v2's "max"
		return 0
	}
	return limit
}

func commaListContains(list, value string) bool {
	for item := range strings.SplitSeq(list, ",") {
		if item == value {
			return true
		}
	}
	return false
}
