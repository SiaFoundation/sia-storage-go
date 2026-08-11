//go:build linux

package siastorage

import "testing"

func TestCgroupPaths(t *testing.T) {
	v2, v1 := cgroupPaths("0::/containers/app\n5:cpu,cpuacct:/ignored\n7:memory,devices:/legacy/app\n")
	if v2 != "/containers/app" {
		t.Fatal("unexpected cgroup v2 path", v2)
	} else if v1 != "/legacy/app" {
		t.Fatal("unexpected cgroup v1 memory path", v1)
	}
}

func TestCgroupFile(t *testing.T) {
	tests := []struct {
		name  string
		group string
		want  string
	}{
		{"cgroup", "/containers/app", "/sys/fs/cgroup/containers/app/memory.max"},
		{"hierarchy root", "/", "/sys/fs/cgroup/memory.max"},
		{"unknown", "", ""},
		{"outside namespace", "/../../../../init.scope", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cgroupFile(cgroupV2Root, tt.group, "memory.max"); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestSystemMemory(t *testing.T) {
	if total := systemMemory(); total == 0 {
		t.Fatal("failed to query system memory")
	}
}
