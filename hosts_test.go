package siastorage

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
)

func TestUpdateHosts(t *testing.T) {
	hk1 := types.PublicKey{1}
	hk2 := types.PublicKey{2}
	hk3 := types.PublicKey{3}

	hc := newHostCache()

	// add two hosts, one GFU and one not
	hc.hosts[hk1] = hosts.HostInfo{PublicKey: hk1, GoodForUpload: true}
	hc.hosts[hk2] = hosts.HostInfo{PublicKey: hk2, GoodForUpload: false}

	// update with hk1 still good, hk2 turned good, and hk3 added as good
	warmup := hc.updateHosts([]hosts.HostInfo{
		{PublicKey: hk1, GoodForUpload: true},
		{PublicKey: hk2, GoodForUpload: true},
		{PublicKey: hk3, GoodForUpload: true},
	})

	// assert hk2 and hk3 need warmup
	warmupSet := make(map[types.PublicKey]bool)
	for _, hi := range warmup {
		warmupSet[hi.PublicKey] = true
	}
	if len(warmup) != 2 {
		t.Fatalf("expected 2 warmup hosts, got %d", len(warmup))
	} else if !warmupSet[hk2] {
		t.Fatal("expected hk2 in warmup")
	} else if !warmupSet[hk3] {
		t.Fatal("expected hk3 in warmup")
	}

	// assert cache has all three
	if len(hc.hosts) != 3 {
		t.Fatalf("expected 3 cached hosts, got %d", len(hc.hosts))
	}

	// demote hk1, keep hk2, drop hk3
	warmup = hc.updateHosts([]hosts.HostInfo{
		{PublicKey: hk1, GoodForUpload: false},
		{PublicKey: hk2, GoodForUpload: true},
	})

	// assert no warmup
	if len(warmup) != 0 {
		t.Fatalf("expected 0 warmup hosts, got %d", len(warmup))
	}

	// assert hk3 was removed
	if len(hc.hosts) != 2 {
		t.Fatalf("expected 2 cached hosts, got %d", len(hc.hosts))
	} else if _, exists := hc.hosts[hk3]; exists {
		t.Fatal("expected hk3 to be removed")
	}

	// empty update
	warmup = hc.updateHosts(nil)

	// assert cache is empty
	if len(warmup) != 0 {
		t.Fatalf("expected 0 warmup hosts, got %d", len(warmup))
	} else if len(hc.hosts) != 0 {
		t.Fatalf("expected 0 cached hosts, got %d", len(hc.hosts))
	}
}
