package siastorage

import (
	"bytes"
	"testing"

	"go.sia.tech/core/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"

	proto "go.sia.tech/core/rhp/v4"
)

// TestUploadInflight asserts uploads release their inflight
// reservations and avoid busy hosts.
func TestUploadInflight(t *testing.T) {
	sdk, hosts := newTestSDK(t, 40, zaptest.NewLogger(t))
	defer sdk.Close()

	// saturate 5 hosts with inflight writes so PickWrite steers the upload
	// onto the 35 idle ones
	usable, _ := hosts.hosts.UsableHosts()
	busy := make(map[types.PublicKey]bool)
	var releases []func()
	for _, hi := range usable[:5] {
		busy[hi.PublicKey] = true
		for range 5 {
			releases = append(releases, hosts.provider.TrackInflightWrite(hi.PublicKey))
		}
	}

	data := frand.Bytes(int(proto.SectorSize) * 10) // one slab, 30 shards
	obj := NewEmptyObject()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// the upload's own reservations must all be released
	hosts.waitInflightDrained(t)

	// the slab's shards should land mostly on idle hosts
	var onBusy int
	for _, slab := range obj.Slabs() {
		for _, sector := range slab.Sectors {
			if busy[sector.HostKey] {
				onBusy++
			}
		}
	}
	if onBusy > 5 {
		t.Fatal("too many shards on busy hosts, inflight not respected", onBusy)
	}

	for _, r := range releases {
		r()
	}
}
