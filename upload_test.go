package siastorage

import (
	"bytes"
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// racingShardUpload builds a shardUpload whose pool holds one slow host and
// five fast hosts. Only the fast hosts have write samples, and the picker
// prefers unsampled hosts, so the slow host wins the initial pick while the
// seeded samples keep the race timeout small enough for a racer to beat it.
func racingShardUpload(t *testing.T, slowDelay time.Duration, waiting *changeCounter) (*shardUpload, types.PublicKey) {
	t.Helper()
	sdk, hosts := newTestSDK(t, 6, zaptest.NewLogger(t))
	t.Cleanup(func() { sdk.Close() })

	usable, _ := hosts.hosts.UsableHosts()
	slow := usable[0].PublicKey
	var fast []types.PublicKey
	for _, hi := range usable[1:] {
		fast = append(fast, hi.PublicKey)
	}

	// seed fast write samples so the race timeout fires before the slow host
	for _, hk := range fast {
		hosts.provider.AddWriteSample(hk, proto.SectorSize, 10*time.Millisecond)
	}
	hosts.SetSlowHostKeys([]types.PublicKey{slow}, slowDelay)

	candidates := append([]types.PublicKey{slow}, fast...)
	su := &shardUpload{
		hosts:      hosts,
		accountKey: sdk.appKey,
		sema:       make(chan struct{}, 4),
		pool:       newUploadPool(hosts, candidates, 1),
		shardsCh:   make(chan shard, 1),
		waiting:    waiting,
	}
	return su, slow
}

func TestUploadRacing(t *testing.T) {
	// upload one slab where 30 of the 50 hosts are slow. the race timeout fires
	// first, so racers grab the leftover shards from the 20 fast hosts
	sdk, hosts := newTestSDK(t, 50, zaptest.NewLogger(t))
	defer sdk.Close()

	slowHosts := hosts.SetSlowHosts(t, 30, 5*time.Second)
	slowSet := make(map[types.PublicKey]bool, len(slowHosts))
	for _, hk := range slowHosts {
		slowSet[hk] = true
	}

	// give every host a fast write sample so the race timeout is well under 5s.
	// without it the first estimate is about 5s and racing never kicks in
	usable, _ := hosts.hosts.UsableHosts()
	for _, hi := range usable {
		hosts.provider.AddWriteSample(hi.PublicKey, proto.SectorSize, 10*time.Millisecond)
	}

	data := frand.Bytes(4096)
	obj := NewEmptyObject()
	err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data))

	// assert the upload produced one slab
	if err != nil {
		t.Fatal(err)
	} else if len(obj.Slabs()) != 1 {
		t.Fatal("unexpected slab count", len(obj.Slabs()))
	}

	// assert racers fired: more than the 30 primary shards got write calls
	var totalWrites int
	for _, n := range hosts.WriteCalls() {
		totalWrites += n
	}
	if totalWrites <= 30 {
		t.Fatal("expected racing to produce extra writes", totalWrites)
	}

	// assert at least some shards landed on the fast hosts
	var fastCount int
	for _, sector := range obj.Slabs()[0].Sectors {
		if !slowSet[sector.HostKey] {
			fastCount++
		}
	}
	if fastCount == 0 {
		t.Fatal("expected shards on fast hosts")
	}

	// assert the data roundtrips
	got, err := readAll(sdk.Download(obj))
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}

	// a shard whose initial host is slow must not race while another shard is
	// still waiting for its first attempt
	waiting := newChangeCounter(1)
	su, slow := racingShardUpload(t, 600*time.Millisecond, waiting)
	waiting.add(1) // this shard's own pending attempt
	sector := make([]byte, proto.SectorSize)
	start := time.Now()
	go su.uploadShard(t.Context(), 0, sector)
	res := <-su.shardsCh

	// assert it waited for the slow host instead of racing
	if res.err != nil {
		t.Fatal(res.err)
	} else if res.host != slow {
		t.Fatal("expected slow host", res.host)
	} else if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Fatal("gated shard raced", elapsed)
	}

	// the other shard starting its attempt at 150ms opens the gate, so racing
	// should begin then instead of waiting out the 1500ms slow host
	waiting = newChangeCounter(1)
	su, slow = racingShardUpload(t, 1500*time.Millisecond, waiting)
	waiting.add(1) // this shard's own pending attempt
	time.AfterFunc(150*time.Millisecond, func() { waiting.add(-1) })
	sector = make([]byte, proto.SectorSize)
	start = time.Now()
	go su.uploadShard(t.Context(), 0, sector)
	res = <-su.shardsCh

	// assert a racer won once the gate opened
	if res.err != nil {
		t.Fatal(res.err)
	} else if res.host == slow {
		t.Fatal("expected racer to win once idle")
	} else if elapsed := time.Since(start); elapsed < 140*time.Millisecond {
		t.Fatal("raced before the gate opened", elapsed)
	} else if elapsed >= 1000*time.Millisecond {
		t.Fatal("did not race when the gate opened", elapsed)
	}
}

func TestUploadGateReleasedOnCancel(t *testing.T) {
	waiting := newChangeCounter(0)
	su, _ := racingShardUpload(t, 600*time.Millisecond, waiting)

	// fill the semaphore so the initial permit acquire blocks
	for range cap(su.sema) {
		su.sema <- struct{}{}
	}

	waiting.add(1)
	ctx, cancel := context.WithCancel(t.Context())
	sector := make([]byte, proto.SectorSize)
	done := make(chan struct{})
	go func() {
		su.uploadShard(ctx, 0, sector)
		close(done)
	}()

	cancel() // cancel before any permit frees
	res := <-su.shardsCh
	<-done

	if res.err == nil {
		t.Fatal("expected cancellation error")
	} else if waiting.load() != 0 {
		t.Fatal("waiting gate leaked", waiting.load())
	}
}

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
