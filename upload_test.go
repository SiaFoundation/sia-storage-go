package siastorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// erroringReader returns err on the read that consumes the last of its data,
// then io.EOF, which io.Reader allows but io.ReadFull hides.
type erroringReader struct {
	data []byte
	err  error
}

func (r *erroringReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

// gatedReader reports when its first and second reads happen and blocks the
// second until released.
type gatedReader struct {
	firstRead  chan struct{}
	secondRead chan struct{}
	unblock    chan struct{}
	reads      int
}

func (r *gatedReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 1 {
		p[0] = 1
		close(r.firstRead)
		return 1, nil
	}
	close(r.secondRead)
	<-r.unblock
	return 0, io.EOF
}

// TestUploadReaderError asserts a failing reader is not mistaken for the end
// of the stream, which would silently truncate the object.
func TestUploadReaderError(t *testing.T) {
	const dataShards, parityShards = 3, 9

	sdk, _ := newTestSDK(t, dataShards+parityShards, zaptest.NewLogger(t))
	defer sdk.Close()

	// a truncated stream surfaces io.ErrUnexpectedEOF instead of being
	// uploaded as a short final slab
	obj := NewEmptyObject()
	err := sdk.Upload(t.Context(), &obj, &erroringReader{
		data: frand.Bytes(1000),
		err:  io.ErrUnexpectedEOF,
	}, WithRedundancy(dataShards, parityShards))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("unexpected", err)
	} else if obj.Size() != 0 {
		t.Fatal("unexpected", obj.Size())
	}

	// an error returned by the read that filled the slab is not discarded
	readErr := errors.New("read failed")
	obj = NewEmptyObject()
	err = sdk.Upload(t.Context(), &obj, &erroringReader{
		data: make([]byte, dataShards*proto.SectorSize),
		err:  readErr,
	}, WithRedundancy(dataShards, parityShards))
	if !errors.Is(err, readErr) {
		t.Fatal("unexpected", err)
	} else if obj.Size() != 0 {
		t.Fatal("unexpected", obj.Size())
	}
}

// racingShardUpload builds a shardUpload whose pool holds one slow host and
// five fast hosts. Only the fast hosts have write samples, and the picker
// prefers unsampled hosts, so the slow host wins the initial pick while the
// seeded samples keep the race timeout small enough for a racer to beat it.
func racingShardUpload(t *testing.T, slowDelay time.Duration, waiting *changeCounter) (*shardUpload, *mockHostClient, types.PublicKey) {
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

	// one slab of four shards, all of which this shard's attempts may use
	limiter := newInflightLimiter(initialUploadInflight, minUploadInflight, 4, 1, zaptest.NewLogger(t))
	slabCommitment, ok := limiter.commit(t.Context(), 4)
	if !ok {
		t.Fatal("failed to commit slab memory")
	}

	candidates := append([]types.PublicKey{slow}, fast...)
	su := &shardUpload{
		hosts:       hosts,
		accountKey:  sdk.appKey,
		hostTimeout: defaultUploadHostTimeout,
		limiter:     limiter,
		pool:        newUploadPool(hosts, candidates, 1),
		commitment:  slabCommitment,
		shardsCh:    make(chan shard, 1),
		waiting:     waiting,
	}
	return su, hosts, slow
}

// waitAvailable waits for host to be back in the pool. A cancelled attempt
// returns its host asynchronously, after releasing its reservation.
func waitAvailable(t testing.TB, p *uploadPool, host types.PublicKey) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		available := slices.Contains(p.available, host)
		p.mu.Unlock()
		if available {
			return
		} else if time.Now().After(deadline) {
			t.Fatal("expected the host back in the pool", host)
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	if totalWrites := hosts.TotalWrites(); totalWrites <= 30 {
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
	su, _, slow := racingShardUpload(t, 600*time.Millisecond, waiting)
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
	su, _, slow = racingShardUpload(t, 1500*time.Millisecond, waiting)
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

// TestUploadPoolRetry asserts only failed writes spend a host's attempt budget.
func TestUploadPoolRetry(t *testing.T) {
	sdk, hosts := newTestSDK(t, 1, zaptest.NewLogger(t))
	defer sdk.Close()

	usable, _ := hosts.hosts.UsableHosts()
	host := usable[0].PublicKey
	pool := newUploadPool(hosts, []types.PublicKey{host}, 0)

	// a host that loses a race was canceled, not failed. repeated races must
	// not spend its attempt budget or racing alone can empty the pool.
	for i := range maxHostAttempts + 1 {
		got, release, ok := pool.pickRacer()
		if !ok {
			t.Fatalf("race %d: pool ran dry without a failed write", i+1)
		} else if got != host {
			t.Fatal("unexpected host", got)
		}
		release()
		pool.restore(got)
	}

	// actual failures still spend the budget and eventually retire the host
	for i := range maxHostAttempts {
		got, release, ok := pool.pickRacer()
		if !ok {
			t.Fatalf("failure %d: expected the host back in the pool", i+1)
		} else if got != host {
			t.Fatal("unexpected host", got)
		}
		release()
		pool.retry(got)
	}
	if _, _, ok := pool.pickRacer(); ok {
		t.Fatal("expected the host removed after its last failed attempt")
	}
}

// TestUploadInitialHostBeaten asserts the host that took the shard's first
// attempt and was still uploading when a racer won is demoted, so it does not
// keep winning the initial pick over the hosts that delivered.
func TestUploadInitialHostBeaten(t *testing.T) {
	waiting := newChangeCounter(1)
	su, hosts, slow := racingShardUpload(t, 1500*time.Millisecond, waiting)
	waiting.add(1) // this shard's own pending attempt

	// the gate opens at 150ms, so a racer beats the 1500ms slow host
	time.AfterFunc(150*time.Millisecond, func() { waiting.add(-1) })
	sector := make([]byte, proto.SectorSize)
	go su.uploadShard(t.Context(), 0, sector)

	res := <-su.shardsCh
	if res.err != nil {
		t.Fatal(res.err)
	} else if res.host == slow {
		t.Fatal("expected a racer to win")
	}

	// only the beaten initial host is demoted, and only against its failure
	// rate: it was cancelled partway through an upload of unknown progress, so
	// the sector over the time the racer took to win would score it far faster
	// than it was
	hks := hosts.waitFailedRPCs(t, 1)
	hosts.waitInflightDrained(t) // let every attempt unwind before asserting
	if len(hks) != 1 {
		t.Fatalf("expected 1 failed RPC, got %d", len(hks))
	} else if hks[0] != slow {
		t.Fatalf("expected the beaten host %v, got %v", slow, hks[0])
	} else if rpcs := hosts.TimedOutRPCs(); len(rpcs) != 0 {
		t.Fatalf("expected no throughput sample from a beaten host, got %d", len(rpcs))
	}

	// the beaten host did not fail, so it returns to the slab's pool for other
	// shards to use
	waitAvailable(t, su.pool, slow)
}

// TestUploadPackedIdleHoldsNoPermits asserts a packed upload waiting for its
// first object holds no permits on the shared limiter, so idle packed uploads
// cannot park every other upload behind the lookahead gate.
func TestUploadPackedIdleHoldsNoPermits(t *testing.T) {
	sdk, _ := newTestSDK(t, 15, zaptest.NewLogger(t))
	t.Cleanup(func() { sdk.Close() })

	for range 2 {
		u, err := sdk.UploadPacked(WithRedundancy(4, 11))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { u.Close() })
	}

	// give the read loops time to reach the pipe. an upload that commits before
	// reading has done so by then
	time.Sleep(200 * time.Millisecond)
	sdk.uploadLimiter.mu.Lock()
	committed := sdk.uploadLimiter.committed
	sdk.uploadLimiter.mu.Unlock()
	if committed != 0 {
		t.Fatalf("idle packed uploads hold %d permits", committed)
	}
}

// TestUploadReservesMemoryBeforeReadingSlab asserts a blocked upload retains
// only its one-byte data probe, not a complete unaccounted raw slab.
func TestUploadReservesMemoryBeforeReadingSlab(t *testing.T) {
	const dataShards, parityShards = 3, 9
	const totalShards = dataShards + parityShards

	sdk, _ := newTestSDK(t, dataShards+parityShards, zaptest.NewLogger(t))
	t.Cleanup(func() { sdk.Close() })
	sdk.uploadLimiter = newInflightLimiter(1, 1, totalShards, 1, zaptest.NewLogger(t))

	// fill the shared memory budget so the upload has to park after probing
	// the reader but before allocating and filling its raw slab
	occupied, ok := sdk.uploadLimiter.commit(t.Context(), totalShards)
	if !ok {
		t.Fatal("failed to fill upload memory budget")
	}

	r := &gatedReader{
		firstRead:  make(chan struct{}),
		secondRead: make(chan struct{}),
		unblock:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		obj := NewEmptyObject()
		done <- sdk.Upload(ctx, &obj, r, WithRedundancy(dataShards, parityShards))
	}()

	select {
	case <-r.firstRead:
	case err := <-done:
		t.Fatal("upload stopped before probing reader:", err)
	case <-time.After(time.Second):
		t.Fatal("upload did not probe reader")
	}
	select {
	case <-r.secondRead:
		close(r.unblock)
		occupied.releaseAll()
		t.Fatal("upload continued filling slab before reserving memory")
	case <-time.After(100 * time.Millisecond):
	}

	occupied.releaseAll()
	select {
	case <-r.secondRead:
	case <-time.After(time.Second):
		t.Fatal("upload did not resume reading after memory was released")
	}
	sdk.uploadLimiter.mu.Lock()
	committed := sdk.uploadLimiter.committed
	sdk.uploadLimiter.mu.Unlock()
	if committed != totalShards {
		t.Fatalf("expected slab to hold %d permits, got %d", totalShards, committed)
	}
	close(r.unblock)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	sdk.uploadLimiter.mu.Lock()
	committed = sdk.uploadLimiter.committed
	sdk.uploadLimiter.mu.Unlock()
	if committed != 0 {
		t.Fatalf("upload leaked %d memory permits", committed)
	}
}

func TestUploadGateReleasedOnCancel(t *testing.T) {
	waiting := newChangeCounter(0)
	su, _, _ := racingShardUpload(t, 600*time.Millisecond, waiting)

	// fill the limiter so the initial permit acquire blocks
	var releases []func()
	for range su.limiter.controller.currentLimit() {
		release, ok := su.limiter.tryAcquire()
		if !ok {
			t.Fatal("failed to fill limiter")
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

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

func TestUploadMaxBufferedSlabs(t *testing.T) {
	if _, _, err := newUploadOption(WithUploadMaxBufferedSlabs(-1)); err == nil {
		t.Fatal("expected a negative max buffered slabs to fail")
	}

	// zero derives the budget from the available memory
	if uo, _, err := newUploadOption(WithUploadMaxBufferedSlabs(0), WithRedundancy(10, 20)); err != nil {
		t.Fatal(err)
	} else if uo.maxBufferedSlabs != defaultSlabsInMemory(30) {
		t.Fatal("unexpected default max buffered slabs", uo.maxBufferedSlabs)
	}

	if uo, _, err := newUploadOption(WithUploadMaxBufferedSlabs(3)); err != nil {
		t.Fatal(err)
	} else if uo.maxBufferedSlabs != 3 {
		t.Fatal("unexpected max buffered slabs", uo.maxBufferedSlabs)
	}

	// an absurd budget is clamped, so it cannot turn into an absurd buffer
	uo, _, err := newUploadOption(WithUploadMaxBufferedSlabs(maxInflightLimit+1), WithRedundancy(10, 20))
	if err != nil {
		t.Fatal(err)
	} else if want := maxInflightLimit / 30; uo.maxBufferedSlabs != want {
		t.Fatalf("expected max buffered slabs clamped to %d, got %d", want, uo.maxBufferedSlabs)
	}
}

func TestUploadHostTimeout(t *testing.T) {
	if _, _, err := newUploadOption(WithUploadHostTimeout(-time.Second)); err == nil {
		t.Fatal("expected a negative host timeout to fail")
	}

	// zero uses the default
	if uo, _, err := newUploadOption(WithUploadHostTimeout(0)); err != nil {
		t.Fatal(err)
	} else if uo.hostTimeout != defaultUploadHostTimeout {
		t.Fatal("unexpected default host timeout", uo.hostTimeout)
	}

	if uo, _, err := newUploadOption(WithUploadHostTimeout(time.Second)); err != nil {
		t.Fatal(err)
	} else if uo.hostTimeout != time.Second {
		t.Fatal("unexpected host timeout", uo.hostTimeout)
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

func TestCollectSlabMaxRedundancy(t *testing.T) {
	sdk, _, _ := newTestSDKWithMocks(t, 0, zaptest.NewLogger(t))
	defer sdk.Close()

	// 128 data and 128 parity shards is the largest redundancy the indexer
	// accepts, and it sums to exactly the uint8 boundary
	uo, _, err := newUploadOption(WithRedundancy(128, 128))
	if err != nil {
		t.Fatal(err)
	}

	const totalShards = 256
	su := shardUpload{
		encryptionKey: frand.Entropy256(),
		shardsCh:      make(chan shard, totalShards),
	}
	for i := range totalShards {
		su.shardsCh <- shard{
			index: i,
			host:  types.GeneratePrivateKey().PublicKey(),
			root:  frand.Entropy256(),
		}
	}

	res := sdk.collectSlab(t.Context(), &su, uo, 1)
	if res.err != nil {
		t.Fatal(res.err)
	} else if len(res.slab.Sectors) != totalShards {
		t.Fatal("unexpected", len(res.slab.Sectors))
	}
}

func TestUploadPinsSlabs(t *testing.T) {
	sdk, appMock, _ := newTestSDKWithMocks(t, 60, zaptest.NewLogger(t))
	defer sdk.Close()

	uo, _, err := newUploadOption()
	if err != nil {
		t.Fatal(err)
	}
	slabSize := int(uo.dataShards) * proto.SectorSize
	data := frand.Bytes(slabSize*2 + 4096)

	obj := NewEmptyObject()
	start := time.Now()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	end := time.Now()

	// every slab is pinned by the upload itself
	if len(obj.slabs) != 3 {
		t.Fatal("unexpected", len(obj.slabs))
	} else if appMock.PinnedSlabs() != 3 {
		t.Fatal("unexpected", appMock.PinnedSlabs())
	}

	// the indexer rejects sectors without a recent upload time, so every
	// pinned sector reports when its shard finished uploading
	params := appMock.PinSlabsParams()
	if len(params) != 3 {
		t.Fatal("unexpected", len(params))
	}
	for i, p := range params {
		if len(p.Sectors) == 0 {
			t.Fatalf("slab %d has no sectors", i)
		}
		for j, sector := range p.Sectors {
			if sector.UploadedAt == nil {
				t.Fatalf("slab %d sector %d: UploadedAt not set", i, j)
			} else if sector.UploadedAt.Before(start) || sector.UploadedAt.After(end) {
				t.Fatalf("slab %d sector %d: UploadedAt %v outside upload window [%v, %v]", i, j, *sector.UploadedAt, start, end)
			}
		}
	}

	// the slabs are already pinned, so pinning the object needs no further
	// PinSlabs requests
	calls := appMock.PinSlabsCalls()
	if err := sdk.PinObject(t.Context(), obj); err != nil {
		t.Fatal(err)
	} else if after := appMock.PinSlabsCalls(); len(after) != len(calls) {
		t.Fatal("unexpected", after)
	}
}

func TestUploadPinRetries(t *testing.T) {
	sdk, appMock, _ := newTestSDKWithMocks(t, 40, zaptest.NewLogger(t))
	defer sdk.Close()

	appMock.SetPinSlabsFailures(maxPinAttempts - 1)

	obj := NewEmptyObject()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(frand.Bytes(4096))); err != nil {
		t.Fatal(err)
	} else if calls := appMock.PinSlabsCalls(); len(calls) != maxPinAttempts {
		t.Fatal("unexpected", calls)
	} else if appMock.PinnedSlabs() != 1 {
		t.Fatal("unexpected", appMock.PinnedSlabs())
	}
}

func TestUploadPinFails(t *testing.T) {
	sdk, appMock, _ := newTestSDKWithMocks(t, 40, zaptest.NewLogger(t))
	defer sdk.Close()

	appMock.SetPinSlabsFailures(maxPinAttempts)

	obj := NewEmptyObject()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(frand.Bytes(4096))); err == nil {
		t.Fatal("expected upload to fail")
	} else if calls := appMock.PinSlabsCalls(); len(calls) != maxPinAttempts {
		t.Fatal("unexpected", calls)
	} else if appMock.PinnedSlabs() != 0 {
		t.Fatal("unexpected", appMock.PinnedSlabs())
	}
}

// overloadHostClient models a network that refuses every write in a batch when
// more than maxConcurrent writes contend for it.
type overloadHostClient struct {
	hostClient

	mu            sync.Mutex
	batch         *overloadBatch
	maxConcurrent int
	delay         time.Duration
}

type overloadBatch struct {
	done       chan struct{}
	writes     int
	overloaded bool
}

func (c *overloadHostClient) WriteSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte) (rhp.RPCWriteSectorResult, error) {
	c.mu.Lock()
	if c.batch == nil {
		c.batch = &overloadBatch{done: make(chan struct{})}
		batch := c.batch
		time.AfterFunc(c.delay, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.batch == batch {
				c.batch = nil
				close(batch.done)
			}
		})
	}
	batch := c.batch
	batch.writes++
	if batch.writes > c.maxConcurrent {
		batch.overloaded = true
		c.batch = nil
		close(batch.done)
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return rhp.RPCWriteSectorResult{}, ctx.Err()
	case <-batch.done:
	}

	c.mu.Lock()
	overloaded := batch.overloaded
	c.mu.Unlock()

	if overloaded {
		return rhp.RPCWriteSectorResult{}, errors.New("network overloaded")
	}
	return c.hostClient.WriteSector(ctx, accountKey, hostKey, data)
}

// TestUploadNoMoreHostsRegression asserts concurrent uploads adapt to one network
// together. Independent per-upload limiters never get below the network's
// combined ceiling, so every retry fails and the candidate pools run dry.
func TestUploadNoMoreHostsRegression(t *testing.T) {
	const dataShards, parityShards = 3, 9
	const totalShards = dataShards + parityShards
	const uploads = 3

	// there are three times as many hosts as a slab needs, so a pool only runs
	// dry if the upload pipeline incorrectly overloads the network itself.
	sdk, hosts := newTestSDK(t, totalShards*3, zaptest.NewLogger(t))
	defer sdk.Close()
	sdk.hosts = &overloadHostClient{
		hostClient:    hosts,
		maxConcurrent: minUploadInflight,
		delay:         200 * time.Millisecond,
	}

	data := frand.Bytes(dataShards * proto.SectorSize)
	errCh := make(chan error, uploads)
	for range uploads {
		go func() {
			obj := NewEmptyObject()
			errCh <- sdk.Upload(t.Context(), &obj, bytes.NewReader(data), WithRedundancy(dataShards, parityShards))
		}()
	}
	for range uploads {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

// TestUploadSaturatedNetwork asserts concurrent uploads sharing one saturated
// network still land their shards: on one limiter they back off together, where
// each probing upward alone would time every write out and empty the pool.
func TestUploadSaturatedNetwork(t *testing.T) {
	const dataShards, parityShards = 3, 9
	const totalShards = dataShards + parityShards
	const uploads = 6

	// hosts to spare, so only timeouts can exhaust the pool
	sdk, hosts := newTestSDK(t, totalShards*3, zaptest.NewLogger(t))
	defer sdk.Close()
	hosts.SetSharedBandwidth(30 * time.Millisecond)

	// the host timeout sits between what the network carries at one shared
	// limit and at uploads-many independent ones
	data := frand.Bytes(dataShards * proto.SectorSize)
	errCh := make(chan error, uploads)
	for range uploads {
		go func() {
			obj := NewEmptyObject()
			errCh <- sdk.Upload(t.Context(), &obj, bytes.NewReader(data), WithRedundancy(dataShards, parityShards), WithUploadHostTimeout(time.Second))
		}()
	}
	for range uploads {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	// racing and retries duplicate writes, and duplicates are what saturate a
	// slow network, so hold them to a small multiple of the shards to land
	if writes, maxWrites := hosts.TotalWrites(), uploads*totalShards*5/2; writes > maxWrites {
		t.Fatalf("expected at most %d writes to land %d shards, got %d", maxWrites, uploads*totalShards, writes)
	}
}
