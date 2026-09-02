package siastorage

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/mux/v3"
	"go.uber.org/zap"
)

func newMockHostStore(n int) *hostCache {
	var update []hosts.HostInfo
	for range n {
		hk := types.GeneratePrivateKey().PublicKey()
		update = append(update, hosts.HostInfo{
			PublicKey:     hk,
			GoodForUpload: true,
		})
	}
	store := newHostCache()
	store.updateHosts(update)
	return store
}

type hostErr struct {
	remaining int
	err       error
}

// timedOutRPC records one [hostClient.AddTimedOutRPC] call.
type timedOutRPC struct {
	hostKey types.PublicKey
	write   bool
	bytes   uint64
	elapsed time.Duration
}

// assertSample checks the throughput sample the timeout was recorded with. The
// host key is left to the caller, which knows whether it expects one host or
// any of a set.
func (rpc timedOutRPC) assertSample(t testing.TB, write bool, bytes uint64) {
	t.Helper()
	if rpc.write != write {
		t.Fatalf("expected write=%v for %v, got %v", write, rpc.hostKey, rpc.write)
	} else if rpc.bytes != bytes {
		t.Fatalf("expected %d bytes for %v, got %d", bytes, rpc.hostKey, rpc.bytes)
	} else if rpc.elapsed <= 0 {
		t.Fatalf("expected a positive elapsed for %v, got %v", rpc.hostKey, rpc.elapsed)
	}
}

type mockHostClient struct {
	provider *client.Provider
	hosts    *hostCache
	inflight atomic.Int64

	delayMu   sync.Mutex
	slowHosts map[types.PublicKey]time.Duration

	sectorDelayMu sync.Mutex
	sectorDelays  map[types.Hash256]time.Duration

	timeoutMu    sync.Mutex
	timedOutRPCs []timedOutRPC

	failedMu   sync.Mutex
	failedRPCs []types.PublicKey

	readJitter func() time.Duration // set before issuing reads; nil for none

	errHostsMu sync.Mutex
	errHosts   map[types.PublicKey]hostErr

	sectorsMu   sync.Mutex
	hostSectors map[types.PublicKey]map[types.Hash256][]byte

	writesMu   sync.Mutex
	writeCalls map[types.PublicKey]int

	pricesMu    sync.Mutex
	pricesCalls map[types.PublicKey]int
}

// Close implements the [hostClient] interface.
func (m *mockHostClient) Close() error {
	return nil
}

// AddTimedOutRPC implements the [hostClient] interface, recording the call for
// inspection by tests.
func (m *mockHostClient) AddTimedOutRPC(hostKey types.PublicKey, write bool, bytes uint64, elapsed time.Duration) {
	m.timeoutMu.Lock()
	m.timedOutRPCs = append(m.timedOutRPCs, timedOutRPC{hostKey: hostKey, write: write, bytes: bytes, elapsed: elapsed})
	m.timeoutMu.Unlock()
	m.provider.AddTimedOutRPC(hostKey, write, bytes, elapsed)
}

// waitTimedOutRPCs waits for n timed out RPCs to be recorded, then returns
// them. Attempts report as they unwind, so a transfer may finish before every
// host it gave up on has reported.
func (m *mockHostClient) waitTimedOutRPCs(t testing.TB, n int) []timedOutRPC {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.timeoutMu.Lock()
		rpcs := slices.Clone(m.timedOutRPCs)
		m.timeoutMu.Unlock()
		if len(rpcs) >= n {
			return rpcs
		} else if time.Now().After(deadline) {
			t.Fatalf("expected %d timed out RPCs, got %d", n, len(rpcs))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TimedOutRPCs returns the timed out RPCs recorded so far, for tests asserting
// that a failed RPC carried no throughput sample.
func (m *mockHostClient) TimedOutRPCs() []timedOutRPC {
	m.timeoutMu.Lock()
	defer m.timeoutMu.Unlock()
	return slices.Clone(m.timedOutRPCs)
}

// AddFailedRPC implements the [hostClient] interface, recording the call for
// inspection by tests. Only the failure samples the SDK decides on land here;
// the ones the client itself records go through
// [mockHostClient.recordFailedRPC].
func (m *mockHostClient) AddFailedRPC(hostKey types.PublicKey) {
	m.failedMu.Lock()
	m.failedRPCs = append(m.failedRPCs, hostKey)
	m.failedMu.Unlock()
	m.provider.AddFailedRPC(hostKey)
}

// waitFailedRPCs waits for n failed RPCs to be recorded, then returns the hosts
// they were recorded against. Attempts report as they unwind, so a transfer may
// finish before every host it gave up on has reported.
func (m *mockHostClient) waitFailedRPCs(t testing.TB, n int) []types.PublicKey {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.failedMu.Lock()
		hks := slices.Clone(m.failedRPCs)
		m.failedMu.Unlock()
		if len(hks) >= n {
			return hks
		} else if time.Now().After(deadline) {
			t.Fatalf("expected %d failed RPCs, got %d", n, len(hks))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// recordFailedRPC mirrors the context half of the real client's isFailedRPC: an
// RPC interrupted by its context is not counted against the host, since the
// client cannot tell a host that misbehaved from a caller that cancelled. Only
// the caller owning the deadline can, so it reports those itself with
// [hostClient.AddTimedOutRPC] or [hostClient.AddFailedRPC].
func (m *mockHostClient) recordFailedRPC(ctx context.Context, hostKey types.PublicKey) {
	if ctx.Err() != nil {
		return
	}
	m.provider.AddFailedRPC(hostKey)
}

// UploadQueue implements the [hostClient] interface.
func (m *mockHostClient) UploadQueue() (*client.HostQueue, error) {
	return m.provider.UploadQueue()
}

// Prioritize implements the [hostClient] interface.
func (m *mockHostClient) Prioritize(hosts []types.PublicKey) []types.PublicKey {
	return m.provider.Prioritize(hosts)
}

// ReadEstimate implements the [hostClient] interface.
func (m *mockHostClient) ReadEstimate(bytes uint64) time.Duration {
	return m.provider.ReadEstimate(bytes)
}

// WriteEstimate implements the [hostClient] interface.
func (m *mockHostClient) WriteEstimate(bytes uint64) time.Duration {
	return m.provider.WriteEstimate(bytes)
}

// PickWrite implements the [hostClient] interface.
func (m *mockHostClient) PickWrite(candidates []types.PublicKey) (types.PublicKey, func(), []types.PublicKey, bool) {
	host, release, remaining, ok := m.provider.PickWrite(candidates)
	if !ok {
		return host, release, remaining, ok
	}
	m.inflight.Add(1)
	return host, func() { m.inflight.Add(-1); release() }, remaining, ok
}

// TrackInflightRead implements the [hostClient] interface.
func (m *mockHostClient) TrackInflightRead(hostKey types.PublicKey) func() {
	release := m.provider.TrackInflightRead(hostKey)
	m.inflight.Add(1)
	return func() { m.inflight.Add(-1); release() }
}

// OutstandingInflight returns the number of inflight reservations whose
// release has not yet been called.
func (m *mockHostClient) OutstandingInflight() int64 {
	return m.inflight.Load()
}

// waitInflightDrained waits for all inflight reservations to be released.
// Releases happen asynchronously as racing goroutines exit, so a completed
// upload or download may still hold reservations for a brief window.
func (m *mockHostClient) waitInflightDrained(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for m.OutstandingInflight() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("leaked inflight reservations", m.OutstandingInflight())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// SetSlowHostKeys marks the given hosts slow, each delaying its RPCs by d.
func (m *mockHostClient) SetSlowHostKeys(keys []types.PublicKey, d time.Duration) {
	m.delayMu.Lock()
	defer m.delayMu.Unlock()
	for _, hk := range keys {
		m.slowHosts[hk] = d
	}
}

// timeoutErr mirrors the real transport, where an expired deadline surfaces
// as a closed mux stream rather than a deadline error.
func timeoutErr(ctx context.Context) error {
	err := context.Cause(ctx)
	if err == context.DeadlineExceeded {
		return mux.ErrClosedStream
	}
	return err
}

func (m *mockHostClient) delay(ctx context.Context, hostKey types.PublicKey) error {
	m.delayMu.Lock()
	delay, ok := m.slowHosts[hostKey]
	m.delayMu.Unlock()
	if !ok || delay <= 0 {
		return nil
	}

	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
	return timeoutErr(ctx)
}

func (m *mockHostClient) sectorDelay(ctx context.Context, root types.Hash256) error {
	m.sectorDelayMu.Lock()
	delay, ok := m.sectorDelays[root]
	if ok {
		m.sectorDelays[root] = delay / 2
	}
	m.sectorDelayMu.Unlock()
	if !ok || delay <= 0 {
		return nil
	}

	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
	return timeoutErr(ctx)
}

func (m *mockHostClient) hostError(hostKey types.PublicKey) error {
	m.errHostsMu.Lock()
	defer m.errHostsMu.Unlock()

	errs := m.errHosts[hostKey]
	if errs.remaining <= 0 {
		return nil
	}
	m.errHosts[hostKey] = hostErr{remaining: errs.remaining - 1, err: errs.err}
	return errs.err
}

// WriteSector implements the [hostClient] interface.
func (m *mockHostClient) WriteSector(ctx context.Context, _ types.PrivateKey, hostKey types.PublicKey, data []byte) (_ rhp.RPCWriteSectorResult, err error) {
	if ok, _ := m.hosts.Usable(hostKey); !ok {
		panic("host not found: " + hostKey.String()) // developer error
	}

	start := time.Now()
	defer func() {
		if err != nil {
			m.recordFailedRPC(ctx, hostKey)
		} else {
			m.provider.AddWriteSample(hostKey, uint64(len(data)), time.Since(start))
		}
	}()

	m.writesMu.Lock()
	m.writeCalls[hostKey]++
	m.writesMu.Unlock()

	// simulate RPC error
	if err := m.hostError(hostKey); err != nil {
		return rhp.RPCWriteSectorResult{}, err
	}

	// simulate i/o
	if err := m.delay(ctx, hostKey); err != nil {
		return rhp.RPCWriteSectorResult{}, err
	}

	m.sectorsMu.Lock()
	defer m.sectorsMu.Unlock()

	var sector [proto.SectorSize]byte
	copy(sector[:], data)

	root := proto.SectorRoot(&sector)
	if _, ok := m.hostSectors[hostKey]; !ok {
		m.hostSectors[hostKey] = make(map[types.Hash256][]byte)
	}
	m.hostSectors[hostKey][root] = sector[:]
	return rhp.RPCWriteSectorResult{Root: root}, nil
}

// ReadSector implements the [hostClient] interface.
func (m *mockHostClient) ReadSector(ctx context.Context, token proto.AccountToken, sectorRoot types.Hash256, w io.Writer, offset, length uint64) (_ rhp.RPCReadSectorResult, err error) {
	hostKey := token.HostKey
	start := time.Now()
	defer func() {
		if err != nil {
			m.recordFailedRPC(ctx, hostKey)
		} else {
			m.provider.AddReadSample(hostKey, length, time.Since(start))
		}
	}()

	// simulate timeout
	if err := m.delay(ctx, hostKey); err != nil {
		return rhp.RPCReadSectorResult{}, err
	} else if err := m.sectorDelay(ctx, sectorRoot); err != nil {
		return rhp.RPCReadSectorResult{}, err
	} else if err := m.jitterDelay(ctx); err != nil {
		return rhp.RPCReadSectorResult{}, err
	}

	m.sectorsMu.Lock()
	defer m.sectorsMu.Unlock()

	sectors, ok := m.hostSectors[hostKey]
	if !ok {
		return rhp.RPCReadSectorResult{}, errors.New("host not found")
	}
	sector, ok := sectors[sectorRoot]
	if !ok {
		return rhp.RPCReadSectorResult{}, errors.New("sector not found")
	}
	if _, err := w.Write(sector[offset : offset+length]); err != nil {
		return rhp.RPCReadSectorResult{}, err
	}
	return rhp.RPCReadSectorResult{}, nil
}

// Prices implements the [hostClient] interface.
func (m *mockHostClient) Prices(ctx context.Context, hostKey types.PublicKey) (_ proto.HostPrices, err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			m.recordFailedRPC(ctx, hostKey)
		} else {
			m.provider.AddSettingsSample(hostKey, time.Since(start))
		}
	}()

	m.pricesMu.Lock()
	m.pricesCalls[hostKey]++
	m.pricesMu.Unlock()

	// simulate delay
	err = m.delay(ctx, hostKey)
	return
}

// PricesCalls returns the number of Prices calls per host.
func (m *mockHostClient) PricesCalls() map[types.PublicKey]int {
	m.pricesMu.Lock()
	defer m.pricesMu.Unlock()
	calls := make(map[types.PublicKey]int, len(m.pricesCalls))
	maps.Copy(calls, m.pricesCalls)
	return calls
}

// WriteCalls returns the number of WriteSector calls per host.
func (m *mockHostClient) WriteCalls() map[types.PublicKey]int {
	m.writesMu.Lock()
	defer m.writesMu.Unlock()
	calls := make(map[types.PublicKey]int, len(m.writeCalls))
	maps.Copy(calls, m.writeCalls)
	return calls
}

// ResetPricesCalls clears the Prices call counters.
func (m *mockHostClient) ResetPricesCalls() {
	m.pricesMu.Lock()
	defer m.pricesMu.Unlock()
	m.pricesCalls = make(map[types.PublicKey]int)
}

func (m *mockHostClient) ResetSlowHosts() {
	m.delayMu.Lock()
	defer m.delayMu.Unlock()
	m.slowHosts = make(map[types.PublicKey]time.Duration)
	m.provider = client.NewProvider(m.hosts) // reset provider to clear host performance metrics
}

func (m *mockHostClient) SetSlowHosts(tb testing.TB, n int, d time.Duration) []types.PublicKey {
	tb.Helper()

	hosts, _ := m.hosts.UsableHosts()
	if n > len(hosts) {
		tb.Fatalf("cannot set %d slow hosts: only %d hosts available", n, len(hosts))
	}

	m.delayMu.Lock()
	defer m.delayMu.Unlock()

	slow := make([]types.PublicKey, 0, n)
	for _, hi := range hosts {
		if len(slow) >= n {
			break
		}
		m.slowHosts[hi.PublicKey] = d
		slow = append(slow, hi.PublicKey)
	}
	return slow
}

func (m *mockHostClient) jitterDelay(ctx context.Context) error {
	if m.readJitter == nil {
		return nil
	}
	d := m.readJitter()
	if d <= 0 {
		return nil
	}

	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
	return timeoutErr(ctx)
}

// SetReadJitter delays every sector read by a duration drawn from fn. It
// affects all hosts equally, so prioritization and racing cannot route around
// it. It must be set before issuing reads.
func (m *mockHostClient) SetReadJitter(fn func() time.Duration) {
	m.readJitter = fn
}

func (m *mockHostClient) SetSectorReadDelay(root types.Hash256, d time.Duration) {
	m.sectorDelayMu.Lock()
	defer m.sectorDelayMu.Unlock()
	m.sectorDelays[root] = d
}

// SetErrHosts marks the first n hosts as failing: each will return
// the given error for its first failCount write attempts.
func (m *mockHostClient) SetErrHosts(tb testing.TB, n, failCount int, err error) {
	tb.Helper()

	hosts, _ := m.hosts.UsableHosts()
	if n > len(hosts) {
		tb.Fatalf("cannot set %d flaky hosts: only %d hosts available", n, len(hosts))
	}

	m.errHostsMu.Lock()
	defer m.errHostsMu.Unlock()

	var set int
	for _, hi := range hosts {
		if set >= n {
			break
		}
		set++
		m.errHosts[hi.PublicKey] = hostErr{remaining: failCount, err: err}
	}
}

func newMockHostClient(hosts *hostCache) *mockHostClient {
	m := &mockHostClient{
		hosts:        hosts,
		provider:     client.NewProvider(hosts),
		slowHosts:    make(map[types.PublicKey]time.Duration),
		sectorDelays: make(map[types.Hash256]time.Duration),
		errHosts:     make(map[types.PublicKey]hostErr),
		hostSectors:  make(map[types.PublicKey]map[types.Hash256][]byte),
		writeCalls:   make(map[types.PublicKey]int),
		pricesCalls:  make(map[types.PublicKey]int),
	}
	return m
}

type (
	// A mockSharingKey is a sharing key together with the objects attached to
	// it. The indexer keeps the aggregate totals on the key row and maintains
	// them with a trigger; the mock recomputes them from the attachments so
	// they cannot drift.
	mockSharingKey struct {
		key sharing.Key
		// seq replaces the auto-incrementing row ID the indexer sorts its key
		// listing by, which map iteration cannot reproduce on its own.
		seq     uint64
		objects map[types.Hash256]*mockSharedObject
	}

	// A mockSharedObject is one object attached to a sharing key: the keys and
	// signatures re-sealed under that sharing key, plus the sizes the indexer
	// captures at attach time to feed the key's totals.
	mockSharedObject struct {
		req sharing.SharedObjectRequest
		// seq orders attachments the way the indexer's attach timestamp does.
		// There is no row ID to stand in for here: shared_objects is keyed by
		// (object, sharing key).
		seq        uint64
		size       uint64
		pinnedData uint64
		pinnedSize uint64
		createdAt  time.Time
		updatedAt  time.Time
	}
)

type mockAppClient struct {
	hosts *hostCache

	// clock, when set, replaces time.Now everywhere the mock records or compares
	// a time, so a test can step past a sharing key's expiry or a slab's prune
	// cutoff instead of sleeping and hoping. Set it before the mock is used, since
	// it is read without holding mu.
	clock func() time.Time

	mu             sync.Mutex
	pinned         map[slabs.SlabID]slabs.PinnedSlab
	pinnedAt       map[slabs.SlabID]time.Time
	objects        map[types.Hash256]slabs.SealedObject
	deleted        map[types.Hash256]time.Time
	sharingKeys    map[types.PublicKey]*mockSharingKey
	sharingSeq     uint64
	hostsOverride  []hosts.HostInfo
	pinSlabsCalls  []int
	pinSlabsParams []slabs.SlabPinParams
	pinSlabsFails  int
}

func newMockAppClient(hosts *hostCache) *mockAppClient {
	return &mockAppClient{
		hosts:       hosts,
		objects:     make(map[types.Hash256]slabs.SealedObject),
		pinned:      make(map[slabs.SlabID]slabs.PinnedSlab),
		pinnedAt:    make(map[slabs.SlabID]time.Time),
		deleted:     make(map[types.Hash256]time.Time),
		sharingKeys: make(map[types.PublicKey]*mockSharingKey),
	}
}

// Account implements the [appClient] interface.
func (mc *mockAppClient) Account(_ context.Context, _ types.PrivateKey) (resp app.AccountResponse, err error) {
	return app.AccountResponse{}, nil
}

// PinSlabs implements the [appClient] interface.
func (mc *mockAppClient) PinSlabs(ctx context.Context, _ types.PrivateKey, toPin ...slabs.SlabPinParams) (digests []slabs.SlabID, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.pinSlabsCalls = append(mc.pinSlabsCalls, len(toPin))
	mc.pinSlabsParams = append(mc.pinSlabsParams, toPin...)
	if mc.pinSlabsFails > 0 {
		mc.pinSlabsFails--
		return nil, &app.HTTPError{StatusCode: http.StatusInternalServerError, Body: "pin slabs unavailable"}
	}

	for _, s := range toPin {
		id := s.Digest()
		digests = append(digests, id)

		ps := slabs.PinnedSlab{
			ID:            id,
			Version:       s.Version,
			EncryptionKey: s.EncryptionKey,
			MinShards:     s.MinShards,
			Sectors:       make([]slabs.PinnedSector, len(s.Sectors)),
		}
		for i, sector := range s.Sectors {
			ps.Sectors[i] = slabs.PinnedSector{
				Root:    sector.Root,
				HostKey: sector.HostKey,
			}
		}
		mc.pinned[id] = ps
		// the indexer refreshes the pin timestamp when an existing slab is
		// pinned again
		mc.pinnedAt[id] = mc.now()
	}
	return
}

// Slab implements the [appClient] interface.
func (mc *mockAppClient) Slab(_ context.Context, _ types.PrivateKey, id slabs.SlabID) (slabs.PinnedSlab, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	slab, ok := mc.pinned[id]
	if !ok {
		return slabs.PinnedSlab{}, errors.New("slab not found")
	}
	return slab, nil
}

// UnpinSlab implements the [appClient] interface.
func (mc *mockAppClient) UnpinSlab(_ context.Context, _ types.PrivateKey, id slabs.SlabID) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.pinned, id)
	delete(mc.pinnedAt, id)
	return nil
}

// Hosts implements the [appClient] interface.
func (mc *mockAppClient) Hosts(context.Context, types.PrivateKey, ...api.URLQueryParameterOption) ([]hosts.HostInfo, error) {
	mc.mu.Lock()
	override := mc.hostsOverride
	mc.mu.Unlock()
	if override != nil {
		return override, nil
	}
	return mc.hosts.UsableHosts()
}

// SetHosts overrides the host list returned by Hosts.
func (mc *mockAppClient) SetHosts(hi []hosts.HostInfo) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.hostsOverride = hi
}

func (mc *mockAppClient) Object(_ context.Context, _ types.PrivateKey, objectKey types.Hash256) (slabs.SealedObject, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	obj, ok := mc.objects[objectKey]
	if !ok {
		return slabs.SealedObject{}, slabs.ErrObjectNotFound
	}
	return obj, nil
}

func (mc *mockAppClient) ListObjects(_ context.Context, _ types.PrivateKey, _ slabs.Cursor, _ int) ([]slabs.ObjectEvent, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	var objs []slabs.ObjectEvent
	for _, obj := range mc.objects {
		objs = append(objs, slabs.ObjectEvent{
			Key:       obj.ID(),
			Deleted:   false,
			UpdatedAt: obj.UpdatedAt,
			Object:    &obj,
		})
	}
	for key, deletedAt := range mc.deleted {
		objs = append(objs, slabs.ObjectEvent{
			Key:       key,
			Deleted:   true,
			UpdatedAt: deletedAt,
		})
	}
	slices.SortFunc(objs, func(a, b slabs.ObjectEvent) int {
		return a.UpdatedAt.Compare(b.UpdatedAt)
	})
	return objs, nil
}

// SharedObject implements the [appClient] interface.
func (mc *mockAppClient) SharedObject(_ context.Context, sharedURL string) (slabs.SharedObject, []byte, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	buf, err := hex.DecodeString(sharedURL)
	if err != nil {
		return slabs.SharedObject{}, nil, errors.New("invalid shared URL")
	} else if len(buf) != 64 {
		return slabs.SharedObject{}, nil, errors.New("invalid shared URL")
	}

	objKey := (types.Hash256)(buf[:32])
	encryptionKey := buf[32:]

	obj, ok := mc.objects[objKey]
	if !ok {
		return slabs.SharedObject{}, nil, errors.New("object not found")
	}

	var objSlabs []slabs.SlabSlice
	for _, slab := range obj.Slabs {
		pinnedSlab := mc.pinned[slab.Digest()]
		objSlabs = append(objSlabs, slabs.SlabSlice{
			Version:       pinnedSlab.Version,
			EncryptionKey: pinnedSlab.EncryptionKey,
			MinShards:     pinnedSlab.MinShards,
			Sectors:       pinnedSlab.Sectors,
			Offset:        slab.Offset,
			Length:        slab.Length,
		})
	}

	return slabs.SharedObject{Slabs: objSlabs}, encryptionKey, nil
}

// PinObject implements the [appClient] interface.
func (mc *mockAppClient) PinObject(_ context.Context, _ types.PrivateKey, obj slabs.SealedObject) (err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	for _, slab := range obj.Slabs {
		if _, ok := mc.pinned[slab.Digest()]; !ok {
			return &app.HTTPError{StatusCode: http.StatusBadRequest, Body: slabs.ErrObjectUnpinnedSlab.Error()}
		}
	}

	mc.objects[obj.ID()] = obj
	return nil
}

// CreateSharedObjectURL implements the [appClient] interface.
func (mc *mockAppClient) CreateSharedObjectURL(_ context.Context, _ types.PrivateKey, objectKey types.Hash256, encryptionKey []byte, _ time.Time) (string, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	_, ok := mc.objects[objectKey]
	if !ok {
		return "", errors.New("object not found")
	}

	key := make([]byte, 64)
	copy(key[:32], objectKey[:])
	copy(key[32:], encryptionKey)
	return hex.EncodeToString(key), nil
}

func (mc *mockAppClient) DeleteObject(_ context.Context, _ types.PrivateKey, key types.Hash256) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, ok := mc.objects[key]; !ok {
		return slabs.ErrObjectNotFound
	}
	delete(mc.objects, key)
	mc.deleted[key] = mc.now()

	// Attachments cascade on the object in the indexer, and the trigger
	// decrements the key's totals for those deletes too.
	now := mc.now()
	for _, sk := range mc.sharingKeys {
		if _, ok := sk.objects[key]; !ok {
			continue
		}
		delete(sk.objects, key)
		sk.key.UpdatedAt = now
	}
	return nil
}

// PruneSlabs implements the [appClient] interface. Like the indexer, it only
// unpins slabs that are unreferenced and were pinned before the cutoff. The
// cutoff defaults to [api.DefaultSlabPruneCutoff] ago and can be overridden
// with [api.WithBefore].
func (mc *mockAppClient) PruneSlabs(_ context.Context, _ types.PrivateKey, opts ...api.URLQueryParameterOption) error {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	cutoff := mc.now().Add(-api.DefaultSlabPruneCutoff)
	if before := values.Get("before"); before != "" {
		t, err := time.Parse(time.RFC3339Nano, before)
		if err != nil {
			return fmt.Errorf("failed to parse before: %w", err)
		}
		cutoff = t
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	used := make(map[slabs.SlabID]bool)
	for _, obj := range mc.objects {
		for _, slab := range obj.Slabs {
			used[slab.Digest()] = true
		}
	}
	for id := range mc.pinned {
		if used[id] || !mc.pinnedAt[id].Before(cutoff) {
			continue
		}
		delete(mc.pinned, id)
		delete(mc.pinnedAt, id)
	}
	return nil
}

// sharingError wraps err the way the indexer's HTTP layer does, so callers can
// classify it with errors.As on [app.HTTPError] exactly as they would a real
// response.
func sharingError(status int, err error) error {
	return &app.HTTPError{StatusCode: status, Body: err.Error()}
}

// invalidSharingRequest builds the 400 the indexer returns for a request that
// fails validation, wrapping the sentinel the way the indexer does so the body
// reads the same.
func invalidSharingRequest(detail string) error {
	return sharingError(http.StatusBadRequest, fmt.Errorf("%w: %s", sharing.ErrInvalidRequest, detail))
}

// now returns the mock's clock. It deliberately does not take mu: most callers
// already hold it, and Go mutexes are not reentrant.
func (mc *mockAppClient) now() time.Time {
	if mc.clock != nil {
		return mc.clock()
	}
	return time.Now()
}

// sharingKeyExpired reports whether the indexer would treat the key as gone.
// Every lookup filters on `expires_at IS NULL OR expires_at > NOW()`, so an
// expired key is indistinguishable from one that never existed.
func sharingKeyExpired(expiresAt *time.Time, now time.Time) bool {
	return expiresAt != nil && !expiresAt.After(now)
}

// validateKeyRequest reimplements the unexported sharing.KeyRequest.validate.
// The mock has to duplicate it because the indexer runs it before touching the
// store, and a request it would reject must not appear to succeed here.
func validateKeyRequest(req sharing.KeyRequest, now time.Time) error {
	switch {
	case req.PublicKey == (types.PublicKey{}):
		return invalidSharingRequest("public key is required")
	case req.Nonce == (sharing.Nonce{}):
		return invalidSharingRequest("nonce is required")
	case req.ExpiresAt != nil && req.ExpiresAt.Before(now):
		return invalidSharingRequest("expires at must be in the future")
	case len(req.Description) > sharing.MaxDescriptionSize:
		return invalidSharingRequest(fmt.Sprintf("description exceeds %d bytes", sharing.MaxDescriptionSize))
	}
	return nil
}

// validateSharedObjectRequest reimplements the unexported
// sharing.SharedObjectRequest.validate, which the mock cannot call. The indexer
// runs it before touching the store, so a request it would reject must not
// appear to succeed here.
func validateSharedObjectRequest(req sharing.SharedObjectRequest) error {
	switch {
	case req.ObjectID == (types.Hash256{}):
		return invalidSharingRequest("object ID is required")
	case len(req.EncryptedDataKey) != sharing.EncryptionKeySize:
		return invalidSharingRequest(fmt.Sprintf("encrypted data key must be %d bytes", sharing.EncryptionKeySize))
	case len(req.EncryptedMetadataKey) != 0 && len(req.EncryptedMetadataKey) != sharing.EncryptionKeySize:
		return invalidSharingRequest(fmt.Sprintf("encrypted metadata key must be %d bytes", sharing.EncryptionKeySize))
	case len(req.EncryptedMetadata) > sharing.MaxMetadataSize:
		return invalidSharingRequest(fmt.Sprintf("encrypted metadata exceeds %d bytes", sharing.MaxMetadataSize))
	}
	return nil
}

// A sharingPage is the offset and limit a paginated route has accepted.
type sharingPage struct {
	offset, limit int
}

// parseSharingPage reads offset and limit the way the indexer's paginated routes
// do, with the same defaults and bounds.
func parseSharingPage(opts ...api.URLQueryParameterOption) (sharingPage, error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}

	page := sharingPage{limit: 100} // api's unexported defaultLimit
	if v := values.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return sharingPage{}, sharingError(http.StatusBadRequest, api.ErrInvalidOffset)
		}
		page.offset = n
	}
	if v := values.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > api.MaxLimit {
			return sharingPage{}, sharingError(http.StatusBadRequest, api.ErrInvalidLimit)
		}
		page.limit = n
	}
	return page, nil
}

// paginateSharing cuts items down to the page, the way the indexer's LIMIT and
// OFFSET do.
func paginateSharing[T any](items []T, page sharingPage) []T {
	if page.offset >= len(items) {
		return nil
	}
	return items[page.offset:min(page.offset+page.limit, len(items))]
}

// ownedSharingKey mirrors the indexer's owner-side lookup, where a key that does
// not exist, has expired, or belongs to another account all produce the same
// 404. Callers must hold mc.mu.
func (mc *mockAppClient) ownedSharingKey(appKey types.PrivateKey, publicKey types.PublicKey) (*mockSharingKey, error) {
	sk, ok := mc.sharingKeys[publicKey]
	if !ok || sk.key.Account != appKey.PublicKey() || sharingKeyExpired(sk.key.ExpiresAt, mc.now()) {
		return nil, sharingError(http.StatusNotFound, sharing.ErrSharingKeyNotFound)
	}
	return sk, nil
}

// withSharingStats returns the key with the aggregate totals the indexer keeps
// on it, summed from the attachments. It reads the attachment map, so callers
// must hold the mutex of the [mockAppClient] that owns the key.
func (sk *mockSharingKey) withSharingStats() sharing.Key {
	key := sk.key
	for _, att := range sk.objects {
		key.ObjectCount++
		key.ObjectSize += att.size
		key.PinnedData += att.pinnedData
		key.PinnedSize += att.pinnedSize
	}
	return key
}

// objectSharingSizes computes the sizes the indexer captures when an object is
// attached: the object's logical size, and its storage footprint before and
// after redundancy. A slab referenced by more than one slice is stored once, so
// it only counts once towards the pinned figures. Callers must hold mc.mu.
func (mc *mockAppClient) objectSharingSizes(obj slabs.SealedObject) (size, pinnedData, pinnedSize uint64) {
	counted := make(map[slabs.SlabID]bool)
	for _, slab := range obj.Slabs {
		size += uint64(slab.Length)

		id := slab.Digest()
		if counted[id] {
			continue
		}
		counted[id] = true

		pinned := mc.pinned[id]
		pinnedData += uint64(pinned.MinShards) * proto.SectorSize
		pinnedSize += uint64(len(pinned.Sectors)) * proto.SectorSize
	}
	return
}

// assertSharedObjectUnique enforces the UNIQUE constraints on the indexer's
// shared_objects columns, which span every sharing key rather than one.
// Re-attaching the same object to the same key is an update, so that row is
// exempt. Callers must hold mc.mu.
func (mc *mockAppClient) assertSharedObjectUnique(sharingKey types.PublicKey, req sharing.SharedObjectRequest) error {
	conflicts := func(a, b []byte) bool {
		return len(a) > 0 && len(b) > 0 && string(a) == string(b)
	}
	for pk, sk := range mc.sharingKeys {
		for objectID, att := range sk.objects {
			if pk == sharingKey && objectID == req.ObjectID {
				continue // this is the row being updated
			}
			if conflicts(att.req.EncryptedDataKey, req.EncryptedDataKey) ||
				conflicts(att.req.EncryptedMetadataKey, req.EncryptedMetadataKey) ||
				att.req.DataSignature == req.DataSignature ||
				att.req.MetadataSignature == req.MetadataSignature {
				return sharingError(http.StatusConflict, sharing.ErrSharedObjectConflict)
			}
		}
	}
	return nil
}

// AddSharingKey implements the [appClient] interface.
func (mc *mockAppClient) AddSharingKey(_ context.Context, appKey types.PrivateKey, req sharing.KeyRequest) (sharing.Key, error) {
	// the indexer validates first, then verifies the signature, then stores
	if err := validateKeyRequest(req, mc.now()); err != nil {
		return sharing.Key{}, err
	} else if err := req.VerifySignature(); err != nil {
		return sharing.Key{}, sharingError(http.StatusBadRequest, err)
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	// public_key and nonce are both globally unique columns, and either
	// violation surfaces as the same conflict
	if _, ok := mc.sharingKeys[req.PublicKey]; ok {
		return sharing.Key{}, sharingError(http.StatusConflict, sharing.ErrSharingKeyExists)
	}
	for _, sk := range mc.sharingKeys {
		if sk.key.Nonce == req.Nonce {
			return sharing.Key{}, sharingError(http.StatusConflict, sharing.ErrSharingKeyExists)
		}
	}

	now := mc.now()
	mc.sharingSeq++
	sk := &mockSharingKey{
		key: sharing.Key{
			Account:     appKey.PublicKey(),
			PublicKey:   req.PublicKey,
			Nonce:       req.Nonce,
			Description: req.Description,
			ExpiresAt:   req.ExpiresAt,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		seq:     mc.sharingSeq,
		objects: make(map[types.Hash256]*mockSharedObject),
	}
	mc.sharingKeys[req.PublicKey] = sk
	return sk.key, nil
}

// SharingKey implements the [appClient] interface.
func (mc *mockAppClient) SharingKey(_ context.Context, appKey types.PrivateKey, publicKey types.PublicKey) (sharing.Key, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	sk, err := mc.ownedSharingKey(appKey, publicKey)
	if err != nil {
		return sharing.Key{}, err
	}
	return sk.withSharingStats(), nil
}

// SharingKeys implements the [appClient] interface.
func (mc *mockAppClient) SharingKeys(_ context.Context, appKey types.PrivateKey, opts ...api.URLQueryParameterOption) ([]sharing.Key, error) {
	page, err := parseSharingPage(opts...)
	if err != nil {
		return nil, err
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	account, now := appKey.PublicKey(), mc.now()
	var owned []*mockSharingKey
	for _, sk := range mc.sharingKeys {
		if sk.key.Account != account || sharingKeyExpired(sk.key.ExpiresAt, now) {
			continue
		}
		owned = append(owned, sk)
	}
	// the indexer orders by row ID descending, newest first
	slices.SortFunc(owned, func(a, b *mockSharingKey) int {
		return cmp.Compare(b.seq, a.seq)
	})

	owned = paginateSharing(owned, page)
	keys := make([]sharing.Key, 0, len(owned))
	for _, sk := range owned {
		keys = append(keys, sk.withSharingStats())
	}
	return keys, nil
}

// DeleteSharingKey implements the [appClient] interface.
func (mc *mockAppClient) DeleteSharingKey(_ context.Context, appKey types.PrivateKey, publicKey types.PublicKey) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// unlike the read paths, the delete is not filtered on expiry: an expired
	// key is invisible but still removable
	sk, ok := mc.sharingKeys[publicKey]
	if !ok || sk.key.Account != appKey.PublicKey() {
		return sharingError(http.StatusNotFound, sharing.ErrSharingKeyNotFound)
	}
	// shared_objects cascades on the sharing key
	delete(mc.sharingKeys, publicKey)
	return nil
}

// AddSharedObject implements the [appClient] interface.
func (mc *mockAppClient) AddSharedObject(_ context.Context, appKey types.PrivateKey, sharingKey types.PublicKey, req sharing.SharedObjectRequest) error {
	// as with AddSharingKey, validation and signature verification happen
	// before the store is consulted, so a bad request on an unknown key is a
	// 400 rather than a 404
	if err := validateSharedObjectRequest(req); err != nil {
		return err
	} else if err := req.VerifySignatures(sharingKey); err != nil {
		return sharingError(http.StatusBadRequest, err)
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	sk, err := mc.ownedSharingKey(appKey, sharingKey)
	if err != nil {
		return err
	}

	// the object must already be pinned; the indexer looks it up in the account's
	// objects, which PinObject only admits once every slab is pinned. The mock's
	// object store is not scoped by account, so unlike the indexer it cannot
	// reject an object owned by someone else.
	obj, ok := mc.objects[req.ObjectID]
	if !ok {
		return sharingError(http.StatusNotFound, slabs.ErrObjectNotFound)
	}
	if err := mc.assertSharedObjectUnique(sharingKey, req); err != nil {
		return err
	}

	size, pinnedData, pinnedSize := mc.objectSharingSizes(obj)
	now := mc.now()
	// the trigger that maintains the key's totals also touches the key itself,
	// and it is the only thing that does
	sk.key.UpdatedAt = now
	if existing, ok := sk.objects[req.ObjectID]; ok {
		// re-attaching overwrites the re-sealed keys and signatures, keeps the
		// original attach time, and leaves the key's object count alone
		existing.req = req
		existing.size, existing.pinnedData, existing.pinnedSize = size, pinnedData, pinnedSize
		existing.updatedAt = now
		return nil
	}
	mc.sharingSeq++
	sk.objects[req.ObjectID] = &mockSharedObject{
		req:        req,
		seq:        mc.sharingSeq,
		size:       size,
		pinnedData: pinnedData,
		pinnedSize: pinnedSize,
		createdAt:  now,
		updatedAt:  now,
	}
	return nil
}

// DeleteSharedObject implements the [appClient] interface.
func (mc *mockAppClient) DeleteSharedObject(_ context.Context, appKey types.PrivateKey, sharingKey types.PublicKey, objectKey types.Hash256) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// the indexer deletes by a join across the key, the account and the object,
	// so every miss is the same not-found
	sk, ok := mc.sharingKeys[sharingKey]
	if !ok || sk.key.Account != appKey.PublicKey() {
		return sharingError(http.StatusNotFound, sharing.ErrSharedObjectNotFound)
	} else if _, ok := sk.objects[objectKey]; !ok {
		return sharingError(http.StatusNotFound, sharing.ErrSharedObjectNotFound)
	}
	delete(sk.objects, objectKey)
	sk.key.UpdatedAt = mc.now()
	return nil
}

// SharingKeyObjects implements the [appClient] interface.
func (mc *mockAppClient) SharingKeyObjects(_ context.Context, appKey types.PrivateKey, sharingKey types.PublicKey, opts ...api.URLQueryParameterOption) ([]slabs.SealedObject, error) {
	page, err := parseSharingPage(opts...)
	if err != nil {
		return nil, err
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	sk, err := mc.ownedSharingKey(appKey, sharingKey)
	if err != nil {
		return nil, err
	}

	attached := make([]*mockSharedObject, 0, len(sk.objects))
	for _, att := range sk.objects {
		attached = append(attached, att)
	}
	// The indexer orders by attach time, most recently attached first, and
	// re-attaching keeps the original time rather than moving the object to the
	// front. It sorts on a timestamp rather than a row ID, so attachments sharing
	// one transaction tie and their order is unspecified; the mock's is stable.
	slices.SortFunc(attached, func(a, b *mockSharedObject) int {
		return cmp.Compare(b.seq, a.seq)
	})

	attached = paginateSharing(attached, page)
	objects := make([]slabs.SealedObject, 0, len(attached))
	for _, att := range attached {
		obj, ok := mc.objects[att.req.ObjectID]
		if !ok {
			// shared_objects cascades when the owner deletes the object, so the
			// attachment simply disappears
			continue
		}
		// the slabs come from the owner's object; only the encryption keys and
		// signatures come from the attachment, re-sealed under the sharing key
		objects = append(objects, slabs.SealedObject{
			EncryptedDataKey:     att.req.EncryptedDataKey,
			Slabs:                obj.Slabs,
			DataSignature:        att.req.DataSignature,
			EncryptedMetadataKey: att.req.EncryptedMetadataKey,
			EncryptedMetadata:    att.req.EncryptedMetadata,
			MetadataSignature:    att.req.MetadataSignature,
			CreatedAt:            att.createdAt,
			UpdatedAt:            att.updatedAt,
		})
	}
	return objects, nil
}

// SetPinSlabsFailures fails the next n PinSlabs calls before they resume
// succeeding.
func (mc *mockAppClient) SetPinSlabsFailures(n int) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.pinSlabsFails = n
}

// PinSlabsCalls returns the slab count of each PinSlabs call, including the
// calls made to fail.
func (mc *mockAppClient) PinSlabsCalls() []int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return slices.Clone(mc.pinSlabsCalls)
}

// PinSlabsParams returns the params of every slab passed to PinSlabs,
// including the calls made to fail.
func (mc *mockAppClient) PinSlabsParams() []slabs.SlabPinParams {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return slices.Clone(mc.pinSlabsParams)
}

func (mc *mockAppClient) PinnedSlabs() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return len(mc.pinned)
}

// newMockBuilder creates a Builder backed by mock implementations.
func newMockBuilder(app appClient, hosts hostClient, hostStore *hostCache) *Builder {
	return &Builder{
		mockApp:       app,
		mockHost:      hosts,
		mockHostCache: hostStore,
		consumed:      &atomic.Bool{},
	}
}

// newTestSDK creates an SDK with mock clients for testing.
func newTestSDK(t testing.TB, hosts int, log *zap.Logger) (*SDK, *mockHostClient) {
	t.Helper()

	sdk, _, hostClient := newTestSDKWithMocks(t, hosts, log)
	return sdk, hostClient
}

func newTestSDKWithMocks(t testing.TB, hosts int, log *zap.Logger) (*SDK, *mockAppClient, *mockHostClient) {
	t.Helper()

	appKey := types.GeneratePrivateKey()
	hostStore := newMockHostStore(hosts)
	appClient := newMockAppClient(hostStore)
	hostClient := newMockHostClient(hostStore)

	b := newMockBuilder(appClient, hostClient, hostStore)
	sdk, err := b.SDK(appKey, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}

	return sdk, appClient, hostClient
}
