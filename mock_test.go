package siastorage

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"maps"
	"slices"
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
	"go.sia.tech/indexd/slabs"
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

type mockHostClient struct {
	provider *client.Provider
	hosts    *hostCache

	delayMu   sync.Mutex
	slowHosts map[types.PublicKey]time.Duration

	sectorDelayMu sync.Mutex
	sectorDelays  map[types.Hash256]time.Duration

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

// AddFailedRPC implements the [hostClient] interface.
func (m *mockHostClient) AddFailedRPC(hostKey types.PublicKey) {
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
	return context.Cause(ctx)
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
	return context.Cause(ctx)
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
			m.provider.AddFailedRPC(hostKey)
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
func (m *mockHostClient) ReadSector(ctx context.Context, _ types.PrivateKey, hostKey types.PublicKey, sectorRoot types.Hash256, w io.Writer, offset, length uint64) (_ rhp.RPCReadSectorResult, err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			m.provider.AddFailedRPC(hostKey)
		} else {
			m.provider.AddReadSample(hostKey, length, time.Since(start))
		}
	}()

	// simulate timeout
	if err := m.delay(ctx, hostKey); err != nil {
		return rhp.RPCReadSectorResult{}, err
	} else if err := m.sectorDelay(ctx, sectorRoot); err != nil {
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
			m.provider.AddFailedRPC(hostKey)
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

type mockAppClient struct {
	hosts *hostCache

	mu            sync.Mutex
	pinned        map[slabs.SlabID]slabs.PinnedSlab
	objects       map[types.Hash256]slabs.SealedObject
	deleted       map[types.Hash256]time.Time
	hostsOverride []hosts.HostInfo
}

func newMockAppClient(hosts *hostCache) *mockAppClient {
	return &mockAppClient{
		hosts:   hosts,
		objects: make(map[types.Hash256]slabs.SealedObject),
		pinned:  make(map[slabs.SlabID]slabs.PinnedSlab),
		deleted: make(map[types.Hash256]time.Time),
	}
}

// Account implements the [appClient] interface.
func (mc *mockAppClient) Account(_ context.Context, _ types.PrivateKey) (resp app.AccountResponse, err error) {
	return app.AccountResponse{}, nil
}

// PinSlabs implements the [appClient] interface.
func (mc *mockAppClient) PinSlabs(_ context.Context, _ types.PrivateKey, toPin ...slabs.SlabPinParams) (digests []slabs.SlabID, err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	for _, s := range toPin {
		id := s.Digest()
		digests = append(digests, id)

		ps := slabs.PinnedSlab{
			ID:            id,
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
	mc.deleted[key] = time.Now()
	return nil
}

func (mc *mockAppClient) PruneSlabs(_ context.Context, _ types.PrivateKey, _ ...api.URLQueryParameterOption) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	used := make(map[slabs.SlabID]slabs.PinnedSlab)
	for _, obj := range mc.objects {
		for _, slab := range obj.Slabs {
			digest := slab.Digest()
			used[digest] = slabs.PinnedSlab{
				ID:            digest,
				EncryptionKey: slab.EncryptionKey,
				MinShards:     slab.MinShards,
				Sectors:       slab.Sectors,
			}
		}
	}
	mc.pinned = used
	return nil
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

	appKey := types.GeneratePrivateKey()
	hostStore := newMockHostStore(hosts)
	appClient := newMockAppClient(hostStore)
	hostClient := newMockHostClient(hostStore)

	b := newMockBuilder(appClient, hostClient, hostStore)
	sdk, err := b.SDK(appKey, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}

	return sdk, hostClient
}
