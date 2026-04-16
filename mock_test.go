package siastorage

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"maps"
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

type mockHostClient struct {
	provider *client.Provider
	hosts    *hostCache

	delayMu   sync.Mutex
	slowHosts map[types.PublicKey]time.Duration

	sectorDelayMu sync.Mutex
	sectorDelays  map[types.Hash256]time.Duration

	flakyMu    sync.Mutex
	flakyHosts map[types.PublicKey]int // fail first N writes

	sectorsMu   sync.Mutex
	hostSectors map[types.PublicKey]map[types.Hash256][]byte

	pricesMu    sync.Mutex
	pricesCalls map[types.PublicKey]int
}

// Close implements the [hostClient] interface.
func (m *mockHostClient) Close() error {
	return nil
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

// WriteSector implements the [hostClient] interface.
func (m *mockHostClient) WriteSector(ctx context.Context, _ types.PrivateKey, hostKey types.PublicKey, data []byte) (_ rhp.RPCWriteSectorResult, err error) {
	if ok, _ := m.hosts.Usable(hostKey); !ok {
		panic("host not found: " + hostKey.String()) // developer error
	}

	start := time.Now()
	defer func() {
		if err != nil {
			m.provider.AddFailedRPC(hostKey, err)
		} else {
			m.provider.AddWriteSample(hostKey, uint64(len(data)), time.Since(start))
		}
	}()

	// simulate flaky hosts that fail their first N writes
	m.flakyMu.Lock()
	if m.flakyHosts[hostKey] > 0 {
		m.flakyHosts[hostKey]--
		m.flakyMu.Unlock()
		return rhp.RPCWriteSectorResult{}, context.DeadlineExceeded
	}
	m.flakyMu.Unlock()

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
			m.provider.AddFailedRPC(hostKey, err)
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
func (m *mockHostClient) Prices(_ context.Context, hostKey types.PublicKey) (prices proto.HostPrices, err error) {
	m.pricesMu.Lock()
	m.pricesCalls[hostKey]++
	m.pricesMu.Unlock()

	m.delayMu.Lock()
	delay, ok := m.slowHosts[hostKey]
	m.delayMu.Unlock()

	// NOTE: we don't simulate a delay here but record a sample so the provider can deprioritize slow hosts
	if ok && delay > 0 {
		m.provider.AddSettingsSample(hostKey, delay)
	}

	return proto.HostPrices{}, nil
}

// PricesCalls returns the number of Prices calls per host.
func (m *mockHostClient) PricesCalls() map[types.PublicKey]int {
	m.pricesMu.Lock()
	defer m.pricesMu.Unlock()
	calls := make(map[types.PublicKey]int, len(m.pricesCalls))
	maps.Copy(calls, m.pricesCalls)
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

func (m *mockHostClient) SetSlowHosts(tb testing.TB, n int, d time.Duration) {
	tb.Helper()

	hosts, _ := m.hosts.UsableHosts()
	if n > len(hosts) {
		tb.Fatalf("cannot set %d flaky hosts: only %d hosts available", n, len(hosts))
	}

	m.delayMu.Lock()
	defer m.delayMu.Unlock()

	var set int
	for _, hi := range hosts {
		if set >= n {
			return // already set enough hosts
		}
		set++
		m.slowHosts[hi.PublicKey] = d
	}
	if set < n {
		tb.Fatalf("not enough hosts to set as slow: only %d hosts available", len(hosts))
	}
}

func (m *mockHostClient) SetSectorReadDelay(root types.Hash256, d time.Duration) {
	m.sectorDelayMu.Lock()
	defer m.sectorDelayMu.Unlock()
	m.sectorDelays[root] = d
}

// SetFlakyHosts marks the first n hosts as flaky: each will fail its
// first failCount write attempts before succeeding.
func (m *mockHostClient) SetFlakyHosts(t *testing.T, n, failCount int) {
	t.Helper()

	hosts, _ := m.hosts.UsableHosts()
	if n > len(hosts) {
		t.Fatalf("cannot set %d flaky hosts: only %d hosts available", n, len(hosts))
	}

	m.flakyMu.Lock()
	defer m.flakyMu.Unlock()

	var set int
	for _, hi := range hosts {
		if set >= n {
			break
		}
		set++
		m.flakyHosts[hi.PublicKey] = failCount
	}
}

func newMockHostClient(hosts *hostCache) *mockHostClient {
	m := &mockHostClient{
		hosts:        hosts,
		provider:     client.NewProvider(hosts),
		slowHosts:    make(map[types.PublicKey]time.Duration),
		sectorDelays: make(map[types.Hash256]time.Duration),
		flakyHosts:   make(map[types.PublicKey]int),
		hostSectors:  make(map[types.PublicKey]map[types.Hash256][]byte),
		pricesCalls:  make(map[types.PublicKey]int),
	}
	return m
}

type mockAppClient struct {
	hosts *hostCache

	mu            sync.Mutex
	pinned        map[slabs.SlabID]slabs.PinnedSlab
	objects       map[types.Hash256]slabs.SealedObject
	hostsOverride []hosts.HostInfo
}

func newMockAppClient(hosts *hostCache) *mockAppClient {
	return &mockAppClient{
		hosts:   hosts,
		objects: make(map[types.Hash256]slabs.SealedObject),
		pinned:  make(map[slabs.SlabID]slabs.PinnedSlab),
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
	return nil
}

func (mc *mockAppClient) PruneSlabs(_ context.Context, _ types.PrivateKey) error {
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
