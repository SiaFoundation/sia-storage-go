package siastorage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

const (
	// maxHostAttempts is the maximum number of upload attempts per host
	// before it is permanently removed from the pool. The attempt counter is
	// tracked by the per-slab uploadPool across all shard goroutines.
	maxHostAttempts = 3

	// uploadTimeout is the per-attempt timeout for uploading a single
	// sector to a host.
	uploadTimeout = 90 * time.Second
)

type (
	shard struct {
		host types.PublicKey
		root types.Hash256

		index int
		err   error
	}

	slabUpload struct {
		encryptionKey [32]byte
		length        uint32

		shardsCh chan shard
		err      error
	}

	shardUpload struct {
		hosts      hostClient
		accountKey types.PrivateKey
		onProgress func(ShardProgress)

		encryptionKey [32]byte
		slabIndex     int

		sema     chan struct{}
		pool     *uploadPool
		shardsCh chan shard

		// waiting counts shards that still need their first upload attempt,
		// across every slab being uploaded. A shard only races slow hosts while
		// this is zero, so a racer never grabs a slot that another shard still
		// needs for its first try.
		waiting *changeCounter
	}

	uploadOption struct {
		dataShards   uint8
		parityShards uint8
		maxInflight  int
		onProgress   func(ShardProgress)
	}
)

// uploadPool holds the candidate hosts for a slab, shared by all of its shard
// goroutines. It reserves one candidate for each shard's initial attempt while
// allowing retries and racers to use any surplus hosts.
type uploadPool struct {
	hosts hostClient

	mu             sync.Mutex
	available      []types.PublicKey
	attempts       map[types.PublicKey]int
	pendingInitial int
}

func newUploadPool(hosts hostClient, candidates []types.PublicKey, pendingInitial int) *uploadPool {
	return &uploadPool{
		hosts:          hosts,
		available:      candidates,
		attempts:       make(map[types.PublicKey]int),
		pendingInitial: pendingInitial,
	}
}

// pickInitial reserves an inflight write slot for a shard's first attempt.
// pendingInitial ensures racers cannot consume hosts needed by shards that
// have not acquired the global upload semaphore yet.
func (p *uploadPool) pickInitial() (types.PublicKey, func(), int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pendingInitial == 0 {
		return types.PublicKey{}, nil, 0, false
	}
	p.pendingInitial--
	return p.pick()
}

// pick reserves an inflight write slot from the pool's surplus capacity. The
// caller must hold p.mu.
func (p *uploadPool) pick() (types.PublicKey, func(), int, bool) {
	if len(p.available) <= p.pendingInitial {
		return types.PublicKey{}, nil, 0, false
	}
	host, release, remaining, ok := p.hosts.PickWrite(p.available)
	if !ok {
		return types.PublicKey{}, nil, 0, false
	}
	p.available = remaining
	p.attempts[host]++
	return host, release, p.attempts[host], true
}

// retry returns host to the pool so a later pick can choose it again.
func (p *uploadPool) retry(host types.PublicKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.available = append(p.available, host)
}

// swap returns oldHost to the pool and picks a replacement under a single
// lock. Doing both atomically prevents another shard's racer from stealing
// the reclaimed host in the window between returning and re-picking, which
// would leave this shard with no host to retry. If returnOld is false the
// old host is not returned, making swap equivalent to pick.
func (p *uploadPool) swap(oldHost types.PublicKey, returnOld bool) (types.PublicKey, func(), int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if returnOld {
		p.available = append(p.available, oldHost)
	}
	return p.pick()
}

// pickRacer reserves an inflight write slot without consuming capacity
// reserved for shards that have not started their initial attempt.
func (p *uploadPool) pickRacer() (types.PublicKey, func(), int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pick()
}

// maxConcurrentSlabs returns the number of slabs that can be uploading at the
// same time. If one slow host is holding up the upload of a slab, we read
// the next slab and start uploading that set of shards.
func (uo uploadOption) maxConcurrentSlabs() int {
	totalShards := int(uo.dataShards) + int(uo.parityShards)
	return (uo.maxInflight+totalShards-1)/totalShards + 1
}

// newUploadOption creates an uploadOption with defaults, applies the given
// options, validates the erasure coding params, and creates the encoder.
func newUploadOption(opts ...UploadOption) (uploadOption, reedsolomon.Encoder, error) {
	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
		maxInflight:  30,
	}
	for _, opt := range opts {
		opt(&uo)
	}

	if uo.maxInflight <= 0 {
		return uo, nil, fmt.Errorf("maxInflight must be positive, got %d", uo.maxInflight)
	}

	totalShards := int(uo.dataShards) + int(uo.parityShards)
	if err := slabs.ValidateECParams(int(uo.dataShards), totalShards); err != nil {
		return uo, nil, err
	}

	enc, err := reedsolomon.New(int(uo.dataShards), int(uo.parityShards))
	if err != nil {
		return uo, nil, fmt.Errorf("failed to create erasure coder: %w", err)
	}

	return uo, enc, nil
}

func (s *SDK) uploadSlabs(ctx context.Context, respCh chan slabUpload, r io.Reader, enc reedsolomon.Encoder, uo uploadOption) {
	// convenience variables
	dataShards := int(uo.dataShards)
	parityShards := int(uo.parityShards)
	totalShards := dataShards + parityShards

	// create semaphore to limit concurrent shard uploads
	shardSema := make(chan struct{}, uo.maxInflight)

	// counts shards that still need their first attempt. shared across every
	// slab so a racer never steals a slot another shard still needs.
	waiting := newChangeCounter(0)

	// send guards against blocking on a full channel when the consumer
	// has stopped reading due to an error or context cancellation
	send := func(su slabUpload) {
		select {
		case <-ctx.Done():
		case respCh <- su:
		}
	}

	// buffer the reader since SlabReader reads 64 bytes at a time
	br := bufio.NewReader(r)
	sr := NewSlabReader(dataShards, parityShards)

	// read slabs in a loop
	for i := 0; ctx.Err() == nil; i++ {
		// fetch hosts and drain into a candidate pool for PickWrite
		queue, err := s.hosts.UploadQueue()
		if err != nil {
			send(slabUpload{err: fmt.Errorf("failed to get upload queue for slab %d: %w", i, err)})
			return
		}
		candidates := make([]types.PublicKey, 0, queue.Available())
		for host := range queue.Iter() {
			candidates = append(candidates, host)
		}
		if len(candidates) < totalShards {
			send(slabUpload{err: fmt.Errorf("not enough hosts available to upload slab %d: %d < %d", i, len(candidates), totalShards)})
			return
		}

		// read next slab
		slab, err := sr.ReadSlab(br)
		if slab.Length == 0 && errors.Is(err, io.EOF) {
			return
		} else if err != nil && !errors.Is(err, io.EOF) {
			send(slabUpload{err: fmt.Errorf("failed to read slab %d: %w", i, err)})
			return
		}

		// count this slab's shards as waiting before encoding so the racing
		// gate cannot open between buffering and the first upload attempts
		waiting.add(len(slab.Shards))

		// encode shards
		if err := enc.Encode(slab.Shards); err != nil {
			waiting.add(-len(slab.Shards))
			send(slabUpload{err: fmt.Errorf("failed to encode slab %d shards: %w", i, err)})
			return
		}

		// launch uploads for all shards
		su := shardUpload{
			hosts:         s.hosts,
			accountKey:    s.appKey,
			onProgress:    uo.onProgress,
			encryptionKey: frand.Entropy256(),
			slabIndex:     i,
			sema:          shardSema,
			pool:          newUploadPool(s.hosts, candidates, totalShards),
			shardsCh:      make(chan shard, totalShards),
			waiting:       waiting,
		}
		for shardIdx, data := range slab.Shards {
			go su.uploadShard(ctx, shardIdx, data)
		}

		// send slab off for collection
		send(slabUpload{
			encryptionKey: su.encryptionKey,
			length:        uint32(slab.Length),
			shardsCh:      su.shardsCh,
		})
	}
}

// uploadShard encrypts and uploads a single shard, racing slow hosts by
// spawning additional attempts after a timeout. Hosts are chosen from the
// shared pool, which reserves an inflight write slot per attempt.
func (su *shardUpload) uploadShard(ctx context.Context, shardIndex int, sector []byte) {
	// encrypt the sector
	nonce := make([]byte, 24)
	nonce[0] = byte(shardIndex)
	c, _ := chacha20.NewUnauthenticatedCipher(su.encryptionKey[:], nonce)
	c.XORKeyStream(sector, sector)

	// this shard counts toward waiting until it gets its first slot. only
	// release once. the defer keeps the count right even if we bail out early,
	// like when the context is cancelled before a slot frees up.
	var releaseOnce sync.Once
	releaseWaiting := func() { releaseOnce.Do(func() { su.waiting.add(-1) }) }
	defer releaseWaiting()

	// acquire semaphore
	select {
	case <-ctx.Done():
		su.shardsCh <- shard{index: shardIndex, err: ctx.Err()}
		return
	case su.sema <- struct{}{}:
	}
	// got a slot, so this shard no longer holds racing back
	releaseWaiting()

	initialHost, initialRelease, initialAttempts, ok := su.pool.pickInitial()
	if !ok {
		<-su.sema
		su.shardsCh <- shard{index: shardIndex, err: ErrNoMoreHosts}
		return
	}

	// shardCtx is cancelled when a write succeeds, aborting any racers
	shardCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type writeResult struct {
		host     types.PublicKey
		root     types.Hash256
		err      error
		elapsed  time.Duration
		canRetry bool
	}
	results := make(chan writeResult, 8)

	var initialDone atomic.Bool
	var active int
	launchWrite := func(host types.PublicKey, release func(), canRetry bool) {
		active++
		go func() {
			start := time.Now()
			root, err := writeSector(shardCtx, su.hosts, su.accountKey, host, sector, uploadTimeout)
			// the write is done, so release the inflight reservation and the
			// semaphore slot before the host can re-enter the pool, keeping the
			// inflight accounting accurate
			release()
			if host == initialHost {
				initialDone.Store(true)
			}
			// if the shard already completed, this result is stale: return the
			// host to the pool instead of racing to enqueue a result nobody
			// will read, which would leak the host out of the pool
			if shardCtx.Err() != nil {
				if ctx.Err() == nil && canRetry {
					su.pool.retry(host)
				}
				return
			}
			select {
			case results <- writeResult{host, root, err, time.Since(start), canRetry}:
			case <-shardCtx.Done():
				// a write won, return this host so other shards can use it
				if ctx.Err() == nil && canRetry {
					su.pool.retry(host)
				}
			}
		}()
	}

	launchWrite(initialHost, func() { initialRelease(); <-su.sema }, initialAttempts < maxHostAttempts)

	// only race a host once it is clearly slower than normal. before we have
	// timing data the estimate is large, so racing stays off until it warms up.
	raceTimeout := time.Duration(float64(su.hosts.WriteEstimate(uint64(len(sector)))) * raceFactor)
	lastEvent := time.Now()

	for {
		// snapshot the waiting count once so the eligibility test and the wakeup
		// channel stay consistent. only race when no shard is still waiting for
		// its first try. whether a host is free is checked when the timer fires
		// by attempting a pick, so the timer polls the pool instead of waiting
		// on an availability signal that might never come
		waiting, idleCh := su.waiting.snapshot()
		var raceCh <-chan time.Time
		if waiting == 0 {
			// Go cleans up this timer even if we never read the channel
			raceCh = time.After(time.Until(lastEvent.Add(raceTimeout)))
			// while racing we wait on the timer, not shards starting
			idleCh = nil
		}

		select {
		case <-ctx.Done():
			// defer cancel() unblocks the write goroutines, which release
			// their inflight reservations as they exit
			su.shardsCh <- shard{index: shardIndex, err: ctx.Err()}
			return

		case res := <-results:
			lastEvent = time.Now()
			if res.err == nil {
				// cancel the racers; they release their reservations as
				// they exit, so completion can be reported immediately
				cancel()

				// penalize the original host if a racer beat it while it was
				// still uploading
				if res.host != initialHost && !initialDone.Load() {
					su.hosts.AddFailedRPC(initialHost)
				}

				if su.onProgress != nil {
					su.onProgress(ShardProgress{
						HostKey:    res.host,
						SlabIndex:  su.slabIndex,
						ShardIndex: shardIndex,
						ShardSize:  uint64(len(sector)),
						Elapsed:    res.elapsed,
					})
				}
				su.shardsCh <- shard{
					index: shardIndex,
					host:  res.host,
					root:  res.root,
				}
				return
			}

			active--

			if active > 0 {
				// another write is still in flight; requeue the failed host so
				// it or another shard can retry it later
				if res.canRetry {
					su.pool.retry(res.host)
				}
			} else {
				// all active writes failed. acquire the semaphore before
				// touching the pool so we don't hold an inflight reservation
				// while blocked on it
				select {
				case <-ctx.Done():
					su.shardsCh <- shard{index: shardIndex, err: ctx.Err()}
					return
				case su.sema <- struct{}{}:
				}
				// atomically requeue the failed host and pick a replacement so
				// the reclaimed host cannot be stolen by another shard's racer
				// in the window between the two
				host, release, attempts, ok := su.pool.swap(res.host, res.canRetry)
				if !ok {
					<-su.sema
					su.shardsCh <- shard{index: shardIndex, err: ErrNoMoreHosts}
					return
				}
				launchWrite(host, func() { release(); <-su.sema }, attempts < maxHostAttempts)
			}

		case <-raceCh:
			lastEvent = time.Now()
			// check the gate again, a shard may have started waiting since
			if su.waiting.load() != 0 {
				continue
			}
			// race a slow host
			select {
			case su.sema <- struct{}{}:
				host, release, attempts, ok := su.pool.pickRacer()
				if !ok {
					<-su.sema
					continue
				}
				launchWrite(host, func() { release(); <-su.sema }, attempts < maxHostAttempts)
			default:
			}

		case <-idleCh:
			// the gate changed, loop around and check again
		}
	}
}

// collectSlabs reads uploaded slabs from the channel and collects their
// shard results into SlabSlices. It returns when the channel is closed
// or an error is encountered.
func collectSlabs(ctx context.Context, ch <-chan slabUpload, uo uploadOption) ([]slabs.SlabSlice, error) {
	totalShards := uo.dataShards + uo.parityShards
	var uploaded []slabs.SlabSlice

	for slab := range ch {
		if slab.err != nil {
			return nil, slab.err
		}

		sectors := make([]slabs.PinnedSector, totalShards)

		for range totalShards {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case shard := <-slab.shardsCh:
				if shard.err != nil {
					return nil, fmt.Errorf("failed to upload slab: shard upload failed: %w", shard.err)
				}
				sectors[shard.index] = slabs.PinnedSector{
					HostKey: shard.host,
					Root:    shard.root,
				}
			}
		}

		uploaded = append(uploaded, slabs.SlabSlice{
			EncryptionKey: slab.encryptionKey,
			MinShards:     uint(uo.dataShards),
			Sectors:       sectors,
			Offset:        0,
			Length:        slab.length,
		})
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return uploaded, nil
}
