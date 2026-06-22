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
	}

	uploadOption struct {
		dataShards   uint8
		parityShards uint8
		maxInflight  int
		onProgress   func(ShardProgress)
	}
)

// uploadPool holds the candidate hosts for a slab, shared by all of its shard
// goroutines. pick reserves an inflight write slot on the best host and retry
// returns a host to the pool so it can be picked again.
type uploadPool struct {
	hosts hostClient

	mu        sync.Mutex
	available []types.PublicKey
	attempts  map[types.PublicKey]int
}

func newUploadPool(hosts hostClient, candidates []types.PublicKey) *uploadPool {
	return &uploadPool{
		hosts:     hosts,
		available: candidates,
		attempts:  make(map[types.PublicKey]int),
	}
}

// pick reserves an inflight write slot on the best available host. The returned
// attempt count starts at 1 and increases each time the same host is picked.
func (p *uploadPool) pick() (types.PublicKey, func(), int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
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

		// encode shards
		if err := enc.Encode(slab.Shards); err != nil {
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
			pool:          newUploadPool(s.hosts, candidates),
			shardsCh:      make(chan shard, totalShards),
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

	// acquire semaphore
	select {
	case <-ctx.Done():
		su.shardsCh <- shard{index: shardIndex, err: ctx.Err()}
		return
	case su.sema <- struct{}{}:
	}

	initialHost, initialRelease, initialAttempts, ok := su.pool.pick()
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

	var wg sync.WaitGroup
	var initialDone atomic.Bool
	launchWrite := func(host types.PublicKey, release func(), canRetry bool) {
		wg.Add(1)
		go func() {
			defer func() { release(); <-su.sema; wg.Done() }()
			start := time.Now()
			root, err := writeSector(shardCtx, su.hosts, su.accountKey, host, sector, uploadTimeout)
			if host == initialHost {
				initialDone.Store(true)
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

	launchWrite(initialHost, initialRelease, initialAttempts < maxHostAttempts)
	active := 1

	// race timer scales with active attempts to avoid stampeding
	raceTimer := time.NewTimer(time.Second)
	defer raceTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			// cancel shardCtx to unblock write goroutines, then wait for
			// them to release their inflight reservations
			cancel()
			wg.Wait()
			su.shardsCh <- shard{index: shardIndex, err: ctx.Err()}
			return

		case res := <-results:
			if res.err == nil {
				cancel()

				// penalize the original host if a racer beat it
				// while it was still uploading
				if res.host != initialHost && !initialDone.Load() {
					su.hosts.AddFailedRPC(initialHost)
				}

				// wait for all write goroutines to release their inflight
				// reservations before reporting completion
				wg.Wait()

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

			// requeue failed host so it or another shard can retry it
			if res.canRetry {
				su.pool.retry(res.host)
			}

			// if all active writes failed, start a new one
			if active == 0 {
				host, release, attempts, ok := su.pool.pick()
				if !ok {
					cancel()
					wg.Wait()
					su.shardsCh <- shard{index: shardIndex, err: ErrNoMoreHosts}
					return
				}
				select {
				case <-ctx.Done():
					release()
					cancel()
					wg.Wait()
					su.shardsCh <- shard{index: shardIndex, err: ctx.Err()}
					return
				case su.sema <- struct{}{}:
				}
				launchWrite(host, release, attempts < maxHostAttempts)
				active++
			}

			// timer may have fired while processing results but the
			// semaphore bounds any spurious race spawns
			raceTimer.Reset(time.Duration(max(active, 1)) * time.Second)

		case <-raceTimer.C:
			// race a slow host
			select {
			case su.sema <- struct{}{}:
				host, release, attempts, ok := su.pool.pick()
				if !ok {
					<-su.sema
					continue
				}
				launchWrite(host, release, attempts < maxHostAttempts)
				active++
			default:
			}

			raceTimer.Reset(time.Duration(max(active, 1)) * time.Second)
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
