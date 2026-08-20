package siastorage

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"golang.org/x/crypto/chacha20"
)

const (
	// maxHostAttempts is the maximum number of upload attempts per host
	// before it is permanently removed from the pool. The attempt counter is
	// tracked by the per-slab uploadPool across all shard goroutines.
	maxHostAttempts = 3

	// uploadTimeout is the per-attempt timeout for uploading a single
	// sector to a host.
	uploadTimeout = 90 * time.Second

	// initialUploadInflight is the number of shard uploads allowed in flight
	// before the controller has measured anything, and minUploadInflight is the
	// floor it may back off to.
	initialUploadInflight = 8
	minUploadInflight     = 2
)

type (
	shard struct {
		host types.PublicKey
		root types.Hash256

		index int
		err   error
	}

	slabUpload struct {
		resultCh chan slabResult
		err      error
	}

	slabResult struct {
		slab slabs.SlabSlice
		err  error
	}

	shardUpload struct {
		hosts      hostClient
		accountKey types.PrivateKey
		onProgress func(ShardProgress)

		encryptionKey [32]byte
		slabIndex     int

		limiter *inflightLimiter
		pool    *uploadPool

		// commitment holds one limiter permit per encoded shard, covering the
		// memory the slab occupies. Each shard frees its own once it finishes.
		commitment *commitment

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
		// maxBufferedSlabs is the memory ceiling for the upload, in encoded
		// slabs. Zero derives it from the memory budget.
		maxBufferedSlabs int
		onProgress       func(ShardProgress)
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
// have not acquired an upload permit yet.
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

// newUploadOption creates an uploadOption with defaults, applies the given
// options, validates the erasure coding params, and creates the encoder.
func newUploadOption(opts ...UploadOption) (uploadOption, reedsolomon.Encoder, error) {
	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
	}
	for _, opt := range opts {
		opt(&uo)
	}

	if uo.maxBufferedSlabs < 0 {
		return uo, nil, fmt.Errorf("max buffered slabs must not be negative, got %d", uo.maxBufferedSlabs)
	}

	totalShards := int(uo.dataShards) + int(uo.parityShards)
	if err := slabs.ValidateECParams(int(uo.dataShards), totalShards); err != nil {
		return uo, nil, err
	}

	enc, err := reedsolomon.New(int(uo.dataShards), int(uo.parityShards))
	if err != nil {
		return uo, nil, fmt.Errorf("failed to create erasure coder: %w", err)
	}
	if uo.maxBufferedSlabs == 0 {
		uo.maxBufferedSlabs = defaultSlabsInMemory(totalShards)
	}
	// a slab holds one permit per encoded shard, so the slab budget becomes a
	// capacity only once multiplied out
	uo.maxBufferedSlabs = clampBufferBudget(uo.maxBufferedSlabs, totalShards)

	return uo, enc, nil
}

// readSlab fills buf and returns the number of bytes read along with the
// error that ended the read. Unlike io.ReadFull it never rewrites a short read
// as io.ErrUnexpectedEOF, so a reader failing with that error is not mistaken
// for a clean io.EOF, and it preserves an error returned by the same read that
// filled buf.
func readSlab(r io.Reader, buf []byte) (int, error) {
	var n int
	for n < len(buf) {
		nn, err := r.Read(buf[n:])
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// uploadPlaintextSlabs uploads slabs read from r, encrypting each one with
// dataKey using the slab's key as the nonce.
func (s *SDK) uploadPlaintextSlabs(ctx context.Context, respCh chan slabUpload, r io.Reader, dataKey [32]byte, slabKeys *slabKeySource, enc reedsolomon.Encoder, uo uploadOption) {
	s.uploadSlabs(ctx, respCh, r, slabKeys, enc, uo, func(slabKey [32]byte, data []byte) {
		newV1CipherStream(&dataKey, &slabKey, 0).XORKeyStream(data, data)
	})
}

// uploadEncryptedSlabs uploads slabs read from r whose object data the caller
// already encrypted, as PackedUpload.Add does.
func (s *SDK) uploadEncryptedSlabs(ctx context.Context, respCh chan slabUpload, r io.Reader, slabKeys *slabKeySource, enc reedsolomon.Encoder, uo uploadOption) {
	s.uploadSlabs(ctx, respCh, r, slabKeys, enc, uo, func([32]byte, []byte) {})
}

// uploadSlabs reads slab-sized chunks from r, applies encrypt to each one, and
// uploads its shards. The slab keys double as the nonce for the object data and
// as the key for the per-shard layer.
func (s *SDK) uploadSlabs(ctx context.Context, respCh chan slabUpload, r io.Reader, slabKeys *slabKeySource, enc reedsolomon.Encoder, uo uploadOption, encrypt func(slabKey [32]byte, data []byte)) {
	// convenience variables
	dataShards := int(uo.dataShards)
	parityShards := int(uo.parityShards)
	totalShards := dataShards + parityShards
	dataSize := dataShards * proto4.SectorSize

	// adapt the number of shard uploads in flight, capped by the memory budget.
	// newUploadOption already clamped the slab budget, so this cannot exceed
	// maxInflightLimit. A shard upload is both the limited and the sampled
	// unit, so the window needs no scaling.
	capacity := uo.maxBufferedSlabs * totalShards
	limiter := newInflightLimiter(initialUploadInflight, minUploadInflight, capacity, 1, s.log)

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

	// read slabs in a loop
	for i := 0; ctx.Err() == nil; i++ {
		// commit memory for the slab's encoded shards before reading it, so
		// encoding cannot run away ahead of the uploads
		slabCommitment, ok := limiter.commit(ctx, totalShards)
		if !ok {
			return
		}

		// fetch hosts and drain into a candidate pool for PickWrite
		queue, err := s.hosts.UploadQueue()
		if err != nil {
			slabCommitment.releaseAll()
			send(slabUpload{err: fmt.Errorf("failed to get upload queue for slab %d: %w", i, err)})
			return
		}
		candidates := make([]types.PublicKey, 0, queue.Available())
		for host := range queue.Iter() {
			candidates = append(candidates, host)
		}
		if len(candidates) < totalShards {
			slabCommitment.releaseAll()
			send(slabUpload{err: fmt.Errorf("not enough hosts available to upload slab %d: %d < %d", i, len(candidates), totalShards)})
			return
		}

		// read the next raw slab; io.EOF means the stream ended, every other
		// error is fatal
		buf := make([]byte, dataSize)
		n, err := readSlab(r, buf)
		last := err == io.EOF
		if err != nil && !last {
			slabCommitment.releaseAll()
			send(slabUpload{err: fmt.Errorf("failed to read slab %d: %w", i, err)})
			return
		} else if n == 0 {
			slabCommitment.releaseAll()
			return
		}

		su := shardUpload{
			hosts:         s.hosts,
			accountKey:    s.appKey,
			onProgress:    uo.onProgress,
			encryptionKey: slabKeys.key(i),
			slabIndex:     i,
			limiter:       limiter,
			pool:          newUploadPool(s.hosts, candidates, totalShards),
			commitment:    slabCommitment,
			shardsCh:      make(chan shard, totalShards),
			waiting:       waiting,
		}
		resultCh := make(chan slabResult, 1)

		// count this slab's shards as waiting before the encode goroutine starts
		// so the racing gate cannot open between buffering and the first upload
		// attempts
		waiting.add(totalShards)

		// encrypt, stripe, and encode off the read loop; buf starts at a slab
		// boundary, so the slab's stream starts at offset 0, and encode errors
		// surface through the shard channel
		buf = buf[:n]
		go func() {
			encrypt(su.encryptionKey, buf)
			shards := make([][]byte, totalShards)
			for j := range shards {
				shards[j] = make([]byte, proto4.SectorSize)
			}
			splitShards(shards[:dataShards], buf)
			if err := enc.Encode(shards); err != nil {
				// no shard gets its first attempt, so release the whole slab
				waiting.add(-totalShards)
				su.commitment.releaseAll()
				resultCh <- slabResult{err: fmt.Errorf("failed to encode slab %d shards: %w", su.slabIndex, err)}
				return
			}
			for shardIdx, data := range shards {
				go su.uploadShard(ctx, shardIdx, data)
			}

			// pin from the slab task so a slab is protected as soon as its
			// own shards land
			resultCh <- s.collectSlab(ctx, &su, uo, uint32(n))
		}()

		// send slab off for collection
		send(slabUpload{resultCh: resultCh})
		if last {
			return
		}
	}
}

// uploadShard encrypts and uploads a single shard, racing slow hosts by
// spawning additional attempts after a timeout. Hosts are chosen from the
// shared pool, which reserves an inflight write slot per attempt.
func (su *shardUpload) uploadShard(ctx context.Context, shardIndex int, sector []byte) {
	// the shard stops counting against the memory budget once its upload is
	// done, whether it succeeded or not
	defer su.commitment.releaseOne()

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

	// wait until the adaptive limit leaves room for this shard's first attempt
	releasePermit, ok := su.limiter.acquire(ctx)
	if !ok {
		su.shardsCh <- shard{index: shardIndex, err: ctx.Err()}
		return
	}
	// got a slot, so this shard no longer holds racing back
	releaseWaiting()

	initialHost, initialRelease, initialAttempts, ok := su.pool.pickInitial()
	if !ok {
		releasePermit()
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

	var active int // only touched by this goroutine, like the launches it gates
	launchWrite := func(host types.PublicKey, release func(), canRetry bool) {
		// only the attempt launched with nothing else in flight races from the
		// front, so only its loss says anything about its own speed
		initial := active == 0
		active++
		go func() {
			permit := su.limiter.sample()
			start := time.Now()
			root, timedOut, err := writeSector(shardCtx, su.hosts, su.accountKey, host, sector, uploadTimeout)
			elapsed := time.Since(start)
			// a racer aborted because another attempt won measured the
			// cancellation, not the network, so it is not a congestion signal
			if shardCtx.Err() == nil {
				su.limiter.record(permit, elapsed, err == nil)
			}
			// attempts are cancelled only once one lands, so an initial attempt
			// still uploading then was beaten by a racer, not merely started late
			beatenByRacer := initial && err != nil && shardCtx.Err() != nil && ctx.Err() == nil
			// the write is done, so release the host's inflight reservation and
			// this attempt's permit before the host can re-enter the pool,
			// keeping the inflight accounting accurate
			release()
			// a write reports no partial progress, so the whole sector over
			// the deadline it burned is the worst-case sample. a racer's win
			// tells us the attempt lost, not how slow it was, so a beaten
			// attempt only counts against the failure rate
			if timedOut {
				su.hosts.AddTimedOutRPC(host, true, uint64(len(sector)), elapsed)
			} else if beatenByRacer {
				su.hosts.AddFailedRPC(host)
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
			case results <- writeResult{host, root, err, elapsed, canRetry}:
			case <-shardCtx.Done():
				// a write won, return this host so other shards can use it
				if ctx.Err() == nil && canRetry {
					su.pool.retry(host)
				}
			}
		}()
	}

	launchWrite(initialHost, func() { initialRelease(); releasePermit() }, initialAttempts < maxHostAttempts)

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
				// all active writes failed. acquire a permit before touching
				// the pool so we don't hold an inflight reservation while
				// blocked on it
				releasePermit, ok := su.limiter.acquire(ctx)
				if !ok {
					su.shardsCh <- shard{index: shardIndex, err: ctx.Err()}
					return
				}
				// atomically requeue the failed host and pick a replacement so
				// the reclaimed host cannot be stolen by another shard's racer
				// in the window between the two
				host, release, attempts, ok := su.pool.swap(res.host, res.canRetry)
				if !ok {
					releasePermit()
					su.shardsCh <- shard{index: shardIndex, err: ErrNoMoreHosts}
					return
				}
				launchWrite(host, func() { release(); releasePermit() }, attempts < maxHostAttempts)
			}

		case <-raceCh:
			lastEvent = time.Now()
			// check the gate again, a shard may have started waiting since
			if su.waiting.load() != 0 {
				continue
			}
			// race a slow host
			if releasePermit, ok := su.limiter.tryAcquire(); ok {
				host, release, attempts, ok := su.pool.pickRacer()
				if !ok {
					releasePermit()
					continue
				}
				launchWrite(host, func() { release(); releasePermit() }, attempts < maxHostAttempts)
			}

		case <-idleCh:
			// the gate changed, loop around and check again
		}
	}
}

// collectSlab waits for the slab's shards, assembles them, and pins the slab to
// the indexer.
func (s *SDK) collectSlab(ctx context.Context, su *shardUpload, uo uploadOption, length uint32) slabResult {
	// 128 data and 128 parity shards is a valid redundancy that overflows
	// uint8, so widen both before summing
	totalShards := int(uo.dataShards) + int(uo.parityShards)

	sectors := make([]slabs.PinnedSector, totalShards)
	for range totalShards {
		select {
		case <-ctx.Done():
			return slabResult{err: ctx.Err()}
		case sh := <-su.shardsCh:
			if sh.err != nil {
				return slabResult{err: fmt.Errorf("failed to upload slab: shard upload failed: %w", sh.err)}
			}
			sectors[sh.index] = slabs.PinnedSector{
				HostKey: sh.host,
				Root:    sh.root,
			}
		}
	}

	slab := slabs.SlabSlice{
		Version:       1,
		EncryptionKey: su.encryptionKey,
		MinShards:     uint(uo.dataShards),
		Sectors:       sectors,
		Offset:        0,
		Length:        length,
	}
	if err := s.pinSlab(ctx, slab); err != nil {
		return slabResult{err: err}
	}
	return slabResult{slab: slab}
}

// collectSlabs reads the uploaded slabs in the order they were started. Each
// slab is assembled and pinned by its own task, so this only orders results.
func (s *SDK) collectSlabs(ctx context.Context, ch <-chan slabUpload) ([]slabs.SlabSlice, error) {
	var uploaded []slabs.SlabSlice

	for su := range ch {
		if su.err != nil {
			return nil, su.err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-su.resultCh:
			if res.err != nil {
				return nil, res.err
			}
			uploaded = append(uploaded, res.slab)
		}
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return uploaded, nil
}
