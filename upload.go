package siastorage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/reedsolomon"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/slabs"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

// uploadTimeout is the per-attempt timeout for uploading a single sector
// to a host.
const uploadTimeout = 90 * time.Second

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

		uploadsCh chan shard
		err       error
	}

	uploadOption struct {
		dataShards   uint8
		parityShards uint8
		maxInflight  int
		onProgress   func(ShardProgress)
	}
)

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

		for n := totalShards; n > 0; n-- {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case shard := <-slab.uploadsCh:
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

func (s *SDK) uploadSlabs(ctx context.Context, respCh chan slabUpload, r io.Reader, enc reedsolomon.Encoder, uo uploadOption) {
	dataShards := int(uo.dataShards)
	parityShards := int(uo.parityShards)
	totalShards := dataShards + parityShards

	// create semaphore to limit concurrent shard uploads
	shardSema := make(chan struct{}, uo.maxInflight)

	// send guards against blocking on a full channel when the consumer
	// has stopped reading due to an error or context cancellation
	send := func(su slabUpload) bool {
		select {
		case <-ctx.Done():
			return false
		case respCh <- su:
			return true
		}
	}

	// buffer the reader since SlabReader reads 64 bytes at a time
	br := bufio.NewReader(r)

	// read slabs in a loop
	sr := NewSlabReader(dataShards, parityShards)
	for i := 0; ctx.Err() == nil; i++ {
		// fetch hosts for this slab
		queue, err := s.hosts.UploadQueue()
		if err != nil {
			send(slabUpload{err: fmt.Errorf("failed to get upload queue for slab %d: %w", i, err)})
			return
		} else if queue.Available() < totalShards {
			send(slabUpload{err: fmt.Errorf("not enough hosts available to upload slab %d: %d < %d", i, queue.Available(), totalShards)})
			return
		}

		// read next slab
		slab, err := sr.ReadSlab(br)
		if slab.Length == 0 && err == io.EOF {
			return
		} else if err != nil && err != io.EOF {
			send(slabUpload{err: fmt.Errorf("failed to read slab %d: %w", i, err)})
			return
		}

		// encode parity shards
		if err := enc.Encode(slab.Shards); err != nil {
			send(slabUpload{err: fmt.Errorf("failed to encode slab %d shards: %w", i, err)})
			return
		}

		// pop initial hosts for all shards
		initialHosts, ok := popN(queue, totalShards)
		if !ok {
			send(slabUpload{err: fmt.Errorf("not enough hosts available to upload slab %d", i)})
			return
		}

		// generate a random encryption key
		encryptionKey := frand.Entropy256()

		// launch uploads for all shards
		uploadsCh := make(chan shard, totalShards)
		for shardIdx, data := range slab.Shards {
			go uploadShard(ctx, s.hosts, s.appKey, shardSema, queue, uploadsCh, encryptionKey, i, shardIdx, initialHosts[shardIdx], data, uo.onProgress)
		}

		// send slab off for collection
		send(slabUpload{
			encryptionKey: encryptionKey,
			length:        uint32(slab.Length),
			uploadsCh:     uploadsCh,
		})
	}
}

// uploadShard encrypts and uploads a single shard to a host, racing slow hosts
// by spawning additional upload attempts after a timeout.
func uploadShard(ctx context.Context, hosts hostClient, accountKey types.PrivateKey, sema chan struct{}, queue *client.HostQueue, resultCh chan shard, encryptionKey [32]byte, slabIndex, shardIndex int, initialHost types.PublicKey, sector []byte, onProgress func(ShardProgress)) {
	// encrypt the sector
	nonce := make([]byte, 24)
	nonce[0] = byte(shardIndex)
	c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce)
	c.XORKeyStream(sector, sector)

	// acquire initial semaphore slot
	select {
	case <-ctx.Done():
		resultCh <- shard{index: shardIndex, err: ctx.Err()}
		return
	case sema <- struct{}{}:
	}

	// shardCtx is cancelled when a write succeeds, aborting any racers
	shardCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(client.ErrAbortedRPC)

	type writeResult struct {
		host    types.PublicKey
		root    types.Hash256
		err     error
		elapsed time.Duration
	}
	results := make(chan writeResult, 8)

	spawnWrite := func(host types.PublicKey) {
		go func() {
			defer func() { <-sema }()
			start := time.Now()
			root, err := writeSector(shardCtx, hosts, accountKey, host, sector, uploadTimeout)
			select {
			case results <- writeResult{host, root, err, time.Since(start)}:
			case <-shardCtx.Done():
			}
		}()
	}

	spawnWrite(initialHost)
	active := 1

	// race timer scales with active attempts to avoid stampeding
	raceTimer := time.NewTimer(time.Second)
	defer raceTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			resultCh <- shard{index: shardIndex, err: ctx.Err()}
			return

		case res := <-results:
			if res.err == nil {
				cancel(client.ErrAbortedRPC)
				if onProgress != nil {
					onProgress(ShardProgress{
						HostKey:    res.host,
						SlabIndex:  slabIndex,
						ShardIndex: shardIndex,
						ShardSize:  uint64(len(sector)),
						Elapsed:    res.elapsed,
					})
				}
				resultCh <- shard{
					index: shardIndex,
					host:  res.host,
					root:  res.root,
				}
				return
			}

			active--

			// requeue failed host so other shards can try it
			queue.Retry(res.host)

			// if all active writes failed, start a new one
			if active == 0 {
				host, _, ok := queue.Next()
				if !ok {
					resultCh <- shard{index: shardIndex, err: ErrNoMoreHosts}
					return
				}
				select {
				case <-ctx.Done():
					resultCh <- shard{index: shardIndex, err: ctx.Err()}
					return
				case sema <- struct{}{}:
				}
				spawnWrite(host)
				active++
			}

			raceTimer.Reset(time.Duration(max(active, 1)) * time.Second)

		case <-raceTimer.C:
			// race a slow host
			select {
			case sema <- struct{}{}:
				host, _, ok := queue.Next()
				if !ok {
					<-sema
					continue
				}
				spawnWrite(host)
				active++
			default:
			}

			raceTimer.Reset(time.Duration(max(active, 1)) * time.Second)
		}
	}
}

// popN pops n hosts from the front of the queue and returns them.
//
// TODO: add Pop() and PopN() to HostQueue in indexd so we don't have to
// discard the attempt count from Next().
func popN(queue *client.HostQueue, n int) ([]types.PublicKey, bool) {
	hosts := make([]types.PublicKey, n)
	for i := range n {
		host, _, ok := queue.Next()
		if !ok {
			return nil, false
		}
		hosts[i] = host
	}
	return hosts, true
}
