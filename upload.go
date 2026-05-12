package siastorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/reedsolomon"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
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
		slabIndex     int

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

func (s *SDK) uploadSlabs(ctx context.Context, respCh chan slabUpload, r io.Reader, enc reedsolomon.Encoder, uo uploadOption) {
	// convenience variables
	dataShards := int(uo.dataShards)
	parityShards := int(uo.parityShards)
	totalShards := dataShards + parityShards

	// create semaphore to limit concurrent shard uploads
	shardSema := make(chan struct{}, uo.maxInflight)

	// read slabs in a loop
	sr := NewSlabReader(dataShards, parityShards)
	for i := 0; ctx.Err() == nil; i++ {
		// fetch hosts for this slab
		queue, err := s.hosts.UploadQueue()
		if err != nil {
			respCh <- slabUpload{err: fmt.Errorf("failed to get upload queue for slab %d: %w", i, err)}
			return
		} else if queue.Available() < totalShards {
			respCh <- slabUpload{err: fmt.Errorf("not enough hosts available to upload slab %d: %d < %d", i, queue.Available(), totalShards)}
			return
		}

		// read next slab
		slab, err := sr.ReadSlab(r)
		if slab.Length == 0 && errors.Is(err, io.EOF) {
			respCh <- slabUpload{err: io.EOF}
			return
		} else if err != nil && !errors.Is(err, io.EOF) {
			respCh <- slabUpload{err: fmt.Errorf("failed to read slab %d: %w", i, err)}
			return
		}

		// encode parity shards
		if err := enc.Encode(slab.Shards); err != nil {
			respCh <- slabUpload{err: fmt.Errorf("failed to encode slab %d shards: %w", i, err)}
			return
		}

		// pop initial hosts for all shards
		initialHosts, ok := popN(queue, totalShards)
		if !ok {
			respCh <- slabUpload{err: fmt.Errorf("not enough hosts available to upload slab %d", i)}
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
		respCh <- slabUpload{
			encryptionKey: encryptionKey,
			length:        uint32(slab.Length),
			slabIndex:     i,
			uploadsCh:     uploadsCh,
		}
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

			// requeue failed host so other shards can use it
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

		case <-time.After(time.Duration(max(active, 1)) * time.Second):
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
		}
	}
}

// popN pops n hosts from the front of the queue and returns them.
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
