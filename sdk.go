package siastorage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
)

// pinBatchSize is the maximum number of slabs sent to the indexer in a
// single PinSlabs request.
const pinBatchSize = 50

type (
	// A hostClient is an interface for interacting with hosts.
	hostClient interface {
		Prices(ctx context.Context, hostKey types.PublicKey) (prices proto4.HostPrices, err error)
		ReadSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, root types.Hash256, w io.Writer, offset, length uint64) (rhp.RPCReadSectorResult, error)
		WriteSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte) (rhp.RPCWriteSectorResult, error)

		AddFailedRPC(hostKey types.PublicKey)
		Prioritize(hosts []types.PublicKey) []types.PublicKey
		UploadQueue() (*client.HostQueue, error)
		Close() error
	}

	// An appClient is an interface for the application API of the indexer.
	appClient interface {
		Account(ctx context.Context, appKey types.PrivateKey) (resp app.AccountResponse, err error)

		Hosts(context.Context, types.PrivateKey, ...api.URLQueryParameterOption) ([]hosts.HostInfo, error)

		CreateSharedObjectURL(ctx context.Context, appKey types.PrivateKey, objectID types.Hash256, encryptionKey []byte, validUntil time.Time) (string, error)
		SharedObject(ctx context.Context, sharedURL string) (slabs.SharedObject, []byte, error)

		ListObjects(ctx context.Context, appKey types.PrivateKey, cursor slabs.Cursor, limit int) ([]slabs.ObjectEvent, error)
		Object(ctx context.Context, appKey types.PrivateKey, key types.Hash256) (slabs.SealedObject, error)
		PinObject(ctx context.Context, appKey types.PrivateKey, obj slabs.SealedObject) error
		DeleteObject(ctx context.Context, appKey types.PrivateKey, key types.Hash256) error

		Slab(context.Context, types.PrivateKey, slabs.SlabID) (slabs.PinnedSlab, error)
		PinSlabs(context.Context, types.PrivateKey, ...slabs.SlabPinParams) ([]slabs.SlabID, error)
		UnpinSlab(context.Context, types.PrivateKey, slabs.SlabID) error
		PruneSlabs(context.Context, types.PrivateKey, ...api.URLQueryParameterOption) error
	}

	downloadOption struct {
		hostTimeout time.Duration
		maxInflight int
		offset      uint64
		length      uint64
		onProgress  func(ShardProgress)
	}

	// A ShardProgress reports the result of a successfully completed
	// shard upload or download.
	ShardProgress struct {
		HostKey    types.PublicKey
		SlabIndex  int
		ShardIndex int
		ShardSize  uint64
		Elapsed    time.Duration
	}

	// An UploadOption configures the upload behavior
	UploadOption func(*uploadOption)

	// A DownloadOption configures the download behavior
	DownloadOption func(*downloadOption)

	// An SDK is a client for the indexd service.
	SDK struct {
		app        appClient
		hosts      hostClient
		hostsCache *hostCache

		appKey types.PrivateKey

		tg  *threadgroup.ThreadGroup
		log *zap.Logger
	}
)

var (
	// ErrNotEnoughShards is returned when not enough shards were
	// uploaded or downloaded to satisfy the minimum required shards.
	ErrNotEnoughShards = errors.New("not enough shards")

	// ErrNoMoreHosts is returned when there are no more hosts
	// available to attempt to upload a shard
	ErrNoMoreHosts = errors.New("no more hosts available")
)

type sectorDownload struct {
	index  int
	sector slabs.PinnedSector
}

func (s *SDK) downloadSlab(ctx context.Context, slab slabs.SlabSlice, slabIndex int, timeout time.Duration, onProgress func(ShardProgress)) ([][]byte, error) {
	if slab.MinShards == 0 {
		return nil, errors.New("invalid slab: min shards cannot be 0")
	} else if int(slab.MinShards) > len(slab.Sectors) {
		return nil, fmt.Errorf("invalid slab: min shards %d exceeds sector count %d", slab.MinShards, len(slab.Sectors))
	}

	slabSectors := make(map[types.PublicKey]sectorDownload)
	slabHosts := make([]types.PublicKey, 0, len(slab.Sectors))
	for i, sector := range slab.Sectors {
		slabSectors[sector.HostKey] = sectorDownload{
			index:  i,
			sector: sector,
		}
		slabHosts = append(slabHosts, sector.HostKey)
	}
	if len(slabHosts) < int(slab.MinShards) {
		return nil, fmt.Errorf("slab has %d sectors with hosts, minimum required: %d: %w", len(slabHosts), slab.MinShards, ErrNotEnoughShards)
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		wg.Wait()
	}()

	// calculate offset and length that's required from each sector to recover
	// the data referenced by the slab slice
	offset, length := sectorRegion(slab)

	// prioritize hosts
	slabHosts = s.hosts.Prioritize(slabHosts)

	// helper to launch download
	type result struct {
		index   int
		buf     []byte
		err     error
		hostKey types.PublicKey
		elapsed time.Duration
	}
	responseCh := make(chan result, len(slab.Sectors))
	var outstanding int
	tryDownloadSector := func(d sectorDownload, initial bool) {
		outstanding++
		wg.Go(func() {
			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			buf := bytes.NewBuffer(make([]byte, 0, length))
			start := time.Now()
			_, err := s.hosts.ReadSector(timeoutCtx, s.appKey, d.sector.HostKey, d.sector.Root, buf, offset, length)
			select {
			case <-ctx.Done():
			case responseCh <- result{
				index:   d.index,
				buf:     buf.Bytes(),
				err:     err,
				hostKey: d.sector.HostKey,
				elapsed: time.Since(start),
			}:
			}
			// a host gets demoted if either
			// 1. it hit the shard timeout
			// 2. it was part of the initial batch of hosts and was interrupted
			if (timeoutCtx.Err() != nil && ctx.Err() == nil) || (initial && ctx.Err() != nil) {
				s.hosts.AddFailedRPC(d.sector.HostKey)
			}
		})
	}

	// launch minShards downloads right away
	for range slab.MinShards {
		tryDownloadSector(slabSectors[slabHosts[0]], true)
		slabHosts = slabHosts[1:]
	}

	// launch more downloads as results come in or periodically to race
	// slow hosts
	var successful int
	shards := make([][]byte, len(slab.Sectors))
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case res := <-responseCh:
			outstanding--
			if res.err == nil {
				// successful download
				shards[res.index] = res.buf
				successful++
				if successful <= int(slab.MinShards) && onProgress != nil {
					onProgress(ShardProgress{
						HostKey:    res.hostKey,
						SlabIndex:  slabIndex,
						ShardIndex: res.index,
						ShardSize:  uint64(len(res.buf)),
						Elapsed:    res.elapsed,
					})
				}
				if successful >= int(slab.MinShards) {
					// enough shards downloaded
					return shards, nil
				}
			}
			// check if enough potential successes remain
			rem := int(slab.MinShards) - successful
			if outstanding+len(slabHosts) < rem {
				return nil, ErrNotEnoughShards
			}
			if res.err != nil && len(slabHosts) > 0 {
				tryDownloadSector(slabSectors[slabHosts[0]], false)
				slabHosts = slabHosts[1:]
			}
		case <-timer.C:
			// periodically launch an extra download to race slow hosts
			if len(slabHosts) > 0 {
				tryDownloadSector(slabSectors[slabHosts[0]], false)
				slabHosts = slabHosts[1:]
			}
		case <-ctx.Done():
			// download got interrupted before it could finish
			return nil, ctx.Err()
		}
		timer.Reset(500 * time.Millisecond)
	}
}

// AppKey returns the app key used by the SDK.
//
// It should be kept secret. Applications
// should store it securely to authenticate with
// the indexer.
func (s *SDK) AppKey() types.PrivateKey {
	return s.appKey
}

// Account retrieves account information for the current app key.
func (s *SDK) Account(ctx context.Context) (app.AccountResponse, error) {
	return s.app.Account(ctx, s.appKey)
}

// PruneSlabs removes all slabs on the account that are not associated with
// an object.
func (s *SDK) PruneSlabs(ctx context.Context, opts ...api.URLQueryParameterOption) error {
	return s.app.PruneSlabs(ctx, s.appKey, opts...)
}

// DeleteObject deletes the object with the given key from the indexer.
func (s *SDK) DeleteObject(ctx context.Context, key types.Hash256) error {
	return s.app.DeleteObject(ctx, s.appKey, key)
}

// Upload uploads the data to hosts.
//
// Appends the metadata of the slabs that were uploaded to the given object.
// After uploading the object, the caller must call PinObject to pin the
// slabs and save the object metadata to the indexer.
func (s *SDK) Upload(ctx context.Context, obj *Object, r io.Reader, opts ...UploadOption) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create upload options
	uo, enc, err := newUploadOption(opts...)
	if err != nil {
		return err
	}

	// encrypt the reader on the fly
	r = encrypt((*[32]byte)(obj.dataKey), r, obj.Size())

	// start uploading slabs
	slabsCh := make(chan slabUpload, uo.maxConcurrentSlabs())
	go func() {
		defer close(slabsCh)
		s.uploadSlabs(ctx, slabsCh, r, enc, uo)
	}()

	// collect uploaded slabs
	uploaded, err := collectSlabs(ctx, slabsCh, uo)
	if err != nil {
		return err
	}

	obj.slabs = append(obj.slabs, uploaded...)
	return nil
}

// Download returns an [io.ReadCloser] streaming the object's data. Closing the
// reader cancels the underlying download. Callers must always Close the
// returned reader to release resources.
func (s *SDK) Download(obj Object, opts ...DownloadOption) (io.ReadCloser, error) {
	do := defaultDownloadOption(obj.Size())
	for _, opt := range opts {
		opt(&do)
	}

	if !do.normalizeRange(obj.Size()) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	if len(obj.dataKey) != 32 {
		return nil, fmt.Errorf("invalid data key length: %d", len(obj.dataKey))
	}

	return s.downloadReader((*[32]byte)(obj.dataKey), obj.slabs, do), nil
}

// DownloadSharedObject returns an [io.ReadCloser] streaming a shared object's
// data. Closing the reader cancels the underlying download. Callers must always
// Close the returned reader to release resources.
func (s *SDK) DownloadSharedObject(ctx context.Context, sharedURL string, opts ...DownloadOption) (io.ReadCloser, error) {
	obj, encryptionKey, err := s.app.SharedObject(ctx, sharedURL)
	if err != nil {
		return nil, err
	}

	do := defaultDownloadOption(obj.Size())
	for _, opt := range opts {
		opt(&do)
	}

	if !do.normalizeRange(obj.Size()) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	return s.downloadReader((*[32]byte)(encryptionKey), obj.Slabs, do), nil
}

// downloadReader streams reconstructed chunks directly to readers. The returned
// reader, when closed, cancels any chunk downloads in flight.
func (s *SDK) downloadReader(key *[32]byte, ss []slabs.SlabSlice, do downloadOption) io.ReadCloser {
	ctx, cancel := context.WithCancel(context.Background())
	return &downloadStream{
		chunks: newChunkDownloader(ctx, cancel, s, do.maxInflight, do.hostTimeout, ss, do.offset, do.length, do.onProgress),
		cipher: newRekeyStream(key, uint64(do.offset)),
	}
}

// downloadStream is an [io.ReadCloser] that streams reconstructed, decrypted
// chunks.
type downloadStream struct {
	chunks *chunkDownloader
	cipher *rekeyStream

	closed atomic.Bool // set by Close, possibly on a different goroutine than Read

	segments *chunkSegments
	segment  []byte // leftover from the previous Read
	err      error
}

func (d *downloadStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	} else if d.closed.Load() {
		return 0, io.ErrClosedPipe
	} else if d.err != nil {
		return 0, d.err
	}

	var total int
	for len(p) > 0 {
		segment, err := d.nextSegment()
		if err != nil {
			d.err = err
			if total > 0 {
				return total, nil
			}
			return 0, err
		}

		n := min(len(p), len(segment))
		d.cipher.XORKeyStream(p[:n], segment[:n])
		if n < len(segment) {
			d.segment = segment[n:]
		}
		p = p[n:]
		total += n
	}
	return total, nil
}

func (d *downloadStream) WriteTo(w io.Writer) (int64, error) {
	if d.closed.Load() {
		return 0, io.ErrClosedPipe
	} else if errors.Is(d.err, io.EOF) {
		return 0, nil
	} else if d.err != nil {
		return 0, d.err
	}

	// buffer the small per-segment writes
	bw := bufio.NewWriterSize(w, 1<<16)
	var total int64
	for {
		segment, err := d.nextSegment()
		if err != nil {
			// flush decrypted data before reporting the source error
			if ferr := bw.Flush(); ferr != nil {
				return total, ferr
			}
			d.err = err
			if errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}

		// in-place XOR is safe; bufio.Writer copies segment into its buffer
		d.cipher.XORKeyStream(segment, segment)
		// a write error is a destination failure, so leave d.err unset
		n, err := bw.Write(segment)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
}

func (d *downloadStream) nextSegment() ([]byte, error) {
	if len(d.segment) > 0 {
		segment := d.segment
		d.segment = nil
		return segment, nil
	}

	for {
		if d.segments != nil {
			segment, err := d.segments.next()
			if err == nil {
				return segment, nil
			} else if !errors.Is(err, io.EOF) {
				return nil, err
			}
			d.segments = nil
		}

		rc, err := d.chunks.nextChunk()
		if err != nil {
			return nil, err
		}
		d.segments = newChunkSegments(rc)
	}
}

func (d *downloadStream) Close() error {
	if d.closed.Swap(true) {
		return nil // already closed
	}
	d.chunks.close()
	return nil
}

func defaultDownloadOption(maxLength uint64) downloadOption {
	return downloadOption{
		hostTimeout: 60 * time.Second, // long to handle slow hosts, racing will ensure we don't waste time unnecessarily
		maxInflight: 80,               // ~20 MiB in memory
		offset:      0,
		length:      maxLength,
	}
}

// normalizeRange clamps the download range to the object size. Returns
// false if the range is empty (nothing to download).
func (do *downloadOption) normalizeRange(maxLength uint64) bool {
	if do.offset >= maxLength || do.length == 0 {
		return false
	}
	do.length = min(do.length, maxLength-do.offset)
	return true
}

// Close closes the SDK and releases all resources.
func (s *SDK) Close() error {
	s.tg.Stop()
	return s.hosts.Close()
}

// PinObject pins the object's slabs and saves the object metadata to the
// indexer.
func (s *SDK) PinObject(ctx context.Context, obj Object) error {
	params := make([]slabs.SlabPinParams, len(obj.slabs))
	for i, slab := range obj.slabs {
		params[i] = slabs.SlabPinParams{
			EncryptionKey: slab.EncryptionKey,
			MinShards:     slab.MinShards,
			Sectors:       slab.Sectors,
		}
		if err := params[i].Validate(); err != nil {
			return fmt.Errorf("slab %d invalid: %w", i, err)
		}
	}

	for i := 0; i < len(params); i += pinBatchSize {
		end := min(i+pinBatchSize, len(params))

		slabIDs, err := s.app.PinSlabs(ctx, s.appKey, params[i:end]...)
		if err != nil {
			return fmt.Errorf("failed to pin slabs: %w", err)
		}

		for j, slab := range obj.slabs[i:end] {
			if expected := slab.Digest(); slabIDs[j] != expected {
				return fmt.Errorf("slab %d: pinned id %s does not match expected id %s", i+j, slabIDs[j], expected)
			}
		}
	}

	return s.app.PinObject(ctx, s.appKey, obj.Seal(s.appKey).SealedObject)
}

const chunkSize = 1 << 18 // 256 KiB

// chunkIter splits slabs into fixed-size chunks for parallel recovery.
// It handles byte-range selection internally: offset is a byte offset into
// the logical stream of all slabs and length limits total output.
type chunkIter struct {
	slabs     []slabs.SlabSlice
	slabIdx   int
	offset    uint64 // position within current slab
	remaining uint64 // total bytes left to yield
}

func newChunkIter(ss []slabs.SlabSlice, offset, length uint64) *chunkIter {
	ci := &chunkIter{
		slabs:     ss,
		remaining: length,
	}
	for ci.slabIdx < len(ci.slabs) {
		slabLength := uint64(ci.slabs[ci.slabIdx].Length)
		if offset < slabLength {
			break
		}
		offset -= slabLength
		ci.slabIdx++
	}
	ci.offset = offset
	return ci
}

func (ci *chunkIter) next() (slabs.SlabSlice, int, bool) {
	for ci.remaining > 0 && ci.slabIdx < len(ci.slabs) {
		slab := ci.slabs[ci.slabIdx]
		available := uint64(slab.Length) - ci.offset
		if available == 0 {
			ci.slabIdx++
			ci.offset = 0
			continue
		}
		chunkLen := min(available, ci.remaining, chunkSize)
		chunk := slab
		chunk.Offset = slab.Offset + uint32(ci.offset)
		chunk.Length = uint32(chunkLen)
		slabIdx := ci.slabIdx
		ci.offset += chunkLen
		if ci.offset >= uint64(slab.Length) {
			ci.offset = 0
			ci.slabIdx++
		}
		ci.remaining -= chunkLen
		return chunk, slabIdx, true
	}
	return slabs.SlabSlice{}, 0, false
}

type recoveredChunk struct {
	shards   [][]byte
	skip     int
	writeLen int
}

func (s *SDK) recoverChunk(ctx context.Context, chunk slabs.SlabSlice, slabIndex int, hostTimeout time.Duration, onProgress func(ShardProgress)) (recoveredChunk, error) {
	shards, err := s.downloadSlab(ctx, chunk, slabIndex, hostTimeout, onProgress)
	if err != nil {
		return recoveredChunk{}, fmt.Errorf("failed to download slab: %w", err)
	}

	// decrypt shards
	counter := chunk.Offset / (proto4.LeafSize * uint32(chunk.MinShards))
	var nonce [24]byte
	for i, shard := range shards {
		if shard == nil {
			continue
		}
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(chunk.EncryptionKey[:], nonce[:])
		c.SetCounter(counter)
		c.XORKeyStream(shard, shard)
	}

	// reconstruct data shards
	enc, err := reedsolomon.New(int(chunk.MinShards), len(shards)-int(chunk.MinShards))
	if err != nil {
		return recoveredChunk{}, fmt.Errorf("failed to create reedsolomon coder: %w", err)
	}
	if err := enc.ReconstructData(shards); err != nil {
		return recoveredChunk{}, fmt.Errorf("failed to reconstruct data shards: %w", err)
	}

	return recoveredChunk{
		shards:   shards[:int(chunk.MinShards)],
		skip:     int(chunk.Offset) % (proto4.LeafSize * int(chunk.MinShards)),
		writeLen: int(chunk.Length),
	}, nil
}

// chunkWork is a chunk handed to a worker for recovery.
type chunkWork struct {
	index     int
	slabIndex int
	chunk     slabs.SlabSlice
}

// chunkResult is the outcome of recovering a chunkWork, tagged with its index
// so out-of-order results can be reordered into the original chunk sequence.
type chunkResult struct {
	index int
	recoveredChunk
	err error
}

// chunkDownloader recovers chunks concurrently and yields them in order via
// nextChunk. The first chunk is recovered synchronously for fast TTFB; the
// worker pool is started lazily once a subsequent chunk is requested.
type chunkDownloader struct {
	s           *SDK
	ctx         context.Context
	cancel      context.CancelFunc
	chunks      *chunkIter
	maxInflight int
	hostTimeout time.Duration
	onProgress  func(ShardProgress)

	workCh    chan chunkWork
	resultCh  chan chunkResult
	completed map[int]chunkResult
	next      int
	err       error

	started atomic.Bool   // set by start, read by close on a different goroutine
	done    chan struct{} // closed by the worker pool on exit
}

func newChunkDownloader(ctx context.Context, cancel context.CancelFunc, s *SDK, maxInflight int, hostTimeout time.Duration, ss []slabs.SlabSlice, offset, length uint64, onProgress func(ShardProgress)) *chunkDownloader {
	return &chunkDownloader{
		s:           s,
		ctx:         ctx,
		cancel:      cancel,
		chunks:      newChunkIter(ss, offset, length),
		maxInflight: maxInflight,
		hostTimeout: hostTimeout,
		onProgress:  onProgress,
		completed:   make(map[int]chunkResult),
		done:        make(chan struct{}),
	}
}

func (d *chunkDownloader) start() {
	d.started.Store(true)
	d.workCh = make(chan chunkWork, d.maxInflight)
	d.resultCh = make(chan chunkResult, d.maxInflight)

	var wg sync.WaitGroup
	for range d.maxInflight {
		wg.Go(func() {
			for work := range d.workCh {
				rc, err := d.s.recoverChunk(d.ctx, work.chunk, work.slabIndex, d.hostTimeout, d.onProgress)
				select {
				case d.resultCh <- chunkResult{index: work.index, recoveredChunk: rc, err: err}:
				case <-d.ctx.Done():
					return
				}
			}
		})
	}

	go func() {
		defer close(d.done)
		defer func() {
			close(d.workCh)
			wg.Wait()
			close(d.resultCh)
		}()
		for chunkIdx := 0; ; chunkIdx++ {
			chunk, slabIdx, ok := d.chunks.next()
			if !ok {
				return
			}
			select {
			case d.workCh <- chunkWork{index: chunkIdx, slabIndex: slabIdx, chunk: chunk}:
			case <-d.ctx.Done():
				return
			}
		}
	}()
}

func (d *chunkDownloader) nextChunk() (recoveredChunk, error) {
	if d.err != nil {
		return recoveredChunk{}, d.err
	} else if d.maxInflight <= 0 {
		d.err = errors.New("download inflight must be greater than 0")
		return recoveredChunk{}, d.err
	}

	if !d.started.Load() {
		chunk, slabIdx, ok := d.chunks.next()
		if !ok {
			d.err = io.EOF
			d.cancel()
			return recoveredChunk{}, io.EOF
		}
		rc, err := d.s.recoverChunk(d.ctx, chunk, slabIdx, d.hostTimeout, d.onProgress)
		if err != nil {
			d.err = err
			d.close()
			return recoveredChunk{}, err
		}
		d.start()
		return rc, nil
	}

	if res, ok := d.completed[d.next]; ok {
		delete(d.completed, d.next)
		d.next++
		return res.recoveredChunk, nil
	}
	for res := range d.resultCh {
		if res.err != nil {
			d.err = res.err
			d.close()
			return recoveredChunk{}, res.err
		}
		if res.index == d.next {
			d.next++
			return res.recoveredChunk, nil
		}
		d.completed[res.index] = res
	}

	if err := d.ctx.Err(); err != nil {
		d.err = err
		return recoveredChunk{}, err
	} else if len(d.completed) > 0 {
		panic(fmt.Sprintf("%d chunks remaining but no tasks in flight", len(d.completed)))
	}
	d.err = io.EOF
	d.cancel()
	return recoveredChunk{}, io.EOF
}

func (d *chunkDownloader) close() {
	d.cancel()
	if d.started.Load() {
		<-d.done
	}
}

// chunkSegments iterates over a recovered chunk, yielding data from its data
// shards one leaf-sized segment at a time. The first 'skip' bytes of the
// recovered data are skipped, and at most 'remaining' bytes are yielded in
// total.
type chunkSegments struct {
	shards    [][]byte
	skip      int
	remaining int
	off       int // byte offset into the current stripe
	shard     int // index of the next shard within the current stripe
}

func newChunkSegments(rc recoveredChunk) *chunkSegments {
	return &chunkSegments{
		shards:    rc.shards,
		skip:      rc.skip,
		remaining: rc.writeLen,
	}
}

func (cs *chunkSegments) next() ([]byte, error) {
	for cs.remaining > 0 {
		if len(cs.shards) == 0 {
			return nil, reedsolomon.ErrShortData
		} else if cs.shard >= len(cs.shards) {
			cs.shard = 0
			cs.off += proto4.LeafSize
		}

		shard := cs.shards[cs.shard]
		cs.shard++
		if cs.off > len(shard) || len(shard)-cs.off < proto4.LeafSize {
			return nil, reedsolomon.ErrShortData
		}
		segment := shard[cs.off:][:proto4.LeafSize]
		if cs.skip >= len(segment) {
			cs.skip -= len(segment)
			continue
		} else if cs.skip > 0 {
			segment = segment[cs.skip:]
			cs.skip = 0
		}
		if cs.remaining < len(segment) {
			segment = segment[:cs.remaining]
		}
		cs.remaining -= len(segment)
		return segment, nil
	}
	return nil, io.EOF
}

func (s *SDK) fetchHosts(ctx context.Context) (all []hosts.HostInfo, _ error) {
	const batchSize = 100

	var exhausted bool
	for offset := 0; !exhausted; offset += batchSize {
		batch, err := s.app.Hosts(ctx, s.appKey, api.WithOffset(offset), api.WithLimit(batchSize))
		if err != nil {
			return nil, fmt.Errorf("failed to fetch hosts from indexer: %w", err)
		} else if len(batch) < batchSize {
			exhausted = true
		}
		all = append(all, batch...)
	}

	return all, nil
}

func (s *SDK) refreshHosts(ctx context.Context, forceWarmup bool) error {
	// fetch all hosts
	allHosts, err := s.fetchHosts(ctx)
	if err != nil {
		return err
	}

	// count GFU hosts for logging
	var gfuCount int
	for _, host := range allHosts {
		if host.GoodForUpload {
			gfuCount++
		}
	}

	// update the hosts cache
	added := s.hostsCache.updateHosts(allHosts)
	s.log.Debug("hosts refreshed", zap.Int("hosts", len(allHosts)), zap.Int("new", len(added)), zap.Int("goodForUpload", gfuCount))

	// warmup all hosts if force is set
	if forceWarmup {
		hks := make([]types.PublicKey, len(allHosts))
		for i, host := range allHosts {
			hks[i] = host.PublicKey
		}
		return s.warmConnections(ctx, hks)
	}

	// otherwise warm up newly added GFU hosts if there are any
	if len(added) > 0 {
		gfu := make([]types.PublicKey, 0, len(added))
		for _, host := range added {
			if host.GoodForUpload {
				gfu = append(gfu, host.PublicKey)
			}
		}
		if len(gfu) > 0 {
			return s.warmConnections(ctx, gfu)
		}
	}

	return nil
}

func (s *SDK) warmConnections(ctx context.Context, hks []types.PublicKey) error {
	var warmed atomic.Uint64

	var wg sync.WaitGroup
	sema := make(chan struct{}, 15)
	for _, hk := range hks {
		select {
		case <-ctx.Done():
			return nil
		case sema <- struct{}{}:
		}

		tCtx, tCancel, err := s.tg.AddContext(ctx)
		if err != nil {
			return err
		}

		wg.Add(1)
		go func(ctx context.Context, hk types.PublicKey) {
			defer func() {
				tCancel()
				wg.Done()
				<-sema
			}()
			pCtx, pCancel := context.WithTimeout(ctx, time.Second)
			_, err := s.hosts.Prices(pCtx, hk)
			pCancel()

			if err == nil {
				warmed.Add(1)
			}
		}(tCtx, hk)
	}

	// wait for all warmups to complete
	wg.Wait()

	s.log.Debug("warmed up hosts", zap.Uint64("n", warmed.Load()))
	return nil
}

// sectorRegion returns the offset and length of the sector region that must be
// downloaded in order to recover the data referenced by the slice.
func sectorRegion(ss slabs.SlabSlice) (offset, length uint64) {
	minChunkSize := proto4.LeafSize * uint32(ss.MinShards)
	start := (ss.Offset / minChunkSize) * proto4.LeafSize
	end := ((ss.Offset + ss.Length) / minChunkSize) * proto4.LeafSize
	if (ss.Offset+ss.Length)%minChunkSize != 0 {
		end += proto4.LeafSize
	}
	return uint64(start), uint64(end - start)
}

// writeSector uploads a single sector to a host with the given timeout.
func writeSector(ctx context.Context, client hostClient, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte, timeout time.Duration) (types.Hash256, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := client.WriteSector(ctx, accountKey, hostKey, data)
	return result.Root, err
}

// WithRedundancy sets the number of data and parity shards for the upload.
// The number of shards must be at least 2x redundancy:
// `(dataShards + parityShards) / dataShards >= 2`.
func WithRedundancy(dataShards, parityShards uint8) UploadOption {
	return func(uo *uploadOption) {
		uo.dataShards = dataShards
		uo.parityShards = parityShards
	}
}

// WithUploadInflight sets the maximum number of concurrent shard uploads.
// This is useful to reduce bandwidth consumption, but will decrease
// performance.
func WithUploadInflight(maxInflight int) UploadOption {
	return func(uo *uploadOption) {
		uo.maxInflight = maxInflight
	}
}

// WithUploadProgress sets a callback that is invoked for each shard that
// completes uploading successfully. Callers should keep the callback short or
// hand off work to a goroutine. The callback may be called concurrently.
func WithUploadProgress(fn func(ShardProgress)) UploadOption {
	return func(uo *uploadOption) {
		uo.onProgress = fn
	}
}

// WithDownloadHostTimeout sets the timeout for reading sectors
// from individual hosts. The default is 60 seconds.
func WithDownloadHostTimeout(timeout time.Duration) DownloadOption {
	return func(do *downloadOption) {
		do.hostTimeout = timeout
	}
}

// WithDownloadInflight sets the maximum number of concurrent chunk
// downloads. The default is 80.
func WithDownloadInflight(maxInflight int) DownloadOption {
	return func(do *downloadOption) {
		do.maxInflight = maxInflight
	}
}

// WithDownloadProgress sets a callback that is invoked for shard downloads
// that complete successfully before the chunk download finishes, i.e. for up
// to MinShards successful shard downloads per chunk. Callers should keep the
// callback short or hand off work to a goroutine. The callback may be called
// concurrently.
func WithDownloadProgress(fn func(ShardProgress)) DownloadOption {
	return func(do *downloadOption) {
		do.onProgress = fn
	}
}

// WithDownloadRange sets the byte range to download from the object. The range
// is clamped to the object size: if offset+length exceeds the object size, only
// the available bytes are returned. If offset is at or beyond the end, or
// length is zero, the returned reader yields no data.
func WithDownloadRange(offset, length uint64) DownloadOption {
	return func(do *downloadOption) {
		do.offset = offset
		do.length = length
	}
}

// An Option configures the SDK.
type Option func(*SDK)

// WithLogger sets the logger for the SDK. The default behavior is to not log
// anything.
func WithLogger(log *zap.Logger) Option {
	return func(s *SDK) {
		s.log = log
	}
}

func initSDK(appKey types.PrivateKey, app appClient, opts ...Option) (*SDK, error) {
	sdk := &SDK{
		appKey:     appKey,
		app:        app,
		hostsCache: newHostCache(),

		tg:  threadgroup.New(),
		log: zap.NewNop(), // no logging by default
	}
	for _, opt := range opts {
		opt(sdk)
	}

	// create the host client
	sdk.hosts = client.New(client.NewProvider(sdk.hostsCache), sdk.log.Named("client"))

	// update hosts and warm connections on init
	err := sdk.refreshHosts(context.Background(), true)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh hosts: %w", err)
	}

	// refresh hosts every 10 minutes in the background
	ctx, cancel, err := sdk.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()

		t := time.NewTicker(10 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}

			err := sdk.refreshHosts(ctx, false)
			if err != nil {
				sdk.log.Warn("failed to refresh hosts", zap.Error(err))
			}
		}
	}()

	return sdk, nil
}
