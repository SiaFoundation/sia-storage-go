package siastorage

/*
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// uploadBufferSize is the size of the staging buffer used to push data across
// the FFI boundary. Larger buffers amortize the cost of each crossing.
const uploadBufferSize = 1 << 20 // 1 MiB

// downloadBufferSize is the size of the staging buffer used when streaming a
// download to an io.Writer via WriteTo.
const downloadBufferSize = 1 << 20 // 1 MiB

type (
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

	uploadOption struct {
		dataShards       uint8
		parityShards     uint8
		setRedundancy    bool
		maxBufferedSlabs int
		onProgress       func(ShardProgress)
	}

	downloadOption struct {
		maxBufferedChunks int
		offset            uint64
		length            uint64
		onProgress        func(ShardProgress)
	}

	// sdkResource owns the Rust SDK handle so it can be freed either by
	// Close or, as a fallback, when the SDK becomes unreachable.
	sdkResource struct {
		ptr *C.sia_sdk_t
	}

	// An SDK is a client for the indexd service.
	SDK struct {
		mu  sync.RWMutex
		res *sdkResource
	}

	// App contains metadata about the application registered on the
	// indexer.
	App struct {
		ID          types.Hash256 `json:"id"`
		Name        string        `json:"name"`
		Description string        `json:"description"`
		LogoURL     string        `json:"logoUrl"`    //nolint:tagliatelle // must match the Rust crate's serde casing
		ServiceURL  string        `json:"serviceUrl"` //nolint:tagliatelle // must match the Rust crate's serde casing
	}

	// An Account describes the state of the user's account on the indexer.
	Account struct {
		AccountKey       types.PublicKey `json:"accountKey"`
		MaxPinnedData    uint64          `json:"maxPinnedData"`
		RemainingStorage uint64          `json:"remainingStorage"`
		PinnedData       uint64          `json:"pinnedData"`
		PinnedSize       uint64          `json:"pinnedSize"`
		Ready            bool            `json:"ready"`
		App              App             `json:"app"`
		LastUsed         time.Time       `json:"lastUsed"`
	}
)

var (
	// ErrNotEnoughShards is returned when not enough shards were
	// uploaded or downloaded to satisfy the minimum required shards.
	ErrNotEnoughShards = errors.New("not enough shards")

	// ErrNoMoreHosts is returned when there are no more hosts
	// available to attempt to upload a shard
	ErrNoMoreHosts = errors.New("no more hosts available")

	// ErrSDKClosed is returned when the SDK is used after Close.
	ErrSDKClosed = errors.New("sdk closed")
)

func newSDK(ptr *C.sia_sdk_t) *SDK {
	res := &sdkResource{ptr: ptr}
	s := &SDK{res: res}
	runtime.AddCleanup(s, func(res *sdkResource) {
		if res.ptr != nil {
			C.sia_sdk_free(res.ptr)
		}
	}, res)
	return s
}

// acquire returns the SDK handle, holding a read lock that prevents Close
// from freeing it until unlock is called.
func (s *SDK) acquire() (*C.sia_sdk_t, func(), error) {
	s.mu.RLock()
	if s.res.ptr == nil {
		s.mu.RUnlock()
		return nil, nil, ErrSDKClosed
	}
	return s.res.ptr, s.mu.RUnlock, nil
}

func (uo *uploadOption) cOptions() (C.sia_upload_options_t, uintptr) {
	id := registerProgress(uo.onProgress)
	opts := C.sia_upload_options_t{
		data_shards:        C.uint8_t(uo.dataShards),
		parity_shards:      C.uint8_t(uo.parityShards),
		set_redundancy:     C.bool(uo.setRedundancy),
		max_buffered_slabs: C.uint64_t(uo.maxBufferedSlabs),
		userdata:           C.uintptr_t(id),
	}
	if id != 0 {
		opts.on_shard = C.sia_go_progress_cb()
	}
	return opts, id
}

func (do *downloadOption) cOptions() (C.sia_download_options_t, uintptr) {
	id := registerProgress(do.onProgress)
	opts := C.sia_download_options_t{
		offset:              C.uint64_t(do.offset),
		has_length:          C.bool(true),
		length:              C.uint64_t(do.length),
		max_buffered_chunks: C.uint64_t(do.maxBufferedChunks),
		userdata:            C.uintptr_t(id),
	}
	if id != 0 {
		opts.on_shard = C.sia_go_progress_cb()
	}
	return opts, id
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

// AppKey returns the app key used by the SDK.
//
// It should be kept secret. Applications
// should store it securely to authenticate with
// the indexer.
func (s *SDK) AppKey() types.PrivateKey {
	ptr, unlock, err := s.acquire()
	if err != nil {
		return nil
	}
	defer unlock()

	var seed [32]byte
	C.sia_sdk_app_key(ptr, cBytes32(&seed))
	return types.NewPrivateKeyFromSeed(seed[:])
}

// Account retrieves account information for the current app key.
func (s *SDK) Account(ctx context.Context) (Account, error) {
	ptr, unlock, err := s.acquire()
	if err != nil {
		return Account{}, err
	}
	defer unlock()

	tok, release := cancelToken(ctx)
	defer release()

	var outJSON, cerr *C.char
	code := C.sia_sdk_account(ptr, tok, &outJSON, &cerr)
	if err := goError(ctx, code, cerr); err != nil {
		return Account{}, fmt.Errorf("failed to get account: %w", err)
	}
	var account Account
	if err := json.Unmarshal([]byte(goString(outJSON)), &account); err != nil {
		return Account{}, fmt.Errorf("failed to decode account: %w", err)
	}
	return account, nil
}

// PruneSlabs removes all slabs on the account that are not associated with
// an object.
func (s *SDK) PruneSlabs(ctx context.Context) error {
	ptr, unlock, err := s.acquire()
	if err != nil {
		return err
	}
	defer unlock()

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_prune_slabs(ptr, tok, &cerr)
	return goError(ctx, code, cerr)
}

// DeleteObject deletes the object with the given key from the indexer.
func (s *SDK) DeleteObject(ctx context.Context, key types.Hash256) error {
	ptr, unlock, err := s.acquire()
	if err != nil {
		return err
	}
	defer unlock()

	tok, release := cancelToken(ctx)
	defer release()

	id := [32]byte(key)
	var cerr *C.char
	code := C.sia_sdk_delete_object(ptr, cBytes32(&id), tok, &cerr)
	return goError(ctx, code, cerr)
}

// PinObject pins the object's slabs and saves the object metadata to the
// indexer.
func (s *SDK) PinObject(ctx context.Context, obj Object) error {
	if obj.h == nil {
		return errNilObject
	}
	ptr, unlock, err := s.acquire()
	if err != nil {
		return err
	}
	defer unlock()

	tok, release := cancelToken(ctx)
	defer release()

	var cerr *C.char
	code := C.sia_sdk_pin_object(ptr, obj.h.ptr, tok, &cerr)
	runtime.KeepAlive(obj.h)
	if err := goError(ctx, code, cerr); err != nil {
		return fmt.Errorf("failed to pin object: %w", err)
	}
	return nil
}

// Upload uploads the data to hosts.
//
// Appends the metadata of the slabs that were uploaded to the given object.
// After uploading the object, the caller must call PinObject to pin the
// slabs and save the object metadata to the indexer.
func (s *SDK) Upload(ctx context.Context, obj *Object, r io.Reader, opts ...UploadOption) error {
	if obj.h == nil {
		return errNilObject
	}
	var uo uploadOption
	for _, opt := range opts {
		opt(&uo)
	}

	ptr, unlock, err := s.acquire()
	if err != nil {
		return err
	}

	copts, pid := uo.cOptions()
	defer unregisterProgress(pid)

	var up *C.sia_upload_t
	var cerr *C.char
	code := C.sia_upload_start(ptr, obj.h.ptr, &copts, &up, &cerr)
	unlock() // the upload task holds its own reference to the Rust SDK
	runtime.KeepAlive(obj.h)
	if err := goError(ctx, code, cerr); err != nil {
		return err
	}
	return uploadStream(ctx, up, obj, r)
}

// uploadStream pushes r into the upload handle until EOF, then finishes the
// upload and replaces obj's handle with the uploaded object. It always frees
// the upload handle; freeing before completion aborts the upload.
func uploadStream(ctx context.Context, up *C.sia_upload_t, obj *Object, r io.Reader) error {
	defer C.sia_upload_free(up)

	tok, release := cancelToken(ctx)
	defer release()

	buf := make([]byte, uploadBufferSize)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			var cerr *C.char
			code := C.sia_upload_write(up, (*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(n), tok, &cerr)
			if err := goError(ctx, code, cerr); err != nil {
				return err
			}
		}
		if errors.Is(rerr, io.EOF) {
			break
		} else if rerr != nil {
			return rerr
		}
	}

	var outObj *C.sia_object_t
	var cerr *C.char
	code := C.sia_upload_finish(up, tok, &outObj, &cerr)
	if err := goError(ctx, code, cerr); err != nil {
		return err
	}
	obj.h = newObjectHandle(outObj)
	return nil
}

// downloadStream streams object data across the FFI boundary. Read blocks
// until data is available; Close cancels the underlying download.
type downloadStream struct {
	mu     sync.Mutex
	closed bool
	once   sync.Once

	ptr *C.sia_download_t
	tok *C.sia_cancel_t
	pid uintptr
}

func (d *downloadStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0, io.ErrClosedPipe
	}
	var n C.size_t
	var cerr *C.char
	code := C.sia_download_read(d.ptr, (*C.uint8_t)(unsafe.Pointer(&p[0])), C.size_t(len(p)), d.tok, &n, &cerr)
	if err := goError(nil, code, cerr); err != nil {
		if errors.Is(err, errCancelled) {
			// only Close cancels the stream's token
			return int(n), io.ErrClosedPipe
		}
		return int(n), err
	}
	if n == 0 {
		return 0, io.EOF
	}
	return int(n), nil
}

// WriteTo streams the download into w using a large staging buffer, keeping
// the number of FFI crossings low when used with io.Copy.
func (d *downloadStream) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, downloadBufferSize)
	var total int64
	for {
		n, err := d.Read(buf)
		if n > 0 {
			wn, werr := w.Write(buf[:n])
			total += int64(wn)
			if werr != nil {
				return total, werr
			}
		}
		if errors.Is(err, io.EOF) {
			return total, nil
		} else if err != nil {
			return total, err
		}
	}
}

func (d *downloadStream) Close() error {
	d.once.Do(func() {
		// unblock any in-flight read before taking the lock
		C.sia_cancel_cancel(d.tok)
		d.mu.Lock()
		defer d.mu.Unlock()
		d.closed = true
		C.sia_download_free(d.ptr)
		C.sia_cancel_free(d.tok)
		unregisterProgress(d.pid)
	})
	return nil
}

// startDownload wraps a started FFI download handle into an io.ReadCloser.
func startDownload(dl *C.sia_download_t, pid uintptr) io.ReadCloser {
	tok, _, _ := newCancelToken()
	return &downloadStream{ptr: dl, tok: tok, pid: pid}
}

// downloadObject starts a download for the object referenced by h.
func (s *SDK) downloadObject(h *objectHandle, size uint64, opts ...DownloadOption) (io.ReadCloser, error) {
	do := downloadOption{length: size}
	for _, opt := range opts {
		opt(&do)
	}
	if !do.normalizeRange(size) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	ptr, unlock, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer unlock()

	copts, pid := do.cOptions()
	var dl *C.sia_download_t
	var cerr *C.char
	code := C.sia_download_start(ptr, h.ptr, &copts, &dl, &cerr)
	runtime.KeepAlive(h)
	if err := goError(nil, code, cerr); err != nil {
		unregisterProgress(pid)
		return nil, err
	}
	return startDownload(dl, pid), nil
}

// Download returns an [io.ReadCloser] streaming the object's data. Closing the
// reader cancels the underlying download. Callers must always Close the
// returned reader to release resources.
func (s *SDK) Download(obj Object, opts ...DownloadOption) (io.ReadCloser, error) {
	if obj.h == nil {
		return nil, errNilObject
	}
	return s.downloadObject(obj.h, obj.Size(), opts...)
}

// DownloadSharedObject returns an [io.ReadCloser] streaming a shared object's
// data. Closing the reader cancels the underlying download. Callers must always
// Close the returned reader to release resources.
func (s *SDK) DownloadSharedObject(ctx context.Context, sharedURL string, opts ...DownloadOption) (io.ReadCloser, error) {
	ptr, unlock, err := s.acquire()
	if err != nil {
		return nil, err
	}

	tok, release := cancelToken(ctx)

	curl := C.CString(sharedURL)
	var objPtr *C.sia_object_t
	var cerr *C.char
	code := C.sia_sdk_shared_object(ptr, curl, tok, &objPtr, &cerr)
	C.free(unsafe.Pointer(curl))
	release()
	unlock()
	if err := goError(ctx, code, cerr); err != nil {
		return nil, fmt.Errorf("failed to get shared object: %w", err)
	}

	obj := newObject(objPtr)
	return s.downloadObject(obj.h, obj.Size(), opts...)
}

// Close closes the SDK and releases all resources.
func (s *SDK) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.res.ptr != nil {
		C.sia_sdk_free(s.res.ptr)
		s.res.ptr = nil
	}
	return nil
}

// WithRedundancy sets the number of data and parity shards for the upload.
// The number of shards must be at least 2x redundancy:
// `(dataShards + parityShards) / dataShards >= 2`.
func WithRedundancy(dataShards, parityShards uint8) UploadOption {
	return func(uo *uploadOption) {
		uo.dataShards = dataShards
		uo.parityShards = parityShards
		uo.setRedundancy = true
	}
}

// WithMaxBufferedSlabs limits the number of slabs held in memory during an
// upload. Lower values reduce memory usage at the cost of parallelism. The
// default is 10% of system memory.
func WithMaxBufferedSlabs(n int) UploadOption {
	return func(uo *uploadOption) {
		uo.maxBufferedSlabs = n
	}
}

// WithUploadProgress sets a callback that is invoked for each shard that
// completes uploading successfully. Callers should keep the callback short or
// hand off work to a goroutine. The callback may be called concurrently and
// must not call back into the SDK.
func WithUploadProgress(fn func(ShardProgress)) UploadOption {
	return func(uo *uploadOption) {
		uo.onProgress = fn
	}
}

// WithMaxBufferedChunks limits the number of chunks held in memory during a
// download. Each chunk is around 1 MiB. Lower values reduce memory usage at
// the cost of parallelism. The default is 10% of system memory.
func WithMaxBufferedChunks(n int) DownloadOption {
	return func(do *downloadOption) {
		do.maxBufferedChunks = n
	}
}

// WithDownloadProgress sets a callback that is invoked for shard downloads
// that complete successfully. Callers should keep the callback short or
// hand off work to a goroutine. The callback may be called concurrently and
// must not call back into the SDK.
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

// WithLogger routes the SDK's log output to the given zap logger. The
// underlying logger is process-wide: the most recently configured logger
// receives the log output of every SDK instance.
func WithLogger(log *zap.Logger) Option {
	return func(*SDK) {
		setGlobalLogger(log)
	}
}
