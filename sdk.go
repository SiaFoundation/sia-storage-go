package siastorage

import (
	"context"
	"io"
	"time"

	"go.sia.tech/core/types"
	ffi "go.sia.tech/siastorage/sia_storage_ffi"
	"go.uber.org/zap"
)

// The SDK interacts with the Sia network through an indexer. Instances are
// created with a [Builder].
type SDK struct {
	inner *ffi.Sdk
	log   *zap.Logger
}

func (s *SDK) applyLogger() {
	if s.log != nil {
		setGlobalLogger(s.log)
	}
}

// AppKey returns the app key used by the SDK.
//
// It should be kept secret. Applications should store it securely to
// authenticate with the indexer.
func (s *SDK) AppKey() types.PrivateKey {
	return privateFromAppKey(s.inner.AppKey())
}

// Account retrieves account information for the current app key.
//
// The native SDK returned app.AccountResponse; this returns the equivalent
// FFI [Account] type.
func (s *SDK) Account(ctx context.Context) (Account, error) {
	return runContext(ctx, s.inner.Account)
}

// PruneSlabs removes all slabs on the account that are not associated with an
// object.
func (s *SDK) PruneSlabs(ctx context.Context) error {
	_, err := runContext(ctx, func() (struct{}, error) {
		return struct{}{}, s.inner.PruneSlabs()
	})
	return err
}

// DeleteObject deletes the object with the given key from the indexer.
func (s *SDK) DeleteObject(ctx context.Context, key types.Hash256) error {
	_, err := runContext(ctx, func() (struct{}, error) {
		return struct{}{}, s.inner.DeleteObject(hashToString(key))
	})
	return err
}

// Upload uploads the data to hosts.
//
// It appends the metadata of the slabs that were uploaded to the given
// object. After uploading, the caller must call [SDK.PinObject] to pin the
// slabs and save the object metadata to the indexer.
func (s *SDK) Upload(ctx context.Context, obj *Object, r io.Reader, opts ...UploadOption) error {
	inner, err := runContext(ctx, func() (*ffi.PinnedObject, error) {
		return s.inner.Upload(obj.inner, newFFIReader(r), uploadOptions(opts))
	})
	if err != nil {
		return err
	}
	obj.inner = inner
	return nil
}

// Download returns an [io.ReadCloser] streaming the object's data. Closing the
// reader cancels the underlying download. Callers must always Close the
// returned reader to release resources.
func (s *SDK) Download(obj Object, opts ...DownloadOption) (io.ReadCloser, error) {
	dl, err := s.inner.Download(obj.inner, downloadOptions(opts))
	if err != nil {
		return nil, err
	}
	return &downloadReader{dl: dl}, nil
}

// DownloadSharedObject returns an [io.ReadCloser] streaming a shared object's
// data. Closing the reader cancels the underlying download. Callers must
// always Close the returned reader to release resources.
func (s *SDK) DownloadSharedObject(ctx context.Context, sharedURL string, opts ...DownloadOption) (io.ReadCloser, error) {
	obj, err := runContext(ctx, func() (*ffi.PinnedObject, error) {
		return s.inner.SharedObject(sharedURL)
	})
	if err != nil {
		return nil, err
	}
	dl, err := s.inner.Download(obj, downloadOptions(opts))
	if err != nil {
		return nil, err
	}
	return &downloadReader{dl: dl}, nil
}

// PinObject pins the object's slabs and saves the object metadata to the
// indexer.
func (s *SDK) PinObject(ctx context.Context, obj Object) error {
	_, err := runContext(ctx, func() (struct{}, error) {
		return struct{}{}, s.inner.PinObject(obj.inner)
	})
	return err
}

// Object retrieves the object with the given key.
func (s *SDK) Object(ctx context.Context, objectKey types.Hash256) (Object, error) {
	inner, err := runContext(ctx, func() (*ffi.PinnedObject, error) {
		return s.inner.Object(hashToString(objectKey))
	})
	if err != nil {
		return Object{}, err
	}
	return Object{inner: inner}, nil
}

// ObjectEvents returns object events from the indexer, starting from the given
// cursor, up to the given limit. The zero-value cursor requests the first
// page.
func (s *SDK) ObjectEvents(ctx context.Context, cursor Cursor, limit int) ([]ObjectEvent, error) {
	var ffiCursor *ffi.ObjectsCursor
	if cursor.Id != "" || !cursor.After.IsZero() {
		ffiCursor = &cursor
	}
	raw, err := runContext(ctx, func() ([]ffi.ObjectEvent, error) {
		return s.inner.ObjectEvents(ffiCursor, uint32(limit))
	})
	if err != nil {
		return nil, err
	}
	events := make([]ObjectEvent, len(raw))
	for i, ev := range raw {
		key, err := parseHash(ev.Id)
		if err != nil {
			return nil, err
		}
		events[i] = ObjectEvent{
			Key:       key,
			Deleted:   ev.Deleted,
			UpdatedAt: ev.UpdatedAt,
		}
		if ev.Object != nil && *ev.Object != nil {
			events[i].Object = &Object{inner: *ev.Object}
		}
	}
	return events, nil
}

// CreateSharedObjectURL creates a URL that can be used to share the object
// until the given time. The URL contains the encryption key required to
// decrypt the object's data and metadata.
//
// Sharing the URL allows anyone with the URL to read the object's data and
// metadata. They will not be able to modify the object or access any other
// objects in the account.
func (s *SDK) CreateSharedObjectURL(ctx context.Context, objectKey types.Hash256, validUntil time.Time) (string, error) {
	return runContext(ctx, func() (string, error) {
		obj, err := s.inner.Object(hashToString(objectKey))
		if err != nil {
			return "", err
		}
		return s.inner.ShareObject(obj, validUntil)
	})
}

// Slab retrieves metadata about a slab stored in the indexer.
func (s *SDK) Slab(ctx context.Context, slabID types.Hash256) (PinnedSlab, error) {
	return runContext(ctx, func() (PinnedSlab, error) {
		return s.inner.Slab(hashToString(slabID))
	})
}

// Hosts returns a list of all usable hosts.
func (s *SDK) Hosts(ctx context.Context) ([]Host, error) {
	return runContext(ctx, s.inner.Hosts)
}

// Close closes the SDK and releases all resources.
func (s *SDK) Close() error {
	s.inner.Destroy()
	return nil
}

// --- options ---

// An Option configures the SDK.
type Option func(*SDK)

// WithLogger sets the logger for the SDK. The default behavior is to not log
// anything.
//
// Logging in the underlying Rust library is process-global, so the most
// recently configured logger applies to all SDK instances.
func WithLogger(log *zap.Logger) Option {
	return func(s *SDK) {
		s.log = log
	}
}

// An UploadOption configures the upload behavior.
type UploadOption func(*ffi.UploadOptions)

// A DownloadOption configures the download behavior.
type DownloadOption func(*ffi.DownloadOptions)

func uploadOptions(opts []UploadOption) ffi.UploadOptions {
	var o ffi.UploadOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func downloadOptions(opts []DownloadOption) ffi.DownloadOptions {
	var o ffi.DownloadOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithRedundancy sets the number of data and parity shards for the upload.
func WithRedundancy(dataShards, parityShards uint8) UploadOption {
	return func(o *ffi.UploadOptions) {
		o.DataShards = &dataShards
		o.ParityShards = &parityShards
	}
}

// WithUploadBufferedSlabs sets the maximum number of slabs the SDK holds in
// memory during an upload. It is a memory/parallelism budget in units of
// slabs, not a shard-concurrency count: the SDK derives its shard-upload
// concurrency ceiling from it (roughly maxBufferedSlabs × total shards) and
// adaptively ramps up to that ceiling. Higher values may increase throughput
// at the cost of memory.
//
// When unset, the SDK defaults to roughly 10% of system memory divided by the
// slab size.
func WithUploadBufferedSlabs(maxBufferedSlabs int) UploadOption {
	return func(o *ffi.UploadOptions) {
		n := uint32(maxBufferedSlabs)
		o.MaxBufferedSlabs = &n
	}
}

// WithUploadProgress sets a callback that is invoked for each shard that
// completes uploading successfully. Callers should keep the callback short or
// hand off work to a goroutine. The callback may be called concurrently.
func WithUploadProgress(fn func(ShardProgress)) UploadOption {
	return func(o *ffi.UploadOptions) {
		o.ShardUploaded = newProgressCallback(fn)
	}
}

// WithDownloadHostTimeout sets the timeout for reading sectors from
// individual hosts.
//
// The FFI download backend manages host timeouts internally and does not
// currently expose this setting, so this option is accepted for
// compatibility but has no effect.
func WithDownloadHostTimeout(_ time.Duration) DownloadOption {
	return func(*ffi.DownloadOptions) {}
}

// WithDownloadBufferedChunks sets the maximum number of ~1 MiB chunks the SDK
// holds in memory during a download. It bounds concurrent chunk downloads: the
// SDK adaptively ramps up to this ceiling. Higher values may increase
// throughput at the cost of memory.
//
// When unset, the SDK defaults to roughly 10% of system memory divided by the
// chunk size.
func WithDownloadBufferedChunks(maxBufferedChunks int) DownloadOption {
	return func(o *ffi.DownloadOptions) {
		n := uint32(maxBufferedChunks)
		o.MaxBufferedChunks = &n
	}
}

// WithDownloadProgress sets a callback that is invoked for shard downloads
// that complete successfully. Callers should keep the callback short or hand
// off work to a goroutine. The callback may be called concurrently.
func WithDownloadProgress(fn func(ShardProgress)) DownloadOption {
	return func(o *ffi.DownloadOptions) {
		o.ShardDownloaded = newProgressCallback(fn)
	}
}

// WithDownloadRange sets the byte range to download from the object. The range
// is clamped to the object size.
func WithDownloadRange(offset, length uint64) DownloadOption {
	return func(o *ffi.DownloadOptions) {
		o.Offset = &offset
		o.Length = &length
	}
}

// progressCallback adapts a func(ShardProgress) to the FFI ProgressCallback
// interface, converting the FFI ShardProgress into the Go type.
type progressCallback func(ShardProgress)

func (fn progressCallback) Progress(p ffi.ShardProgress) {
	hostKey, _ := parsePublicKey(p.HostKey)
	fn(ShardProgress{
		HostKey:    hostKey,
		SlabIndex:  int(p.SlabIndex),
		ShardIndex: int(p.ShardIndex),
		ShardSize:  p.ShardSize,
		Elapsed:    time.Duration(p.ElapsedMs) * time.Millisecond,
	})
}

func newProgressCallback(fn func(ShardProgress)) *ffi.ProgressCallback {
	var cb ffi.ProgressCallback = progressCallback(fn)
	return &cb
}
