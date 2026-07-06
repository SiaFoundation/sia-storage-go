package siastorage

import (
	"context"
	"errors"
	"io"

	ffi "go.sia.tech/siastorage/sia_storage_ffi"
)

var (
	// ErrUploadClosed is returned when trying to add an object to a closed
	// upload.
	ErrUploadClosed = errors.New("upload is closed")
)

// A PackedUpload allows multiple objects to be uploaded together in a single
// upload. This can be more efficient than uploading each object separately if
// the size of the objects is less than the optimal data size.
//
// A packed upload is not thread-safe.
type PackedUpload struct {
	inner           *ffi.PackedUpload
	optimalDataSize int64
}

// UploadPacked creates a new packed upload. This allows multiple objects to be
// packed together for more efficient uploads. The returned PackedUpload can be
// used to add objects and then finalized to get the resulting objects.
func (s *SDK) UploadPacked(opts ...UploadOption) (*PackedUpload, error) {
	inner, err := s.inner.UploadPacked(uploadOptions(opts))
	if err != nil {
		return nil, err
	}
	// at length 0, Remaining reports the optimal data size.
	return &PackedUpload{inner: inner, optimalDataSize: int64(inner.Remaining())}, nil
}

// Add adds a new object to the upload. The data will be read until EOF and
// packed into the upload. The caller must call [PackedUpload.Finalize] to get
// the resulting objects after all objects have been added.
func (u *PackedUpload) Add(ctx context.Context, r io.Reader) (int64, error) {
	n, err := runContext(ctx, func() (uint64, error) {
		return u.inner.Add(newFFIReader(r))
	})
	if err != nil && errors.Is(err, ffi.ErrUploadErrorClosed) {
		return int64(n), ErrUploadClosed
	}
	return int64(n), err
}

// Close closes the packed upload and releases any resources. The caller must
// always call Close to ensure proper cleanup.
func (u *PackedUpload) Close() error {
	u.inner.Cancel()
	return nil
}

// Finalize finalizes the upload and returns the resulting objects. This will
// wait for all slabs to be uploaded before returning. The caller must call
// [SDK.PinObject] for each returned object to pin the slabs and save the
// object metadata to the indexer.
func (u *PackedUpload) Finalize(ctx context.Context) ([]Object, error) {
	raw, err := runContext(ctx, u.inner.Finalize)
	if err != nil {
		return nil, err
	}
	objects := make([]Object, len(raw))
	for i, o := range raw {
		objects[i] = Object{inner: o}
	}
	return objects, nil
}

// Length returns the cumulative number of bytes written to the upload.
func (u *PackedUpload) Length() int64 {
	return int64(u.inner.Length())
}

// Remaining returns the number of bytes remaining until reaching the optimal
// packed size. Adding objects larger than this will span multiple slabs. To
// minimize padding, prioritize objects that fit within the remaining size.
func (u *PackedUpload) Remaining() int64 {
	return int64(u.inner.Remaining())
}

// OptimalDataSize returns the data portion of a slab based on the number of
// data shards.
func (u *PackedUpload) OptimalDataSize() int64 {
	return u.optimalDataSize
}
