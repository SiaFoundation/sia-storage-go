package siastorage

import (
	"io"
	"time"

	ffi "go.sia.tech/siastorage/sia_storage_ffi"
)

// The SDK interacts with the Sia network through an indexer. Instances are
// created with a Builder.
type SDK struct {
	inner *ffi.Sdk
}

// AppKey returns the application key used by the SDK. It must be kept secret
// and stored securely by the application.
func (s *SDK) AppKey() *AppKey {
	return s.inner.AppKey()
}

// Account returns the current account.
func (s *SDK) Account() (Account, error) {
	return s.inner.Account()
}

// Hosts returns a list of all usable hosts.
func (s *SDK) Hosts() ([]Host, error) {
	return s.inner.Hosts()
}

// Upload uploads data read from r to the Sia network.
//
// Pass NewObject for new uploads. To resume a previous upload, pass the
// object returned from the earlier call. The returned object contains all
// slabs from the input object plus the newly uploaded slabs; the caller must
// pin it to the indexer afterward with PinObject.
func (s *SDK) Upload(obj *Object, r io.Reader, opts UploadOptions) (*Object, error) {
	return s.inner.Upload(obj, newFFIReader(r), opts)
}

// UploadPacked creates a new packed upload, allowing multiple small objects
// to be packed together for more efficient uploads.
func (s *SDK) UploadPacked(opts UploadOptions) (*PackedUpload, error) {
	inner, err := s.inner.UploadPacked(opts)
	if err != nil {
		return nil, err
	}
	return &PackedUpload{inner: inner}, nil
}

// Download returns a reader streaming the data referenced by the object.
// Closing the reader cancels the download and releases its resources.
func (s *SDK) Download(obj *Object, opts DownloadOptions) (io.ReadCloser, error) {
	dl, err := s.inner.Download(obj, opts)
	if err != nil {
		return nil, err
	}
	return &downloadReader{dl: dl}, nil
}

// PinObject pins an object to the indexer.
func (s *SDK) PinObject(obj *Object) error {
	return s.inner.PinObject(obj)
}

// Object returns metadata about a specific object stored in the indexer.
func (s *SDK) Object(key string) (*Object, error) {
	return s.inner.Object(key)
}

// ObjectEvents returns objects stored in the indexer. When syncing, the
// caller should provide the last seen cursor to avoid missing or duplicating
// objects. A nil cursor returns the first page of results.
func (s *SDK) ObjectEvents(cursor *ObjectsCursor, limit uint32) ([]ObjectEvent, error) {
	return s.inner.ObjectEvents(cursor, limit)
}

// UpdateObjectMetadata updates the metadata of an object already pinned to
// the indexer.
func (s *SDK) UpdateObjectMetadata(obj *Object) error {
	return s.inner.UpdateObjectMetadata(obj)
}

// DeleteObject deletes an object from the indexer.
func (s *SDK) DeleteObject(key string) error {
	return s.inner.DeleteObject(key)
}

// Slab returns metadata about a slab stored in the indexer.
func (s *SDK) Slab(slabID string) (PinnedSlab, error) {
	return s.inner.Slab(slabID)
}

// PruneSlabs unpins slabs not used by any object on the account.
func (s *SDK) PruneSlabs() error {
	return s.inner.PruneSlabs()
}

// ShareObject creates a signed URL that can be used to share object metadata
// with other people using an indexer.
func (s *SDK) ShareObject(obj *Object, validUntil time.Time) (string, error) {
	return s.inner.ShareObject(obj, validUntil)
}

// SharedObject retrieves a shared object from a signed URL.
func (s *SDK) SharedObject(sharedURL string) (*Object, error) {
	return s.inner.SharedObject(sharedURL)
}

// A PackedUpload packs multiple objects into shared slabs for more efficient
// uploads of data smaller than the minimum slab size.
type PackedUpload struct {
	inner *ffi.PackedUpload
}

// Add reads r until EOF and packs the data into the current slab. It returns
// the number of bytes consumed. Call Finalize once all objects have been
// added to get the resulting objects.
func (u *PackedUpload) Add(r io.Reader) (uint64, error) {
	return u.inner.Add(newFFIReader(r))
}

// Finalize finalizes the upload and returns the resulting objects. The
// caller must pin the resulting objects to the indexer when ready.
func (u *PackedUpload) Finalize() ([]*Object, error) {
	return u.inner.Finalize()
}

// Cancel cancels the upload, interrupting any in-flight Add or Finalize
// operations.
func (u *PackedUpload) Cancel() {
	u.inner.Cancel()
}

// Length returns the number of bytes added so far.
func (u *PackedUpload) Length() uint64 {
	return u.inner.Length()
}

// Remaining returns the number of bytes remaining until reaching the optimal
// packed size. Adding objects larger than this will start a new slab.
func (u *PackedUpload) Remaining() uint64 {
	return u.inner.Remaining()
}

// Slabs returns the number of slabs in the upload.
func (u *PackedUpload) Slabs() uint64 {
	return u.inner.Slabs()
}
