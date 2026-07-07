package siastorage

/*
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"unsafe"
)

var (
	// ErrEmptyObject is returned when trying to add an empty object.
	ErrEmptyObject = errors.New("empty object")
	// ErrUploadClosed is returned when trying to add an object to a closed upload.
	ErrUploadClosed = errors.New("upload is closed")
	// ErrUploadFinalized is returned when trying to add an object to an
	// already finalized upload.
	ErrUploadFinalized = errors.New("upload already finalized")
)

// A PackedUpload allows multiple objects to be uploaded together in a single
// upload. This can be more efficient than uploading each object separately
// if the size of the objects is less than the optimal data size. A packed
// upload is not thread-safe.
type PackedUpload struct {
	mu        sync.Mutex
	finalized bool
	closed    bool
	closeCh   chan struct{}
	closeOnce sync.Once

	// adds counts objects registered on the Rust side; dead marks the
	// indices of failed or empty adds, which are dropped from Finalize's
	// result to preserve the "not added" contract.
	adds int
	dead map[int]bool

	ptr *C.sia_packed_upload_t
	pid uintptr
}

func newPackedUpload(ptr *C.sia_packed_upload_t, pid uintptr) *PackedUpload {
	return &PackedUpload{
		ptr:     ptr,
		pid:     pid,
		closeCh: make(chan struct{}),
		dead:    make(map[int]bool),
	}
}

// checkState returns an error if the upload is closed or finalized.
func (u *PackedUpload) checkState() error {
	if u.closed {
		return ErrUploadClosed
	} else if u.finalized {
		return ErrUploadFinalized
	}
	return nil
}

// mapCloseErr translates a cancellation triggered by Close into
// ErrUploadClosed to preserve the sentinel contract. Close signals closeCh
// before it can take the mutex, so the channel is checked rather than the
// closed flag.
func (u *PackedUpload) mapCloseErr(err error) error {
	if errors.Is(err, errCancelled) {
		select {
		case <-u.closeCh:
			return ErrUploadClosed
		default:
		}
	}
	return err
}

// addToken creates a cancellation token wired to both ctx and Close.
func (u *PackedUpload) addToken(ctx context.Context) (tok *C.sia_cancel_t, release func()) {
	tok, cancel, free := newCancelToken()
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			cancel()
		case <-u.closeCh:
			cancel()
		case <-done:
		}
	}()
	return tok, func() {
		close(done)
		<-exited
		free()
	}
}

// Add adds a new object to the upload. The data will be read until EOF and
// packed into the upload. The caller must call Finalize to get the resulting
// objects after all objects have been added.
func (u *PackedUpload) Add(ctx context.Context, r io.Reader) (int64, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.checkState(); err != nil {
		return 0, err
	}

	var cerr *C.char
	if code := C.sia_packed_upload_add_begin(u.ptr, &cerr); code != C.SIA_OK {
		return 0, goError(ctx, code, cerr)
	}
	index := u.adds
	u.adds++

	tok, release := u.addToken(ctx)
	defer release()

	mapErr := u.mapCloseErr

	finish := func() (int64, error) {
		var written C.uint64_t
		var cerr *C.char
		code := C.sia_packed_upload_add_finish(u.ptr, tok, &written, &cerr)
		if err := goError(ctx, code, cerr); err != nil {
			u.dead[index] = true
			return 0, mapErr(err)
		}
		return int64(written), nil
	}

	buf := make([]byte, uploadBufferSize)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			var cerr *C.char
			code := C.sia_packed_upload_add_write(u.ptr, (*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(n), tok, &cerr)
			if err := goError(ctx, code, cerr); err != nil {
				u.dead[index] = true
				return 0, mapErr(err)
			}
		}
		if errors.Is(rerr, io.EOF) {
			break
		} else if rerr != nil {
			// reader error: flush the partial data as dead padding so
			// the upload stays usable, then drop the object
			if _, err := finish(); err != nil {
				return 0, err
			}
			u.dead[index] = true
			return 0, fmt.Errorf("failed to add object: %w", rerr)
		}
	}

	written, err := finish()
	if err != nil {
		return 0, err
	} else if written == 0 {
		u.dead[index] = true
		return 0, ErrEmptyObject
	}
	return written, nil
}

// Finalize finalizes the upload and returns the resulting objects. This will
// wait for all slabs to be uploaded before returning. The resulting objects
// will contain the metadata needed to download the objects. The caller must
// call PinObject for each returned object to pin the slabs and save the
// object metadata to the indexer.
func (u *PackedUpload) Finalize(ctx context.Context) ([]Object, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.checkState(); err != nil {
		return nil, err
	}
	u.finalized = true

	tok, release := u.addToken(ctx)
	defer release()

	var objs **C.sia_object_t
	var count C.size_t
	var cerr *C.char
	code := C.sia_packed_upload_finalize(u.ptr, tok, &objs, &count, &cerr)
	if err := goError(ctx, code, cerr); err != nil {
		return nil, u.mapCloseErr(err)
	}
	defer C.sia_object_array_free(objs, count)

	ptrs := unsafe.Slice(objs, int(count))
	objects := make([]Object, 0, len(ptrs))
	for i, ptr := range ptrs {
		if u.dead[i] {
			C.sia_object_free(ptr)
			continue
		}
		objects = append(objects, newObject(ptr))
	}
	return objects, nil
}

// Length returns the cumulative number of bytes written to the upload pipeline,
// including dead padding from errored reads.
func (u *PackedUpload) Length() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return 0
	}
	return int64(C.sia_packed_upload_length(u.ptr))
}

// Remaining returns the number of bytes remaining until reaching the optimal
// packed size. Adding objects larger than this will span multiple slabs. To
// minimize padding, prioritize objects that fit within the remaining size.
func (u *PackedUpload) Remaining() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return 0
	}
	return int64(C.sia_packed_upload_remaining(u.ptr))
}

// OptimalDataSize returns the data portion of a slab based on the number of
// data shards.
func (u *PackedUpload) OptimalDataSize() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return 0
	}
	return int64(C.sia_packed_upload_optimal_data_size(u.ptr))
}

// Close closes the packed upload and releases any resources. If the upload
// has not been finalized, it is aborted. The caller must always call Close
// to ensure proper cleanup.
func (u *PackedUpload) Close() error {
	u.closeOnce.Do(func() {
		// unblock any in-flight Add or Finalize before taking the lock
		close(u.closeCh)
		u.mu.Lock()
		defer u.mu.Unlock()
		u.closed = true
		C.sia_packed_upload_free(u.ptr)
		unregisterProgress(u.pid)
	})
	return nil
}

// UploadPacked creates a new packed upload. This allows multiple objects to be
// packed together for more efficient uploads. The returned PackedUpload can be
// used to add objects and then finalized to get the resulting objects. A packed
// upload is not thread-safe.
func (s *SDK) UploadPacked(opts ...UploadOption) (*PackedUpload, error) {
	var uo uploadOption
	for _, opt := range opts {
		opt(&uo)
	}

	ptr, unlock, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer unlock()

	copts, pid := uo.cOptions()
	var up *C.sia_packed_upload_t
	var cerr *C.char
	code := C.sia_packed_upload_start(ptr, &copts, &up, &cerr)
	if err := goError(nil, code, cerr); err != nil {
		unregisterProgress(pid)
		return nil, err
	}
	return newPackedUpload(up, pid), nil
}
