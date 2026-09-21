package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"io"
	"runtime"
	"sync"
	"unsafe"
)

// UploadOptions configures an upload. The zero value uses the SDK's defaults.
type UploadOptions struct {
	// DataShards and ParityShards set the slab's erasure coding. Both must be
	// non zero to take effect; leaving either at zero keeps the SDK default.
	DataShards   uint8
	ParityShards uint8

	// MaxBufferedSlabs bounds how many encoded slabs are held in memory. Zero
	// uses the SDK default, which is a share of system memory.
	MaxBufferedSlabs uint64

	// OnShard is called for every shard that finishes uploading. It runs on a
	// goroutine the upload owns rather than on the Rust thread that reported
	// the shard, so it may block without stalling the transfer, though events
	// are dropped once it falls far enough behind. A panic in the handler is
	// recovered and costs that event only.
	OnShard func(ShardProgress)
}

// An Upload streams data into an object. It implements [io.Writer], so
// [io.Copy] drives it.
//
// Call Finish to signal EOF and get the uploaded object. Close aborts an
// upload that has not finished, and is safe to call afterwards either way, so
// `defer up.Close()` is the right pattern.
type Upload struct {
	ptr        *C.sia_upload_t
	cleanup    runtime.Cleanup
	ctx        context.Context
	tok        *C.sia_cancel_t
	tokenOwner *streamCancel
	progressID uintptr
	dropped    uint64

	// mu serialises the native calls and guards done. Close fires the token
	// before taking it, so a write already blocked inside Rust returns rather
	// than holding mu for the rest of the transfer.
	mu   sync.Mutex
	done bool
}

// Upload begins streaming data into obj, which supplies the metadata the
// finished object carries. Write the data, then call [Upload.Finish].
//
// ctx cancels the whole transfer, not just the call that starts it.
func (s *SDK) Upload(ctx context.Context, obj *Object, opts UploadOptions) (*Upload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj.mu.RLock()
	defer obj.mu.RUnlock()
	if s.closed || obj.closed {
		return nil, errClosed
	}

	progressID := registerProgress(opts.OnShard)

	copts := C.sia_upload_options_t{
		max_buffered_slabs: C.uint64_t(opts.MaxBufferedSlabs),
		userdata:           C.uintptr_t(progressID),
	}
	if opts.DataShards > 0 && opts.ParityShards > 0 {
		copts.data_shards = C.uint8_t(opts.DataShards)
		copts.parity_shards = C.uint8_t(opts.ParityShards)
		copts.set_redundancy = true
	}
	if opts.OnShard != nil {
		copts.on_shard = C.sia_go_progress_cb()
	}

	var ptr *C.sia_upload_t
	var cerr *C.char
	code := C.sia_upload_start(s.ptr, obj.ptr, &copts, &ptr, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(obj)
	if code != C.SIA_OK {
		unregisterProgress(progressID)
		return nil, goError(ctx, code, cerr)
	}

	tok, tokenOwner := newStreamCancel(ctx)
	u := &Upload{
		ptr:        ptr,
		ctx:        ctx,
		tok:        tok,
		tokenOwner: tokenOwner,
		progressID: progressID,
	}
	// As with every handle here, the cleanup must not capture u, so u is
	// collectable from the moment a method loads u.ptr. Each call keeps it
	// alive across the boundary itself.
	u.cleanup = runtime.AddCleanup(u, func(p *C.sia_upload_t) {
		C.sia_upload_free(p)
	}, ptr)
	return u, nil
}

// Write streams p into the upload, returning how many bytes reached it.
//
// A cancelled write reports the count it managed, so the remainder can be
// written again once the cause is resolved rather than restarting the object.
func (u *Upload) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.done {
		return 0, errClosed
	}

	var written C.size_t
	var cerr *C.char
	code := C.sia_upload_write(u.ptr,
		(*C.uint8_t)(unsafe.Pointer(&p[0])), C.size_t(len(p)), u.tok, &written, &cerr)
	runtime.KeepAlive(u)
	runtime.KeepAlive(p)
	if code != C.SIA_OK {
		return int(written), goError(u.ctx, code, cerr)
	}
	// io.Writer requires a non-nil error whenever fewer bytes were taken than
	// offered, and io.Copy turns a violation into a confusing short write far
	// from its cause. The C ABI only promises *written on every status, not
	// that a successful write is a complete one.
	if int(written) < len(p) {
		return int(written), io.ErrShortWrite
	}
	return int(written), nil
}

// Finish signals EOF, waits for the transfer to complete and returns the
// uploaded object, which carries the slabs and the timestamps the indexer
// assigned.
//
// It consumes the upload whatever it returns, so a failure uploads nothing
// further and Close afterwards is a no op.
func (u *Upload) Finish() (*Object, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.done {
		return nil, errClosed
	}

	var ptr *C.sia_object_t
	var cerr *C.char
	code := C.sia_upload_finish(u.ptr, u.tok, &ptr, &cerr)
	runtime.KeepAlive(u)
	// Finish drains the transfer and consumes the upload task, but the handle
	// itself is still ours to free, whatever it returned.
	u.free()
	if code != C.SIA_OK {
		return nil, goError(u.ctx, code, cerr)
	}
	return wrapObject(ptr), nil
}

// Close aborts an upload that has not finished. It is safe to call more than
// once, and after Finish.
func (u *Upload) Close() error {
	// Fire the token before taking mu so a write blocked inside Rust returns
	// and releases it. Cancelling an already finished upload is harmless.
	u.tokenOwner.fire()
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.done {
		return nil
	}
	u.free()
	return nil
}

// Dropped reports how many shard events the handler never saw because it could
// not keep up. It is final once the upload has finished or been closed.
//
// The byte count is not lost with them: every delivered event carries the
// running total in [ShardProgress.Transferred].
func (u *Upload) Dropped() uint64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.dropped
}

// free releases the native handle and everything around it, exactly once. The
// caller holds mu. Freeing a running upload aborts it.
func (u *Upload) free() {
	u.done = true
	u.cleanup.Stop()
	C.sia_upload_free(u.ptr)
	// Only once the call that can emit events has returned, so the handler
	// cannot fire after this.
	_, u.dropped = unregisterProgress(u.progressID)
	u.tokenOwner.close()
}
