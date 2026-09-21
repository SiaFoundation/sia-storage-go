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

// DownloadOptions configures a download. The zero value reads the whole object.
type DownloadOptions struct {
	// Offset is the byte to start at.
	Offset uint64

	// Length bounds how many bytes to read from Offset. A nil Length reads to
	// the end of the object, which is not the same as a zero Length.
	Length *uint64

	// MaxBufferedChunks bounds how many recovered chunks are held in memory.
	// Zero uses the SDK default.
	MaxBufferedChunks uint64

	// OnShard is called for every shard that finishes downloading, under the
	// same terms as the upload's handler.
	OnShard func(ShardProgress)
}

// A Download streams an object's data. It implements [io.ReadCloser], so
// [io.Copy] drives it and the usual buffering wrappers apply.
//
// Close releases the transfer and must be called, whether or not the read
// reached EOF.
type Download struct {
	ptr        *C.sia_download_t
	cleanup    runtime.Cleanup
	ctx        context.Context
	tok        *C.sia_cancel_t
	tokenOwner *streamCancel
	progressID uintptr
	dropped    uint64

	// mu serialises the native calls and guards done. Close fires the token
	// before taking it, because the native side forbids freeing a download
	// underneath a blocked read; cancelling first is what unblocks it.
	mu   sync.Mutex
	done bool
}

// Download begins streaming obj's data.
//
// ctx cancels the whole transfer, not just the call that starts it.
func (s *SDK) Download(ctx context.Context, obj *Object, opts DownloadOptions) (*Download, error) {
	progressID := registerProgress(opts.OnShard)

	copts := C.sia_download_options_t{
		offset:              C.uint64_t(opts.Offset),
		max_buffered_chunks: C.uint64_t(opts.MaxBufferedChunks),
		userdata:            C.uintptr_t(progressID),
	}
	if opts.Length != nil {
		copts.has_length = true
		copts.length = C.uint64_t(*opts.Length)
	}
	if opts.OnShard != nil {
		copts.on_shard = C.sia_go_progress_cb()
	}

	var ptr *C.sia_download_t
	var cerr *C.char
	code := C.sia_download_start(s.ptr, obj.ptr, &copts, &ptr, &cerr)
	runtime.KeepAlive(s)
	runtime.KeepAlive(obj)
	if code != C.SIA_OK {
		unregisterProgress(progressID)
		return nil, goError(ctx, code, cerr)
	}

	tok, tokenOwner := newStreamCancel(ctx)
	d := &Download{
		ptr:        ptr,
		ctx:        ctx,
		tok:        tok,
		tokenOwner: tokenOwner,
		progressID: progressID,
	}
	d.cleanup = runtime.AddCleanup(d, func(p *C.sia_download_t) {
		C.sia_download_free(p)
	}, ptr)
	return d, nil
}

// Read fills p with recovered data, blocking until at least one byte is
// available and then taking whatever else is ready without blocking again.
// It returns [io.EOF] once the requested range is exhausted.
func (d *Download) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done {
		return 0, errClosed
	}

	var n C.size_t
	var cerr *C.char
	code := C.sia_download_read(d.ptr,
		(*C.uint8_t)(unsafe.Pointer(&p[0])), C.size_t(len(p)), d.tok, &n, &cerr)
	runtime.KeepAlive(d)
	runtime.KeepAlive(p)
	if code != C.SIA_OK {
		return 0, goError(d.ctx, code, cerr)
	}
	if n == 0 {
		return 0, io.EOF
	}
	return int(n), nil
}

// Close releases the download, cancelling any recovery still in flight. It is
// safe to call more than once.
func (d *Download) Close() error {
	// The native side forbids freeing a download while a read is blocked on
	// it, so fire the token first and only then wait for mu, which the blocked
	// read holds until it returns.
	d.tokenOwner.fire()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done {
		return nil
	}
	d.done = true
	d.cleanup.Stop()
	C.sia_download_free(d.ptr)
	// Only once the call that can emit events has returned, so the handler
	// cannot fire after this.
	_, d.dropped = unregisterProgress(d.progressID)
	d.tokenOwner.close()
	return nil
}

// Dropped reports how many shard events the handler never saw because it could
// not keep up. It is final once the download has been closed.
func (d *Download) Dropped() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dropped
}
