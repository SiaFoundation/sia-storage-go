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

// packedChunk is how much of an object crosses the boundary per call. Packed
// uploads exist for objects too small to fill a slab, so a large buffer would
// only add peak memory.
const packedChunk = 256 << 10

// A PackedUpload packs several objects into shared slabs, so a set of files too
// small to fill a slab each does not pay for a slab each.
//
// Add the objects one at a time, then call Finalize, which returns one object
// per Add in the order they were added. Close abandons an upload that has not
// been finalized and is safe afterwards either way.
type PackedUpload struct {
	ptr        *C.sia_packed_upload_t
	cleanup    runtime.Cleanup
	ctx        context.Context
	tok        *C.sia_cancel_t
	tokenOwner *streamCancel
	progressID uintptr
	dropped    uint64

	// mu serialises the native calls and guards done, as for a plain upload.
	mu   sync.Mutex
	done bool
}

// PackedUpload starts packing objects into shared slabs. The redundancy and
// progress options apply to the slabs the whole set lands in.
//
// ctx cancels the whole upload, not just the call that starts it.
func (s *SDK) PackedUpload(ctx context.Context, opts UploadOptions) (*PackedUpload, error) {
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

	var ptr *C.sia_packed_upload_t
	var cerr *C.char
	code := C.sia_packed_upload_start(s.ptr, &copts, &ptr, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		unregisterProgress(progressID)
		return nil, goError(ctx, code, cerr)
	}

	tok, tokenOwner := newStreamCancel(ctx)
	p := &PackedUpload{
		ptr:        ptr,
		ctx:        ctx,
		tok:        tok,
		tokenOwner: tokenOwner,
		progressID: progressID,
	}
	p.cleanup = runtime.AddCleanup(p, func(h *C.sia_packed_upload_t) {
		C.sia_packed_upload_free(h)
	}, ptr)
	return p, nil
}

// Add packs one object, reading r to EOF, and reports how many bytes it held.
//
// Objects are added one at a time and an add runs to completion before the next
// one starts, so this blocks for the whole object.
func (p *PackedUpload) Add(r io.Reader) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return 0, errClosed
	}

	var cerr *C.char
	if code := C.sia_packed_upload_add_begin(p.ptr, &cerr); code != C.SIA_OK {
		runtime.KeepAlive(p)
		return 0, goError(p.ctx, code, cerr)
	}

	buf := make([]byte, packedChunk)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			var cerr *C.char
			code := C.sia_packed_upload_add_write(p.ptr,
				(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(n), p.tok, &cerr)
			runtime.KeepAlive(p)
			if code != C.SIA_OK {
				// Finish the object anyway, so the handle is not left with an
				// add in progress that would block Finalize.
				p.finishAdd()
				return 0, goError(p.ctx, code, cerr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			p.finishAdd()
			return 0, rerr
		}
	}

	var written C.uint64_t
	var ferr *C.char
	code := C.sia_packed_upload_add_finish(p.ptr, p.tok, &written, &ferr)
	runtime.KeepAlive(p)
	if code != C.SIA_OK {
		return 0, goError(p.ctx, code, ferr)
	}
	return uint64(written), nil
}

// finishAdd closes off an add that failed part way, discarding the outcome.
// The caller holds mu.
func (p *PackedUpload) finishAdd() {
	var written C.uint64_t
	var cerr *C.char
	C.sia_packed_upload_add_finish(p.ptr, p.tok, &written, &cerr)
	runtime.KeepAlive(p)
	if cerr != nil {
		C.sia_string_free(cerr)
	}
}

// Remaining reports how many more bytes fit in the slab being packed.
//
// It blocks while an Add is running, because both take the same native lock.
func (p *PackedUpload) Remaining() uint64 {
	n := uint64(C.sia_packed_upload_remaining(p.ptr))
	runtime.KeepAlive(p)
	return n
}

// Length reports how many bytes have been packed so far. It blocks while an Add
// is running, as Remaining does.
func (p *PackedUpload) Length() uint64 {
	n := uint64(C.sia_packed_upload_length(p.ptr))
	runtime.KeepAlive(p)
	return n
}

// OptimalDataSize reports the payload size that fills a slab exactly, which is
// the size to aim a batch at. It blocks while an Add is running, as Remaining
// does.
func (p *PackedUpload) OptimalDataSize() uint64 {
	n := uint64(C.sia_packed_upload_optimal_data_size(p.ptr))
	runtime.KeepAlive(p)
	return n
}

// Finalize uploads the packed slabs and returns one object per Add, in the
// order they were added. Every object is a handle the caller owns and must
// Close.
//
// It consumes the upload whatever it returns, so Close afterwards is a no op.
func (p *PackedUpload) Finalize() ([]*Object, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return nil, errClosed
	}

	var objs **C.sia_object_t
	var n C.size_t
	var cerr *C.char
	code := C.sia_packed_upload_finalize(p.ptr, p.tok, &objs, &n, &cerr)
	runtime.KeepAlive(p)
	// Finalize consumes the packed upload but not the handle around it.
	p.free()
	if code != C.SIA_OK {
		return nil, goError(p.ctx, code, cerr)
	}
	if objs == nil || n == 0 {
		return nil, nil
	}
	// The array is ours to free; the objects in it go to the caller.
	defer C.sia_object_array_free(objs, n)

	out := make([]*Object, 0, int(n))
	for _, ptr := range unsafe.Slice(objs, int(n)) {
		out = append(out, wrapObject(ptr))
	}
	return out, nil
}

// Close abandons a packed upload that has not been finalized. It is safe to
// call more than once, and after Finalize.
func (p *PackedUpload) Close() error {
	// Fire the token before taking mu so an add blocked inside Rust returns
	// and releases it.
	p.tokenOwner.fire()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return nil
	}
	p.free()
	return nil
}

// Dropped reports how many shard events the handler never saw because it could
// not keep up. It is final once the upload has been finalized or closed.
func (p *PackedUpload) Dropped() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dropped
}

// free releases the native handle and everything around it, exactly once. The
// caller holds mu. Freeing an unfinalized upload abandons it.
func (p *PackedUpload) free() {
	p.done = true
	p.cleanup.Stop()
	C.sia_packed_upload_free(p.ptr)
	// Only once the call that can emit events has returned, so the handler
	// cannot fire after this.
	_, p.dropped = unregisterProgress(p.progressID)
	p.tokenOwner.close()
}
