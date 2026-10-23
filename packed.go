package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"errors"
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

	// mu serialises the native calls and guards done, as for a plain upload.
	mu   sync.Mutex
	done bool
}

// UploadPacked starts packing objects into shared slabs. The redundancy and
// progress options apply to the slabs the whole set lands in.
//
// ctx cancels the whole upload, not just the call that starts it.
func (s *SDK) UploadPacked(ctx context.Context, opts ...UploadOption) (*PackedUpload, error) {
	o := uploadOptions{}.with(opts)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	if o.startOffset != nil {
		// A packed add always appends, and the options the native side takes
		// for one carry no offset to pass this through as.
		return nil, errors.New("a packed upload cannot take a start offset")
	}

	progressID := registerProgress(o.onShard)

	copts := C.sia_packed_upload_options_t{
		max_buffered_slabs: C.uint64_t(o.maxBufferedSlabs),
		userdata:           C.uintptr_t(progressID),
	}
	if o.dataShards > 0 && o.parityShards > 0 {
		copts.data_shards = C.uint8_t(o.dataShards)
		copts.parity_shards = C.uint8_t(o.parityShards)
		copts.set_redundancy = true
	}
	if o.onShard != nil {
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
//
// A failed add contributes no object. The bytes that reached the pack before
// the failure stay in the slab and are paid for, but nothing references them,
// so [PackedUpload.Finalize] still returns exactly one object per successful
// Add, in order.
func (p *PackedUpload) Add(r io.Reader) (int64, error) {
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
				// Abandon the object rather than finishing it. Finishing would
				// leave a short but structurally valid object in the pack that
				// the caller cannot tell apart from a complete one.
				p.abortAdd()
				return 0, goError(p.ctx, code, cerr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			p.abortAdd()
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
	return int64(written), nil
}

// abortAdd abandons an add that failed part way, so the pack is left with no
// object for it and the handle is free to take the next one. The caller holds
// mu.
//
// The token is deliberately not passed. Whenever the context is what failed
// the add, the token has already fired, and the native abort checks it after
// tearing the add down but before detaching it: it would return early with
// the add still attached, and the next Finalize would report an add in
// progress instead of the cancellation.
//
// Waiting without a token is safe because the teardown comes first. The reader
// is already failing by the time the abort joins the task, so it is waiting
// on something that is ending rather than on the network.
func (p *PackedUpload) abortAdd() {
	var cerr *C.char
	C.sia_packed_upload_add_abort(p.ptr, nil, &cerr)
	runtime.KeepAlive(p)
	if cerr != nil {
		C.sia_string_free(cerr)
	}
}

// Remaining reports how many more bytes fit in the slab being packed, and
// zero once the upload has been finalized or closed.
//
// It blocks while an Add is running, because both take the same lock.
func (p *PackedUpload) Remaining() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return 0
	}
	n := int64(C.sia_packed_upload_remaining(p.ptr))
	runtime.KeepAlive(p)
	return n
}

// Length reports how many bytes have been packed so far, and zero once the
// upload has been finalized or closed. It blocks while an Add is running, as
// Remaining does.
func (p *PackedUpload) Length() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return 0
	}
	n := int64(C.sia_packed_upload_length(p.ptr))
	runtime.KeepAlive(p)
	return n
}

// OptimalDataSize reports the payload size that fills a slab exactly, which is
// the size to aim a batch at, and zero once the upload has been finalized or
// closed. It blocks while an Add is running, as Remaining does.
func (p *PackedUpload) OptimalDataSize() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return 0
	}
	n := int64(C.sia_packed_upload_optimal_data_size(p.ptr))
	runtime.KeepAlive(p)
	return n
}

// Slabs reports how many slabs the finalized upload will occupy, and zero once
// the upload has been finalized or closed. It blocks while an Add is running,
// as Remaining does.
func (p *PackedUpload) Slabs() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return 0
	}
	n := int64(C.sia_packed_upload_slabs(p.ptr))
	runtime.KeepAlive(p)
	return n
}

// Finalize uploads the packed slabs and returns one object per Add, in the
// order they were added.
//
// It consumes the upload whatever it returns, so Close afterwards is a no op.
func (p *PackedUpload) Finalize() ([]*Object, error) {
	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return nil, errClosed
	}

	var objs **C.sia_object_t
	var n C.size_t
	var cerr *C.char
	code := C.sia_packed_upload_finalize(p.ptr, p.tok, &objs, &n, &cerr)
	runtime.KeepAlive(p)
	// Finalize consumes the packed upload but not the handle around it.
	p.retire()
	p.mu.Unlock()
	defer p.drain()

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
	if p.done {
		p.mu.Unlock()
		return nil
	}
	p.retire()
	p.mu.Unlock()
	p.drain()
	return nil
}

// retire releases the native handle, exactly once. The caller holds mu, and
// must call drain once it no longer does. Retiring an unfinalized upload
// abandons it.
func (p *PackedUpload) retire() {
	p.done = true
	p.cleanup.Stop()
	C.sia_packed_upload_free(p.ptr)
}

// drain waits for the queued progress events to reach the handler, then frees
// the token. Outside mu, because a handler calling Remaining or Length would
// be waiting for mu. After retire, which is what stops further events
// arriving.
func (p *PackedUpload) drain() {
	unregisterProgress(p.progressID)
	p.tokenOwner.close()
}
