package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.sia.tech/core/types"
)

// An ObjectEvent is one change to the account's objects, which is how a
// consumer keeping its own index stays in step without re listing everything.
type ObjectEvent struct {
	ID types.Hash256

	// Deleted reports whether the object was removed rather than written.
	Deleted bool

	UpdatedAt time.Time

	// Object is nil for a deletion.
	Object *Object
}

// An EventCursor resumes a listing directly after the event it names. Both
// fields come from the last [ObjectEvent] of the previous page, because events
// sharing a timestamp are ordered by ID.
//
// The zero cursor starts from the beginning, which is the same thing it means
// as a value: nothing precedes the zero time.
type EventCursor struct {
	After   time.Time
	AfterID types.Hash256
}

// IsZero reports whether the cursor names no event, and so starts a listing
// from the beginning.
func (c EventCursor) IsZero() bool {
	return c.After.IsZero() && c.AfterID == types.Hash256{}
}

// cArgs splits the cursor into the arguments both event listings take. The ID
// comes back by value so the caller holds an addressable copy for the call.
func (c EventCursor) cArgs() (hasCursor C.bool, afterMicro C.int64_t, afterID types.Hash256) {
	if c.IsZero() {
		return false, 0, types.Hash256{}
	}
	return true, C.int64_t(c.After.UnixMicro()), c.AfterID
}

// pageArg converts a paging count for the C boundary. A negative value would
// wrap to an enormous number there, so it means the same as zero: the
// indexer's default.
func pageArg(n int) C.uint64_t {
	if n < 0 {
		return 0
	}
	return C.uint64_t(n)
}

// ObjectEvents lists changes to the account's objects in order, oldest first.
//
// A zero after starts from the beginning. A zero limit takes the indexer's
// default. Reaching the end returns no events rather than an error, so a
// consumer polls by passing the cursor it built from the last page.
func (s *SDK) ObjectEvents(ctx context.Context, after EventCursor, limit int) ([]ObjectEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	hasCursor, afterMicro, afterID := after.cArgs()
	var evs *C.sia_events_t
	var cerr *C.char
	code := C.sia_sdk_object_events(s.ptr, hasCursor, afterMicro,
		cBytes32((*[32]byte)(&afterID)), pageArg(limit), tok, &evs, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	defer C.sia_events_free(evs)

	out := make([]ObjectEvent, 0, int(C.sia_events_len(evs)))
	for i := C.size_t(0); ; i++ {
		var (
			id      types.Hash256
			deleted C.bool
			updated C.int64_t
			obj     *C.sia_object_t
		)
		// Each index hands its object over once, so this loop must visit every
		// index exactly once or the handle leaks.
		if !bool(C.sia_events_at(evs, i, cBytes32((*[32]byte)(&id)), &deleted, &updated, &obj)) {
			break
		}
		e := ObjectEvent{
			ID:        id,
			Deleted:   bool(deleted),
			UpdatedAt: unixMicro(int64(updated)),
		}
		if obj != nil {
			e.Object = wrapObject(obj)
		}
		out = append(out, e)
	}
	return out, nil
}

// Cursor returns the cursor that resumes a listing after e.
func (e ObjectEvent) Cursor() EventCursor {
	return EventCursor{After: e.UpdatedAt, AfterID: e.ID}
}

// An ObjectEventSummary is an [ObjectEvent] carrying an [ObjectSummary] in
// place of the object, so a listing does not pay for slabs it never reads.
type ObjectEventSummary struct {
	ID types.Hash256

	// Deleted reports whether the object was removed rather than written.
	Deleted bool

	UpdatedAt time.Time

	// Object is nil for a deletion.
	Object *ObjectSummary
}

// Cursor returns the cursor that resumes a listing after e.
func (e ObjectEventSummary) Cursor() EventCursor {
	return EventCursor{After: e.UpdatedAt, AfterID: e.ID}
}

// ObjectEventSummaries lists the same events as [SDK.ObjectEvents], paged by
// the same cursor, without the objects' slabs. Each page is a small fraction
// of the size, which is what a listing that shows names and sizes wants.
// [SDK.Object] fetches the whole object by ID to download it.
func (s *SDK) ObjectEventSummaries(ctx context.Context, after EventCursor, limit int) ([]ObjectEventSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	hasCursor, afterMicro, afterID := after.cArgs()
	var evs *C.sia_event_summaries_t
	var cerr *C.char
	code := C.sia_sdk_object_event_summaries(s.ptr, hasCursor, afterMicro,
		cBytes32((*[32]byte)(&afterID)), pageArg(limit), tok, &evs, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	defer C.sia_event_summaries_free(evs)

	n := int(C.sia_event_summaries_len(evs))
	out := make([]ObjectEventSummary, 0, n)
	for i := range n {
		var (
			id        types.Hash256
			deleted   C.bool
			updated   C.int64_t
			hasObject C.bool
			sum       C.sia_object_summary_t
		)
		if !bool(C.sia_event_summaries_at(evs, C.size_t(i), cBytes32((*[32]byte)(&id)),
			&deleted, &updated, &hasObject, &sum)) {
			return nil, fmt.Errorf("event %d of %d is out of range", i, n)
		}
		e := ObjectEventSummary{
			ID:        id,
			Deleted:   bool(deleted),
			UpdatedAt: unixMicro(int64(updated)),
		}
		if bool(hasObject) {
			obj := goObjectSummary(sum, readMetadata(func(buf *C.uint8_t, n C.size_t) C.size_t {
				return C.sia_event_summaries_metadata(evs, C.size_t(i), buf, n)
			}))
			e.Object = &obj
		}
		out = append(out, e)
	}
	return out, nil
}
