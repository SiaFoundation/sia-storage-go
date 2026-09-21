package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
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

	// Object is nil for a deletion. Otherwise it is a handle the caller owns
	// and must Close.
	Object *Object
}

// An EventCursor resumes a listing directly after the event it names. Both
// fields come from the last [ObjectEvent] of the previous page, because events
// sharing a timestamp are ordered by ID.
type EventCursor struct {
	After   time.Time
	AfterID types.Hash256
}

// ObjectEvents lists changes to the account's objects in order, oldest first.
//
// A nil after starts from the beginning. A zero limit takes the indexer's
// default. Reaching the end returns no events rather than an error, so a
// consumer polls by passing the cursor it built from the last page.
func (s *SDK) ObjectEvents(ctx context.Context, after *EventCursor, limit uint64) ([]ObjectEvent, error) {
	tok, release := cancelToken(ctx)
	defer release()

	var (
		hasCursor  C.bool
		afterMicro C.int64_t
		afterID    types.Hash256
	)
	if after != nil {
		hasCursor = true
		afterMicro = C.int64_t(after.After.UnixMicro())
		afterID = after.AfterID
	}

	var evs *C.sia_events_t
	var cerr *C.char
	code := C.sia_sdk_object_events(s.ptr, hasCursor, afterMicro,
		cBytes32((*[32]byte)(&afterID)), C.uint64_t(limit), tok, &evs, &cerr)
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

// CloseObjects releases the handles in a page of events, for a caller that only
// wanted the IDs.
func CloseObjects(events []ObjectEvent) {
	for _, e := range events {
		if e.Object != nil {
			e.Object.Close()
		}
	}
}
