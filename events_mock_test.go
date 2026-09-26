//go:build siastorage_mock

package siastorage

import (
	"context"
	"testing"

	"go.sia.tech/core/types"
)

// TestObjectEventsReportWrites proves a pinned object shows up in the change
// feed, carrying a usable handle rather than just an ID.
func TestObjectEventsReportWrites(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	obj := uploadPinned(t, sdk, payload(1<<20))
	defer obj.Close()

	events, err := sdk.ObjectEvents(ctx, EventCursor{}, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer CloseObjects(events)

	var found *ObjectEvent
	for i := range events {
		if events[i].ID == obj.ID() {
			found = &events[i]
		}
	}
	if found == nil {
		t.Fatalf("the pinned object is absent from %d events", len(events))
	}
	if found.Deleted {
		t.Fatal("a write was reported as a deletion")
	}
	if found.UpdatedAt.IsZero() {
		t.Fatal("updatedAt is zero")
	}
	if found.Object == nil {
		t.Fatal("a write event carried no object")
	}
	if found.Object.Size() != obj.Size() {
		t.Fatalf("the event's object is %d bytes, the original %d", found.Object.Size(), obj.Size())
	}
}

// TestObjectEventsReportDeletes proves a removal arrives as a deletion with no
// object attached, which is the case a consumer has to special case.
func TestObjectEventsReportDeletes(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	obj := uploadPinned(t, sdk, payload(1<<20))
	id := obj.ID()
	obj.Close()

	if err := sdk.DeleteObject(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}

	events, err := sdk.ObjectEvents(ctx, EventCursor{}, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer CloseObjects(events)

	var deletion *ObjectEvent
	for i := range events {
		if events[i].ID == id && events[i].Deleted {
			deletion = &events[i]
		}
	}
	if deletion == nil {
		t.Fatalf("no deletion event for %v among %d events", id, len(events))
	}
	if deletion.Object != nil {
		t.Fatal("a deletion carried an object")
	}
}

// TestObjectEventsCursor proves a cursor resumes after the event it names,
// which is what makes polling the feed possible rather than re listing.
func TestObjectEventsCursor(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	for i := range 3 {
		obj := uploadPinned(t, sdk, payload((i+1)<<20))
		obj.Close()
	}

	first, err := sdk.ObjectEvents(ctx, EventCursor{}, 1)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	defer CloseObjects(first)
	if len(first) != 1 {
		t.Fatalf("asked for one event, got %d", len(first))
	}

	rest, err := sdk.ObjectEvents(ctx, first[0].Cursor(), 0)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	defer CloseObjects(rest)
	if len(rest) == 0 {
		t.Fatal("resuming from the cursor returned nothing")
	}
	for _, e := range rest {
		if e.ID == first[0].ID {
			t.Fatal("the cursor replayed the event it was built from")
		}
	}
}

// TestObjectEventsEmptyTail proves polling past the end is an empty page rather
// than an error, so a consumer can loop on it.
func TestObjectEventsEmptyTail(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	obj := uploadPinned(t, sdk, payload(1<<20))
	defer obj.Close()

	events, err := sdk.ObjectEvents(ctx, EventCursor{}, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer CloseObjects(events)
	if len(events) == 0 {
		t.Fatal("expected at least one event to page past")
	}

	tail, err := sdk.ObjectEvents(ctx, events[len(events)-1].Cursor(), 0)
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	defer CloseObjects(tail)
	if len(tail) != 0 {
		t.Fatalf("paging past the last event returned %d more", len(tail))
	}
}

// TestObjectEventsUnknownCursor proves a cursor naming an event that never
// existed is handled rather than crossing the boundary as a bad timestamp.
func TestObjectEventsUnknownCursor(t *testing.T) {
	_, sdk := transferSDK(t)

	var id types.Hash256
	id[0] = 0xEF
	events, err := sdk.ObjectEvents(context.Background(), EventCursor{AfterID: id}, 0)
	if err != nil {
		t.Fatalf("unknown cursor: %v", err)
	}
	CloseObjects(events)
}

func ptr[T any](v T) *T { return &v }
