//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"context"
	"testing"

	"go.sia.tech/core/types"
)

// TestObjectEventsReportWrites proves a pinned object shows up in the change
// feed, carrying a usable object rather than just an ID.
func TestObjectEventsReportWrites(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	obj := uploadPinned(t, sdk, payload(1<<20))

	events, err := sdk.ObjectEvents(ctx, EventCursor{}, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}

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

	if err := sdk.DeleteObject(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}

	events, err := sdk.ObjectEvents(ctx, EventCursor{}, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}

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
		uploadPinned(t, sdk, payload((i+1)<<20))
	}

	first, err := sdk.ObjectEvents(ctx, EventCursor{}, 1)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("asked for one event, got %d", len(first))
	}

	rest, err := sdk.ObjectEvents(ctx, first[0].Cursor(), 0)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
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

	uploadPinned(t, sdk, payload(1<<20))

	events, err := sdk.ObjectEvents(ctx, EventCursor{}, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one event to page past")
	}

	tail, err := sdk.ObjectEvents(ctx, events[len(events)-1].Cursor(), 0)
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
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
	if _, err := sdk.ObjectEvents(context.Background(), EventCursor{AfterID: id}, 0); err != nil {
		t.Fatalf("unknown cursor: %v", err)
	}
}

// TestObjectEventSummariesMatchEvents proves the summaries list the same
// events as ObjectEvents, in the same order, with each summary describing the
// object the full event carries.
func TestObjectEventSummariesMatchEvents(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	meta := []byte(`{"name":"kept"}`)
	kept := uploadPayload(t, sdk, payload(1<<20)).WithMetadata(meta)
	if err := sdk.PinObject(ctx, kept); err != nil {
		t.Fatalf("pin: %v", err)
	}
	removed := uploadPinned(t, sdk, payload(1<<19))
	if err := sdk.DeleteObject(ctx, removed.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}

	events, err := sdk.ObjectEvents(ctx, EventCursor{}, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	summaries, err := sdk.ObjectEventSummaries(ctx, EventCursor{}, 0)
	if err != nil {
		t.Fatalf("summaries: %v", err)
	}
	if len(summaries) != len(events) || len(events) != 2 {
		t.Fatalf("%d summaries for %d events, want 2 of each", len(summaries), len(events))
	}

	for i, e := range events {
		s := summaries[i]
		if s.ID != e.ID || s.Deleted != e.Deleted || !s.UpdatedAt.Equal(e.UpdatedAt) {
			t.Fatalf("summary %d is %v deleted=%v at %v, the event %v deleted=%v at %v",
				i, s.ID, s.Deleted, s.UpdatedAt, e.ID, e.Deleted, e.UpdatedAt)
		}
		if e.Object == nil {
			if s.Object != nil {
				t.Fatalf("the deletion of %v carried a summary", e.ID)
			}
			continue
		}
		if s.Object == nil {
			t.Fatalf("the write of %v carried no summary", e.ID)
		}
		if s.Object.ID != e.ID || s.Object.Size != e.Object.Size() {
			t.Fatalf("summary %v of %d bytes, the object %v of %d", s.Object.ID, s.Object.Size, e.ID, e.Object.Size())
		}
		if !bytes.Equal(s.Object.Metadata, e.Object.Metadata()) {
			t.Fatalf("summary metadata %q, the object's %q", s.Object.Metadata, e.Object.Metadata())
		}
		if !s.Object.CreatedAt.Equal(e.Object.CreatedAt()) {
			t.Fatalf("summary created at %v, the object at %v", s.Object.CreatedAt, e.Object.CreatedAt())
		}
	}

	var sawMeta bool
	for _, s := range summaries {
		if s.ID == kept.ID() {
			sawMeta = bytes.Equal(s.Object.Metadata, meta)
		}
	}
	if !sawMeta {
		t.Fatal("the kept object's metadata did not come through")
	}
}

// TestObjectEventSummariesCursor proves the summaries page with the same
// cursor, without replaying the event it names, and end on an empty page.
func TestObjectEventSummariesCursor(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	for i := range 3 {
		uploadPinned(t, sdk, payload((i+1)<<18))
	}

	first, err := sdk.ObjectEventSummaries(ctx, EventCursor{}, 1)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("asked for one event, got %d", len(first))
	}

	rest, err := sdk.ObjectEventSummaries(ctx, first[0].Cursor(), 0)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(rest) != 2 {
		t.Fatalf("two events remain after the first, got %d", len(rest))
	}
	for _, e := range rest {
		if e.ID == first[0].ID {
			t.Fatal("the cursor replayed the event it was built from")
		}
	}

	tail, err := sdk.ObjectEventSummaries(ctx, rest[len(rest)-1].Cursor(), 0)
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("paging past the last event returned %d more", len(tail))
	}
}

func ptr[T any](v T) *T { return &v }
