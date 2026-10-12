//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSharedObjectSummariesMatchObjects proves a summary describes the same
// object the full listing does, in the same order and with its metadata
// decrypted, so a page built from summaries shows what a page built from
// objects would. The slab layout is the only thing it leaves out, and that is
// the whole point: it is most of what an object weighs and a listing reads
// none of it.
func TestSharedObjectSummariesMatchObjects(t *testing.T) {
	net, sdk := transferSDK(t)
	ctx := context.Background()

	key, err := sdk.CreateSharingKey(ctx, "reader", time.Time{})
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	defer key.Close()

	const count = 3
	for i := range count {
		obj := uploadPayload(t, sdk, payload(64<<10+i))
		// Only the middle object carries metadata, so the test covers both a
		// summary that has some and one that has none.
		if i == 1 {
			obj = obj.WithMetadata([]byte(fmt.Sprintf(`{"name":"object %d"}`, i)))
		}
		if err := sdk.PinObject(ctx, obj); err != nil {
			t.Fatalf("pin %d: %v", i, err)
		}
		if err := sdk.ShareObject(ctx, key, obj); err != nil {
			t.Fatalf("share %d: %v", i, err)
		}
	}

	shared, err := net.SharedSDK(ctx, key.Export())
	if err != nil {
		t.Fatalf("shared sdk: %v", err)
	}
	defer shared.Close()

	objs, err := shared.Objects(ctx, 0, 0)
	if err != nil {
		t.Fatalf("objects: %v", err)
	}
	sums, err := shared.ObjectSummaries(ctx, 0, 0)
	if err != nil {
		t.Fatalf("summaries: %v", err)
	}
	if len(objs) != count {
		t.Fatalf("the recipient sees %d objects, want %d", len(objs), count)
	}
	if len(sums) != len(objs) {
		t.Fatalf("%d summaries for %d objects", len(sums), len(objs))
	}

	var withMetadata int
	for i, sum := range sums {
		obj := objs[i]
		if sum.ID != obj.ID() {
			t.Fatalf("summary %d is %x, the object there is %x", i, sum.ID, obj.ID())
		}
		if sum.Size != obj.Size() {
			t.Fatalf("summary %d reports %d bytes, the object reports %d", i, sum.Size, obj.Size())
		}
		if !sum.CreatedAt.Equal(obj.CreatedAt()) || !sum.UpdatedAt.Equal(obj.UpdatedAt()) {
			t.Fatalf("summary %d has %v/%v, the object has %v/%v",
				i, sum.CreatedAt, sum.UpdatedAt, obj.CreatedAt(), obj.UpdatedAt())
		}
		if sum.CreatedAt.IsZero() {
			t.Fatalf("summary %d has a zero createdAt, so the field is not being read", i)
		}
		if !bytes.Equal(sum.Metadata, obj.Metadata()) {
			t.Fatalf("summary %d has metadata %q, the object has %q", i, sum.Metadata, obj.Metadata())
		}
		if len(sum.Metadata) > 0 {
			withMetadata++
		}
	}
	// Equal and empty on both sides would satisfy the comparison above while
	// meaning the summary never decrypts anything.
	if withMetadata != 1 {
		t.Fatalf("%d summaries carry metadata, want 1", withMetadata)
	}
}

// TestSharedObjectSummariesAfterClose proves the listing takes the closed
// check rather than reading a handle Close already freed.
func TestSharedObjectSummariesAfterClose(t *testing.T) {
	net, sdk := transferSDK(t)
	ctx := context.Background()

	key, err := sdk.CreateSharingKey(ctx, "reader", time.Time{})
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	defer key.Close()

	shared, err := net.SharedSDK(ctx, key.Export())
	if err != nil {
		t.Fatalf("shared sdk: %v", err)
	}
	if err := shared.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := shared.ObjectSummaries(ctx, 0, 0); err == nil {
		t.Fatal("summaries after close returned no error")
	}
}
