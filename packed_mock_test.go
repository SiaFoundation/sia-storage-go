//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

// TestPackedUploadRoundTrip proves several small objects pack into shared slabs
// and each comes back out as its own downloadable object.
func TestPackedUploadRoundTrip(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	want := [][]byte{
		payload(4 << 10),
		payload(64 << 10),
		payload(1 << 20),
	}

	pu, err := sdk.PackedUpload(ctx, UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	defer pu.Close()

	for i, data := range want {
		n, err := pu.Add(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if n != uint64(len(data)) {
			t.Fatalf("add %d packed %d bytes of %d", i, n, len(data))
		}
	}

	objs, err := pu.Finalize()
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	defer func() {
		for _, o := range objs {
			o.Close()
		}
	}()
	if len(objs) != len(want) {
		t.Fatalf("finalize returned %d objects for %d adds", len(objs), len(want))
	}

	for i, obj := range objs {
		if obj.Size() != uint64(len(want[i])) {
			t.Fatalf("object %d is %d bytes, added %d", i, obj.Size(), len(want[i]))
		}
		dl, err := sdk.Download(ctx, obj, DownloadOptions{})
		if err != nil {
			t.Fatalf("download %d: %v", i, err)
		}
		got, err := io.ReadAll(dl)
		dl.Close()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("object %d did not round trip", i)
		}
	}
}

// TestPackedUploadSharesSlabs is the reason the feature exists: objects too
// small to fill a slab each should not pay for a slab each.
func TestPackedUploadSharesSlabs(t *testing.T) {
	net, sdk := transferSDK(t)
	ctx := context.Background()

	pu, err := sdk.PackedUpload(ctx, UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	defer pu.Close()

	const objects = 5
	for range objects {
		if _, err := pu.Add(bytes.NewReader(payload(4 << 10))); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	objs, err := pu.Finalize()
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	for _, o := range objs {
		o.Close()
	}

	if slabs := net.PinnedSlabs(); slabs >= objects {
		t.Fatalf("%d tiny objects used %d slabs, so they were not packed", objects, slabs)
	}
}

// TestPackedUploadAccounting proves the size accessors track what has been
// packed, which is how a caller decides when to finalize a batch.
func TestPackedUploadAccounting(t *testing.T) {
	_, sdk := transferSDK(t)

	pu, err := sdk.PackedUpload(context.Background(), UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	defer pu.Close()

	optimal := pu.OptimalDataSize()
	if optimal == 0 {
		t.Fatal("the optimal data size is zero")
	}
	if pu.Length() != 0 {
		t.Fatalf("a fresh packed upload already holds %d bytes", pu.Length())
	}
	before := pu.Remaining()
	if before == 0 {
		t.Fatal("a fresh packed upload has no room")
	}

	const size = 64 << 10
	if _, err := pu.Add(bytes.NewReader(payload(size))); err != nil {
		t.Fatalf("add: %v", err)
	}
	if pu.Length() < size {
		t.Fatalf("length is %d after packing %d bytes", pu.Length(), size)
	}
	if pu.Remaining() >= before {
		t.Fatalf("remaining went from %d to %d after an add", before, pu.Remaining())
	}
}

// TestPackedUploadEmptyFinalize proves finalizing without adding anything is a
// clean no op rather than an error or a stray object.
func TestPackedUploadEmptyFinalize(t *testing.T) {
	_, sdk := transferSDK(t)

	pu, err := sdk.PackedUpload(context.Background(), UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	defer pu.Close()

	objs, err := pu.Finalize()
	if err != nil {
		t.Fatalf("finalize with no adds: %v", err)
	}
	for _, o := range objs {
		o.Close()
	}
	if len(objs) != 0 {
		t.Fatalf("finalizing an empty upload produced %d objects", len(objs))
	}
}

// TestPackedUploadCloseSemantics proves the handle refuses use after it is
// retired, whether by Close or by Finalize, and never frees twice.
func TestPackedUploadCloseSemantics(t *testing.T) {
	_, sdk := transferSDK(t)
	ctx := context.Background()

	pu, err := sdk.PackedUpload(ctx, UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	if _, err := pu.Add(bytes.NewReader(payload(4 << 10))); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := pu.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := pu.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := pu.Add(bytes.NewReader(payload(1 << 10))); !errors.Is(err, errClosed) {
		t.Fatalf("add after close returned %v, want errClosed", err)
	}
	if _, err := pu.Finalize(); !errors.Is(err, errClosed) {
		t.Fatalf("finalize after close returned %v, want errClosed", err)
	}

	// The same again, retired by Finalize rather than Close.
	other, err := sdk.PackedUpload(ctx, UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	defer other.Close()
	if _, err := other.Add(bytes.NewReader(payload(4 << 10))); err != nil {
		t.Fatalf("add: %v", err)
	}
	objs, err := other.Finalize()
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	for _, o := range objs {
		o.Close()
	}
	if _, err := other.Finalize(); !errors.Is(err, errClosed) {
		t.Fatalf("second finalize returned %v, want errClosed", err)
	}
	if err := other.Close(); err != nil {
		t.Fatalf("close after finalize: %v", err)
	}
}

// TestPackedUploadAddReaderError proves a failing reader does not leave an add
// in progress, which would block Finalize with an invalid state.
func TestPackedUploadAddReaderError(t *testing.T) {
	_, sdk := transferSDK(t)

	pu, err := sdk.PackedUpload(context.Background(), UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	defer pu.Close()

	want := errors.New("reader blew up")
	if _, err := pu.Add(io.MultiReader(bytes.NewReader(payload(1<<10)), errReader{want})); !errors.Is(err, want) {
		t.Fatalf("add with a failing reader returned %v", err)
	}

	// The handle has to still be usable, which it is not if the add was left
	// open.
	objs, err := pu.Finalize()
	if err != nil {
		t.Fatalf("finalize after a failed add: %v", err)
	}
	for _, o := range objs {
		o.Close()
	}
	// The failed add must contribute nothing. Finishing it instead of
	// aborting produced a short but structurally valid object here, which a
	// caller had no way to tell apart from a complete one.
	if len(objs) != 0 {
		t.Errorf("a failed Add contributed %d object(s) to Finalize, want 0", len(objs))
	}
}

// TestPackedUploadSurvivesAFailedAdd proves a failed add costs only its own
// object: the adds around it still come back, in order, and still round trip.
func TestPackedUploadSurvivesAFailedAdd(t *testing.T) {
	_, sdk := transferSDK(t)

	pu, err := sdk.PackedUpload(context.Background(), UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	defer pu.Close()

	first := payload(4 << 10)
	if _, err := pu.Add(bytes.NewReader(first)); err != nil {
		t.Fatalf("first add: %v", err)
	}

	boom := errors.New("reader blew up")
	r := io.MultiReader(bytes.NewReader(payload(2<<10)), errReader{boom})
	if _, err := pu.Add(r); !errors.Is(err, boom) {
		t.Fatalf("second add returned %v, want the reader error", err)
	}

	third := payload(8 << 10)
	if _, err := pu.Add(bytes.NewReader(third)); err != nil {
		t.Fatalf("third add after a failed one: %v", err)
	}

	objs, err := pu.Finalize()
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	defer func() {
		for _, o := range objs {
			o.Close()
		}
	}()
	if len(objs) != 2 {
		t.Fatalf("expected the two successful adds, got %d object(s)", len(objs))
	}
	if objs[0].Size() != uint64(len(first)) {
		t.Errorf("first object is %d bytes, want %d", objs[0].Size(), len(first))
	}
	if objs[1].Size() != uint64(len(third)) {
		t.Errorf("second object is %d bytes, want %d", objs[1].Size(), len(third))
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// TestPackedUploadQueriesAfterClose proves the three query methods do not read
// a handle that Close already freed. They were the only methods on the type
// that took neither the mutex nor the done flag, so before the guard this test
// killed the process with SIGBUS rather than failing.
func TestPackedUploadQueriesAfterClose(t *testing.T) {
	_, sdk := transferSDK(t)

	pu, err := sdk.PackedUpload(context.Background(), UploadOptions{})
	if err != nil {
		t.Fatalf("packed start: %v", err)
	}
	if pu.OptimalDataSize() == 0 {
		t.Fatal("a live upload should report a non-zero slab size")
	}
	if err := pu.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if n := pu.Remaining(); n != 0 {
		t.Errorf("Remaining after Close = %d, want 0", n)
	}
	if n := pu.Length(); n != 0 {
		t.Errorf("Length after Close = %d, want 0", n)
	}
	if n := pu.OptimalDataSize(); n != 0 {
		t.Errorf("OptimalDataSize after Close = %d, want 0", n)
	}
}
