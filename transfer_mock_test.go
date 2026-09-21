//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// The default redundancy is 10 data and 20 parity shards, so a slab needs 30
// distinct hosts. Anything smaller fails before a byte moves.
const transferHosts = 40

// payloadSize spans several 4 MiB sectors, so the erasure coder and the slab
// pipeline actually run rather than the whole payload landing in one shard.
const payloadSize = 9 << 20

// transferSDK brings up a mock network large enough for the default redundancy.
func transferSDK(t *testing.T) (*MockNetwork, *SDK) {
	t.Helper()
	net := NewMockNetwork(transferHosts)
	t.Cleanup(func() { net.Close() })

	var seed [32]byte
	seed[0] = 9
	sdk, err := net.SDK(context.Background(), seed)
	if err != nil {
		t.Fatalf("mock sdk: %v", err)
	}
	t.Cleanup(func() { sdk.Close() })
	return net, sdk
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// uploadPayload runs a whole upload through io.Copy and returns the object.
func uploadPayload(t *testing.T, sdk *SDK, data []byte, opts UploadOptions) *Object {
	t.Helper()
	up, err := sdk.Upload(context.Background(), NewObject(), opts)
	if err != nil {
		t.Fatalf("upload start: %v", err)
	}
	defer up.Close()

	if n, err := io.Copy(up, bytes.NewReader(data)); err != nil {
		t.Fatalf("upload copy: %v", err)
	} else if n != int64(len(data)) {
		t.Fatalf("copied %d bytes of %d", n, len(data))
	}
	obj, err := up.Finish()
	if err != nil {
		t.Fatalf("upload finish: %v", err)
	}
	return obj
}

// TestTransferRoundTrip is the whole point of the layer: bytes in through
// io.Copy, the same bytes out, with real erasure coding and encryption in
// between and only the network faked.
func TestTransferRoundTrip(t *testing.T) {
	net, sdk := transferSDK(t)
	want := payload(payloadSize)

	obj := uploadPayload(t, sdk, want, UploadOptions{})
	defer obj.Close()

	if obj.Size() != uint64(len(want)) {
		t.Fatalf("object size is %d, want %d", obj.Size(), len(want))
	}
	if obj.EncodedSize() <= obj.Size() {
		t.Fatalf("encoded size %d should exceed the logical size %d", obj.EncodedSize(), obj.Size())
	}
	if net.PinnedSlabs() == 0 {
		t.Fatal("the upload pinned no slabs")
	}

	dl, err := sdk.Download(context.Background(), obj, DownloadOptions{})
	if err != nil {
		t.Fatalf("download start: %v", err)
	}
	defer dl.Close()

	got, err := io.ReadAll(dl)
	if err != nil {
		t.Fatalf("download read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("downloaded %d bytes, wanted %d, and they differ", len(got), len(want))
	}
}

// TestTransferRange proves the offset and length options reach the native side,
// which is what a range request over this SDK depends on.
func TestTransferRange(t *testing.T) {
	_, sdk := transferSDK(t)
	want := payload(payloadSize)

	obj := uploadPayload(t, sdk, want, UploadOptions{})
	defer obj.Close()

	const offset, length = 1 << 20, 64 << 10
	n := uint64(length)
	dl, err := sdk.Download(context.Background(), obj, DownloadOptions{
		Offset: offset,
		Length: &n,
	})
	if err != nil {
		t.Fatalf("download start: %v", err)
	}
	defer dl.Close()

	got, err := io.ReadAll(dl)
	if err != nil {
		t.Fatalf("download read: %v", err)
	}
	if !bytes.Equal(got, want[offset:offset+length]) {
		t.Fatalf("range returned %d bytes and they do not match the source", len(got))
	}
}

// TestTransferRedundancyOption proves the erasure coding option reaches the
// native side. The encoded size is the only observable, and since a payload
// smaller than one slab pads every shard to a whole sector, fewer total shards
// has to mean a smaller object.
func TestTransferRedundancyOption(t *testing.T) {
	_, sdk := transferSDK(t)
	want := payload(payloadSize)

	// The default is 10 of 30. 5 of 20 is a valid alternative that clears the
	// SDK's recovery probability floor and uses ten fewer shards.
	fewer := uploadPayload(t, sdk, want, UploadOptions{DataShards: 5, ParityShards: 15})
	defer fewer.Close()
	def := uploadPayload(t, sdk, want, UploadOptions{})
	defer def.Close()

	if fewer.Size() != def.Size() {
		t.Fatalf("the same payload produced sizes %d and %d", fewer.Size(), def.Size())
	}
	if fewer.EncodedSize() >= def.EncodedSize() {
		t.Fatalf("20 shards encoded to %d and 30 shards to %d, so the option did not take",
			fewer.EncodedSize(), def.EncodedSize())
	}
}

// TestTransferProgress proves the shard callback survives the crossing from a
// Rust thread, and that the running total it carries reaches the payload size.
func TestTransferProgress(t *testing.T) {
	_, sdk := transferSDK(t)
	want := payload(payloadSize)

	var mu sync.Mutex
	var events int
	var transferred uint64
	obj := uploadPayload(t, sdk, want, UploadOptions{
		OnShard: func(p ShardProgress) {
			mu.Lock()
			defer mu.Unlock()
			events++
			if p.Transferred > transferred {
				transferred = p.Transferred
			}
		},
	})
	defer obj.Close()

	mu.Lock()
	defer mu.Unlock()
	if events == 0 {
		t.Fatal("no shard progress events arrived")
	}
	if transferred < uint64(len(want)) {
		t.Fatalf("progress reported %d bytes for a %d byte payload", transferred, len(want))
	}
}

// TestTransferMissingSectors proves a download that cannot be satisfied reports
// the sentinel rather than a bare message, which is what callers match on.
func TestTransferMissingSectors(t *testing.T) {
	net, sdk := transferSDK(t)

	obj := uploadPayload(t, sdk, payload(payloadSize), UploadOptions{})
	defer obj.Close()

	net.ClearSectors()

	dl, err := sdk.Download(context.Background(), obj, DownloadOptions{})
	if err != nil {
		// Failing at start is an acceptable shape for the same condition.
		if !errors.Is(err, ErrNotEnoughShards) {
			t.Fatalf("download start failed with %v, want ErrNotEnoughShards", err)
		}
		return
	}
	defer dl.Close()

	if _, err := io.ReadAll(dl); !errors.Is(err, ErrNotEnoughShards) {
		t.Fatalf("read after clearing sectors returned %v, want ErrNotEnoughShards", err)
	}
}

// TestUploadCloseWithoutFinish proves abandoning an upload is safe, that Close
// is idempotent, and that the handle refuses use afterwards rather than
// reaching into freed memory.
func TestUploadCloseWithoutFinish(t *testing.T) {
	_, sdk := transferSDK(t)

	up, err := sdk.Upload(context.Background(), NewObject(), UploadOptions{})
	if err != nil {
		t.Fatalf("upload start: %v", err)
	}
	if _, err := up.Write(payload(1 << 20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := up.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := up.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := up.Write([]byte("x")); !errors.Is(err, errClosed) {
		t.Fatalf("write after close returned %v, want errClosed", err)
	}
	if _, err := up.Finish(); !errors.Is(err, errClosed) {
		t.Fatalf("finish after close returned %v, want errClosed", err)
	}
}

// TestUploadCloseAfterFinish proves the deferred Close that every caller will
// write does not free the handle a second time.
func TestUploadCloseAfterFinish(t *testing.T) {
	_, sdk := transferSDK(t)

	up, err := sdk.Upload(context.Background(), NewObject(), UploadOptions{})
	if err != nil {
		t.Fatalf("upload start: %v", err)
	}
	if _, err := up.Write(payload(1 << 20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	obj, err := up.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	defer obj.Close()

	if err := up.Close(); err != nil {
		t.Fatalf("close after finish: %v", err)
	}
	if _, err := up.Finish(); !errors.Is(err, errClosed) {
		t.Fatalf("second finish returned %v, want errClosed", err)
	}
}

// TestDownloadCloseUnblocksRead covers the one ordering rule the C header
// states outright, that a download must be cancelled before it is freed and
// never freed underneath a blocked read. Close has to enforce it, because a
// caller reading on another goroutine cannot.
func TestDownloadCloseUnblocksRead(t *testing.T) {
	_, sdk := transferSDK(t)

	obj := uploadPayload(t, sdk, payload(payloadSize), UploadOptions{})
	defer obj.Close()

	dl, err := sdk.Download(context.Background(), obj, DownloadOptions{})
	if err != nil {
		t.Fatalf("download start: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, dl)
	}()

	if err := dl.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	<-done

	if err := dl.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := dl.Read(make([]byte, 1)); !errors.Is(err, errClosed) {
		t.Fatalf("read after close returned %v, want errClosed", err)
	}
}

// TestUploadCancelledContext proves the context reaches the transfer rather
// than only the call that starts it.
func TestUploadCancelledContext(t *testing.T) {
	_, sdk := transferSDK(t)

	ctx, cancel := context.WithCancel(context.Background())
	up, err := sdk.Upload(ctx, NewObject(), UploadOptions{})
	if err != nil {
		t.Fatalf("upload start: %v", err)
	}
	defer up.Close()

	cancel()
	// Either the write or the finish observes it, depending on how far the
	// transfer got, but the object must not come back.
	_, werr := up.Write(payload(payloadSize))
	_, ferr := up.Finish()
	if werr == nil && ferr == nil {
		t.Fatal("a cancelled context produced a completed upload")
	}
}

// TestUploadEmptyWriteIsANoop keeps io.Copy's habit of handing over empty
// buffers from reaching the boundary, where a nil data pointer would be
// undefined.
func TestUploadEmptyWriteIsANoop(t *testing.T) {
	_, sdk := transferSDK(t)

	up, err := sdk.Upload(context.Background(), NewObject(), UploadOptions{})
	if err != nil {
		t.Fatalf("upload start: %v", err)
	}
	defer up.Close()

	if n, err := up.Write(nil); n != 0 || err != nil {
		t.Fatalf("empty write returned %d and %v", n, err)
	}
}

// TestSlowHostsAffectTransfers covers the mock's degraded host controls, which
// are the only way to reach host selection, racing and timeout behaviour from
// Go. Every benchmark so far ran with uniformly fast hosts, so none of that
// code path has ever been exercised here.
//
// The assertions are deliberately one-sided. A delay applied to every host is
// a floor the upload cannot beat, which holds on any machine; a ratio against
// a warm baseline would only hold on an idle one.
func TestSlowHostsAffectTransfers(t *testing.T) {
	net, sdk := transferSDK(t)

	// The first upload pays for contracts and connections, which swamps the
	// delay. Measuring it would say more about warmup than about hosts.
	uploadPayload(t, sdk, payload(payloadSize), UploadOptions{})

	timed := func() time.Duration {
		start := time.Now()
		uploadPayload(t, sdk, payload(payloadSize), UploadOptions{})
		return time.Since(start)
	}

	warm := timed()

	// Every host, so selection cannot route around the delay. With only some
	// hosts slow the uploader is free to avoid them, which is the behaviour
	// working rather than the control failing.
	const delay = 300 * time.Millisecond
	net.SetSlowHosts(transferHosts, delay)
	slow := timed()
	if slow < delay {
		t.Errorf("every host delays by %v, so the upload cannot take %v", delay, slow)
	}

	net.ResetSlowHosts()
	if after := timed(); after >= slow {
		t.Errorf("reset left the upload at %v, no better than the %v with slow hosts", after, slow)
	}

	t.Logf("warm %v, all hosts slow %v", warm, slow)
}

// TestSlowHostsBoundsAreHarmless proves the count is clamped, so a caller
// asking for more hosts than exist marks all of them rather than panicking on
// the Rust side, and that the controls are inert once the network is closed.
func TestSlowHostsBoundsAreHarmless(t *testing.T) {
	net, _ := transferSDK(t)
	net.SetSlowHosts(transferHosts*10, time.Millisecond) // clamped
	net.SetSlowHosts(0, time.Millisecond)                // marks nothing
	net.SetSlowHosts(-1, time.Millisecond)               // rejected before crossing
	net.ResetSlowHosts()

	closed := NewMockNetwork(2)
	closed.Close()
	closed.SetSlowHosts(1, time.Millisecond) // must not touch a freed handle
	closed.ResetSlowHosts()
}
