//go:build siastorage_mock

package siastorage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"lukechampine.com/frand"
)

// newTestData returns a deterministic pseudo-random buffer.
func newTestData(t *testing.T, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	frand.Read(buf)
	return buf
}

func newTestMock(t *testing.T) *mockSDK {
	t.Helper()
	var seed [32]byte
	frand.Read(seed[:])
	return newMockSDK(60, seed)
}

func TestUploadDownload(t *testing.T) {
	m := newTestMock(t)
	data := newTestData(t, 5<<20)

	var mu sync.Mutex
	var uploadedShards int
	obj := NewEmptyObject()
	err := m.Upload(t.Context(), &obj, bytes.NewReader(data), WithUploadProgress(func(p ShardProgress) {
		mu.Lock()
		defer mu.Unlock()
		uploadedShards++
		if p.HostKey == ([32]byte{}) {
			t.Error("progress host key is zero")
		}
		if p.ShardSize == 0 {
			t.Error("progress shard size is zero")
		}
	}))
	if err != nil {
		t.Fatal(err)
	}

	if obj.Size() != uint64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), obj.Size())
	}
	if obj.ID() == ([32]byte{}) {
		t.Fatal("expected non-zero object ID")
	}
	if obj.EncodedSize() == 0 || obj.EncodedSize() < obj.Size() {
		t.Fatalf("unexpected encoded size %d", obj.EncodedSize())
	}
	mu.Lock()
	if uploadedShards == 0 {
		t.Fatal("expected upload progress callbacks")
	}
	mu.Unlock()

	var downloadedShards int
	rc, err := m.Download(obj, WithDownloadProgress(func(_ ShardProgress) {
		mu.Lock()
		defer mu.Unlock()
		downloadedShards++
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded data mismatch: got %d bytes", len(got))
	}
	mu.Lock()
	if downloadedShards == 0 {
		t.Fatal("expected download progress callbacks")
	}
	mu.Unlock()
}

func TestUploadAppend(t *testing.T) {
	m := newTestMock(t)
	first := newTestData(t, 1<<20)
	second := newTestData(t, 2<<20)

	obj := NewEmptyObject()
	if err := m.Upload(t.Context(), &obj, bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	}
	firstID := obj.ID()
	if err := m.Upload(t.Context(), &obj, bytes.NewReader(second)); err != nil {
		t.Fatal(err)
	}
	if obj.ID() == firstID {
		t.Fatal("appending must change the object ID")
	}
	if obj.Size() != uint64(len(first)+len(second)) {
		t.Fatalf("expected size %d, got %d", len(first)+len(second), obj.Size())
	}

	rc, err := m.Download(obj)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append(append([]byte(nil), first...), second...)) {
		t.Fatal("downloaded data mismatch")
	}
}

func TestDownloadRange(t *testing.T) {
	m := newTestMock(t)
	data := newTestData(t, 3<<20)

	obj := NewEmptyObject()
	if err := m.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name           string
		offset, length uint64
		want           []byte
	}{
		{"interior", 1 << 19, 1 << 20, data[1<<19 : (1<<19)+(1<<20)]},
		{"clamped", uint64(len(data)) - 100, 1 << 20, data[len(data)-100:]},
		{"pastEnd", uint64(len(data)) + 1, 10, nil},
		{"zeroLength", 0, 0, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc, err := m.Download(obj, WithDownloadRange(tt.offset, tt.length))
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("expected %d bytes, got %d", len(tt.want), len(got))
			}
		})
	}
}

func TestDownloadCloseInterrupts(t *testing.T) {
	m := newTestMock(t)
	data := newTestData(t, 1<<20)

	obj := NewEmptyObject()
	if err := m.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	rc, err := m.Download(obj)
	if err != nil {
		t.Fatal(err)
	}
	// read a little, close, then confirm reads fail
	buf := make([]byte, 1024)
	if _, err := io.ReadFull(rc, buf); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Read(buf); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("expected ErrClosedPipe, got %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal("second close should be a no-op")
	}
}

func TestUploadInvalidRedundancy(t *testing.T) {
	m := newTestMock(t)
	obj := NewEmptyObject()
	err := m.Upload(t.Context(), &obj, strings.NewReader("hello"), WithRedundancy(10, 1))
	if err == nil || !strings.Contains(err.Error(), "invalid options") {
		t.Fatalf("expected invalid options error, got %v", err)
	}
}

func TestUploadPacked(t *testing.T) {
	m := newTestMock(t)
	first := newTestData(t, 1<<20)
	second := newTestData(t, 1<<19)

	up, err := m.UploadPacked()
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()

	if up.OptimalDataSize() == 0 {
		t.Fatal("expected non-zero optimal data size")
	}
	if up.Remaining() != up.OptimalDataSize() {
		t.Fatalf("expected remaining %d, got %d", up.OptimalDataSize(), up.Remaining())
	}

	if n, err := up.Add(t.Context(), bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	} else if n != int64(len(first)) {
		t.Fatalf("expected %d bytes, got %d", len(first), n)
	}
	if _, err := up.Add(t.Context(), bytes.NewReader(nil)); !errors.Is(err, ErrEmptyObject) {
		t.Fatalf("expected ErrEmptyObject, got %v", err)
	}
	if n, err := up.Add(t.Context(), bytes.NewReader(second)); err != nil {
		t.Fatal(err)
	} else if n != int64(len(second)) {
		t.Fatalf("expected %d bytes, got %d", len(second), n)
	}
	if up.Length() != int64(len(first)+len(second)) {
		t.Fatalf("expected length %d, got %d", len(first)+len(second), up.Length())
	}

	objects, err := up.Finalize(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objects))
	}
	if objects[0].ID() == objects[1].ID() {
		t.Fatal("expected distinct object IDs")
	}

	for i, want := range [][]byte{first, second} {
		rc, err := m.Download(objects[i])
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("object %d data mismatch", i)
		}
	}

	// adding after finalize fails
	if _, err := up.Add(t.Context(), bytes.NewReader(first)); !errors.Is(err, ErrUploadFinalized) {
		t.Fatalf("expected ErrUploadFinalized, got %v", err)
	}
	// closing and re-adding fails
	if err := up.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := up.Add(t.Context(), bytes.NewReader(first)); !errors.Is(err, ErrUploadClosed) {
		t.Fatalf("expected ErrUploadClosed, got %v", err)
	}
}

func TestUploadPackedReaderError(t *testing.T) {
	m := newTestMock(t)
	boom := errors.New("boom")

	up, err := m.UploadPacked()
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()

	// a failing reader must not register an object and must leave the
	// upload usable
	r := io.MultiReader(bytes.NewReader(newTestData(t, 1024)), &errReader{err: boom})
	if _, err := up.Add(t.Context(), r); !errors.Is(err, boom) {
		t.Fatalf("expected reader error, got %v", err)
	}

	data := newTestData(t, 2048)
	if _, err := up.Add(t.Context(), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	objects, err := up.Finalize(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objects))
	}
	rc, err := m.Download(objects[0])
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded data mismatch")
	}
}

type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }

// TestLargeRoundtrip exercises a large streaming transfer; enable with
// SIA_TEST_LARGE=1.
func TestLargeRoundtrip(t *testing.T) {
	if os.Getenv("SIA_TEST_LARGE") == "" {
		t.Skip("set SIA_TEST_LARGE=1 to run")
	}
	m := newTestMock(t)
	const size = 512 << 20

	src := frand.NewCustom(make([]byte, 32), 1024, 12)
	sum := sha256.New()
	obj := NewEmptyObject()
	if err := m.Upload(t.Context(), &obj, io.TeeReader(io.LimitReader(src, size), sum), WithMaxBufferedSlabs(2)); err != nil {
		t.Fatal(err)
	}
	want := sum.Sum(nil)

	rc, err := m.Download(obj)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got := sha256.New()
	if n, err := io.Copy(got, rc); err != nil {
		t.Fatal(err)
	} else if n != size {
		t.Fatalf("expected %d bytes, got %d", size, n)
	}
	if !bytes.Equal(got.Sum(nil), want) {
		t.Fatal("data mismatch")
	}
}
