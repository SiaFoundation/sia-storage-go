package siastorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type countWriter struct {
	count int

	w io.Writer
}

func (c *countWriter) Write(p []byte) (int, error) {
	c.count++
	return c.w.Write(p)
}

func TestRoundtripCount(t *testing.T) {
	sdk, _ := newTestSDK(t, 50, zaptest.NewLogger(t))
	defer sdk.Close()

	// 1 MB
	data := frand.Bytes(1 << 20)
	obj := NewEmptyObject()
	err := sdk.Upload(context.Background(), &obj, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(obj.Slabs()) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(obj.Slabs()))
	} else if obj.Slabs()[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), obj.Slabs()[0].Length)
	}

	buf := bytes.NewBuffer(nil)
	cw := &countWriter{w: buf}
	if err := sdk.Download(context.Background(), cw, obj); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}
	t.Logf("Downloaded: %d bytes, Write calls: %d", buf.Len(), cw.count)
}

func TestUpload(t *testing.T) {
	data := frand.Bytes(4096)

	t.Run("timeout", func(t *testing.T) {
		// use only 25 hosts so there are not enough for a full slab
		sdk, _ := newTestSDK(t, 25, zaptest.NewLogger(t))
		defer sdk.Close()

		obj := NewEmptyObject()
		err := sdk.Upload(context.Background(), &obj, bytes.NewReader(data))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("slow", func(t *testing.T) {
		sdk, hosts := newTestSDK(t, 50, zaptest.NewLogger(t))
		defer sdk.Close()

		// make most of the hosts slow, but not enough to fail.
		// progressive timeout starts at 15s so 1s delay succeeds.
		hosts.SetSlowHosts(t, 20, time.Second)
		obj := NewEmptyObject()
		err := sdk.Upload(context.Background(), &obj, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		} else if len(obj.Slabs()) != 1 {
			t.Fatalf("expected 1 slab, got %d", len(obj.Slabs()))
		}
	})

	t.Run("all slow", func(t *testing.T) {
		sdk, hosts := newTestSDK(t, 50, zaptest.NewLogger(t))
		defer sdk.Close()

		hosts.SetSlowHosts(t, 50, 2*time.Second)

		obj := NewEmptyObject()
		err := sdk.Upload(context.Background(), &obj, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		} else if len(obj.Slabs()) != 1 {
			t.Fatalf("expected 1 slab, got %d", len(obj.Slabs()))
		}
	})

	t.Run("flaky hosts", func(t *testing.T) {
		sdk, hosts := newTestSDK(t, 30, zaptest.NewLogger(t))
		defer sdk.Close()

		// 10 of 30 hosts fail their first write but succeed on retry.
		// the upload should complete because failed hosts are requeued
		// for other shards via Retry.
		hosts.SetFlakyHosts(t, 10, 1)

		obj := NewEmptyObject()
		err := sdk.Upload(context.Background(), &obj, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		} else if len(obj.Slabs()) != 1 {
			t.Fatalf("expected 1 slab, got %d", len(obj.Slabs()))
		}
	})

	t.Run("no hosts", func(t *testing.T) {
		sdk, _ := newTestSDK(t, 0, zaptest.NewLogger(t))
		defer sdk.Close()

		obj := NewEmptyObject()
		err := sdk.Upload(context.Background(), &obj, bytes.NewReader(data))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestResumableUpload(t *testing.T) {
	sdk, _ := newTestSDK(t, 50, zaptest.NewLogger(t))
	defer sdk.Close()

	obj := NewEmptyObject()
	data := frand.Bytes(5000)

	for _, part := range [][]byte{data[:100], data[100:3000], data[3000:]} {
		err := sdk.Upload(context.Background(), &obj, bytes.NewReader(part))
		if err != nil {
			t.Fatal(err)
		}
	}

	buf := bytes.NewBuffer(nil)
	if err := sdk.Download(t.Context(), buf, obj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}
}

func TestDownload(t *testing.T) {
	sdk, hosts := newTestSDK(t, 30, zaptest.NewLogger(t))
	defer sdk.Close()

	slabSize := uint64(proto.SectorSize) * 10
	dataSize := slabSize * 3 // 3 slabs
	data := frand.Bytes(int(dataSize))

	obj := NewEmptyObject()
	err := sdk.Upload(context.Background(), &obj, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	}

	err = sdk.PinObject(t.Context(), obj)
	if err != nil {
		t.Fatalf("failed to pin object: %v", err)
	}

	sharedURL, err := sdk.CreateSharedObjectURL(t.Context(), obj.ID(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("failed to create shared object URL: %v", err)
	}

	t.Run("full", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		if err = sdk.Download(context.Background(), buf, obj); err != nil {
			t.Fatalf("failed to download: %v", err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}

		buf.Reset()
		if err := sdk.DownloadSharedObject(t.Context(), buf, sharedURL); err != nil {
			t.Fatalf("failed to download shared object: %v", err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}
	})

	t.Run("ranges", func(t *testing.T) {
		randomOffsetLength := func() [2]uint64 {
			offset := frand.Uint64n(dataSize - 1)
			length := frand.Uint64n(dataSize - offset + 1)
			return [2]uint64{offset, length}
		}

		cases := [][2]uint64{
			{0, proto.SectorSize},
			{proto.SectorSize, proto.SectorSize},
			{proto.LeafSize, proto.LeafSize},
			{proto.LeafSize + 1, proto.LeafSize / 2},            // within a leaf
			{proto.LeafSize + proto.LeafSize/2, proto.LeafSize}, // across leaves
			{slabSize / 2, 2 * slabSize},                        // across slabs
			{dataSize - proto.SectorSize, proto.SectorSize},
			{dataSize - proto.LeafSize, proto.LeafSize},
			{dataSize, 0},
		}
		for range 10 {
			cases = append(cases, randomOffsetLength())
		}

		for _, c := range cases {
			buf := bytes.NewBuffer(nil)
			if err = sdk.Download(context.Background(), buf, obj, WithDownloadRange(c[0], c[1])); err != nil {
				t.Fatalf("failed to download: %v", err)
			} else if !bytes.Equal(buf.Bytes(), data[c[0]:c[0]+c[1]]) {
				t.Fatal("data mismatch")
			}

			buf.Reset()
			if err := sdk.DownloadSharedObject(t.Context(), buf, sharedURL, WithDownloadRange(c[0], c[1])); err != nil {
				t.Fatalf("failed to download shared object: %v", err)
			} else if !bytes.Equal(buf.Bytes(), data[c[0]:c[0]+c[1]]) {
				t.Fatal("data mismatch")
			}
		}

		// ranges that extend past EOF should be clamped.
		buf := bytes.NewBuffer(nil)
		if err := sdk.Download(context.Background(), buf, obj, WithDownloadRange(dataSize-10, 100)); err != nil {
			t.Fatalf("failed to clamp range to EOF: %v", err)
		} else if !bytes.Equal(buf.Bytes(), data[dataSize-10:]) {
			t.Fatal("data mismatch")
		}
		buf.Reset()
		if err := sdk.DownloadSharedObject(t.Context(), buf, sharedURL, WithDownloadRange(dataSize-10, 100)); err != nil {
			t.Fatalf("failed to clamp shared range to EOF: %v", err)
		} else if !bytes.Equal(buf.Bytes(), data[dataSize-10:]) {
			t.Fatal("data mismatch")
		}

		// offsets at or beyond EOF should return no data.
		buf.Reset()
		if err := sdk.Download(context.Background(), buf, obj, WithDownloadRange(dataSize, 1)); err != nil {
			t.Fatalf("expected empty EOF download, got %v", err)
		} else if buf.Len() != 0 {
			t.Fatalf("expected empty EOF download, got %d bytes", buf.Len())
		}
		buf.Reset()
		if err := sdk.DownloadSharedObject(t.Context(), buf, sharedURL, WithDownloadRange(dataSize+1, 1)); err != nil {
			t.Fatalf("expected empty shared EOF download, got %v", err)
		} else if buf.Len() != 0 {
			t.Fatalf("expected empty shared EOF download, got %d bytes", buf.Len())
		}
	})

	t.Run("timeout", func(t *testing.T) {
		hosts.ResetSlowHosts()
		// make enough hosts timeout to fail to download
		hosts.SetSlowHosts(t, 21, time.Second)
		buf := bytes.NewBuffer(nil)
		err = sdk.Download(context.Background(), buf, obj, WithDownloadHostTimeout(200*time.Millisecond))
		if !errors.Is(err, ErrNotEnoughShards) {
			t.Fatalf("expected ErrNotEnoughShards, got %v", err)
		}
	})

	t.Run("slow", func(t *testing.T) {
		hosts.ResetSlowHosts()
		// make most of the hosts timeout
		hosts.SetSlowHosts(t, 20, time.Second)
		buf := bytes.NewBuffer(nil)
		err = sdk.Download(context.Background(), buf, obj, WithDownloadHostTimeout(200*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}
	})
}

func TestE2E(t *testing.T) {
	sdk, _ := newTestSDK(t, 30, zaptest.NewLogger(t))
	defer sdk.Close()

	assertShareable := func(obj Object, data []byte) {
		t.Helper()

		// assert we can download the object
		buf := bytes.NewBuffer(nil)
		if err := sdk.PinObject(t.Context(), obj); err != nil {
			t.Fatalf("failed to pin object: %v", err)
		} else if _, err := sdk.Object(t.Context(), obj.ID()); err != nil {
			t.Fatalf("failed to get object: %v", err)
		} else if err := sdk.Download(t.Context(), buf, obj); err != nil {
			t.Fatalf("failed to download: %v", err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}

		// assert we can share the object
		sharedURL, err := sdk.CreateSharedObjectURL(t.Context(), obj.ID(), time.Now().Add(time.Hour))
		if err != nil {
			t.Fatalf("failed to create shared object URL: %v", err)
		}
		buf.Reset()
		if err := sdk.DownloadSharedObject(t.Context(), buf, sharedURL); err != nil {
			t.Fatalf("failed to download shared object: %v", err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}
	}

	// regular object upload
	data := frand.Bytes(4096)
	obj := NewEmptyObject()
	err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data), WithRedundancy(4, 11))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if _, err := sdk.Object(t.Context(), obj.ID()); err == nil || !strings.Contains(err.Error(), slabs.ErrObjectNotFound.Error()) {
		t.Fatal("object should not be pinned yet")
	}
	assertShareable(obj, data)

	// packed upload
	packed, err := sdk.UploadPacked(WithRedundancy(4, 11))
	if err != nil {
		t.Fatalf("failed to create packed upload: %v", err)
	}
	defer packed.Close()

	data1 := frand.Bytes(3000)
	if _, err := packed.Add(t.Context(), bytes.NewReader(data1)); err != nil {
		t.Fatalf("failed to add first object: %v", err)
	}
	data2 := frand.Bytes(5000)
	if _, err := packed.Add(t.Context(), bytes.NewReader(data2)); err != nil {
		t.Fatalf("failed to add second object: %v", err)
	}

	objects, err := packed.Finalize(t.Context())
	if err != nil {
		t.Fatalf("failed to finalize packed upload: %v", err)
	} else if len(objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objects))
	}

	assertShareable(objects[0], data1)
	assertShareable(objects[1], data2)

	// packed upload spanning multiple slabs
	packedL, err := sdk.UploadPacked(WithRedundancy(4, 11))
	if err != nil {
		t.Fatalf("failed to create multi-slab packed upload: %v", err)
	}
	defer packedL.Close()

	data3 := frand.Bytes(9 * 1 << 20) // 9 MiB
	if _, err := packedL.Add(t.Context(), bytes.NewReader(data3)); err != nil {
		t.Fatalf("failed to add first large object: %v", err)
	}
	data4 := frand.Bytes(9 * 1 << 20) // 9 MiB
	if _, err := packedL.Add(t.Context(), bytes.NewReader(data4)); err != nil {
		t.Fatalf("failed to add second large object: %v", err)
	}

	objects2, err := packedL.Finalize(t.Context())
	if err != nil {
		t.Fatalf("failed to finalize multi-slab packed upload: %v", err)
	} else if len(objects2) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objects2))
	}

	assertShareable(objects2[0], data3)
	assertShareable(objects2[1], data4)
}

func BenchmarkUpload(b *testing.B) {
	sdk, hosts := newTestSDK(b, 30, zap.NewNop())
	defer sdk.Close()

	const benchmarkSize = 256 * 1000 * 1000 // 256 MB
	data := frand.Bytes(benchmarkSize)

	benchMatrix := func(b *testing.B, slow, timeout, inflight int) {
		b.Helper()
		b.Run(fmt.Sprintf("slow %d timeout %d inflight %d", slow, timeout, inflight), func(b *testing.B) {
			hosts.ResetSlowHosts()
			hosts.SetSlowHosts(b, slow, time.Second)       // slow, but not too slow
			hosts.SetSlowHosts(b, timeout, 30*time.Second) // longer than the default timeout

			r := bytes.NewReader(data)
			b.SetBytes(benchmarkSize)
			b.ResetTimer()
			for b.Loop() {
				r.Reset(data)
				obj := NewEmptyObject()
				if err := sdk.Upload(context.Background(), &obj, r, WithUploadInflight(inflight)); err != nil {
					b.Fatalf("failed to upload: %v", err)
				}
			}
		})
	}

	inflight := []int{runtime.NumCPU(), 5, 10, 20, 30}
	// testing more variants is not particularly useful
	slow := []int{0, 1, 3, 5}
	timeout := []int{0, 1, 3, 5}
	for _, s := range slow {
		for _, t := range timeout {
			for _, i := range inflight {
				benchMatrix(b, s, t, i)
			}
		}
	}
}

func BenchmarkDownload(b *testing.B) {
	sdk, hosts := newTestSDK(b, 30, zap.NewNop())
	defer sdk.Close()

	const benchmarkSize = 256 * 1000 * 1000 // 256 MB
	data := frand.Bytes(benchmarkSize)
	obj := NewEmptyObject()
	err := sdk.Upload(b.Context(), &obj, bytes.NewReader(data))
	if err != nil {
		b.Fatalf("failed to upload: %v", err)
	}

	benchMatrix := func(b *testing.B, slow, inflight int) {
		b.Helper()
		b.Run(fmt.Sprintf("slow %d inflight %d", slow, inflight), func(b *testing.B) {
			// needs to be longer than the default timeout
			hosts.ResetSlowHosts()
			hosts.SetSlowHosts(b, slow, 30*time.Second)

			// NOTE: refreshing hosts before the benchmark makes all downloads fast because of warmup
			// sdk.refreshHosts(b.Context(), true)

			buf := bytes.NewBuffer(nil)
			b.SetBytes(benchmarkSize)
			b.ResetTimer()
			for b.Loop() {
				buf.Reset()
				err = sdk.Download(context.Background(), buf, obj, WithDownloadInflight(inflight))
				if err != nil {
					b.Fatalf("failed to download: %v", err)
				}
			}
		})
	}

	benchMatrix(b, 0, runtime.NumCPU())

	inflight := []int{1, 3, 5, 10, 20, 30}
	slow := []int{0, 1, 3, 5, 10, 20}

	for _, s := range slow {
		for _, i := range inflight {
			benchMatrix(b, s, i)
		}
	}
}
