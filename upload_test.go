package siastorage

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"go.sia.tech/core/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"

	proto "go.sia.tech/core/rhp/v4"
)

// erroringReader returns err on the read that consumes the last of its data,
// then io.EOF, which io.Reader allows but io.ReadFull hides.
type erroringReader struct {
	data []byte
	err  error
}

func (r *erroringReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

// TestUploadReaderError asserts a failing reader is not mistaken for the end
// of the stream, which would silently truncate the object.
func TestUploadReaderError(t *testing.T) {
	const dataShards, parityShards = 3, 9

	sdk, _ := newTestSDK(t, dataShards+parityShards, zaptest.NewLogger(t))
	defer sdk.Close()

	// a truncated stream surfaces io.ErrUnexpectedEOF instead of being
	// uploaded as a short final slab
	obj := NewEmptyObject()
	err := sdk.Upload(t.Context(), &obj, &erroringReader{
		data: frand.Bytes(1000),
		err:  io.ErrUnexpectedEOF,
	}, WithRedundancy(dataShards, parityShards))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("unexpected", err)
	} else if obj.Size() != 0 {
		t.Fatal("unexpected", obj.Size())
	}

	// an error returned by the read that filled the slab is not discarded
	readErr := errors.New("read failed")
	obj = NewEmptyObject()
	err = sdk.Upload(t.Context(), &obj, &erroringReader{
		data: make([]byte, dataShards*proto.SectorSize),
		err:  readErr,
	}, WithRedundancy(dataShards, parityShards))
	if !errors.Is(err, readErr) {
		t.Fatal("unexpected", err)
	} else if obj.Size() != 0 {
		t.Fatal("unexpected", obj.Size())
	}
}

// TestUploadInflight asserts uploads release their inflight
// reservations and avoid busy hosts.
func TestUploadInflight(t *testing.T) {
	sdk, hosts := newTestSDK(t, 40, zaptest.NewLogger(t))
	defer sdk.Close()

	// saturate 5 hosts with inflight writes so PickWrite steers the upload
	// onto the 35 idle ones
	usable, _ := hosts.hosts.UsableHosts()
	busy := make(map[types.PublicKey]bool)
	var releases []func()
	for _, hi := range usable[:5] {
		busy[hi.PublicKey] = true
		for range 5 {
			releases = append(releases, hosts.provider.TrackInflightWrite(hi.PublicKey))
		}
	}

	data := frand.Bytes(int(proto.SectorSize) * 10) // one slab, 30 shards
	obj := NewEmptyObject()
	if err := sdk.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// the upload's own reservations must all be released
	hosts.waitInflightDrained(t)

	// the slab's shards should land mostly on idle hosts
	var onBusy int
	for _, slab := range obj.Slabs() {
		for _, sector := range slab.Sectors {
			if busy[sector.HostKey] {
				onBusy++
			}
		}
	}
	if onBusy > 5 {
		t.Fatal("too many shards on busy hosts, inflight not respected", onBusy)
	}

	for _, r := range releases {
		r()
	}
}
