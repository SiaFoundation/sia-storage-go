//go:build siastorage_mock

package siastorage

/*
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"io"
	"runtime"
)

// mockSDK drives the real upload and download pipelines against in-process
// hosts. It exists for tests; the exported SDK requires an indexer.
type mockSDK struct {
	ptr *C.sia_mock_t
}

func newMockSDK(numHosts int, appKeySeed [32]byte) *mockSDK {
	m := &mockSDK{ptr: C.sia_mock_new(C.size_t(numHosts), cBytes32(&appKeySeed))}
	runtime.AddCleanup(m, func(p *C.sia_mock_t) {
		C.sia_mock_free(p)
	}, m.ptr)
	return m
}

func (m *mockSDK) Upload(ctx context.Context, obj *Object, r io.Reader, opts ...UploadOption) error {
	if obj.h == nil {
		return errNilObject
	}
	var uo uploadOption
	for _, opt := range opts {
		opt(&uo)
	}
	copts, pid := uo.cOptions()
	defer unregisterProgress(pid)

	var up *C.sia_upload_t
	var cerr *C.char
	code := C.sia_mock_upload_start(m.ptr, obj.h.ptr, &copts, &up, &cerr)
	runtime.KeepAlive(obj.h)
	runtime.KeepAlive(m)
	if err := goError(ctx, code, cerr); err != nil {
		return err
	}
	return uploadStream(ctx, up, obj, r)
}

func (m *mockSDK) Download(obj Object, opts ...DownloadOption) (io.ReadCloser, error) {
	if obj.h == nil {
		return nil, errNilObject
	}
	do := downloadOption{length: obj.Size()}
	for _, opt := range opts {
		opt(&do)
	}
	if !do.normalizeRange(obj.Size()) {
		return io.NopCloser(emptyReader{}), nil
	}

	copts, pid := do.cOptions()
	var dl *C.sia_download_t
	var cerr *C.char
	code := C.sia_mock_download_start(m.ptr, obj.h.ptr, &copts, &dl, &cerr)
	runtime.KeepAlive(obj.h)
	runtime.KeepAlive(m)
	if err := goError(nil, code, cerr); err != nil {
		unregisterProgress(pid)
		return nil, err
	}
	return startDownload(dl, pid), nil
}

func (m *mockSDK) UploadPacked(opts ...UploadOption) (*PackedUpload, error) {
	var uo uploadOption
	for _, opt := range opts {
		opt(&uo)
	}
	copts, pid := uo.cOptions()
	var up *C.sia_packed_upload_t
	var cerr *C.char
	code := C.sia_mock_packed_upload_start(m.ptr, &copts, &up, &cerr)
	runtime.KeepAlive(m)
	if err := goError(nil, code, cerr); err != nil {
		unregisterProgress(pid)
		return nil, err
	}
	return newPackedUpload(up, pid), nil
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }
