package siastorage

import (
	"io"

	ffi "go.sia.tech/siastorage/sia_storage_ffi"
)

// uploadChunkSize is the size of the chunks handed to the FFI layer during
// uploads.
const uploadChunkSize = 1 << 20 // 1 MiB

// ffiReader adapts an io.Reader to the generated Reader interface. The FFI
// contract signals EOF with an empty chunk.
type ffiReader struct {
	r   io.Reader
	buf []byte
}

func newFFIReader(r io.Reader) ffi.Reader {
	return &ffiReader{r: r, buf: make([]byte, uploadChunkSize)}
}

func (fr *ffiReader) Read() ([]byte, error) {
	for {
		n, err := fr.r.Read(fr.buf)
		if n > 0 {
			// fr.buf is reused across calls, so hand out a copy
			chunk := make([]byte, n)
			copy(chunk, fr.buf)
			return chunk, nil
		} else if err == io.EOF {
			return []byte{}, nil
		} else if err != nil {
			return nil, ffi.NewIoErrorIo()
		}
	}
}

// downloadReader adapts the generated Download handle to an io.ReadCloser.
type downloadReader struct {
	dl  *ffi.Download
	buf []byte
	err error
}

func (dr *downloadReader) Read(p []byte) (int, error) {
	for len(dr.buf) == 0 {
		if dr.err != nil {
			return 0, dr.err
		}
		chunk, err := dr.dl.Read()
		if err != nil {
			dr.err = err
			return 0, err
		} else if len(chunk) == 0 {
			dr.err = io.EOF
			return 0, io.EOF
		}
		dr.buf = chunk
	}
	n := copy(p, dr.buf)
	dr.buf = dr.buf[n:]
	return n, nil
}

// Close cancels the download and aborts any in-flight work. Subsequent reads
// return an error.
func (dr *downloadReader) Close() error {
	dr.dl.Cancel()
	if dr.err == nil {
		dr.err = io.ErrClosedPipe
	}
	return nil
}
