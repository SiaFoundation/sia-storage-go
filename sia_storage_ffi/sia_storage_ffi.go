package sia_storage_ffi

// #include <sia_storage_ffi.h>
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"runtime/cgo"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// This is needed, because as of go 1.24
// type RustBuffer C.RustBuffer cannot have methods,
// RustBuffer is treated as non-local type
type GoRustBuffer struct {
	inner C.RustBuffer
}

type RustBufferI interface {
	AsReader() *bytes.Reader
	Free()
	ToGoBytes() []byte
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

// C.RustBuffer fields exposed as an interface so they can be accessed in different Go packages.
// See https://github.com/golang/go/issues/13467
type ExternalCRustBuffer interface {
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

func RustBufferFromC(b C.RustBuffer) ExternalCRustBuffer {
	return GoRustBuffer{
		inner: b,
	}
}

func CFromRustBuffer(b ExternalCRustBuffer) C.RustBuffer {
	return C.RustBuffer{
		capacity: C.uint64_t(b.Capacity()),
		len:      C.uint64_t(b.Len()),
		data:     (*C.uchar)(b.Data()),
	}
}

func RustBufferFromExternal(b ExternalCRustBuffer) GoRustBuffer {
	return GoRustBuffer{
		inner: C.RustBuffer{
			capacity: C.uint64_t(b.Capacity()),
			len:      C.uint64_t(b.Len()),
			data:     (*C.uchar)(b.Data()),
		},
	}
}

func (cb GoRustBuffer) Capacity() uint64 {
	return uint64(cb.inner.capacity)
}

func (cb GoRustBuffer) Len() uint64 {
	return uint64(cb.inner.len)
}

func (cb GoRustBuffer) Data() unsafe.Pointer {
	return unsafe.Pointer(cb.inner.data)
}

func (cb GoRustBuffer) AsReader() *bytes.Reader {
	b := unsafe.Slice((*byte)(cb.inner.data), C.uint64_t(cb.inner.len))
	return bytes.NewReader(b)
}

func (cb GoRustBuffer) Free() {
	rustCall(func(status *C.RustCallStatus) bool {
		C.ffi_sia_storage_ffi_rustbuffer_free(cb.inner, status)
		return false
	})
}

func (cb GoRustBuffer) ToGoBytes() []byte {
	return C.GoBytes(unsafe.Pointer(cb.inner.data), C.int(cb.inner.len))
}

func stringToRustBuffer(str string) C.RustBuffer {
	return bytesToRustBuffer([]byte(str))
}

func bytesToRustBuffer(b []byte) C.RustBuffer {
	if len(b) == 0 {
		return C.RustBuffer{}
	}
	// We can pass the pointer along here, as it is pinned
	// for the duration of this call
	foreign := C.ForeignBytes{
		len:  C.int(len(b)),
		data: (*C.uchar)(unsafe.Pointer(&b[0])),
	}

	return rustCall(func(status *C.RustCallStatus) C.RustBuffer {
		return C.ffi_sia_storage_ffi_rustbuffer_from_bytes(foreign, status)
	})
}

type BufLifter[GoType any] interface {
	Lift(value RustBufferI) GoType
}

type BufLowerer[GoType any] interface {
	Lower(value GoType) C.RustBuffer
}

type BufReader[GoType any] interface {
	Read(reader io.Reader) GoType
}

type BufWriter[GoType any] interface {
	Write(writer io.Writer, value GoType)
}

func LowerIntoRustBuffer[GoType any](bufWriter BufWriter[GoType], value GoType) C.RustBuffer {
	// This might be not the most efficient way but it does not require knowing allocation size
	// beforehand
	var buffer bytes.Buffer
	bufWriter.Write(&buffer, value)

	bytes, err := io.ReadAll(&buffer)
	if err != nil {
		panic(fmt.Errorf("reading written data: %w", err))
	}
	return bytesToRustBuffer(bytes)
}

func LiftFromRustBuffer[GoType any](bufReader BufReader[GoType], rbuf RustBufferI) GoType {
	defer rbuf.Free()
	reader := rbuf.AsReader()
	item := bufReader.Read(reader)
	if reader.Len() > 0 {
		// TODO: Remove this
		leftover, _ := io.ReadAll(reader)
		panic(fmt.Errorf("Junk remaining in buffer after lifting: %s", string(leftover)))
	}
	return item
}

func rustCallWithError[E any, U any](converter BufReader[E], callback func(*C.RustCallStatus) U) (U, E) {
	var status C.RustCallStatus
	returnValue := callback(&status)
	err := checkCallStatus(converter, status)
	return returnValue, err
}

func checkCallStatus[E any](converter BufReader[E], status C.RustCallStatus) E {
	switch status.code {
	case 0:
		var zero E
		return zero
	case 1:
		return LiftFromRustBuffer(converter, GoRustBuffer{inner: status.errorBuf})
	case 2:
		// when the rust code sees a panic, it tries to construct a rustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{inner: status.errorBuf})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		panic(fmt.Errorf("unknown status code: %d", status.code))
	}
}

func checkCallStatusUnknown(status C.RustCallStatus) error {
	switch status.code {
	case 0:
		return nil
	case 1:
		panic(fmt.Errorf("function not returning an error returned an error"))
	case 2:
		// when the rust code sees a panic, it tries to construct a C.RustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: status.errorBuf,
			})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		return fmt.Errorf("unknown status code: %d", status.code)
	}
}

func rustCall[U any](callback func(*C.RustCallStatus) U) U {
	returnValue, err := rustCallWithError[error](nil, callback)
	if err != nil {
		panic(err)
	}
	return returnValue
}

type NativeError interface {
	AsError() error
}

func writeInt8(writer io.Writer, value int8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint8(writer io.Writer, value uint8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt16(writer io.Writer, value int16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint16(writer io.Writer, value uint16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt32(writer io.Writer, value int32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint32(writer io.Writer, value uint32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt64(writer io.Writer, value int64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint64(writer io.Writer, value uint64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat32(writer io.Writer, value float32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat64(writer io.Writer, value float64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func readInt8(reader io.Reader) int8 {
	var result int8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint8(reader io.Reader) uint8 {
	var result uint8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt16(reader io.Reader) int16 {
	var result int16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint16(reader io.Reader) uint16 {
	var result uint16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt32(reader io.Reader) int32 {
	var result int32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint32(reader io.Reader) uint32 {
	var result uint32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt64(reader io.Reader) int64 {
	var result int64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint64(reader io.Reader) uint64 {
	var result uint64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat32(reader io.Reader) float32 {
	var result float32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat64(reader io.Reader) float64 {
	var result float64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func init() {

	FfiConverterLoggerINSTANCE.register()
	FfiConverterProgressCallbackINSTANCE.register()
	FfiConverterReaderINSTANCE.register()
	uniffiCheckChecksums()
}

func uniffiCheckChecksums() {
	// Get the bindings contract version from our ComponentInterface
	bindingsContractVersion := 30
	// Get the scaffolding contract version by calling the into the dylib
	scaffoldingContractVersion := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint32_t {
		return C.ffi_sia_storage_ffi_uniffi_contract_version()
	})
	if bindingsContractVersion != int(scaffoldingContractVersion) {
		// If this happens try cleaning and rebuilding your project
		panic("sia_storage_ffi: UniFFI contract version mismatch")
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_func_encoded_size()
		})
		if checksum != 52940 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_func_encoded_size: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_func_generate_recovery_phrase()
		})
		if checksum != 50091 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_func_generate_recovery_phrase: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_func_validate_recovery_phrase()
		})
		if checksum != 24248 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_func_validate_recovery_phrase: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_func_set_logger()
		})
		if checksum != 54631 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_func_set_logger: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_download_cancel()
		})
		if checksum != 44264 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_download_cancel: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_download_read()
		})
		if checksum != 37314 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_download_read: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_packedupload_add()
		})
		if checksum != 51351 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_packedupload_add: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_packedupload_cancel()
		})
		if checksum != 64519 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_packedupload_cancel: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_packedupload_finalize()
		})
		if checksum != 48196 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_packedupload_finalize: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_packedupload_length()
		})
		if checksum != 7379 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_packedupload_length: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_packedupload_remaining()
		})
		if checksum != 11061 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_packedupload_remaining: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_packedupload_slabs()
		})
		if checksum != 22197 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_packedupload_slabs: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_created_at()
		})
		if checksum != 6326 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_created_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_encoded_size()
		})
		if checksum != 12774 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_encoded_size: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_id()
		})
		if checksum != 8920 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_metadata()
		})
		if checksum != 44967 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_seal()
		})
		if checksum != 63803 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_seal: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_size()
		})
		if checksum != 18457 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_size: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_slabs()
		})
		if checksum != 33285 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_slabs: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_update_metadata()
		})
		if checksum != 21836 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_update_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_pinnedobject_updated_at()
		})
		if checksum != 809 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_pinnedobject_updated_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_progresscallback_progress()
		})
		if checksum != 12410 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_progresscallback_progress: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_account()
		})
		if checksum != 12349 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_account: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_app_key()
		})
		if checksum != 17767 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_app_key: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_delete_object()
		})
		if checksum != 3966 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_delete_object: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_download()
		})
		if checksum != 48699 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_download: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_hosts()
		})
		if checksum != 44133 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_hosts: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_object()
		})
		if checksum != 28006 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_object: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_object_events()
		})
		if checksum != 51406 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_object_events: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_pin_object()
		})
		if checksum != 29905 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_pin_object: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_prune_slabs()
		})
		if checksum != 20696 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_prune_slabs: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_share_object()
		})
		if checksum != 27092 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_share_object: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_shared_object()
		})
		if checksum != 842 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_shared_object: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_slab()
		})
		if checksum != 11224 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_slab: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_update_object_metadata()
		})
		if checksum != 27986 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_update_object_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_upload()
		})
		if checksum != 27415 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_upload: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_sdk_upload_packed()
		})
		if checksum != 37714 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_sdk_upload_packed: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_appkey_export()
		})
		if checksum != 16630 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_appkey_export: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_appkey_public_key()
		})
		if checksum != 21541 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_appkey_public_key: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_appkey_sign()
		})
		if checksum != 10910 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_appkey_sign: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_appkey_verify_signature()
		})
		if checksum != 38967 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_appkey_verify_signature: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_builder_connected()
		})
		if checksum != 44195 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_builder_connected: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_builder_register()
		})
		if checksum != 39536 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_builder_register: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_builder_request_connection()
		})
		if checksum != 35070 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_builder_request_connection: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_builder_response_url()
		})
		if checksum != 34413 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_builder_response_url: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_builder_wait_for_approval()
		})
		if checksum != 26618 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_builder_wait_for_approval: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_reader_read()
		})
		if checksum != 59516 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_reader_read: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_logger_info()
		})
		if checksum != 36400 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_logger_info: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_logger_warn()
		})
		if checksum != 1887 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_logger_warn: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_logger_error()
		})
		if checksum != 5086 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_logger_error: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_method_logger_debug()
		})
		if checksum != 60732 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_method_logger_debug: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_constructor_pinnedobject_new()
		})
		if checksum != 8222 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_constructor_pinnedobject_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_constructor_pinnedobject_open()
		})
		if checksum != 15872 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_constructor_pinnedobject_open: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_constructor_appkey_new()
		})
		if checksum != 6640 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_constructor_appkey_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_sia_storage_ffi_checksum_constructor_builder_new()
		})
		if checksum != 24760 {
			// If this happens try cleaning and rebuilding your project
			panic("sia_storage_ffi: uniffi_sia_storage_ffi_checksum_constructor_builder_new: UniFFI API checksum mismatch")
		}
	}
}

type FfiConverterUint8 struct{}

var FfiConverterUint8INSTANCE = FfiConverterUint8{}

func (FfiConverterUint8) Lower(value uint8) C.uint8_t {
	return C.uint8_t(value)
}

func (FfiConverterUint8) Write(writer io.Writer, value uint8) {
	writeUint8(writer, value)
}

func (FfiConverterUint8) Lift(value C.uint8_t) uint8 {
	return uint8(value)
}

func (FfiConverterUint8) Read(reader io.Reader) uint8 {
	return readUint8(reader)
}

type FfiDestroyerUint8 struct{}

func (FfiDestroyerUint8) Destroy(_ uint8) {}

type FfiConverterUint32 struct{}

var FfiConverterUint32INSTANCE = FfiConverterUint32{}

func (FfiConverterUint32) Lower(value uint32) C.uint32_t {
	return C.uint32_t(value)
}

func (FfiConverterUint32) Write(writer io.Writer, value uint32) {
	writeUint32(writer, value)
}

func (FfiConverterUint32) Lift(value C.uint32_t) uint32 {
	return uint32(value)
}

func (FfiConverterUint32) Read(reader io.Reader) uint32 {
	return readUint32(reader)
}

type FfiDestroyerUint32 struct{}

func (FfiDestroyerUint32) Destroy(_ uint32) {}

type FfiConverterUint64 struct{}

var FfiConverterUint64INSTANCE = FfiConverterUint64{}

func (FfiConverterUint64) Lower(value uint64) C.uint64_t {
	return C.uint64_t(value)
}

func (FfiConverterUint64) Write(writer io.Writer, value uint64) {
	writeUint64(writer, value)
}

func (FfiConverterUint64) Lift(value C.uint64_t) uint64 {
	return uint64(value)
}

func (FfiConverterUint64) Read(reader io.Reader) uint64 {
	return readUint64(reader)
}

type FfiDestroyerUint64 struct{}

func (FfiDestroyerUint64) Destroy(_ uint64) {}

type FfiConverterFloat64 struct{}

var FfiConverterFloat64INSTANCE = FfiConverterFloat64{}

func (FfiConverterFloat64) Lower(value float64) C.double {
	return C.double(value)
}

func (FfiConverterFloat64) Write(writer io.Writer, value float64) {
	writeFloat64(writer, value)
}

func (FfiConverterFloat64) Lift(value C.double) float64 {
	return float64(value)
}

func (FfiConverterFloat64) Read(reader io.Reader) float64 {
	return readFloat64(reader)
}

type FfiDestroyerFloat64 struct{}

func (FfiDestroyerFloat64) Destroy(_ float64) {}

type FfiConverterBool struct{}

var FfiConverterBoolINSTANCE = FfiConverterBool{}

func (FfiConverterBool) Lower(value bool) C.int8_t {
	if value {
		return C.int8_t(1)
	}
	return C.int8_t(0)
}

func (FfiConverterBool) Write(writer io.Writer, value bool) {
	if value {
		writeInt8(writer, 1)
	} else {
		writeInt8(writer, 0)
	}
}

func (FfiConverterBool) Lift(value C.int8_t) bool {
	return value != 0
}

func (FfiConverterBool) Read(reader io.Reader) bool {
	return readInt8(reader) != 0
}

type FfiDestroyerBool struct{}

func (FfiDestroyerBool) Destroy(_ bool) {}

type FfiConverterString struct{}

var FfiConverterStringINSTANCE = FfiConverterString{}

func (FfiConverterString) Lift(rb RustBufferI) string {
	defer rb.Free()
	reader := rb.AsReader()
	b, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Errorf("reading reader: %w", err))
	}
	return string(b)
}

func (FfiConverterString) Read(reader io.Reader) string {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading string, expected %d, read %d", length, read_length))
	}
	return string(buffer)
}

func (FfiConverterString) Lower(value string) C.RustBuffer {
	return stringToRustBuffer(value)
}

func (c FfiConverterString) LowerExternal(value string) ExternalCRustBuffer {
	return RustBufferFromC(stringToRustBuffer(value))
}

func (FfiConverterString) Write(writer io.Writer, value string) {
	if len(value) > math.MaxInt32 {
		panic("String is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := io.WriteString(writer, value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing string, expected %d, written %d", len(value), write_length))
	}
}

type FfiDestroyerString struct{}

func (FfiDestroyerString) Destroy(_ string) {}

type FfiConverterBytes struct{}

var FfiConverterBytesINSTANCE = FfiConverterBytes{}

func (c FfiConverterBytes) Lower(value []byte) C.RustBuffer {
	return LowerIntoRustBuffer[[]byte](c, value)
}

func (c FfiConverterBytes) LowerExternal(value []byte) ExternalCRustBuffer {
	return RustBufferFromC(c.Lower(value))
}

func (c FfiConverterBytes) Write(writer io.Writer, value []byte) {
	if len(value) > math.MaxInt32 {
		panic("[]byte is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := writer.Write(value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing []byte, expected %d, written %d", len(value), write_length))
	}
}

func (c FfiConverterBytes) Lift(rb RustBufferI) []byte {
	return LiftFromRustBuffer[[]byte](c, rb)
}

func (c FfiConverterBytes) Read(reader io.Reader) []byte {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading []byte, expected %d, read %d", length, read_length))
	}
	return buffer
}

type FfiDestroyerBytes struct{}

func (FfiDestroyerBytes) Destroy(_ []byte) {}

type FfiConverterTimestamp struct{}

var FfiConverterTimestampINSTANCE = FfiConverterTimestamp{}

func (c FfiConverterTimestamp) Lift(rb RustBufferI) time.Time {
	return LiftFromRustBuffer[time.Time](c, rb)
}

func (c FfiConverterTimestamp) Read(reader io.Reader) time.Time {
	sec := readInt64(reader)
	nsec := readUint32(reader)

	var sign int64 = 1
	if sec < 0 {
		sign = -1
	}

	return time.Unix(sec, int64(nsec)*sign)
}

func (c FfiConverterTimestamp) Lower(value time.Time) C.RustBuffer {
	return LowerIntoRustBuffer[time.Time](c, value)
}

func (c FfiConverterTimestamp) LowerExternal(value time.Time) ExternalCRustBuffer {
	return RustBufferFromC(c.Lower(value))
}

func (c FfiConverterTimestamp) Write(writer io.Writer, value time.Time) {
	sec := value.Unix()
	nsec := uint32(value.Nanosecond())
	if value.Unix() < 0 {
		nsec = 1_000_000_000 - nsec
		sec += 1
	}

	writeInt64(writer, sec)
	writeUint32(writer, nsec)
}

type FfiDestroyerTimestamp struct{}

func (FfiDestroyerTimestamp) Destroy(_ time.Time) {}

// Below is an implementation of synchronization requirements outlined in the link.
// https://github.com/mozilla/uniffi-rs/blob/0dc031132d9493ca812c3af6e7dd60ad2ea95bf0/uniffi_bindgen/src/bindings/kotlin/templates/ObjectRuntime.kt#L31

type FfiObject struct {
	handle        C.uint64_t
	callCounter   atomic.Int64
	cloneFunction func(C.uint64_t, *C.RustCallStatus) C.uint64_t
	freeFunction  func(C.uint64_t, *C.RustCallStatus)
	destroyed     atomic.Bool
}

func newFfiObject(
	handle C.uint64_t,
	cloneFunction func(C.uint64_t, *C.RustCallStatus) C.uint64_t,
	freeFunction func(C.uint64_t, *C.RustCallStatus),
) FfiObject {
	return FfiObject{
		handle:        handle,
		cloneFunction: cloneFunction,
		freeFunction:  freeFunction,
	}
}

func (ffiObject *FfiObject) incrementPointer(debugName string) C.uint64_t {
	for {
		counter := ffiObject.callCounter.Load()
		if counter <= -1 {
			panic(fmt.Errorf("%v object has already been destroyed", debugName))
		}
		if counter == math.MaxInt64 {
			panic(fmt.Errorf("%v object call counter would overflow", debugName))
		}
		if ffiObject.callCounter.CompareAndSwap(counter, counter+1) {
			break
		}
	}

	return rustCall(func(status *C.RustCallStatus) C.uint64_t {
		return ffiObject.cloneFunction(ffiObject.handle, status)
	})
}

func (ffiObject *FfiObject) decrementPointer() {
	if ffiObject.callCounter.Add(-1) == -1 {
		ffiObject.freeRustArcPtr()
	}
}

func (ffiObject *FfiObject) destroy() {
	if ffiObject.destroyed.CompareAndSwap(false, true) {
		if ffiObject.callCounter.Add(-1) == -1 {
			ffiObject.freeRustArcPtr()
		}
	}
}

func (ffiObject *FfiObject) freeRustArcPtr() {
	if ffiObject.handle == 0 {
		return
	}
	rustCall(func(status *C.RustCallStatus) int32 {
		ffiObject.freeFunction(ffiObject.handle, status)
		return 0
	})
}

// An AppKey is used to sign requests to the indexer.
//
// AppKeys can be registered with an indexer during
// onboarding with a [Builder]. They are derived from
// a BIP-39 recovery phrase, which can be generated
// using [generate_recovery_phrase].
//
// It must be stored securely by the application and
// never shared publicly. If exposed, a user's data
// is compromised.
//
// Mishandling the app key will lead to data loss
// and inability to access stored objects.
type AppKeyInterface interface {
	// Exports the AppKey. The app key can be re-imported later
	// using [AppKey::new].
	//
	// AppKeys should be stored securely by the application in lieu of the
	// recovery phrase.
	Export() []byte
	// Returns the public key corresponding to the AppKey.
	//
	// This can be safely shared with others.
	PublicKey() string
	// Signs a message using the AppKey.
	Sign(message []byte) []byte
	// Verifies a signature for a given message using the AppKey.
	VerifySignature(message []byte, signature []byte) (bool, error)
}

// An AppKey is used to sign requests to the indexer.
//
// AppKeys can be registered with an indexer during
// onboarding with a [Builder]. They are derived from
// a BIP-39 recovery phrase, which can be generated
// using [generate_recovery_phrase].
//
// It must be stored securely by the application and
// never shared publicly. If exposed, a user's data
// is compromised.
//
// Mishandling the app key will lead to data loss
// and inability to access stored objects.
type AppKey struct {
	ffiObject FfiObject
}

// Imports an AppKey from the provided byte array.
//
// # Arguments
// * `key` - A 32-byte array representing the app key.
func NewAppKey(key []byte) (*AppKey, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*AppKeyError](FfiConverterAppKeyError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_constructor_appkey_new(FfiConverterBytesINSTANCE.Lower(key), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *AppKey
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterAppKeyINSTANCE.Lift(_uniffiRV), nil
	}
}

// Exports the AppKey. The app key can be re-imported later
// using [AppKey::new].
//
// AppKeys should be stored securely by the application in lieu of the
// recovery phrase.
func (_self *AppKey) Export() []byte {
	_pointer := _self.ffiObject.incrementPointer("*AppKey")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterBytesINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_appkey_export(
				_pointer, _uniffiStatus),
		}
	}))
}

// Returns the public key corresponding to the AppKey.
//
// This can be safely shared with others.
func (_self *AppKey) PublicKey() string {
	_pointer := _self.ffiObject.incrementPointer("*AppKey")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_appkey_public_key(
				_pointer, _uniffiStatus),
		}
	}))
}

// Signs a message using the AppKey.
func (_self *AppKey) Sign(message []byte) []byte {
	_pointer := _self.ffiObject.incrementPointer("*AppKey")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterBytesINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_appkey_sign(
				_pointer, FfiConverterBytesINSTANCE.Lower(message), _uniffiStatus),
		}
	}))
}

// Verifies a signature for a given message using the AppKey.
func (_self *AppKey) VerifySignature(message []byte, signature []byte) (bool, error) {
	_pointer := _self.ffiObject.incrementPointer("*AppKey")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[*AppKeyError](FfiConverterAppKeyError{}, func(_uniffiStatus *C.RustCallStatus) C.int8_t {
		return C.uniffi_sia_storage_ffi_fn_method_appkey_verify_signature(
			_pointer, FfiConverterBytesINSTANCE.Lower(message), FfiConverterBytesINSTANCE.Lower(signature), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue bool
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterBoolINSTANCE.Lift(_uniffiRV), nil
	}
}
func (object *AppKey) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterAppKey struct{}

var FfiConverterAppKeyINSTANCE = FfiConverterAppKey{}

func (c FfiConverterAppKey) Lift(handle C.uint64_t) *AppKey {
	result := &AppKey{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_sia_storage_ffi_fn_clone_appkey(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_sia_storage_ffi_fn_free_appkey(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*AppKey).Destroy)
	return result
}

func (c FfiConverterAppKey) Read(reader io.Reader) *AppKey {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterAppKey) Lower(value *AppKey) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*AppKey")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterAppKey) Write(writer io.Writer, value *AppKey) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalAppKey(handle uint64) *AppKey {
	return FfiConverterAppKeyINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalAppKey(value *AppKey) uint64 {
	return uint64(FfiConverterAppKeyINSTANCE.Lower(value))
}

type FfiDestroyerAppKey struct{}

func (_ FfiDestroyerAppKey) Destroy(value *AppKey) {
	value.Destroy()
}

type BuilderInterface interface {
	// Attempts to connect using the provided app key.
	// If the app key is valid, returns Some([Sdk]), otherwise returns None.
	//
	// If you receive None, call [Builder::request_connection] to request a new connection.
	//
	// # Arguments
	// * `app_key` - The application key used for authentication.
	Connected(appKey *AppKey) (**Sdk, error)
	// Registers the application with the indexer using the provided mnemonic.
	// Once registered, returns an [Sdk] instance that can be used to interact
	// with the indexer.
	//
	// # Arguments
	// * `mnemonic` - The user's mnemonic phrase used to derive the application key.
	Register(mnemonic string) (*Sdk, error)
	// Requests connection approval for the application. The
	// user must approve the connection request for the app to be registered and receive an SDK instance.
	//
	// After calling this method, call [Builder::response_url] to get the URL that the user should
	// visit to approve the connection request, and [Builder::wait_for_approval] to wait for the
	// user to approve the connection request.
	RequestConnection() (*Builder, error)
	// Retrieves the response URL for the connection request.
	// This URL can be used to approve the connection request.
	// It should be displayed to the user.
	ResponseUrl() (string, error)
	// Waits for the connection request to be approved.
	// Once approved, the app can be registered and used to create an
	// SDK instance.
	WaitForApproval() (*Builder, error)
}
type Builder struct {
	ffiObject FfiObject
}

// Creates a new SDK builder with the provided indexer URL.
//
// After creating the builder, call [Builder::connected] to attempt
// to connect using an existing app key, or [Builder::request_connection]
// to request a new connection.
func NewBuilder(indexerUrl string, appMeta AppMetadata) (*Builder, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BuilderError](FfiConverterBuilderError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_constructor_builder_new(FfiConverterStringINSTANCE.Lower(indexerUrl), FfiConverterAppMetadataINSTANCE.Lower(appMeta), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Builder
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterBuilderINSTANCE.Lift(_uniffiRV), nil
	}
}

// Attempts to connect using the provided app key.
// If the app key is valid, returns Some([Sdk]), otherwise returns None.
//
// If you receive None, call [Builder::request_connection] to request a new connection.
//
// # Arguments
// * `app_key` - The application key used for authentication.
func (_self *Builder) Connected(appKey *AppKey) (**Sdk, error) {
	_pointer := _self.ffiObject.incrementPointer("*Builder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*BuilderError](
		FfiConverterBuilderErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_sia_storage_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) **Sdk {
			return FfiConverterOptionalSdkINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_builder_connected(
			_pointer, FfiConverterAppKeyINSTANCE.Lower(appKey)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Registers the application with the indexer using the provided mnemonic.
// Once registered, returns an [Sdk] instance that can be used to interact
// with the indexer.
//
// # Arguments
// * `mnemonic` - The user's mnemonic phrase used to derive the application key.
func (_self *Builder) Register(mnemonic string) (*Sdk, error) {
	_pointer := _self.ffiObject.incrementPointer("*Builder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*BuilderError](
		FfiConverterBuilderErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_sia_storage_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *Sdk {
			return FfiConverterSdkINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_builder_register(
			_pointer, FfiConverterStringINSTANCE.Lower(mnemonic)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Requests connection approval for the application. The
// user must approve the connection request for the app to be registered and receive an SDK instance.
//
// After calling this method, call [Builder::response_url] to get the URL that the user should
// visit to approve the connection request, and [Builder::wait_for_approval] to wait for the
// user to approve the connection request.
func (_self *Builder) RequestConnection() (*Builder, error) {
	_pointer := _self.ffiObject.incrementPointer("*Builder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*BuilderError](
		FfiConverterBuilderErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_sia_storage_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *Builder {
			return FfiConverterBuilderINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_builder_request_connection(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Retrieves the response URL for the connection request.
// This URL can be used to approve the connection request.
// It should be displayed to the user.
func (_self *Builder) ResponseUrl() (string, error) {
	_pointer := _self.ffiObject.incrementPointer("*Builder")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[*BuilderError](FfiConverterBuilderError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_builder_response_url(
				_pointer, _uniffiStatus),
		}
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue string
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterStringINSTANCE.Lift(_uniffiRV), nil
	}
}

// Waits for the connection request to be approved.
// Once approved, the app can be registered and used to create an
// SDK instance.
func (_self *Builder) WaitForApproval() (*Builder, error) {
	_pointer := _self.ffiObject.incrementPointer("*Builder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*BuilderError](
		FfiConverterBuilderErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_sia_storage_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *Builder {
			return FfiConverterBuilderINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_builder_wait_for_approval(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}
func (object *Builder) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterBuilder struct{}

var FfiConverterBuilderINSTANCE = FfiConverterBuilder{}

func (c FfiConverterBuilder) Lift(handle C.uint64_t) *Builder {
	result := &Builder{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_sia_storage_ffi_fn_clone_builder(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_sia_storage_ffi_fn_free_builder(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Builder).Destroy)
	return result
}

func (c FfiConverterBuilder) Read(reader io.Reader) *Builder {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterBuilder) Lower(value *Builder) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*Builder")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterBuilder) Write(writer io.Writer, value *Builder) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalBuilder(handle uint64) *Builder {
	return FfiConverterBuilderINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalBuilder(value *Builder) uint64 {
	return uint64(FfiConverterBuilderINSTANCE.Lower(value))
}

type FfiDestroyerBuilder struct{}

func (_ FfiDestroyerBuilder) Destroy(value *Builder) {
	value.Destroy()
}

// A download handle. Call [Download::read] repeatedly to receive chunks of
// decoded data. An empty Vec signals end of stream. All in-flight work is
// cancelled when the handle is dropped or [Download::cancel] is called.
type DownloadInterface interface {
	// Cancels the download and aborts any in-flight chunk recovery tasks.
	// Interrupts an in-flight [Download::read] immediately. Subsequent reads
	// return [DownloadError::Cancelled].
	Cancel()
	// Reads the next chunk of decoded data.
	//
	// # Returns
	// An empty Vec on EOF or [DownloadError::Cancelled] if the download has been cancelled. Otherwise, returns a chunk of decoded data.
	Read() ([]byte, error)
}

// A download handle. Call [Download::read] repeatedly to receive chunks of
// decoded data. An empty Vec signals end of stream. All in-flight work is
// cancelled when the handle is dropped or [Download::cancel] is called.
type Download struct {
	ffiObject FfiObject
}

// Cancels the download and aborts any in-flight chunk recovery tasks.
// Interrupts an in-flight [Download::read] immediately. Subsequent reads
// return [DownloadError::Cancelled].
func (_self *Download) Cancel() {
	_pointer := _self.ffiObject.incrementPointer("*Download")
	defer _self.ffiObject.decrementPointer()
	uniffiRustCallAsync[error](
		nil,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_sia_storage_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_sia_storage_ffi_fn_method_download_cancel(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_void(handle)
		},
	)

}

// Reads the next chunk of decoded data.
//
// # Returns
// An empty Vec on EOF or [DownloadError::Cancelled] if the download has been cancelled. Otherwise, returns a chunk of decoded data.
func (_self *Download) Read() ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Download")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*DownloadError](
		FfiConverterDownloadErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_sia_storage_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []byte {
			return FfiConverterBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_download_read(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}
func (object *Download) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDownload struct{}

var FfiConverterDownloadINSTANCE = FfiConverterDownload{}

func (c FfiConverterDownload) Lift(handle C.uint64_t) *Download {
	result := &Download{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_sia_storage_ffi_fn_clone_download(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_sia_storage_ffi_fn_free_download(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Download).Destroy)
	return result
}

func (c FfiConverterDownload) Read(reader io.Reader) *Download {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDownload) Lower(value *Download) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*Download")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDownload) Write(writer io.Writer, value *Download) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDownload(handle uint64) *Download {
	return FfiConverterDownloadINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDownload(value *Download) uint64 {
	return uint64(FfiConverterDownloadINSTANCE.Lower(value))
}

type FfiDestroyerDownload struct{}

func (_ FfiDestroyerDownload) Destroy(value *Download) {
	value.Destroy()
}

type Logger interface {
	Info(msg string)
	Warn(msg string)
	Error(msg string)
	Debug(msg string)
}
type LoggerImpl struct {
	ffiObject FfiObject
}

func (_self *LoggerImpl) Info(msg string) {
	_pointer := _self.ffiObject.incrementPointer("Logger")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_method_logger_info(
			_pointer, FfiConverterStringINSTANCE.Lower(msg), _uniffiStatus)
		return false
	})
}

func (_self *LoggerImpl) Warn(msg string) {
	_pointer := _self.ffiObject.incrementPointer("Logger")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_method_logger_warn(
			_pointer, FfiConverterStringINSTANCE.Lower(msg), _uniffiStatus)
		return false
	})
}

func (_self *LoggerImpl) Error(msg string) {
	_pointer := _self.ffiObject.incrementPointer("Logger")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_method_logger_error(
			_pointer, FfiConverterStringINSTANCE.Lower(msg), _uniffiStatus)
		return false
	})
}

func (_self *LoggerImpl) Debug(msg string) {
	_pointer := _self.ffiObject.incrementPointer("Logger")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_method_logger_debug(
			_pointer, FfiConverterStringINSTANCE.Lower(msg), _uniffiStatus)
		return false
	})
}
func (object *LoggerImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterLogger struct {
	handleMap *concurrentHandleMap[Logger]
}

var FfiConverterLoggerINSTANCE = FfiConverterLogger{
	handleMap: newConcurrentHandleMap[Logger](),
}

func (c FfiConverterLogger) Lift(handle C.uint64_t) Logger {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &LoggerImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_sia_storage_ffi_fn_clone_logger(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_sia_storage_ffi_fn_free_logger(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*LoggerImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterLogger) Read(reader io.Reader) Logger {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterLogger) Lower(value Logger) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*LoggerImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("Logger")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterLogger) Write(writer io.Writer, value Logger) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalLogger(handle uint64) Logger {
	return FfiConverterLoggerINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalLogger(value Logger) uint64 {
	return uint64(FfiConverterLoggerINSTANCE.Lower(value))
}

type FfiDestroyerLogger struct{}

func (_ FfiDestroyerLogger) Destroy(value Logger) {
	if val, ok := value.(*LoggerImpl); ok {
		val.Destroy()
	}
}

type uniffiCallbackResult C.int8_t

const (
	uniffiIdxCallbackFree               uniffiCallbackResult = 0
	uniffiCallbackResultSuccess         uniffiCallbackResult = 0
	uniffiCallbackResultError           uniffiCallbackResult = 1
	uniffiCallbackUnexpectedResultError uniffiCallbackResult = 2
	uniffiCallbackCancelled             uniffiCallbackResult = 3
)

type concurrentHandleMap[T any] struct {
	handles       map[uint64]T
	currentHandle uint64
	lock          sync.RWMutex
}

func newConcurrentHandleMap[T any]() *concurrentHandleMap[T] {
	return &concurrentHandleMap[T]{
		handles:       map[uint64]T{},
		currentHandle: 1,
	}
}

func (cm *concurrentHandleMap[T]) insert(obj T) uint64 {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	handle := cm.currentHandle
	cm.currentHandle = cm.currentHandle + 2
	cm.handles[handle] = obj
	return handle
}

func (cm *concurrentHandleMap[T]) remove(handle uint64) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	delete(cm.handles, handle)
}

func (cm *concurrentHandleMap[T]) tryGet(handle uint64) (T, bool) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	val, ok := cm.handles[handle]
	return val, ok
}

//export sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod0
func sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod0(uniffiHandle C.uint64_t, msg C.RustBuffer, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterLoggerINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Info(
		FfiConverterStringINSTANCE.Lift(GoRustBuffer{
			inner: msg,
		}),
	)

}

//export sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod1
func sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod1(uniffiHandle C.uint64_t, msg C.RustBuffer, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterLoggerINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Warn(
		FfiConverterStringINSTANCE.Lift(GoRustBuffer{
			inner: msg,
		}),
	)

}

//export sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod2
func sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod2(uniffiHandle C.uint64_t, msg C.RustBuffer, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterLoggerINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Error(
		FfiConverterStringINSTANCE.Lift(GoRustBuffer{
			inner: msg,
		}),
	)

}

//export sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod3
func sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod3(uniffiHandle C.uint64_t, msg C.RustBuffer, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterLoggerINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Debug(
		FfiConverterStringINSTANCE.Lift(GoRustBuffer{
			inner: msg,
		}),
	)

}

var UniffiVTableCallbackInterfaceLoggerINSTANCE = C.UniffiVTableCallbackInterfaceLogger{
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerClone),
	info:        (C.UniffiCallbackInterfaceLoggerMethod0)(C.sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod0),
	warn:        (C.UniffiCallbackInterfaceLoggerMethod1)(C.sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod1),
	error:       (C.UniffiCallbackInterfaceLoggerMethod2)(C.sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod2),
	debug:       (C.UniffiCallbackInterfaceLoggerMethod3)(C.sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerMethod3),
}

//export sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerFree
func sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerFree(handle C.uint64_t) {
	FfiConverterLoggerINSTANCE.handleMap.remove(uint64(handle))
}

//export sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerClone
func sia_storage_ffi_logging_cgo_dispatchCallbackInterfaceLoggerClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterLoggerINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterLoggerINSTANCE.handleMap.insert(val))
}

func (c FfiConverterLogger) register() {
	C.uniffi_sia_storage_ffi_fn_init_callback_vtable_logger(&UniffiVTableCallbackInterfaceLoggerINSTANCE)
}

// A packed upload allows multiple objects to be uploaded together in a single upload. This can be more
// efficient than uploading each object separately if the size of the object is less than the minimum
// slab size.
type PackedUploadInterface interface {
	// Adds a new object to the upload. The data is read until EOF and packed into
	// the current slab. Returns the number of bytes consumed; call
	// [finalize](Self::finalize) once all objects have been added to get the
	// resulting objects.
	//
	// If the reader errors part-way, it's safe to continue calling
	// [add](Self::add); no object is registered for the failed call. Or call
	// [finalize](Self::finalize) to collect the objects added so far.
	Add(reader Reader) (uint64, error)
	// Cancels the upload. This will immediately cancel any in-progress [add](Self::add) or [finalize](Self::finalize) operations and prevent
	// any new ones from starting. Any in-flight operations will return an error once cancelled.
	Cancel()
	// Finalizes the upload and returns the resulting objects. This will wait for all readers
	// to finish and all slabs to be uploaded before returning. The resulting objects will contain the metadata needed to download the objects.
	//
	// The caller must pin the resulting objects to the indexer when ready.
	Finalize() ([]*PinnedObject, error)
	// Returns the number of bytes added so far.
	Length() uint64
	// Returns the number of bytes remaining until reaching the optimal
	// packed size. Adding objects larger than this will start a new slab.
	// To minimize padding, prioritize objects that fit within the remaining
	// size.
	Remaining() uint64
	// Returns the number of slabs in the upload.
	Slabs() uint64
}

// A packed upload allows multiple objects to be uploaded together in a single upload. This can be more
// efficient than uploading each object separately if the size of the object is less than the minimum
// slab size.
type PackedUpload struct {
	ffiObject FfiObject
}

// Adds a new object to the upload. The data is read until EOF and packed into
// the current slab. Returns the number of bytes consumed; call
// [finalize](Self::finalize) once all objects have been added to get the
// resulting objects.
//
// If the reader errors part-way, it's safe to continue calling
// [add](Self::add); no object is registered for the failed call. Or call
// [finalize](Self::finalize) to collect the objects added so far.
func (_self *PackedUpload) Add(reader Reader) (uint64, error) {
	_pointer := _self.ffiObject.incrementPointer("*PackedUpload")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*UploadError](
		FfiConverterUploadErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_sia_storage_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) uint64 {
			return FfiConverterUint64INSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_packedupload_add(
			_pointer, FfiConverterReaderINSTANCE.Lower(reader)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Cancels the upload. This will immediately cancel any in-progress [add](Self::add) or [finalize](Self::finalize) operations and prevent
// any new ones from starting. Any in-flight operations will return an error once cancelled.
func (_self *PackedUpload) Cancel() {
	_pointer := _self.ffiObject.incrementPointer("*PackedUpload")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_method_packedupload_cancel(
			_pointer, _uniffiStatus)
		return false
	})
}

// Finalizes the upload and returns the resulting objects. This will wait for all readers
// to finish and all slabs to be uploaded before returning. The resulting objects will contain the metadata needed to download the objects.
//
// The caller must pin the resulting objects to the indexer when ready.
func (_self *PackedUpload) Finalize() ([]*PinnedObject, error) {
	_pointer := _self.ffiObject.incrementPointer("*PackedUpload")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*UploadError](
		FfiConverterUploadErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_sia_storage_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []*PinnedObject {
			return FfiConverterSequencePinnedObjectINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_packedupload_finalize(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns the number of bytes added so far.
func (_self *PackedUpload) Length() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*PackedUpload")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_method_packedupload_length(
			_pointer, _uniffiStatus)
	}))
}

// Returns the number of bytes remaining until reaching the optimal
// packed size. Adding objects larger than this will start a new slab.
// To minimize padding, prioritize objects that fit within the remaining
// size.
func (_self *PackedUpload) Remaining() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*PackedUpload")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_method_packedupload_remaining(
			_pointer, _uniffiStatus)
	}))
}

// Returns the number of slabs in the upload.
func (_self *PackedUpload) Slabs() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*PackedUpload")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_method_packedupload_slabs(
			_pointer, _uniffiStatus)
	}))
}
func (object *PackedUpload) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterPackedUpload struct{}

var FfiConverterPackedUploadINSTANCE = FfiConverterPackedUpload{}

func (c FfiConverterPackedUpload) Lift(handle C.uint64_t) *PackedUpload {
	result := &PackedUpload{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_sia_storage_ffi_fn_clone_packedupload(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_sia_storage_ffi_fn_free_packedupload(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*PackedUpload).Destroy)
	return result
}

func (c FfiConverterPackedUpload) Read(reader io.Reader) *PackedUpload {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterPackedUpload) Lower(value *PackedUpload) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*PackedUpload")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterPackedUpload) Write(writer io.Writer, value *PackedUpload) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalPackedUpload(handle uint64) *PackedUpload {
	return FfiConverterPackedUploadINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalPackedUpload(value *PackedUpload) uint64 {
	return uint64(FfiConverterPackedUploadINSTANCE.Lower(value))
}

type FfiDestroyerPackedUpload struct{}

func (_ FfiDestroyerPackedUpload) Destroy(value *PackedUpload) {
	value.Destroy()
}

// An object that has been pinned to an indexer. Objects are immutable
// data stored on the Sia network. The data is erasure-coded and distributed across
// multiple storage providers. The object is encrypted with a unique encryption key,
// which is used to encrypt the metadata.
//
// Custom user-defined metadata can be associated with the object. It is
// recommended to use a portable format like JSON for metadata.
//
// It can be sealed for secure offline storage or transmission and
// later opened using the app key.
//
// It has no public fields to prevent accidental leakage or corruption.
type PinnedObjectInterface interface {
	// Returns the time the object was created.
	CreatedAt() time.Time
	// Returns the total encoded size of the object after erasure coding
	// by summing the sizes of its slabs.
	EncodedSize() uint64
	// Returns the object's ID, which is the Blake2b hash of its slabs.
	Id() string
	// Returns the metadata associated with the object.
	Metadata() []byte
	// Seal the object for offline storage.
	// # Arguments
	// * `app_key` - The app key used to derive the master key to encrypt the object's encryption key.
	//
	// # Returns
	// The sealed object.
	Seal(appKey *AppKey) SealedObject
	// Returns the total size of the object by summing the lengths of its slabs.
	Size() uint64
	// Returns the slabs that make up the object.
	Slabs() []Slab
	// Updates the metadata associated with the object.
	UpdateMetadata(metadata []byte)
	// Returns the time the object was last updated.
	UpdatedAt() time.Time
}

// An object that has been pinned to an indexer. Objects are immutable
// data stored on the Sia network. The data is erasure-coded and distributed across
// multiple storage providers. The object is encrypted with a unique encryption key,
// which is used to encrypt the metadata.
//
// Custom user-defined metadata can be associated with the object. It is
// recommended to use a portable format like JSON for metadata.
//
// It can be sealed for secure offline storage or transmission and
// later opened using the app key.
//
// It has no public fields to prevent accidental leakage or corruption.
type PinnedObject struct {
	ffiObject FfiObject
}

// Creates a new empty object.
func NewPinnedObject() *PinnedObject {
	return FfiConverterPinnedObjectINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_constructor_pinnedobject_new(_uniffiStatus)
	}))
}

// Opens a sealed object using the provided app key.
//
// # Arguments
// * `app_key` - The app key that was used to seal the object.
// * `sealed` - The sealed object to open.
//
// # Returns
// The unsealed object or an error if the object could not be opened.
func PinnedObjectOpen(appKey *AppKey, sealed SealedObject) (*PinnedObject, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*ObjectError](FfiConverterObjectError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_constructor_pinnedobject_open(FfiConverterAppKeyINSTANCE.Lower(appKey), FfiConverterSealedObjectINSTANCE.Lower(sealed), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *PinnedObject
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterPinnedObjectINSTANCE.Lift(_uniffiRV), nil
	}
}

// Returns the time the object was created.
func (_self *PinnedObject) CreatedAt() time.Time {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterTimestampINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_pinnedobject_created_at(
				_pointer, _uniffiStatus),
		}
	}))
}

// Returns the total encoded size of the object after erasure coding
// by summing the sizes of its slabs.
func (_self *PinnedObject) EncodedSize() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_method_pinnedobject_encoded_size(
			_pointer, _uniffiStatus)
	}))
}

// Returns the object's ID, which is the Blake2b hash of its slabs.
func (_self *PinnedObject) Id() string {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_pinnedobject_id(
				_pointer, _uniffiStatus),
		}
	}))
}

// Returns the metadata associated with the object.
func (_self *PinnedObject) Metadata() []byte {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterBytesINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_pinnedobject_metadata(
				_pointer, _uniffiStatus),
		}
	}))
}

// Seal the object for offline storage.
// # Arguments
// * `app_key` - The app key used to derive the master key to encrypt the object's encryption key.
//
// # Returns
// The sealed object.
func (_self *PinnedObject) Seal(appKey *AppKey) SealedObject {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterSealedObjectINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_pinnedobject_seal(
				_pointer, FfiConverterAppKeyINSTANCE.Lower(appKey), _uniffiStatus),
		}
	}))
}

// Returns the total size of the object by summing the lengths of its slabs.
func (_self *PinnedObject) Size() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_method_pinnedobject_size(
			_pointer, _uniffiStatus)
	}))
}

// Returns the slabs that make up the object.
func (_self *PinnedObject) Slabs() []Slab {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterSequenceSlabINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_pinnedobject_slabs(
				_pointer, _uniffiStatus),
		}
	}))
}

// Updates the metadata associated with the object.
func (_self *PinnedObject) UpdateMetadata(metadata []byte) {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_method_pinnedobject_update_metadata(
			_pointer, FfiConverterBytesINSTANCE.Lower(metadata), _uniffiStatus)
		return false
	})
}

// Returns the time the object was last updated.
func (_self *PinnedObject) UpdatedAt() time.Time {
	_pointer := _self.ffiObject.incrementPointer("*PinnedObject")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterTimestampINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_pinnedobject_updated_at(
				_pointer, _uniffiStatus),
		}
	}))
}
func (object *PinnedObject) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterPinnedObject struct{}

var FfiConverterPinnedObjectINSTANCE = FfiConverterPinnedObject{}

func (c FfiConverterPinnedObject) Lift(handle C.uint64_t) *PinnedObject {
	result := &PinnedObject{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_sia_storage_ffi_fn_clone_pinnedobject(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_sia_storage_ffi_fn_free_pinnedobject(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*PinnedObject).Destroy)
	return result
}

func (c FfiConverterPinnedObject) Read(reader io.Reader) *PinnedObject {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterPinnedObject) Lower(value *PinnedObject) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*PinnedObject")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterPinnedObject) Write(writer io.Writer, value *PinnedObject) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalPinnedObject(handle uint64) *PinnedObject {
	return FfiConverterPinnedObjectINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalPinnedObject(value *PinnedObject) uint64 {
	return uint64(FfiConverterPinnedObjectINSTANCE.Lower(value))
}

type FfiDestroyerPinnedObject struct{}

func (_ FfiDestroyerPinnedObject) Destroy(value *PinnedObject) {
	value.Destroy()
}

type ProgressCallback interface {
	Progress(progress ShardProgress)
}
type ProgressCallbackImpl struct {
	ffiObject FfiObject
}

func (_self *ProgressCallbackImpl) Progress(progress ShardProgress) {
	_pointer := _self.ffiObject.incrementPointer("ProgressCallback")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_method_progresscallback_progress(
			_pointer, FfiConverterShardProgressINSTANCE.Lower(progress), _uniffiStatus)
		return false
	})
}
func (object *ProgressCallbackImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterProgressCallback struct {
	handleMap *concurrentHandleMap[ProgressCallback]
}

var FfiConverterProgressCallbackINSTANCE = FfiConverterProgressCallback{
	handleMap: newConcurrentHandleMap[ProgressCallback](),
}

func (c FfiConverterProgressCallback) Lift(handle C.uint64_t) ProgressCallback {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &ProgressCallbackImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_sia_storage_ffi_fn_clone_progresscallback(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_sia_storage_ffi_fn_free_progresscallback(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*ProgressCallbackImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterProgressCallback) Read(reader io.Reader) ProgressCallback {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterProgressCallback) Lower(value ProgressCallback) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*ProgressCallbackImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("ProgressCallback")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterProgressCallback) Write(writer io.Writer, value ProgressCallback) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalProgressCallback(handle uint64) ProgressCallback {
	return FfiConverterProgressCallbackINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalProgressCallback(value ProgressCallback) uint64 {
	return uint64(FfiConverterProgressCallbackINSTANCE.Lower(value))
}

type FfiDestroyerProgressCallback struct{}

func (_ FfiDestroyerProgressCallback) Destroy(value ProgressCallback) {
	if val, ok := value.(*ProgressCallbackImpl); ok {
		val.Destroy()
	}
}

//export sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackMethod0
func sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackMethod0(uniffiHandle C.uint64_t, progress C.RustBuffer, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterProgressCallbackINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Progress(
		FfiConverterShardProgressINSTANCE.Lift(GoRustBuffer{
			inner: progress,
		}),
	)

}

var UniffiVTableCallbackInterfaceProgressCallbackINSTANCE = C.UniffiVTableCallbackInterfaceProgressCallback{
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackClone),
	progress:    (C.UniffiCallbackInterfaceProgressCallbackMethod0)(C.sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackMethod0),
}

//export sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackFree
func sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackFree(handle C.uint64_t) {
	FfiConverterProgressCallbackINSTANCE.handleMap.remove(uint64(handle))
}

//export sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackClone
func sia_storage_ffi_cgo_dispatchCallbackInterfaceProgressCallbackClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterProgressCallbackINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterProgressCallbackINSTANCE.handleMap.insert(val))
}

func (c FfiConverterProgressCallback) register() {
	C.uniffi_sia_storage_ffi_fn_init_callback_vtable_progresscallback(&UniffiVTableCallbackInterfaceProgressCallbackINSTANCE)
}

// A foreign reader that can be used to transfer data across FFI boundaries.
//
// Implementations should send an empty chunk to signal completion. It is recommended
// that implementations chunk data into reasonably sized pieces (e.g. 64KiB) to avoid
// excessive memory usage.
//
// If an error is returned by `read`, the reader will be closed and no
// further calls will be made.
type Reader interface {
	Read() ([]byte, error)
}

// A foreign reader that can be used to transfer data across FFI boundaries.
//
// Implementations should send an empty chunk to signal completion. It is recommended
// that implementations chunk data into reasonably sized pieces (e.g. 64KiB) to avoid
// excessive memory usage.
//
// If an error is returned by `read`, the reader will be closed and no
// further calls will be made.
type ReaderImpl struct {
	ffiObject FfiObject
}

func (_self *ReaderImpl) Read() ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("Reader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*IoError](
		FfiConverterIoErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_sia_storage_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []byte {
			return FfiConverterBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_reader_read(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}
func (object *ReaderImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterReader struct {
	handleMap *concurrentHandleMap[Reader]
}

var FfiConverterReaderINSTANCE = FfiConverterReader{
	handleMap: newConcurrentHandleMap[Reader](),
}

func (c FfiConverterReader) Lift(handle C.uint64_t) Reader {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &ReaderImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_sia_storage_ffi_fn_clone_reader(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_sia_storage_ffi_fn_free_reader(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*ReaderImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterReader) Read(reader io.Reader) Reader {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterReader) Lower(value Reader) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*ReaderImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("Reader")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterReader) Write(writer io.Writer, value Reader) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalReader(handle uint64) Reader {
	return FfiConverterReaderINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalReader(value Reader) uint64 {
	return uint64(FfiConverterReaderINSTANCE.Lower(value))
}

type FfiDestroyerReader struct{}

func (_ FfiDestroyerReader) Destroy(value Reader) {
	if val, ok := value.(*ReaderImpl); ok {
		val.Destroy()
	}
}

//export sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderMethod0
func sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderMethod0(uniffiHandle C.uint64_t, uniffiFutureCallback C.UniffiForeignFutureCompleteRustBuffer, uniffiCallbackData C.uint64_t, uniffiOutDroppedCallback *C.UniffiForeignFutureDroppedCallbackStruct) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterReaderINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	result := make(chan C.UniffiForeignFutureResultRustBuffer, 1)
	cancel := make(chan struct{}, 1)
	guardHandle := cgo.NewHandle(cancel)
	*uniffiOutDroppedCallback = C.UniffiForeignFutureDroppedCallbackStruct{
		handle: C.uint64_t(guardHandle),
		free:   C.UniffiForeignFutureDroppedCallback(C.sia_storage_ffi_uniffiFreeGorutine),
	}

	// Wait for compleation or cancel
	go func() {
		select {
		case <-cancel:
		case res := <-result:
			C.call_UniffiForeignFutureCompleteRustBuffer(uniffiFutureCallback, uniffiCallbackData, res)
		}
	}()

	// Eval callback asynchroniously
	go func() {
		asyncResult := &C.UniffiForeignFutureResultRustBuffer{}
		uniffiOutReturn := &asyncResult.returnValue
		callStatus := &asyncResult.callStatus
		defer func() {
			result <- *asyncResult
		}()

		res, err :=
			uniffiObj.Read()

		if err != nil {
			var actualError *IoError
			if errors.As(err, &actualError) {
				*callStatus = C.RustCallStatus{
					code:     C.int8_t(uniffiCallbackResultError),
					errorBuf: FfiConverterIoErrorINSTANCE.Lower(actualError),
				}
			} else {
				*callStatus = C.RustCallStatus{
					code: C.int8_t(uniffiCallbackUnexpectedResultError),
				}
			}
			return
		}

		*uniffiOutReturn = FfiConverterBytesINSTANCE.Lower(res)
	}()
}

var UniffiVTableCallbackInterfaceReaderINSTANCE = C.UniffiVTableCallbackInterfaceReader{
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderClone),
	read:        (C.UniffiCallbackInterfaceReaderMethod0)(C.sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderMethod0),
}

//export sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderFree
func sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderFree(handle C.uint64_t) {
	FfiConverterReaderINSTANCE.handleMap.remove(uint64(handle))
}

//export sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderClone
func sia_storage_ffi_io_cgo_dispatchCallbackInterfaceReaderClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterReaderINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterReaderINSTANCE.handleMap.insert(val))
}

func (c FfiConverterReader) register() {
	C.uniffi_sia_storage_ffi_fn_init_callback_vtable_reader(&UniffiVTableCallbackInterfaceReaderINSTANCE)
}

type SdkInterface interface {
	// Returns the current account.
	Account() (Account, error)
	// Returns the application key used by the SDK.
	//
	// This should be kept secret and secure. Applications
	// must never share their app key publicly. Store
	// it safely.
	AppKey() *AppKey
	// Deletes an object from the indexer.
	DeleteObject(key string) error
	// Initiates a download of the data referenced by the object, starting at `offset` and reading `length` bytes.
	// Returns a [Download] handle that yields chunks via [Download::read].
	Download(object *PinnedObject, options DownloadOptions) (*Download, error)
	// Returns a list of all usable hosts.
	Hosts() ([]Host, error)
	// Returns metadata about a specific object stored in the indexer.
	Object(key string) (*PinnedObject, error)
	// Returns objects stored in the indexer. When syncing, the caller should
	// provide the last `updated_at` timestamp and `id` seen in the `cursor`
	// parameter to avoid missing or duplicating objects.
	//
	// # Arguments
	// * `cursor` can be used to paginate through the results. If `cursor` is `None`, the first page of results will be returned.
	// * `limit` specifies the maximum number of objects to return.
	ObjectEvents(cursor *ObjectsCursor, limit uint32) ([]ObjectEvent, error)
	// Pins an object to the indexer
	PinObject(object *PinnedObject) error
	// Unpins slabs not used by any object on the account.
	PruneSlabs() error
	// Creates a signed URL that can be used to share object metadata
	// with other people using an indexer.
	ShareObject(object *PinnedObject, validUntil time.Time) (string, error)
	// Retrieves a shared object from a signed URL.
	SharedObject(sharedUrl string) (*PinnedObject, error)
	// Returns metadata about a slab stored in the indexer.
	Slab(slabId string) (PinnedSlab, error)
	// Updates the metadata of an object stored in the indexer. The object must already be pinned to
	// the indexer.
	UpdateObjectMetadata(object *PinnedObject) error
	// Uploads data to the Sia network.
	//
	// Pass [PinnedObject::new] for new uploads. To resume a previous upload,
	// pass the object returned from the earlier call. Appending data changes
	// an object's ID. It must be re-pinned afterward and any references to
	// the previous ID must be updated.
	//
	// # Arguments
	// * `object` - The object to upload into. Use [PinnedObject::new] for new uploads.
	// * `r` - The reader to read the data from.
	// * `options` - The [UploadOptions] to use for the upload.
	//
	// # Returns
	// A new object containing all slabs from the input object plus the newly
	// uploaded slabs. The caller must pin the object to the indexer afterward.
	Upload(object *PinnedObject, r Reader, options UploadOptions) (*PinnedObject, error)
	// Creates a new packed upload. This allows multiple objects to be packed together
	// for more efficient uploads. The returned `PackedUpload` can be used to add objects to the upload, and then finalized to get the resulting objects.
	//
	// # Arguments
	// * `options` - The [UploadOptions] to use for the upload.
	//
	// # Returns
	// A [PackedUpload] that can be used to add objects and finalize the upload.
	UploadPacked(options UploadOptions) (*PackedUpload, error)
}
type Sdk struct {
	ffiObject FfiObject
}

// Returns the current account.
func (_self *Sdk) Account() (Account, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_sia_storage_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Account {
			return FfiConverterAccountINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_sdk_account(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns the application key used by the SDK.
//
// This should be kept secret and secure. Applications
// must never share their app key publicly. Store
// it safely.
func (_self *Sdk) AppKey() *AppKey {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterAppKeyINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_method_sdk_app_key(
			_pointer, _uniffiStatus)
	}))
}

// Deletes an object from the indexer.
func (_self *Sdk) DeleteObject(key string) error {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_sia_storage_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_sia_storage_ffi_fn_method_sdk_delete_object(
			_pointer, FfiConverterStringINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Initiates a download of the data referenced by the object, starting at `offset` and reading `length` bytes.
// Returns a [Download] handle that yields chunks via [Download::read].
func (_self *Sdk) Download(object *PinnedObject, options DownloadOptions) (*Download, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[*DownloadError](FfiConverterDownloadError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_method_sdk_download(
			_pointer, FfiConverterPinnedObjectINSTANCE.Lower(object), FfiConverterDownloadOptionsINSTANCE.Lower(options), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Download
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterDownloadINSTANCE.Lift(_uniffiRV), nil
	}
}

// Returns a list of all usable hosts.
func (_self *Sdk) Hosts() ([]Host, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_sia_storage_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []Host {
			return FfiConverterSequenceHostINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_sdk_hosts(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns metadata about a specific object stored in the indexer.
func (_self *Sdk) Object(key string) (*PinnedObject, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_sia_storage_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *PinnedObject {
			return FfiConverterPinnedObjectINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_sdk_object(
			_pointer, FfiConverterStringINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns objects stored in the indexer. When syncing, the caller should
// provide the last `updated_at` timestamp and `id` seen in the `cursor`
// parameter to avoid missing or duplicating objects.
//
// # Arguments
// * `cursor` can be used to paginate through the results. If `cursor` is `None`, the first page of results will be returned.
// * `limit` specifies the maximum number of objects to return.
func (_self *Sdk) ObjectEvents(cursor *ObjectsCursor, limit uint32) ([]ObjectEvent, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_sia_storage_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []ObjectEvent {
			return FfiConverterSequenceObjectEventINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_sdk_object_events(
			_pointer, FfiConverterOptionalObjectsCursorINSTANCE.Lower(cursor), FfiConverterUint32INSTANCE.Lower(limit)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Pins an object to the indexer
func (_self *Sdk) PinObject(object *PinnedObject) error {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_sia_storage_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_sia_storage_ffi_fn_method_sdk_pin_object(
			_pointer, FfiConverterPinnedObjectINSTANCE.Lower(object)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Unpins slabs not used by any object on the account.
func (_self *Sdk) PruneSlabs() error {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_sia_storage_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_sia_storage_ffi_fn_method_sdk_prune_slabs(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Creates a signed URL that can be used to share object metadata
// with other people using an indexer.
func (_self *Sdk) ShareObject(object *PinnedObject, validUntil time.Time) (string, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_method_sdk_share_object(
				_pointer, FfiConverterPinnedObjectINSTANCE.Lower(object), FfiConverterTimestampINSTANCE.Lower(validUntil), _uniffiStatus),
		}
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue string
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterStringINSTANCE.Lift(_uniffiRV), nil
	}
}

// Retrieves a shared object from a signed URL.
func (_self *Sdk) SharedObject(sharedUrl string) (*PinnedObject, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_sia_storage_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *PinnedObject {
			return FfiConverterPinnedObjectINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_sdk_shared_object(
			_pointer, FfiConverterStringINSTANCE.Lower(sharedUrl)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns metadata about a slab stored in the indexer.
func (_self *Sdk) Slab(slabId string) (PinnedSlab, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_sia_storage_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) PinnedSlab {
			return FfiConverterPinnedSlabINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_sdk_slab(
			_pointer, FfiConverterStringINSTANCE.Lower(slabId)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Updates the metadata of an object stored in the indexer. The object must already be pinned to
// the indexer.
func (_self *Sdk) UpdateObjectMetadata(object *PinnedObject) error {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_sia_storage_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_sia_storage_ffi_fn_method_sdk_update_object_metadata(
			_pointer, FfiConverterPinnedObjectINSTANCE.Lower(object)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Uploads data to the Sia network.
//
// Pass [PinnedObject::new] for new uploads. To resume a previous upload,
// pass the object returned from the earlier call. Appending data changes
// an object's ID. It must be re-pinned afterward and any references to
// the previous ID must be updated.
//
// # Arguments
// * `object` - The object to upload into. Use [PinnedObject::new] for new uploads.
// * `r` - The reader to read the data from.
// * `options` - The [UploadOptions] to use for the upload.
//
// # Returns
// A new object containing all slabs from the input object plus the newly
// uploaded slabs. The caller must pin the object to the indexer afterward.
func (_self *Sdk) Upload(object *PinnedObject, r Reader, options UploadOptions) (*PinnedObject, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*UploadError](
		FfiConverterUploadErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_sia_storage_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *PinnedObject {
			return FfiConverterPinnedObjectINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_sdk_upload(
			_pointer, FfiConverterPinnedObjectINSTANCE.Lower(object), FfiConverterReaderINSTANCE.Lower(r), FfiConverterUploadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Creates a new packed upload. This allows multiple objects to be packed together
// for more efficient uploads. The returned `PackedUpload` can be used to add objects to the upload, and then finalized to get the resulting objects.
//
// # Arguments
// * `options` - The [UploadOptions] to use for the upload.
//
// # Returns
// A [PackedUpload] that can be used to add objects and finalize the upload.
func (_self *Sdk) UploadPacked(options UploadOptions) (*PackedUpload, error) {
	_pointer := _self.ffiObject.incrementPointer("*Sdk")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*UploadError](
		FfiConverterUploadErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_sia_storage_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *PackedUpload {
			return FfiConverterPackedUploadINSTANCE.Lift(ffi)
		},
		C.uniffi_sia_storage_ffi_fn_method_sdk_upload_packed(
			_pointer, FfiConverterUploadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_sia_storage_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}
func (object *Sdk) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterSdk struct{}

var FfiConverterSdkINSTANCE = FfiConverterSdk{}

func (c FfiConverterSdk) Lift(handle C.uint64_t) *Sdk {
	result := &Sdk{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_sia_storage_ffi_fn_clone_sdk(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_sia_storage_ffi_fn_free_sdk(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Sdk).Destroy)
	return result
}

func (c FfiConverterSdk) Read(reader io.Reader) *Sdk {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterSdk) Lower(value *Sdk) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*Sdk")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterSdk) Write(writer io.Writer, value *Sdk) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalSdk(handle uint64) *Sdk {
	return FfiConverterSdkINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalSdk(value *Sdk) uint64 {
	return uint64(FfiConverterSdkINSTANCE.Lower(value))
}

type FfiDestroyerSdk struct{}

func (_ FfiDestroyerSdk) Destroy(value *Sdk) {
	value.Destroy()
}

// An account registered on the indexer.
type Account struct {
	AccountKey string
	// The maximum amount of data that can be pinned to the indexer for this account.
	MaxPinnedData uint64
	// Remaining amount of data in bytes that can still be pinned, after applying both the account limit and current quota limit.
	RemainingStorage uint64
	// The amount of data currently pinned to the indexer for this account. This
	// counts towards max pinned data.
	PinnedData uint64
	// The amount of data after erasure encoding. This is the actual amount of data on the network.
	PinnedSize uint64
	// Whether the account is ready to be used. After registering an app, the account may not be
	// immediately ready as the indexer needs to process the registration and sync with the network.
	// The account will become ready once it has propagated on the network.
	Ready    bool
	App      App
	LastUsed time.Time
}

func (r *Account) Destroy() {
	FfiDestroyerString{}.Destroy(r.AccountKey)
	FfiDestroyerUint64{}.Destroy(r.MaxPinnedData)
	FfiDestroyerUint64{}.Destroy(r.RemainingStorage)
	FfiDestroyerUint64{}.Destroy(r.PinnedData)
	FfiDestroyerUint64{}.Destroy(r.PinnedSize)
	FfiDestroyerBool{}.Destroy(r.Ready)
	FfiDestroyerApp{}.Destroy(r.App)
	FfiDestroyerTimestamp{}.Destroy(r.LastUsed)
}

type FfiConverterAccount struct{}

var FfiConverterAccountINSTANCE = FfiConverterAccount{}

func (c FfiConverterAccount) Lift(rb RustBufferI) Account {
	return LiftFromRustBuffer[Account](c, rb)
}

func (c FfiConverterAccount) Read(reader io.Reader) Account {
	return Account{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterAppINSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
	}
}

func (c FfiConverterAccount) Lower(value Account) C.RustBuffer {
	return LowerIntoRustBuffer[Account](c, value)
}

func (c FfiConverterAccount) LowerExternal(value Account) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Account](c, value))
}

func (c FfiConverterAccount) Write(writer io.Writer, value Account) {
	FfiConverterStringINSTANCE.Write(writer, value.AccountKey)
	FfiConverterUint64INSTANCE.Write(writer, value.MaxPinnedData)
	FfiConverterUint64INSTANCE.Write(writer, value.RemainingStorage)
	FfiConverterUint64INSTANCE.Write(writer, value.PinnedData)
	FfiConverterUint64INSTANCE.Write(writer, value.PinnedSize)
	FfiConverterBoolINSTANCE.Write(writer, value.Ready)
	FfiConverterAppINSTANCE.Write(writer, value.App)
	FfiConverterTimestampINSTANCE.Write(writer, value.LastUsed)
}

type FfiDestroyerAccount struct{}

func (_ FfiDestroyerAccount) Destroy(value Account) {
	value.Destroy()
}

type App struct {
	Id          string
	Name        string
	Description string
	ServiceUrl  *string
	LogoUrl     *string
}

func (r *App) Destroy() {
	FfiDestroyerString{}.Destroy(r.Id)
	FfiDestroyerString{}.Destroy(r.Name)
	FfiDestroyerString{}.Destroy(r.Description)
	FfiDestroyerOptionalString{}.Destroy(r.ServiceUrl)
	FfiDestroyerOptionalString{}.Destroy(r.LogoUrl)
}

type FfiConverterApp struct{}

var FfiConverterAppINSTANCE = FfiConverterApp{}

func (c FfiConverterApp) Lift(rb RustBufferI) App {
	return LiftFromRustBuffer[App](c, rb)
}

func (c FfiConverterApp) Read(reader io.Reader) App {
	return App{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterApp) Lower(value App) C.RustBuffer {
	return LowerIntoRustBuffer[App](c, value)
}

func (c FfiConverterApp) LowerExternal(value App) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[App](c, value))
}

func (c FfiConverterApp) Write(writer io.Writer, value App) {
	FfiConverterStringINSTANCE.Write(writer, value.Id)
	FfiConverterStringINSTANCE.Write(writer, value.Name)
	FfiConverterStringINSTANCE.Write(writer, value.Description)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.ServiceUrl)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.LogoUrl)
}

type FfiDestroyerApp struct{}

func (_ FfiDestroyerApp) Destroy(value App) {
	value.Destroy()
}

// Metadata about an application connecting to the indexer.
type AppMetadata struct {
	Id          []byte
	Name        string
	Description string
	ServiceUrl  string
	LogoUrl     *string
	CallbackUrl *string
}

func (r *AppMetadata) Destroy() {
	FfiDestroyerBytes{}.Destroy(r.Id)
	FfiDestroyerString{}.Destroy(r.Name)
	FfiDestroyerString{}.Destroy(r.Description)
	FfiDestroyerString{}.Destroy(r.ServiceUrl)
	FfiDestroyerOptionalString{}.Destroy(r.LogoUrl)
	FfiDestroyerOptionalString{}.Destroy(r.CallbackUrl)
}

type FfiConverterAppMetadata struct{}

var FfiConverterAppMetadataINSTANCE = FfiConverterAppMetadata{}

func (c FfiConverterAppMetadata) Lift(rb RustBufferI) AppMetadata {
	return LiftFromRustBuffer[AppMetadata](c, rb)
}

func (c FfiConverterAppMetadata) Read(reader io.Reader) AppMetadata {
	return AppMetadata{
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterAppMetadata) Lower(value AppMetadata) C.RustBuffer {
	return LowerIntoRustBuffer[AppMetadata](c, value)
}

func (c FfiConverterAppMetadata) LowerExternal(value AppMetadata) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[AppMetadata](c, value))
}

func (c FfiConverterAppMetadata) Write(writer io.Writer, value AppMetadata) {
	FfiConverterBytesINSTANCE.Write(writer, value.Id)
	FfiConverterStringINSTANCE.Write(writer, value.Name)
	FfiConverterStringINSTANCE.Write(writer, value.Description)
	FfiConverterStringINSTANCE.Write(writer, value.ServiceUrl)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.LogoUrl)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.CallbackUrl)
}

type FfiDestroyerAppMetadata struct{}

func (_ FfiDestroyerAppMetadata) Destroy(value AppMetadata) {
	value.Destroy()
}

// Provides options for a download operation.
type DownloadOptions struct {
	MaxBufferedChunks *uint32
	Offset            *uint64
	Length            *uint64
	// Optional callback to report download progress.
	ShardDownloaded *ProgressCallback
}

func (r *DownloadOptions) Destroy() {
	FfiDestroyerOptionalUint32{}.Destroy(r.MaxBufferedChunks)
	FfiDestroyerOptionalUint64{}.Destroy(r.Offset)
	FfiDestroyerOptionalUint64{}.Destroy(r.Length)
	FfiDestroyerOptionalProgressCallback{}.Destroy(r.ShardDownloaded)
}

type FfiConverterDownloadOptions struct{}

var FfiConverterDownloadOptionsINSTANCE = FfiConverterDownloadOptions{}

func (c FfiConverterDownloadOptions) Lift(rb RustBufferI) DownloadOptions {
	return LiftFromRustBuffer[DownloadOptions](c, rb)
}

func (c FfiConverterDownloadOptions) Read(reader io.Reader) DownloadOptions {
	return DownloadOptions{
		FfiConverterOptionalUint32INSTANCE.Read(reader),
		FfiConverterOptionalUint64INSTANCE.Read(reader),
		FfiConverterOptionalUint64INSTANCE.Read(reader),
		FfiConverterOptionalProgressCallbackINSTANCE.Read(reader),
	}
}

func (c FfiConverterDownloadOptions) Lower(value DownloadOptions) C.RustBuffer {
	return LowerIntoRustBuffer[DownloadOptions](c, value)
}

func (c FfiConverterDownloadOptions) LowerExternal(value DownloadOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DownloadOptions](c, value))
}

func (c FfiConverterDownloadOptions) Write(writer io.Writer, value DownloadOptions) {
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.MaxBufferedChunks)
	FfiConverterOptionalUint64INSTANCE.Write(writer, value.Offset)
	FfiConverterOptionalUint64INSTANCE.Write(writer, value.Length)
	FfiConverterOptionalProgressCallbackINSTANCE.Write(writer, value.ShardDownloaded)
}

type FfiDestroyerDownloadOptions struct{}

func (_ FfiDestroyerDownloadOptions) Destroy(value DownloadOptions) {
	value.Destroy()
}

// Information about a storage provider on the
// Sia network.
type Host struct {
	PublicKey     string
	Addresses     []NetAddress
	CountryCode   string
	Latitude      float64
	Longitude     float64
	GoodForUpload bool
}

func (r *Host) Destroy() {
	FfiDestroyerString{}.Destroy(r.PublicKey)
	FfiDestroyerSequenceNetAddress{}.Destroy(r.Addresses)
	FfiDestroyerString{}.Destroy(r.CountryCode)
	FfiDestroyerFloat64{}.Destroy(r.Latitude)
	FfiDestroyerFloat64{}.Destroy(r.Longitude)
	FfiDestroyerBool{}.Destroy(r.GoodForUpload)
}

type FfiConverterHost struct{}

var FfiConverterHostINSTANCE = FfiConverterHost{}

func (c FfiConverterHost) Lift(rb RustBufferI) Host {
	return LiftFromRustBuffer[Host](c, rb)
}

func (c FfiConverterHost) Read(reader io.Reader) Host {
	return Host{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterSequenceNetAddressINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterFloat64INSTANCE.Read(reader),
		FfiConverterFloat64INSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterHost) Lower(value Host) C.RustBuffer {
	return LowerIntoRustBuffer[Host](c, value)
}

func (c FfiConverterHost) LowerExternal(value Host) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Host](c, value))
}

func (c FfiConverterHost) Write(writer io.Writer, value Host) {
	FfiConverterStringINSTANCE.Write(writer, value.PublicKey)
	FfiConverterSequenceNetAddressINSTANCE.Write(writer, value.Addresses)
	FfiConverterStringINSTANCE.Write(writer, value.CountryCode)
	FfiConverterFloat64INSTANCE.Write(writer, value.Latitude)
	FfiConverterFloat64INSTANCE.Write(writer, value.Longitude)
	FfiConverterBoolINSTANCE.Write(writer, value.GoodForUpload)
}

type FfiDestroyerHost struct{}

func (_ FfiDestroyerHost) Destroy(value Host) {
	value.Destroy()
}

// A network address of a storage provider on the Sia network.
type NetAddress struct {
	Protocol AddressProtocol
	Address  string
}

func (r *NetAddress) Destroy() {
	FfiDestroyerAddressProtocol{}.Destroy(r.Protocol)
	FfiDestroyerString{}.Destroy(r.Address)
}

type FfiConverterNetAddress struct{}

var FfiConverterNetAddressINSTANCE = FfiConverterNetAddress{}

func (c FfiConverterNetAddress) Lift(rb RustBufferI) NetAddress {
	return LiftFromRustBuffer[NetAddress](c, rb)
}

func (c FfiConverterNetAddress) Read(reader io.Reader) NetAddress {
	return NetAddress{
		FfiConverterAddressProtocolINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterNetAddress) Lower(value NetAddress) C.RustBuffer {
	return LowerIntoRustBuffer[NetAddress](c, value)
}

func (c FfiConverterNetAddress) LowerExternal(value NetAddress) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[NetAddress](c, value))
}

func (c FfiConverterNetAddress) Write(writer io.Writer, value NetAddress) {
	FfiConverterAddressProtocolINSTANCE.Write(writer, value.Protocol)
	FfiConverterStringINSTANCE.Write(writer, value.Address)
}

type FfiDestroyerNetAddress struct{}

func (_ FfiDestroyerNetAddress) Destroy(value NetAddress) {
	value.Destroy()
}

// An ObjectEvent represents an object and whether it was deleted or not.
type ObjectEvent struct {
	Id        string
	Deleted   bool
	UpdatedAt time.Time
	Object    **PinnedObject
}

func (r *ObjectEvent) Destroy() {
	FfiDestroyerString{}.Destroy(r.Id)
	FfiDestroyerBool{}.Destroy(r.Deleted)
	FfiDestroyerTimestamp{}.Destroy(r.UpdatedAt)
	FfiDestroyerOptionalPinnedObject{}.Destroy(r.Object)
}

type FfiConverterObjectEvent struct{}

var FfiConverterObjectEventINSTANCE = FfiConverterObjectEvent{}

func (c FfiConverterObjectEvent) Lift(rb RustBufferI) ObjectEvent {
	return LiftFromRustBuffer[ObjectEvent](c, rb)
}

func (c FfiConverterObjectEvent) Read(reader io.Reader) ObjectEvent {
	return ObjectEvent{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
		FfiConverterOptionalPinnedObjectINSTANCE.Read(reader),
	}
}

func (c FfiConverterObjectEvent) Lower(value ObjectEvent) C.RustBuffer {
	return LowerIntoRustBuffer[ObjectEvent](c, value)
}

func (c FfiConverterObjectEvent) LowerExternal(value ObjectEvent) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ObjectEvent](c, value))
}

func (c FfiConverterObjectEvent) Write(writer io.Writer, value ObjectEvent) {
	FfiConverterStringINSTANCE.Write(writer, value.Id)
	FfiConverterBoolINSTANCE.Write(writer, value.Deleted)
	FfiConverterTimestampINSTANCE.Write(writer, value.UpdatedAt)
	FfiConverterOptionalPinnedObjectINSTANCE.Write(writer, value.Object)
}

type FfiDestroyerObjectEvent struct{}

func (_ FfiDestroyerObjectEvent) Destroy(value ObjectEvent) {
	value.Destroy()
}

// Used to paginate through objects stored in the indexer.
//
// When syncing changes from an indexer, `after` should be set to the
// last `updated_at` timestamp seen, and `key` should be set to the
// last object's key seen.
type ObjectsCursor struct {
	Id    string
	After time.Time
}

func (r *ObjectsCursor) Destroy() {
	FfiDestroyerString{}.Destroy(r.Id)
	FfiDestroyerTimestamp{}.Destroy(r.After)
}

type FfiConverterObjectsCursor struct{}

var FfiConverterObjectsCursorINSTANCE = FfiConverterObjectsCursor{}

func (c FfiConverterObjectsCursor) Lift(rb RustBufferI) ObjectsCursor {
	return LiftFromRustBuffer[ObjectsCursor](c, rb)
}

func (c FfiConverterObjectsCursor) Read(reader io.Reader) ObjectsCursor {
	return ObjectsCursor{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
	}
}

func (c FfiConverterObjectsCursor) Lower(value ObjectsCursor) C.RustBuffer {
	return LowerIntoRustBuffer[ObjectsCursor](c, value)
}

func (c FfiConverterObjectsCursor) LowerExternal(value ObjectsCursor) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ObjectsCursor](c, value))
}

func (c FfiConverterObjectsCursor) Write(writer io.Writer, value ObjectsCursor) {
	FfiConverterStringINSTANCE.Write(writer, value.Id)
	FfiConverterTimestampINSTANCE.Write(writer, value.After)
}

type FfiDestroyerObjectsCursor struct{}

func (_ FfiDestroyerObjectsCursor) Destroy(value ObjectsCursor) {
	value.Destroy()
}

// A sector stored on a specific host.
type PinnedSector struct {
	Root    string
	HostKey string
}

func (r *PinnedSector) Destroy() {
	FfiDestroyerString{}.Destroy(r.Root)
	FfiDestroyerString{}.Destroy(r.HostKey)
}

type FfiConverterPinnedSector struct{}

var FfiConverterPinnedSectorINSTANCE = FfiConverterPinnedSector{}

func (c FfiConverterPinnedSector) Lift(rb RustBufferI) PinnedSector {
	return LiftFromRustBuffer[PinnedSector](c, rb)
}

func (c FfiConverterPinnedSector) Read(reader io.Reader) PinnedSector {
	return PinnedSector{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterPinnedSector) Lower(value PinnedSector) C.RustBuffer {
	return LowerIntoRustBuffer[PinnedSector](c, value)
}

func (c FfiConverterPinnedSector) LowerExternal(value PinnedSector) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[PinnedSector](c, value))
}

func (c FfiConverterPinnedSector) Write(writer io.Writer, value PinnedSector) {
	FfiConverterStringINSTANCE.Write(writer, value.Root)
	FfiConverterStringINSTANCE.Write(writer, value.HostKey)
}

type FfiDestroyerPinnedSector struct{}

func (_ FfiDestroyerPinnedSector) Destroy(value PinnedSector) {
	value.Destroy()
}

// A PinnedSlab represents a slab that has been pinned to the indexer.
type PinnedSlab struct {
	Id            string
	EncryptionKey []byte
	MinShards     uint8
	Sectors       []PinnedSector
}

func (r *PinnedSlab) Destroy() {
	FfiDestroyerString{}.Destroy(r.Id)
	FfiDestroyerBytes{}.Destroy(r.EncryptionKey)
	FfiDestroyerUint8{}.Destroy(r.MinShards)
	FfiDestroyerSequencePinnedSector{}.Destroy(r.Sectors)
}

type FfiConverterPinnedSlab struct{}

var FfiConverterPinnedSlabINSTANCE = FfiConverterPinnedSlab{}

func (c FfiConverterPinnedSlab) Lift(rb RustBufferI) PinnedSlab {
	return LiftFromRustBuffer[PinnedSlab](c, rb)
}

func (c FfiConverterPinnedSlab) Read(reader io.Reader) PinnedSlab {
	return PinnedSlab{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterUint8INSTANCE.Read(reader),
		FfiConverterSequencePinnedSectorINSTANCE.Read(reader),
	}
}

func (c FfiConverterPinnedSlab) Lower(value PinnedSlab) C.RustBuffer {
	return LowerIntoRustBuffer[PinnedSlab](c, value)
}

func (c FfiConverterPinnedSlab) LowerExternal(value PinnedSlab) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[PinnedSlab](c, value))
}

func (c FfiConverterPinnedSlab) Write(writer io.Writer, value PinnedSlab) {
	FfiConverterStringINSTANCE.Write(writer, value.Id)
	FfiConverterBytesINSTANCE.Write(writer, value.EncryptionKey)
	FfiConverterUint8INSTANCE.Write(writer, value.MinShards)
	FfiConverterSequencePinnedSectorINSTANCE.Write(writer, value.Sectors)
}

type FfiDestroyerPinnedSlab struct{}

func (_ FfiDestroyerPinnedSlab) Destroy(value PinnedSlab) {
	value.Destroy()
}

// A sealed object represents an object that has been encrypted
// for secure offline storage or processing. It can be opened using
// an app key to retrieve the original object.
type SealedObject struct {
	Id                   string
	EncryptedDataKey     []byte
	EncryptedMetadataKey []byte
	Slabs                []Slab
	EncryptedMetadata    []byte
	DataSignature        []byte
	MetadataSignature    []byte
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

func (r *SealedObject) Destroy() {
	FfiDestroyerString{}.Destroy(r.Id)
	FfiDestroyerBytes{}.Destroy(r.EncryptedDataKey)
	FfiDestroyerBytes{}.Destroy(r.EncryptedMetadataKey)
	FfiDestroyerSequenceSlab{}.Destroy(r.Slabs)
	FfiDestroyerBytes{}.Destroy(r.EncryptedMetadata)
	FfiDestroyerBytes{}.Destroy(r.DataSignature)
	FfiDestroyerBytes{}.Destroy(r.MetadataSignature)
	FfiDestroyerTimestamp{}.Destroy(r.CreatedAt)
	FfiDestroyerTimestamp{}.Destroy(r.UpdatedAt)
}

type FfiConverterSealedObject struct{}

var FfiConverterSealedObjectINSTANCE = FfiConverterSealedObject{}

func (c FfiConverterSealedObject) Lift(rb RustBufferI) SealedObject {
	return LiftFromRustBuffer[SealedObject](c, rb)
}

func (c FfiConverterSealedObject) Read(reader io.Reader) SealedObject {
	return SealedObject{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterSequenceSlabINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
	}
}

func (c FfiConverterSealedObject) Lower(value SealedObject) C.RustBuffer {
	return LowerIntoRustBuffer[SealedObject](c, value)
}

func (c FfiConverterSealedObject) LowerExternal(value SealedObject) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[SealedObject](c, value))
}

func (c FfiConverterSealedObject) Write(writer io.Writer, value SealedObject) {
	FfiConverterStringINSTANCE.Write(writer, value.Id)
	FfiConverterBytesINSTANCE.Write(writer, value.EncryptedDataKey)
	FfiConverterBytesINSTANCE.Write(writer, value.EncryptedMetadataKey)
	FfiConverterSequenceSlabINSTANCE.Write(writer, value.Slabs)
	FfiConverterBytesINSTANCE.Write(writer, value.EncryptedMetadata)
	FfiConverterBytesINSTANCE.Write(writer, value.DataSignature)
	FfiConverterBytesINSTANCE.Write(writer, value.MetadataSignature)
	FfiConverterTimestampINSTANCE.Write(writer, value.CreatedAt)
	FfiConverterTimestampINSTANCE.Write(writer, value.UpdatedAt)
}

type FfiDestroyerSealedObject struct{}

func (_ FfiDestroyerSealedObject) Destroy(value SealedObject) {
	value.Destroy()
}

// Information about a successfully uploaded or downloaded shard.
type ShardProgress struct {
	HostKey    string
	ShardSize  uint64
	ShardIndex uint32
	SlabIndex  uint32
	ElapsedMs  uint64
}

func (r *ShardProgress) Destroy() {
	FfiDestroyerString{}.Destroy(r.HostKey)
	FfiDestroyerUint64{}.Destroy(r.ShardSize)
	FfiDestroyerUint32{}.Destroy(r.ShardIndex)
	FfiDestroyerUint32{}.Destroy(r.SlabIndex)
	FfiDestroyerUint64{}.Destroy(r.ElapsedMs)
}

type FfiConverterShardProgress struct{}

var FfiConverterShardProgressINSTANCE = FfiConverterShardProgress{}

func (c FfiConverterShardProgress) Lift(rb RustBufferI) ShardProgress {
	return LiftFromRustBuffer[ShardProgress](c, rb)
}

func (c FfiConverterShardProgress) Read(reader io.Reader) ShardProgress {
	return ShardProgress{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
	}
}

func (c FfiConverterShardProgress) Lower(value ShardProgress) C.RustBuffer {
	return LowerIntoRustBuffer[ShardProgress](c, value)
}

func (c FfiConverterShardProgress) LowerExternal(value ShardProgress) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ShardProgress](c, value))
}

func (c FfiConverterShardProgress) Write(writer io.Writer, value ShardProgress) {
	FfiConverterStringINSTANCE.Write(writer, value.HostKey)
	FfiConverterUint64INSTANCE.Write(writer, value.ShardSize)
	FfiConverterUint32INSTANCE.Write(writer, value.ShardIndex)
	FfiConverterUint32INSTANCE.Write(writer, value.SlabIndex)
	FfiConverterUint64INSTANCE.Write(writer, value.ElapsedMs)
}

type FfiDestroyerShardProgress struct{}

func (_ FfiDestroyerShardProgress) Destroy(value ShardProgress) {
	value.Destroy()
}

// A Slab represents a contiguous erasure-coded segment of a file stored on the Sia network.
type Slab struct {
	EncryptionKey []byte
	MinShards     uint8
	Sectors       []PinnedSector
	Offset        uint32
	Length        uint32
}

func (r *Slab) Destroy() {
	FfiDestroyerBytes{}.Destroy(r.EncryptionKey)
	FfiDestroyerUint8{}.Destroy(r.MinShards)
	FfiDestroyerSequencePinnedSector{}.Destroy(r.Sectors)
	FfiDestroyerUint32{}.Destroy(r.Offset)
	FfiDestroyerUint32{}.Destroy(r.Length)
}

type FfiConverterSlab struct{}

var FfiConverterSlabINSTANCE = FfiConverterSlab{}

func (c FfiConverterSlab) Lift(rb RustBufferI) Slab {
	return LiftFromRustBuffer[Slab](c, rb)
}

func (c FfiConverterSlab) Read(reader io.Reader) Slab {
	return Slab{
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterUint8INSTANCE.Read(reader),
		FfiConverterSequencePinnedSectorINSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
	}
}

func (c FfiConverterSlab) Lower(value Slab) C.RustBuffer {
	return LowerIntoRustBuffer[Slab](c, value)
}

func (c FfiConverterSlab) LowerExternal(value Slab) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Slab](c, value))
}

func (c FfiConverterSlab) Write(writer io.Writer, value Slab) {
	FfiConverterBytesINSTANCE.Write(writer, value.EncryptionKey)
	FfiConverterUint8INSTANCE.Write(writer, value.MinShards)
	FfiConverterSequencePinnedSectorINSTANCE.Write(writer, value.Sectors)
	FfiConverterUint32INSTANCE.Write(writer, value.Offset)
	FfiConverterUint32INSTANCE.Write(writer, value.Length)
}

type FfiDestroyerSlab struct{}

func (_ FfiDestroyerSlab) Destroy(value Slab) {
	value.Destroy()
}

// Provides options for an upload operation.
type UploadOptions struct {
	MaxBufferedSlabs *uint32
	DataShards       *uint8
	ParityShards     *uint8
	// Optional callback to report upload progress.
	ShardUploaded *ProgressCallback
}

func (r *UploadOptions) Destroy() {
	FfiDestroyerOptionalUint32{}.Destroy(r.MaxBufferedSlabs)
	FfiDestroyerOptionalUint8{}.Destroy(r.DataShards)
	FfiDestroyerOptionalUint8{}.Destroy(r.ParityShards)
	FfiDestroyerOptionalProgressCallback{}.Destroy(r.ShardUploaded)
}

type FfiConverterUploadOptions struct{}

var FfiConverterUploadOptionsINSTANCE = FfiConverterUploadOptions{}

func (c FfiConverterUploadOptions) Lift(rb RustBufferI) UploadOptions {
	return LiftFromRustBuffer[UploadOptions](c, rb)
}

func (c FfiConverterUploadOptions) Read(reader io.Reader) UploadOptions {
	return UploadOptions{
		FfiConverterOptionalUint32INSTANCE.Read(reader),
		FfiConverterOptionalUint8INSTANCE.Read(reader),
		FfiConverterOptionalUint8INSTANCE.Read(reader),
		FfiConverterOptionalProgressCallbackINSTANCE.Read(reader),
	}
}

func (c FfiConverterUploadOptions) Lower(value UploadOptions) C.RustBuffer {
	return LowerIntoRustBuffer[UploadOptions](c, value)
}

func (c FfiConverterUploadOptions) LowerExternal(value UploadOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[UploadOptions](c, value))
}

func (c FfiConverterUploadOptions) Write(writer io.Writer, value UploadOptions) {
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.MaxBufferedSlabs)
	FfiConverterOptionalUint8INSTANCE.Write(writer, value.DataShards)
	FfiConverterOptionalUint8INSTANCE.Write(writer, value.ParityShards)
	FfiConverterOptionalProgressCallbackINSTANCE.Write(writer, value.ShardUploaded)
}

type FfiDestroyerUploadOptions struct{}

func (_ FfiDestroyerUploadOptions) Destroy(value UploadOptions) {
	value.Destroy()
}

// The protocol used in a network address.
type AddressProtocol uint

const (
	AddressProtocolSiaMux AddressProtocol = 1
	AddressProtocolQuic   AddressProtocol = 2
)

type FfiConverterAddressProtocol struct{}

var FfiConverterAddressProtocolINSTANCE = FfiConverterAddressProtocol{}

func (c FfiConverterAddressProtocol) Lift(rb RustBufferI) AddressProtocol {
	return LiftFromRustBuffer[AddressProtocol](c, rb)
}

func (c FfiConverterAddressProtocol) Lower(value AddressProtocol) C.RustBuffer {
	return LowerIntoRustBuffer[AddressProtocol](c, value)
}

func (c FfiConverterAddressProtocol) LowerExternal(value AddressProtocol) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[AddressProtocol](c, value))
}
func (FfiConverterAddressProtocol) Read(reader io.Reader) AddressProtocol {
	id := readInt32(reader)
	return AddressProtocol(id)
}

func (FfiConverterAddressProtocol) Write(writer io.Writer, value AddressProtocol) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerAddressProtocol struct{}

func (_ FfiDestroyerAddressProtocol) Destroy(value AddressProtocol) {
}

type AppKeyError struct {
	err error
}

// Convience method to turn *AppKeyError into error
// Avoiding treating nil pointer as non nil error interface
func (err *AppKeyError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err AppKeyError) Error() string {
	return fmt.Sprintf("AppKeyError: %s", err.err.Error())
}

func (err AppKeyError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrAppKeyErrorInvalidLength = fmt.Errorf("AppKeyErrorInvalidLength")
var ErrAppKeyErrorSignatureLength = fmt.Errorf("AppKeyErrorSignatureLength")

// Variant structs
type AppKeyErrorInvalidLength struct {
	message string
}

func NewAppKeyErrorInvalidLength() *AppKeyError {
	return &AppKeyError{err: &AppKeyErrorInvalidLength{}}
}

func (e AppKeyErrorInvalidLength) destroy() {
}

func (err AppKeyErrorInvalidLength) Error() string {
	return fmt.Sprintf("InvalidLength: %s", err.message)
}

func (self AppKeyErrorInvalidLength) Is(target error) bool {
	return target == ErrAppKeyErrorInvalidLength
}

type AppKeyErrorSignatureLength struct {
	message string
}

func NewAppKeyErrorSignatureLength() *AppKeyError {
	return &AppKeyError{err: &AppKeyErrorSignatureLength{}}
}

func (e AppKeyErrorSignatureLength) destroy() {
}

func (err AppKeyErrorSignatureLength) Error() string {
	return fmt.Sprintf("SignatureLength: %s", err.message)
}

func (self AppKeyErrorSignatureLength) Is(target error) bool {
	return target == ErrAppKeyErrorSignatureLength
}

type FfiConverterAppKeyError struct{}

var FfiConverterAppKeyErrorINSTANCE = FfiConverterAppKeyError{}

func (c FfiConverterAppKeyError) Lift(eb RustBufferI) *AppKeyError {
	return LiftFromRustBuffer[*AppKeyError](c, eb)
}

func (c FfiConverterAppKeyError) Lower(value *AppKeyError) C.RustBuffer {
	return LowerIntoRustBuffer[*AppKeyError](c, value)
}

func (c FfiConverterAppKeyError) LowerExternal(value *AppKeyError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*AppKeyError](c, value))
}

func (c FfiConverterAppKeyError) Read(reader io.Reader) *AppKeyError {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &AppKeyError{&AppKeyErrorInvalidLength{message}}
	case 2:
		return &AppKeyError{&AppKeyErrorSignatureLength{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterAppKeyError.Read()", errorID))
	}

}

func (c FfiConverterAppKeyError) Write(writer io.Writer, value *AppKeyError) {
	switch variantValue := value.err.(type) {
	case *AppKeyErrorInvalidLength:
		writeInt32(writer, 1)
	case *AppKeyErrorSignatureLength:
		writeInt32(writer, 2)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterAppKeyError.Write", value))
	}
}

type FfiDestroyerAppKeyError struct{}

func (_ FfiDestroyerAppKeyError) Destroy(value *AppKeyError) {
	switch variantValue := value.err.(type) {
	case AppKeyErrorInvalidLength:
		variantValue.destroy()
	case AppKeyErrorSignatureLength:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerAppKeyError.Destroy", value))
	}
}

type BuilderError struct {
	err error
}

// Convience method to turn *BuilderError into error
// Avoiding treating nil pointer as non nil error interface
func (err *BuilderError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err BuilderError) Error() string {
	return fmt.Sprintf("BuilderError: %s", err.err.Error())
}

func (err BuilderError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrBuilderErrorError = fmt.Errorf("BuilderErrorError")
var ErrBuilderErrorInvalidState = fmt.Errorf("BuilderErrorInvalidState")
var ErrBuilderErrorCrypto = fmt.Errorf("BuilderErrorCrypto")
var ErrBuilderErrorJoinError = fmt.Errorf("BuilderErrorJoinError")
var ErrBuilderErrorCustom = fmt.Errorf("BuilderErrorCustom")

// Variant structs
type BuilderErrorError struct {
	message string
}

func NewBuilderErrorError() *BuilderError {
	return &BuilderError{err: &BuilderErrorError{}}
}

func (e BuilderErrorError) destroy() {
}

func (err BuilderErrorError) Error() string {
	return fmt.Sprintf("Error: %s", err.message)
}

func (self BuilderErrorError) Is(target error) bool {
	return target == ErrBuilderErrorError
}

type BuilderErrorInvalidState struct {
	message string
}

func NewBuilderErrorInvalidState() *BuilderError {
	return &BuilderError{err: &BuilderErrorInvalidState{}}
}

func (e BuilderErrorInvalidState) destroy() {
}

func (err BuilderErrorInvalidState) Error() string {
	return fmt.Sprintf("InvalidState: %s", err.message)
}

func (self BuilderErrorInvalidState) Is(target error) bool {
	return target == ErrBuilderErrorInvalidState
}

type BuilderErrorCrypto struct {
	message string
}

func NewBuilderErrorCrypto() *BuilderError {
	return &BuilderError{err: &BuilderErrorCrypto{}}
}

func (e BuilderErrorCrypto) destroy() {
}

func (err BuilderErrorCrypto) Error() string {
	return fmt.Sprintf("Crypto: %s", err.message)
}

func (self BuilderErrorCrypto) Is(target error) bool {
	return target == ErrBuilderErrorCrypto
}

type BuilderErrorJoinError struct {
	message string
}

func NewBuilderErrorJoinError() *BuilderError {
	return &BuilderError{err: &BuilderErrorJoinError{}}
}

func (e BuilderErrorJoinError) destroy() {
}

func (err BuilderErrorJoinError) Error() string {
	return fmt.Sprintf("JoinError: %s", err.message)
}

func (self BuilderErrorJoinError) Is(target error) bool {
	return target == ErrBuilderErrorJoinError
}

type BuilderErrorCustom struct {
	message string
}

func NewBuilderErrorCustom() *BuilderError {
	return &BuilderError{err: &BuilderErrorCustom{}}
}

func (e BuilderErrorCustom) destroy() {
}

func (err BuilderErrorCustom) Error() string {
	return fmt.Sprintf("Custom: %s", err.message)
}

func (self BuilderErrorCustom) Is(target error) bool {
	return target == ErrBuilderErrorCustom
}

type FfiConverterBuilderError struct{}

var FfiConverterBuilderErrorINSTANCE = FfiConverterBuilderError{}

func (c FfiConverterBuilderError) Lift(eb RustBufferI) *BuilderError {
	return LiftFromRustBuffer[*BuilderError](c, eb)
}

func (c FfiConverterBuilderError) Lower(value *BuilderError) C.RustBuffer {
	return LowerIntoRustBuffer[*BuilderError](c, value)
}

func (c FfiConverterBuilderError) LowerExternal(value *BuilderError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*BuilderError](c, value))
}

func (c FfiConverterBuilderError) Read(reader io.Reader) *BuilderError {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &BuilderError{&BuilderErrorError{message}}
	case 2:
		return &BuilderError{&BuilderErrorInvalidState{message}}
	case 3:
		return &BuilderError{&BuilderErrorCrypto{message}}
	case 4:
		return &BuilderError{&BuilderErrorJoinError{message}}
	case 5:
		return &BuilderError{&BuilderErrorCustom{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterBuilderError.Read()", errorID))
	}

}

func (c FfiConverterBuilderError) Write(writer io.Writer, value *BuilderError) {
	switch variantValue := value.err.(type) {
	case *BuilderErrorError:
		writeInt32(writer, 1)
	case *BuilderErrorInvalidState:
		writeInt32(writer, 2)
	case *BuilderErrorCrypto:
		writeInt32(writer, 3)
	case *BuilderErrorJoinError:
		writeInt32(writer, 4)
	case *BuilderErrorCustom:
		writeInt32(writer, 5)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterBuilderError.Write", value))
	}
}

type FfiDestroyerBuilderError struct{}

func (_ FfiDestroyerBuilderError) Destroy(value *BuilderError) {
	switch variantValue := value.err.(type) {
	case BuilderErrorError:
		variantValue.destroy()
	case BuilderErrorInvalidState:
		variantValue.destroy()
	case BuilderErrorCrypto:
		variantValue.destroy()
	case BuilderErrorJoinError:
		variantValue.destroy()
	case BuilderErrorCustom:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerBuilderError.Destroy", value))
	}
}

type ConnectError struct {
	err error
}

// Convience method to turn *ConnectError into error
// Avoiding treating nil pointer as non nil error interface
func (err *ConnectError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err ConnectError) Error() string {
	return fmt.Sprintf("ConnectError: %s", err.err.Error())
}

func (err ConnectError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrConnectErrorAppClient = fmt.Errorf("ConnectErrorAppClient")
var ErrConnectErrorJoinError = fmt.Errorf("ConnectErrorJoinError")
var ErrConnectErrorCustom = fmt.Errorf("ConnectErrorCustom")

// Variant structs
type ConnectErrorAppClient struct {
	message string
}

func NewConnectErrorAppClient() *ConnectError {
	return &ConnectError{err: &ConnectErrorAppClient{}}
}

func (e ConnectErrorAppClient) destroy() {
}

func (err ConnectErrorAppClient) Error() string {
	return fmt.Sprintf("AppClient: %s", err.message)
}

func (self ConnectErrorAppClient) Is(target error) bool {
	return target == ErrConnectErrorAppClient
}

type ConnectErrorJoinError struct {
	message string
}

func NewConnectErrorJoinError() *ConnectError {
	return &ConnectError{err: &ConnectErrorJoinError{}}
}

func (e ConnectErrorJoinError) destroy() {
}

func (err ConnectErrorJoinError) Error() string {
	return fmt.Sprintf("JoinError: %s", err.message)
}

func (self ConnectErrorJoinError) Is(target error) bool {
	return target == ErrConnectErrorJoinError
}

type ConnectErrorCustom struct {
	message string
}

func NewConnectErrorCustom() *ConnectError {
	return &ConnectError{err: &ConnectErrorCustom{}}
}

func (e ConnectErrorCustom) destroy() {
}

func (err ConnectErrorCustom) Error() string {
	return fmt.Sprintf("Custom: %s", err.message)
}

func (self ConnectErrorCustom) Is(target error) bool {
	return target == ErrConnectErrorCustom
}

type FfiConverterConnectError struct{}

var FfiConverterConnectErrorINSTANCE = FfiConverterConnectError{}

func (c FfiConverterConnectError) Lift(eb RustBufferI) *ConnectError {
	return LiftFromRustBuffer[*ConnectError](c, eb)
}

func (c FfiConverterConnectError) Lower(value *ConnectError) C.RustBuffer {
	return LowerIntoRustBuffer[*ConnectError](c, value)
}

func (c FfiConverterConnectError) LowerExternal(value *ConnectError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*ConnectError](c, value))
}

func (c FfiConverterConnectError) Read(reader io.Reader) *ConnectError {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &ConnectError{&ConnectErrorAppClient{message}}
	case 2:
		return &ConnectError{&ConnectErrorJoinError{message}}
	case 3:
		return &ConnectError{&ConnectErrorCustom{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterConnectError.Read()", errorID))
	}

}

func (c FfiConverterConnectError) Write(writer io.Writer, value *ConnectError) {
	switch variantValue := value.err.(type) {
	case *ConnectErrorAppClient:
		writeInt32(writer, 1)
	case *ConnectErrorJoinError:
		writeInt32(writer, 2)
	case *ConnectErrorCustom:
		writeInt32(writer, 3)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterConnectError.Write", value))
	}
}

type FfiDestroyerConnectError struct{}

func (_ FfiDestroyerConnectError) Destroy(value *ConnectError) {
	switch variantValue := value.err.(type) {
	case ConnectErrorAppClient:
		variantValue.destroy()
	case ConnectErrorJoinError:
		variantValue.destroy()
	case ConnectErrorCustom:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerConnectError.Destroy", value))
	}
}

type DownloadError struct {
	err error
}

// Convience method to turn *DownloadError into error
// Avoiding treating nil pointer as non nil error interface
func (err *DownloadError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err DownloadError) Error() string {
	return fmt.Sprintf("DownloadError: %s", err.err.Error())
}

func (err DownloadError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrDownloadErrorDownload = fmt.Errorf("DownloadErrorDownload")
var ErrDownloadErrorJoinError = fmt.Errorf("DownloadErrorJoinError")
var ErrDownloadErrorCancelled = fmt.Errorf("DownloadErrorCancelled")

// Variant structs
type DownloadErrorDownload struct {
	message string
}

func NewDownloadErrorDownload() *DownloadError {
	return &DownloadError{err: &DownloadErrorDownload{}}
}

func (e DownloadErrorDownload) destroy() {
}

func (err DownloadErrorDownload) Error() string {
	return fmt.Sprintf("Download: %s", err.message)
}

func (self DownloadErrorDownload) Is(target error) bool {
	return target == ErrDownloadErrorDownload
}

type DownloadErrorJoinError struct {
	message string
}

func NewDownloadErrorJoinError() *DownloadError {
	return &DownloadError{err: &DownloadErrorJoinError{}}
}

func (e DownloadErrorJoinError) destroy() {
}

func (err DownloadErrorJoinError) Error() string {
	return fmt.Sprintf("JoinError: %s", err.message)
}

func (self DownloadErrorJoinError) Is(target error) bool {
	return target == ErrDownloadErrorJoinError
}

type DownloadErrorCancelled struct {
	message string
}

func NewDownloadErrorCancelled() *DownloadError {
	return &DownloadError{err: &DownloadErrorCancelled{}}
}

func (e DownloadErrorCancelled) destroy() {
}

func (err DownloadErrorCancelled) Error() string {
	return fmt.Sprintf("Cancelled: %s", err.message)
}

func (self DownloadErrorCancelled) Is(target error) bool {
	return target == ErrDownloadErrorCancelled
}

type FfiConverterDownloadError struct{}

var FfiConverterDownloadErrorINSTANCE = FfiConverterDownloadError{}

func (c FfiConverterDownloadError) Lift(eb RustBufferI) *DownloadError {
	return LiftFromRustBuffer[*DownloadError](c, eb)
}

func (c FfiConverterDownloadError) Lower(value *DownloadError) C.RustBuffer {
	return LowerIntoRustBuffer[*DownloadError](c, value)
}

func (c FfiConverterDownloadError) LowerExternal(value *DownloadError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*DownloadError](c, value))
}

func (c FfiConverterDownloadError) Read(reader io.Reader) *DownloadError {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &DownloadError{&DownloadErrorDownload{message}}
	case 2:
		return &DownloadError{&DownloadErrorJoinError{message}}
	case 3:
		return &DownloadError{&DownloadErrorCancelled{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterDownloadError.Read()", errorID))
	}

}

func (c FfiConverterDownloadError) Write(writer io.Writer, value *DownloadError) {
	switch variantValue := value.err.(type) {
	case *DownloadErrorDownload:
		writeInt32(writer, 1)
	case *DownloadErrorJoinError:
		writeInt32(writer, 2)
	case *DownloadErrorCancelled:
		writeInt32(writer, 3)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterDownloadError.Write", value))
	}
}

type FfiDestroyerDownloadError struct{}

func (_ FfiDestroyerDownloadError) Destroy(value *DownloadError) {
	switch variantValue := value.err.(type) {
	case DownloadErrorDownload:
		variantValue.destroy()
	case DownloadErrorJoinError:
		variantValue.destroy()
	case DownloadErrorCancelled:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerDownloadError.Destroy", value))
	}
}

type Error struct {
	err error
}

// Convience method to turn *Error into error
// Avoiding treating nil pointer as non nil error interface
func (err *Error) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err Error) Error() string {
	return fmt.Sprintf("Error: %s", err.err.Error())
}

func (err Error) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrErrorSdk = fmt.Errorf("ErrorSdk")
var ErrErrorHexParseError = fmt.Errorf("ErrorHexParseError")
var ErrErrorSealedObject = fmt.Errorf("ErrorSealedObject")
var ErrErrorJoinError = fmt.Errorf("ErrorJoinError")
var ErrErrorCustom = fmt.Errorf("ErrorCustom")

// Variant structs
type ErrorSdk struct {
	message string
}

func NewErrorSdk() *Error {
	return &Error{err: &ErrorSdk{}}
}

func (e ErrorSdk) destroy() {
}

func (err ErrorSdk) Error() string {
	return fmt.Sprintf("Sdk: %s", err.message)
}

func (self ErrorSdk) Is(target error) bool {
	return target == ErrErrorSdk
}

type ErrorHexParseError struct {
	message string
}

func NewErrorHexParseError() *Error {
	return &Error{err: &ErrorHexParseError{}}
}

func (e ErrorHexParseError) destroy() {
}

func (err ErrorHexParseError) Error() string {
	return fmt.Sprintf("HexParseError: %s", err.message)
}

func (self ErrorHexParseError) Is(target error) bool {
	return target == ErrErrorHexParseError
}

type ErrorSealedObject struct {
	message string
}

func NewErrorSealedObject() *Error {
	return &Error{err: &ErrorSealedObject{}}
}

func (e ErrorSealedObject) destroy() {
}

func (err ErrorSealedObject) Error() string {
	return fmt.Sprintf("SealedObject: %s", err.message)
}

func (self ErrorSealedObject) Is(target error) bool {
	return target == ErrErrorSealedObject
}

type ErrorJoinError struct {
	message string
}

func NewErrorJoinError() *Error {
	return &Error{err: &ErrorJoinError{}}
}

func (e ErrorJoinError) destroy() {
}

func (err ErrorJoinError) Error() string {
	return fmt.Sprintf("JoinError: %s", err.message)
}

func (self ErrorJoinError) Is(target error) bool {
	return target == ErrErrorJoinError
}

type ErrorCustom struct {
	message string
}

func NewErrorCustom() *Error {
	return &Error{err: &ErrorCustom{}}
}

func (e ErrorCustom) destroy() {
}

func (err ErrorCustom) Error() string {
	return fmt.Sprintf("Custom: %s", err.message)
}

func (self ErrorCustom) Is(target error) bool {
	return target == ErrErrorCustom
}

type FfiConverterError struct{}

var FfiConverterErrorINSTANCE = FfiConverterError{}

func (c FfiConverterError) Lift(eb RustBufferI) *Error {
	return LiftFromRustBuffer[*Error](c, eb)
}

func (c FfiConverterError) Lower(value *Error) C.RustBuffer {
	return LowerIntoRustBuffer[*Error](c, value)
}

func (c FfiConverterError) LowerExternal(value *Error) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*Error](c, value))
}

func (c FfiConverterError) Read(reader io.Reader) *Error {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &Error{&ErrorSdk{message}}
	case 2:
		return &Error{&ErrorHexParseError{message}}
	case 3:
		return &Error{&ErrorSealedObject{message}}
	case 4:
		return &Error{&ErrorJoinError{message}}
	case 5:
		return &Error{&ErrorCustom{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterError.Read()", errorID))
	}

}

func (c FfiConverterError) Write(writer io.Writer, value *Error) {
	switch variantValue := value.err.(type) {
	case *ErrorSdk:
		writeInt32(writer, 1)
	case *ErrorHexParseError:
		writeInt32(writer, 2)
	case *ErrorSealedObject:
		writeInt32(writer, 3)
	case *ErrorJoinError:
		writeInt32(writer, 4)
	case *ErrorCustom:
		writeInt32(writer, 5)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterError.Write", value))
	}
}

type FfiDestroyerError struct{}

func (_ FfiDestroyerError) Destroy(value *Error) {
	switch variantValue := value.err.(type) {
	case ErrorSdk:
		variantValue.destroy()
	case ErrorHexParseError:
		variantValue.destroy()
	case ErrorSealedObject:
		variantValue.destroy()
	case ErrorJoinError:
		variantValue.destroy()
	case ErrorCustom:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerError.Destroy", value))
	}
}

type IoError struct {
	err error
}

// Convience method to turn *IoError into error
// Avoiding treating nil pointer as non nil error interface
func (err *IoError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err IoError) Error() string {
	return fmt.Sprintf("IoError: %s", err.err.Error())
}

func (err IoError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrIoErrorIo = fmt.Errorf("IoErrorIo")
var ErrIoErrorClosed = fmt.Errorf("IoErrorClosed")
var ErrIoErrorCancelled = fmt.Errorf("IoErrorCancelled")

// Variant structs
type IoErrorIo struct {
	message string
}

func NewIoErrorIo() *IoError {
	return &IoError{err: &IoErrorIo{}}
}

func (e IoErrorIo) destroy() {
}

func (err IoErrorIo) Error() string {
	return fmt.Sprintf("Io: %s", err.message)
}

func (self IoErrorIo) Is(target error) bool {
	return target == ErrIoErrorIo
}

type IoErrorClosed struct {
	message string
}

func NewIoErrorClosed() *IoError {
	return &IoError{err: &IoErrorClosed{}}
}

func (e IoErrorClosed) destroy() {
}

func (err IoErrorClosed) Error() string {
	return fmt.Sprintf("Closed: %s", err.message)
}

func (self IoErrorClosed) Is(target error) bool {
	return target == ErrIoErrorClosed
}

type IoErrorCancelled struct {
	message string
}

func NewIoErrorCancelled() *IoError {
	return &IoError{err: &IoErrorCancelled{}}
}

func (e IoErrorCancelled) destroy() {
}

func (err IoErrorCancelled) Error() string {
	return fmt.Sprintf("Cancelled: %s", err.message)
}

func (self IoErrorCancelled) Is(target error) bool {
	return target == ErrIoErrorCancelled
}

type FfiConverterIoError struct{}

var FfiConverterIoErrorINSTANCE = FfiConverterIoError{}

func (c FfiConverterIoError) Lift(eb RustBufferI) *IoError {
	return LiftFromRustBuffer[*IoError](c, eb)
}

func (c FfiConverterIoError) Lower(value *IoError) C.RustBuffer {
	return LowerIntoRustBuffer[*IoError](c, value)
}

func (c FfiConverterIoError) LowerExternal(value *IoError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*IoError](c, value))
}

func (c FfiConverterIoError) Read(reader io.Reader) *IoError {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &IoError{&IoErrorIo{message}}
	case 2:
		return &IoError{&IoErrorClosed{message}}
	case 3:
		return &IoError{&IoErrorCancelled{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterIoError.Read()", errorID))
	}

}

func (c FfiConverterIoError) Write(writer io.Writer, value *IoError) {
	switch variantValue := value.err.(type) {
	case *IoErrorIo:
		writeInt32(writer, 1)
	case *IoErrorClosed:
		writeInt32(writer, 2)
	case *IoErrorCancelled:
		writeInt32(writer, 3)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterIoError.Write", value))
	}
}

type FfiDestroyerIoError struct{}

func (_ FfiDestroyerIoError) Destroy(value *IoError) {
	switch variantValue := value.err.(type) {
	case IoErrorIo:
		variantValue.destroy()
	case IoErrorClosed:
		variantValue.destroy()
	case IoErrorCancelled:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerIoError.Destroy", value))
	}
}

type ObjectError struct {
	err error
}

// Convience method to turn *ObjectError into error
// Avoiding treating nil pointer as non nil error interface
func (err *ObjectError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err ObjectError) Error() string {
	return fmt.Sprintf("ObjectError: %s", err.err.Error())
}

func (err ObjectError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrObjectErrorSealedObject = fmt.Errorf("ObjectErrorSealedObject")
var ErrObjectErrorEncoding = fmt.Errorf("ObjectErrorEncoding")

// Variant structs
type ObjectErrorSealedObject struct {
	message string
}

func NewObjectErrorSealedObject() *ObjectError {
	return &ObjectError{err: &ObjectErrorSealedObject{}}
}

func (e ObjectErrorSealedObject) destroy() {
}

func (err ObjectErrorSealedObject) Error() string {
	return fmt.Sprintf("SealedObject: %s", err.message)
}

func (self ObjectErrorSealedObject) Is(target error) bool {
	return target == ErrObjectErrorSealedObject
}

type ObjectErrorEncoding struct {
	message string
}

func NewObjectErrorEncoding() *ObjectError {
	return &ObjectError{err: &ObjectErrorEncoding{}}
}

func (e ObjectErrorEncoding) destroy() {
}

func (err ObjectErrorEncoding) Error() string {
	return fmt.Sprintf("Encoding: %s", err.message)
}

func (self ObjectErrorEncoding) Is(target error) bool {
	return target == ErrObjectErrorEncoding
}

type FfiConverterObjectError struct{}

var FfiConverterObjectErrorINSTANCE = FfiConverterObjectError{}

func (c FfiConverterObjectError) Lift(eb RustBufferI) *ObjectError {
	return LiftFromRustBuffer[*ObjectError](c, eb)
}

func (c FfiConverterObjectError) Lower(value *ObjectError) C.RustBuffer {
	return LowerIntoRustBuffer[*ObjectError](c, value)
}

func (c FfiConverterObjectError) LowerExternal(value *ObjectError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*ObjectError](c, value))
}

func (c FfiConverterObjectError) Read(reader io.Reader) *ObjectError {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &ObjectError{&ObjectErrorSealedObject{message}}
	case 2:
		return &ObjectError{&ObjectErrorEncoding{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterObjectError.Read()", errorID))
	}

}

func (c FfiConverterObjectError) Write(writer io.Writer, value *ObjectError) {
	switch variantValue := value.err.(type) {
	case *ObjectErrorSealedObject:
		writeInt32(writer, 1)
	case *ObjectErrorEncoding:
		writeInt32(writer, 2)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterObjectError.Write", value))
	}
}

type FfiDestroyerObjectError struct{}

func (_ FfiDestroyerObjectError) Destroy(value *ObjectError) {
	switch variantValue := value.err.(type) {
	case ObjectErrorSealedObject:
		variantValue.destroy()
	case ObjectErrorEncoding:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerObjectError.Destroy", value))
	}
}

type SeedError struct {
	err error
}

// Convience method to turn *SeedError into error
// Avoiding treating nil pointer as non nil error interface
func (err *SeedError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err SeedError) Error() string {
	return fmt.Sprintf("SeedError: %s", err.err.Error())
}

func (err SeedError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrSeedErrorInvalidMnemonic = fmt.Errorf("SeedErrorInvalidMnemonic")

// Variant structs
type SeedErrorInvalidMnemonic struct {
	message string
}

func NewSeedErrorInvalidMnemonic() *SeedError {
	return &SeedError{err: &SeedErrorInvalidMnemonic{}}
}

func (e SeedErrorInvalidMnemonic) destroy() {
}

func (err SeedErrorInvalidMnemonic) Error() string {
	return fmt.Sprintf("InvalidMnemonic: %s", err.message)
}

func (self SeedErrorInvalidMnemonic) Is(target error) bool {
	return target == ErrSeedErrorInvalidMnemonic
}

type FfiConverterSeedError struct{}

var FfiConverterSeedErrorINSTANCE = FfiConverterSeedError{}

func (c FfiConverterSeedError) Lift(eb RustBufferI) *SeedError {
	return LiftFromRustBuffer[*SeedError](c, eb)
}

func (c FfiConverterSeedError) Lower(value *SeedError) C.RustBuffer {
	return LowerIntoRustBuffer[*SeedError](c, value)
}

func (c FfiConverterSeedError) LowerExternal(value *SeedError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*SeedError](c, value))
}

func (c FfiConverterSeedError) Read(reader io.Reader) *SeedError {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &SeedError{&SeedErrorInvalidMnemonic{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterSeedError.Read()", errorID))
	}

}

func (c FfiConverterSeedError) Write(writer io.Writer, value *SeedError) {
	switch variantValue := value.err.(type) {
	case *SeedErrorInvalidMnemonic:
		writeInt32(writer, 1)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterSeedError.Write", value))
	}
}

type FfiDestroyerSeedError struct{}

func (_ FfiDestroyerSeedError) Destroy(value *SeedError) {
	switch variantValue := value.err.(type) {
	case SeedErrorInvalidMnemonic:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerSeedError.Destroy", value))
	}
}

type UploadError struct {
	err error
}

// Convience method to turn *UploadError into error
// Avoiding treating nil pointer as non nil error interface
func (err *UploadError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err UploadError) Error() string {
	return fmt.Sprintf("UploadError: %s", err.err.Error())
}

func (err UploadError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrUploadErrorClosed = fmt.Errorf("UploadErrorClosed")
var ErrUploadErrorIo = fmt.Errorf("UploadErrorIo")
var ErrUploadErrorUpload = fmt.Errorf("UploadErrorUpload")
var ErrUploadErrorJoinError = fmt.Errorf("UploadErrorJoinError")
var ErrUploadErrorCustom = fmt.Errorf("UploadErrorCustom")

// Variant structs
type UploadErrorClosed struct {
	message string
}

func NewUploadErrorClosed() *UploadError {
	return &UploadError{err: &UploadErrorClosed{}}
}

func (e UploadErrorClosed) destroy() {
}

func (err UploadErrorClosed) Error() string {
	return fmt.Sprintf("Closed: %s", err.message)
}

func (self UploadErrorClosed) Is(target error) bool {
	return target == ErrUploadErrorClosed
}

type UploadErrorIo struct {
	message string
}

func NewUploadErrorIo() *UploadError {
	return &UploadError{err: &UploadErrorIo{}}
}

func (e UploadErrorIo) destroy() {
}

func (err UploadErrorIo) Error() string {
	return fmt.Sprintf("Io: %s", err.message)
}

func (self UploadErrorIo) Is(target error) bool {
	return target == ErrUploadErrorIo
}

type UploadErrorUpload struct {
	message string
}

func NewUploadErrorUpload() *UploadError {
	return &UploadError{err: &UploadErrorUpload{}}
}

func (e UploadErrorUpload) destroy() {
}

func (err UploadErrorUpload) Error() string {
	return fmt.Sprintf("Upload: %s", err.message)
}

func (self UploadErrorUpload) Is(target error) bool {
	return target == ErrUploadErrorUpload
}

type UploadErrorJoinError struct {
	message string
}

func NewUploadErrorJoinError() *UploadError {
	return &UploadError{err: &UploadErrorJoinError{}}
}

func (e UploadErrorJoinError) destroy() {
}

func (err UploadErrorJoinError) Error() string {
	return fmt.Sprintf("JoinError: %s", err.message)
}

func (self UploadErrorJoinError) Is(target error) bool {
	return target == ErrUploadErrorJoinError
}

type UploadErrorCustom struct {
	message string
}

func NewUploadErrorCustom() *UploadError {
	return &UploadError{err: &UploadErrorCustom{}}
}

func (e UploadErrorCustom) destroy() {
}

func (err UploadErrorCustom) Error() string {
	return fmt.Sprintf("Custom: %s", err.message)
}

func (self UploadErrorCustom) Is(target error) bool {
	return target == ErrUploadErrorCustom
}

type FfiConverterUploadError struct{}

var FfiConverterUploadErrorINSTANCE = FfiConverterUploadError{}

func (c FfiConverterUploadError) Lift(eb RustBufferI) *UploadError {
	return LiftFromRustBuffer[*UploadError](c, eb)
}

func (c FfiConverterUploadError) Lower(value *UploadError) C.RustBuffer {
	return LowerIntoRustBuffer[*UploadError](c, value)
}

func (c FfiConverterUploadError) LowerExternal(value *UploadError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*UploadError](c, value))
}

func (c FfiConverterUploadError) Read(reader io.Reader) *UploadError {
	errorID := readUint32(reader)

	message := FfiConverterStringINSTANCE.Read(reader)
	switch errorID {
	case 1:
		return &UploadError{&UploadErrorClosed{message}}
	case 2:
		return &UploadError{&UploadErrorIo{message}}
	case 3:
		return &UploadError{&UploadErrorUpload{message}}
	case 4:
		return &UploadError{&UploadErrorJoinError{message}}
	case 5:
		return &UploadError{&UploadErrorCustom{message}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterUploadError.Read()", errorID))
	}

}

func (c FfiConverterUploadError) Write(writer io.Writer, value *UploadError) {
	switch variantValue := value.err.(type) {
	case *UploadErrorClosed:
		writeInt32(writer, 1)
	case *UploadErrorIo:
		writeInt32(writer, 2)
	case *UploadErrorUpload:
		writeInt32(writer, 3)
	case *UploadErrorJoinError:
		writeInt32(writer, 4)
	case *UploadErrorCustom:
		writeInt32(writer, 5)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterUploadError.Write", value))
	}
}

type FfiDestroyerUploadError struct{}

func (_ FfiDestroyerUploadError) Destroy(value *UploadError) {
	switch variantValue := value.err.(type) {
	case UploadErrorClosed:
		variantValue.destroy()
	case UploadErrorIo:
		variantValue.destroy()
	case UploadErrorUpload:
		variantValue.destroy()
	case UploadErrorJoinError:
		variantValue.destroy()
	case UploadErrorCustom:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerUploadError.Destroy", value))
	}
}

type FfiConverterOptionalUint8 struct{}

var FfiConverterOptionalUint8INSTANCE = FfiConverterOptionalUint8{}

func (c FfiConverterOptionalUint8) Lift(rb RustBufferI) *uint8 {
	return LiftFromRustBuffer[*uint8](c, rb)
}

func (_ FfiConverterOptionalUint8) Read(reader io.Reader) *uint8 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint8INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint8) Lower(value *uint8) C.RustBuffer {
	return LowerIntoRustBuffer[*uint8](c, value)
}

func (c FfiConverterOptionalUint8) LowerExternal(value *uint8) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint8](c, value))
}

func (_ FfiConverterOptionalUint8) Write(writer io.Writer, value *uint8) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint8INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint8 struct{}

func (_ FfiDestroyerOptionalUint8) Destroy(value *uint8) {
	if value != nil {
		FfiDestroyerUint8{}.Destroy(*value)
	}
}

type FfiConverterOptionalUint32 struct{}

var FfiConverterOptionalUint32INSTANCE = FfiConverterOptionalUint32{}

func (c FfiConverterOptionalUint32) Lift(rb RustBufferI) *uint32 {
	return LiftFromRustBuffer[*uint32](c, rb)
}

func (_ FfiConverterOptionalUint32) Read(reader io.Reader) *uint32 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint32INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint32) Lower(value *uint32) C.RustBuffer {
	return LowerIntoRustBuffer[*uint32](c, value)
}

func (c FfiConverterOptionalUint32) LowerExternal(value *uint32) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint32](c, value))
}

func (_ FfiConverterOptionalUint32) Write(writer io.Writer, value *uint32) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint32INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint32 struct{}

func (_ FfiDestroyerOptionalUint32) Destroy(value *uint32) {
	if value != nil {
		FfiDestroyerUint32{}.Destroy(*value)
	}
}

type FfiConverterOptionalUint64 struct{}

var FfiConverterOptionalUint64INSTANCE = FfiConverterOptionalUint64{}

func (c FfiConverterOptionalUint64) Lift(rb RustBufferI) *uint64 {
	return LiftFromRustBuffer[*uint64](c, rb)
}

func (_ FfiConverterOptionalUint64) Read(reader io.Reader) *uint64 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint64INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint64) Lower(value *uint64) C.RustBuffer {
	return LowerIntoRustBuffer[*uint64](c, value)
}

func (c FfiConverterOptionalUint64) LowerExternal(value *uint64) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint64](c, value))
}

func (_ FfiConverterOptionalUint64) Write(writer io.Writer, value *uint64) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint64INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint64 struct{}

func (_ FfiDestroyerOptionalUint64) Destroy(value *uint64) {
	if value != nil {
		FfiDestroyerUint64{}.Destroy(*value)
	}
}

type FfiConverterOptionalString struct{}

var FfiConverterOptionalStringINSTANCE = FfiConverterOptionalString{}

func (c FfiConverterOptionalString) Lift(rb RustBufferI) *string {
	return LiftFromRustBuffer[*string](c, rb)
}

func (_ FfiConverterOptionalString) Read(reader io.Reader) *string {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterStringINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalString) Lower(value *string) C.RustBuffer {
	return LowerIntoRustBuffer[*string](c, value)
}

func (c FfiConverterOptionalString) LowerExternal(value *string) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*string](c, value))
}

func (_ FfiConverterOptionalString) Write(writer io.Writer, value *string) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalString struct{}

func (_ FfiDestroyerOptionalString) Destroy(value *string) {
	if value != nil {
		FfiDestroyerString{}.Destroy(*value)
	}
}

type FfiConverterOptionalPinnedObject struct{}

var FfiConverterOptionalPinnedObjectINSTANCE = FfiConverterOptionalPinnedObject{}

func (c FfiConverterOptionalPinnedObject) Lift(rb RustBufferI) **PinnedObject {
	return LiftFromRustBuffer[**PinnedObject](c, rb)
}

func (_ FfiConverterOptionalPinnedObject) Read(reader io.Reader) **PinnedObject {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterPinnedObjectINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalPinnedObject) Lower(value **PinnedObject) C.RustBuffer {
	return LowerIntoRustBuffer[**PinnedObject](c, value)
}

func (c FfiConverterOptionalPinnedObject) LowerExternal(value **PinnedObject) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[**PinnedObject](c, value))
}

func (_ FfiConverterOptionalPinnedObject) Write(writer io.Writer, value **PinnedObject) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterPinnedObjectINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalPinnedObject struct{}

func (_ FfiDestroyerOptionalPinnedObject) Destroy(value **PinnedObject) {
	if value != nil {
		FfiDestroyerPinnedObject{}.Destroy(*value)
	}
}

type FfiConverterOptionalProgressCallback struct{}

var FfiConverterOptionalProgressCallbackINSTANCE = FfiConverterOptionalProgressCallback{}

func (c FfiConverterOptionalProgressCallback) Lift(rb RustBufferI) *ProgressCallback {
	return LiftFromRustBuffer[*ProgressCallback](c, rb)
}

func (_ FfiConverterOptionalProgressCallback) Read(reader io.Reader) *ProgressCallback {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterProgressCallbackINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalProgressCallback) Lower(value *ProgressCallback) C.RustBuffer {
	return LowerIntoRustBuffer[*ProgressCallback](c, value)
}

func (c FfiConverterOptionalProgressCallback) LowerExternal(value *ProgressCallback) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*ProgressCallback](c, value))
}

func (_ FfiConverterOptionalProgressCallback) Write(writer io.Writer, value *ProgressCallback) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterProgressCallbackINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalProgressCallback struct{}

func (_ FfiDestroyerOptionalProgressCallback) Destroy(value *ProgressCallback) {
	if value != nil {
		FfiDestroyerProgressCallback{}.Destroy(*value)
	}
}

type FfiConverterOptionalSdk struct{}

var FfiConverterOptionalSdkINSTANCE = FfiConverterOptionalSdk{}

func (c FfiConverterOptionalSdk) Lift(rb RustBufferI) **Sdk {
	return LiftFromRustBuffer[**Sdk](c, rb)
}

func (_ FfiConverterOptionalSdk) Read(reader io.Reader) **Sdk {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterSdkINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalSdk) Lower(value **Sdk) C.RustBuffer {
	return LowerIntoRustBuffer[**Sdk](c, value)
}

func (c FfiConverterOptionalSdk) LowerExternal(value **Sdk) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[**Sdk](c, value))
}

func (_ FfiConverterOptionalSdk) Write(writer io.Writer, value **Sdk) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterSdkINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalSdk struct{}

func (_ FfiDestroyerOptionalSdk) Destroy(value **Sdk) {
	if value != nil {
		FfiDestroyerSdk{}.Destroy(*value)
	}
}

type FfiConverterOptionalObjectsCursor struct{}

var FfiConverterOptionalObjectsCursorINSTANCE = FfiConverterOptionalObjectsCursor{}

func (c FfiConverterOptionalObjectsCursor) Lift(rb RustBufferI) *ObjectsCursor {
	return LiftFromRustBuffer[*ObjectsCursor](c, rb)
}

func (_ FfiConverterOptionalObjectsCursor) Read(reader io.Reader) *ObjectsCursor {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterObjectsCursorINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalObjectsCursor) Lower(value *ObjectsCursor) C.RustBuffer {
	return LowerIntoRustBuffer[*ObjectsCursor](c, value)
}

func (c FfiConverterOptionalObjectsCursor) LowerExternal(value *ObjectsCursor) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*ObjectsCursor](c, value))
}

func (_ FfiConverterOptionalObjectsCursor) Write(writer io.Writer, value *ObjectsCursor) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterObjectsCursorINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalObjectsCursor struct{}

func (_ FfiDestroyerOptionalObjectsCursor) Destroy(value *ObjectsCursor) {
	if value != nil {
		FfiDestroyerObjectsCursor{}.Destroy(*value)
	}
}

type FfiConverterSequencePinnedObject struct{}

var FfiConverterSequencePinnedObjectINSTANCE = FfiConverterSequencePinnedObject{}

func (c FfiConverterSequencePinnedObject) Lift(rb RustBufferI) []*PinnedObject {
	return LiftFromRustBuffer[[]*PinnedObject](c, rb)
}

func (c FfiConverterSequencePinnedObject) Read(reader io.Reader) []*PinnedObject {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]*PinnedObject, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterPinnedObjectINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequencePinnedObject) Lower(value []*PinnedObject) C.RustBuffer {
	return LowerIntoRustBuffer[[]*PinnedObject](c, value)
}

func (c FfiConverterSequencePinnedObject) LowerExternal(value []*PinnedObject) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]*PinnedObject](c, value))
}

func (c FfiConverterSequencePinnedObject) Write(writer io.Writer, value []*PinnedObject) {
	if len(value) > math.MaxInt32 {
		panic("[]*PinnedObject is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterPinnedObjectINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequencePinnedObject struct{}

func (FfiDestroyerSequencePinnedObject) Destroy(sequence []*PinnedObject) {
	for _, value := range sequence {
		FfiDestroyerPinnedObject{}.Destroy(value)
	}
}

type FfiConverterSequenceHost struct{}

var FfiConverterSequenceHostINSTANCE = FfiConverterSequenceHost{}

func (c FfiConverterSequenceHost) Lift(rb RustBufferI) []Host {
	return LiftFromRustBuffer[[]Host](c, rb)
}

func (c FfiConverterSequenceHost) Read(reader io.Reader) []Host {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]Host, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterHostINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceHost) Lower(value []Host) C.RustBuffer {
	return LowerIntoRustBuffer[[]Host](c, value)
}

func (c FfiConverterSequenceHost) LowerExternal(value []Host) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]Host](c, value))
}

func (c FfiConverterSequenceHost) Write(writer io.Writer, value []Host) {
	if len(value) > math.MaxInt32 {
		panic("[]Host is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterHostINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceHost struct{}

func (FfiDestroyerSequenceHost) Destroy(sequence []Host) {
	for _, value := range sequence {
		FfiDestroyerHost{}.Destroy(value)
	}
}

type FfiConverterSequenceNetAddress struct{}

var FfiConverterSequenceNetAddressINSTANCE = FfiConverterSequenceNetAddress{}

func (c FfiConverterSequenceNetAddress) Lift(rb RustBufferI) []NetAddress {
	return LiftFromRustBuffer[[]NetAddress](c, rb)
}

func (c FfiConverterSequenceNetAddress) Read(reader io.Reader) []NetAddress {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]NetAddress, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterNetAddressINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceNetAddress) Lower(value []NetAddress) C.RustBuffer {
	return LowerIntoRustBuffer[[]NetAddress](c, value)
}

func (c FfiConverterSequenceNetAddress) LowerExternal(value []NetAddress) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]NetAddress](c, value))
}

func (c FfiConverterSequenceNetAddress) Write(writer io.Writer, value []NetAddress) {
	if len(value) > math.MaxInt32 {
		panic("[]NetAddress is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterNetAddressINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceNetAddress struct{}

func (FfiDestroyerSequenceNetAddress) Destroy(sequence []NetAddress) {
	for _, value := range sequence {
		FfiDestroyerNetAddress{}.Destroy(value)
	}
}

type FfiConverterSequenceObjectEvent struct{}

var FfiConverterSequenceObjectEventINSTANCE = FfiConverterSequenceObjectEvent{}

func (c FfiConverterSequenceObjectEvent) Lift(rb RustBufferI) []ObjectEvent {
	return LiftFromRustBuffer[[]ObjectEvent](c, rb)
}

func (c FfiConverterSequenceObjectEvent) Read(reader io.Reader) []ObjectEvent {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]ObjectEvent, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterObjectEventINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceObjectEvent) Lower(value []ObjectEvent) C.RustBuffer {
	return LowerIntoRustBuffer[[]ObjectEvent](c, value)
}

func (c FfiConverterSequenceObjectEvent) LowerExternal(value []ObjectEvent) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]ObjectEvent](c, value))
}

func (c FfiConverterSequenceObjectEvent) Write(writer io.Writer, value []ObjectEvent) {
	if len(value) > math.MaxInt32 {
		panic("[]ObjectEvent is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterObjectEventINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceObjectEvent struct{}

func (FfiDestroyerSequenceObjectEvent) Destroy(sequence []ObjectEvent) {
	for _, value := range sequence {
		FfiDestroyerObjectEvent{}.Destroy(value)
	}
}

type FfiConverterSequencePinnedSector struct{}

var FfiConverterSequencePinnedSectorINSTANCE = FfiConverterSequencePinnedSector{}

func (c FfiConverterSequencePinnedSector) Lift(rb RustBufferI) []PinnedSector {
	return LiftFromRustBuffer[[]PinnedSector](c, rb)
}

func (c FfiConverterSequencePinnedSector) Read(reader io.Reader) []PinnedSector {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]PinnedSector, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterPinnedSectorINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequencePinnedSector) Lower(value []PinnedSector) C.RustBuffer {
	return LowerIntoRustBuffer[[]PinnedSector](c, value)
}

func (c FfiConverterSequencePinnedSector) LowerExternal(value []PinnedSector) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]PinnedSector](c, value))
}

func (c FfiConverterSequencePinnedSector) Write(writer io.Writer, value []PinnedSector) {
	if len(value) > math.MaxInt32 {
		panic("[]PinnedSector is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterPinnedSectorINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequencePinnedSector struct{}

func (FfiDestroyerSequencePinnedSector) Destroy(sequence []PinnedSector) {
	for _, value := range sequence {
		FfiDestroyerPinnedSector{}.Destroy(value)
	}
}

type FfiConverterSequenceSlab struct{}

var FfiConverterSequenceSlabINSTANCE = FfiConverterSequenceSlab{}

func (c FfiConverterSequenceSlab) Lift(rb RustBufferI) []Slab {
	return LiftFromRustBuffer[[]Slab](c, rb)
}

func (c FfiConverterSequenceSlab) Read(reader io.Reader) []Slab {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]Slab, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterSlabINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceSlab) Lower(value []Slab) C.RustBuffer {
	return LowerIntoRustBuffer[[]Slab](c, value)
}

func (c FfiConverterSequenceSlab) LowerExternal(value []Slab) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]Slab](c, value))
}

func (c FfiConverterSequenceSlab) Write(writer io.Writer, value []Slab) {
	if len(value) > math.MaxInt32 {
		panic("[]Slab is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterSlabINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceSlab struct{}

func (FfiDestroyerSequenceSlab) Destroy(sequence []Slab) {
	for _, value := range sequence {
		FfiDestroyerSlab{}.Destroy(value)
	}
}

const (
	uniffiRustFuturePollReady      int8 = 0
	uniffiRustFuturePollMaybeReady int8 = 1
)

type rustFuturePollFunc func(C.uint64_t, C.UniffiRustFutureContinuationCallback, C.uint64_t)
type rustFutureCompleteFunc[T any] func(C.uint64_t, *C.RustCallStatus) T
type rustFutureFreeFunc func(C.uint64_t)

//export sia_storage_ffi_uniffiFutureContinuationCallback
func sia_storage_ffi_uniffiFutureContinuationCallback(data C.uint64_t, pollResult C.int8_t) {
	h := cgo.Handle(uintptr(data))
	waiter := h.Value().(chan int8)
	waiter <- int8(pollResult)
}

func uniffiRustCallAsync[E any, T any, F any](
	errConverter BufReader[E],
	completeFunc rustFutureCompleteFunc[F],
	liftFunc func(F) T,
	rustFuture C.uint64_t,
	pollFunc rustFuturePollFunc,
	freeFunc rustFutureFreeFunc,
) (T, E) {
	defer freeFunc(rustFuture)

	pollResult := int8(-1)
	waiter := make(chan int8, 1)

	chanHandle := cgo.NewHandle(waiter)
	defer chanHandle.Delete()

	for pollResult != uniffiRustFuturePollReady {
		pollFunc(
			rustFuture,
			(C.UniffiRustFutureContinuationCallback)(C.sia_storage_ffi_uniffiFutureContinuationCallback),
			C.uint64_t(chanHandle),
		)
		pollResult = <-waiter
	}

	ffiValue, err := rustCallWithError(errConverter, func(status *C.RustCallStatus) F {
		return completeFunc(rustFuture, status)
	})
	return liftFunc(ffiValue), err
}

//export sia_storage_ffi_uniffiFreeGorutine
func sia_storage_ffi_uniffiFreeGorutine(data C.uint64_t) {
	handle := cgo.Handle(uintptr(data))
	defer handle.Delete()

	guard := handle.Value().(chan struct{})
	guard <- struct{}{}
}

// Calculates the encoded size of data given the original size and erasure coding parameters.
func EncodedSize(size uint64, dataShards uint8, parityShards uint8) uint64 {
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_sia_storage_ffi_fn_func_encoded_size(FfiConverterUint64INSTANCE.Lower(size), FfiConverterUint8INSTANCE.Lower(dataShards), FfiConverterUint8INSTANCE.Lower(parityShards), _uniffiStatus)
	}))
}

// Generates a new BIP-39 12-word recovery phrase.
func GenerateRecoveryPhrase() string {
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_sia_storage_ffi_fn_func_generate_recovery_phrase(_uniffiStatus),
		}
	}))
}

// Validates a BIP-39 recovery phrase.
func ValidateRecoveryPhrase(phrase string) error {
	_, _uniffiErr := rustCallWithError[*SeedError](FfiConverterSeedError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_func_validate_recovery_phrase(FfiConverterStringINSTANCE.Lower(phrase), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Sets a foreign logger to receive log messages from the SDK.
func SetLogger(logger Logger, level string) {
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_sia_storage_ffi_fn_func_set_logger(FfiConverterLoggerINSTANCE.Lower(logger), FfiConverterStringINSTANCE.Lower(level), _uniffiStatus)
		return false
	})
}
