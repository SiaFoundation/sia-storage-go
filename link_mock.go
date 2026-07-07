//go:build siastorage_mock

package siastorage

// Links the test-only FFI library built with the mock cargo feature, which
// swaps the host transport for an in-memory one. NEVER use this build for
// anything but tests: uploads and downloads do not touch the network. Build
// it with `make testlib`; it is not committed.
//
// Directive order matters: GNU ld resolves symbols left to right, so the
// system libraries must come after the archive that references them. Keep
// link.go in sync.

/*
#cgo darwin,arm64 LDFLAGS: ${SRCDIR}/ffi/lib/darwin_arm64/libsia_storage_ffi_mock.a
#cgo darwin,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/darwin_amd64/libsia_storage_ffi_mock.a
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/ffi/lib/linux_arm64/libsia_storage_ffi_mock.a
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/linux_amd64/libsia_storage_ffi_mock.a
#cgo windows,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/windows_amd64/libsia_storage_ffi_mock.a
#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation -framework SystemConfiguration -framework IOKit
#cgo linux LDFLAGS: -lm -ldl -lpthread
#cgo windows LDFLAGS: -lws2_32 -lbcrypt -luserenv -lntdll -lcrypt32 -lsecur32 -lncrypt -liphlpapi
*/
import "C"
