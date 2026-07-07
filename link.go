//go:build !siastorage_mock

package siastorage

// Links the production FFI library, built without the mock transport. These
// are the libraries committed under ffi/lib/ so the module is `go get`-able.
//
// Directive order matters: GNU ld resolves symbols left to right, so the
// system libraries must come after the archive that references them. Keep
// link_mock.go in sync.

/*
#cgo darwin,arm64 LDFLAGS: ${SRCDIR}/ffi/lib/darwin_arm64/libsia_storage_ffi.a
#cgo darwin,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/darwin_amd64/libsia_storage_ffi.a
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/ffi/lib/linux_arm64/libsia_storage_ffi.a
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/linux_amd64/libsia_storage_ffi.a
#cgo windows,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/windows_amd64/libsia_storage_ffi.a
#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation -framework SystemConfiguration -framework IOKit
#cgo linux LDFLAGS: -lm -ldl -lpthread
#cgo windows LDFLAGS: -lws2_32 -lbcrypt -luserenv -lntdll -lcrypt32 -lsecur32 -lncrypt -liphlpapi
*/
import "C"
