//go:build !siastorage_mock

package siastorage

// Links the production FFI library. These archives are committed under ffi/lib/
// so the module is `go get`-able: Go modules have no build hooks, so a consumer
// running `go build` cannot compile Rust first.
//
// link_mock.go links the test-only mock archive instead, which is never
// committed. The two platform lists must stay in step.
//
// Directive order matters: GNU ld resolves symbols left to right, so the system
// libraries must come after the archive that references them.

/*
#cgo darwin,arm64 LDFLAGS: ${SRCDIR}/ffi/lib/darwin_arm64/libsia_storage_cabi.a
#cgo darwin,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/darwin_amd64/libsia_storage_cabi.a
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/ffi/lib/linux_arm64/libsia_storage_cabi.a
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/linux_amd64/libsia_storage_cabi.a
#cgo windows,amd64 LDFLAGS: ${SRCDIR}/ffi/lib/windows_amd64/libsia_storage_cabi.a
#cgo windows,arm64 LDFLAGS: ${SRCDIR}/ffi/lib/windows_arm64/libsia_storage_cabi.a
#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation -framework SystemConfiguration -framework IOKit
#cgo linux LDFLAGS: -lm -ldl -lpthread
#cgo windows LDFLAGS: -lws2_32 -lbcrypt -luserenv -lntdll -lcrypt32 -lsecur32 -lncrypt -liphlpapi
*/
import "C"
