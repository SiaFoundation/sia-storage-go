package sia_storage_ffi

// This file is not generated. It provides the cgo flags required to compile
// and link the generated bindings against the Rust sia_storage_ffi library.
//
// Build the static library with `make lib`, which places
// libsia_storage_ffi.a in the repository's lib/ directory.

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR}/../lib -lsia_storage_ffi
#cgo darwin LDFLAGS: -framework Security -framework SystemConfiguration -framework CoreFoundation -framework IOKit
#cgo linux LDFLAGS: -lm -ldl -lpthread
*/
import "C"
