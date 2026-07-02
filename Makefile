UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
CDYLIB := target/release/libsia_storage_ffi.dylib
else
CDYLIB := target/release/libsia_storage_ffi.so
endif

# Builds the Rust FFI library and copies the static library into lib/ where
# the cgo directives in sia_storage_ffi/cgo.go expect it.
.PHONY: lib
lib:
	cargo build --release
	mkdir -p lib
	cp target/release/libsia_storage_ffi.a lib/

# Regenerates the Go bindings from the compiled library. Requires
# uniffi-bindgen-go:
#   cargo install uniffi-bindgen-go --git https://github.com/NordSecurity/uniffi-bindgen-go --tag v0.7.0+v0.31.0
.PHONY: bindings
bindings: lib
	uniffi-bindgen-go --library $(CDYLIB) -o . -c uniffi.toml

.PHONY: test
test: lib
	go test ./...

.PHONY: clean
clean:
	cargo clean
	rm -rf lib
