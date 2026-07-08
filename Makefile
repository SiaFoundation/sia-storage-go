# Builds the Rust FFI static library and installs it where the cgo
# directives expect it: ffi/lib/<goos>_<goarch>/libsia_storage_ffi.a
#
# The production static libraries are committed so the module stays
# `go get`-able; rebuild and commit them whenever the ffi crate or its
# dependencies change.
#
# `testlib` builds a SEPARATE library with the mock cargo feature, which
# swaps the host transport for an in-memory one. It is linked only by
# `go build -tags siastorage_mock`, is required by the E2E tests, and must
# never be committed or shipped.

GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
LIB_DIR := ffi/lib/$(GOOS)_$(GOARCH)

# Keep the committed darwin libraries usable on older systems.
export MACOSX_DEPLOYMENT_TARGET ?= 12.0

.PHONY: lib
lib:
	cd ffi && cargo build --release
	mkdir -p $(LIB_DIR)
	cp ffi/target/release/libsia_storage_ffi.a $(LIB_DIR)/
	go clean -cache

.PHONY: testlib
testlib:
	cd ffi && cargo build --release --features mock
	mkdir -p $(LIB_DIR)
	cp ffi/target/release/libsia_storage_ffi.a $(LIB_DIR)/libsia_storage_ffi_mock.a
	go clean -cache

.PHONY: test
test: lib testlib
	go test -race ./...
	go test -race -tags siastorage_mock ./...

.PHONY: lint
lint: lib testlib
	golangci-lint run
	cd examples && golangci-lint run
