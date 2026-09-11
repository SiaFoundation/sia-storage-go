# The Rust C ABI lives in sia-sdk-rs, in the sia_storage_cabi crate. This repo
# carries only the vendored header, and links the prebuilt archives from the
# siastorage-libs module, so that Rust is maintained in one place.
#
# Point SIA_SDK_RS at a local sia-sdk-rs checkout to build the archive
# yourself, and LIBS at the siastorage-libs checkout to write it into. Released
# archives are produced by sia-sdk-rs CI and published from there so the module
# stays `go get`-able.
#
# `testlib` builds a SEPARATE archive with the mock cargo feature, which adds
# an in-memory host transport alongside the real one. It is linked only by
# `go build -tags siastorage_mock`, and must never be committed or shipped.
#
# Both builds write the same cargo output path, so each target copies its
# archive out immediately and whichever ran last owns target/release. Never
# copy from there by hand; run `make lib` and let `check-no-mock` confirm what
# landed.

SIA_SDK_RS ?= ../rs_sia_suite/sia-sdk-rs
CABI := $(SIA_SDK_RS)/sia_storage_cabi

# Asked of cargo rather than assumed, because the answer moves. A crate inside
# the workspace builds into the workspace target dir, and one carrying its own
# [workspace] table builds into its own. Guessing wrong copies whatever archive
# happens to be sitting at the guessed path, which is how a mock linked build
# gets shipped as production.
CABI_TARGET := $(shell cargo metadata --format-version 1 --no-deps \
	--manifest-path $(CABI)/Cargo.toml \
	| python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
CABI_ARCHIVE := $(CABI_TARGET)/release/libsia_storage_cabi.a

GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

# Production archives are published from the siastorage-libs module, so that a
# fresh copy of every platform does not land in this repository's history on
# each rebuild. The mock archive is a local build artifact that is never
# committed anywhere, so it stays here.
LIBS ?= ../sia-storage-cabi-libs
LIB_DIR := $(LIBS)/$(GOOS)_$(GOARCH)
MOCK_DIR := ffi/lib/$(GOOS)_$(GOARCH)

# Keep the committed darwin libraries usable on older systems.
export MACOSX_DEPLOYMENT_TARGET ?= 12.0

# Removed before each build so a failed compile leaves no archive behind for
# the next copy to pick up.
.PHONY: lib
lib: sync-header
	rm -f $(CABI_ARCHIVE)
	RUSTFLAGS="$$RUSTFLAGS -C strip=debuginfo" cargo build --release --manifest-path $(CABI)/Cargo.toml
	mkdir -p $(LIB_DIR)
	cp $(CABI_ARCHIVE) $(LIB_DIR)/
	go clean -cache

.PHONY: testlib
testlib: sync-header
	rm -f $(CABI_ARCHIVE)
	cargo build --release --features mock --manifest-path $(CABI)/Cargo.toml
	mkdir -p $(MOCK_DIR)
	cp $(CABI_ARCHIVE) $(MOCK_DIR)/libsia_storage_cabi_mock.a
	go clean -cache

# The header is the contract between the two repos. Copying it on every build
# means a Rust signature change cannot silently disagree with the cgo calls.
.PHONY: sync-header
sync-header:
	cp $(CABI)/include/sia_storage.h ffi/include/sia_storage.h

# Fails when the vendored header has drifted from sia-sdk-rs, which is what CI
# should check on a repo that does not build the Rust itself.
.PHONY: check-header
check-header:
	diff -u ffi/include/sia_storage.h $(CABI)/include/sia_storage.h

# Asserts the shipped archive was not built with the mock transport.
.PHONY: check-no-mock
check-no-mock:
	@n=$$(nm -g $(LIB_DIR)/libsia_storage_cabi.a 2>/dev/null | grep -c _sia_mock || true); \
	if [ "$$n" != "0" ]; then echo "FAIL: production archive exports $$n mock symbols"; exit 1; fi; \
	echo "ok: production archive exports no mock symbols"

.PHONY: test
test: lib check-no-mock testlib
	go test -race ./...
	go test -race -tags siastorage_mock ./...

.PHONY: lint
lint: lib testlib
	golangci-lint run
	cd examples && golangci-lint run
