# The Rust C ABI lives in sia-sdk-rs, in the sia_storage_cabi crate. This repo
# carries only the vendored header and the prebuilt archives that the cgo
# directives link, so that Rust is maintained in one place.
#
# PRODUCTION ARCHIVES ARE NEVER BUILT HERE. The only archives that may be
# committed are the ones the `Build FFI Libraries` workflow produced, because a
# locally built archive cannot be traced back to a revision and cannot be
# reproduced by anyone else. There is deliberately no `lib` target: nothing you
# compile on this machine can reach the path the cgo directives link.
#
# To refresh the committed archives, run the `Build FFI Libraries` workflow.
# It builds all five platforms and opens a PR with the results.
#
# To run against a real indexer locally, `make fetch-lib` downloads the archive
# for this platform from a workflow run rather than building one.
#
# `testlib` is the one archive you do build here. It carries the mock cargo
# feature, which swaps the host transport for an in-memory one. It is linked
# only by `go build -tags siastorage_mock`, is covered by .gitignore, and must
# never be committed or shipped. Shipping a mock linked archive as production
# is what invalidated the original throughput benchmark.

SIA_SDK_RS ?= ../../rs_sia_suite/sia-sdk-rs
CABI := $(SIA_SDK_RS)/sia_storage_cabi

# Lazy, not `:=`. A recursive assignment would run cargo when make parses this
# file, for every target, so `fetch-lib` and `check-no-mock` would fail on a
# machine with no sia-sdk-rs checkout even though neither one needs Rust.
#
# Asked of cargo rather than assumed, because the answer moves. A crate inside
# the workspace builds into the workspace target dir, and one carrying its own
# [workspace] table builds into its own.
CABI_TARGET = $(shell cargo metadata --format-version 1 --no-deps \
	--manifest-path $(CABI)/Cargo.toml \
	| python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
CABI_ARCHIVE = $(CABI_TARGET)/release/libsia_storage_cabi.a

GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
PLATFORM := $(GOOS)_$(GOARCH)
LIB_DIR := ffi/lib/$(PLATFORM)

REPO ?= SiaFoundation/sia-storage-go

# The workflow run to take archives from. Defaults to the most recent
# successful one, which is what you want unless you are reproducing an older
# build. Lazy so that `gh` is only invoked by the target that needs it.
# Matched on the run name rather than --workflow build-libs.yml, because
# resolving a workflow by path goes through an API that only knows about the
# default branch. Until this workflow reaches master that lookup 404s even
# though the runs themselves exist.
RUN = $(shell gh run list --repo $(REPO) --limit 50 \
	--json databaseId,name,conclusion \
	--jq '[.[] | select(.name == "Build FFI Libraries" and .conclusion == "success")][0].databaseId')

# Downloads this platform's archive from a workflow run. The archive that lands
# is the same artifact the committed one came from, so a clean tree stays clean
# unless you fetch a newer run than the one that was committed, which git will
# then show you.
.PHONY: fetch-lib
fetch-lib:
	@run="$(RUN)"; \
	test -n "$$run" || { echo "no successful build-libs run found on $(REPO)"; exit 1; }; \
	echo "fetching $(PLATFORM) from run $$run"; \
	mkdir -p $(LIB_DIR); \
	gh run download "$$run" --repo $(REPO) --name $(PLATFORM) --dir $(LIB_DIR)
	$(MAKE) check-no-mock
	go clean -cache

# The mock archive, and the only one built from local Rust. Removed before each
# build so a failed compile leaves no archive behind for the next copy to pick
# up.
.PHONY: testlib
testlib: sync-header
	rm -f $(CABI_ARCHIVE)
	cargo build --release --features mock --manifest-path $(CABI)/Cargo.toml
	mkdir -p $(LIB_DIR)
	cp $(CABI_ARCHIVE) $(LIB_DIR)/libsia_storage_cabi_mock.a
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

# Asserts the archive in place was not built with the mock transport. This is
# the check that a mock linked library once defeated.
.PHONY: check-no-mock
check-no-mock:
	@n=$$(nm -g $(LIB_DIR)/libsia_storage_cabi.a 2>/dev/null | grep -c _sia_mock || true); \
	if [ "$$n" != "0" ]; then echo "FAIL: production archive exports $$n mock symbols"; exit 1; fi; \
	echo "ok: production archive exports no mock symbols"

# Expects the committed archive to already be in place, which it is in a fresh
# checkout. Run `make fetch-lib` first only if you deleted it.
.PHONY: test
test: check-no-mock testlib
	go test -race ./...
	go test -race -tags siastorage_mock ./...

# Lint needs no archive: cgo type checks without linking.
.PHONY: lint
lint:
	golangci-lint run
	golangci-lint run --build-tags siastorage_mock
	cd examples && golangci-lint run
