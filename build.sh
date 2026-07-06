#!/usr/bin/env bash
set -euo pipefail

# Build the Rust FFI library and (optionally) regenerate the Go bindings.
#
# Usage:
#   ./build.sh            # build the static library into lib/
#   ./build.sh bindings   # additionally regenerate the Go bindings
#
# Regenerating the bindings requires uniffi-bindgen-go:
#   cargo install uniffi-bindgen-go --git https://github.com/NordSecurity/uniffi-bindgen-go --tag v0.7.0+v0.31.0

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

cargo build --release
mkdir -p lib
cp target/release/libsia_storage_ffi.a lib/

if [[ "${1:-}" == "bindings" ]]; then
    case "$(uname -s)" in
        Darwin) CDYLIB="target/release/libsia_storage_ffi.dylib" ;;
        *) CDYLIB="target/release/libsia_storage_ffi.so" ;;
    esac
    uniffi-bindgen-go --library "$CDYLIB" -o . -c uniffi.toml
fi
