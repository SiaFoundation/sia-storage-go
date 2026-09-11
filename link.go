//go:build !siastorage_mock

package siastorage

// link_mock.go links a locally built archive from ffi/lib/ instead, because the
// mock archive is never committed. Its platform list must match siastorage-libs.

import _ "go.sia.tech/siastorage-libs" // for effect, it carries the link directives
