package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"unsafe"

	"go.sia.tech/core/types"
)

// A NetAddress is one way to reach a host.
type NetAddress struct {
	Protocol string `json:"protocol"`
	Address  string `json:"address"`
}

// A Host is a storage host the indexer knows about.
type Host struct {
	PublicKey     types.PublicKey `json:"publicKey"`
	Addresses     []NetAddress    `json:"addresses"`
	CountryCode   string          `json:"countryCode"`
	Latitude      float64         `json:"latitude"`
	Longitude     float64         `json:"longitude"`
	GoodForUpload bool            `json:"goodForUpload"`
}

// A GeoLocation is a point to sort hosts by proximity to.
type GeoLocation struct {
	Latitude  float64
	Longitude float64
}

// A HostQuery filters a host listing. The zero value applies no filters.
//
// There is no protocol filter. Listings are always scoped to siamux, because
// that is the only protocol the transport behind this package dials, so
// filtering to anything else could only return hosts it cannot reach.
type HostQuery struct {
	// Location sorts hosts by proximity to a point. Nil keeps the indexer's
	// own order.
	Location *GeoLocation

	// Offset and Limit page the result. Zero takes the indexer's defaults.
	Offset uint64
	Limit  uint64

	// Country filters to one ISO 3166-1 alpha-2 code. Empty matches any.
	Country string
}

// cQuery converts the query, returning a release for the C string it may own.
func (q HostQuery) cQuery() (C.sia_host_query_t, func()) {
	cq := C.sia_host_query_t{
		offset: C.uint64_t(q.Offset),
		limit:  C.uint64_t(q.Limit),
	}
	if q.Location != nil {
		cq.has_location = true
		cq.latitude = C.double(q.Location.Latitude)
		cq.longitude = C.double(q.Location.Longitude)
	}
	if q.Country == "" {
		return cq, func() {}
	}
	country := C.CString(q.Country)
	cq.country = country
	return cq, func() { C.free(unsafe.Pointer(country)) }
}

// goHosts copies a host array out of C memory and frees it. The slices are
// never nil, so an empty listing reads the same as it did over JSON.
func goHosts(cHosts *C.sia_host_t, n C.uintptr_t) []Host {
	defer C.sia_host_array_free(cHosts, n)
	hosts := make([]Host, 0, int(n))
	for _, c := range unsafe.Slice(cHosts, int(n)) {
		h := Host{
			Addresses:     make([]NetAddress, 0, int(c.addresses_len)),
			CountryCode:   C.GoString(c.country_code),
			Latitude:      float64(c.latitude),
			Longitude:     float64(c.longitude),
			GoodForUpload: bool(c.good_for_upload),
		}
		for i, b := range c.public_key {
			h.PublicKey[i] = byte(b)
		}
		for _, a := range unsafe.Slice(c.addresses, int(c.addresses_len)) {
			h.Addresses = append(h.Addresses, NetAddress{
				Protocol: protocolName(a.protocol),
				Address:  C.GoString(a.address),
			})
		}
		hosts = append(hosts, h)
	}
	return hosts
}

// protocolName maps the C ABI's protocol constants to the names the indexer
// uses for them.
func protocolName(p C.uint8_t) string {
	switch p {
	case C.SIA_PROTOCOL_SIAMUX:
		return "siamux"
	case C.SIA_PROTOCOL_QUIC:
		return "quic"
	default:
		return fmt.Sprintf("unknown protocol %d", p)
	}
}

// Hosts lists the usable hosts the indexer knows about.
//
// The listing is always scoped to siamux hosts, see [HostQuery].
func (s *SDK) Hosts(ctx context.Context, query HostQuery) ([]Host, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	cq, freeQuery := query.cQuery()
	defer freeQuery()

	var cHosts *C.sia_host_t
	var n C.uintptr_t
	var cerr *C.char
	code := C.sia_sdk_hosts(s.ptr, &cq, tok, &cHosts, &n, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return goHosts(cHosts, n), nil
}

// Hosts lists the hosts serving this sharing key's objects, scoped to the key
// rather than to everything the indexer knows.
func (s *SharedSDK) Hosts(ctx context.Context, query HostQuery) ([]Host, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errClosed
	}

	tok, release := cancelToken(ctx)
	defer release()

	cq, freeQuery := query.cQuery()
	defer freeQuery()

	var cHosts *C.sia_host_t
	var n C.uintptr_t
	var cerr *C.char
	code := C.sia_shared_sdk_hosts(s.ptr, &cq, tok, &cHosts, &n, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return goHosts(cHosts, n), nil
}
