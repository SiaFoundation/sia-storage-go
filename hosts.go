package siastorage

/*
#cgo CFLAGS: -I${SRCDIR}/ffi/include
#include <stdlib.h>
#include "sia_storage_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
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

// decodeHosts parses the JSON array the C ABI hands back. goString owns the
// free, so this must not release cJSON itself.
func decodeHosts(cJSON *C.char) ([]Host, error) {
	var hosts []Host
	if err := json.Unmarshal([]byte(goString(cJSON)), &hosts); err != nil {
		return nil, fmt.Errorf("decoding hosts: %w", err)
	}
	return hosts, nil
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

	var cJSON, cerr *C.char
	code := C.sia_sdk_hosts(s.ptr, &cq, tok, &cJSON, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return decodeHosts(cJSON)
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

	var cJSON, cerr *C.char
	code := C.sia_shared_sdk_hosts(s.ptr, &cq, tok, &cJSON, &cerr)
	runtime.KeepAlive(s)
	if code != C.SIA_OK {
		return nil, goError(ctx, code, cerr)
	}
	return decodeHosts(cJSON)
}
