//go:build siastorage_mock

package siastorage

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

// testSDK brings up a mock network and an SDK on it, registering cleanup for
// both. Every slice of the Go layer is tested this way, against in-process
// hosts rather than an indexer, so the suite needs no network or credentials.
func testSDK(t *testing.T) (*MockNetwork, *SDK) {
	t.Helper()
	net := NewMockNetwork(10)

	var seed [32]byte
	seed[0] = 1
	sdk, err := net.SDK(context.Background(), seed)
	if err != nil {
		t.Fatalf("mock sdk: %v", err)
	}
	t.Cleanup(func() { sdk.Close() })
	return net, sdk
}

// TestSDKHandleLifecycle proves an SDK handle survives the round trip into Rust
// and back, that Close releases it, and that a second Close is harmless. The
// double close matters because the cleanup is a backstop for callers who never
// call Close, and running both would free the same pointer twice.
func TestSDKHandleLifecycle(t *testing.T) {
	net := NewMockNetwork(10)

	var seed [32]byte
	seed[0] = 7
	sdk, err := net.SDK(context.Background(), seed)
	if err != nil {
		t.Fatalf("mock sdk: %v", err)
	}
	if err := sdk.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := sdk.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestSDKAppKey proves the seed handed to the native side comes back, and that
// the key rebuilt from it is a usable private key rather than raw bytes.
func TestSDKAppKey(t *testing.T) {
	net := NewMockNetwork(10)

	var seed [32]byte
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	sdk, err := net.SDK(context.Background(), seed)
	if err != nil {
		t.Fatalf("mock sdk: %v", err)
	}
	defer sdk.Close()

	key := sdk.AppKey()
	if len(key) != 64 {
		t.Fatalf("expected a 64 byte private key, got %d", len(key))
	}
	for i, b := range seed {
		if key[i] != b {
			t.Fatalf("seed byte %d came back as %d, wanted %d", i, key[i], b)
		}
	}
	if key.PublicKey() == (types.PublicKey{}) {
		t.Fatal("the derived public key is zero")
	}
}

// TestSDKAppKeyAfterClose proves the key a closed SDK returns is long enough
// to take a public key from, so a caller reading one during shutdown gets a
// zeroed key rather than a panic.
func TestSDKAppKeyAfterClose(t *testing.T) {
	net := NewMockNetwork(1)
	var seed [32]byte
	seed[0] = 7
	sdk, err := net.SDK(context.Background(), seed)
	if err != nil {
		t.Fatalf("mock sdk: %v", err)
	}
	sdk.Close()

	key := sdk.AppKey()
	if len(key) != 64 {
		t.Fatalf("a closed SDK returned a %d byte key, want 64", len(key))
	}
	if key.PublicKey() != (types.PublicKey{}) {
		t.Fatal("a closed SDK returned a key that is not zeroed")
	}
}

// TestSDKAccount proves every account field crosses from the struct the C ABI
// returns, including the unlimited limit the mock reports and the optional
// URLs it leaves unset.
func TestSDKAccount(t *testing.T) {
	_, sdk := testSDK(t)

	account, err := sdk.Account(context.Background())
	if err != nil {
		t.Fatalf("account: %v", err)
	}
	if want := sdk.AppKey().PublicKey(); account.AccountKey != want {
		t.Fatalf("account key %v, want the app key's %v", account.AccountKey, want)
	}
	if account.MaxPinnedData != math.MaxUint64 || account.RemainingStorage != math.MaxUint64 {
		t.Fatalf("limits %d and %d, want the mock's unlimited %d",
			account.MaxPinnedData, account.RemainingStorage, uint64(math.MaxUint64))
	}
	if account.PinnedData != 0 || account.PinnedSize != 0 {
		t.Fatalf("a fresh account reports %d pinned, %d encoded", account.PinnedData, account.PinnedSize)
	}
	if !account.Ready {
		t.Fatal("the mock account must be ready")
	}
	if account.App.Name != "mock" || account.App.Description != "mock indexer" {
		t.Fatalf("app %q, %q", account.App.Name, account.App.Description)
	}
	if account.App.LogoURL != nil || account.App.ServiceURL != nil {
		t.Fatal("unset URLs must stay nil rather than become empty strings")
	}
	if age := time.Since(account.LastUsed); age < 0 || age > time.Minute {
		t.Fatalf("lastUsed %v is not recent", account.LastUsed)
	}
}

// TestSDKHosts proves a host listing crosses with every field and address
// intact, and that paging reaches the native side.
func TestSDKHosts(t *testing.T) {
	_, sdk := testSDK(t)
	ctx := context.Background()

	hosts, err := sdk.Hosts(ctx, HostQuery{})
	if err != nil {
		t.Fatalf("hosts: %v", err)
	}
	if len(hosts) != 10 {
		t.Fatalf("listed %d hosts, want the mock's 10", len(hosts))
	}
	// The mock announces every host on QUIC, which covers the protocol
	// constant that is not zero.
	want := []NetAddress{{Protocol: "quic", Address: "localhost:9984"}}
	seen := make(map[types.PublicKey]bool)
	for _, h := range hosts {
		if h.PublicKey == (types.PublicKey{}) || seen[h.PublicKey] {
			t.Fatalf("host key %v is zero or repeated", h.PublicKey)
		}
		seen[h.PublicKey] = true
		if !slices.Equal(h.Addresses, want) {
			t.Fatalf("addresses %v, want %v", h.Addresses, want)
		}
		if h.CountryCode != "US" || !h.GoodForUpload {
			t.Fatalf("host %+v", h)
		}
	}

	page, err := sdk.Hosts(ctx, HostQuery{Offset: 8, Limit: 5})
	if err != nil {
		t.Fatalf("paged hosts: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("offset 8 of 10 listed %d hosts, want 2", len(page))
	}

	empty, err := sdk.Hosts(ctx, HostQuery{Offset: 10})
	if err != nil {
		t.Fatalf("empty hosts: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("a listing past the end must be empty but not nil, got %#v", empty)
	}
}

// TestSDKAccountCancelled proves a cancelled context reaches the native side
// through the cancellation token rather than being ignored.
func TestSDKAccountCancelled(t *testing.T) {
	_, sdk := testSDK(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sdk.Account(ctx); err == nil {
		t.Fatal("expected a cancelled context to fail the call")
	}
}
