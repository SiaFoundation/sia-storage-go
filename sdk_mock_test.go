//go:build siastorage_mock

package siastorage

import (
	"context"
	"testing"

	"go.sia.tech/core/types"
)

// testSDK brings up a mock network and an SDK on it, registering cleanup for
// both. Every slice of the Go layer is tested this way, against in-process
// hosts rather than an indexer, so the suite needs no network or credentials.
func testSDK(t *testing.T) (*MockNetwork, *SDK) {
	t.Helper()
	net := NewMockNetwork(10)
	t.Cleanup(func() { net.Close() })

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
	defer net.Close()

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
	defer net.Close()

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

// TestSDKAccount proves the account JSON the native side emits decodes into
// this package's Account, which is the crossing most likely to break silently
// if either side renames a field.
func TestSDKAccount(t *testing.T) {
	_, sdk := testSDK(t)

	account, err := sdk.Account(context.Background())
	if err != nil {
		t.Fatalf("account: %v", err)
	}
	if account.AccountKey == (types.PublicKey{}) {
		t.Fatal("accountKey decoded as zero, so the field name is wrong")
	}
	if account.MaxPinnedData == 0 {
		t.Fatal("maxPinnedData decoded as zero, so the field name is wrong")
	}
	if account.LastUsed.IsZero() {
		t.Fatal("lastUsed decoded as zero, so the field name is wrong")
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
