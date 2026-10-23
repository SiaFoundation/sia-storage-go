package siastorage

import (
	"context"
	"errors"
	"testing"

	"go.sia.tech/core/types"
)

// testBuilder returns a builder pointed at a host that does not exist. Every
// test here stops before the native side would reach it.
func testBuilder(t *testing.T) *Builder {
	t.Helper()
	b, err := NewBuilder("https://indexer.invalid", AppMetadata{
		AppID: types.HashBytes([]byte("builder test")),
		Name:  "builder test",
	})
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// TestBuilderInspectorsRequireApproval proves both inspectors refuse a builder
// that has not been approved rather than answering false, which a caller would
// read as "this phrase is not the registered one" and register over the top of
// an existing account.
func TestBuilderInspectorsRequireApproval(t *testing.T) {
	b := testBuilder(t)
	mnemonic := NewSeedPhrase()

	if _, err := b.Reconnecting(); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("reconnecting on an unapproved builder: %v", err)
	}
	if _, err := b.MatchesExistingAppKey(context.Background(), mnemonic); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("matchesExistingAppKey on an unapproved builder: %v", err)
	}
}

// TestConnectPreAuthorizedRejectsShortKey proves the length is checked in Go.
// The native side takes a bare pointer to 32 bytes, so a short slice would be
// read past its end rather than rejected.
func TestConnectPreAuthorizedRejectsShortKey(t *testing.T) {
	b := testBuilder(t)
	mnemonic := NewSeedPhrase()

	if _, err := b.ConnectPreAuthorized(context.Background(), types.PrivateKey(make([]byte, 31)), mnemonic); err == nil {
		t.Fatal("a 31 byte key was accepted")
	}
	if _, err := b.ConnectPreAuthorized(context.Background(), nil, mnemonic); err == nil {
		t.Fatal("a nil key was accepted")
	}
}

// TestBuilderClosedRefusesNewCalls proves the three new methods take the same
// closed check as the rest, so none of them hands a freed handle to C.
func TestBuilderClosedRefusesNewCalls(t *testing.T) {
	b := testBuilder(t)
	mnemonic := NewSeedPhrase()
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := b.Reconnecting(); !errors.Is(err, errClosed) {
		t.Fatalf("reconnecting after close: %v", err)
	}
	if _, err := b.MatchesExistingAppKey(context.Background(), mnemonic); !errors.Is(err, errClosed) {
		t.Fatalf("matchesExistingAppKey after close: %v", err)
	}
	if _, err := b.ConnectPreAuthorized(context.Background(), types.PrivateKey(make([]byte, 32)), mnemonic); !errors.Is(err, errClosed) {
		t.Fatalf("connectPreAuthorized after close: %v", err)
	}
}
