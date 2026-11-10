package siastorage

import (
	"context"
	"errors"
	"strings"
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

// TestNewBuilderWithEveryField proves metadata with the optional URLs set
// crosses too. testBuilder leaves them empty, which sends them as null.
func TestNewBuilderWithEveryField(t *testing.T) {
	b, err := NewBuilder("https://indexer.invalid", AppMetadata{
		AppID:       types.HashBytes([]byte("every field")),
		Name:        "every field",
		Description: "a builder with every metadata field set",
		ServiceURL:  "https://service.invalid",
		LogoURL:     "https://logo.invalid",
		CallbackURL: "https://callback.invalid",
	})
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}
	b.Close()
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

// TestConnectPreAuthorizedChecksPhraseFirst proves a bad phrase is refused
// before the request goes out, since a request that reached the indexer would
// spend one of the key's uses.
func TestConnectPreAuthorizedChecksPhraseFirst(t *testing.T) {
	b := testBuilder(t)

	_, err := b.ConnectPreAuthorized(context.Background(), NewPreAuthorizedKey(), "not a recovery phrase")
	if err == nil || !strings.Contains(err.Error(), "mnemonic") {
		t.Fatalf("expected the phrase to be refused before the request, got %v", err)
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
	if _, err := b.ConnectPreAuthorized(context.Background(), NewPreAuthorizedKey(), mnemonic); !errors.Is(err, errClosed) {
		t.Fatalf("connectPreAuthorized after close: %v", err)
	}
}
