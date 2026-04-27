package siastorage

import (
	"errors"
	"testing"

	"go.sia.tech/core/types"
)

func TestBuilder(t *testing.T) {
	t.Run("WaitForApprovalBeforeRequestConnection", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(), nil)
		if err := b.WaitForApproval(t.Context()); !errors.Is(err, ErrNoConnectionRequest) {
			t.Fatalf("expected ErrNoConnectionRequest, got %v", err)
		}
	})

	t.Run("RegisterBeforeApproval", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(), nil)
		if _, err := b.Register(t.Context(), NewSeedPhrase()); !errors.Is(err, ErrNotApproved) {
			t.Fatalf("expected ErrNotApproved, got %v", err)
		}
	})

	t.Run("MethodsAfterConsumed", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(), nil)
		if _, err := b.SDK(types.GeneratePrivateKey()); err != nil {
			t.Fatalf("first SDK call failed: %v", err)
		}

		if err := b.WaitForApproval(t.Context()); !errors.Is(err, ErrBuilderConsumed) {
			t.Fatalf("WaitForApproval: expected ErrBuilderConsumed, got %v", err)
		}
		if _, err := b.RequestConnection(t.Context()); !errors.Is(err, ErrBuilderConsumed) {
			t.Fatalf("RequestConnection: expected ErrBuilderConsumed, got %v", err)
		}
		if _, err := b.Register(t.Context(), NewSeedPhrase()); !errors.Is(err, ErrBuilderConsumed) {
			t.Fatalf("Register: expected ErrBuilderConsumed, got %v", err)
		}
		if _, err := b.SDK(types.GeneratePrivateKey()); !errors.Is(err, ErrBuilderConsumed) {
			t.Fatalf("SDK: expected ErrBuilderConsumed, got %v", err)
		}
	})
}
