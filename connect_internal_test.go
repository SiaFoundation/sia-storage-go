package siastorage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/app"
)

func TestBuilder(t *testing.T) {
	t.Run("WaitForApprovalBeforeRequestConnection", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		if err := b.WaitForApproval(t.Context()); !errors.Is(err, ErrNoConnectionRequest) {
			t.Fatalf("expected ErrNoConnectionRequest, got %v", err)
		}
	})

	t.Run("WaitForApprovalExpired", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		b.registerResp = &app.RegisterAppResponse{Expiration: time.Now().Add(-time.Second)}
		if err := b.WaitForApproval(t.Context()); !errors.Is(err, ErrRequestExpired) {
			t.Fatalf("expected ErrRequestExpired, got %v", err)
		}
	})

	t.Run("WaitForApprovalDeadline", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		b.registerResp = &app.RegisterAppResponse{Expiration: time.Now().Add(50 * time.Millisecond)}
		if err := b.WaitForApproval(t.Context()); !errors.Is(err, ErrRequestExpired) {
			t.Fatalf("expected ErrRequestExpired, got %v", err)
		}
	})

	t.Run("WaitForApprovalRejected", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		defer srv.Close()

		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		b.ephemeralKey = types.GeneratePrivateKey()
		b.client = app.NewClient(srv.URL)
		b.registerResp = &app.RegisterAppResponse{
			StatusURL:  srv.URL,
			Expiration: time.Now().Add(time.Hour),
		}
		if err := b.WaitForApproval(t.Context()); !errors.Is(err, ErrUserRejected) {
			t.Fatalf("expected ErrUserRejected, got %v", err)
		}
	})

	t.Run("WaitForApprovalCancel", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		b.registerResp = &app.RegisterAppResponse{Expiration: time.Now().Add(time.Hour)}

		cause := errors.New("cause")
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(cause)

		if err := b.WaitForApproval(ctx); !errors.Is(err, cause) {
			t.Fatalf("expected parent cause, got %v", err)
		}
	})

	t.Run("RegisterBeforeApproval", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		if _, err := b.Register(t.Context(), NewSeedPhrase()); !errors.Is(err, ErrNotApproved) {
			t.Fatalf("expected ErrNotApproved, got %v", err)
		}
	})

	t.Run("RegisterInvalidMnemonic", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		b.sharedSecret = types.Hash256{1}
		if _, err := b.Register(t.Context(), "not a valid mnemonic"); err == nil {
			t.Fatal("expected error for invalid mnemonic, got nil")
		}
	})

	t.Run("Success", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		sdk, err := b.SDK(types.GeneratePrivateKey())
		if err != nil {
			t.Fatalf("SDK: %v", err)
		} else if sdk == nil {
			t.Fatal("expected non-nil SDK")
		}
	})

	t.Run("MethodsAfterConsumed", func(t *testing.T) {
		b := newMockBuilder(newMockAppClient(nil), nil, nil)
		if _, err := b.SDK(types.GeneratePrivateKey()); err != nil {
			t.Fatalf("first SDK call failed: %v", err)
		}

		if err := b.WaitForApproval(t.Context()); !errors.Is(err, ErrBuilderConsumed) {
			t.Fatalf("WaitForApproval: expected ErrBuilderConsumed, got %v", err)
		} else if _, err := b.RequestConnection(t.Context()); !errors.Is(err, ErrBuilderConsumed) {
			t.Fatalf("RequestConnection: expected ErrBuilderConsumed, got %v", err)
		} else if _, err := b.Register(t.Context(), NewSeedPhrase()); !errors.Is(err, ErrBuilderConsumed) {
			t.Fatalf("Register: expected ErrBuilderConsumed, got %v", err)
		} else if _, err := b.SDK(types.GeneratePrivateKey()); !errors.Is(err, ErrBuilderConsumed) {
			t.Fatalf("SDK: expected ErrBuilderConsumed, got %v", err)
		}
	})
}
