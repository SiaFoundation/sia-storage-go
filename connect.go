package siastorage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/keys"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var (
	// ErrBuilderConsumed is returned when a [Builder] method is called
	// after the builder has already created an SDK. A builder is
	// single-use; create a new one to start another connection.
	ErrBuilderConsumed = errors.New("builder already consumed")

	// ErrNoConnectionRequest is returned by [Builder.WaitForApproval]
	// when called before [Builder.RequestConnection].
	ErrNoConnectionRequest = errors.New("no connection request")

	// ErrRequestExpired is returned by [Builder.WaitForApproval]
	// when the connection request has expired.
	ErrRequestExpired = errors.New("connection request expired")

	// ErrNotApproved is returned by [Builder.Register] when called
	// before [Builder.WaitForApproval] has successfully completed.
	ErrNotApproved = errors.New("connection not approved")

	// ErrUnauthorized is returned by [Builder.SDK] when the supplied
	// app key is not authorized by the indexer.
	ErrUnauthorized = errors.New("app key is not authorized")

	// ErrUserRejected is returned by [Builder.WaitForApproval] when
	// the user rejects the connection request.
	ErrUserRejected = app.ErrUserRejected
)

type (
	// AppMetadata contains metadata about an application.
	// This metadata is provided during app registration
	// and is used to identify the application to users.
	AppMetadata struct {
		// ID is a unique identifier for an application.
		// It should be generated once and stay constant for
		// the lifetime of the app.
		//
		// Changing it will invalidate any existing app keys
		// and prevent access to associated data.
		//
		// It should be a randomly generated 32-byte value.
		// You can use GenerateAppID to create a new app ID.
		ID          types.Hash256
		Name        string
		Description string
		LogoURL     string
		ServiceURL  string
		CallbackURL string
	}
)

// A Builder helps connect an application to an indexer
// and initialize an SDK instance.
//
// A Builder is single-use: once it has created an SDK, any further calls
// on the same Builder return [ErrBuilderConsumed].
type Builder struct {
	ephemeralKey types.PrivateKey
	client       *app.Client

	request      app.RegisterAppRequest
	registerResp *app.RegisterAppResponse
	sharedSecret types.Hash256

	consumed *atomic.Bool

	// mock overrides — when set, SDK() bypasses auth and host
	// discovery and uses these directly.
	mockApp  appClient
	mockHost hostClient
}

// consume marks the builder as consumed, preventing further use. It returns
// [ErrBuilderConsumed] if the builder has already been consumed.
func (b *Builder) consume() error {
	if !b.consumed.CompareAndSwap(false, true) {
		return ErrBuilderConsumed
	}
	return nil
}

// checkConsumed returns [ErrBuilderConsumed] if the builder has already been consumed.
func (b *Builder) checkConsumed() error {
	if b.consumed.Load() {
		return ErrBuilderConsumed
	}
	return nil
}

// WaitForApproval waits for the user to approve the app connection request.
// The user must visit the response URL returned by [Builder.RequestConnection]
// to approve the request. It blocks until the request is approved, denied, or
// the context is cancelled.
//
// It returns [ErrUserRejected] if the user denied the request and
// [ErrBuilderConsumed] if the builder has already created an SDK instance.
// Callers can branch on these using [errors.Is].
func (b *Builder) WaitForApproval(ctx context.Context) error {
	if err := b.checkConsumed(); err != nil {
		return err
	}

	if b.registerResp == nil {
		return ErrNoConnectionRequest
	} else if time.Until(b.registerResp.Expiration) <= 0 {
		return ErrRequestExpired
	}

	ctx, cancel := context.WithDeadlineCause(ctx, b.registerResp.Expiration, ErrRequestExpired)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			status, err := b.client.RequestStatus(ctx, b.ephemeralKey, b.registerResp.StatusURL)
			if errors.Is(err, ErrUserRejected) {
				return ErrUserRejected
			} else if err != nil {
				if cause := context.Cause(ctx); cause != nil {
					return cause
				}
				return fmt.Errorf("failed to check request status: %w", err)
			} else if status.Approved {
				b.sharedSecret = status.UserSecret
				return nil
			}
		}
	}
}

// Register derives an application key from a BIP-39 seed phrase and
// registers it with the indexer.
//
// This key should be stored securely by the application and never
// shared with anyone else. It can be regenerated using the same app
// ID, user account, and seed phrase.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance and [ErrNotApproved] if [Builder.WaitForApproval] has not yet
// returned successfully.
func (b *Builder) Register(ctx context.Context, mnemonic string) (*SDK, error) {
	if err := b.checkConsumed(); err != nil {
		return nil, err
	}

	if b.sharedSecret == (types.Hash256{}) {
		return nil, ErrNotApproved
	}

	appKey, err := deriveAppKey(mnemonic, b.request.AppID, b.sharedSecret)
	if err != nil {
		return nil, fmt.Errorf("failed to derive app key: %w", err)
	} else if err := b.client.RegisterApp(ctx, b.registerResp.RegisterURL, b.ephemeralKey, appKey); err != nil {
		return nil, fmt.Errorf("failed to register app key: %w", err)
	}

	// prevent attempted re-use
	b.registerResp = nil
	clear(b.sharedSecret[:])
	return b.SDK(appKey)
}

// RequestConnection sends a request to connect an application to the indexer.
//
// It returns a response URL that the user must visit to approve the request.
// The app should display the response URL to the user.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance.
func (b *Builder) RequestConnection(ctx context.Context) (string, error) {
	if err := b.checkConsumed(); err != nil {
		return "", err
	}
	resp, err := b.client.RequestAppConnection(ctx, b.ephemeralKey, b.request)
	if err != nil {
		return "", fmt.Errorf("failed to request app connection: %w", err)
	}
	b.registerResp = &resp
	return resp.ResponseURL, nil
}

// SDK creates a new SDK instance using the given application key. If the
// key is not authorized, an error is returned.
//
// It returns [ErrBuilderConsumed] if the builder has already created an SDK
// instance and [ErrUnauthorized] if the app key is not authorized by the
// indexer.
func (b *Builder) SDK(appKey types.PrivateKey, opts ...Option) (*SDK, error) {
	if err := b.checkConsumed(); err != nil {
		return nil, err
	}

	if b.mockApp != nil {
		if err := b.consume(); err != nil {
			return nil, err
		}
		sdk := &SDK{
			appKey: appKey,
			log:    zap.NewNop(),
			client: b.mockApp,
			hosts:  b.mockHost,
		}
		for _, opt := range opts {
			opt(sdk)
		}
		return sdk, nil
	}

	if ok, err := b.client.CheckAppAuth(context.Background(), appKey); err != nil {
		return nil, fmt.Errorf("failed to check app auth: %w", err)
	} else if !ok {
		return nil, ErrUnauthorized
	}
	hostStore, err := newCachedHostStore(b.client, appKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create host store: %w", err)
	}
	if err := b.consume(); err != nil {
		return nil, err
	}
	return initSDK(appKey, b.client, client.NewProvider(hostStore), opts...), nil
}

func deriveAppKey(mnemonic string, appID types.Hash256, sharedSecret types.Hash256) (types.PrivateKey, error) {
	var seed [32]byte
	if err := wallet.SeedFromPhrase(&seed, mnemonic); err != nil {
		return nil, fmt.Errorf("failed to derive seed from phrase: %w", err)
	}
	defer clear(seed[:])
	buf := keys.Derive(append(seed[:], sharedSecret[:]...), appID[:], []byte("indexd app key derivation"), 32)
	defer clear(buf)

	return types.NewPrivateKeyFromSeed(buf), nil
}

// NewBuilder creates a new Builder for connecting applications to the indexer.
//
// A builder can only be used to create a single SDK instance. Methods called
// on a builder that has already created an SDK return [ErrBuilderConsumed].
func NewBuilder(indexerURL string, metadata AppMetadata) *Builder {
	return &Builder{
		ephemeralKey: types.GeneratePrivateKey(),
		request: app.RegisterAppRequest{
			AppID:       metadata.ID,
			Name:        metadata.Name,
			Description: metadata.Description,
			LogoURL:     metadata.LogoURL,
			ServiceURL:  metadata.ServiceURL,
			CallbackURL: metadata.CallbackURL,
		},
		client:   app.NewClient(indexerURL),
		consumed: &atomic.Bool{},
	}
}

// GenerateAppID generates a new random application ID.
func GenerateAppID() (id types.Hash256) {
	frand.Read(id[:])
	return id
}

// NewSeedPhrase generates a new seed phrase.
func NewSeedPhrase() string {
	return wallet.NewSeedPhrase()
}
