//go:build !siastorage_mock

package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/siastorage"
)

// demoApp identifies this program to the indexer. The app ID is derived into
// the account's encryption keys, so it is generated once and never changed;
// changing it would make everything written under the old one unreachable.
var demoApp = siastorage.AppMetadata{
	AppID:       mustAppID("8f2b1c7d4e5a6039b8c1d2e3f405162738495a6b7c8d9e0f1a2b3c4d5e6f7081"),
	Name:        "sia-storage-cabi demo",
	Description: "Exercises the Go bindings over the C ABI",
	ServiceURL:  "https://sia.tech",
}

// connect authorizes against a real indexer. With an app key it connects
// straight away; without one it walks the approval flow, which needs a human.
func connect(ctx context.Context, opts connectOptions) (*siastorage.SDK, func(), error) {
	if opts.IndexerURL == "" {
		return nil, nil, errors.New("pass -indexer, or rebuild with -tags siastorage_mock to run against the mock")
	}
	indexerURL = opts.IndexerURL

	stage("Connect to %s", opts.IndexerURL)
	builder, err := siastorage.NewBuilder(opts.IndexerURL, demoApp)
	if err != nil {
		return nil, nil, err
	}
	defer builder.Close()

	if opts.AppKeyHex != "" {
		raw, err := hex.DecodeString(opts.AppKeyHex)
		if err != nil {
			return nil, nil, fmt.Errorf("decode app key: %w", err)
		}
		if len(raw) < 32 {
			return nil, nil, errors.New("an app key is at least 32 bytes")
		}
		sdk, err := builder.Connect(ctx, types.PrivateKey(raw))
		if err != nil {
			return nil, nil, err
		}
		info("connected with the supplied app key")
		return sdk, func() {}, nil
	}

	phrase := opts.RecoveryPhrase
	if phrase == "" {
		phrase = siastorage.GenerateRecoveryPhrase()
		info("generated a recovery phrase, write it down or the account is unreachable:")
		info("%s", phrase)
	}

	url, err := builder.RequestConnection(ctx)
	if err != nil {
		return nil, nil, err
	}
	info("approve this connection in a browser, then this will continue:")
	info("%s", url)
	if err := builder.WaitForApproval(ctx); err != nil {
		return nil, nil, err
	}
	info("approved")

	sdk, err := builder.Register(ctx, phrase)
	if err != nil {
		return nil, nil, err
	}
	info("registered; reconnect later with -app-key %x", sdk.AppKey()[:32])
	return sdk, func() {}, nil
}

// report has nothing indexer side to add that the account did not already say.
// indexerURL is remembered so connectShared can reach the same indexer with
// no account of its own.
var indexerURL string

// connectShared connects as the recipient of a sharing key, which needs only
// the indexer URL and the seed.
func connectShared(ctx context.Context, seed [32]byte) (*siastorage.SharedSDK, error) {
	return siastorage.ConnectShared(ctx, indexerURL, seed)
}

func report(*siastorage.SDK) {}

func mustAppID(s string) types.Hash256 {
	var id types.Hash256
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != len(id) {
		panic("bad app id literal")
	}
	copy(id[:], raw)
	return id
}
