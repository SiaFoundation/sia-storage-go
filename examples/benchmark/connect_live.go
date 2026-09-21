//go:build !siastorage_mock

package main

import (
	"context"
	"encoding/hex"
	"errors"

	"go.sia.tech/core/types"
	"go.sia.tech/siastorage"
)

// engineName tags every sample, so a merged run says which arm it came from.
const engineName = "cabi"

var benchApp = siastorage.AppMetadata{
	AppID:       mustAppID("3f9c2a815d47e6b09c8a1f2e3d4b5a6978869504132e7f8a9b0c1d2e3f405162"),
	Name:        "siastorage benchmark",
	Description: "Measures the C ABI against the native Go engine",
	ServiceURL:  "https://sia.tech",
}

func connect(ctx context.Context, indexer, appKey string, _ int) (*siastorage.SDK, func(), error) {
	if indexer == "" {
		return nil, nil, errors.New("pass -indexer, or rebuild with -tags siastorage_mock")
	}
	if appKey == "" {
		return nil, nil, errors.New("pass -app-key; the benchmark does not run the approval flow")
	}
	key, err := parseAppKey(appKey)
	if err != nil {
		return nil, nil, err
	}
	builder, err := siastorage.NewBuilder(indexer, benchApp)
	if err != nil {
		return nil, nil, err
	}
	defer builder.Close()

	sdk, err := builder.Connect(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	return sdk, func() {}, nil
}

func mustAppID(s string) types.Hash256 {
	var id types.Hash256
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != len(id) {
		panic("bad app id literal")
	}
	copy(id[:], raw)
	return id
}
