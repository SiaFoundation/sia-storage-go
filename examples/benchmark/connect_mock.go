//go:build siastorage_mock

package main

import (
	"context"
	"crypto/rand"
	"errors"

	"go.sia.tech/siastorage"
)

// engineName tags every sample, so a merged run says which arm it came from.
const engineName = "cabi-mock"

func connect(ctx context.Context, indexer, appKey string, hosts int) (*siastorage.SDK, func(), error) {
	if indexer != "" {
		return nil, nil, errors.New("built with siastorage_mock, so it cannot reach an indexer; rebuild without the tag")
	}
	net := siastorage.NewMockNetwork(hosts)
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		net.Close()
		return nil, nil, err
	}
	sdk, err := net.SDK(ctx, seed)
	if err != nil {
		net.Close()
		return nil, nil, err
	}
	return sdk, func() { net.Close() }, nil
}
