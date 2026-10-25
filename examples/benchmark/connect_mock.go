//go:build siastorage_mock

package main

import (
	"context"
	"crypto/rand"
	"errors"

	"go.sia.tech/siastorage"
)

// engineName tags every sample, so a merged run says which build produced it.
const engineName = "cabi-mock"

func connect(ctx context.Context, indexer, _ string, hosts int) (*siastorage.SDK, error) {
	if indexer != "" {
		return nil, errors.New("built with siastorage_mock, so it cannot reach an indexer; rebuild without the tag")
	}
	net := siastorage.NewMockNetwork(hosts)
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, err
	}
	sdk, err := net.SDK(ctx, seed)
	if err != nil {
		return nil, err
	}
	betweenReps = net.ClearSectors
	return sdk, nil
}
