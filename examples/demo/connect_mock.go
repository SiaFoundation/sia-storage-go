//go:build siastorage_mock

package main

import (
	"context"
	"crypto/rand"
	"errors"

	"go.sia.tech/siastorage"
)

// mockHosts is comfortably above the 30 a default 10 of 30 slab needs, leaving
// spare hosts for the uploader to race.
const mockHosts = 40

// network is kept for report, which has mock only numbers to show.
var network *siastorage.MockNetwork

// connect brings up an in process network and an SDK on it. Real erasure
// coding, encryption and the whole transfer pipeline run against it; only the
// network itself is faked.
func connect(ctx context.Context, opts connectOptions) (*siastorage.SDK, func(), error) {
	if opts.IndexerURL != "" {
		return nil, nil, errors.New("this binary was built with siastorage_mock, so it cannot reach an indexer; rebuild without the tag")
	}

	stage("Start a mock network of %d hosts", mockHosts)
	network = siastorage.NewMockNetwork(mockHosts)

	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		network.Close()
		return nil, nil, err
	}
	sdk, err := network.SDK(ctx, seed)
	if err != nil {
		network.Close()
		return nil, nil, err
	}
	info("connected, no indexer and no credentials needed")
	return sdk, func() { network.Close() }, nil
}

// connectShared brings up the recipient side on the same mock network, so the
// demo exercises SharedSDK rather than the owner's view of a key.
func connectShared(ctx context.Context, seed [32]byte) (*siastorage.SharedSDK, error) {
	return network.SharedSDK(ctx, seed)
}

// report prints what the mock can see that a real indexer cannot.
func report(*siastorage.SDK) {
	stage("Mock network")
	info("%d slab(s) still pinned after pruning", network.PinnedSlabs())
}
