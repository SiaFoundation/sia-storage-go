//go:build siastorage_mock

package main

import (
	"context"
	"crypto/rand"
	"errors"

	"go.sia.tech/siastorage"
)

// connect ignores the profile: a mock network needs no indexer and no app key,
// which is the whole reason the mock build exists.
func connect(ctx context.Context, _ string, hosts int) (*siastorage.SDK, error) {
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

var errMockBuild = errors.New("built with siastorage_mock, which reaches no indexer; rebuild without the tag")

func login(context.Context, string, string, bool) error { return errMockBuild }

func listProfiles() error { return errMockBuild }
