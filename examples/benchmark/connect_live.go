//go:build !siastorage_mock

package main

import (
	"context"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/siastorage"
)

// benchApp matches the Rust benchmark's APP_META exactly, so an app key
// registered by either tool is the same application to the indexer and the
// profiles they share are usable in both.
var benchApp = siastorage.AppMetadata{
	AppID:       mustAppID("5c0b1af28e6ac76395b2087ea987297b9c496f90d2ab3e3d3d07980ae4c43633"),
	Name:        "Benchmark",
	Description: "A simple upload and download benchmark for the SDK",
	ServiceURL:  "https://sia.tech",
}

// connect opens an SDK from a stored profile, so a run needs no app key on the
// command line.
func connect(ctx context.Context, profileName string, _ int) (*siastorage.SDK, error) {
	indexer, key, err := readProfile(profileName)
	if err != nil {
		return nil, err
	}
	builder, err := siastorage.NewBuilder(indexer, benchApp)
	if err != nil {
		return nil, err
	}
	defer builder.Close()

	sdk, err := builder.Connect(ctx, key)
	if errors.Is(err, siastorage.ErrUnauthorized) {
		return nil, fmt.Errorf("the app key for profile %q is not authorized; run `benchmark login -profile %s`", profileName, profileName)
	} else if err != nil {
		return nil, err
	}
	return sdk, nil
}

// login runs the approval flow once and stores the app key under profileName,
// so later runs skip it.
func login(ctx context.Context, profileName, indexer string, newPhrase bool) error {
	builder, err := siastorage.NewBuilder(indexer, benchApp)
	if err != nil {
		return err
	}
	defer builder.Close()

	url, err := builder.RequestConnection(ctx)
	if err != nil {
		return fmt.Errorf("request connection: %w", err)
	}
	fmt.Println("Visit the following URL to authorize the application:", url)
	if err := builder.WaitForApproval(ctx); err != nil {
		return fmt.Errorf("wait for approval: %w", err)
	}
	fmt.Println("Connection approved.")

	var phrase string
	if newPhrase {
		phrase = siastorage.NewSeedPhrase()
		fmt.Printf("Generated recovery phrase, write it down:\n  %s\n", phrase)
	} else if phrase, err = readPhrase(); err != nil {
		return err
	}

	sdk, err := builder.Register(ctx, phrase)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	defer sdk.Close()

	path, err := upsertProfile(profileName, indexer, sdk.AppKey())
	if err != nil {
		return fmt.Errorf("save profile: %w", err)
	}
	fmt.Printf("Profile %q saved to %s, indexer %s\n", profileName, path, indexer)
	return nil
}

func mustAppID(s string) types.Hash256 {
	var id types.Hash256
	if err := id.UnmarshalText([]byte(s)); err != nil {
		panic("bad app id literal")
	}
	return id
}
