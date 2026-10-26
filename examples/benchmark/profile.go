//go:build !siastorage_mock

package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"go.sia.tech/core/types"
	"golang.org/x/term"
)

// The on-disk location and format match the Rust benchmark in sia-sdk-rs, so a
// profile created by either tool works in the other:
//   - location: the `directories` crate's ProjectDirs("tech", "Sia", "sia-benchmark")
//   - format:   TOML, with a `[profiles.<name>]` table per profile
//   - app_key:  the 32-byte app-key seed, hex-encoded, matching AppKey::export

type config struct {
	Profiles map[string]profile `toml:"profiles"`
}

type profile struct {
	Indexer string `toml:"indexer"`
	AppKey  string `toml:"app_key"`
}

func configPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("could not determine config directory: %w", err)
	}
	var rel string
	switch runtime.GOOS {
	case "darwin":
		rel = filepath.Join("tech.Sia.sia-benchmark", "config.toml")
	case "windows":
		rel = filepath.Join("Sia", "sia-benchmark", "config", "config.toml")
	default: // linux and other XDG platforms
		rel = filepath.Join("sia-benchmark", "config.toml")
	}
	return filepath.Join(base, rel), nil
}

func loadConfig() (config, error) {
	path, err := configPath()
	if err != nil {
		return config{}, err
	}
	cfg := config{Profiles: map[string]profile{}}
	if _, err := toml.DecodeFile(path, &cfg); errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	} else if err != nil {
		return config{}, err
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]profile{}
	}
	return cfg, nil
}

func saveConfig(cfg config) (string, error) {
	path, err := configPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	// The file holds app key seeds, so it is written for this user only.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	if err := toml.NewEncoder(f).Encode(cfg); err != nil {
		return "", errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return path, nil
}

func readProfile(name string) (string, types.PrivateKey, error) {
	cfg, err := loadConfig()
	if err != nil {
		return "", nil, err
	}
	p, ok := cfg.Profiles[name]
	if !ok {
		return "", nil, fmt.Errorf("profile %q not found; run `benchmark login -profile %s` first", name, name)
	}
	seed, err := hex.DecodeString(p.AppKey)
	if err != nil {
		return "", nil, fmt.Errorf("profile %q has an invalid app key: %w", name, err)
	}
	if len(seed) != 32 {
		return "", nil, fmt.Errorf("profile %q has an invalid app key: expected 32 bytes, got %d", name, len(seed))
	}
	return p.Indexer, types.NewPrivateKeyFromSeed(seed), nil
}

func upsertProfile(name, indexer string, key types.PrivateKey) (string, error) {
	if len(key) < 32 {
		return "", fmt.Errorf("app key too short: %d bytes", len(key))
	}
	cfg, err := loadConfig()
	if err != nil {
		return "", err
	}
	cfg.Profiles[name] = profile{
		Indexer: strings.TrimSpace(indexer),
		// The seed rather than the expanded key, matching AppKey::export, so
		// the profile is interchangeable with the Rust benchmark.
		AppKey: hex.EncodeToString(key[:32]),
	}
	return saveConfig(cfg)
}

func listProfiles() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if len(cfg.Profiles) == 0 {
		path, err := configPath()
		if err != nil {
			return err
		}
		fmt.Printf("no profiles in %s; run `benchmark login` to create one\n", path)
		return nil
	}
	names := make([]string, 0, len(cfg.Profiles))
	width := 0
	for name := range cfg.Profiles {
		names = append(names, name)
		if len(name) > width {
			width = len(name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf("  %-*s  %s\n", width, name, cfg.Profiles[name].Indexer)
	}
	return nil
}

// readPhrase reads the recovery phrase from stdin, without echoing it when
// stdin is a terminal. A single line otherwise, so the benchmark can still be
// driven from a pipe.
func readPhrase() (string, error) {
	fmt.Println("Enter recovery phrase:")
	if term.IsTerminal(int(os.Stdin.Fd())) {
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err != nil {
			return "", fmt.Errorf("failed to read recovery phrase: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("failed to read recovery phrase: %w", err)
		}
		return "", errors.New("failed to read recovery phrase: unexpected EOF on stdin")
	}
	return strings.TrimSpace(scanner.Text()), nil
}
