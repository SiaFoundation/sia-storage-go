package siastorage

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestAccountDecodesIndexerFieldNames pins the field names the indexer sends
// against the tags this package declares.
//
// Decoding alone would not catch a misspelled tag, because encoding/json falls
// back to a case insensitive match when no tag matches exactly. Encoding has no
// such fallback, so the marshalled form is what actually holds the tags to the
// names the indexer uses.
func TestAccountDecodesIndexerFieldNames(t *testing.T) {
	raw := `{
		"accountKey": "ed25519:0000000000000000000000000000000000000000000000000000000000000001",
		"maxPinnedData": 1024,
		"remainingStorage": 512,
		"pinnedData": 256,
		"pinnedSize": 768,
		"ready": true,
		"lastUsed": "2026-01-02T03:04:05Z",
		"app": {
			"id": "0000000000000000000000000000000000000000000000000000000000000002",
			"name": "some app",
			"description": "some description",
			"logoURL": "https://example.invalid/logo.png",
			"serviceURL": "https://example.invalid"
		}
	}`

	var a Account
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		t.Fatalf("account should decode: %v", err)
	}
	if a.MaxPinnedData != 1024 || a.RemainingStorage != 512 {
		t.Fatalf("quota fields decoded wrong, got %d and %d", a.MaxPinnedData, a.RemainingStorage)
	}
	if !a.Ready {
		t.Fatal("ready decoded as false")
	}
	if a.LastUsed.IsZero() {
		t.Fatal("lastUsed decoded as zero")
	}
	if a.App.Name != "some app" {
		t.Fatalf("app name decoded as %q", a.App.Name)
	}
	if a.App.LogoURL == nil || *a.App.LogoURL != "https://example.invalid/logo.png" {
		t.Fatalf("logoURL did not decode, got %v", a.App.LogoURL)
	}
	if a.App.ServiceURL == nil || *a.App.ServiceURL != "https://example.invalid" {
		t.Fatalf("serviceURL did not decode, got %v", a.App.ServiceURL)
	}

	// The half that can fail.
	out, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("account should encode: %v", err)
	}
	for _, want := range []string{`"logoURL"`, `"serviceURL"`, `"maxPinnedData"`, `"accountKey"`} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("encoded account is missing %s, got %s", want, out)
		}
	}
	for _, unwanted := range []string{`"logoUrl"`, `"serviceUrl"`} {
		if strings.Contains(string(out), unwanted) {
			t.Fatalf("encoded account still uses %s, got %s", unwanted, out)
		}
	}
}
