package siastorage

import (
	"bytes"
	"testing"

	"go.sia.tech/core/types"
)

func TestSeedPhrase(t *testing.T) {
	if NewSeedPhrase() == "" {
		t.Fatal("expected a non-empty seed phrase")
	}
}

func TestGenerateAppID(t *testing.T) {
	a, b := GenerateAppID(), GenerateAppID()
	if a == (types.Hash256{}) {
		t.Fatal("expected a non-zero app ID")
	}
	if a == b {
		t.Fatal("expected distinct app IDs")
	}
}

func TestObjectSealRoundtrip(t *testing.T) {
	appKey := types.GeneratePrivateKey()

	obj := NewEmptyObject()
	meta := []byte(`{"name":"test.txt"}`)
	obj.UpdateMetadata(meta)

	sealed := obj.Seal(appKey)
	opened, err := sealed.Open(appKey)
	if err != nil {
		t.Fatal(err)
	}
	if opened.ID() != obj.ID() {
		t.Fatalf("object ID changed across seal/open: %v != %v", opened.ID(), obj.ID())
	}
	if !bytes.Equal(opened.Metadata(), meta) {
		t.Fatalf("metadata changed across seal/open: %q != %q", opened.Metadata(), meta)
	}

	// opening with a different key must fail
	if _, err := sealed.Open(types.GeneratePrivateKey()); err == nil {
		t.Fatal("expected open with wrong key to fail")
	}
}

func TestSealedObjectSerializable(t *testing.T) {
	appKey := types.GeneratePrivateKey()
	obj := NewEmptyObject()
	sealed := obj.Seal(appKey)
	if sealed.Id != hashToString(obj.ID()) {
		t.Fatalf("sealed object ID %q does not match object ID %q", sealed.Id, hashToString(obj.ID()))
	}
}

func TestEncodedSize(t *testing.T) {
	const sectorSize = 1 << 22 // 4 MiB
	if got := EncodedSize(1, 10, 20); got != 30*sectorSize {
		t.Fatalf("unexpected encoded size: %d", got)
	}
}
