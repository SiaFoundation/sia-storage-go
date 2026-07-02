package siastorage

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestRecoveryPhrase(t *testing.T) {
	phrase := GenerateRecoveryPhrase()
	if err := ValidateRecoveryPhrase(phrase); err != nil {
		t.Fatalf("generated phrase failed validation: %v", err)
	}
	if err := ValidateRecoveryPhrase("not a valid phrase"); err == nil {
		t.Fatal("expected invalid phrase to fail validation")
	}
}

func TestAppKey(t *testing.T) {
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	key, err := NewAppKey(seed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key.Export(), seed) {
		t.Fatal("exported key does not match seed")
	}

	msg := []byte("hello, world")
	sig := key.Sign(msg)
	if ok, err := key.VerifySignature(msg, sig); err != nil || !ok {
		t.Fatalf("expected signature to verify, got ok=%v err=%v", ok, err)
	}
	if ok, err := key.VerifySignature([]byte("tampered"), sig); err != nil || ok {
		t.Fatalf("expected tampered message to fail verification, got ok=%v err=%v", ok, err)
	}

	if _, err := NewAppKey(seed[:16]); err == nil {
		t.Fatal("expected short key to be rejected")
	}
}

func TestObjectSealRoundtrip(t *testing.T) {
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	key, err := NewAppKey(seed)
	if err != nil {
		t.Fatal(err)
	}

	obj := NewObject()
	meta := []byte(`{"name":"test.txt"}`)
	obj.UpdateMetadata(meta)

	sealed := obj.Seal(key)
	opened, err := OpenObject(key, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Id() != obj.Id() {
		t.Fatalf("object ID changed across seal/open: %v != %v", opened.Id(), obj.Id())
	}
	if !bytes.Equal(opened.Metadata(), meta) {
		t.Fatalf("metadata changed across seal/open: %q != %q", opened.Metadata(), meta)
	}

	// opening with a different key must fail
	otherKey, err := NewAppKey(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenObject(otherKey, obj.Seal(key)); err == nil {
		t.Fatal("expected open with wrong key to fail")
	}
}

func TestEncodedSize(t *testing.T) {
	const sectorSize = 1 << 22 // 4 MiB
	if got := EncodedSize(1, 10, 20); got != 30*sectorSize {
		t.Fatalf("unexpected encoded size: %d", got)
	}
}
