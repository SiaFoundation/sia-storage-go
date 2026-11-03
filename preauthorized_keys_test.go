package siastorage

import (
	"bytes"
	"encoding/hex"
	"math"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

// TestPreAuthorizedKeySeedRoundTrip proves the seed is the whole credential.
func TestPreAuthorizedKeySeedRoundTrip(t *testing.T) {
	key := NewPreAuthorizedKey()
	seed := key.Export()
	if seed == ([32]byte{}) {
		t.Fatal("the exported seed is all zeroes")
	}
	if NewPreAuthorizedKey().Export() == seed {
		t.Fatal("two generated keys share a seed")
	}

	imported := ImportPreAuthorizedKey(seed)
	if imported.PublicKey() != key.PublicKey() {
		t.Fatal("a key rebuilt from its seed has a different public key")
	}
	if imported.Export() != seed {
		t.Fatal("the reimported key exports a different seed")
	}
}

// TestPreAuthorizedKeySign checks signatures against core's ed25519, so a
// mismatch is caught here rather than by an indexer refusing the request.
func TestPreAuthorizedKeySign(t *testing.T) {
	key := NewPreAuthorizedKey()

	h := types.HashBytes([]byte("hello"))
	sig := key.Sign(h[:])
	if !key.PublicKey().VerifyHash(h, sig) {
		t.Fatal("core rejects the key's signature")
	}
	if !key.VerifySignature(h[:], sig) {
		t.Fatal("the key rejects its own signature")
	}
	wrong := types.HashBytes([]byte("wrong"))
	if key.VerifySignature(wrong[:], sig) {
		t.Fatal("a signature verified against the wrong message")
	}
	if ImportPreAuthorizedKey([32]byte{1}).VerifySignature(h[:], sig) {
		t.Fatal("a signature verified against the wrong key")
	}

	for _, empty := range [][]byte{nil, {}} {
		sig := key.Sign(empty)
		if !key.VerifySignature(empty, sig) {
			t.Fatalf("the signature of %#v does not verify", empty)
		}
	}
}

// TestPreAuthorizedKeySignRequestMatchesIndexd verifies the signed request with
// indexd's own SigHash, which is what the indexer checks on submission.
func TestPreAuthorizedKeySignRequestMatchesIndexd(t *testing.T) {
	key := NewPreAuthorizedKey()
	appID := types.Hash256{3}
	expiration := time.Date(2030, 1, 2, 3, 4, 5, 123456789, time.UTC)

	for _, allowed := range []*types.Hash256{nil, &appID} {
		signed, err := key.SignRequest(PreAuthorizedKeyRequest{
			ConnectKey:   "connect-key",
			Expiration:   expiration,
			TotalUses:    5,
			AllowedAppID: allowed,
		})
		if err != nil {
			t.Fatalf("sign request: %v", err)
		}
		if !signed.PublicKey.VerifyHash(signed.SigHash(), signed.Signature) {
			t.Fatal("indexd rejects the signature")
		}
		if signed.PublicKey != key.PublicKey() {
			t.Fatalf("public key %v, want %v", signed.PublicKey, key.PublicKey())
		}
		if signed.ConnectKey != "connect-key" || signed.TotalUses != 5 {
			t.Fatalf("connect key %q and total uses %d were not carried over", signed.ConnectKey, signed.TotalUses)
		}
		if want := expiration.Truncate(time.Microsecond); !signed.Expiration.Equal(want) {
			t.Fatalf("expiration %v, want %v", signed.Expiration, want)
		}
		if (signed.AllowedAppID == nil) != (allowed == nil) || (allowed != nil && *signed.AllowedAppID != *allowed) {
			t.Fatalf("allowed app ID %v, want %v", signed.AllowedAppID, allowed)
		}
	}
}

// TestPreAuthorizedKeySignRequestVector pins the one Rust test vector that is
// exact at microsecond precision, so this package and the other bindings sign
// byte for byte the same request.
func TestPreAuthorizedKeySignRequestVector(t *testing.T) {
	appID := types.Hash256(bytes.Repeat([]byte{3}, 32))
	signed, err := ImportPreAuthorizedKey([32]byte(bytes.Repeat([]byte{2}, 32))).SignRequest(PreAuthorizedKeyRequest{
		ConnectKey:   "connect-🔑-é",
		Expiration:   time.Unix(-1, 500_000_000),
		TotalUses:    math.MaxUint32,
		AllowedAppID: &appID,
	})
	if err != nil {
		t.Fatalf("sign request: %v", err)
	}

	const (
		wantPublicKey = "ed25519:8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394"
		wantSigHash   = "e5cddeb3526d842a2d4c58f27e5aa6e589dfa5b4eda2b1e01a35593ddc322ff4"
		wantSignature = "bccab3b043a53103b44266479e2d5c02529ac012c73a3224ae098678001d3299a9d5198bd96b1ee762f0f620654123ec8b5009ce20bd683eb80ad0da8d8e2809"
	)
	if got := signed.PublicKey.String(); got != wantPublicKey {
		t.Fatalf("public key %s, want %s", got, wantPublicKey)
	}
	if got := signed.SigHash(); hex.EncodeToString(got[:]) != wantSigHash {
		t.Fatalf("sig hash %x, want %s", got, wantSigHash)
	}
	if got := hex.EncodeToString(signed.Signature[:]); got != wantSignature {
		t.Fatalf("signature %s, want %s", got, wantSignature)
	}
}
