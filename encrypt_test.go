package siastorage

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// TestDeriveAppKeyGolden tests that deriving an app key from
// a known mnemonic, app ID, and shared secret produces
// the expected app key. This is to ensure compatibility
// with other implementations.
func TestDeriveAppKeyGolden(t *testing.T) {
	const (
		mnemonic          = "glare own entire dish exact open theme family harsh room scrap rose"
		appIDStr          = "0e90d697f5045a6593f1c43ebf79a369e2bc72cc5c7b6282f3b5aeb0de6e4005"
		sharedSecretStr   = "cf02d945fe4bfe614d823dc13c19aa8501699e656d0f7915490c3056d5c97dc6"
		expectedAppKeyStr = "b75061f34bb3aeab232b0671da2d0347c547343a0026bb5535c291d964fd09a1"
	)

	seed, err := hex.DecodeString(expectedAppKeyStr)
	if err != nil {
		t.Fatal(err)
	}
	expectedAppKey := types.NewPrivateKeyFromSeed(seed)

	var appID, sharedSecret types.Hash256
	if err := appID.UnmarshalText([]byte(appIDStr)); err != nil {
		t.Fatal(err)
	} else if err := sharedSecret.UnmarshalText([]byte(sharedSecretStr)); err != nil {
		t.Fatal(err)
	}

	appKey, err := deriveAppKey(mnemonic, appID, sharedSecret)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(appKey, expectedAppKey) {
		t.Fatal("derived app key does not match expected")
	}
}

func TestRekeyStreamSeek(t *testing.T) {
	// a stream positioned at an interior offset must continue the keystream
	// of one that streamed continuously from the start, so chunks and slabs
	// can be encrypted and decrypted independently of how the object was
	// originally streamed
	var data [1 << 16]byte // 64 KiB
	frand.Read(data[:])

	var key [32]byte
	frand.Read(key[:])

	// segment sizes chosen to misalign with the 64 byte block size
	sizes := []int{1, 13, 63, 64, 65, 100, 1000, 4096, 16384}

	for _, base := range []uint64{0, 16, 2061, maxBytesPerNonce - 1<<15, maxBytesPerNonce - 63, 3*maxBytesPerNonce - 1000} {
		t.Run(fmt.Sprint(base), func(t *testing.T) {
			// encrypt with one continuous stream
			enc := make([]byte, len(data))
			newRekeyStream(&key, base).XORKeyStream(enc, data[:])

			// decrypt piecewise with a fresh stream per segment
			dec := make([]byte, len(data))
			for pos, i := 0, 0; pos < len(data); i++ {
				n := min(sizes[i%len(sizes)], len(data)-pos)
				newRekeyStream(&key, base+uint64(pos)).XORKeyStream(dec[pos:pos+n], enc[pos:pos+n])
				pos += n
			}

			if !bytes.Equal(dec, data[:]) {
				t.Fatal("mismatch")
			}
		})
	}
}

func TestEncryptRoundtrip(t *testing.T) {
	var data [4096]byte
	frand.Read(data[:])

	var key [32]byte
	frand.Read(key[:])

	for _, offset := range []uint64{0, 16, 31, 63, 64, 96, 128, 2048, 4096, maxBytesPerNonce - 127, maxBytesPerNonce - 128, maxBytesPerNonce - 63, maxBytesPerNonce - 64, maxBytesPerNonce, 2 * maxBytesPerNonce} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			r := encrypt(&key, bytes.NewReader(data[:]), offset)

			read, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}

			// chacha20 is symmetric, so encrypting the ciphertext again with
			// the same key and offset recovers the plaintext.
			decrypted, err := io.ReadAll(encrypt(&key, bytes.NewReader(read), offset))
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(data[:], decrypted) {
				t.Fatalf("data mismatch: expected %v, got %v", data[:], decrypted)
			}
		})
	}
}
