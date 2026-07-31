package siastorage

import (
	"bytes"
	"crypto/cipher"
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

// encryptV0 returns a cipher.StreamReader that encrypts r with key starting at
// the given offset using the legacy object-wide cipher.
func encryptV0(key *[32]byte, r io.Reader, offset uint64) cipher.StreamReader {
	return cipher.StreamReader{S: newV0CipherStream(key, offset), R: r}
}

func TestEncryptRoundtripV0(t *testing.T) {
	var data [4096]byte
	frand.Read(data[:])

	var key [32]byte
	frand.Read(key[:])

	for _, offset := range []uint64{0, 16, 31, 63, 64, 96, 128, 2048, 4096, maxBytesPerNonce - 127, maxBytesPerNonce - 128, maxBytesPerNonce - 63, maxBytesPerNonce - 64, maxBytesPerNonce, 2 * maxBytesPerNonce} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			r := encryptV0(&key, bytes.NewReader(data[:]), offset)

			read, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}

			// chacha20 is symmetric, so encrypting the ciphertext again with
			// the same key and offset recovers the plaintext.
			decrypted, err := io.ReadAll(encryptV0(&key, bytes.NewReader(read), offset))
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(data[:], decrypted) {
				t.Fatalf("data mismatch: expected %v, got %v", data[:], decrypted)
			}
		})
	}
}

func TestEncryptRoundtripV1(t *testing.T) {
	var data [4096]byte
	frand.Read(data[:])

	dataKey := frand.Entropy256()
	slabKey := frand.Entropy256()

	for _, offset := range []uint64{0, 16, 31, 63, 64, 96, 128, 2048, 4096, maxBytesPerNonce - 127, maxBytesPerNonce - 128, maxBytesPerNonce - 63, maxBytesPerNonce - 64, maxBytesPerNonce, 2 * maxBytesPerNonce} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			ciphertext := data
			newV1CipherStream(&dataKey, &slabKey, offset).XORKeyStream(ciphertext[:], ciphertext[:])

			plaintext := ciphertext
			newV1CipherStream(&dataKey, &slabKey, offset).XORKeyStream(plaintext[:], plaintext[:])

			if !bytes.Equal(data[:], plaintext[:]) {
				t.Fatalf("data mismatch: expected %v, got %v", data[:], plaintext[:])
			}
		})
	}
}

// TestEncryptGolden tests that the v0 and v1 cipher streams produce known
// ciphertext. The expected values were generated with the Rust SDK's
// Chacha20Cipher to ensure both implementations remain compatible.
func TestEncryptGolden(t *testing.T) {
	var dataKey, slabKey [32]byte
	for i := range dataKey {
		dataKey[i] = byte(i)
		slabKey[i] = byte(255 - i)
	}
	plaintext := make([]byte, 192)
	for i := range plaintext {
		plaintext[i] = byte(i)
	}

	tests := []struct {
		offset uint64
		v0     string
		v1     string
	}{
		{0,
			"c2a69994e4ec1dd0d88e99c27049091903fa44f7fa5154ce90200d908dd63e35f31a4dd63fded6f67d5679ec97ffeb9f40f6cc95ded49bd71cb49788c993e028d547ce84a8cda35e5ec73572a05b70a56cedffc02ec51b5c93b4cf068bbb446e06eb02db5d273c97b796c5bf3f60fcbc3932a6cadbd58e7d885a11285769fd26e99b3b9203008430c456640900a79426cdb15a01bfe3bd6455b2dc81ea99def00cf2f54a2da1f909b9109e9a1636664c46be8666fcd95c99e191b409f90f1433",
			"377d9a49ee1d139afad0ef61052b513aaf6e1549455c15518adc712ff1663dc483279aa88f6e42640f1d895636295b8da747cfdc53a0da26e8ee5caf6717e036ecb11e31ffaba02acd7ed354c7d1c56f2e7307caf5b01400f550bc6b078b2fcb720c1ff9ac4a7adc102c43e84200a0a16cf925ac9bd3ec47184208c45f05ab1ce1858065a49ce2665cee90347550e4d0f2a59f0dd7cc17b6d9c206f641a01f6a365c569324109a7ffa0b8a994e51232c3a38a955230638b8f9ed9d1c01248a65"},
		{1,
			"a79a95e3ed1ed1d78f9ac377480a181cfb47f6fd5057cf9f210e918ad73d34cc1b4ed738dfd5f772577aed90fee89e5ff7cf94d9d598d613b59489ce92e329aa46cd85afcca05f51c63673a75a73a473ecfcc129c4185d9cb5cc078cba476f39ea01da5a263f96b897c6be3861ffbd2633a5cbdcd48d7c875b12295068fe27169a389304018731cb57670807a69727d2b05900b8e2be655ab3df80ed98ddf133f3f64b2aa0fa08b6119d9b1137654d59bf8567fbd85f98ee90b708fe0e173255",
			"7c9948e91c109bf5d1ec60022a523bb06f1648425d165085dd722ef6673ec5bc2699a9886f4165001c8a573128588cb846ccdd54a1d927e7ef5fae6016e33793b01d30f8aaa32bc27fd055c0d0c66e317204cbf2b11701fa51bf6a008a2cca4d0d1cf8ab4b79dd1f2d40e94501a3a073f826ad9cd2ef4617430bc55804a81d1e848364a39de16753ef93357251e7d1eda49c0cd0cd14b7d6c305f746a11c6b095d55922311997ef50a89984950202d2539aa5424073bb9f6ec9e1d06258964a4"},
		{63,
			"1794048fc3e98ee2111f847435e11831fa2daebe876f865a13d2f78e41caf8051147a8439c1c647dd8f6d584f87e23bde37871e78d9a96cf32c919506f162abc192858fa55c2c345ff0595a5cec16455f90c729bc67e207cab94711d462b5a1f0fcd31348dec6238c678d35f5dd7f5a793877d47a13d1a9d56205275ce38ccd50c6b2a0c5ed7fac42a0574049e8d48d79f86e683f8238db8fc57e0a07952614d1d1d4a92d3022ee2a30b0ba03ff4242b6bfb692c7e03ea08c02ba9969f693d99",
			"09adf25f76bee8e1658c3d9213869284306f30468db4f3554fb413fd2c46c86eb4334f5ebeed093b93516f02af0343e1fe2dba64ebda90ad08590149831e46ea23204641a2655f23a99d2d51f3b493250f33665eca160fd6791801c7318063de95f79f9754e5d35bb03bc84b5e8f92e2f3fbfb6892e2c5f977382e5cdbc0e74b5a9a4abade443525880215d04499d67227216ce02efb90f69914bfda5e6e7ea5817e5517006458f34a14a73d822e6f6aa832477004fc10fa5f94fbfd063e63e0"},
		{64,
			"95078ec4e88de31e1e877532e01b30e52cadbf806e855b1cd3f48f46cbfb042e46ab429b1d677cd7f7d685ff7f20bcfc7972e68a9b95ce3dc81a51681729bd66295bfb52c3c044f00496a4c9c06754e60d719ac17f237da495721c412a591e30cc32358aed6139c979d05e5ad6f6a68c867e46a63c199c59215174c939cfd4f36a290d59d6f9c525047705998c4bd68087e582ff228eb9f356e3a17e53624c221c4993d4032de3ac0a08a138f5272a74fa6a2d7902e909cf2aaa9798683e9866",
			"acf15e71bfebe06a8d3e93148791852f6e33478ab5f05440b510fc2b47cb6f8b324c5fb9ec0a3a9c506c03a80240e0e12cb965ecdb93ac07580248841f45eb5c214540a5645c22a69c2e50f4b590241032655fcd170cd7761902c6368160dfaaf69c9653e4d05abf3acb4a598e91e3ecfaf86995e3c6f878392d5ddcc1e44aa59b49bbd9453624870316d14398d57338206fe129fa93f79615bcdb596f7da4be7f561607655bf24515a43c852f6c6bb733447103fd13fb5095f8fc013f60e1f0"},
		{100,
			"394358f3dbfaa9d35b0498d8454edab6bfb1ea19e4367d44330d99427507a70ea7a4209468fac8a5a4033082710de6bd1b4719c0f91e702d4e3d7a54906e69d6c9451ded55fc7276f2d282a8ba427a9a183db87d0d7d58e51debf0d7b6f5d185321d21c1e89be97568af32647b197e03c66a5d17ba0f4d92b786a8c6c0954f082709c78826248d14d1030e50c656114526cd2deb0686bbb44c1abc42f9cf14873835d2168bacbbf969c572a399b358d0f2e510f520f2600d7c32463e4f0922a5",
			"c82e1eb87c402f842664c4c5108559d0ffb78823742e64a83b61cf787d191cf9003846c2f0423c98d1f440744e1923b17368b312756eaa5ae504bbceaac0ca0fc0f47e9b16e76675aab5c7c8c6c455a9c7e2dc5c150171f0e5c06e8147956705a1d2c063effa3daf7c3197dcdc931dd51e771372f95037b58b99405aa38acadb417fd661398810a90b484f930f784d3fd937df74b9d4d02d1b44c5d444c315233194bfa22186821cb55182d7947702fb13d4e04093738db2dbce96385c198ad9"},
		{maxBytesPerNonce - 127,
			"4578c43575c4d5efe2078888f7a5e4fb5c18d9d57fb4481c1944ac2ef8820fbc75f7f348ac05201a0cafb845c41f91f694cdd784c343604c0970145ef36fb383c5497350861ed8a0d5f9fe7610137a43f4c86ee300c627202a49b96cc6c5cca2d499cbc61151c75c06ca6e95a91cb47b234ddd1873f76d7237ceb389578268be878ef7cf993270fdaa6abf323848832cd40ffe0c5ee1da9fca9559e680aa82492c0a5cacbd6bc9a8e9574cc8cdcee067fa06d7d15a5eaba35db5d0913915998d",
			"8374988e4fc254c08bd04f3638f9d854a0ff67c34ae3828cf580ad1ab8db1f45bf13afc7cd9848aad91a5f0c1da9a190d767dab623ba2c6eef08c0d4294d1fe46373793786c6788235139b90e33da58f0a4360d81e7ab6a9530b5985487f9e737910b10c497edc16be7fc130a06aebedd382aa91941d5531d9b2dd4adf8c8f75a1ddaf1ebceff497261afbfdf0ad5dce163bac27574c8b56daf0d9224fa8c82788d2d95e7f5b5ef138521854dbdc629884ca9c690f327c5faa3e372512eec26c"},
		{maxBytesPerNonce - 64,
			"bc840a3217c75d99ef94babf3151503b1cb58b2fa44185666f6b0af82b87868ddd95da8a815012861347892fd2e85ff524620e9c5f32b42c3d768df2ce16c12981464d360858f1b1326ba97ef5f98b42f315cc3fcb9f221b500b569821416943b6edc99d6b7ca8086728948d0f0c0d21b83bc516169b9d6a6c9c761156f8d658b2e9d5a70812f0633c76323084970ff7860055f87969cdd996dcd0488eae385779b916b0cc2bed183e1977c1f3d2e13917d893b99b25de8326fe0c28cd122d3b",
			"db22303870c78539cd7450dad7a27ee4d04b00219f5f39f7e6124818c2093cdf0c3853f04b083d9d59ff3c8077e129aab292c1ebd6d55e147e98f19c0d9ecfce4a601e6ed97d2c3558e7d93a3a316e9c11d7f86de0968f4a991b3318e58e6b09d849111899be989f3ef991d9931a1fa34745095daecef1bd906bfdf6e2d32d035368fd25893ea0f3ede02b09d38a552149554afd0871cd45b25e5cb173cad06b1f7713466c1a654da7fa91eee5f75faa751224485241d1808e965e4e105910fc"},
		{maxBytesPerNonce,
			"c1060d764818b1f1722be93eb5b9cb02b3558c7f8bdf625b104b16d861012903f6ad89dd2b3ce8482768d4cd4f4c4d61f87b855656dbdd2a2cdc365116b8961872291567c8d230a3fcb6f2f04457cf3746c09538b9a90d19561c10884e6ef897b979d6700ceb2dd8fed9b701331221f9d71853795be51e43e63ecce80dd2edfbbe22c24e2249a0277cc343a4cf6c89d584788acf20224c3b614a6fb622475bacc32b64cc3b5755257fafa9403bd0487f4220261851cfa83da811c30af0caa953",
			"377d9a49ee1d139afad0ef61052b513aaf6e1549455c15518adc712ff1663dc483279aa88f6e42640f1d895636295b8da747cfdc53a0da26e8ee5caf6717e036ecb11e31ffaba02acd7ed354c7d1c56f2e7307caf5b01400f550bc6b078b2fcb720c1ff9ac4a7adc102c43e84200a0a16cf925ac9bd3ec47184208c45f05ab1ce1858065a49ce2665cee90347550e4d0f2a59f0dd7cc17b6d9c206f641a01f6a365c569324109a7ffa0b8a994e51232c3a38a955230638b8f9ed9d1c01248a65"},
		{maxBytesPerNonce + 100,
			"688f49bc92b5db6d5776459dab642f053f817a278a52a08469b6899f22be5ed286ed0483d06fef086bc82d7138c436738486e89fcde6c31a86e3ff085fb7f850dfb3b1c1934345acdf34ac9bbedcdae4b52b4cd944fd2fe6142e4db7b017101c6e85c89c4ad129d89daf266b4d314551574fb6f2cd64f30dd9da47bcadb39615ab8d39f437e1f9e9e2b265fe8f49526dacd0f86dd05db70ba53d84137aacbbbe0623def33eb48b8f180c32a1bfb0a9666b9c02e84d2fcbeca0ba275d54d754e4",
			"c82e1eb87c402f842664c4c5108559d0ffb78823742e64a83b61cf787d191cf9003846c2f0423c98d1f440744e1923b17368b312756eaa5ae504bbceaac0ca0fc0f47e9b16e76675aab5c7c8c6c455a9c7e2dc5c150171f0e5c06e8147956705a1d2c063effa3daf7c3197dcdc931dd51e771372f95037b58b99405aa38acadb417fd661398810a90b484f930f784d3fd937df74b9d4d02d1b44c5d444c315233194bfa22186821cb55182d7947702fb13d4e04093738db2dbce96385c198ad9"},
		{2 * maxBytesPerNonce,
			"89da682c278d50043b5bdc978d0a67a472d506cd5ed4a6f0d48d840f402b6f86561130ac91d269633376c8ab19589491ef94a5fc8a56606d03701762aad71bcb2055c7ce826f2c903f55aae2c681bbb49bec6a08e0fd35f707f82bdd2f2485a2a1a07f2e9195f0b3ec4df23a214710f9c2481857761991c1a67014ba2232968ac3311748fb8878709a23fd2eead1b87b13569a21ec324d4998f7ab30e0c7f9ee45c4af0683bdc3abf9ec6d588ecde3a16d84f0218291d47be36bda746cae07ab",
			"377d9a49ee1d139afad0ef61052b513aaf6e1549455c15518adc712ff1663dc483279aa88f6e42640f1d895636295b8da747cfdc53a0da26e8ee5caf6717e036ecb11e31ffaba02acd7ed354c7d1c56f2e7307caf5b01400f550bc6b078b2fcb720c1ff9ac4a7adc102c43e84200a0a16cf925ac9bd3ec47184208c45f05ab1ce1858065a49ce2665cee90347550e4d0f2a59f0dd7cc17b6d9c206f641a01f6a365c569324109a7ffa0b8a994e51232c3a38a955230638b8f9ed9d1c01248a65"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.offset), func(t *testing.T) {
			v0 := make([]byte, len(plaintext))
			newV0CipherStream(&dataKey, tt.offset).XORKeyStream(v0, plaintext)
			if got := hex.EncodeToString(v0); got != tt.v0 {
				t.Fatalf("v0 ciphertext mismatch: expected %s, got %s", tt.v0, got)
			}

			v1 := make([]byte, len(plaintext))
			newV1CipherStream(&dataKey, &slabKey, tt.offset).XORKeyStream(v1, plaintext)
			if got := hex.EncodeToString(v1); got != tt.v1 {
				t.Fatalf("v1 ciphertext mismatch: expected %s, got %s", tt.v1, got)
			}
		})
	}
}
