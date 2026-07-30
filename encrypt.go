package siastorage

import (
	"crypto/cipher"
	"encoding/binary"
	"io"
	"math"

	"golang.org/x/crypto/chacha20"
)

type rekeyStream struct {
	key []byte
	c   *chacha20.Cipher

	counter uint64
	nonce   uint64
}

const (
	// maximum amount of data we can encrypt with a single nonce because
	// counter is a uint32 and each tick is 64 bytes
	maxBytesPerNonce = 64 * math.MaxUint32
)

func (rs *rekeyStream) XORKeyStream(dst, src []byte) {
	if len(src) == 0 {
		return
	}

	rs.counter += uint64(len(src))
	if rs.counter < maxBytesPerNonce {
		rs.c.XORKeyStream(dst, src)
		return
	}

	// counter overflow; xor remaining bytes, then increment nonce and xor again
	rem := maxBytesPerNonce - (rs.counter - uint64(len(src)))
	rs.c.XORKeyStream(dst[:rem], src[:rem])
	src = src[rem:]
	dst = dst[rem:]

	// reset counter and re-key the cipher with an incremented nonce
	rs.counter = uint64(len(src))
	rs.nonce++
	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], rs.nonce)
	rs.c, _ = chacha20.NewUnauthenticatedCipher(rs.key, nonce)

	rs.c.XORKeyStream(dst, src)
}

func nonce(offset uint64) (nonce [24]byte, nonce64 uint64) {
	nonce64 = offset / maxBytesPerNonce
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	return
}

func newRekeyStream(key *[32]byte, offset uint64) *rekeyStream {
	n, n64 := nonce(offset)
	offset %= maxBytesPerNonce
	skip := int(offset % 64)

	c, _ := chacha20.NewUnauthenticatedCipher(key[:], n[:])
	c.SetCounter(uint32(offset / 64))
	if skip > 0 {
		var discard [64]byte
		c.XORKeyStream(discard[:skip], discard[:skip])
	}
	return &rekeyStream{key: key[:], c: c, counter: offset, nonce: n64}
}

// encrypt returns a cipher.StreamReader that encrypts r with k starting at the
// given offset.
func encrypt(key *[32]byte, r io.Reader, offset uint64) cipher.StreamReader {
	return cipher.StreamReader{S: newRekeyStream(key, offset), R: r}
}
