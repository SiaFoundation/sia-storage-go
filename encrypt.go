package siastorage

import (
	"crypto/cipher"
	"encoding/binary"
	"io"
	"math"
	"sync"

	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

type rekeyStream struct {
	key     []byte
	c       *chacha20.Cipher
	nonce   [24]byte
	nonce64 uint64 // offset / maxBytesPerNonce

	counter uint64
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

	// reset the counter and re-key with an incremented nonce. Only v0
	// object-wide streams reach here; a v1 slab never spans a full nonce.
	rs.counter = uint64(len(src))
	rs.nonce64++
	binary.LittleEndian.PutUint64(rs.nonce[16:], rs.nonce64)
	rs.c, _ = chacha20.NewUnauthenticatedCipher(rs.key, rs.nonce[:])

	rs.c.XORKeyStream(dst, src)
}

func newCipherStream(key *[32]byte, nonce [24]byte, offset uint64) *rekeyStream {
	nonce64 := offset / maxBytesPerNonce
	offset %= maxBytesPerNonce
	skip := int(offset % 64)

	c, _ := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	c.SetCounter(uint32(offset / 64))
	if skip > 0 {
		var discard [64]byte
		c.XORKeyStream(discard[:skip], discard[:skip])
	}
	return &rekeyStream{
		key:     key[:],
		c:       c,
		nonce:   nonce,
		nonce64: nonce64,
		counter: offset,
	}
}

func newV0CipherStream(key *[32]byte, offset uint64) *rekeyStream {
	var nonce [24]byte
	binary.LittleEndian.PutUint64(nonce[16:], offset/maxBytesPerNonce)
	return newCipherStream(key, nonce, offset)
}

func newV1CipherStream(dataKey, slabKey *[32]byte, offset uint64) *rekeyStream {
	var nonce [24]byte
	copy(nonce[:], slabKey[:])
	return newCipherStream(dataKey, nonce, offset)
}

// encrypt returns a cipher.StreamReader that encrypts r with k starting at the
// given offset.
func encrypt(key *[32]byte, r io.Reader, offset uint64) cipher.StreamReader {
	return cipher.StreamReader{S: newV0CipherStream(key, offset), R: r}
}

// slabKeySource coordinates the random encryption keys used to encrypt both a
// slab's object data and its individual shards.
type slabKeySource struct {
	mu   sync.Mutex
	keys [][32]byte
}

// key returns the encryption key for the slab at slabIndex, generating any
// missing keys on demand. It is safe for concurrent use.
func (s *slabKeySource) key(slabIndex int) [32]byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.keys) <= slabIndex {
		s.keys = append(s.keys, frand.Entropy256())
	}
	return s.keys[slabIndex]
}

// slabEncryptReader encrypts object data using each slab's key as the nonce on
// the data key, so a slab can be overwritten without re-encrypting the whole
// object or reusing a key.
type slabEncryptReader struct {
	r         io.Reader
	dataKey   *[32]byte
	slabKeys  *slabKeySource
	slabSize  uint64
	offset    uint64
	slabIndex int
	stream    cipher.Stream
}

// encryptV1 returns an io.Reader that encrypts r with dataKey, using the key
// of each slab the data crosses as the nonce.
func encryptV1(dataKey *[32]byte, r io.Reader, slabKeys *slabKeySource, slabSize, offset uint64) io.Reader {
	return &slabEncryptReader{
		r:         r,
		dataKey:   dataKey,
		slabKeys:  slabKeys,
		slabSize:  slabSize,
		offset:    offset,
		slabIndex: -1,
	}
}

func (r *slabEncryptReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	slabIndex := int(r.offset / r.slabSize)
	slabOffset := r.offset % r.slabSize
	if r.stream == nil || r.slabIndex != slabIndex {
		slabKey := r.slabKeys.key(slabIndex)
		r.stream = newV1CipherStream(r.dataKey, &slabKey, slabOffset)
		r.slabIndex = slabIndex
	}

	p = p[:min(uint64(len(p)), r.slabSize-slabOffset)]
	n, err := r.r.Read(p)
	r.stream.XORKeyStream(p[:n], p[:n])
	r.offset += uint64(n)
	return n, err
}
