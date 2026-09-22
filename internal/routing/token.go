package routing

import (
	"crypto/sha256"
	"encoding/binary"
)

// Token is a position on the 64-bit consistent-hash ring. Both a key and a
// virtual node hash to a Token; ownership is decided by comparing them
// (docs/ROUTING.md §2, §3).
type Token uint64

// TokenOf maps opaque key bytes to a ring token:
//
//	TokenOf(b) = big-endian uint64 of the first 8 bytes of sha256(b)
//
// The key is hashed exactly as given — no normalisation, case folding, trimming,
// or UTF-8 interpretation (INV-A5). The empty slice, NUL bytes and invalid UTF-8
// are all valid inputs; TokenOf is total over []byte and never allocates on the
// heap (sha256.Sum256 returns an array by value).
//
// Big-endian matches the internal-key sequence encoding (docs/DESIGN.md §1), and
// "first 8 bytes" is the simplest fully-specified window; SHA-256's uniformity
// makes any fixed 8-byte window uniform. token("") is the standard SHA-256 empty
// vector 0xe3b0c44298fc1c14, which doubles as a sanity check that this is
// ordinary SHA-256.
func TokenOf(b []byte) Token {
	sum := sha256.Sum256(b)
	return Token(binary.BigEndian.Uint64(sum[:8]))
}

// Label namespaces for ring-position tokens. Each is an ASCII tag followed by a
// NUL, so a shard virtual node can never collide with a node virtual node by
// construction of the label (docs/ROUTING.md §2).
var (
	labelShard  = []byte("DKVSHARD\x00")
	labelNode   = []byte("DKVNODE\x00")
	labelAnchor = []byte("DKVANCHOR\x00")
)

// shardVNodeToken is the ring position of shard s's i-th virtual node:
//
//	token("DKVSHARD\x00" || be32(s) || be32(i))
//
// The 4-byte big-endian i is fixed-width and last, so the label is injective in
// (s, i).
func shardVNodeToken(s ShardID, i uint32) Token {
	var buf [len("DKVSHARD\x00") + 8]byte
	n := copy(buf[:], labelShard)
	binary.BigEndian.PutUint32(buf[n:], uint32(s))
	binary.BigEndian.PutUint32(buf[n+4:], i)
	return TokenOf(buf[:])
}

// nodeVNodeToken is the ring position of node id's i-th virtual node:
//
//	token("DKVNODE\x00" || id || be32(i))
//
// id is the raw identifier bytes; the fixed-width be32(i) suffix keeps the label
// injective in (id, i) even though id is variable-length.
func nodeVNodeToken(id NodeID, i uint32) Token {
	buf := make([]byte, 0, len(labelNode)+len(id)+4)
	buf = append(buf, labelNode...)
	buf = append(buf, id...)
	var ib [4]byte
	binary.BigEndian.PutUint32(ib[:], i)
	buf = append(buf, ib[:]...)
	return TokenOf(buf)
}

// anchorToken is the single ring position from which shard s's replica group is
// walked out on the node ring (docs/ROUTING.md §5):
//
//	token("DKVANCHOR\x00" || be32(s))
func anchorToken(s ShardID) Token {
	var buf [len("DKVANCHOR\x00") + 4]byte
	n := copy(buf[:], labelAnchor)
	binary.BigEndian.PutUint32(buf[n:], uint32(s))
	return TokenOf(buf[:])
}
