// Package bloom implements Quorum's per-SSTable Bloom filter
// (docs/DESIGN.md §5, docs/BLOOM.md).
//
// A Bloom filter answers one question about one SSTable: "might this file
// contain any version of this user key?" A "no" is authoritative and lets a
// lookup skip the file without reading a single data block. A "yes" means
// "consult the file", which is what Phase 3 already did unconditionally.
//
// # The only property that matters
//
// The filter may never return false for a key it was built from. A false
// positive costs a wasted block read; a false NEGATIVE loses a key that is
// durably on disk, which is indistinguishable from data loss. Everything in
// this package is arranged so that the "no" path is the one that is hard to
// reach by accident: an absent filter is not a filter that says no, a
// malformed filter is an error rather than a no, and a filter over zero keys
// is the only case where every answer is legitimately no.
//
// # What is hashed
//
// USER keys, not internal keys (docs/DESIGN.md §5). A lookup knows the user
// key and nothing about which sequence numbers the file happens to hold, so a
// filter over internal keys could not be probed at all — every version would
// be a different filter entry and the read path has no way to enumerate them.
// Three versions of one user key therefore contribute one entry.
//
// # Sizing (docs/DESIGN.md §5)
//
//	m = max(MinBits, ceil(n * bitsPerKey))   bits, n = distinct user keys
//	k = clamp(round(bitsPerKey * ln2), 1, MaxProbes)   probes
//
// At the default bitsPerKey = 10 that is k = 7 and a theoretical false-positive
// rate near 1%. MinBits exists so that a one-key file does not get a
// ten-bit filter whose false-positive rate is dominated by rounding; it costs
// eight bytes.
//
// # Hashing
//
// One 64-bit hash per key, split into two 32-bit halves and combined by the
// Kirsch–Mitzenmacher construction: g_i = h1 + i*h2, taken modulo m. One real
// hash and k cheap derivations, instead of k independent hashes.
//
// The hash is FNV-1a 64 followed by the MurmurHash3 64-bit finalizer. FNV-1a
// alone is a poor choice here: its high bits barely mix, and this construction
// reads the high half as h2, so h2 would be nearly constant across keys and the
// k probes would collapse toward one bit. The finalizer is what makes the two
// halves independent. It is three shifts and two multiplies.
//
// It is deliberately not a cryptographic hash. A Bloom filter is a performance
// structure; an adversary who can choose keys can inflate the false-positive
// rate, which costs block reads and cannot cause a wrong answer. It is also
// deliberately not hash/maphash: that is seeded randomly per process, and this
// hash is persisted inside an SSTable and must produce identical bits in every
// process that ever reads the file.
//
// # Versioning
//
// The encoding is the one docs/DESIGN.md §5 specifies — `k u8 | m u32 | bits`
// — with no separate version field. The filter's identity is versioned by the
// SSTable that carries it: the footer magic ("DKVSST01") names the whole file
// format, this hash included. Changing the hash or the probe construction is a
// format change and must bump that magic, because an old file's bits would
// otherwise be probed with a new hash and the filter would return false for
// keys that are present. See ADR-009.
package bloom

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	// DefaultBitsPerKey is the docs/DESIGN.md §5 default, giving k = 7 and a
	// theoretical false-positive rate near 1%.
	DefaultBitsPerKey = 10

	// HeaderSize is the fixed prefix: k u8 | m u32.
	HeaderSize = 5

	// MaxProbes bounds k. A filter claiming more probes than this is rejected
	// rather than honoured: k comes off disk, and each probe is work.
	MaxProbes = 30

	// MinBits is the floor on m, so a tiny file still gets a filter whose
	// false-positive rate is set by the math rather than by rounding.
	MinBits = 64

	// MaxBits bounds m before it is used to size an allocation, for the same
	// reason record.MaxRecordSize exists: m is read from disk and a corrupted
	// value must be rejected before it is acted on. 2^31 bits is 256 MiB of
	// filter, which at the default 10 bits per key is roughly 214 million keys
	// in a single SSTable — far above anything this engine produces.
	MaxBits = 1 << 31
)

// ErrMalformed means a serialized filter could not be decoded: a short buffer,
// an impossible probe count, or a bit array whose length disagrees with the
// bit count in the header.
//
// It is a distinct error and never a "no". A caller that treated a malformed
// filter as "key absent" would turn corruption into a missing key, which is the
// failure this whole engine is arranged to prevent (INV-L7).
var ErrMalformed = errors.New("bloom: malformed filter")

// Probes returns the number of probes k for a given bitsPerKey, clamped to
// [1, MaxProbes]. It is exported so that a test can assert the docs/DESIGN.md
// §5 arithmetic rather than restate it.
func Probes(bitsPerKey int) int {
	k := int(math.Round(float64(bitsPerKey) * math.Ln2))
	if k < 1 {
		return 1
	}
	if k > MaxProbes {
		return MaxProbes
	}
	return k
}

// Bits returns the bit count m for n distinct keys at bitsPerKey bits each.
func Bits(n, bitsPerKey int) uint32 {
	if n <= 0 {
		return MinBits
	}
	m := uint64(n) * uint64(bitsPerKey)
	if m < MinBits {
		m = MinBits
	}
	if m > MaxBits {
		m = MaxBits
	}
	return uint32(m)
}

// hash is the 64-bit key hash: FNV-1a 64 with a MurmurHash3 finalizer.
//
// Both halves of the result are used as independent 32-bit hashes, which is
// what the finalizer is for; see the package comment.
func hash(key []byte) uint64 {
	const (
		offset64 = uint64(14695981039346656037)
		prime64  = uint64(1099511628211)
	)
	h := offset64
	for _, c := range key {
		h ^= uint64(c)
		h *= prime64
	}
	// MurmurHash3 fmix64.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// Builder accumulates the distinct user keys of one SSTable and serializes a
// filter over them.
//
// Add must be called with user keys in the order they are written to the file,
// which is non-decreasing (the SSTable writer enforces strictly increasing
// internal keys, and equal user keys are therefore adjacent). The builder
// deduplicates by comparing against the previous key only, which is exact for
// that ordering and costs no memory. Out-of-order input would merely oversize
// the filter — never undersize it — so a caller mistake cannot produce a false
// negative.
type Builder struct {
	bitsPerKey int
	hashes     []uint64
	last       []byte
	hasLast    bool
}

// NewBuilder returns a Builder. A bitsPerKey of zero or less selects
// DefaultBitsPerKey.
func NewBuilder(bitsPerKey int) *Builder {
	if bitsPerKey <= 0 {
		bitsPerKey = DefaultBitsPerKey
	}
	return &Builder{bitsPerKey: bitsPerKey}
}

// Add records a user key. Repeating the key most recently added is a no-op.
func (b *Builder) Add(userKey []byte) {
	if b.hasLast && bytesEqual(b.last, userKey) {
		return
	}
	b.last = append(b.last[:0], userKey...)
	b.hasLast = true
	b.hashes = append(b.hashes, hash(userKey))
}

// Keys returns the number of distinct user keys added.
func (b *Builder) Keys() int { return len(b.hashes) }

// Finish serializes the filter: k u8 | m u32 | bits[ceil(m/8)].
//
// A builder with no keys still produces a well-formed filter, over zero keys.
// Every probe of it returns false, which is correct rather than degenerate: the
// file it describes holds no keys, so "definitely absent" is the true answer
// for every key.
func (b *Builder) Finish() []byte {
	m := Bits(len(b.hashes), b.bitsPerKey)
	k := Probes(b.bitsPerKey)

	out := make([]byte, HeaderSize+bytesForBits(m))
	out[0] = byte(k)
	binary.LittleEndian.PutUint32(out[1:], m)
	bits := out[HeaderSize:]

	for _, h := range b.hashes {
		setBits(bits, h, m, k)
	}
	return out
}

// Reset clears the builder for reuse.
func (b *Builder) Reset() {
	b.hashes = b.hashes[:0]
	b.last = b.last[:0]
	b.hasLast = false
}

// Filter is a decoded, immutable filter. The zero value is not usable; it
// reports Present() == false, meaning "there is no filter here".
type Filter struct {
	k       int
	m       uint32
	bits    []byte
	present bool
}

// Decode parses a serialized filter.
//
// Every field is validated against the buffer before use. A filter that does
// not decode is an error, never an empty filter: an empty filter answers "no"
// to everything, and silently substituting one for damaged bytes would hide
// every key in the file.
func Decode(buf []byte) (Filter, error) {
	if len(buf) < HeaderSize {
		return Filter{}, fmt.Errorf("%w: %d bytes is shorter than the %d-byte header",
			ErrMalformed, len(buf), HeaderSize)
	}
	k := int(buf[0])
	m := binary.LittleEndian.Uint32(buf[1:])

	if k < 1 || k > MaxProbes {
		return Filter{}, fmt.Errorf("%w: probe count %d is outside [1,%d]",
			ErrMalformed, k, MaxProbes)
	}
	if m == 0 {
		return Filter{}, fmt.Errorf("%w: bit count is zero", ErrMalformed)
	}
	if m > MaxBits {
		return Filter{}, fmt.Errorf("%w: bit count %d exceeds the %d-bit maximum",
			ErrMalformed, m, MaxBits)
	}
	bits := buf[HeaderSize:]
	if want := bytesForBits(m); len(bits) != want {
		return Filter{}, fmt.Errorf("%w: header declares %d bits (%d bytes) but %d bytes follow",
			ErrMalformed, m, want, len(bits))
	}
	return Filter{k: k, m: m, bits: bits, present: true}, nil
}

// Present reports whether this is a real filter. The zero Filter is not, which
// is how "this SSTable carries no filter" is represented — a Phase 3 file, whose
// filter block has length zero.
func (f Filter) Present() bool { return f.present }

// MayContain reports whether userKey might be in the file this filter
// describes.
//
// A false return means the key is definitely absent and the file may be
// skipped. A true return means nothing on its own; the file must be consulted.
// A filter that is not Present returns true for every key, so that code which
// forgets to check Present degrades to Phase 3 behaviour — consult the file —
// rather than to skipping it.
func (f Filter) MayContain(userKey []byte) bool {
	if !f.present {
		return true
	}
	h := hash(userKey)
	h1 := uint32(h)
	h2 := uint32(h >> 32)
	for i := 0; i < f.k; i++ {
		pos := (h1 + uint32(i)*h2) % f.m
		if f.bits[pos>>3]&(1<<(pos&7)) == 0 {
			return false
		}
	}
	return true
}

// NumBits returns m.
func (f Filter) NumBits() uint32 { return f.m }

// NumProbes returns k.
func (f Filter) NumProbes() int { return f.k }

// setBits marks the k probe positions for one key hash.
//
// It mirrors MayContain exactly. The two must stay in step: any divergence
// between how a bit is set and how it is tested is a false negative, which is
// why the position arithmetic appears once in each and nowhere else.
func setBits(bits []byte, h uint64, m uint32, k int) {
	h1 := uint32(h)
	h2 := uint32(h >> 32)
	for i := 0; i < k; i++ {
		pos := (h1 + uint32(i)*h2) % m
		bits[pos>>3] |= 1 << (pos & 7)
	}
}

func bytesForBits(m uint32) int { return int((uint64(m) + 7) / 8) }

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
