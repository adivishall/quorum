package bloom_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/adivishall/quorum/internal/storage/bloom"
)

// build returns a decoded filter over keys.
func build(t *testing.T, bitsPerKey int, keys [][]byte) bloom.Filter {
	t.Helper()
	b := bloom.NewBuilder(bitsPerKey)
	for _, k := range keys {
		b.Add(k)
	}
	f, err := bloom.Decode(b.Finish())
	if err != nil {
		t.Fatalf("Decode of a freshly built filter failed: %v", err)
	}
	return f
}

func keyN(i int) []byte { return []byte(fmt.Sprintf("key%08d", i)) }

// ---------------------------------------------------------------- the invariant

// TestZeroFalseNegativesOverALargeCorpus is INV-S7. Every key the filter was
// built from must test positive. A single failure here is a key that is on disk
// and unreachable, which is data loss.
func TestZeroFalseNegativesOverALargeCorpus(t *testing.T) {
	for _, n := range []int{1, 2, 10, 1000, 50000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			keys := make([][]byte, 0, n)
			for i := 0; i < n; i++ {
				keys = append(keys, keyN(i))
			}
			f := build(t, bloom.DefaultBitsPerKey, keys)

			for i, k := range keys {
				if !f.MayContain(k) {
					t.Fatalf("false negative: key %d (%q) was added but MayContain returned false; "+
						"a false negative hides a key that is durably on disk", i, k)
				}
			}
		})
	}
}

// TestZeroFalseNegativesForRandomByteKeys repeats the invariant over keys that
// are arbitrary bytes rather than printable text, including the byte values most
// likely to expose an indexing mistake.
func TestZeroFalseNegativesForRandomByteKeys(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	keys := make([][]byte, 0, 20000)
	for i := 0; i < 20000; i++ {
		k := make([]byte, 1+rng.Intn(64))
		for j := range k {
			k[j] = byte(rng.Intn(256))
		}
		keys = append(keys, k)
	}
	// Plus some deliberately awkward ones.
	keys = append(keys,
		[]byte{0x00}, []byte{0xff}, []byte{0x00, 0x00}, []byte{0xff, 0xff},
		[]byte("a\nb"), []byte("a\x00b"), []byte("ключ"), []byte{0xc3, 0x28},
		bytes.Repeat([]byte{0x00}, 4096), bytes.Repeat([]byte{0xff}, 4096),
	)

	f := build(t, bloom.DefaultBitsPerKey, keys)
	for i, k := range keys {
		if !f.MayContain(k) {
			t.Fatalf("false negative for key %d (%x)", i, k)
		}
	}
}

// TestFalsePositiveRateIsNearTheoretical is the other half of the pair. A filter
// that returned true for everything would pass every false-negative test ever
// written, so the false-positive rate has to be measured too.
func TestFalsePositiveRateIsNearTheoretical(t *testing.T) {
	const (
		n      = 10000
		probes = 100000
	)
	keys := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		keys = append(keys, keyN(i))
	}
	f := build(t, bloom.DefaultBitsPerKey, keys)

	// Absent keys, disjoint from the corpus by construction.
	var positives int
	for i := 0; i < probes; i++ {
		if f.MayContain([]byte(fmt.Sprintf("absent%08d", i))) {
			positives++
		}
	}
	rate := float64(positives) / float64(probes)

	// Theoretical: (1 - e^(-k*n/m))^k with m = n*bitsPerKey, k = 7.
	k := float64(bloom.Probes(bloom.DefaultBitsPerKey))
	m := float64(bloom.Bits(n, bloom.DefaultBitsPerKey))
	theoretical := math.Pow(1-math.Exp(-k*float64(n)/m), k)

	t.Logf("measured false-positive rate %.4f%% over %d probes (theoretical %.4f%%, k=%d, m=%d bits)",
		rate*100, probes, theoretical*100, bloom.Probes(bloom.DefaultBitsPerKey),
		bloom.Bits(n, bloom.DefaultBitsPerKey))

	if rate > 0.03 {
		t.Fatalf("false-positive rate %.4f%% is far above the ~%.2f%% theoretical rate; "+
			"the filter is barely filtering (a filter that always says 'maybe' still has "+
			"zero false negatives, which is why this test exists)", rate*100, theoretical*100)
	}
}

// ---------------------------------------------------------------- shape

// TestEmptyFilterSaysNoToEverything: a file with no keys legitimately answers
// "definitely absent" for every key.
func TestEmptyFilterSaysNoToEverything(t *testing.T) {
	f := build(t, bloom.DefaultBitsPerKey, nil)
	if !f.Present() {
		t.Fatal("a filter built over zero keys must still be Present; it is a real filter over an empty set")
	}
	for i := 0; i < 1000; i++ {
		if f.MayContain(keyN(i)) {
			t.Fatalf("an empty filter reported key %d as maybe-present", i)
		}
	}
}

// TestZeroValueFilterFailsSafe: the zero Filter means "no filter here" (a Phase
// 3 SSTable). It must say "maybe" to everything, so that code which does not
// check Present degrades to consulting the file rather than skipping it.
func TestZeroValueFilterFailsSafe(t *testing.T) {
	var f bloom.Filter
	if f.Present() {
		t.Fatal("the zero Filter must not report Present")
	}
	for i := 0; i < 100; i++ {
		if !f.MayContain(keyN(i)) {
			t.Fatalf("the zero Filter answered 'definitely absent' for key %d; "+
				"an absent filter must never eliminate a file", i)
		}
	}
}

func TestDuplicateUserKeysCountOnce(t *testing.T) {
	b := bloom.NewBuilder(bloom.DefaultBitsPerKey)
	// This is what the SSTable writer feeds for a@seq3, a@seq2, a@seq1, b@seq1:
	// the same user key repeatedly, then the next one.
	b.Add([]byte("a"))
	b.Add([]byte("a"))
	b.Add([]byte("a"))
	b.Add([]byte("b"))
	if got := b.Keys(); got != 2 {
		t.Fatalf("Keys() = %d, want 2: three versions of one user key are one filter entry", got)
	}

	f, err := bloom.Decode(b.Finish())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b"} {
		if !f.MayContain([]byte(k)) {
			t.Fatalf("key %q is absent from the filter", k)
		}
	}
}

// TestTombstonedKeysAreInTheFilter: a tombstone is an entry in the file, and a
// lookup for a deleted key must still reach the file that holds the tombstone.
// If the filter omitted tombstoned keys, the lookup would skip that file and
// fall through to an older one holding the value — resurrecting a deleted key.
func TestTombstonedKeysAreInTheFilter(t *testing.T) {
	b := bloom.NewBuilder(bloom.DefaultBitsPerKey)
	// The writer adds the user key regardless of kind; both of these are
	// tombstones as far as the SSTable is concerned.
	for _, k := range []string{"deleted1", "deleted2", "live"} {
		b.Add([]byte(k))
	}
	f, err := bloom.Decode(b.Finish())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"deleted1", "deleted2", "live"} {
		if !f.MayContain([]byte(k)) {
			t.Fatalf("tombstoned key %q is not in the filter; the file holding its "+
				"tombstone would be skipped and an older value would resurrect", k)
		}
	}
}

func TestSizingMatchesTheDesign(t *testing.T) {
	// docs/DESIGN.md §5: bitsPerKey = 10 gives k = round(10*ln2) = 7.
	if got := bloom.Probes(10); got != 7 {
		t.Fatalf("Probes(10) = %d, want 7 (docs/DESIGN.md §5)", got)
	}
	if got := bloom.Probes(0); got != 1 {
		t.Fatalf("Probes(0) = %d, want the floor of 1", got)
	}
	if got := bloom.Probes(1000); got > bloom.MaxProbes {
		t.Fatalf("Probes(1000) = %d, want clamping to %d", got, bloom.MaxProbes)
	}
	// m = ceil(n * bitsPerKey), floored at MinBits.
	if got := bloom.Bits(1000, 10); got != 10000 {
		t.Fatalf("Bits(1000,10) = %d, want 10000", got)
	}
	if got := bloom.Bits(1, 10); got != bloom.MinBits {
		t.Fatalf("Bits(1,10) = %d, want the %d-bit floor", got, bloom.MinBits)
	}
	if got := bloom.Bits(0, 10); got != bloom.MinBits {
		t.Fatalf("Bits(0,10) = %d, want the %d-bit floor", got, bloom.MinBits)
	}
}

// TestFilterBytesAreDeterministic matters because the filter is persisted. Two
// builds over the same keys must produce identical bytes, or an SSTable's
// contents would depend on something other than its data.
func TestFilterBytesAreDeterministic(t *testing.T) {
	keys := make([][]byte, 0, 500)
	for i := 0; i < 500; i++ {
		keys = append(keys, keyN(i))
	}
	var first []byte
	for run := 0; run < 3; run++ {
		b := bloom.NewBuilder(bloom.DefaultBitsPerKey)
		for _, k := range keys {
			b.Add(k)
		}
		got := b.Finish()
		if run == 0 {
			first = got
			continue
		}
		if !bytes.Equal(first, got) {
			t.Fatalf("run %d produced different filter bytes; the encoding must be deterministic "+
				"because it is written into an SSTable", run)
		}
	}
}

func TestBuilderResetClearsState(t *testing.T) {
	b := bloom.NewBuilder(bloom.DefaultBitsPerKey)
	b.Add([]byte("a"))
	b.Add([]byte("b"))
	b.Reset()
	if got := b.Keys(); got != 0 {
		t.Fatalf("Keys() = %d after Reset, want 0", got)
	}
	// And the dedup state is cleared too, so "a" is counted again.
	b.Add([]byte("a"))
	if got := b.Keys(); got != 1 {
		t.Fatalf("Keys() = %d, want 1", got)
	}
}

// ---------------------------------------------------------------- encoding

func TestEncodeDecodeRoundTripAgrees(t *testing.T) {
	keys := make([][]byte, 0, 2000)
	for i := 0; i < 2000; i++ {
		keys = append(keys, keyN(i))
	}
	b := bloom.NewBuilder(bloom.DefaultBitsPerKey)
	for _, k := range keys {
		b.Add(k)
	}
	raw := b.Finish()

	f, err := bloom.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	// Decoding a copy of the same bytes must answer identically for present and
	// absent keys alike.
	g, err := bloom.Decode(append([]byte(nil), raw...))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		k := keyN(i) // first 2000 present, rest absent
		if f.MayContain(k) != g.MayContain(k) {
			t.Fatalf("two decodes of identical bytes disagree on key %d", i)
		}
	}
	if f.NumBits() != g.NumBits() || f.NumProbes() != g.NumProbes() {
		t.Fatalf("header mismatch: (%d,%d) vs (%d,%d)",
			f.NumBits(), f.NumProbes(), g.NumBits(), g.NumProbes())
	}
}

func TestDecodeRejectsMalformedFilters(t *testing.T) {
	// A known-good filter to mutate.
	b := bloom.NewBuilder(bloom.DefaultBitsPerKey)
	for i := 0; i < 100; i++ {
		b.Add(keyN(i))
	}
	good := b.Finish()

	withHeader := func(k byte, m uint32, bodyLen int) []byte {
		out := make([]byte, bloom.HeaderSize+bodyLen)
		out[0] = k
		binary.LittleEndian.PutUint32(out[1:], m)
		return out
	}

	cases := []struct {
		name string
		buf  []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"shorter than the header", good[:bloom.HeaderSize-1]},
		{"probe count zero", withHeader(0, 64, 8)},
		{"probe count above the maximum", withHeader(bloom.MaxProbes+1, 64, 8)},
		{"bit count zero", withHeader(7, 0, 0)},
		{"bit count above the maximum", withHeader(7, bloom.MaxBits+1, 8)},
		{"bit array one byte short", good[:len(good)-1]},
		{"bit array one byte long", append(append([]byte(nil), good...), 0)},
		{"truncated mid-body", good[:bloom.HeaderSize+1]},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bloom.Decode(tc.buf)
			if err == nil {
				t.Fatal("Decode accepted a malformed filter; a malformed filter must be an " +
					"error, never an empty filter that answers 'absent' for every key")
			}
			if !errors.Is(err, bloom.ErrMalformed) {
				t.Fatalf("Decode = %v, want ErrMalformed", err)
			}
		})
	}
}

// TestDecodeNeverPanics feeds Decode arbitrary bytes. Its input comes off disk,
// so every shape has to be a clean error rather than a panic.
func TestDecodeNeverPanics(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 5000; i++ {
		buf := make([]byte, rng.Intn(40))
		for j := range buf {
			buf[j] = byte(rng.Intn(256))
		}
		f, err := bloom.Decode(buf)
		if err == nil {
			// If it happened to decode, probing must also not panic.
			f.MayContain([]byte("anything"))
		}
	}
}

// TestBitFlipNeverCausesAFalseNegativeOnAFullFilter documents an easily
// misunderstood property. Flipping a bit from 1 to 0 in a filter CAN cause a
// false negative — the filter carries no checksum of its own. That is why the
// SSTable stores the filter in a block with a crc32c and verifies it at Open:
// the filter's integrity is the block checksum's job, not the filter's.
//
// This test asserts the direction that IS guaranteed: setting extra bits (0->1)
// only ever adds false positives.
func TestExtraBitsOnlyAddFalsePositives(t *testing.T) {
	keys := make([][]byte, 0, 500)
	for i := 0; i < 500; i++ {
		keys = append(keys, keyN(i))
	}
	b := bloom.NewBuilder(bloom.DefaultBitsPerKey)
	for _, k := range keys {
		b.Add(k)
	}
	raw := b.Finish()

	// Set every bit in the body: the filter now says "maybe" to everything.
	saturated := append([]byte(nil), raw...)
	for i := bloom.HeaderSize; i < len(saturated); i++ {
		saturated[i] = 0xff
	}
	f, err := bloom.Decode(saturated)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !f.MayContain(k) {
			t.Fatal("a saturated filter produced a false negative, which is impossible")
		}
	}
	if !f.MayContain([]byte("definitely-not-present")) {
		t.Fatal("a saturated filter should say maybe to everything")
	}
}

// ---------------------------------------------------------------- fuzz

// FuzzFilterHasNoFalseNegatives is the property-based form of INV-S7: whatever
// keys go in, every one of them tests positive after a serialize/parse round
// trip.
func FuzzFilterHasNoFalseNegatives(f *testing.F) {
	f.Add([]byte("a"), []byte("b"), []byte("c"))
	f.Add([]byte(""), []byte("\x00"), []byte("\xff\xff"))
	f.Add([]byte("same"), []byte("same"), []byte("same"))

	f.Fuzz(func(t *testing.T, k1, k2, k3 []byte) {
		keys := [][]byte{k1, k2, k3}

		b := bloom.NewBuilder(bloom.DefaultBitsPerKey)
		for _, k := range keys {
			b.Add(k)
		}
		flt, err := bloom.Decode(b.Finish())
		if err != nil {
			t.Fatalf("a filter this package built failed to decode: %v", err)
		}
		for i, k := range keys {
			if !flt.MayContain(k) {
				t.Fatalf("false negative for input %d (%x)", i, k)
			}
		}
	})
}

// FuzzDecodeIsTotal requires Decode to either fail cleanly or produce a filter
// that can be probed, for any input bytes.
func FuzzDecodeIsTotal(f *testing.F) {
	f.Add([]byte{7, 64, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, buf []byte) {
		flt, err := bloom.Decode(buf)
		if err != nil {
			if !errors.Is(err, bloom.ErrMalformed) {
				t.Fatalf("Decode returned %v, which is not ErrMalformed", err)
			}
			return
		}
		if !flt.Present() {
			t.Fatal("Decode returned a non-Present filter with no error")
		}
		// Probing must be safe for any key.
		flt.MayContain(nil)
		flt.MayContain([]byte("x"))
		flt.MayContain(bytes.Repeat([]byte("k"), 300))
	})
}
