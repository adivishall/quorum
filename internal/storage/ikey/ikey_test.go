package ikey_test

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/adivishall/quorum/internal/storage/ikey"
)

func enc(userKey string, seq uint64, kind ikey.Kind) []byte {
	return ikey.Encode(nil, []byte(userKey), seq, kind)
}

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		key  string
		seq  uint64
		kind ikey.Kind
	}{
		{"a", 0, ikey.KindValue},
		{"a", 1, ikey.KindTombstone},
		{"user:123", 42, ikey.KindValue},
		{"\x00\xff binary \n", 1 << 30, ikey.KindTombstone},
		{"z", ikey.MaxSeq, ikey.KindValue},
	}
	for _, tc := range cases {
		ik := enc(tc.key, tc.seq, tc.kind)
		if got := string(ikey.UserKey(ik)); got != tc.key {
			t.Errorf("UserKey(%s) = %q, want %q", ikey.String(ik), got, tc.key)
		}
		if got := ikey.Seq(ik); got != tc.seq {
			t.Errorf("Seq(%s) = %d, want %d", ikey.String(ik), got, tc.seq)
		}
		if got := ikey.KindOf(ik); got != tc.kind {
			t.Errorf("KindOf(%s) = %v, want %v", ikey.String(ik), got, tc.kind)
		}
		if len(ik) != len(tc.key)+ikey.TrailerSize {
			t.Errorf("encoded length = %d, want %d", len(ik), len(tc.key)+ikey.TrailerSize)
		}
		if !ikey.Valid(ik) {
			t.Errorf("Valid(%s) = false", ikey.String(ik))
		}
	}
}

func TestEncodePanicsOnOversizedSequence(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Encode accepted a sequence number larger than 2^56-1; " +
				"truncating it would silently corrupt the ordering of every later key")
		}
	}()
	ikey.Encode(nil, []byte("k"), ikey.MaxSeq+1, ikey.KindValue)
}

// TestOrdering is the specification from docs/DESIGN.md §1 expressed as a
// sorted list. If any of these relationships inverts, the "first match wins"
// read path silently returns stale data.
func TestOrdering(t *testing.T) {
	want := [][]byte{
		enc("a", 9, ikey.KindValue),
		enc("a", 2, ikey.KindTombstone),
		enc("a", 1, ikey.KindValue),
		enc("ab", 100, ikey.KindValue), // "a" < "ab": user key wins over sequence
		enc("b", 5, ikey.KindValue),
	}

	got := make([][]byte, len(want))
	copy(got, want)
	rng := rand.New(rand.NewSource(7))
	rng.Shuffle(len(got), func(i, j int) { got[i], got[j] = got[j], got[i] })
	sort.Slice(got, func(i, j int) bool { return ikey.Compare(got[i], got[j]) < 0 })

	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("position %d: got %s, want %s", i, ikey.String(got[i]), ikey.String(want[i]))
		}
	}
}

// TestSameKeyNewestFirst pins rule 2 on its own, because it is the rule that
// is counter-intuitive: within one user key, a HIGHER sequence number sorts
// EARLIER.
func TestSameKeyNewestFirst(t *testing.T) {
	older := enc("k", 1, ikey.KindValue)
	newer := enc("k", 2, ikey.KindValue)
	if ikey.Compare(newer, older) >= 0 {
		t.Fatalf("Compare(seq=2, seq=1) = %d, want < 0 (newer sorts first)",
			ikey.Compare(newer, older))
	}
	// And the bytewise comparison goes the other way, which is exactly why
	// Compare cannot be bytes.Compare over the whole key.
	if bytes.Compare(newer, older) <= 0 {
		t.Fatal("test premise is wrong: the big-endian trailer should sort ascending bytewise")
	}
}

// TestKindDescendingForEqualSequence covers rule 3. It cannot occur in data
// this engine writes — a sequence number is consumed once — but Compare must
// still be a total order over keys that arrive from a damaged file.
func TestKindDescendingForEqualSequence(t *testing.T) {
	value := enc("k", 5, ikey.KindValue)         // 0x01
	tombstone := enc("k", 5, ikey.KindTombstone) // 0x00
	if ikey.Compare(value, tombstone) >= 0 {
		t.Fatalf("Compare(value, tombstone) at equal seq = %d, want < 0 (kind descending)",
			ikey.Compare(value, tombstone))
	}
}

func TestCompareIsATotalOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	keys := make([][]byte, 0, 400)
	for i := 0; i < 400; i++ {
		uk := make([]byte, 1+rng.Intn(6))
		for j := range uk {
			uk[j] = byte('a' + rng.Intn(4))
		}
		kind := ikey.KindValue
		if rng.Intn(2) == 0 {
			kind = ikey.KindTombstone
		}
		keys = append(keys, ikey.Encode(nil, uk, uint64(rng.Intn(50)), kind))
	}

	for _, a := range keys {
		if ikey.Compare(a, a) != 0 {
			t.Fatalf("Compare is not reflexive at %s", ikey.String(a))
		}
		for _, b := range keys {
			ab, ba := ikey.Compare(a, b), ikey.Compare(b, a)
			if ab != -ba {
				t.Fatalf("Compare is not antisymmetric: %s vs %s gave %d and %d",
					ikey.String(a), ikey.String(b), ab, ba)
			}
		}
	}

	// Transitivity over a sorted slice: sorting must be stable under the
	// relation, which sort.SliceIsSorted checks against every adjacent pair
	// after a sort that itself assumed transitivity.
	sort.Slice(keys, func(i, j int) bool { return ikey.Compare(keys[i], keys[j]) < 0 })
	for i := 1; i < len(keys); i++ {
		if ikey.Compare(keys[i-1], keys[i]) > 0 {
			t.Fatalf("sorted slice is out of order at %d: %s then %s",
				i, ikey.String(keys[i-1]), ikey.String(keys[i]))
		}
	}
}

// TestSeekSortsBeforeEveryVersion is the property the read path depends on:
// seeking with a sequence bound lands at or before the newest visible version
// of the key and never inside a different user key.
func TestSeekSortsBeforeEveryVersion(t *testing.T) {
	seek := ikey.Seek([]byte("k"), ikey.MaxSeq)

	for _, seq := range []uint64{0, 1, 1000, ikey.MaxSeq} {
		for _, kind := range []ikey.Kind{ikey.KindValue, ikey.KindTombstone} {
			real := ikey.Encode(nil, []byte("k"), seq, kind)
			if ikey.Compare(seek, real) > 0 {
				t.Fatalf("seek key sorts after %s; a lookup would skip it", ikey.String(real))
			}
		}
	}
	// It must still sort after every version of the preceding user key and
	// before every version of the following one.
	if ikey.Compare(seek, enc("j", 0, ikey.KindValue)) <= 0 {
		t.Fatal("seek key for \"k\" sorts at or before a version of \"j\"")
	}
	if ikey.Compare(seek, enc("l", ikey.MaxSeq, ikey.KindValue)) >= 0 {
		t.Fatal("seek key for \"k\" sorts at or after a version of \"l\"")
	}
}

// TestSeekWithSequenceBoundSkipsNewerVersions is not used by the Phase 3 read
// path, which always reads at the newest sequence, but the encoding has to
// support it or the Phase 12 snapshot reads would need a different key format.
func TestSeekWithSequenceBoundSkipsNewerVersions(t *testing.T) {
	seek := ikey.Seek([]byte("k"), 5)
	if ikey.Compare(seek, enc("k", 6, ikey.KindValue)) <= 0 {
		t.Fatal("seek at seq=5 sorts before seq=6; a bounded read would see a newer version")
	}
	if ikey.Compare(seek, enc("k", 5, ikey.KindValue)) >= 0 {
		t.Fatal("seek at seq=5 sorts after seq=5; a bounded read would miss its own snapshot")
	}
}

func TestMalformedKeysDoNotPanic(t *testing.T) {
	bad := [][]byte{nil, {}, {0x01}, bytes.Repeat([]byte{0}, ikey.TrailerSize-1)}
	good := enc("k", 1, ikey.KindValue)

	for _, b := range bad {
		if ikey.Valid(b) {
			t.Errorf("Valid(%d-byte key) = true", len(b))
		}
		if ikey.Compare(b, good) >= 0 {
			t.Errorf("a malformed key did not sort before a well-formed one")
		}
		if ikey.Compare(good, b) <= 0 {
			t.Errorf("a well-formed key did not sort after a malformed one")
		}
		_ = ikey.String(b)
		_ = ikey.UserKey(b)
		_ = ikey.Seq(b)
		_ = ikey.KindOf(b)
	}
	// An exactly-trailer-length key has an empty user key: well formed by
	// length but not something the Store contract can have produced.
	empty := make([]byte, ikey.TrailerSize)
	if ikey.Valid(empty) {
		t.Error("Valid(empty user key) = true; empty keys are rejected by the Store contract")
	}
}

func TestKindValidity(t *testing.T) {
	if !ikey.KindValue.Valid() || !ikey.KindTombstone.Valid() {
		t.Fatal("the two real kinds must be valid")
	}
	if ikey.Kind(0x02).Valid() || ikey.Kind(0xff).Valid() {
		t.Fatal("an unknown kind must not be accepted; a file carrying one is not readable by this version")
	}
}
