package compaction_test

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/adivishall/quorum/internal/storage/compaction"
	"github.com/adivishall/quorum/internal/storage/ikey"
)

// ---------------------------------------------------------------- fixtures

// entry is one logical (user key, seq, kind, value).
type entry struct {
	key  string
	seq  uint64
	kind ikey.Kind
	val  string
}

func put(key string, seq uint64, val string) entry {
	return entry{key: key, seq: seq, kind: ikey.KindValue, val: val}
}
func del(key string, seq uint64) entry {
	return entry{key: key, seq: seq, kind: ikey.KindTombstone}
}

// memStream is an in-memory Stream, so the merge can be tested with no files.
type memStream struct {
	entries []entry
	i       int
	key     []byte
	val     []byte
	failAt  int // 1-based index at which Next reports a failure; 0 = never
	err     error
}

func newStream(entries ...entry) *memStream {
	// A Stream must be sorted in internal-key order, exactly as an SSTable is.
	sorted := append([]entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		return ikey.Compare(enc(sorted[i]), enc(sorted[j])) < 0
	})
	return &memStream{entries: sorted}
}

func enc(e entry) []byte { return ikey.Encode(nil, []byte(e.key), e.seq, e.kind) }

func (s *memStream) Next() bool {
	if s.failAt > 0 && s.i+1 == s.failAt {
		s.err = errors.New("simulated input failure")
		return false
	}
	if s.i >= len(s.entries) {
		return false
	}
	e := s.entries[s.i]
	s.i++
	s.key = enc(e)
	s.val = []byte(e.val)
	return true
}
func (s *memStream) Key() []byte   { return s.key }
func (s *memStream) Value() []byte { return s.val }
func (s *memStream) Err() error    { return s.err }

// collect drains a merger into a comparable form.
func collect(t *testing.T, m *compaction.Merger) []entry {
	t.Helper()
	var out []entry
	for m.Next() {
		k := m.Key()
		out = append(out, entry{
			key:  string(ikey.UserKey(k)),
			seq:  ikey.Seq(k),
			kind: ikey.KindOf(k),
			val:  string(m.Value()),
		})
	}
	if err := m.Err(); err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	return out
}

// reference computes the expected merge output independently of the
// implementation: newest version per user key, tombstones dropped only when
// bottom-most, sorted in internal-key order.
func reference(inputs [][]entry, bottomMost bool) []entry {
	newest := map[string]entry{}
	for _, in := range inputs {
		for _, e := range in {
			cur, ok := newest[e.key]
			if !ok || e.seq > cur.seq {
				newest[e.key] = e
			}
		}
	}
	var out []entry
	for _, e := range newest {
		if e.kind == ikey.KindTombstone && bottomMost {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		return ikey.Compare(enc(out[i]), enc(out[j])) < 0
	})
	return out
}

func assertEntries(t *testing.T, got, want []entry, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d entries, want %d\n got: %v\nwant: %v", what, len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: entry %d = %+v, want %+v", what, i, got[i], want[i])
		}
	}
}

func merge(t *testing.T, bottomMost bool, streams ...*memStream) *compaction.Merger {
	t.Helper()
	in := make([]compaction.Stream, 0, len(streams))
	for _, s := range streams {
		in = append(in, s)
	}
	return compaction.New(in, compaction.Options{BottomMost: bottomMost})
}

// ---------------------------------------------------------------- ordering

func TestMergeProducesOneSortedStream(t *testing.T) {
	a := newStream(put("a", 1, "a1"), put("d", 4, "d4"))
	b := newStream(put("b", 2, "b2"), put("e", 5, "e5"))
	c := newStream(put("c", 3, "c3"), put("f", 6, "f6"))

	got := collect(t, merge(t, false, a, b, c))
	want := []entry{
		put("a", 1, "a1"), put("b", 2, "b2"), put("c", 3, "c3"),
		put("d", 4, "d4"), put("e", 5, "e5"), put("f", 6, "f6"),
	}
	assertEntries(t, got, want, "three-way merge")
}

func TestMergeOfNoInputsIsEmpty(t *testing.T) {
	m := compaction.New(nil, compaction.Options{})
	if m.Next() {
		t.Fatal("a merge with no inputs produced an entry")
	}
	if err := m.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
}

func TestMergeOfEmptyStreams(t *testing.T) {
	got := collect(t, merge(t, false, newStream(), newStream()))
	if len(got) != 0 {
		t.Fatalf("got %v, want nothing", got)
	}
}

func TestMergeOfOneStreamIsTheStream(t *testing.T) {
	got := collect(t, merge(t, false, newStream(put("a", 1, "x"), put("b", 2, "y"))))
	assertEntries(t, got, []entry{put("a", 1, "x"), put("b", 2, "y")}, "single input")
}

// ---------------------------------------------------------------- version elimination

func TestNewestVersionWinsAndOlderVersionsAreDropped(t *testing.T) {
	// Three files each holding one version of "k", newest in the newest file.
	old := newStream(put("k", 5, "oldest"))
	mid := newStream(put("k", 8, "middle"))
	new := newStream(put("k", 10, "newest"))

	m := merge(t, false, old, mid, new)
	got := collect(t, m)
	assertEntries(t, got, []entry{put("k", 10, "newest")}, "version elimination")

	st := m.Stats()
	if st.InputEntries != 3 || st.OutputEntries != 1 || st.VersionsDropped != 2 {
		t.Fatalf("stats = %+v, want 3 in, 1 out, 2 versions dropped", st)
	}
}

func TestVersionsWithinOneStreamAreCollapsed(t *testing.T) {
	s := newStream(put("k", 1, "v1"), put("k", 2, "v2"), put("k", 3, "v3"), put("z", 1, "z"))
	got := collect(t, merge(t, false, s))
	assertEntries(t, got, []entry{put("k", 3, "v3"), put("z", 1, "z")}, "intra-stream collapse")
}

// ---------------------------------------------------------------- tombstones

// TestTombstoneIsKeptWhenNotBottomMost is the INV-S3 guard at the merge level.
// An older file outside this compaction may still hold a value for the key, and
// the tombstone is the only thing hiding it.
func TestTombstoneIsKeptWhenNotBottomMost(t *testing.T) {
	a := newStream(put("k", 1, "value"))
	b := newStream(del("k", 2))

	m := merge(t, false, a, b)
	got := collect(t, m)
	assertEntries(t, got, []entry{del("k", 2)}, "tombstone retained")

	st := m.Stats()
	if st.TombstonesKept != 1 || st.TombstonesDropped != 0 {
		t.Fatalf("stats = %+v, want 1 tombstone kept and 0 dropped", st)
	}
}

func TestTombstoneIsDroppedOnlyWhenBottomMost(t *testing.T) {
	a := newStream(put("k", 1, "value"))
	b := newStream(del("k", 2))

	m := merge(t, true, a, b)
	got := collect(t, m)
	if len(got) != 0 {
		t.Fatalf("got %v, want nothing: a bottom-most compaction has nothing older "+
			"to hide, so the tombstone can go", got)
	}
	st := m.Stats()
	if st.TombstonesDropped != 1 || st.TombstonesKept != 0 {
		t.Fatalf("stats = %+v, want 1 tombstone dropped", st)
	}
	// The value it was hiding went with it.
	if st.VersionsDropped != 1 {
		t.Fatalf("VersionsDropped = %d, want 1", st.VersionsDropped)
	}
}

// TestDroppedTombstoneStillSuppressesOlderVersions is the subtle one. When a
// tombstone is dropped, the versions behind it must be dropped too. Tracking only
// what was *emitted* would let the next older version through and resurrect the
// key with a stale value.
func TestDroppedTombstoneStillSuppressesOlderVersions(t *testing.T) {
	s := newStream(
		put("k", 1, "oldest"),
		put("k", 2, "middle"),
		del("k", 3),
		put("survivor", 4, "here"),
	)
	got := collect(t, merge(t, true, s))
	assertEntries(t, got, []entry{put("survivor", 4, "here")},
		"a dropped tombstone must suppress everything behind it")
}

func TestTombstoneNewerThanValueInTheSameStream(t *testing.T) {
	// Not bottom-most: the tombstone survives and hides the value.
	s := newStream(put("k", 1, "v"), del("k", 2))
	got := collect(t, merge(t, false, s))
	assertEntries(t, got, []entry{del("k", 2)}, "tombstone over value")
}

func TestValueNewerThanTombstoneIsRestored(t *testing.T) {
	// put, delete, put again: the newest put wins and no tombstone survives.
	s := newStream(put("k", 1, "first"), del("k", 2), put("k", 3, "second"))
	for _, bottom := range []bool{false, true} {
		got := collect(t, merge(t, bottom, newStreamCopy(s)))
		assertEntries(t, got, []entry{put("k", 3, "second")},
			fmt.Sprintf("value over tombstone (bottomMost=%v)", bottom))
	}
}

func newStreamCopy(s *memStream) *memStream {
	return &memStream{entries: append([]entry(nil), s.entries...)}
}

func TestEmptyValueIsNotATombstone(t *testing.T) {
	s := newStream(put("k", 1, ""))
	got := collect(t, merge(t, true, s))
	// Bottom-most, but this is a present key with an empty value, not a delete.
	assertEntries(t, got, []entry{put("k", 1, "")}, "empty value survives a bottom-most merge")
	if got[0].kind != ikey.KindValue {
		t.Fatal("an empty value was treated as a tombstone")
	}
}

// ---------------------------------------------------------------- failures

// TestInputFailureStopsTheMergeAndIsReported is what keeps a compaction from
// silently producing a short output. A stream that fails must not look like one
// that finished.
func TestInputFailureStopsTheMergeAndIsReported(t *testing.T) {
	good := newStream(put("a", 1, "a"), put("b", 2, "b"))
	bad := newStream(put("c", 3, "c"), put("d", 4, "d"))
	bad.failAt = 2 // fails on its second entry

	m := merge(t, false, good, bad)
	for m.Next() {
	}
	if m.Err() == nil {
		t.Fatal("a failing input produced a clean end of merge; the output would be " +
			"a structurally perfect file that is silently missing data")
	}
}

func TestInputFailureAtPositioningIsReported(t *testing.T) {
	bad := newStream(put("a", 1, "a"))
	bad.failAt = 1 // fails immediately

	m := merge(t, false, bad)
	if m.Next() {
		t.Fatal("Next succeeded on a merge whose input failed to position")
	}
	if m.Err() == nil {
		t.Fatal("Err is nil after an input failed to position")
	}
}

// TestOverlappingInputsAreRefused: the same internal key in two inputs means two
// files claim the same mutation, which the disjoint sequence ranges are supposed
// to make impossible. The merge refuses rather than silently emitting one.
func TestOverlappingInputsAreRefused(t *testing.T) {
	a := newStream(put("k", 7, "from a"))
	b := newStream(put("k", 7, "from b"))

	m := merge(t, false, a, b)
	var n int
	for m.Next() {
		n++
	}
	if m.Err() == nil {
		t.Fatalf("two inputs holding the same internal key were merged silently "+
			"(emitted %d entries); overlapping sequence ranges are corruption", n)
	}
	if !errors.Is(m.Err(), compaction.ErrCorrupt) {
		t.Fatalf("Err = %v, want ErrCorrupt", m.Err())
	}
}

func TestMalformedInternalKeyIsRefused(t *testing.T) {
	m := compaction.New([]compaction.Stream{&rawStream{keys: [][]byte{{0x01, 0x02}}}},
		compaction.Options{})
	if m.Next() {
		t.Fatal("a malformed internal key was accepted")
	}
	if !errors.Is(m.Err(), compaction.ErrCorrupt) {
		t.Fatalf("Err = %v, want ErrCorrupt", m.Err())
	}
}

// rawStream emits arbitrary key bytes, bypassing the ikey encoder.
type rawStream struct {
	keys [][]byte
	i    int
}

func (s *rawStream) Next() bool {
	if s.i >= len(s.keys) {
		return false
	}
	s.i++
	return true
}
func (s *rawStream) Key() []byte   { return s.keys[s.i-1] }
func (s *rawStream) Value() []byte { return nil }
func (s *rawStream) Err() error    { return nil }

// ---------------------------------------------------------------- differential

// TestAgainstReferenceModel generates random file sets and compares the merge
// against an independent computation of what the result should be.
func TestAgainstReferenceModel(t *testing.T) {
	for _, bottomMost := range []bool{false, true} {
		for seed := int64(1); seed <= 40; seed++ {
			t.Run(fmt.Sprintf("bottomMost=%v/seed=%d", bottomMost, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))

				// Build k files whose sequence ranges are disjoint and ascending,
				// which is the invariant the real engine maintains.
				k := 1 + rng.Intn(6)
				var (
					inputs  [][]entry
					streams []*memStream
					seq     uint64
				)
				for f := 0; f < k; f++ {
					n := rng.Intn(25)
					var es []entry
					// Within one file a user key appears at most once per
					// sequence, and a file is a set of distinct internal keys.
					used := map[string]bool{}
					for i := 0; i < n; i++ {
						seq++
						key := fmt.Sprintf("key%02d", rng.Intn(15))
						if used[fmt.Sprintf("%s@%d", key, seq)] {
							continue
						}
						used[fmt.Sprintf("%s@%d", key, seq)] = true
						if rng.Intn(4) == 0 {
							es = append(es, del(key, seq))
						} else {
							es = append(es, put(key, seq, fmt.Sprintf("v%d", seq)))
						}
					}
					inputs = append(inputs, es)
					streams = append(streams, newStream(es...))
				}

				got := collect(t, merge(t, bottomMost, streams...))
				want := reference(inputs, bottomMost)
				assertEntries(t, got, want, "differential")
			})
		}
	}
}

// TestLargeMergeStaysOrdered runs enough entries that a heap bug would show up as
// an ordering violation rather than a coin flip.
func TestLargeMergeStaysOrdered(t *testing.T) {
	const (
		files      = 8
		perFile    = 4000
		keyspace   = 6000
		bottomMost = false
	)
	rng := rand.New(rand.NewSource(99))
	var (
		inputs  [][]entry
		streams []*memStream
		seq     uint64
	)
	for f := 0; f < files; f++ {
		var es []entry
		for i := 0; i < perFile; i++ {
			seq++
			key := fmt.Sprintf("key%06d", rng.Intn(keyspace))
			if rng.Intn(5) == 0 {
				es = append(es, del(key, seq))
			} else {
				es = append(es, put(key, seq, fmt.Sprintf("v%d", seq)))
			}
		}
		inputs = append(inputs, es)
		streams = append(streams, newStream(es...))
	}

	m := merge(t, bottomMost, streams...)
	var (
		prev  []byte
		count int
	)
	for m.Next() {
		k := m.Key()
		if prev != nil && ikey.Compare(prev, k) >= 0 {
			t.Fatalf("output is not strictly increasing at entry %d", count)
		}
		prev = append(prev[:0], k...)
		count++
	}
	if err := m.Err(); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	want := reference(inputs, bottomMost)
	if count != len(want) {
		t.Fatalf("emitted %d entries, want %d", count, len(want))
	}
	st := m.Stats()
	if st.InputEntries != files*perFile {
		t.Fatalf("InputEntries = %d, want %d", st.InputEntries, files*perFile)
	}
	if st.OutputEntries+st.VersionsDropped+st.TombstonesDropped != st.InputEntries {
		t.Fatalf("stats do not balance: %+v", st)
	}
	t.Logf("%d input entries across %d files -> %d output entries (%d obsolete versions dropped, "+
		"%d tombstones kept)", st.InputEntries, files, st.OutputEntries, st.VersionsDropped,
		st.TombstonesKept)
}

// ---------------------------------------------------------------- fuzz

// FuzzMergeMatchesReference is the differential property with fuzzer-chosen
// inputs: whatever goes in, the merge agrees with the independent computation.
func FuzzMergeMatchesReference(f *testing.F) {
	f.Add([]byte("abc"), []byte("bcd"), uint8(3), false)
	f.Add([]byte(""), []byte("a"), uint8(0), true)

	f.Fuzz(func(t *testing.T, ka, kb []byte, kinds uint8, bottomMost bool) {
		if len(ka) > 32 || len(kb) > 32 {
			t.Skip()
		}
		// Two files, disjoint ascending sequences, keys drawn from the inputs.
		var a, b []entry
		seq := uint64(0)
		add := func(dst *[]entry, keys []byte, bit uint) {
			for i, c := range keys {
				seq++
				k := fmt.Sprintf("k%c", c)
				if kinds&(1<<((uint(i)+bit)%8)) != 0 {
					*dst = append(*dst, del(k, seq))
				} else {
					*dst = append(*dst, put(k, seq, fmt.Sprintf("v%d", seq)))
				}
			}
		}
		add(&a, ka, 0)
		add(&b, kb, 1)

		got := collect(t, merge(t, bottomMost, newStream(a...), newStream(b...)))
		want := reference([][]entry{a, b}, bottomMost)
		assertEntries(t, got, want, "fuzz differential")
	})
}
