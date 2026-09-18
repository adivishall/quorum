package memtable_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/adivishall/quorum/internal/storage/ikey"
	"github.com/adivishall/quorum/internal/storage/memtable"
)

func newTable() *memtable.MemTable { return memtable.New(1) }

// collect drains an iterator into a printable, comparable form.
func collect(m *memtable.MemTable) []string {
	var out []string
	it := m.NewIterator()
	for it.Next() {
		out = append(out, fmt.Sprintf("%s=%q", ikey.String(it.Key()), it.Value()))
	}
	return out
}

func TestEmptyTable(t *testing.T) {
	m := newTable()
	if !m.Empty() || m.Len() != 0 {
		t.Fatalf("a new memtable is not empty: Len=%d", m.Len())
	}
	if m.ApproxSize() != 0 {
		t.Fatalf("ApproxSize = %d, want 0", m.ApproxSize())
	}
	if _, _, found := m.Get([]byte("k"), ikey.MaxSeq); found {
		t.Fatal("Get on an empty memtable reported a hit")
	}
	it := m.NewIterator()
	if it.Next() {
		t.Fatal("iterator over an empty memtable produced an entry")
	}
	if it.Valid() || it.Key() != nil || it.Value() != nil {
		t.Fatal("an exhausted iterator is not reporting itself as exhausted")
	}
}

func TestAddAndGet(t *testing.T) {
	m := newTable()
	m.Add(1, ikey.KindValue, []byte("k"), []byte("v"))

	val, kind, found := m.Get([]byte("k"), ikey.MaxSeq)
	if !found {
		t.Fatal("Get did not find the key just added")
	}
	if kind != ikey.KindValue {
		t.Fatalf("kind = %v, want value", kind)
	}
	if string(val) != "v" {
		t.Fatalf("value = %q, want %q", val, "v")
	}
	if _, _, found := m.Get([]byte("other"), ikey.MaxSeq); found {
		t.Fatal("Get found a key that was never added")
	}
}

// TestOrderedIteration is the property a map cannot provide and the reason the
// memtable is a skip list: entries come out sorted regardless of insert order.
func TestOrderedIteration(t *testing.T) {
	m := newTable()
	keys := []string{"delta", "alpha", "charlie", "bravo", "echo"}
	for i, k := range keys {
		m.Add(uint64(i+1), ikey.KindValue, []byte(k), []byte("v"))
	}

	var got []string
	it := m.NewIterator()
	for it.Next() {
		got = append(got, string(ikey.UserKey(it.Key())))
	}

	want := append([]string(nil), keys...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("iterated %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("position %d: got %q, want %q (full order: %v)", i, got[i], want[i], got)
		}
	}
}

// TestVersionsOfOneKeyAreNewestFirst pins the ordering rule that makes the
// read path "first match wins".
func TestVersionsOfOneKeyAreNewestFirst(t *testing.T) {
	m := newTable()
	m.Add(1, ikey.KindValue, []byte("k"), []byte("first"))
	m.Add(3, ikey.KindValue, []byte("k"), []byte("third"))
	m.Add(2, ikey.KindTombstone, []byte("k"), nil)

	want := []string{
		`"k"@3:value="third"`,
		`"k"@2:tombstone=""`,
		`"k"@1:value="first"`,
	}
	got := collect(m)
	if len(got) != len(want) {
		t.Fatalf("got %d entries %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("position %d: got %s, want %s", i, got[i], want[i])
		}
	}

	// All three versions are retained; only the read path collapses them.
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3: every version is a separate entry", m.Len())
	}
	val, kind, _ := m.Get([]byte("k"), ikey.MaxSeq)
	if kind != ikey.KindValue || string(val) != "third" {
		t.Fatalf("Get = (%q, %v), want (\"third\", value)", val, kind)
	}
}

func TestOverwriteReturnsNewestValue(t *testing.T) {
	m := newTable()
	for i, v := range []string{"a", "b", "c", ""} {
		m.Add(uint64(i+1), ikey.KindValue, []byte("k"), []byte(v))
		got, kind, found := m.Get([]byte("k"), ikey.MaxSeq)
		if !found || kind != ikey.KindValue {
			t.Fatalf("after write %d: found=%v kind=%v", i, found, kind)
		}
		if string(got) != v {
			t.Fatalf("after write %d: Get = %q, want %q", i, got, v)
		}
	}
}

// TestTombstoneIsAnAnswerNotAnAbsence is the distinction the whole delete path
// rests on: a tombstone means "found, deleted", so the caller stops searching
// older tables instead of falling through to a stale value.
func TestTombstoneIsAnAnswerNotAnAbsence(t *testing.T) {
	m := newTable()
	m.Add(1, ikey.KindValue, []byte("k"), []byte("v"))
	m.Add(2, ikey.KindTombstone, []byte("k"), nil)

	val, kind, found := m.Get([]byte("k"), ikey.MaxSeq)
	if !found {
		t.Fatal("a deleted key must still be FOUND in the memtable; " +
			"reporting absence would let an older SSTable resurrect it")
	}
	if kind != ikey.KindTombstone {
		t.Fatalf("kind = %v, want tombstone", kind)
	}
	if len(val) != 0 {
		t.Fatalf("a tombstone carries a value %q", val)
	}

	// Writing the key again after the delete resurrects it deliberately.
	m.Add(3, ikey.KindValue, []byte("k"), []byte("again"))
	val, kind, _ = m.Get([]byte("k"), ikey.MaxSeq)
	if kind != ikey.KindValue || string(val) != "again" {
		t.Fatalf("after re-put: (%q, %v), want (\"again\", value)", val, kind)
	}
}

func TestEmptyValueIsNotATombstone(t *testing.T) {
	m := newTable()
	m.Add(1, ikey.KindValue, []byte("k"), []byte{})

	val, kind, found := m.Get([]byte("k"), ikey.MaxSeq)
	if !found || kind != ikey.KindValue {
		t.Fatalf("(found, kind) = (%v, %v), want (true, value)", found, kind)
	}
	if val == nil || len(val) != 0 {
		t.Fatalf("value = %v, want a non-nil zero-length slice", val)
	}
}

// TestSequenceBoundHidesNewerVersions is not used by the Phase 3 read path,
// which always reads at the newest sequence. It is tested because the memtable
// is the structure Phase 12's snapshot reads will use unchanged.
func TestSequenceBoundHidesNewerVersions(t *testing.T) {
	m := newTable()
	m.Add(1, ikey.KindValue, []byte("k"), []byte("old"))
	m.Add(5, ikey.KindValue, []byte("k"), []byte("new"))

	if val, _, _ := m.Get([]byte("k"), 4); string(val) != "old" {
		t.Fatalf("Get at seq=4 = %q, want %q", val, "old")
	}
	if val, _, _ := m.Get([]byte("k"), 5); string(val) != "new" {
		t.Fatalf("Get at seq=5 = %q, want %q", val, "new")
	}
	if _, _, found := m.Get([]byte("k"), 0); found {
		t.Fatal("Get at seq=0 found a key whose first version is seq=1")
	}
}

func TestAddCopiesCallerBuffers(t *testing.T) {
	m := newTable()
	key := []byte("key")
	value := []byte("value")
	m.Add(1, ikey.KindValue, key, value)

	copy(key, []byte("CLO"))
	copy(value, []byte("MUTAT"))

	got, _, found := m.Get([]byte("key"), ikey.MaxSeq)
	if !found {
		t.Fatal("the key vanished after the caller reused its key buffer")
	}
	if string(got) != "value" {
		t.Fatalf("value = %q, want %q: Add retained the caller's slice", got, "value")
	}
}

func TestSeek(t *testing.T) {
	m := newTable()
	for i := 0; i < 10; i++ {
		m.Add(uint64(i+1), ikey.KindValue, []byte(fmt.Sprintf("key%02d", i*2)), []byte("v"))
	}

	it := m.NewIterator()
	if !it.Seek(ikey.Seek([]byte("key07"), ikey.MaxSeq)) {
		t.Fatal("Seek past an absent key found nothing")
	}
	if got := string(ikey.UserKey(it.Key())); got != "key08" {
		t.Fatalf("Seek(key07) landed on %q, want %q", got, "key08")
	}

	if !it.Seek(ikey.Seek([]byte("key00"), ikey.MaxSeq)) {
		t.Fatal("Seek to the first key found nothing")
	}
	if got := string(ikey.UserKey(it.Key())); got != "key00" {
		t.Fatalf("Seek(key00) landed on %q", got)
	}

	if it.Seek(ikey.Seek([]byte("zzz"), ikey.MaxSeq)) {
		t.Fatalf("Seek past the last key landed on %s", ikey.String(it.Key()))
	}
	if it.Valid() {
		t.Fatal("iterator reports valid after seeking past the end")
	}
}

func TestApproxSizeGrowsWithContent(t *testing.T) {
	m := newTable()
	var last int64
	for i := 0; i < 50; i++ {
		m.Add(uint64(i+1), ikey.KindValue, []byte(fmt.Sprintf("key%04d", i)), bytes.Repeat([]byte("v"), 100))
		size := m.ApproxSize()
		if size <= last {
			t.Fatalf("after %d entries ApproxSize = %d, did not grow from %d", i+1, size, last)
		}
		last = size
	}
	// The estimate must at least cover the payload it is accounting for.
	minPayload := int64(50 * (len("key0000") + ikey.TrailerSize + 100))
	if last < minPayload {
		t.Fatalf("ApproxSize = %d, which is below the %d bytes of payload alone", last, minPayload)
	}
}

func TestFreeze(t *testing.T) {
	m := newTable()
	m.Add(1, ikey.KindValue, []byte("k"), []byte("v"))
	if m.Frozen() {
		t.Fatal("a new memtable reports itself frozen")
	}
	m.Freeze()
	m.Freeze() // idempotent
	if !m.Frozen() {
		t.Fatal("Frozen() = false after Freeze()")
	}

	// Reads keep working on a frozen table: that is the point of freezing
	// rather than discarding.
	if _, _, found := m.Get([]byte("k"), ikey.MaxSeq); !found {
		t.Fatal("a frozen memtable stopped answering reads")
	}

	defer func() {
		if recover() == nil {
			t.Fatal("Add on a frozen memtable did not panic; a mutation that lands " +
				"after the flush snapshot would be silently lost")
		}
	}()
	m.Add(2, ikey.KindValue, []byte("k2"), []byte("v"))
}

// TestDeterministicAcrossSeeds is the guarantee that the height PRNG is a
// performance knob and nothing else: the same mutations produce the same
// ordered content whatever the seed.
func TestDeterministicAcrossSeeds(t *testing.T) {
	build := func(seed uint64) []string {
		m := memtable.New(seed)
		rng := rand.New(rand.NewSource(99))
		for i := 0; i < 500; i++ {
			k := fmt.Sprintf("key%03d", rng.Intn(120))
			if i%7 == 0 {
				m.Add(uint64(i+1), ikey.KindTombstone, []byte(k), nil)
			} else {
				m.Add(uint64(i+1), ikey.KindValue, []byte(k), []byte(fmt.Sprintf("v%d", i)))
			}
		}
		return collect(m)
	}

	reference := build(1)
	if len(reference) != 500 {
		t.Fatalf("built %d entries, want 500", len(reference))
	}
	for _, seed := range []uint64{1, 2, 0xdeadbeef, memtable.DefaultSeed} {
		got := build(seed)
		if len(got) != len(reference) {
			t.Fatalf("seed %d produced %d entries, want %d", seed, len(got), len(reference))
		}
		for i := range reference {
			if got[i] != reference[i] {
				t.Fatalf("seed %d differs at position %d: %s vs %s", seed, i, got[i], reference[i])
			}
		}
	}
}

// TestRepeatedBuildsAreIdentical is the plain determinism check: the same
// sequence of operations twice gives byte-identical iteration.
func TestRepeatedBuildsAreIdentical(t *testing.T) {
	build := func() []string {
		m := newTable()
		for i := 0; i < 200; i++ {
			m.Add(uint64(i+1), ikey.KindValue, []byte(fmt.Sprintf("k%03d", (i*37)%50)), []byte("v"))
		}
		return collect(m)
	}
	a, b := build(), build()
	if len(a) != len(b) {
		t.Fatalf("two identical builds produced %d and %d entries", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("two identical builds differ at %d: %s vs %s", i, a[i], b[i])
		}
	}
}

// TestLargeTableOrdering exercises the tower logic past the point where the
// list has grown to several levels.
func TestLargeTableOrdering(t *testing.T) {
	const n = 20000
	m := newTable()
	rng := rand.New(rand.NewSource(5))
	perm := rng.Perm(n)
	for i, p := range perm {
		m.Add(uint64(i+1), ikey.KindValue, []byte(fmt.Sprintf("key%08d", p)), []byte("v"))
	}
	if m.Len() != n {
		t.Fatalf("Len = %d, want %d", m.Len(), n)
	}

	it := m.NewIterator()
	prev := []byte(nil)
	count := 0
	for it.Next() {
		if prev != nil && ikey.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("iteration is out of order at entry %d: %s then %s",
				count, ikey.String(prev), ikey.String(it.Key()))
		}
		prev = append(prev[:0], it.Key()...)
		count++
	}
	if count != n {
		t.Fatalf("iterated %d entries, want %d", count, n)
	}

	for i := 0; i < n; i += 97 {
		if _, _, found := m.Get([]byte(fmt.Sprintf("key%08d", i)), ikey.MaxSeq); !found {
			t.Fatalf("key%08d is missing", i)
		}
	}
}

// TestConcurrentReadsDuringWrites runs under -race. Nodes are never removed
// and keys never change, so readers may run against a live memtable; this is
// the test that keeps that true.
func TestConcurrentReadsDuringWrites(t *testing.T) {
	m := newTable()
	const n = 2000

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < n; i++ {
			m.Add(uint64(i+1), ikey.KindValue, []byte(fmt.Sprintf("key%06d", i)), []byte("value"))
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// A point read of a key that may or may not be there yet.
				if val, kind, found := m.Get([]byte(fmt.Sprintf("key%06d", r*13)), ikey.MaxSeq); found {
					if kind != ikey.KindValue || string(val) != "value" {
						t.Errorf("reader %d saw (%q, %v)", val, kind, r)
						return
					}
				}
				// A full scan, asserting order the whole way.
				it := m.NewIterator()
				var prev []byte
				for it.Next() {
					if prev != nil && ikey.Compare(prev, it.Key()) >= 0 {
						t.Errorf("reader %d saw an out-of-order pair: %s then %s",
							r, ikey.String(prev), ikey.String(it.Key()))
						return
					}
					prev = append(prev[:0], it.Key()...)
				}
			}
		}(r)
	}
	wg.Wait()

	if m.Len() != n {
		t.Fatalf("Len = %d after concurrent access, want %d", m.Len(), n)
	}
}

// TestConcurrentWritersDistinctKeys checks the write lock itself.
func TestConcurrentWritersDistinctKeys(t *testing.T) {
	m := newTable()
	const writers, perWriter = 8, 500

	var wg sync.WaitGroup
	var seq uint64
	var seqMu sync.Mutex
	nextSeq := func() uint64 {
		seqMu.Lock()
		defer seqMu.Unlock()
		seq++
		return seq
	}

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				m.Add(nextSeq(), ikey.KindValue, []byte(fmt.Sprintf("w%02d-k%05d", w, i)), []byte("v"))
			}
		}(w)
	}
	wg.Wait()

	if got := m.Len(); got != writers*perWriter {
		t.Fatalf("Len = %d, want %d: a concurrent insert was lost", got, writers*perWriter)
	}
	it := m.NewIterator()
	var prev []byte
	for it.Next() {
		if prev != nil && ikey.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("out of order after concurrent writes: %s then %s",
				ikey.String(prev), ikey.String(it.Key()))
		}
		prev = append(prev[:0], it.Key()...)
	}
}

func TestOpaqueKeysRoundTrip(t *testing.T) {
	m := newTable()
	keys := [][]byte{
		[]byte(" "), []byte("a\nb"), []byte("a\x00b"), {0xff, 0xfe}, {0xc3, 0x28},
		[]byte("ключ"), []byte(`{"k":"v"}`),
	}
	for i, k := range keys {
		m.Add(uint64(i+1), ikey.KindValue, k, []byte(fmt.Sprintf("v%d", i)))
	}
	if m.Len() != len(keys) {
		t.Fatalf("Len = %d, want %d: keys were normalised or collided", m.Len(), len(keys))
	}
	for i, k := range keys {
		got, kind, found := m.Get(k, ikey.MaxSeq)
		if !found || kind != ikey.KindValue {
			t.Fatalf("key %d (%q) not found", i, k)
		}
		if want := fmt.Sprintf("v%d", i); string(got) != want {
			t.Fatalf("key %d: got %q, want %q", i, got, want)
		}
	}
}

// TestPrefixKeysAreDistinct guards the boundary between the user key and the
// trailer. "a" and "a\x00..." must not be confusable once the trailer is
// appended.
func TestPrefixKeysAreDistinct(t *testing.T) {
	m := newTable()
	m.Add(1, ikey.KindValue, []byte("a"), []byte("short"))
	m.Add(2, ikey.KindValue, []byte("a\x00"), []byte("long"))
	m.Add(3, ikey.KindValue, append([]byte("a"), make([]byte, ikey.TrailerSize)...), []byte("trailerish"))

	for _, tc := range []struct{ key, want string }{
		{"a", "short"},
		{"a\x00", "long"},
		{"a" + string(make([]byte, ikey.TrailerSize)), "trailerish"},
	} {
		got, _, found := m.Get([]byte(tc.key), ikey.MaxSeq)
		if !found {
			t.Fatalf("key %q not found", tc.key)
		}
		if string(got) != tc.want {
			t.Fatalf("Get(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}
