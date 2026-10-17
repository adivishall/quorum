package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/storage"
)

// S4 (docs/STORAGE_INTEGRATION.md §10.3, §10.6): the engine checkpoint and
// ingest.

// kv is one entry of an ingest or a checkpoint.
type kv struct{ k, v string }

// sliceSource is an IngestSource over entries, optionally failing at the end.
type sliceSource struct {
	entries []kv
	i       int
	endErr  error
}

func (s *sliceSource) Next() bool    { s.i++; return s.i <= len(s.entries) }
func (s *sliceSource) Key() []byte   { return []byte(s.entries[s.i-1].k) }
func (s *sliceSource) Value() []byte { return []byte(s.entries[s.i-1].v) }
func (s *sliceSource) Err() error {
	if s.i > len(s.entries) {
		return s.endErr
	}
	return nil
}

func source(entries ...kv) *sliceSource { return &sliceSource{entries: entries} }

// readCheckpoint returns every entry a checkpoint iterates, in order.
func readCheckpoint(t *testing.T, c *storage.Checkpoint) []kv {
	t.Helper()
	var out []kv
	it := c.Iterator()
	for it.Next() {
		out = append(out, kv{string(it.Key()), string(it.Value())})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterating the checkpoint: %v", err)
	}
	return out
}

// sorted returns a map's entries in key order.
func sorted(m map[string]string) []kv {
	out := make([]kv, 0, len(m))
	for k, v := range m {
		out = append(out, kv{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].k < out[j].k })
	return out
}

// contents returns a store's whole live state.
func contents(t *testing.T, s *storage.LSMStore) map[string]string {
	t.Helper()
	all, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string, len(all))
	for k, v := range all {
		out[k] = string(v)
	}
	return out
}

func sameEntries(a, b []kv) bool {
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

// TestACheckpointIsTheDurableStateAtItsAppliedIndex: a checkpoint flushes the
// engine and pins it at its applied index, which is then the durable index; it
// iterates exactly the live state in key order — deletions absent — and goes
// on iterating exactly that while the engine applies, flushes and compacts
// after it; and its tables stay on disk until it is closed.
func TestACheckpointIsTheDurableStateAtItsAppliedIndex(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(1<<10, 2)
	s := openLSM(t, dir, opts)
	defer s.Close()
	model := newApplyModel()
	step := func(st applyStep) {
		mustApply(t, s, st)
		model = model.after(st)
	}
	for i := 1; i <= 40; i++ {
		muts := []storage.Mutation{mput(fmt.Sprintf("k%03d", i%25), strings.Repeat("v", i))}
		if i%7 == 0 {
			muts = append(muts, mdel(fmt.Sprintf("k%03d", (i+3)%25)))
		}
		step(applyStep{muts, ai(uint64(i), 1)})
	}
	step(applyStep{nil, ai(41, 2)}) // an entry that writes nothing moves the index alone
	if s.DurableApplied() == ai(41, 2) {
		t.Fatal("premise: the engine is not yet durable at the last index")
	}

	c, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if c.Applied() != ai(41, 2) || s.DurableApplied() != ai(41, 2) {
		t.Fatalf("checkpoint at %+v, durable %+v; want both {41 2}", c.Applied(), s.DurableApplied())
	}
	want := sorted(model.state)
	if got := readCheckpoint(t, c); !sameEntries(got, want) {
		t.Fatalf("checkpoint holds %v, want %v", got, want)
	}
	pinned := sstFiles(t, dir)

	// The engine moves on; the checkpoint does not.
	for i := 42; i <= 60; i++ {
		step(applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%03d", i%25), "after"), mdel("k001")}, ai(uint64(i), 2)})
	}
	mustFlush(t, s)
	if n, err := s.CompactAll(); err != nil || n == 0 {
		t.Fatalf("CompactAll: %d, %v", n, err)
	}
	if got := readCheckpoint(t, c); !sameEntries(got, want) {
		t.Fatalf("after writes and a compaction the checkpoint holds %v, want %v", got, want)
	}
	for _, name := range pinned {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("a table the checkpoint pins was deleted under it: %v", err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	live := map[string]bool{}
	for _, info := range s.SSTables() {
		live[fmt.Sprintf("%06d.sst", info.Number)] = true
	}
	for _, name := range sstFiles(t, dir) {
		if !live[name] {
			t.Fatalf("closing the checkpoint left the retired table %s on disk", name)
		}
	}
	if got := sorted(contents(t, s)); !sameEntries(got, sorted(model.state)) {
		t.Fatal("the engine's own state is not the model after the checkpoint")
	}
}

// TestACheckpointOfAnEmptyEngineIsEmpty: no entries, applied index 0.
func TestACheckpointOfAnEmptyEngineIsEmpty(t *testing.T) {
	s := openLSM(t, t.TempDir(), lsmOpts(1<<20))
	defer s.Close()
	c, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := readCheckpoint(t, c); len(got) != 0 || c.Applied() != (storage.AppliedIndex{}) || c.Tables() != 0 {
		t.Fatalf("an empty engine's checkpoint: %v at %+v, %d tables", got, c.Applied(), c.Tables())
	}
}

// TestRepeatedCheckpointsReleaseEveryPin: checkpoints taken and closed between
// writes and compactions leave exactly the live tables on disk.
func TestRepeatedCheckpointsReleaseEveryPin(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, manualCompactOpts(512, 2))
	defer s.Close()
	for round := 1; round <= 15; round++ {
		for i := 0; i < 10; i++ {
			mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%02d", i), fmt.Sprintf("r%d-%s", round, strings.Repeat("x", 40)))},
				ai(uint64(round*10+i), 1)})
		}
		c, err := s.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.CompactAll(); err != nil {
			t.Fatal(err)
		}
		if got := readCheckpoint(t, c); len(got) != 10 {
			t.Fatalf("round %d: the checkpoint holds %d keys, want 10", round, len(got))
		}
		_ = c.Close()
	}
	if got, want := len(sstFiles(t, dir)), len(s.SSTables()); got != want {
		t.Fatalf("%d tables on disk, %d live: a checkpoint's pin was never released", got, want)
	}
}

// ingestBase builds a store holding an old state: flushed tables, a memtable,
// deletions, and keys the ingest will also carry.
func ingestBase(t *testing.T, s *storage.LSMStore) applyModel {
	t.Helper()
	model := newApplyModel()
	for i := 1; i <= 12; i++ {
		st := applyStep{[]storage.Mutation{mput(fmt.Sprintf("o%02d", i), "old"), mput(fmt.Sprintf("n%02d", i), "stale")}, ai(uint64(i), 1)}
		if i%4 == 0 {
			st.muts = append(st.muts, mdel(fmt.Sprintf("o%02d", i-1)))
		}
		mustApply(t, s, st)
		model = model.after(st)
		if i == 6 {
			mustFlush(t, s)
		}
	}
	return model
}

// newContents is what the tests ingest.
func newContents(n int) []kv {
	out := make([]kv, n)
	for i := range out {
		out[i] = kv{fmt.Sprintf("n%02d", i), strings.Repeat(string(rune('a'+i%26)), i%9)} // includes empty values
	}
	return out
}

// TestIngestReplacesTheWholeEngine: every old key is gone, every ingested key
// is present with its value (an empty value present), the applied index is
// the ingest's and is durable, the sequence continues above everything used,
// the old tables and WAL segments are retired, and all of it survives reopening.
func TestIngestReplacesTheWholeEngine(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(256) // several tables per ingest
	opts.WAL.SegmentSize = 128
	s := openLSM(t, dir, opts)
	old := ingestBase(t, s)
	seqBefore := s.Sequence()
	entries := newContents(40)
	stats, err := s.Ingest(source(entries...), ai(100, 3))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tables < 2 || stats.Entries != 40 || stats.Level < 1 || stats.Retired == 0 {
		t.Fatalf("ingest stats %+v: want several tables at level >= 1 replacing the old ones", stats)
	}
	check := func(s *storage.LSMStore, when string) {
		t.Helper()
		want := map[string]string{}
		for _, e := range entries {
			want[e.k] = e.v
		}
		if got := contents(t, s); !sameEntries(sorted(got), sorted(want)) {
			t.Fatalf("%s: the engine holds %v, want exactly the ingested %v", when, sorted(got), sorted(want))
		}
		if _, err := s.Get(context.Background(), []byte("o01")); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("%s: an old key survived the ingest: %v", when, err)
		}
		if s.AppliedIndex() != ai(100, 3) || s.DurableApplied() != ai(100, 3) {
			t.Fatalf("%s: applied %+v, durable %+v; want {100 3}", when, s.AppliedIndex(), s.DurableApplied())
		}
		if s.Sequence() != seqBefore+40 || s.AppliedSequence() != seqBefore+40 {
			t.Fatalf("%s: sequence %d, applied sequence %d; want %d", when, s.Sequence(), s.AppliedSequence(), seqBefore+40)
		}
	}
	check(s, "after the ingest")
	if old.seq != seqBefore {
		t.Fatalf("premise: the old state's sequence %d is the store's %d", old.seq, seqBefore)
	}
	for _, info := range s.SSTables() {
		if info.SmallestSeq <= seqBefore {
			t.Fatalf("table %d covers sequence %d, not above the old %d", info.Number, info.SmallestSeq, seqBefore)
		}
	}
	seg := s.WALStats().ActiveSegment
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if segs := walSegments(t, dir); len(segs) != 1 || segs[0] != seg {
		t.Fatalf("WAL segments after the ingest: %v, want only %d", segs, seg)
	}
	s = openLSM(t, dir, opts)
	defer s.Close()
	check(s, "after reopening")
	if got, want := len(sstFiles(t, dir)), len(s.SSTables()); got != want {
		t.Fatalf("%d tables on disk, %d live after reopening", got, want)
	}
	// The engine goes on.
	mustApply(t, s, applyStep{[]storage.Mutation{mput("after", "x")}, ai(101, 3)})
	mustFlush(t, s)
	if v, err := s.Get(context.Background(), []byte("after")); err != nil || string(v) != "x" {
		t.Fatalf("a write after the ingest: %q %v", v, err)
	}
}

// TestIngestOfNothingEmptiesTheEngine: an empty state is a valid state.
func TestIngestOfNothingEmptiesTheEngine(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(256))
	ingestBase(t, s)
	stats, err := s.Ingest(source(), ai(50, 2))
	if err != nil || stats.Tables != 0 {
		t.Fatalf("ingesting nothing: %+v %v", stats, err)
	}
	_ = s.Close()
	s = openLSM(t, dir, lsmOpts(256))
	defer s.Close()
	if got := contents(t, s); len(got) != 0 || s.DurableApplied() != ai(50, 2) || len(s.SSTables()) != 0 {
		t.Fatalf("after ingesting nothing: %d keys, durable %+v, %d tables", len(got), s.DurableApplied(), len(s.SSTables()))
	}
}

// TestARefusedIngestLeavesTheEngineUnchangedAndUsable: keys out of order or
// repeated, a key or value over the limits, an applied index that does not
// advance, a source that fails at its end — each is refused, nothing staged is
// left on disk, the state and indexes are unchanged, and the engine takes the
// next batch.
func TestARefusedIngestLeavesTheEngineUnchangedAndUsable(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(256)
	s := openLSM(t, dir, opts)
	defer s.Close()
	model := ingestBase(t, s)
	tables := sstFiles(t, dir)
	errSource := errors.New("the source's last check failed")
	failing := source(newContents(30)...)
	failing.endErr = errSource
	cases := []struct {
		name string
		src  storage.IngestSource
		at   storage.AppliedIndex
		want error
	}{
		{"unordered", source(kv{"b", "1"}, kv{"a", "2"}), ai(100, 3), storage.ErrInvalidIngest},
		{"repeated", source(kv{"a", "1"}, kv{"a", "2"}), ai(100, 3), storage.ErrInvalidIngest},
		{"empty key", source(kv{"", "1"}), ai(100, 3), storage.ErrInvalidIngest},
		{"key too long", source(kv{strings.Repeat("k", opts.MaxKeySize+1), "1"}), ai(100, 3), storage.ErrInvalidIngest},
		{"value too long", source(kv{"a", strings.Repeat("v", opts.MaxValueSize+1)}), ai(100, 3), storage.ErrInvalidIngest},
		{"index does not advance", source(kv{"a", "1"}), ai(12, 1), storage.ErrInvalidIngest},
		{"term goes back", source(kv{"a", "1"}), ai(100, 0), storage.ErrInvalidIngest},
		{"the source fails at its end", failing, ai(100, 3), errSource},
	}
	for _, tc := range cases {
		if _, err := s.Ingest(tc.src, tc.at); !errors.Is(err, tc.want) {
			t.Fatalf("%s: %v, want %v", tc.name, err, tc.want)
		}
		if d := diff(s, model, append(keysOfModel(model), "a", "b", "n05")); d != "" {
			t.Fatalf("%s: the refused ingest changed the engine: %s", tc.name, d)
		}
		if got := sstFiles(t, dir); strings.Join(got, ",") != strings.Join(tables, ",") {
			t.Fatalf("%s: tables on disk %v, before %v: a staged table was left behind", tc.name, got, tables)
		}
	}
	mustApply(t, s, applyStep{[]storage.Mutation{mput("next", "1")}, ai(13, 1)})
}

func keysOfModel(m applyModel) []string {
	var keys []string
	for k := range m.state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestAnIngestLargerThanOneWALRecord: the S3 restore wrote a state as ONE WAL
// record and refused anything over ~63 MiB; ingest writes tables and has no
// such bound. 80 one-MiB values, more than the 64 MiB record limit.
func TestAnIngestLargerThanOneWALRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 80 MiB")
	}
	dir := t.TempDir()
	opts := lsmOpts(8 << 20)
	s := openLSM(t, dir, opts)
	mustApply(t, s, applyStep{[]storage.Mutation{mput("old", "1")}, ai(1, 1)})
	val := strings.Repeat("v", 1<<20)
	entries := make([]kv, 80)
	for i := range entries {
		entries[i] = kv{fmt.Sprintf("big%03d", i), val[:len(val)-i]}
	}
	stats, err := s.Ingest(source(entries...), ai(2, 1))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Bytes < 80<<20-80*80 {
		t.Fatalf("ingested %d table bytes for 80 MiB of values", stats.Bytes)
	}
	_ = s.Close()
	s = openLSM(t, dir, opts)
	defer s.Close()
	for _, i := range []int{0, 41, 79} {
		v, err := s.Get(context.Background(), []byte(entries[i].k))
		if err != nil || len(v) != len(entries[i].v) {
			t.Fatalf("key %s after reopening: %d bytes, %v", entries[i].k, len(v), err)
		}
	}
	if _, err := s.Get(context.Background(), []byte("old")); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("the old key survived: %v", err)
	}
	t.Logf("ingested %d MiB in %d tables at level %d", stats.Bytes>>20, stats.Tables, stats.Level)
}

// TestIngestedTablesAreNotCompactedAway: placed at a level whose budget holds
// them, the ingested tables are not merged the moment they land.
func TestIngestedTablesAreNotCompactedAway(t *testing.T) {
	s := openLSM(t, t.TempDir(), manualCompactOpts(256, 2))
	defer s.Close()
	ingestBase(t, s)
	stats, err := s.Ingest(source(newContents(60)...), ai(100, 3))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tables < 3 {
		t.Fatalf("premise: several tables ingested, got %d", stats.Tables)
	}
	if ran, err := s.Compact(); err != nil || ran {
		t.Fatalf("a compaction was due right after the ingest (ran %v, %v)", ran, err)
	}
}

// TestAFailedIngestCommitLatchesAndReopensOldOrNew: when the commit's MANIFEST
// append fails, the ingest is in doubt: the store refuses every later write,
// and a reopen finds the old state or the new one, whole — the old when nothing
// of the edit reached the file, the new when all of it did.
func TestAFailedIngestCommitLatchesAndReopensOldOrNew(t *testing.T) {
	for _, tc := range []struct {
		name string
		inj  fault.Injection
		want string
	}{
		{"nothing written", fault.Injection{Op: fault.OpWrite}, "old"},
		{"torn", fault.Injection{Op: fault.OpWrite, Short: 9}, "old"},
		{"written, not synced", fault.Injection{Op: fault.OpSync}, "new"}, // a process crash keeps it
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			inj := injectManifest(t) // before the open: the MANIFEST's writer is made there
			s := openLSM(t, dir, lsmOpts(256))
			old := ingestBase(t, s)
			inj.Arm(tc.inj)
			durable := s.DurableApplied()
			if _, err := s.Ingest(source(newContents(20)...), ai(100, 3)); err == nil {
				t.Fatal("the ingest succeeded although its commit failed")
			}
			inj.Disarm()
			// An ambiguous persistence failure never advances the durable
			// watermark R2 compacts against (S3's rule, kept by ingest).
			if got := s.DurableApplied(); got != durable {
				t.Fatalf("a failed ingest commit moved the durable applied index from %+v to %+v", durable, got)
			}
			if err := s.Apply(context.Background(), nil, ai(101, 3)); err == nil {
				t.Fatal("a store whose ingest commit failed took another batch")
			}
			_ = s.Close()
			s = openLSM(t, dir, lsmOpts(256))
			defer s.Close()
			switch tc.want {
			case "old":
				if d := diff(s, old, append(keysOfModel(old), "n05")); d != "" {
					t.Fatalf("reopened: %s, want the old state", d)
				}
			case "new":
				if s.AppliedIndex() != ai(100, 3) || len(contents(t, s)) != 20 {
					t.Fatalf("reopened at %+v with %d keys, want the ingested 20 at {100 3}", s.AppliedIndex(), len(contents(t, s)))
				}
			}
			if got, want := len(sstFiles(t, dir)), len(s.SSTables()); got != want {
				t.Fatalf("%d tables on disk, %d live after reopening", got, want)
			}
		})
	}
}

// TestAFailedCheckpointFlushLatchesAndPinsNothing: a checkpoint whose flush
// fails — here its MANIFEST append — returns no checkpoint, latches the engine
// as any failed flush does (every later write is refused until a reopen), and
// leaves the durable watermark where it was; the reopen recovers every batch.
func TestAFailedCheckpointFlushLatchesAndPinsNothing(t *testing.T) {
	dir := t.TempDir()
	inj := injectManifest(t) // before the open: the MANIFEST's writer is made there
	s := openLSM(t, dir, lsmOpts(1<<20))
	model := ingestBase(t, s)
	durable := s.DurableApplied()
	mustApply(t, s, applyStep{[]storage.Mutation{mput("tail", "1")}, ai(13, 1)})
	model = model.after(applyStep{[]storage.Mutation{mput("tail", "1")}, ai(13, 1)})
	inj.Arm(fault.Injection{Op: fault.OpWrite})
	if c, err := s.Checkpoint(); err == nil {
		_ = c.Close()
		t.Fatal("a checkpoint whose flush failed was returned")
	}
	inj.Disarm()
	if got := s.DurableApplied(); got != durable {
		t.Fatalf("a failed checkpoint flush moved the durable index from %+v to %+v", durable, got)
	}
	if err := s.Apply(context.Background(), nil, ai(14, 1)); err == nil {
		t.Fatal("a store whose checkpoint flush failed took another batch")
	}
	_ = s.Close()
	s = openLSM(t, dir, lsmOpts(1<<20))
	defer s.Close()
	if d := diff(s, model, append(keysOfModel(model), "tail")); d != "" {
		t.Fatalf("reopened after the failed checkpoint: %s", d)
	}
}

// TestACheckpointClosedFromManyGoroutinesReleasesOnce: Close is idempotent
// under concurrency — the pinned version is released exactly once. A second
// release of a checkpoint that pins the CURRENT version would drop its count
// to zero and delete tables the MANIFEST still names (S4 review).
func TestACheckpointClosedFromManyGoroutinesReleasesOnce(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(256))
	ingestBase(t, s)
	c, err := s.Checkpoint() // pins the current version: nothing changes after it
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Close()
		}()
	}
	wg.Wait()
	for _, info := range s.SSTables() {
		if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%06d.sst", info.Number))); err != nil {
			t.Fatalf("closing a checkpoint concurrently deleted the live table %d: %v", info.Number, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLSM(t, dir, lsmOpts(256))
	_ = s.Close()
}

// TestALatchedStoreRefusesSetAppliedIndex: once a flush (or an ingest's
// commit) has failed and latched the store, SetAppliedIndex is refused like
// every other write — after an ingest in doubt the WAL has rotated to the
// segment the edit may name, and a record there would be replayed onto
// whichever state the next open finds (S4 review).
func TestALatchedStoreRefusesSetAppliedIndex(t *testing.T) {
	dir := t.TempDir()
	inj := injectManifest(t) // before the open: the MANIFEST's writer is made there
	s := openLSM(t, dir, lsmOpts(1<<20))
	defer s.Close()
	mustApply(t, s, applyStep{[]storage.Mutation{mput("a", "1")}, ai(1, 1)})
	inj.Arm(fault.Injection{Op: fault.OpWrite})
	if err := s.Flush(); err == nil {
		t.Fatal("premise: the flush succeeded although its MANIFEST append failed")
	}
	inj.Disarm()
	if err := s.SetAppliedIndex(context.Background(), ai(2, 1)); err == nil {
		t.Fatal("a latched store recorded an applied index")
	}
}
