package storage_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/manifest"
)

// ---------------------------------------------------------------- helpers

// flushWith writes one key and flushes, producing exactly one L0 SSTable.
func flushWith(t *testing.T, s *storage.LSMStore, key, value string) {
	t.Helper()
	mustPut(t, s, key, value)
	mustFlush(t, s)
}

func mustCompact(t *testing.T, s *storage.LSMStore) bool {
	t.Helper()
	ran, err := s.Compact()
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	return ran
}

func mustCompactAll(t *testing.T, s *storage.LSMStore) int {
	t.Helper()
	n, err := s.CompactAll()
	if err != nil {
		t.Fatalf("CompactAll: %v", err)
	}
	return n
}

// waitFor polls until cond holds or the deadline passes. Used only for the
// background compactor, whose timing is genuinely asynchronous; every other test
// drives compaction explicitly.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---------------------------------------------------------------- the trigger

func TestL0TriggerMergesAllOfLevelZero(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, manualCompactOpts(storage.DefaultMemTableSize, 4))
	defer func() { _ = s.Close() }()

	for i := 0; i < 3; i++ {
		flushWith(t, s, fmt.Sprintf("k%d", i), "v")
	}
	// Three files is below the trigger of four.
	if ran := mustCompact(t, s); ran {
		t.Fatal("compaction ran with 3 L0 files and a trigger of 4")
	}
	if got := len(s.SSTables()); got != 3 {
		t.Fatalf("%d SSTables, want 3", got)
	}

	flushWith(t, s, "k3", "v")
	if ran := mustCompact(t, s); !ran {
		t.Fatal("compaction did not run with 4 L0 files and a trigger of 4")
	}

	// All four L0 files became one L1 file (docs/DESIGN.md §7).
	files := s.SSTables()
	if len(files) != 1 {
		t.Fatalf("%d SSTables after compaction, want 1: %+v", len(files), files)
	}
	if files[0].Level != 1 {
		t.Fatalf("output is at level %d, want 1", files[0].Level)
	}
	if files[0].Entries != 4 {
		t.Fatalf("output holds %d entries, want 4", files[0].Entries)
	}
	// Its sequence range spans every input.
	if files[0].SmallestSeq != 1 || files[0].LargestSeq != 4 {
		t.Fatalf("output covers [%d,%d], want [1,4]", files[0].SmallestSeq, files[0].LargestSeq)
	}

	for i := 0; i < 4; i++ {
		assertValue(t, s, fmt.Sprintf("k%d", i), "v")
	}
}

func TestCompactionIsNotTriggeredByASingleDeeperFile(t *testing.T) {
	dir := t.TempDir()
	// A tiny L1 budget, so a single L1 file is already over it.
	opts := manualCompactOpts(storage.DefaultMemTableSize, 2)
	opts.L1MaxBytes = 1
	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	flushWith(t, s, "a", "1")
	flushWith(t, s, "b", "2")
	if !mustCompact(t, s) {
		t.Fatal("the L0 compaction did not run")
	}
	// One L1 file, already over the budget. Compacting it alone would rewrite it
	// at level 2 without merging anything, and then again at level 3, forever.
	if ran := mustCompact(t, s); ran {
		t.Fatal("a single file was compacted into the next level, which merges nothing " +
			"and would repeat down the whole hierarchy")
	}
}

func TestDeeperLevelCompactsWhenOverBudget(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 2)
	opts.L1MaxBytes = 1 // any two L1 files exceed it
	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	// Two rounds of (2 flushes -> L0 compaction) leaves two L1 files.
	for round := 0; round < 2; round++ {
		flushWith(t, s, fmt.Sprintf("a%d", round), "v")
		flushWith(t, s, fmt.Sprintf("b%d", round), "v")
		if !mustCompact(t, s) {
			t.Fatalf("round %d: the L0 compaction did not run", round)
		}
	}
	levels := s.LevelSummary()
	if levels[1].Files != 2 {
		t.Fatalf("level 1 holds %d files, want 2: %+v", levels[1].Files, levels)
	}

	// Now level 1 is over budget with two files, so it merges into level 2.
	if !mustCompact(t, s) {
		t.Fatal("level 1 did not compact although it is over budget with two files")
	}
	files := s.SSTables()
	if len(files) != 1 || files[0].Level != 2 {
		t.Fatalf("after the L1 compaction: %+v, want one file at level 2", files)
	}
	for round := 0; round < 2; round++ {
		assertValue(t, s, fmt.Sprintf("a%d", round), "v")
		assertValue(t, s, fmt.Sprintf("b%d", round), "v")
	}
}

func TestBackgroundCompactionRunsWithoutBeingAsked(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, compactOpts(storage.DefaultMemTableSize, 3))
	defer func() { _ = s.Close() }()

	for i := 0; i < 6; i++ {
		flushWith(t, s, fmt.Sprintf("k%02d", i), "v")
	}

	waitFor(t, "the background compactor to reduce the file count", func() bool {
		for _, f := range s.SSTables() {
			if f.Level > 0 {
				return true
			}
		}
		return false
	})
	if err := s.CompactionError(); err != nil {
		t.Fatalf("background compaction failed: %v", err)
	}
	for i := 0; i < 6; i++ {
		assertValue(t, s, fmt.Sprintf("k%02d", i), "v")
	}
	t.Logf("levels after background compaction: %+v", s.LevelSummary())
}

// ---------------------------------------------------------------- version elimination

func TestCompactionDropsSupersededVersions(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, manualCompactOpts(storage.DefaultMemTableSize, 4))
	defer func() { _ = s.Close() }()

	// Four files, each a newer version of the same key.
	for i := 0; i < 4; i++ {
		flushWith(t, s, "k", fmt.Sprintf("v%d", i))
	}
	before := s.SSTables()
	var beforeEntries uint64
	for _, f := range before {
		beforeEntries += f.Entries
	}
	if beforeEntries != 4 {
		t.Fatalf("%d entries before compaction, want 4", beforeEntries)
	}

	if !mustCompact(t, s) {
		t.Fatal("compaction did not run")
	}

	after := s.SSTables()
	if len(after) != 1 || after[0].Entries != 1 {
		t.Fatalf("after compaction: %+v, want one file holding one entry", after)
	}
	assertValue(t, s, "k", "v3")

	st := s.CompactionStats()
	if st.VersionsDropped != 3 {
		t.Fatalf("VersionsDropped = %d, want 3", st.VersionsDropped)
	}
	if st.InputEntries != 4 || st.OutputEntries != 1 {
		t.Fatalf("stats = %+v, want 4 in and 1 out", st)
	}
}

// ---------------------------------------------------------------- INV-S3

// TestDeletedKeyNeverReappearsAfterCompaction is the INV-S3 sequence from the
// phase brief, exactly: put, flush, delete, flush, compact, restart.
func TestDeletedKeyNeverReappearsAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 2)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "value")
	mustFlush(t, s)
	mustDelete(t, s, "k")
	mustFlush(t, s)
	assertAbsent(t, s, "k")

	if !mustCompact(t, s) {
		t.Fatal("compaction did not run")
	}
	assertAbsent(t, s, "k")

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	assertAbsent(t, reopened, "k")

	// This compaction held every live file, so it was bottom-most: both the
	// tombstone and the value it hid could go, and the key is simply gone.
	if got := len(reopened.SSTables()); got != 0 {
		t.Logf("SSTables after a fully-collapsing compaction: %+v", reopened.SSTables())
		if got != 0 {
			t.Fatalf("%d SSTables remain, want 0: every entry was dropped", got)
		}
	}
}

// TestTombstoneIsNotDroppedWhileAnOlderFileCouldHoldTheValue is the regression
// test the phase brief asks for: it is built specifically to fail if tombstones
// are dropped too aggressively.
//
// The shape matters. The value ends up in a level-1 file, and the tombstone is
// then compacted from level 0 into level 1 WITHOUT that file being an input. If
// the compaction dropped the tombstone — because it was "a compaction", or
// because the code looked at levels rather than at what is actually older — the
// level-1 value would become the newest surviving version and the deleted key
// would come back.
func TestTombstoneIsNotDroppedWhileAnOlderFileCouldHoldTheValue(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 2)

	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	// Round 1: the value, plus a filler, compacted down to one level-1 file.
	mustPut(t, s, "k", "the old value")
	mustFlush(t, s)
	flushWith(t, s, "filler-a", "x")
	if !mustCompact(t, s) {
		t.Fatal("the first compaction did not run")
	}
	l1 := s.SSTables()
	if len(l1) != 1 || l1[0].Level != 1 {
		t.Fatalf("expected one level-1 file, got %+v", l1)
	}
	assertValue(t, s, "k", "the old value")

	// Round 2: the delete, plus a filler, at level 0.
	mustDelete(t, s, "k")
	mustFlush(t, s)
	flushWith(t, s, "filler-b", "x")

	statsBefore := s.CompactionStats()
	if !mustCompact(t, s) {
		t.Fatal("the second compaction did not run")
	}
	statsAfter := s.CompactionStats()

	// The tombstone must have been RETAINED: its input set did not include the
	// oldest file, so there was something underneath it that could still hold a
	// value for "k" — and there was.
	kept := statsAfter.TombstonesKept - statsBefore.TombstonesKept
	dropped := statsAfter.TombstonesDropped - statsBefore.TombstonesDropped
	if kept != 1 || dropped != 0 {
		t.Fatalf("the second compaction kept %d and dropped %d tombstones, want 1 and 0: "+
			"an older file still held a value for the deleted key", kept, dropped)
	}

	assertAbsent(t, s, "k")
	assertValue(t, s, "filler-a", "x")
	assertValue(t, s, "filler-b", "x")

	// And across a restart.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	assertAbsent(t, reopened, "k")
}

func TestTombstoneSurvivesManyCompactionGenerations(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 2)

	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	// An old value buried under a compaction, so it is never again an input to a
	// bottom-most compaction while the tombstone is being moved around.
	mustPut(t, s, "buried", "original")
	mustFlush(t, s)
	flushWith(t, s, "pad0", "x")
	mustCompact(t, s) // -> level 1 holds "buried"

	mustDelete(t, s, "buried")
	mustFlush(t, s)

	// Now push the tombstone through several generations of compaction.
	//
	// The compaction count is accumulated across generations rather than read at
	// the end, because CompactionStats is per store instance and resets on reopen.
	// Without that, this test would keep passing if it stopped compacting
	// altogether — which is the failure mode it exists to rule out.
	var totalRuns int64
	for gen := 0; gen < 6; gen++ {
		flushWith(t, s, fmt.Sprintf("pad%d", gen+1), "x")
		if _, err := s.CompactAll(); err != nil {
			t.Fatalf("generation %d: CompactAll: %v", gen, err)
		}
		assertAbsent(t, s, "buried")
		totalRuns += s.CompactionStats().Runs

		// And after a restart at every generation.
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s = openLSM(t, dir, opts)
		assertAbsent(t, s, "buried")
	}
	if totalRuns < 3 {
		t.Fatalf("only %d compactions ran across 6 generations; the tombstone was not "+
			"pushed through compaction at all", totalRuns)
	}
	t.Logf("%d compactions across 6 generations, levels %+v", totalRuns, s.LevelSummary())
}

func TestDeleteThenRewriteSurvivesCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 2)

	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	mustPut(t, s, "k", "first")
	mustFlush(t, s)
	mustDelete(t, s, "k")
	mustFlush(t, s)
	mustPut(t, s, "k", "second")
	mustFlush(t, s)
	mustCompactAll(t, s)

	assertValue(t, s, "k", "second")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	assertValue(t, reopened, "k", "second")
}

// TestCompactionThatDropsEverythingLeavesNoFile covers the outcome that is easy
// to get wrong by writing an empty SSTable: a bottom-most compaction of nothing
// but tombstones has no output at all, and the correct result is that the input
// files simply cease to exist.
func TestCompactionThatDropsEverythingLeavesNoFile(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 2)

	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	mustPut(t, s, "a", "v")
	mustPut(t, s, "b", "v")
	mustFlush(t, s)
	mustDelete(t, s, "a")
	mustDelete(t, s, "b")
	mustFlush(t, s)

	if !mustCompact(t, s) {
		t.Fatal("compaction did not run")
	}
	if got := len(s.SSTables()); got != 0 {
		t.Fatalf("%d SSTables remain, want 0: every entry was a droppable tombstone "+
			"or a value one hid", got)
	}
	if got := s.CompactionStats().EmptyOutputs; got != 1 {
		t.Fatalf("EmptyOutputs = %d, want 1", got)
	}
	// No empty SSTable was written.
	if files := sstFiles(t, dir); len(files) != 0 {
		t.Fatalf("files on disk = %v, want none", files)
	}
	assertAbsent(t, s, "a")
	assertAbsent(t, s, "b")

	// And the empty state survives a restart, which is where an empty SSTable
	// would have been refused.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	assertAbsent(t, reopened, "a")
	if got := reopened.Len(); got != 0 {
		t.Fatalf("recovered %d live keys, want 0", got)
	}
}

// ---------------------------------------------------------------- flush vs compaction

// TestFlushDuringCompactionIsNotLost is Part 18's requirement. A compaction
// publishes an edit that deletes its inputs and adds its output; a file that
// appeared while the merge was running must not be deleted, and must not vanish
// from the MANIFEST because the compaction published a file set computed before
// it existed.
func TestFlushDuringCompactionIsNotLost(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 4)

	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	for i := 0; i < 4; i++ {
		flushWith(t, s, fmt.Sprintf("old%d", i), "v")
	}

	// Run the compaction and a flush concurrently. The compaction snapshots the
	// four L0 files; the flush adds a fifth while it works.
	var wg sync.WaitGroup
	wg.Add(2)
	var compactErr, flushErr error
	go func() {
		defer wg.Done()
		_, compactErr = s.Compact()
	}()
	go func() {
		defer wg.Done()
		if err := s.Put(context.Background(), []byte("during"), []byte("v")); err != nil {
			flushErr = err
			return
		}
		flushErr = s.Flush()
	}()
	wg.Wait()
	if compactErr != nil {
		t.Fatalf("Compact: %v", compactErr)
	}
	if flushErr != nil {
		t.Fatalf("concurrent flush: %v", flushErr)
	}

	// Everything is readable, including the key written during the compaction.
	for i := 0; i < 4; i++ {
		assertValue(t, s, fmt.Sprintf("old%d", i), "v")
	}
	assertValue(t, s, "during", "v")

	// And after a restart, so the MANIFEST really carries both.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	for i := 0; i < 4; i++ {
		assertValue(t, reopened, fmt.Sprintf("old%d", i), "v")
	}
	assertValue(t, reopened, "during", "v")
}

// ---------------------------------------------------------------- restart

func TestCompactedStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(2048, 3)

	reference := map[string]string{}
	s := openLSM(t, dir, opts)
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 400; i++ {
		k := fmt.Sprintf("key%03d", rng.Intn(60))
		if rng.Intn(4) == 0 {
			mustDelete(t, s, k)
			delete(reference, k)
		} else {
			v := fmt.Sprintf("v%d", i)
			mustPut(t, s, k, v)
			reference[k] = v
		}
		if i%50 == 49 {
			mustFlush(t, s)
			mustCompactAll(t, s)
		}
	}
	mustFlush(t, s)
	mustCompactAll(t, s)

	snapBefore, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertSameState(t, reference, snapBefore)
	levelsBefore := s.LevelSummary()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	snapAfter, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertSameState(t, reference, snapAfter)
	assertSameBytes(t, snapBefore, snapAfter, "after restart")

	// The MANIFEST carried the level assignment, not just the file names.
	if got := reopened.LevelSummary(); fmt.Sprint(got) != fmt.Sprint(levelsBefore) {
		t.Fatalf("levels after restart = %+v, want %+v", got, levelsBefore)
	}
	t.Logf("%d live keys across %+v", len(snapAfter), reopened.LevelSummary())
}

func TestManyCompactionGenerationsWithRestarts(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(1024, 3)

	reference := map[string]string{}
	for session := 0; session < 5; session++ {
		s := openLSM(t, dir, opts)
		for i := 0; i < 60; i++ {
			k := fmt.Sprintf("key%02d", (session*60+i)%40)
			if i%7 == 6 {
				mustDelete(t, s, k)
				delete(reference, k)
			} else {
				v := fmt.Sprintf("s%d-%d", session, i)
				mustPut(t, s, k, v)
				reference[k] = v
			}
		}
		mustFlush(t, s)
		n := mustCompactAll(t, s)
		t.Logf("session %d: %d compactions, levels %+v", session, n, s.LevelSummary())

		got, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		assertSameState(t, reference, got)
		if err := s.Close(); err != nil {
			t.Fatalf("session %d: Close: %v", session, err)
		}
	}

	final := openLSM(t, dir, opts)
	defer func() { _ = final.Close() }()
	got, err := final.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertSameState(t, reference, got)
}

// ---------------------------------------------------------------- MANIFEST

func TestManifestRecordsTheLiveFileSet(t *testing.T) {
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 4)

	s := openLSM(t, dir, opts)
	for i := 0; i < 4; i++ {
		flushWith(t, s, fmt.Sprintf("k%d", i), "v")
	}
	mustCompact(t, s)
	want := s.SSTables()
	if len(want) != 1 {
		t.Fatalf("expected one file after compaction, got %+v", want)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The MANIFEST names exactly the surviving file.
	state, _, err := manifest.Recover(dir)
	if err != nil {
		t.Fatalf("manifest.Recover: %v", err)
	}
	if len(state.Files) != 1 || state.Files[0].Num != want[0].Number {
		t.Fatalf("the MANIFEST names %+v, want only file %d", state.Files, want[0].Number)
	}
	if state.Files[0].Level != 1 {
		t.Fatalf("the MANIFEST records level %d, want 1", state.Files[0].Level)
	}

	// And the compaction's inputs are gone from disk.
	if files := sstFiles(t, dir); len(files) != 1 {
		t.Fatalf("files on disk = %v, want only the compaction output", files)
	}
}

func TestOrphanSSTableIsDeletedAtStartup(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "v")
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// A complete-looking file the MANIFEST does not name: exactly what a crash
	// between a compaction's rename and its MANIFEST append leaves behind. It is
	// not part of the database, whatever it contains.
	orphan := filepath.Join(dir, "000900.sst")
	src, err := os.ReadFile(filepath.Join(dir, "000001.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan, src, 0o644); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()

	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("an unreferenced SSTable survived startup; the MANIFEST is supposed to be " +
			"the only authority on which files are live")
	}
	if got := reopened.Recovery().OrphansRemoved; got != 1 {
		t.Fatalf("OrphansRemoved = %d, want 1", got)
	}
	assertValue(t, reopened, "k", "v")
}

func TestTempFileIsDeletedAtStartup(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "v")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, "000007.sst.tmp")
	if err := os.WriteFile(tmp, []byte("half a table"), 0o644); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s survived startup", tmp)
	}
	if got := reopened.Recovery().TempFilesRemoved; got != 1 {
		t.Fatalf("TempFilesRemoved = %d, want 1", got)
	}
	assertValue(t, reopened, "k", "v")
}

func TestReferencedButMissingSSTableIsFatal(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "v")
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(dir, "000001.sst")); err != nil {
		t.Fatal(err)
	}
	_, err := storage.OpenLSMStore(dir, opts)
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("opening with a referenced file missing = %v, want ErrCorrupt: the "+
			"publication protocol never removes a referenced file, so this is not a state "+
			"a crash can produce", err)
	}
}

func TestSSTablesWithoutACurrentFileAreRefused(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "v")
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Lose CURRENT. The directory still holds a perfectly good SSTable, and that
	// is precisely why it must be refused: nothing distinguishes this from a
	// Phase 3 directory, and silently falling back to reading the directory would
	// make the MANIFEST advisory rather than authoritative.
	if err := os.Remove(filepath.Join(dir, manifest.CurrentName)); err != nil {
		t.Fatal(err)
	}
	_, err := storage.OpenLSMStore(dir, opts)
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("opening SSTables with no CURRENT = %v, want ErrCorrupt", err)
	}

	// With the explicit opt-in it opens, and the data is intact.
	adopting := opts
	adopting.AdoptLegacySSTables = true
	adopted := openLSM(t, dir, adopting)
	defer func() { _ = adopted.Close() }()
	assertValue(t, adopted, "k", "v")
	if got := adopted.Recovery().AdoptedLegacy; got != 1 {
		t.Fatalf("AdoptedLegacy = %d, want 1", got)
	}
	if !adopted.Recovery().Bootstrapped {
		t.Fatal("Bootstrapped is false after adopting a legacy directory")
	}
}

func TestEmptyDirectoryBootstrapsAManifest(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = s.Close() }()

	if !s.Recovery().Bootstrapped {
		t.Fatal("opening an empty directory did not report bootstrapping a MANIFEST")
	}
	if _, err := os.Stat(filepath.Join(dir, manifest.CurrentName)); err != nil {
		t.Fatalf("CURRENT was not created: %v", err)
	}
	mustPut(t, s, "k", "v")
	assertValue(t, s, "k", "v")
}

// TestManifestIsReinstalledOnEveryOpen: a manifest that grew with every edit for
// the life of the database would make recovery time grow with total history
// rather than with current state.
func TestManifestIsReinstalledOnEveryOpen(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	var last uint64
	for gen := 0; gen < 4; gen++ {
		s := openLSM(t, dir, opts)
		num := s.ManifestNumber()
		if num <= last {
			t.Fatalf("generation %d installed manifest %d, which is not newer than %d",
				gen, num, last)
		}
		last = num
		flushWith(t, s, fmt.Sprintf("k%d", gen), "v")
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}

		// Exactly one manifest survives: the live one.
		nums, err := manifest.List(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(nums) != 1 || nums[0] != num {
			t.Fatalf("generation %d left manifests %v, want only [%d]", gen, nums, num)
		}
	}

	final := openLSM(t, dir, opts)
	defer func() { _ = final.Close() }()
	for gen := 0; gen < 4; gen++ {
		assertValue(t, final, fmt.Sprintf("k%d", gen), "v")
	}
	// The point: recovery replays a snapshot plus only the previous session's
	// edits, never the whole history. Four generations each did one flush; a
	// manifest that accumulated would be replaying at least four edits by now.
	if got := final.Recovery().ManifestEdits; got != 2 {
		t.Fatalf("ManifestEdits = %d, want 2 (the snapshot plus the last session's one "+
			"flush); a growing number here would mean manifest length tracks total "+
			"history rather than current state", got)
	}
}

func TestCorruptManifestIsRefused(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 3; i++ {
		flushWith(t, s, fmt.Sprintf("k%d", i), "v")
	}
	num := s.ManifestNumber()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, manifest.Name(num))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		edit func([]byte) []byte
	}{
		{"a byte inside the first record", func(b []byte) []byte {
			out := append([]byte(nil), b...)
			out[12] ^= 0xff
			return out
		}},
		{"emptied", func(b []byte) []byte { return nil }},
		{"a stray byte at the front", func(b []byte) []byte {
			return append([]byte{0x01}, b...)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, tc.edit(raw), 0o644); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.WriteFile(path, raw, 0o644) })

			got, err := storage.OpenLSMStore(dir, opts)
			if err == nil {
				_ = got.Close()
				t.Fatal("a damaged MANIFEST was accepted; the file set would be whatever " +
					"survived, which is the guessing the MANIFEST exists to prevent")
			}
			if !errors.Is(err, storage.ErrCorrupt) {
				t.Fatalf("got %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestStartupDoesNotFullyScanByDefault(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 200; i++ {
		mustPut(t, s, fmt.Sprintf("key%04d", i), "value")
	}
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	plain := openLSM(t, dir, opts)
	if plain.Recovery().FullyVerified {
		t.Fatal("the default configuration fully verified every SSTable; the MANIFEST " +
			"records what Phase 3 had to read the file to learn")
	}
	// No data block was read at startup: the footer, filter and index were, but
	// nothing else.
	if got := plain.ReadCounters().BlockReads; got != 0 {
		t.Fatalf("startup read %d data blocks, want 0", got)
	}
	assertValue(t, plain, "key0000", "value")
	if err := plain.Close(); err != nil {
		t.Fatal(err)
	}

	verifying := opts
	verifying.VerifySSTablesOnOpen = true
	strict := openLSM(t, dir, verifying)
	defer func() { _ = strict.Close() }()
	if !strict.Recovery().FullyVerified {
		t.Fatal("VerifySSTablesOnOpen did not fully verify")
	}
	if got := strict.ReadCounters().BlockReads; got == 0 {
		t.Fatal("VerifySSTablesOnOpen read no data blocks")
	}
}

// TestManifestDisagreeingWithAFileIsRefused covers the cross-check that makes
// skipping the full scan safe: the MANIFEST and the file's own footer record the
// entry count and size independently, so a disagreement is caught cheaply.
func TestManifestDisagreeingWithAFileIsRefused(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 20; i++ {
		mustPut(t, s, fmt.Sprintf("k%02d", i), "v")
	}
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a byte to the SSTable: its footer is now at the wrong offset, so the
	// size the MANIFEST recorded no longer matches.
	f, err := os.OpenFile(filepath.Join(dir, "000001.sst"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := storage.OpenLSMStore(dir, opts); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("opening with a file that disagrees with the MANIFEST = %v, want ErrCorrupt", err)
	}
}

// ---------------------------------------------------------------- concurrency

// TestReadsDuringCompactionSeeACoherentFileSet is INV-S5 for compaction. Readers
// hammer a key set while compaction repeatedly replaces the files holding it. A
// reader that fell between "the inputs are gone" and "the output is live" would
// see ErrNotFound for a key that has existed continuously.
func TestReadsDuringCompactionSeeACoherentFileSet(t *testing.T) {
	const (
		keys    = 60
		readers = 8
	)
	dir := t.TempDir()
	s := openLSM(t, dir, manualCompactOpts(storage.DefaultMemTableSize, 2))
	defer func() { _ = s.Close() }()

	for i := 0; i < keys; i++ {
		mustPut(t, s, fmt.Sprintf("key%02d", i), "stable")
	}
	mustFlush(t, s)

	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		failures = make(chan string, 64)
	)

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < keys; i++ {
					k := fmt.Sprintf("key%02d", i)
					got, err := s.Get(ctx, []byte(k))
					if err != nil {
						select {
						case failures <- fmt.Sprintf("Get(%s) during compaction: %v "+
							"(this key has existed continuously)", k, err):
						default:
						}
						return
					}
					if string(got) != "stable" {
						select {
						case failures <- fmt.Sprintf("Get(%s) = %q, want \"stable\"", k, got):
						default:
						}
						return
					}
				}
			}
		}()
	}

	// Compact repeatedly while the readers run, adding a file each round so there
	// is always something to merge.
	for round := 0; round < 12; round++ {
		if err := s.Put(context.Background(), []byte(fmt.Sprintf("pad%02d", round)), []byte("v")); err != nil {
			t.Fatalf("round %d: Put: %v", round, err)
		}
		if err := s.Flush(); err != nil {
			t.Fatalf("round %d: Flush: %v", round, err)
		}
		if _, err := s.CompactAll(); err != nil {
			t.Fatalf("round %d: CompactAll: %v", round, err)
		}
	}
	close(stop)
	wg.Wait()

	select {
	case msg := <-failures:
		t.Fatal(msg)
	default:
	}

	if got := s.CompactionStats().Runs; got < 5 {
		t.Fatalf("only %d compactions ran; the test did not exercise publication", got)
	}
	t.Logf("%d readers ran across %d compactions", readers, s.CompactionStats().Runs)
}

// TestWritesAndFlushesDuringCompaction runs the whole engine at once: writers,
// readers, flushes and compactions, with the logical state checked at the end
// against what was written.
func TestWritesAndFlushesDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, compactOpts(4096, 3))
	defer func() { _ = s.Close() }()

	const (
		writers = 4
		perW    = 150
	)
	var wg sync.WaitGroup
	errs := make(chan error, writers+8)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < perW; i++ {
				k := fmt.Sprintf("w%d-k%03d", w, i)
				if err := s.Put(ctx, []byte(k), []byte(fmt.Sprintf("v%d", i))); err != nil {
					errs <- fmt.Errorf("Put(%s): %w", k, err)
					return
				}
			}
		}(w)
	}
	// A reader and a flusher alongside.
	wg.Add(2)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		for i := 0; i < 400; i++ {
			if _, err := s.Get(ctx, []byte("w0-k000")); err != nil && !errors.Is(err, storage.ErrNotFound) {
				errs <- fmt.Errorf("Get during compaction: %w", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := s.Flush(); err != nil {
				errs <- fmt.Errorf("Flush: %w", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if err := s.CompactionError(); err != nil {
		t.Fatalf("background compaction failed: %v", err)
	}

	// Every write is present.
	for w := 0; w < writers; w++ {
		for i := 0; i < perW; i++ {
			assertValue(t, s, fmt.Sprintf("w%d-k%03d", w, i), fmt.Sprintf("v%d", i))
		}
	}
	t.Logf("levels: %+v, compactions: %d", s.LevelSummary(), s.CompactionStats().Runs)
}

func TestCloseDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, compactOpts(2048, 2))

	for i := 0; i < 40; i++ {
		mustPut(t, s, fmt.Sprintf("key%03d", i), "value-with-some-padding-to-fill-the-memtable")
	}
	mustFlush(t, s)

	// Close while the background compactor is very likely mid-run. It must neither
	// panic nor race, and must leave a directory that reopens.
	if err := s.Close(); err != nil {
		t.Fatalf("Close during compaction: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	reopened := openLSM(t, dir, compactOpts(2048, 2))
	defer func() { _ = reopened.Close() }()
	for i := 0; i < 40; i++ {
		assertValue(t, reopened, fmt.Sprintf("key%03d", i),
			"value-with-some-padding-to-fill-the-memtable")
	}
}

func TestOperationsAfterCloseAreRefused(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, compactOpts(storage.DefaultMemTableSize, 2))
	mustPut(t, s, "k", "v")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("Compact after Close = %v, want ErrClosed", err)
	}
	if _, err := s.CompactAll(); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("CompactAll after Close = %v, want ErrClosed", err)
	}
}

// ---------------------------------------------------------------- differential

// TestAgainstReferenceModelWithCompaction is Part 20: the Phase 3 differential
// test extended with flushes, automatic compaction, manual compaction and
// restarts in the generated operation stream.
func TestAgainstReferenceModelWithCompaction(t *testing.T) {
	for _, tc := range []struct {
		name         string
		memTableSize int64
		l0Trigger    int
		ops          int
		keyspace     int
	}{
		{"flush every write", 1, 2, 250, 30},
		{"small memtable", 2048, 3, 2000, 100},
		{"one memtable", storage.DefaultMemTableSize, 4, 1200, 150},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := manualCompactOpts(tc.memTableSize, tc.l0Trigger)

			s := openLSM(t, dir, opts)
			ref := reference{}
			rng := rand.New(rand.NewSource(int64(len(tc.name)) * 31))
			ctx := context.Background()
			// Accumulated across the restarts the op stream performs, because
			// CompactionStats resets with each new store instance.
			var totalRuns int64

			for i := 0; i < tc.ops; i++ {
				key := fmt.Sprintf("key%04d", rng.Intn(tc.keyspace))
				switch n := rng.Intn(20); {
				case n < 11: // put
					value := fmt.Sprintf("v%d-%d", i, rng.Intn(1000))
					if rng.Intn(20) == 0 {
						value = ""
					}
					if err := s.Put(ctx, []byte(key), []byte(value)); err != nil {
						t.Fatalf("op %d: Put(%q): %v", i, key, err)
					}
					ref.put(key, value)
				case n < 15: // delete
					if err := s.Delete(ctx, []byte(key)); err != nil {
						t.Fatalf("op %d: Delete(%q): %v", i, key, err)
					}
					ref.delete(key)
				case n < 17: // read back, compared below
				case n < 18: // flush
					if err := s.Flush(); err != nil {
						t.Fatalf("op %d: Flush: %v", i, err)
					}
				case n < 19: // compact
					if _, err := s.CompactAll(); err != nil {
						t.Fatalf("op %d: CompactAll: %v", i, err)
					}
				default: // restart
					totalRuns += s.CompactionStats().Runs
					if err := s.Close(); err != nil {
						t.Fatalf("op %d: Close: %v", i, err)
					}
					s = openLSM(t, dir, opts)
				}

				assertMatchesReference(t, s, ref, key, i)
				if i%250 == 0 {
					for k := 0; k < tc.keyspace; k++ {
						assertMatchesReference(t, s, ref, fmt.Sprintf("key%04d", k), i)
					}
				}
			}

			// Compact everything, then compare the whole keyspace.
			if _, err := s.CompactAll(); err != nil {
				t.Fatalf("final CompactAll: %v", err)
			}
			for k := 0; k < tc.keyspace; k++ {
				assertMatchesReference(t, s, ref, fmt.Sprintf("key%04d", k), -1)
			}
			if got := s.Len(); got != len(ref) {
				t.Fatalf("store holds %d live keys, reference holds %d", got, len(ref))
			}
			stats := s.CompactionStats()
			totalRuns += stats.Runs
			if totalRuns == 0 {
				t.Fatal("no compaction ran; this variant is not testing compaction")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			reopened := openLSM(t, dir, opts)
			defer func() { _ = reopened.Close() }()
			for k := 0; k < tc.keyspace; k++ {
				assertMatchesReference(t, reopened, ref, fmt.Sprintf("key%04d", k), -1)
			}
			snap, err := reopened.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			assertSameState(t, ref, snap)
			t.Logf("%d ops, %d compactions (all sessions), %d versions dropped, %d tombstones dropped, "+
				"%d kept, levels %+v", tc.ops, totalRuns, stats.VersionsDropped,
				stats.TombstonesDropped, stats.TombstonesKept, reopened.LevelSummary())
		})
	}
}

// ---------------------------------------------------------------- bloom at the store

func TestFilterSkipsFilesForAbsentKeys(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	// Several files, each holding a disjoint key range.
	for f := 0; f < 6; f++ {
		for i := 0; i < 50; i++ {
			mustPut(t, s, fmt.Sprintf("file%d-key%03d", f, i), "v")
		}
		mustFlush(t, s)
	}
	if got := len(s.SSTables()); got != 6 {
		t.Fatalf("%d SSTables, want 6", got)
	}
	for _, f := range s.SSTables() {
		if !f.HasFilter {
			t.Fatalf("file %d carries no filter", f.Number)
		}
	}

	before := s.ReadCounters()
	const probes = 500
	for i := 0; i < probes; i++ {
		assertAbsent(t, s, fmt.Sprintf("definitely-absent-%04d", i))
	}
	after := s.ReadCounters()

	skips := after.FilterSkips - before.FilterSkips
	reads := after.BlockReads - before.BlockReads
	t.Logf("%d absent lookups across 6 files: %d file-skips by filter, %d data blocks read",
		probes, skips, reads)

	// Without filters each lookup would consult all six files. With them, almost
	// every file is eliminated without any block I/O.
	if skips < uint64(probes*6)*9/10 {
		t.Fatalf("filters skipped %d of a possible %d file consultations; "+
			"the read path is not using them", skips, probes*6)
	}

	// And present keys are still all found.
	for f := 0; f < 6; f++ {
		for i := 0; i < 50; i += 7 {
			assertValue(t, s, fmt.Sprintf("file%d-key%03d", f, i), "v")
		}
	}
}

func TestDisabledFilterStillServesEveryKey(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)
	opts.DisableBloomFilter = true

	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	for i := 0; i < 200; i++ {
		mustPut(t, s, fmt.Sprintf("key%04d", i), "v")
	}
	mustFlush(t, s)
	for _, f := range s.SSTables() {
		if f.HasFilter {
			t.Fatalf("file %d carries a filter although filters are disabled", f.Number)
		}
	}
	for i := 0; i < 200; i++ {
		assertValue(t, s, fmt.Sprintf("key%04d", i), "v")
	}
	assertAbsent(t, s, "nope")
	if got := s.ReadCounters().FilterSkips; got != 0 {
		t.Fatalf("FilterSkips = %d with filters disabled, want 0", got)
	}
}

// TestMixedFilterAndFilterlessFilesResolveCorrectly: a database written across
// an upgrade has files of both kinds, and every key must still resolve.
func TestMixedFilterAndFilterlessFilesResolveCorrectly(t *testing.T) {
	dir := t.TempDir()

	withoutFilter := lsmOpts(storage.DefaultMemTableSize)
	withoutFilter.DisableBloomFilter = true
	s := openLSM(t, dir, withoutFilter)
	for i := 0; i < 50; i++ {
		mustPut(t, s, fmt.Sprintf("old%03d", i), "old")
	}
	mustPut(t, s, "shared", "old-value")
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// A trigger of two, so the compaction below actually has work to do: the
	// point of this test is what a merge across a filterless and a filtered file
	// produces.
	withFilter := manualCompactOpts(storage.DefaultMemTableSize, 2)
	s2 := openLSM(t, dir, withFilter)
	defer func() { _ = s2.Close() }()
	for i := 0; i < 50; i++ {
		mustPut(t, s2, fmt.Sprintf("new%03d", i), "new")
	}
	mustPut(t, s2, "shared", "new-value")
	mustDelete(t, s2, "old000")
	mustFlush(t, s2)

	files := s2.SSTables()
	if len(files) != 2 {
		t.Fatalf("%d files, want 2", len(files))
	}
	if files[0].HasFilter || !files[1].HasFilter {
		t.Fatalf("expected the older file to have no filter and the newer one to have one: %+v", files)
	}

	for i := 1; i < 50; i++ {
		assertValue(t, s2, fmt.Sprintf("old%03d", i), "old")
	}
	for i := 0; i < 50; i++ {
		assertValue(t, s2, fmt.Sprintf("new%03d", i), "new")
	}
	assertValue(t, s2, "shared", "new-value")
	assertAbsent(t, s2, "old000")

	// A compaction across the two produces one file that does have a filter.
	if n, err := s2.CompactAll(); err != nil {
		t.Fatalf("CompactAll: %v", err)
	} else if n == 0 {
		t.Fatal("no compaction ran; the mixed-file merge is not being exercised")
	}
	if got := len(s2.SSTables()); got != 1 {
		t.Fatalf("%d files after compaction, want 1", got)
	}
	for _, f := range s2.SSTables() {
		if !f.HasFilter {
			t.Fatalf("the compaction output %d carries no filter", f.Number)
		}
	}
	assertValue(t, s2, "shared", "new-value")
	assertAbsent(t, s2, "old000")
}
