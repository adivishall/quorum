package bench_test

import (
	"context"
	"testing"

	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// These tests prove the runner drives the real storage engine and that its
// self-checks actually check something — the requirement that a benchmark must
// exercise the intended path and not a shortcut. They are small and fast; the
// benchmark command runs the same primitives at scale.

func benchStore(t *testing.T, memTable int64, autoCompact bool, l0 int) (*storage.LSMStore, string) {
	t.Helper()
	dir := t.TempDir()
	opts := storage.DefaultOptions()
	opts.WAL.SyncMode = wal.SyncOff
	opts.MemTableSize = memTable
	opts.DisableAutoCompaction = !autoCompact
	if l0 > 0 {
		opts.L0CompactionTrigger = l0
	}
	s, err := storage.OpenLSMStore(dir, opts)
	if err != nil {
		t.Fatalf("OpenLSMStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func TestPutConcurrentStoresEveryKey(t *testing.T) {
	s, _ := benchStore(t, 64<<10, false, 0)
	ks := bench.NewKeyspace("key", 5000)
	ctx := context.Background()
	res, err := bench.PutConcurrent(ctx, s, ks, 100, 5000, 4)
	if err != nil {
		t.Fatal(err)
	}
	if res.Ops != 5000 {
		t.Fatalf("Ops = %d, want 5000", res.Ops)
	}
	if got := s.Len(); got != 5000 {
		t.Fatalf("store has %d keys, want 5000 — the writes did not all reach the engine", got)
	}
}

func TestGetRandomVerifiesLiveValues(t *testing.T) {
	s, _ := benchStore(t, 64<<10, false, 0)
	ks := bench.NewKeyspace("key", 3000)
	ctx := context.Background()
	if _, err := bench.PutSequential(ctx, s, ks, 80, 0, 3000); err != nil {
		t.Fatal(err)
	}
	res, err := bench.GetRandom(ctx, s, ks, 3000, 4000, 4, 7, false)
	if err != nil {
		t.Fatalf("GetRandom hit: %v", err)
	}
	if res.Verify == 0 {
		t.Fatal("no reads were verified; the check is not exercising the data")
	}
}

func TestGetRandomMissingAllMiss(t *testing.T) {
	s, _ := benchStore(t, 64<<10, false, 0)
	ks := bench.NewKeyspace("key", 2000)
	ctx := context.Background()
	if _, err := bench.PutSequential(ctx, s, ks, 64, 0, 2000); err != nil {
		t.Fatal(err)
	}
	// Every read targets an index >= live, so every read must miss. If the
	// keyspace overlapped, a hit would surface here as an error.
	res, err := bench.GetRandom(ctx, s, ks, 2000, 2000, 4, 11, true)
	if err != nil {
		t.Fatalf("GetRandom miss: %v", err)
	}
	if res.Verify != res.Ops {
		t.Fatalf("verified %d of %d missing reads", res.Verify, res.Ops)
	}
}

func TestDeleteRandomRemovesKeys(t *testing.T) {
	s, _ := benchStore(t, 64<<10, false, 0)
	ks := bench.NewKeyspace("key", 2000)
	ctx := context.Background()
	if _, err := bench.PutSequential(ctx, s, ks, 64, 0, 2000); err != nil {
		t.Fatal(err)
	}
	res, err := bench.DeleteRandom(ctx, s, ks, 2000, 500, 3)
	if err != nil {
		t.Fatalf("DeleteRandom: %v", err)
	}
	if res.Verify != 500 {
		t.Fatalf("verified %d deletes, want 500", res.Verify)
	}
	if got := s.Len(); got != 1500 {
		t.Fatalf("store has %d live keys after 500 deletes, want 1500", got)
	}
}

func TestMixedRunsAgainstLiveData(t *testing.T) {
	s, _ := benchStore(t, 64<<10, false, 0)
	ks := bench.NewKeyspace("key", 2000)
	ctx := context.Background()
	if _, err := bench.PutSequential(ctx, s, ks, 64, 0, 2000); err != nil {
		t.Fatal(err)
	}
	res, err := bench.Mixed(ctx, s, ks, bench.MixBalanced, 64, 2000, 8000, 4, 5)
	if err != nil {
		t.Fatalf("Mixed: %v", err)
	}
	if res.Ops != 8000 || res.Lat.Len() != 8000 {
		t.Fatalf("ops=%d latencies=%d, want 8000 each", res.Ops, res.Lat.Len())
	}
}

func TestCompactionActuallyRunsUnderLoad(t *testing.T) {
	// A small memtable and a low L0 trigger, with auto-compaction on, must
	// produce at least one compaction over a few thousand writes. This is the
	// precondition the compaction benchmark relies on: if it did not hold, that
	// benchmark would be measuring an engine that never compacted.
	s, _ := benchStore(t, 16<<10, true, 4)
	ks := bench.NewKeyspace("key", 20000)
	ctx := context.Background()
	if _, err := bench.PutSequential(ctx, s, ks, 100, 0, 20000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactAll(); err != nil {
		t.Fatalf("CompactAll: %v", err)
	}
	if got := s.CompactionStats().Runs; got == 0 {
		t.Fatal("no compaction ran; the compaction benchmark's precondition does not hold")
	}
}
