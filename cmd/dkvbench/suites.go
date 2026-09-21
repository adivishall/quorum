package main

import (
	"context"
	"fmt"
	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
	"math/rand"
)

var ctx = context.Background()

// budgetBytes caps how much a value-scaled benchmark writes, so a large-value
// run does not need gigabytes. The dataset for a value size is min(requested,
// budget/valueSize).
const budgetBytes = 256 << 20

func (h *harness) nForValue(base, valueSize int) int {
	n := budgetBytes / (valueSize + 24)
	if n < base {
		return n
	}
	return base
}

// populate writes indices [0,n) sequentially. Used to build a dataset a read or
// mixed benchmark then measures against.
func (h *harness) populate(s storage.Store, ks bench.Keyspace, valueSize, n int) error {
	_, err := bench.PutSequential(ctx, s, ks, valueSize, 0, n)
	return err
}

// populateShuffled writes indices [0,n) in a seeded random order, so that every
// flushed SSTable spans the whole keyspace and a point read must consider every
// file. This is the layout that makes the Bloom filter's file-skipping visible;
// sequential writes give each file a disjoint range that a cheap range check
// already excludes.
func (h *harness) populateShuffled(s storage.Store, ks bench.Keyspace, valueSize, n int, seed int64) error {
	perm := rand.New(rand.NewSource(seed)).Perm(n)
	val := make([]byte, valueSize)
	var key []byte
	for _, idx := range perm {
		key = ks.AppendKey(key[:0], idx)
		bench.FillValue(val, idx)
		if err := s.Put(ctx, key, val); err != nil {
			return err
		}
	}
	return nil
}

// freshRun opens a fresh store, runs one phase against it, snapshots the storage
// counters and closes. It is the fresh-database path: every run starts empty, so
// nothing carries over between runs.
func (h *harness) freshRun(name string, sp storeSpec, runIdx int, phase func(s *storage.LSMStore) (bench.PhaseResult, error)) (bench.PhaseResult, bench.StorageMetrics, error) {
	s, dir, err := h.openStore(fmt.Sprintf("%s-r%d", name, runIdx), sp)
	if err != nil {
		return bench.PhaseResult{}, bench.StorageMetrics{}, err
	}
	defer func() { _ = s.Close() }()
	pr, err := phase(s)
	if err != nil {
		return bench.PhaseResult{}, bench.StorageMetrics{}, err
	}
	sm := bench.SnapshotStorage(s, dir)
	return pr, sm, nil
}

// suitePut measures write throughput: sequential single-writer at three value
// sizes, then the same 100-byte workload at increasing writer counts. The store
// runs its real default configuration (4 MiB memtable, batch sync, compaction
// on); each run starts from an empty directory.
func (h *harness) suitePut() error {
	h.section("PUT throughput (§3.1)")
	sp := defaultSpec()

	for _, vs := range []int{100, 1024, 16384} {
		n := h.nForValue(h.dataset, vs)
		ks := bench.NewKeyspace("key", n)
		cfg := sp.config(n, ks.KeyBytes(), vs, 1, "put-sequential", "", h.onTmpfs)
		label := fmt.Sprintf("put seq   value=%-6s n=%d", byteLabel(vs), n)
		if _, err := h.runRepeated(label, cfg, "fresh DB, single writer, ascending keys", func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
			pr, sm, err := h.freshRun("put-seq", sp, runIdx, func(s *storage.LSMStore) (bench.PhaseResult, error) {
				return bench.PutSequential(ctx, s, ks, vs, 0, n)
			})
			return pr.Result(label), sm, err
		}); err != nil {
			return err
		}
	}

	const vs = 100
	n := h.nForValue(h.dataset, vs)
	ks := bench.NewKeyspace("key", n)
	for _, workers := range []int{1, 2, 4, 8} {
		cfg := sp.config(n, ks.KeyBytes(), vs, workers, "put-concurrent", "", h.onTmpfs)
		label := fmt.Sprintf("put conc   workers=%d  n=%d", workers, n)
		if _, err := h.runRepeated(label, cfg, "fresh DB, distinct keys per worker", func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
			pr, sm, err := h.freshRun("put-conc", sp, runIdx, func(s *storage.LSMStore) (bench.PhaseResult, error) {
				return bench.PutConcurrent(ctx, s, ks, vs, n, workers)
			})
			return pr.Result(label), sm, err
		}); err != nil {
			return err
		}
	}
	return nil
}

// suiteGet measures read throughput and latency against a warm, fully compacted
// dataset — hits, then misses. The dataset is built once and reused across runs
// with a warmup pass, so the numbers are steady-state reads, not a cold build.
func (h *harness) suiteGet() error {
	h.section("GET throughput (§3.2)")
	sp := defaultSpec()
	sp.sync = wal.SyncOff // reads do not touch the WAL; off makes the build quick
	sp.memtable = 1 << 20 // several SSTables during the build, then compacted
	n := h.dataset
	ks := bench.NewKeyspace("key", n)

	s, dir, err := h.openStore("get", sp)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	if err := h.populate(s, ks, h.valueSize, n); err != nil {
		return err
	}
	if err := s.Flush(); err != nil {
		return err
	}
	if _, err := s.CompactAll(); err != nil {
		return err
	}
	// Warmup: one unmeasured read pass to warm the page cache and reader state.
	if _, err := bench.GetRandom(ctx, s, ks, n, n/10+1, h.concurrency, h.seed, false); err != nil {
		return err
	}
	settled := fmt.Sprintf("warm reused DB, %d SSTable(s) after CompactAll", len(s.SSTables()))

	for _, missing := range []bool{false, true} {
		kind := "hit"
		if missing {
			kind = "miss"
		}
		label := fmt.Sprintf("get %-4s  conc=%d", kind, h.concurrency)
		cfg := sp.config(n, ks.KeyBytes(), h.valueSize, h.concurrency, "get-"+kind, "", h.onTmpfs)
		if _, err := h.runRepeated(label, cfg, settled, func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
			pr, err := bench.GetRandom(ctx, s, ks, n, n, h.concurrency, seed, missing)
			if err != nil {
				return bench.Result{}, bench.StorageMetrics{}, err
			}
			return pr.Result(label), bench.SnapshotStorage(s, dir), nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// suiteDelete measures delete throughput and latency on a populated dataset.
// Each run rebuilds the dataset and deletes half of it (distinct keys, so half
// the keyspace becomes tombstones), verifying the keys read back absent.
func (h *harness) suiteDelete() error {
	h.section("DELETE throughput (§3.3)")
	sp := defaultSpec()
	sp.sync = wal.SyncBatch
	sp.memtable = 1 << 20
	n := h.dataset
	if n > 100_000 {
		n = 100_000
	}
	ndel := n / 2
	ks := bench.NewKeyspace("key", n)
	cfg := sp.config(n, ks.KeyBytes(), h.valueSize, 1, "delete", "", h.onTmpfs)
	label := fmt.Sprintf("delete    n=%d of %d", ndel, n)
	_, err := h.runRepeated(label, cfg, "fresh DB rebuilt each run; deletes verified absent", func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
		pr, sm, err := h.freshRun("delete", sp, runIdx, func(s *storage.LSMStore) (bench.PhaseResult, error) {
			if err := h.populate(s, ks, h.valueSize, n); err != nil {
				return bench.PhaseResult{}, err
			}
			return bench.DeleteRandom(ctx, s, ks, n, ndel, seed)
		})
		return pr.Result(label), sm, err
	})
	return err
}

// suiteMixed runs the documented read/write/delete mixes against a warm dataset.
// Reads target live keys, writes overwrite them, deletes remove them; latency
// covers every operation, which is the tail a client sees.
func (h *harness) suiteMixed() error {
	h.section("MIXED workload (§3.4)")
	for _, mix := range bench.StandardMixes() {
		sp := defaultSpec()
		sp.sync = wal.SyncBatch
		sp.memtable = 1 << 20
		n := h.dataset
		if n > 100_000 {
			n = 100_000
		}
		ops := 2 * n
		ks := bench.NewKeyspace("key", n)

		s, dir, err := h.openStore("mixed-"+mix.Name, sp)
		if err != nil {
			return err
		}
		if err := h.populate(s, ks, h.valueSize, n); err != nil {
			_ = s.Close()
			return err
		}
		cfg := sp.config(n, ks.KeyBytes(), h.valueSize, h.concurrency, "mixed-"+mix.Name, mix.Ratio(), h.onTmpfs)
		label := fmt.Sprintf("mixed %-11s (R/W/D %s) conc=%d", mix.Name, mix.Ratio(), h.concurrency)
		if _, err := h.runRepeated(label, cfg, "warm reused DB", func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
			pr, err := bench.Mixed(ctx, s, ks, mix, h.valueSize, n, ops, h.concurrency, seed)
			if err != nil {
				return bench.Result{}, bench.StorageMetrics{}, err
			}
			return pr.Result(label), bench.SnapshotStorage(s, dir), nil
		}); err != nil {
			_ = s.Close()
			return err
		}
		_ = s.Close()
	}
	return nil
}

func byteLabel(n int) string {
	switch {
	case n >= 1024 && n%1024 == 0:
		return fmt.Sprintf("%dKiB", n/1024)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
