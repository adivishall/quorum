package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// applyArm is one way of recording state-machine applications in the engine.
type applyArm struct {
	name     string
	perBatch int  // applied entries per call
	legacy   bool // Put, then SetAppliedIndex: the only way before S1
}

// applyRun is what one run of an arm measured beyond its throughput and latency.
type applyRun struct {
	entries     int
	walBytes    int64
	syncs       int64
	records     int64 // WAL records recovery replayed
	recovery    time.Duration
	recApplied  storage.AppliedIndex
	recSequence uint64
}

// suiteApply measures what S1's atomic apply batches cost
// (docs/STORAGE_INTEGRATION.md §7.8). The baseline is the only way to record an
// application before S1: a Put, then SetAppliedIndex — two records per entry,
// and not atomic. Against it, Apply with one entry per batch (the same work, one
// record) and with 16 (one batch per Raft cycle carrying several entries, as the
// hosted engine will). Each arm writes the same entries — one 100-byte put per
// entry — under each sync mode, then the store is reopened and its recovery
// timed. No optimization is measured: this is the cost of atomicity.
func (h *harness) suiteApply() error {
	h.section("APPLY batches (S1, docs/STORAGE_INTEGRATION.md §7.8)")
	arms := []applyArm{
		{name: "legacy put+applied-index", perBatch: 1, legacy: true},
		{name: "apply, 1 entry/batch", perBatch: 1},
		{name: "apply, 16 entries/batch", perBatch: 16},
	}
	modes := []struct {
		mode    wal.SyncMode
		entries int
	}{
		{wal.SyncOff, 32_000},
		{wal.SyncBatch, 32_000},
		{wal.SyncAlways, 1_600}, // an fsync is milliseconds; the legacy arm makes two per entry
	}
	const valueSize = 100
	ks := bench.NewKeyspace("key", 32_000)
	type row struct {
		mode string
		arm  applyArm
		agg  bench.Aggregate
		runs []applyRun
	}
	var rows []row
	for _, m := range modes {
		for _, arm := range arms {
			sp := defaultSpec()
			sp.sync = m.mode
			sp.memtable = 64 << 20 // no memtable flush: the WAL is what is measured
			sp.autoCompact = false
			cfg := sp.config(m.entries, ks.KeyBytes(), valueSize, 1, "apply-"+m.mode.String(), fmt.Sprintf("%d entries/call", arm.perBatch), h.onTmpfs)
			label := fmt.Sprintf("%-6s %s", m.mode, arm.name)
			var runs []applyRun
			agg, err := h.runRepeatedOpts(label, cfg, "", false, func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
				r, sm, ar, err := h.applyOnce(fmt.Sprintf("apply-%s-%d-%d", m.mode, arm.perBatch, runIdx), sp, arm, ks, m.entries, valueSize)
				if err != nil {
					return bench.Result{}, bench.StorageMetrics{}, err
				}
				runs = append(runs, ar)
				r.Benchmark = label
				return r, sm, nil
			})
			if err != nil {
				return err
			}
			rows = append(rows, row{mode: m.mode.String(), arm: arm, agg: agg, runs: runs})
		}
	}
	fmt.Fprintf(h.out, "  %-6s %-26s %12s %11s %11s %9s %9s %7s %11s %s\n",
		"mode", "arm", "entries/s", "call p50us", "call p99us", "rec/entry", "B/entry", "fsyncs", "recovery ms", "recovered (index, seq)")
	for _, r := range rows {
		last := r.runs[len(r.runs)-1]
		e := float64(last.entries)
		var recMs []float64
		for _, ar := range r.runs {
			recMs = append(recMs, float64(ar.recovery.Microseconds())/1000)
		}
		sort.Float64s(recMs)
		fmt.Fprintf(h.out, "  %-6s %-26s %12.0f %11.1f %11.1f %9.3f %9.1f %7d %11.2f (%d, %d)\n",
			r.mode, r.arm.name, r.agg.OpsPerSecMed*float64(r.arm.perBatch), r.agg.P50USMed, r.agg.P99USMed,
			float64(last.records)/e, float64(last.walBytes)/e, last.syncs, recMs[len(recMs)/2],
			last.recApplied.Index, last.recSequence)
	}
	fmt.Fprintf(h.out, "  note: entries/s counts applied entries (a call carries 1 or 16). fsyncs is the run's total.\n")
	fmt.Fprintf(h.out, "        Call latency is per call: one\n")
	fmt.Fprintf(h.out, "        Put+SetAppliedIndex pair, or one Apply. Recovery is the median reopen; the recovered\n")
	fmt.Fprintf(h.out, "        index and sequence are checked against what was written.\n")
	return nil
}

// applyOnce writes n entries through arm into a fresh store, then reopens it and
// checks it recovered every entry's index and sequence.
func (h *harness) applyOnce(name string, sp storeSpec, arm applyArm, ks bench.Keyspace, n, valueSize int) (bench.Result, bench.StorageMetrics, applyRun, error) {
	s, dir, err := h.openStore(name, sp)
	if err != nil {
		return bench.Result{}, bench.StorageMetrics{}, applyRun{}, err
	}
	ctx := context.Background()
	calls := n / arm.perBatch
	lat := bench.NewLatencies(calls)
	val := make([]byte, valueSize)
	var bytesMoved int64
	start := time.Now()
	for c := 0; c < calls; c++ {
		applied := storage.AppliedIndex{Index: uint64(c + 1), Term: 1}
		muts := make([]storage.Mutation, arm.perBatch)
		for j := range muts {
			i := c*arm.perBatch + j
			bench.FillValue(val, i)
			muts[j] = storage.Mutation{Kind: storage.MutationPut, Key: ks.Key(i), Value: append([]byte(nil), val...)}
			bytesMoved += int64(len(muts[j].Key) + valueSize)
		}
		t0 := time.Now()
		if arm.legacy {
			if err := s.Put(ctx, muts[0].Key, muts[0].Value); err != nil {
				_ = s.Close()
				return bench.Result{}, bench.StorageMetrics{}, applyRun{}, err
			}
			if err := s.SetAppliedIndex(ctx, applied); err != nil {
				_ = s.Close()
				return bench.Result{}, bench.StorageMetrics{}, applyRun{}, err
			}
		} else if err := s.Apply(ctx, muts, applied); err != nil {
			_ = s.Close()
			return bench.Result{}, bench.StorageMetrics{}, applyRun{}, err
		}
		lat.Record(time.Since(t0))
	}
	elapsed := time.Since(start)
	sm := bench.SnapshotStorage(s, dir)
	if err := s.Close(); err != nil {
		return bench.Result{}, bench.StorageMetrics{}, applyRun{}, err
	}
	d, rec, _, err := h.reopenApplied(dir, sp)
	if err != nil {
		return bench.Result{}, bench.StorageMetrics{}, applyRun{}, err
	}
	want := storage.AppliedIndex{Index: uint64(calls), Term: 1}
	if rec.applied != want || rec.seq != uint64(n) {
		return bench.Result{}, bench.StorageMetrics{}, applyRun{}, fmt.Errorf("%s recovered (%+v, %d), wrote (%+v, %d)", name, rec.applied, rec.seq, want, n)
	}
	r := bench.PhaseResult{Ops: int64(calls), Bytes: bytesMoved, Elapsed: elapsed, Lat: lat}.Result(name)
	return r, sm, applyRun{entries: n, walBytes: sm.WALBytes, syncs: sm.WALSyncs, records: rec.records,
		recovery: d, recApplied: rec.applied, recSequence: rec.seq}, nil
}

type recoveredApply struct {
	applied storage.AppliedIndex
	seq     uint64
	records int64
}

// reopenApplied times an open and reports what it recovered.
func (h *harness) reopenApplied(dir string, sp storeSpec) (time.Duration, recoveredApply, storage.LSMRecovery, error) {
	t0 := time.Now()
	s, err := storage.OpenLSMStore(dir, sp.options())
	if err != nil {
		return 0, recoveredApply{}, storage.LSMRecovery{}, err
	}
	d := time.Since(t0)
	rec := s.Recovery()
	out := recoveredApply{applied: s.AppliedIndex(), seq: s.Sequence(), records: rec.RecordsApplied}
	_ = s.Close()
	return d, out, rec, nil
}
