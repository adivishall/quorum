package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// reopen opens a store, times the open, snapshots its recovery report and
// counters, and closes it. The open time is restart cost; the recovery report
// says what that time was spent on.
func (h *harness) reopen(dir string, sp storeSpec) (time.Duration, storage.LSMRecovery, bench.StorageMetrics, error) {
	t0 := time.Now()
	s, err := storage.OpenLSMStore(dir, sp.options())
	if err != nil {
		return 0, storage.LSMRecovery{}, bench.StorageMetrics{}, err
	}
	d := time.Since(t0)
	rec := s.Recovery()
	sm := bench.SnapshotStorage(s, dir)
	_ = s.Close()
	return d, rec, sm, nil
}

// suiteScaling measures how build time, read latency, restart time and on-disk
// size move with the dataset size across two orders of magnitude.
func (h *harness) suiteScaling() error {
	h.section("DATASET size scaling (§3.5)")
	sizes := []int{10_000, 100_000, 1_000_000}
	sp := defaultSpec()
	sp.sync = wal.SyncOff // isolate engine cost from device flush across sizes
	sp.memtable = 4 << 20

	fmt.Fprintf(h.out, "  %-10s %12s %12s %12s %12s %10s\n", "dataset", "build ops/s", "get p99 us", "reopen ms", "on-disk MB", "sstables")
	for _, n := range sizes {
		ks := bench.NewKeyspace("key", n)
		dir, err := h.dataDir(fmt.Sprintf("scale-%d", n))
		if err != nil {
			return err
		}
		s, err := storage.OpenLSMStore(dir, sp.options())
		if err != nil {
			return err
		}
		build, err := bench.PutSequential(ctx, s, ks, h.valueSize, 0, n)
		if err != nil {
			_ = s.Close()
			return err
		}
		if err := s.Flush(); err != nil {
			_ = s.Close()
			return err
		}
		if _, err := s.CompactAll(); err != nil {
			_ = s.Close()
			return err
		}
		get, err := bench.GetRandom(ctx, s, ks, n, n, h.concurrency, h.seed, false)
		if err != nil {
			_ = s.Close()
			return err
		}
		diskBytes := bench.DirBytes(dir)
		sstables := len(s.SSTables())
		sm := bench.SnapshotStorage(s, dir)
		_ = s.Close()

		reopenD, _, _, err := h.reopen(dir, sp)
		if err != nil {
			return err
		}

		cfg := sp.config(n, ks.KeyBytes(), h.valueSize, h.concurrency, "scaling", "", h.onTmpfs)
		br := build.Result(fmt.Sprintf("scaling build n=%d", n))
		h.record(br, cfg, h.seed, 0, sm, "sequential build, fresh DB")
		gr := get.Result(fmt.Sprintf("scaling get n=%d", n))
		h.record(gr, cfg, h.seed, 0, sm, "compacted, warm")
		rr := bench.NewResult(fmt.Sprintf("scaling reopen n=%d", n), 0, 0, reopenD)
		h.record(rr, cfg, h.seed, 0, sm, "restart time, WAL fully replayed")

		fmt.Fprintf(h.out, "  %-10d %12.0f %12.1f %12.1f %12.2f %10d\n",
			n, br.OpsPerSec, gr.Latency.P99US, float64(reopenD.Microseconds())/1000.0,
			float64(diskBytes)/(1<<20), sstables)
	}
	return nil
}

// suiteWAL compares the three durability modes under the same small write
// workload, sized so no flush occurs, isolating the per-append sync cost.
func (h *harness) suiteWAL() error {
	h.section("WAL sync modes (§3.6)")
	const walOps = 2000
	// The default SyncBytes is 1 MiB; a 2,000 x 100 B workload writes only
	// ~240 KB, so with the default threshold the batch arm would never perform a
	// byte-triggered fsync inside the timed interval and would not actually
	// measure batching. Reduce the threshold to 16 KiB for this suite (a
	// documented benchmark configuration, not a production semantics change): the
	// workload then crosses it repeatedly, and the WAL's fsync counter proves it.
	const walSyncBytes = 16 << 10
	ks := bench.NewKeyspace("key", walOps)
	for _, mode := range []wal.SyncMode{wal.SyncOff, wal.SyncBatch, wal.SyncAlways} {
		sp := defaultSpec()
		sp.sync = mode
		sp.syncBytes = walSyncBytes
		sp.memtable = 64 << 20 // no memtable flush during the run
		cfg := sp.config(walOps, ks.KeyBytes(), 100, 1, "wal-"+mode.String(), "", h.onTmpfs)
		label := fmt.Sprintf("wal %-6s  n=%d", mode.String(), walOps)
		var lastSyncs int64
		if _, err := h.runRepeated(label, cfg, fmt.Sprintf("no memtable flush; SyncBytes=%dKiB; measures append+sync", walSyncBytes>>10), func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
			pr, sm, err := h.freshRun("wal-"+mode.String(), sp, runIdx, func(s *storage.LSMStore) (bench.PhaseResult, error) {
				return bench.PutSequential(ctx, s, ks, 100, 0, walOps)
			})
			if err != nil {
				return bench.Result{}, bench.StorageMetrics{}, err
			}
			// Prove each mode did what its name says, using the fsync counter.
			if verr := verifyWALSyncs(mode, sm.WALSyncs, walOps); verr != nil {
				return bench.Result{}, bench.StorageMetrics{}, verr
			}
			lastSyncs = sm.WALSyncs
			return pr.Result(label), sm, nil
		}); err != nil {
			return err
		}
		fmt.Fprintf(h.out, "      %-6s performed %d fsync(s) over %d appends within the timed run\n", mode.String(), lastSyncs, walOps)
	}
	fmt.Fprintf(h.out, "  note: the same workload under different durability policies, not a ranking.\n")
	fmt.Fprintf(h.out, "        All three survive process death (a completed write(2) is in the kernel);\n")
	fmt.Fprintf(h.out, "        they differ in flushing to the device, and power-loss durability is untested.\n")
	return nil
}

// verifyWALSyncs checks the fsync counter matches the sync mode's contract, so a
// benchmark cannot report a "batch" number for a run that never batched. off
// must never fsync; always must fsync once per append; batch (with the reduced
// SyncBytes above) must fsync several times as the threshold is crossed.
func verifyWALSyncs(mode wal.SyncMode, syncs int64, ops int) error {
	switch mode {
	case wal.SyncOff:
		if syncs != 0 {
			return fmt.Errorf("wal off performed %d fsyncs, want 0", syncs)
		}
	case wal.SyncAlways:
		if syncs != int64(ops) {
			return fmt.Errorf("wal sync performed %d fsyncs over %d appends, want %d", syncs, ops, ops)
		}
	case wal.SyncBatch:
		if syncs < 3 {
			return fmt.Errorf("wal batch performed only %d fsyncs — the batch threshold was not crossed within the timed run", syncs)
		}
	}
	return nil
}

// suiteCompaction measures compaction two ways: foreground write throughput
// while background compaction runs, and an isolated compaction whose duration
// and byte movement are measured directly.
func (h *harness) suiteCompaction() error {
	h.section("COMPACTION impact (§3.7)")
	n := h.dataset
	if n > 200_000 {
		n = 200_000
	}
	gens := 4
	keys := n / gens
	ks := bench.NewKeyspace("key", keys)

	// A. Foreground writes with background compaction active.
	spA := defaultSpec()
	spA.sync = wal.SyncOff
	spA.memtable = 256 << 10
	spA.l0Trigger = 4
	cfgA := spA.config(n, ks.KeyBytes(), h.valueSize, 1, "compaction-foreground", "", h.onTmpfs)
	label := fmt.Sprintf("foreground write (bg compaction) n=%d", n)
	if _, err := h.runRepeated(label, cfgA, fmt.Sprintf("%d generations over %d keys, small memtable", gens, keys), func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
		pr, sm, err := h.freshRun("compact-fg", spA, runIdx, func(s *storage.LSMStore) (bench.PhaseResult, error) {
			start := time.Now()
			val := make([]byte, h.valueSize)
			var key []byte
			lat := bench.NewLatencies(n)
			for g := 0; g < gens; g++ {
				for k := 0; k < keys; k++ {
					key = ks.AppendKey(key[:0], k)
					bench.FillValue(val, g*keys+k)
					t0 := time.Now()
					if err := s.Put(ctx, key, val); err != nil {
						return bench.PhaseResult{}, err
					}
					lat.Record(time.Since(t0))
				}
			}
			return bench.PhaseResult{Ops: int64(gens * keys), Bytes: int64(gens*keys) * int64(ks.KeyBytes()+h.valueSize), Elapsed: time.Since(start), Lat: lat}, nil
		})
		if err != nil {
			return bench.Result{}, bench.StorageMetrics{}, err
		}
		// Prove the benchmark measured what it claims: a foreground-write
		// benchmark labelled "bg compaction" is meaningless if no compaction
		// ran. Fail loudly rather than post a number for the wrong workload.
		if sm.CompactionRuns == 0 {
			return bench.Result{}, bench.StorageMetrics{}, fmt.Errorf("compaction benchmark: no background compaction ran (sstables=%d) — precondition not met", sm.SSTables)
		}
		return pr.Result(label), sm, nil
	}); err != nil {
		return err
	}

	// B. Isolated compaction: build L0 files with auto-compaction off, then time
	// a single CompactAll and read exactly what it moved.
	spB := defaultSpec()
	spB.sync = wal.SyncOff
	spB.memtable = 256 << 10
	spB.autoCompact = false
	s, dir, err := h.openStore("compact-run", spB)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	val := make([]byte, h.valueSize)
	var key []byte
	for g := 0; g < gens; g++ {
		for k := 0; k < keys; k++ {
			key = ks.AppendKey(key[:0], k)
			bench.FillValue(val, g*keys+k)
			if err := s.Put(ctx, key, val); err != nil {
				return err
			}
		}
		if err := s.Flush(); err != nil {
			return err
		}
	}
	filesBefore := len(s.SSTables())
	bytesBefore := bench.DirBytes(dir)
	t0 := time.Now()
	if _, err := s.CompactAll(); err != nil {
		return err
	}
	dur := time.Since(t0)
	cs := s.CompactionStats()
	sm := bench.SnapshotStorage(s, dir)
	bytesAfter := bench.DirBytes(dir)

	// Prove compaction actually happened before reporting its numbers: at least
	// one run, and the file set genuinely shrank. Without this, a build that
	// silently produced too few files to compact would report a "compaction"
	// result that measured nothing.
	if cs.Runs == 0 || filesBefore <= len(s.SSTables()) {
		return fmt.Errorf("isolated compaction did not occur: runs=%d, files %d->%d", cs.Runs, filesBefore, len(s.SSTables()))
	}

	compThroughput := 0.0
	if dur.Seconds() > 0 {
		compThroughput = float64(cs.InputBytes) / (1 << 20) / dur.Seconds()
	}
	note := fmt.Sprintf("CompactAll: %d files -> %d in %s; in=%dB out=%dB; versions dropped=%d; tombstones kept=%d dropped=%d; %.1f MiB/s input",
		filesBefore, len(s.SSTables()), dur.Round(time.Millisecond), cs.InputBytes, cs.OutputBytes,
		cs.VersionsDropped, cs.TombstonesKept, cs.TombstonesDropped, compThroughput)
	cfgB := spB.config(gens*keys, ks.KeyBytes(), h.valueSize, 1, "compaction-run", "", h.onTmpfs)
	rr := bench.NewResult("compaction run", cs.InputEntries, cs.InputBytes, dur)
	h.single("compaction run", cfgB, note, rr, sm)
	fmt.Fprintf(h.out, "  isolated: %s\n", note)
	fmt.Fprintf(h.out, "  on-disk %0.2f MiB -> %0.2f MiB (%.2fx)\n",
		float64(bytesBefore)/(1<<20), float64(bytesAfter)/(1<<20), ratio(bytesAfter, bytesBefore))
	return nil
}

// suiteWriteAmp reports write amplification with an explicit formula. Logical
// bytes are the key+value bytes the client asked to store; physical bytes are
// what the engine wrote — WAL segments, flushed SSTables, compaction output.
func (h *harness) suiteWriteAmp() error {
	h.section("WRITE amplification (§3.8)")
	gens := 5
	keys := h.dataset / gens
	if keys > 40_000 {
		keys = 40_000
	}
	ks := bench.NewKeyspace("key", keys)
	sp := defaultSpec()
	sp.sync = wal.SyncBatch
	sp.memtable = 512 << 10
	sp.l0Trigger = 4

	s, dir, err := h.openStore("writeamp", sp)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	var logical int64
	val := make([]byte, h.valueSize)
	var key []byte
	for g := 0; g < gens; g++ {
		for k := 0; k < keys; k++ {
			key = ks.AppendKey(key[:0], k)
			bench.FillValue(val, g*keys+k)
			if err := s.Put(ctx, key, val); err != nil {
				return err
			}
			logical += int64(len(key) + h.valueSize)
		}
	}
	if err := s.Flush(); err != nil {
		return err
	}
	if _, err := s.CompactAll(); err != nil {
		return err
	}
	sm := bench.SnapshotStorage(s, dir)

	// The write-amplification workload overwrites keys across generations
	// specifically so compaction rewrites data; if it did not run, the
	// compaction term of the amplification would be a silent zero. Prove it ran.
	if s.CompactionStats().Runs == 0 {
		return fmt.Errorf("write-amp: no compaction ran, so the compaction term would be unmeasured")
	}

	walB := sm.WALBytes
	flushB := sm.FlushBytes
	compB := sm.CompactOutBytes
	physical := walB + flushB + compB
	waTotal := ratio64(physical, logical)
	waNoWAL := ratio64(flushB+compB, logical)

	note := fmt.Sprintf("logical=%dB wal=%dB flush=%dB compaction_out=%dB; WA_total=(wal+flush+compaction)/logical=%.2fx; WA_storage=(flush+compaction)/logical=%.2fx",
		logical, walB, flushB, compB, waTotal, waNoWAL)
	cfg := sp.config(gens*keys, ks.KeyBytes(), h.valueSize, 1, "write-amp", "", h.onTmpfs)
	rr := bench.NewResult("write amplification", int64(gens*keys), logical, time.Millisecond)
	rr.OpsPerSec = 0 // not a throughput measurement
	rr.MBPerSec = 0
	h.single("write amplification", cfg, note, rr, sm)

	fmt.Fprintf(h.out, "  formula: WA = physical bytes written / logical bytes stored\n")
	fmt.Fprintf(h.out, "  logical           %8.2f MiB  (%d writes of %dB key + %dB value)\n", mib(logical), gens*keys, ks.KeyBytes(), h.valueSize)
	fmt.Fprintf(h.out, "  WAL segments      %8.2f MiB\n", mib(walB))
	fmt.Fprintf(h.out, "  flushed SSTables  %8.2f MiB\n", mib(flushB))
	fmt.Fprintf(h.out, "  compaction output %8.2f MiB\n", mib(compB))
	fmt.Fprintf(h.out, "  WA_total = %.2fx   WA_storage (excl. WAL) = %.2fx\n", waTotal, waNoWAL)
	fmt.Fprintf(h.out, "  (physical write accounting is storage-engine level, not filesystem; see docs/BENCHMARKS.md §3.8)\n")
	return nil
}

// suiteReadAmp measures the storage work a read causes, with the Bloom filter on
// and off over the same shuffled dataset, for hits and for misses. The shuffled
// layout makes every SSTable span the keyspace, so without the filter a point
// read must open a block in every file.
func (h *harness) suiteReadAmp() error {
	h.section("READ amplification / Bloom effect (§3.9)")
	n := h.dataset
	if n > 60_000 {
		n = 60_000
	}
	reads := n
	ks := bench.NewKeyspace("key", n)

	fmt.Fprintf(h.out, "  %-16s %8s %12s %12s %14s\n", "config", "sstables", "block reads", "filter skips", "us/read p99")
	for _, bloomOn := range []bool{true, false} {
		sp := defaultSpec()
		sp.sync = wal.SyncOff
		sp.memtable = 256 << 10
		sp.autoCompact = false // keep many overlapping files
		sp.bloom = bloomOn

		name := "readamp-bloom-off"
		if bloomOn {
			name = "readamp-bloom-on"
		}
		s, dir, err := h.openStore(name, sp)
		if err != nil {
			return err
		}
		if err := h.populateShuffled(s, ks, h.valueSize, n, h.seed); err != nil {
			_ = s.Close()
			return err
		}
		if err := s.Flush(); err != nil {
			_ = s.Close()
			return err
		}
		sstables := len(s.SSTables())

		for _, missing := range []bool{false, true} {
			before := s.ReadCounters()
			pr, err := bench.GetRandom(ctx, s, ks, n, reads, 1, h.seed, missing)
			if err != nil {
				_ = s.Close()
				return err
			}
			after := s.ReadCounters()
			blockReads := after.BlockReads - before.BlockReads
			filterSkips := after.FilterSkips - before.FilterSkips

			kind := "hit"
			if missing {
				kind = "miss"
			}
			cfg := sp.config(n, ks.KeyBytes(), h.valueSize, 1, fmt.Sprintf("readamp-%s", kind), "", h.onTmpfs)
			r := pr.Result(fmt.Sprintf("readamp %s bloom=%v", kind, bloomOn))
			sm := bench.SnapshotStorage(s, dir)
			note := fmt.Sprintf("%d reads: block_reads=%d filter_skips=%d over %d files", reads, blockReads, filterSkips, sstables)
			h.single(r.Benchmark, cfg, note, r, sm)
			fmt.Fprintf(h.out, "  bloom=%-5v %-4s %8d %12d %12d %14.1f\n", bloomOn, kind, sstables, blockReads, filterSkips, r.Latency.P99US)
		}
		_ = s.Close()
	}
	return nil
}

// suiteStartup measures restart time at several dataset sizes and reports what
// recovery spent the time on. The WAL is not truncated in this phase, so a
// restart replays every mutation ever written; the numbers show that cost.
func (h *harness) suiteStartup() error {
	h.section("STARTUP / reopen (§3.10)")
	sizes := []int{10_000, 100_000, 300_000}
	sp := defaultSpec()
	sp.sync = wal.SyncOff
	sp.memtable = 1 << 20

	fmt.Fprintf(h.out, "  %-10s %11s %9s %9s %8s %12s %12s %10s\n",
		"dataset", "reopen med", "min", "max", "spread", "records", "ops replayed", "sstables")
	for _, n := range sizes {
		ks := bench.NewKeyspace("key", n)
		dir, err := h.dataDir(fmt.Sprintf("startup-%d", n))
		if err != nil {
			return err
		}
		s, err := storage.OpenLSMStore(dir, sp.options())
		if err != nil {
			return err
		}
		if err := h.populate(s, ks, h.valueSize, n); err != nil {
			_ = s.Close()
			return err
		}
		_ = s.Close()

		cfg := sp.config(n, ks.KeyBytes(), h.valueSize, 1, "startup", "", h.onTmpfs)
		// Repeat the reopen and preserve the distribution: record every reopen as
		// its own result (RunIndex) rather than silently keeping the minimum, and
		// report the median as representative with min/max/spread beside it.
		durs := make([]time.Duration, 0, h.runs)
		var rec storage.LSMRecovery
		var sm bench.StorageMetrics
		for i := 0; i < h.runs; i++ {
			d, r, m, err := h.reopen(dir, sp)
			if err != nil {
				return err
			}
			durs = append(durs, d)
			rec, sm = r, m // identical across reopens (same directory)
			rr := bench.NewResult(fmt.Sprintf("startup n=%d", n), rec.RecordsApplied, 0, d)
			note := fmt.Sprintf("segments=%d sstables=%d records=%d replayed=%d skipped=%d",
				rec.SegmentsScanned, rec.SSTablesLoaded, rec.RecordsApplied, rec.OpsReplayed, rec.OpsSkipped)
			h.record(rr, cfg, h.seed, i, sm, note)
		}
		med, mn, mx, spread := durStatsMS(durs)
		fmt.Fprintf(h.out, "  %-10d %10.1f %8.1f %8.1f %6.0f%% %12d %12d %10d\n",
			n, med, mn, mx, spread, rec.RecordsApplied, rec.OpsReplayed, sm.SSTables)
	}
	fmt.Fprintf(h.out, "  note: reopen scans the whole WAL (no truncation in this phase); most records are\n")
	fmt.Fprintf(h.out, "        already durable in an SSTable and skipped, but the scan cost is paid.\n")
	return nil
}

// durStatsMS returns the median, min, max (all in milliseconds) and the spread
// percent (max-min)/median of a set of durations.
func durStatsMS(ds []time.Duration) (med, mn, mx, spreadPct float64) {
	if len(ds) == 0 {
		return 0, 0, 0, 0
	}
	xs := make([]float64, len(ds))
	for i, d := range ds {
		xs[i] = float64(d.Microseconds()) / 1000.0
	}
	sort.Float64s(xs)
	mn, mx = xs[0], xs[len(xs)-1]
	m := len(xs)
	if m%2 == 1 {
		med = xs[m/2]
	} else {
		med = (xs[m/2-1] + xs[m/2]) / 2
	}
	if med > 0 {
		spreadPct = (mx - mn) / med * 100
	}
	return med, mn, mx, spreadPct
}

// suiteManifest measures how restart scales with the number of SSTables, and the
// cost of the optional full block verification, holding the key count fixed.
func (h *harness) suiteManifest() error {
	h.section("MANIFEST / SSTable metadata scaling (§3.11)")
	// Hold the total logical data (and therefore WAL length) approximately
	// constant and vary only the number of SSTables it is split across, so the
	// experiment isolates per-file metadata cost instead of confounding it with a
	// larger dataset. totalKeys is chosen to divide every file count evenly.
	const totalKeys = 64_000
	counts := []int{8, 32, 128}

	fmt.Fprintf(h.out, "  total keys held constant at %d; only the SSTable count varies\n", totalKeys)
	fmt.Fprintf(h.out, "  %-10s %10s %14s %10s\n", "sstables", "keys/file", "reopen med ms", "+verify ms")
	for _, files := range counts {
		perFile := totalKeys / files
		ks := bench.NewKeyspace("key", totalKeys)
		sp := defaultSpec()
		sp.sync = wal.SyncOff
		sp.autoCompact = false
		sp.memtable = 512 << 20 // never auto-flush; we flush explicitly per file

		dir, err := h.dataDir(fmt.Sprintf("manifest-%d", files))
		if err != nil {
			return err
		}
		s, err := storage.OpenLSMStore(dir, sp.options())
		if err != nil {
			return err
		}
		val := make([]byte, h.valueSize)
		var key []byte
		idx := 0
		for f := 0; f < files; f++ {
			for k := 0; k < perFile; k++ {
				key = ks.AppendKey(key[:0], idx)
				bench.FillValue(val, idx)
				if err := s.Put(ctx, key, val); err != nil {
					return err
				}
				idx++
			}
			if err := s.Flush(); err != nil {
				return err
			}
		}
		got := len(s.SSTables())
		_ = s.Close()

		cfg := sp.config(totalKeys, ks.KeyBytes(), h.valueSize, 1, "manifest-scaling", "", h.onTmpfs)

		// Default reopen (footer cross-check), repeated; record every run.
		plain := make([]time.Duration, 0, h.runs)
		for i := 0; i < h.runs; i++ {
			d, _, sm, err := h.reopen(dir, sp)
			if err != nil {
				return err
			}
			plain = append(plain, d)
			rr := bench.NewResult(fmt.Sprintf("manifest reopen files=%d", got), 0, 0, d)
			h.record(rr, cfg, h.seed, i, sm, fmt.Sprintf("%d sstables, %d keys/file, default reopen", got, perFile))
		}

		// Full block verification, repeated.
		spvOpts := sp.options()
		spvOpts.VerifySSTablesOnOpen = true
		verify := make([]time.Duration, 0, h.runs)
		for i := 0; i < h.runs; i++ {
			t0 := time.Now()
			vs, err := storage.OpenLSMStore(dir, spvOpts)
			if err != nil {
				return err
			}
			d := time.Since(t0)
			_ = vs.Close()
			verify = append(verify, d)
			rr := bench.NewResult(fmt.Sprintf("manifest reopen+verify files=%d", got), 0, 0, d)
			h.record(rr, cfg, h.seed, i, bench.StorageMetrics{SSTables: got}, fmt.Sprintf("%d sstables, %d keys/file, full block verification", got, perFile))
		}

		plainMed, _, _, _ := durStatsMS(plain)
		verifyMed, _, _, _ := durStatsMS(verify)
		fmt.Fprintf(h.out, "  %-10d %10d %14.1f %10.1f\n", got, perFile, plainMed, verifyMed)
	}
	fmt.Fprintf(h.out, "  note: total data is fixed, so the change across rows isolates file-count cost.\n")
	fmt.Fprintf(h.out, "        default reopen cross-checks each file's footer; +verify reads every block.\n")
	return nil
}

// suiteConcurrency measures read throughput and tail latency as reader count
// grows, against a warm, fully compacted dataset.
func (h *harness) suiteConcurrency() error {
	h.section("CONCURRENCY scaling (§3.12)")
	n := h.dataset
	if n > 200_000 {
		n = 200_000
	}
	ks := bench.NewKeyspace("key", n)
	sp := defaultSpec()
	sp.sync = wal.SyncOff
	sp.memtable = 1 << 20

	s, dir, err := h.openStore("concurrency", sp)
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
	if _, err := bench.GetRandom(ctx, s, ks, n, n/10+1, 1, h.seed, false); err != nil {
		return err
	}

	fmt.Fprintf(h.out, "  %-8s %12s %10s %10s %10s\n", "workers", "ops/s", "p50 us", "p95 us", "p99 us")
	for _, workers := range []int{1, 2, 4, 8} {
		cfg := sp.config(n, ks.KeyBytes(), h.valueSize, workers, "concurrency-get", "", h.onTmpfs)
		label := fmt.Sprintf("get workers=%d", workers)
		a, err := h.runRepeatedOpts(label, cfg, "warm, fully compacted, read-only", false, func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error) {
			pr, err := bench.GetRandom(ctx, s, ks, n, n, workers, seed, false)
			if err != nil {
				return bench.Result{}, bench.StorageMetrics{}, err
			}
			return pr.Result(label), bench.SnapshotStorage(s, dir), nil
		})
		if err != nil {
			return err
		}
		fmt.Fprintf(h.out, "  %-8d %12.0f %10.1f %10.1f %10.1f\n", workers, a.OpsPerSecMed, a.P50USMed, a.P95USMed, a.P99USMed)
	}
	fmt.Fprintf(h.out, "  note: scaling beyond physical cores is not expected to keep rising.\n")
	return nil
}

// ---- small formatting helpers ----

func mib(b int64) float64        { return float64(b) / (1 << 20) }
func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
func ratio64(a, b int64) float64 { return ratio(a, b) }
