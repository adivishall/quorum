package storage_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/storage"
)

// Development measurements for Phase 4 (docs/BLOOM.md §5, docs/COMPACTION.md §7,
// docs/MANIFEST.md §7).
//
// These are NOT benchmarks in the sense Phase 5 will mean. They run inside the
// ordinary test suite on whatever machine happens to be running it, nothing about
// the environment is controlled, and no number they print may be quoted anywhere
// as a result. What they are for is establishing the SHAPE of the effect —
// whether the filter actually avoids work, whether compaction actually removes
// versions — and each one asserts that direction so that a regression is a test
// failure rather than a number nobody re-reads.
//
// The assertions are deliberately on counted work (files consulted, blocks read,
// entries dropped) rather than on wall-clock time. Work avoided is the part of the
// result that generalises off this laptop; microseconds are not.

// measureEnv records what the numbers below were produced on, so a printed table
// is never separated from its conditions.
func measureEnv() string {
	return fmt.Sprintf("go %s, %s/%s, %d CPU",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
}

// percentiles returns p50, p95 and p99 of a duration sample.
func percentiles(d []time.Duration) (p50, p95, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0
	}
	sorted := append([]time.Duration(nil), d...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(q float64) time.Duration {
		i := int(q * float64(len(sorted)-1))
		return sorted[i]
	}
	return at(0.50), at(0.95), at(0.99)
}

// bloomWorkload is one arm of the with/without-filter comparison.
type bloomWorkload struct {
	name        string
	files       int
	blockReads  uint64
	filterSkips uint64
	p50         time.Duration
	p95         time.Duration
	p99         time.Duration
	hits        int
	misses      int
}

// runBloomArm builds an identical dataset and runs an identical lookup workload,
// with the filter either enabled or disabled.
func runBloomArm(t *testing.T, name string, disableFilter bool, keys, perFile, probes int) bloomWorkload {
	t.Helper()

	opts := lsmOpts(storage.DefaultMemTableSize)
	opts.DisableBloomFilter = disableFilter
	// Every arm must see the same file layout, so flushes are explicit and
	// compaction is off. A different number of files would make the two arms
	// incomparable, which is the easiest way to produce a flattering number by
	// accident.
	dir := t.TempDir()
	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	for i := 0; i < keys; i++ {
		mustPut(t, s, fmt.Sprintf("key%07d", i), "value-padding-padding-padding")
		if (i+1)%perFile == 0 {
			mustFlush(t, s)
		}
	}
	mustFlush(t, s)

	arm := bloomWorkload{name: name, files: len(s.SSTables())}
	before := s.ReadCounters()

	// A fixed, seeded mix: half the lookups are for keys that exist, half for keys
	// that do not. The miss half is where a filter can help; including the hit half
	// keeps the comparison honest about the cost it adds.
	rng := rand.New(rand.NewSource(20240919))
	ctx := context.Background()
	lat := make([]time.Duration, 0, probes)
	for i := 0; i < probes; i++ {
		var key string
		miss := i%2 == 1
		if miss {
			key = fmt.Sprintf("absent%07d", rng.Intn(1<<20))
		} else {
			key = fmt.Sprintf("key%07d", rng.Intn(keys))
		}

		start := time.Now()
		_, err := s.Get(ctx, []byte(key))
		lat = append(lat, time.Since(start))

		switch {
		case miss:
			if !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("%s: Get(%q) = %v, want ErrNotFound", name, key, err)
			}
			arm.misses++
		default:
			if err != nil {
				t.Fatalf("%s: Get(%q) = %v", name, key, err)
			}
			arm.hits++
		}
	}

	after := s.ReadCounters()
	arm.blockReads = after.BlockReads - before.BlockReads
	arm.filterSkips = after.FilterSkips - before.FilterSkips
	arm.p50, arm.p95, arm.p99 = percentiles(lat)
	return arm
}

// TestBloomEffectMeasurement is Part 3: the same dataset and the same workload,
// with the filter on and off.
//
// The headline is not the microseconds. It is that the filter removes almost all
// of the per-file work on a miss, which is the effect that holds regardless of how
// fast the disk is.
func TestBloomEffectMeasurement(t *testing.T) {
	const (
		keys    = 20000
		perFile = 1200 // ~17 SSTables
		probes  = 4000
	)

	without := runBloomArm(t, "filter disabled (Phase 3 behaviour)", true, keys, perFile, probes)
	with := runBloomArm(t, "filter enabled", false, keys, perFile, probes)

	if without.files != with.files {
		t.Fatalf("the two arms produced %d and %d files; they are not comparable",
			without.files, with.files)
	}

	t.Logf("Bloom filter effect — %s", measureEnv())
	t.Logf("dataset: %d keys in %d SSTables, %d lookups (%d hits, %d misses), "+
		"bits/key=%d, no compaction",
		keys, with.files, probes, with.hits, with.misses, storage.DefaultOptions().BitsPerKey)
	t.Logf("%-38s %10s %12s %9s %9s %9s", "arm", "blockReads", "filterSkips", "p50", "p95", "p99")
	for _, a := range []bloomWorkload{without, with} {
		t.Logf("%-38s %10d %12d %9s %9s %9s",
			a.name, a.blockReads, a.filterSkips, a.p50, a.p95, a.p99)
	}

	// Without a filter, every miss consults every file and reads one block from
	// each. The floor is therefore misses*files block reads.
	wantFloor := uint64(without.misses * without.files)
	if without.blockReads < wantFloor {
		t.Fatalf("the filterless arm read %d blocks, fewer than the %d a miss across "+
			"%d files requires; the arms are not doing the same work",
			without.blockReads, wantFloor, without.files)
	}
	if with.filterSkips == 0 {
		t.Fatal("the filtered arm skipped nothing; the filter is not being consulted")
	}
	// The property worth asserting: the filter removes the large majority of the
	// block reads. A tolerant threshold, because the exact figure depends on the
	// false-positive rate and the hit/miss mix.
	if with.blockReads*2 >= without.blockReads {
		t.Fatalf("the filter reduced block reads only from %d to %d; it is not "+
			"eliminating files on a miss", without.blockReads, with.blockReads)
	}
	t.Logf("block reads %d -> %d (%.1f%% avoided); %d file consultations eliminated "+
		"without any I/O",
		without.blockReads, with.blockReads,
		100*(1-float64(with.blockReads)/float64(without.blockReads)), with.filterSkips)
}

// TestCompactionMeasurement is the compaction half of Part 22: bytes in and out,
// write amplification, and what was removed.
func TestCompactionMeasurement(t *testing.T) {
	const (
		keys     = 12000
		keyspace = 3000 // so most writes are overwrites and there is real work to do
		perFile  = 1000
	)
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 4)
	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key%06d", rng.Intn(keyspace))
		if rng.Intn(6) == 0 {
			mustDelete(t, s, k)
		} else {
			mustPut(t, s, k, fmt.Sprintf("value-%06d-padding", i))
		}
		if (i+1)%perFile == 0 {
			mustFlush(t, s)
		}
	}
	mustFlush(t, s)

	var beforeBytes int64
	var beforeEntries uint64
	beforeFiles := len(s.SSTables())
	for _, f := range s.SSTables() {
		beforeBytes += f.Bytes
		beforeEntries += f.Entries
	}

	start := time.Now()
	runs := mustCompactAll(t, s)
	elapsed := time.Since(start)

	var afterBytes int64
	var afterEntries uint64
	for _, f := range s.SSTables() {
		afterBytes += f.Bytes
		afterEntries += f.Entries
	}
	st := s.CompactionStats()

	t.Logf("Compaction — %s", measureEnv())
	t.Logf("dataset: %d mutations over %d distinct keys, L0 trigger 4", keys, keyspace)
	t.Logf("before: %d files, %d entries, %d bytes", beforeFiles, beforeEntries, beforeBytes)
	t.Logf("after:  %d files, %d entries, %d bytes, levels %+v",
		len(s.SSTables()), afterEntries, afterBytes, s.LevelSummary())
	t.Logf("%d compactions in %s: %d input bytes -> %d output bytes "+
		"(write amplification %.2fx of the input read)",
		runs, elapsed.Round(time.Millisecond), st.InputBytes, st.OutputBytes,
		float64(st.OutputBytes)/float64(st.InputBytes))
	t.Logf("entries: %d in -> %d out; %d superseded versions dropped, "+
		"%d tombstones dropped, %d retained",
		st.InputEntries, st.OutputEntries, st.VersionsDropped,
		st.TombstonesDropped, st.TombstonesKept)

	if runs == 0 {
		t.Fatal("no compaction ran; this measures nothing")
	}
	// The point of compaction: fewer files and fewer entries than it was given.
	if len(s.SSTables()) >= beforeFiles {
		t.Fatalf("compaction left %d files, no fewer than the %d it started with",
			len(s.SSTables()), beforeFiles)
	}
	if st.VersionsDropped == 0 {
		t.Fatal("no superseded version was dropped although most writes were overwrites")
	}
	if afterEntries >= beforeEntries {
		t.Fatalf("compaction left %d entries, no fewer than the %d it started with",
			afterEntries, beforeEntries)
	}
}

// TestStartupMeasurement is Part 16: what the MANIFEST changed about opening a
// database.
//
// Three arms over the same data — many files with the MANIFEST's metadata, the
// same files with a full verification pass, and the compacted result — with the
// data blocks read at startup reported alongside the time, because the read count
// is the part that does not depend on the machine.
func TestStartupMeasurement(t *testing.T) {
	const (
		keys    = 20000
		perFile = 1000 // 20 SSTables
	)
	dir := t.TempDir()
	opts := manualCompactOpts(storage.DefaultMemTableSize, 4)

	s := openLSM(t, dir, opts)
	for i := 0; i < keys; i++ {
		mustPut(t, s, fmt.Sprintf("key%07d", i), "value-padding-padding-padding")
		if (i+1)%perFile == 0 {
			mustFlush(t, s)
		}
	}
	mustFlush(t, s)
	files := len(s.SSTables())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	type arm struct {
		name       string
		elapsed    time.Duration
		blockReads uint64
		files      int
		replayed   int64
		skipped    int64
	}
	open := func(name string, o storage.Options) arm {
		start := time.Now()
		r := openLSM(t, dir, o)
		elapsed := time.Since(start)
		rec := r.Recovery()
		a := arm{
			name: name, elapsed: elapsed,
			blockReads: r.ReadCounters().BlockReads,
			files:      rec.SSTablesLoaded,
			replayed:   rec.OpsReplayed, skipped: rec.OpsSkipped,
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		return a
	}

	fromManifest := open("MANIFEST metadata only (default)", opts)

	verifying := opts
	verifying.VerifySSTablesOnOpen = true
	fullScan := open("full verification (Phase 3 behaviour)", verifying)

	// Now compact and measure again.
	c := openLSM(t, dir, opts)
	if _, err := c.CompactAll(); err != nil {
		t.Fatal(err)
	}
	compactedFiles := len(c.SSTables())
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	afterCompaction := open("after compaction", opts)

	t.Logf("Startup — %s", measureEnv())
	t.Logf("dataset: %d keys, %d SSTables before compaction, %d after; "+
		"the WAL is never truncated, so every arm replays the whole log",
		keys, files, compactedFiles)
	t.Logf("%-38s %10s %12s %7s %9s %9s", "arm", "elapsed", "blockReads", "files", "replayed", "skipped")
	for _, a := range []arm{fromManifest, fullScan, afterCompaction} {
		t.Logf("%-38s %10s %12d %7d %9d %9d",
			a.name, a.elapsed.Round(time.Microsecond), a.blockReads, a.files, a.replayed, a.skipped)
	}

	// The claim: the default reads no data blocks at startup, and the Phase 3
	// behaviour reads all of them. That is a counted fact, not a timing.
	if fromManifest.blockReads != 0 {
		t.Fatalf("the default arm read %d data blocks at startup, want 0", fromManifest.blockReads)
	}
	if fullScan.blockReads == 0 {
		t.Fatal("the full-verification arm read no data blocks")
	}
	if afterCompaction.files >= fromManifest.files {
		t.Fatalf("compaction left %d files to open, no fewer than %d",
			afterCompaction.files, fromManifest.files)
	}
}

// TestFilterSizeMeasurement reports what the filter costs on disk and how long it
// takes to build, which is the other half of the Bloom trade.
func TestFilterSizeMeasurement(t *testing.T) {
	const keys = 20000

	build := func(disable bool) (bytes int64, entries uint64, elapsed time.Duration) {
		dir := t.TempDir()
		opts := lsmOpts(storage.DefaultMemTableSize)
		opts.DisableBloomFilter = disable
		s := openLSM(t, dir, opts)
		defer func() { _ = s.Close() }()

		for i := 0; i < keys; i++ {
			mustPut(t, s, fmt.Sprintf("key%07d", i), "value-padding-padding-padding")
		}
		start := time.Now()
		mustFlush(t, s)
		elapsed = time.Since(start)
		for _, f := range s.SSTables() {
			bytes += f.Bytes
			entries += f.Entries
		}
		return bytes, entries, elapsed
	}

	withoutBytes, entries, withoutTime := build(true)
	withBytes, _, withTime := build(false)

	overhead := withBytes - withoutBytes
	t.Logf("Filter size — %s", measureEnv())
	t.Logf("%d keys in one SSTable at %d bits/key", entries, storage.DefaultOptions().BitsPerKey)
	t.Logf("without filter: %d bytes, flush %s", withoutBytes, withoutTime.Round(time.Microsecond))
	t.Logf("with filter:    %d bytes, flush %s", withBytes, withTime.Round(time.Microsecond))
	t.Logf("filter overhead: %d bytes (%.2f bits/key, %.2f%% of the file)",
		overhead, 8*float64(overhead)/float64(entries),
		100*float64(overhead)/float64(withoutBytes))

	if overhead <= 0 {
		t.Fatalf("the filter added %d bytes; it is not being written", overhead)
	}
	// It should cost about bitsPerKey bits per key, plus the 5-byte header. A wild
	// deviation means the sizing arithmetic drifted from docs/DESIGN.md §5.
	gotBits := 8 * float64(overhead) / float64(entries)
	want := float64(storage.DefaultOptions().BitsPerKey)
	if gotBits < want*0.9 || gotBits > want*1.1+1 {
		t.Fatalf("the filter costs %.2f bits/key, want about %.0f", gotBits, want)
	}
}
