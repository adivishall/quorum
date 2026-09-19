package storage

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/adivishall/quorum/internal/storage/compaction"
	"github.com/adivishall/quorum/internal/storage/manifest"
	"github.com/adivishall/quorum/internal/storage/sstable"
)

// Compaction policy (docs/DESIGN.md §7, ADR-007, docs/COMPACTION.md).
//
// Size-tiered, not leveled. ADR-007 chose it because it is simpler to prove
// correct — especially the tombstone rule — and because choosing leveled
// compaction on the strength of a blog post rather than a measurement is the
// cargo-culting this project exists to avoid.
const (
	// DefaultL0CompactionTrigger is the L0 file count that starts a compaction
	// (docs/DESIGN.md §7: "when L0 file count >= 4, merge all L0 files").
	DefaultL0CompactionTrigger = 4

	// DefaultL1MaxBytes is the size budget for level 1. Deeper levels get ten
	// times the level above, per DESIGN §7.
	DefaultL1MaxBytes = 64 << 20

	// levelSizeRatio is DESIGN §7's 10x ratio between levels.
	levelSizeRatio = 10

	// maxLevel bounds the hierarchy. With a 64 MiB L1 and a 10x ratio, level 7
	// budgets 64 TiB, so reaching it is not a case this engine needs to handle
	// gracefully — but a level number has to stop somewhere, and an unbounded
	// one would let a pathological loop compact forever.
	maxLevel = 7
)

// CompactionStats records what compaction has done since the store was opened.
//
// These are development measurements (docs/COMPACTION.md). Phase 5 owns
// benchmarking; nothing here may be quoted as a result.
type CompactionStats struct {
	Runs              int64
	InputFiles        int64
	InputBytes        int64
	OutputBytes       int64
	InputEntries      int64
	OutputEntries     int64
	VersionsDropped   int64
	TombstonesDropped int64
	TombstonesKept    int64
	EmptyOutputs      int64 // compactions whose output held nothing at all
	ObsoleteRemoved   int64
	RemovalFailures   int64
}

// compactionCounters is the atomic form held by the store.
type compactionCounters struct {
	runs              atomic.Int64
	inputFiles        atomic.Int64
	inputBytes        atomic.Int64
	outputBytes       atomic.Int64
	inputEntries      atomic.Int64
	outputEntries     atomic.Int64
	versionsDropped   atomic.Int64
	tombstonesDropped atomic.Int64
	tombstonesKept    atomic.Int64
	emptyOutputs      atomic.Int64
	obsoleteRemoved   atomic.Int64
	removalFailures   atomic.Int64
}

func (c *compactionCounters) snapshot() CompactionStats {
	return CompactionStats{
		Runs:              c.runs.Load(),
		InputFiles:        c.inputFiles.Load(),
		InputBytes:        c.inputBytes.Load(),
		OutputBytes:       c.outputBytes.Load(),
		InputEntries:      c.inputEntries.Load(),
		OutputEntries:     c.outputEntries.Load(),
		VersionsDropped:   c.versionsDropped.Load(),
		TombstonesDropped: c.tombstonesDropped.Load(),
		TombstonesKept:    c.tombstonesKept.Load(),
		EmptyOutputs:      c.emptyOutputs.Load(),
		ObsoleteRemoved:   c.obsoleteRemoved.Load(),
		RemovalFailures:   c.removalFailures.Load(),
	}
}

func (s *LSMStore) countObsoleteRemoved() { s.compactStats.obsoleteRemoved.Add(1) }

func (s *LSMStore) noteObsoleteRemovalFailure(num uint64, err error) {
	s.compactStats.removalFailures.Add(1)
	_ = num
	_ = err
}

// CompactionStats returns the counters.
func (s *LSMStore) CompactionStats() CompactionStats { return s.compactStats.snapshot() }

// levelMaxBytes returns level's size budget: L1MaxBytes * 10^(level-1).
func (s *LSMStore) levelMaxBytes(level int) int64 {
	budget := s.opts.L1MaxBytes
	for i := 1; i < level; i++ {
		budget *= levelSizeRatio
	}
	return budget
}

// compactionJob is a chosen compaction: which files to merge, where the output
// goes, and whether tombstones may be dropped.
type compactionJob struct {
	level      int // the level being compacted
	outLevel   int
	inputs     []*sstFile
	bottomMost bool
	inputBytes int64
}

// pickCompaction chooses the next compaction, or reports that none is due.
//
// The policy is deliberately simple and deterministic, which is what lets a test
// predict it: L0 first, because a large L0 is what makes reads expensive and is
// the only level whose trigger is a file count; then each deeper level in turn,
// by size budget. "Merge all of the level" is DESIGN §7's rule, and it is also
// what keeps the read path's correctness argument intact — see the comment on
// bottomMost below.
//
// It makes no claim to be optimal. It is a v1 policy chosen to be explicable.
func (s *LSMStore) pickCompaction(v *version) (compactionJob, bool) {
	levels := v.byLevel()

	// Level 0: triggered by file count.
	if l0 := levels[0]; len(l0) >= s.opts.L0CompactionTrigger {
		return s.buildJob(v, 0, l0), true
	}
	// Deeper levels: triggered by total size.
	for level := 1; level < maxLevel; level++ {
		files := levels[level]
		if len(files) < 2 {
			// Compacting one file into the next level rewrites it without
			// merging anything, which cannot drop a version or a tombstone that
			// a later compaction would not also drop. Skipping it is what stops
			// a single large file from being copied down the hierarchy forever.
			continue
		}
		var total int64
		for _, f := range files {
			total += f.meta.Size
		}
		if total > s.levelMaxBytes(level) {
			return s.buildJob(v, level, files), true
		}
	}
	return compactionJob{}, false
}

// buildJob assembles a job and decides whether tombstones may be dropped.
func (s *LSMStore) buildJob(v *version, level int, inputs []*sstFile) compactionJob {
	job := compactionJob{level: level, outLevel: level + 1, inputs: inputs}

	var inputMin uint64 = ^uint64(0)
	for _, f := range inputs {
		job.inputBytes += f.meta.Size
		if f.meta.SmallestSeq < inputMin {
			inputMin = f.meta.SmallestSeq
		}
	}

	// A tombstone may only be dropped when this compaction holds the oldest live
	// data in the database, because only then is there nothing underneath it that
	// could still hold a value for the deleted key (docs/COMPACTION.md,
	// docs/DESIGN.md §7, INV-S3).
	//
	// Live files have pairwise-disjoint sequence ranges, so the file holding the
	// global minimum is unique: the input set contains the oldest data exactly
	// when its minimum equals the global minimum.
	var globalMin uint64 = ^uint64(0)
	for _, f := range v.files {
		if f.meta.SmallestSeq < globalMin {
			globalMin = f.meta.SmallestSeq
		}
	}
	job.bottomMost = len(v.files) > 0 && inputMin == globalMin
	return job
}

// Compact runs one compaction if the policy says one is due, and reports whether
// it ran.
//
// It exists for tests and for an operator who wants the work done now. The store
// compacts on its own; nothing about correctness depends on this being called.
func (s *LSMStore) Compact() (bool, error) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return false, opErr("compact", nil, ErrClosed)
	}
	ran, err := s.compactOnce()
	if err != nil {
		return false, classify("compact", nil, err)
	}
	return ran, nil
}

// CompactAll runs compactions until the policy is satisfied.
//
// Bounded so that a policy bug becomes a test failure rather than a hang.
func (s *LSMStore) CompactAll() (int, error) {
	const limit = 200
	var n int
	for i := 0; i < limit; i++ {
		ran, err := s.Compact()
		if err != nil {
			return n, err
		}
		if !ran {
			return n, nil
		}
		n++
	}
	return n, fmt.Errorf("lsm: compaction did not converge after %d runs", limit)
}

// compactOnce performs at most one compaction.
//
// The shape of this function is the concurrency design (docs/COMPACTION.md):
//
//	pick        under mu (read), against an acquired version
//	merge       under NO store lock at all — the long part
//	publish     under writeMu, briefly: one manifest append and one pointer swap
//
// Holding a lock across the merge would make compaction exclude reads and writes
// for as long as it takes to rewrite gigabytes, which is the thing an LSM engine
// exists to avoid. The acquired version is what makes the lock-free middle safe:
// the input files cannot be closed or deleted underneath the merge, because the
// version holding them has a reference.
func (s *LSMStore) compactOnce() (bool, error) {
	// One compaction at a time. Two concurrent compactions could pick
	// overlapping input sets and the second would publish a version derived from
	// a file set the first had already replaced.
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	v := s.acquire()
	if v == nil {
		return false, nil
	}
	defer s.release(v)

	job, ok := s.pickCompaction(v)
	if !ok {
		return false, nil
	}

	num, err := s.allocFileNum()
	if err != nil {
		return false, err
	}

	meta, produced, err := s.runMerge(job, num)
	if err != nil {
		return false, err
	}
	if err := s.publishCompaction(job, num, meta, produced); err != nil {
		if produced {
			// The output was renamed into place but never referenced. Removing it
			// here is tidiness, not correctness: it is already an orphan and the
			// next startup would sweep it.
			_ = os.Remove(s.sstPath(num))
		}
		return false, err
	}

	s.compactStats.runs.Add(1)
	s.compactStats.inputFiles.Add(int64(len(job.inputs)))
	s.compactStats.inputBytes.Add(job.inputBytes)
	if produced {
		s.compactStats.outputBytes.Add(meta.FileSize)
	}
	return true, nil
}

// runMerge streams the inputs into a new SSTable and publishes it to its final
// name. It holds no store lock.
//
// produced is false when the merge emitted nothing — every input entry was an
// obsolete version or a droppable tombstone. That is a real outcome, not an
// error: the compaction's result is that all of those files should cease to
// exist. An empty SSTable is never written, because the engine refuses to open
// one and it would carry no information.
func (s *LSMStore) runMerge(job compactionJob, num uint64) (sstable.Metadata, bool, error) {
	streams := make([]compaction.Stream, 0, len(job.inputs))
	for _, f := range job.inputs {
		streams = append(streams, f.r.NewIterator())
	}
	m := compaction.New(streams, compaction.Options{BottomMost: job.bottomMost})

	tmpPath := s.tmpPath(num)
	meta, err := sstable.WriteFile(tmpPath, m, s.writerOptions())
	if err != nil {
		_ = os.Remove(tmpPath)
		return sstable.Metadata{}, false, fmt.Errorf("lsm: compacting level %d into %s: %w",
			job.level, sstName(num), err)
	}

	st := m.Stats()
	s.compactStats.inputEntries.Add(st.InputEntries)
	s.compactStats.outputEntries.Add(st.OutputEntries)
	s.compactStats.versionsDropped.Add(st.VersionsDropped)
	s.compactStats.tombstonesDropped.Add(st.TombstonesDropped)
	s.compactStats.tombstonesKept.Add(st.TombstonesKept)

	if meta.NumEntries == 0 {
		_ = os.Remove(tmpPath)
		s.compactStats.emptyOutputs.Add(1)
		return sstable.Metadata{}, false, nil
	}

	// Same publication order as a flush, for the same reasons (docs/LSM.md §7):
	// the file is complete and fsynced before any name refers to it, the rename
	// is atomic, and the directory fsync makes the rename itself durable.
	if err := os.Rename(tmpPath, s.sstPath(num)); err != nil {
		_ = os.Remove(tmpPath)
		return sstable.Metadata{}, false, fmt.Errorf("lsm: publishing %s: %w", sstName(num), err)
	}
	if err := sstable.SyncDir(s.dir); err != nil {
		return sstable.Metadata{}, false, fmt.Errorf("lsm: syncing %s after publishing %s: %w",
			s.dir, sstName(num), err)
	}
	return meta, true, nil
}

// publishCompaction installs the compaction's result.
//
// This is the only part that synchronises, and it is the step the whole MANIFEST
// exists for. The order is docs/DESIGN.md §6's commit protocol:
//
//	the output file already exists, fsynced, under its final name
//	append AddFile(output) + DeleteFile(inputs) as ONE record, and fsync it
//	swap the in-memory version pointer
//	unlink the inputs once no reader holds them (release -> reap)
//
// A crash before the manifest append leaves the output as an unreferenced orphan
// and the old file set authoritative. A crash after it leaves the inputs as
// orphans and the new file set authoritative. There is no third outcome, and
// neither requires guessing.
func (s *LSMStore) publishCompaction(job compactionJob, num uint64, meta sstable.Metadata, produced bool) error {
	var (
		r   *sstable.Reader
		err error
	)
	if produced {
		// Opened before the manifest append, so a file that cannot be opened is
		// found before it is declared live rather than after.
		r, err = sstable.Open(s.sstPath(num))
		if err != nil {
			return err
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.RLock()
	cur := s.cur
	s.mu.RUnlock()
	if cur == nil {
		if r != nil {
			_ = r.Close()
		}
		return ErrClosed
	}

	// The inputs must still be live. Only compaction retires a file and only one
	// compaction runs at a time, so this cannot fail; it is checked because
	// publishing an edit that deletes a file the manifest no longer holds would
	// be refused by manifest.Apply at the next startup, turning a logic error
	// here into a database that will not open.
	for _, f := range job.inputs {
		if cur.fileByNum(f.meta.Num) == nil {
			if r != nil {
				_ = r.Close()
			}
			return fmt.Errorf("lsm: compaction input %s is no longer live; "+
				"the file set changed underneath a compaction", sstName(f.meta.Num))
		}
	}

	var edit manifest.Edit
	var out *sstFile
	if produced {
		fm := manifest.FileMeta{
			Level:       job.outLevel,
			Num:         num,
			Size:        meta.FileSize,
			NumEntries:  meta.NumEntries,
			SmallestKey: meta.SmallestKey,
			LargestKey:  meta.LargestKey,
			SmallestSeq: meta.SmallestSeq,
			LargestSeq:  meta.LargestSeq,
		}
		edit.AddFile(fm)
		out = &sstFile{meta: fm, r: r, path: s.sstPath(num)}
	}
	for _, f := range job.inputs {
		edit.DeleteFile(f.meta.Level, f.meta.Num)
	}
	edit.SetNextFileNum(s.nextNum)

	if err := s.manifest.Append(&edit); err != nil {
		if r != nil {
			_ = r.Close()
		}
		return fmt.Errorf("lsm: recording compaction in the manifest: %w", err)
	}

	// Build the new file set: everything still live, minus the inputs, plus the
	// output. Files that appeared while the merge ran — a flush's new L0 file —
	// are in cur.files and are not in the input set, so they carry over
	// untouched. That is the property TestFlushDuringCompactionIsNotLost covers.
	retired := make(map[uint64]bool, len(job.inputs))
	for _, f := range job.inputs {
		retired[f.meta.Num] = true
	}
	files := make([]*sstFile, 0, len(cur.files)+1)
	for _, f := range cur.files {
		if !retired[f.meta.Num] {
			files = append(files, f)
		}
	}
	if out != nil {
		files = append(files, out)
	}

	s.install(newVersion(cur.mem, cur.imm, files))
	return nil
}

// ---------------------------------------------------------------- background

// compactionSignal asks the background compactor to look for work. It never
// blocks: the channel has room for one pending wake-up, and more than one
// pending wake-up would mean the same thing as one.
func (s *LSMStore) compactionSignal() {
	if s.opts.DisableAutoCompaction {
		return
	}
	select {
	case s.compactWake <- struct{}{}:
	default:
	}
}

// runCompactor is the background compaction loop.
//
// Compaction runs on its own goroutine because it must not make writers wait.
// Phase 3 kept the flush synchronous deliberately, to avoid adding a scheduler to
// a phase whose job was to be obviously correct; compaction does not have that
// option, because it rewrites whole levels and blocking writes for the duration
// would defeat the point.
//
// The loop is deliberately dull: wake, compact until nothing is due, sleep. There
// is no rate limiting and no I/O budget, which is a real limitation and is
// recorded in docs/LIMITATIONS.md rather than described as a policy.
func (s *LSMStore) runCompactor() {
	defer close(s.compactStopped)
	for {
		select {
		case <-s.compactQuit:
			return
		case <-s.compactWake:
		}

		for {
			select {
			case <-s.compactQuit:
				return
			default:
			}

			s.closeMu.RLock()
			if s.closed {
				s.closeMu.RUnlock()
				return
			}
			ran, err := s.compactOnce()
			s.closeMu.RUnlock()

			if err != nil {
				// A failed compaction is latched the same way a failed flush is:
				// the data is intact and still readable, but continuing to
				// accumulate files while something is wrong would turn one
				// failure into an unbounded one.
				s.setCompactErr(err)
				break
			}
			if !ran {
				break
			}
		}
	}
}

func (s *LSMStore) setCompactErr(err error) {
	s.writeMu.Lock()
	if s.compactErr == nil {
		s.compactErr = err
	}
	s.writeMu.Unlock()
}

// CompactionError returns the latched background-compaction failure, if any.
//
// Compaction failing does not make the database wrong — every acknowledged write
// is still in the WAL and still readable — so it does not fail reads or writes.
// It does mean the file count is no longer being controlled, which an operator
// needs to know about, so it is exposed rather than only logged.
func (s *LSMStore) CompactionError() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.compactErr
}
