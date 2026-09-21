package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/adivishall/quorum/internal/storage/ikey"
	"github.com/adivishall/quorum/internal/storage/manifest"
	"github.com/adivishall/quorum/internal/storage/memtable"
	"github.com/adivishall/quorum/internal/storage/sstable"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// SSTable file naming. Numbers ascend with creation order, so a higher number is
// a newer file. Six digits matches the WAL's segment naming.
//
// Since Phase 4 the numbering may contain gaps: a compaction allocates a number
// before it knows whether it will produce output, and a failed compaction leaves
// its number unused. That is not a problem the way it was in Phase 3, because the
// MANIFEST now says exactly which files exist and a gap carries no information.
const (
	sstSuffix     = ".sst"
	sstTempSuffix = ".sst.tmp"
	sstDigits     = 6
	maxSSTNumber  = uint64(999999)
)

func sstName(n uint64) string     { return fmt.Sprintf("%0*d%s", sstDigits, n, sstSuffix) }
func sstTempName(n uint64) string { return fmt.Sprintf("%0*d%s", sstDigits, n, sstTempSuffix) }

// LSMStore is Quorum's durable log-structured storage engine.
//
//	write:      WAL append  ->  memtable
//	flush:      memtable    ->  SSTable at level 0, recorded in the MANIFEST
//	compaction: level N     ->  one SSTable at level N+1, recorded in the MANIFEST
//	read:       memtable -> immutable memtables -> SSTables, newest first,
//	            each SSTable's Bloom filter consulted before its blocks
//
// # What the MANIFEST changed
//
// Phase 3 inferred the live file set by listing the directory. Phase 4 reads it
// from the MANIFEST, which is the only authority (INV-S6): a file on disk that
// the MANIFEST does not name is an orphan and is deleted, and a file the MANIFEST
// names but that is missing is fatal. That distinction is what makes compaction
// possible at all — after a crash mid-compaction both the inputs and the output
// are on disk, and only the MANIFEST can say which set is the database.
//
// # Locking
//
//	closeMu    held for read by every operation for its whole duration; Close
//	           takes it for write. It is a drain barrier, not a data lock.
//	writeMu    serialises writers across sequence assignment, the log append and
//	           the memtable publish — and serialises every MANIFEST append,
//	           whether it comes from a flush or from a compaction's publication.
//	compactMu  admits one compaction at a time.
//	mu         guards the current-version pointer and the per-file reference
//	           counts. Critical sections are pointer swaps; a reader never waits
//	           on disk I/O and a publication never waits on a reader.
//
// Order is always closeMu -> compactMu -> writeMu -> mu, and never the reverse.
//
// # Reads during compaction
//
// A read acquires the current version once and holds it for the operation, so it
// observes one coherent file set (INV-S5). A compaction merges with no store lock
// held at all, and synchronises only to append its MANIFEST record and swap the
// version pointer. The version's reference count is what keeps the merge's input
// files open while that happens, and what delays unlinking a retired file until
// no reader can reach it.
type LSMStore struct {
	opts Options
	dir  string

	closeMu sync.RWMutex
	closed  bool

	writeMu sync.Mutex
	seq     uint64 // last assigned sequence number; writeMu
	nextNum uint64 // next unused file number; writeMu

	// flushErr latches a failed flush. The mutation that triggered it genuinely
	// succeeded — it is in the log and visible in memory — so failing that call
	// would report a write that happened as one that did not. The next mutation
	// reports it instead, which also stops the memtable growing without bound.
	flushErr error

	// compactErr latches a failed background compaction. It does not fail reads
	// or writes: nothing is wrong with the data, only with the file count.
	compactErr error

	manifest *manifest.Writer // writeMu

	mu  sync.RWMutex
	cur *version

	applied AppliedIndex

	w        *wal.WAL
	recovery LSMRecovery

	compactMu      sync.Mutex
	compactStats   compactionCounters
	flushStats     flushCounters
	compactWake    chan struct{}
	compactQuit    chan struct{}
	compactStopped chan struct{}
	stopOnce       sync.Once
}

var _ Store = (*LSMStore)(nil)

// LSMRecovery summarises what opening the store found on disk.
type LSMRecovery struct {
	// MANIFEST.
	ManifestNum       uint64
	ManifestEdits     int
	ManifestTruncated bool
	Bootstrapped      bool // no MANIFEST existed; one was created
	AdoptedLegacy     int  // Phase 3 SSTables adopted into a new MANIFEST

	// SSTables.
	SSTablesLoaded   int
	SSTableEntries   uint64
	SSTableBytes     int64
	TempFilesRemoved int
	OrphansRemoved   int
	ManifestsRemoved int
	FullyVerified    bool
	MaxFlushedSeq    uint64

	// WAL replay.
	SegmentsScanned int
	BytesScanned    int64
	RecordsApplied  int64
	BatchesApplied  int64
	OpsReplayed     int64
	OpsSkipped      int64 // already durable in an SSTable
	FlushesOnReplay int

	// Sequence is the last sequence number assigned during replay, which is the
	// total number of mutations the WAL holds.
	Sequence uint64

	// Truncated reports a repaired torn tail in the WAL.
	Truncated       bool
	TruncatedFile   string
	TruncatedAt     int64
	TruncatedBytes  int64
	TruncatedReason string
}

// OpenLSMStore opens or creates a durable LSM store in dir.
//
// The startup sequence, and why it is in this order:
//
//  1. Read CURRENT and replay the MANIFEST it names. That produces the
//     authoritative live file set, each file's sequence range, and the next file
//     number. A torn final record is a crash during that append and is repaired;
//     anything else is refused.
//  2. Open every referenced file and cross-check it against what the MANIFEST
//     claims. A referenced file that is missing is fatal — it is the one state
//     the publication protocol cannot produce.
//  3. Check the file set is coherent: sequence ranges disjoint and ascending,
//     no empty tables. This is what the read path's "first source wins" rests on.
//  4. Sweep orphans: *.sst.tmp, *.sst the MANIFEST does not name, and superseded
//     manifests. Safe only because step 1 succeeded, which is why it is here and
//     not earlier.
//  5. Install a fresh MANIFEST holding a snapshot, so a manifest never grows
//     without bound and recovery never replays more than one database's history.
//  6. Replay the WAL, assigning sequence numbers from 1 in log order. A mutation
//     at or below the highest flushed sequence is already in a table and is
//     skipped.
//
// Step 6 is unchanged from Phase 3 and rests on the same property: sequence
// assignment is deterministic, so "seq <= maxFlushedSeq" is exactly "this
// mutation is already durable in a table". Compaction does not disturb it — a
// compacted file represents the combined effect of every mutation in its range,
// so skipping that range is still correct.
func OpenLSMStore(dir string, opts Options) (*LSMStore, error) {
	// Validate before filling in defaults, or a negative value would be
	// "corrected" into the default and a caller's mistake would go unreported.
	if err := opts.validate(); err != nil {
		return nil, err
	}
	opts.applyLSMDefaults()
	if dir == "" {
		return nil, opErr("open", nil, fmt.Errorf("%w: data directory is empty", ErrInvalidOptions))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, classify("open", nil, err)
	}

	s := &LSMStore{
		opts:           opts,
		dir:            dir,
		compactWake:    make(chan struct{}, 1),
		compactQuit:    make(chan struct{}),
		compactStopped: make(chan struct{}),
	}

	state, err := s.recoverManifest()
	if err != nil {
		return nil, classify("open", nil, err)
	}

	files, err := s.openFiles(state.Files)
	if err != nil {
		for _, f := range files {
			_ = f.r.Close()
		}
		return nil, classify("open", nil, err)
	}
	if err := checkCoherentFileSet(files); err != nil {
		for _, f := range files {
			_ = f.r.Close()
		}
		return nil, classify("open", nil, err)
	}

	s.nextNum = state.NextFileNum
	if s.nextNum == 0 {
		s.nextNum = 1
	}
	s.applied = AppliedIndex{Index: state.Applied.Index, Term: state.Applied.Term}
	s.install(newVersion(memtable.New(opts.MemTableSeed), nil, files))

	s.recovery.SSTablesLoaded = len(files)
	for _, f := range files {
		s.recovery.SSTableEntries += f.meta.NumEntries
		s.recovery.SSTableBytes += f.meta.Size
	}
	s.recovery.MaxFlushedSeq = s.cur.maxFlushedSeq()

	if err := s.sweepOrphans(files); err != nil {
		s.closeAllReaders()
		return nil, classify("open", nil, err)
	}

	// A fresh manifest holding a snapshot of the recovered state. Installing it
	// here, before replay, means a flush triggered during replay records itself
	// through exactly the same path as one during normal operation.
	if err := s.installManifest(state); err != nil {
		s.closeAllReaders()
		return nil, classify("open", nil, err)
	}

	if err := s.replayWAL(); err != nil {
		s.closeManifest()
		s.closeAllReaders()
		return nil, classify("open", nil, err)
	}

	w, err := wal.Create(filepath.Join(dir, walDirName), opts.WAL)
	if err != nil {
		s.closeManifest()
		s.closeAllReaders()
		return nil, classify("open", nil, err)
	}
	s.w = w

	go s.runCompactor()
	// A store that opens with a level already over its trigger — because the
	// previous process died before compacting, or because replay produced many
	// files — should start catching up without waiting for a write.
	s.compactionSignal()

	return s, nil
}

// recoverManifest reads the MANIFEST, or bootstraps one.
func (s *LSMStore) recoverManifest() (manifest.State, error) {
	state, rec, err := manifest.Recover(s.dir)
	switch {
	case err == nil:
		s.recovery.ManifestNum = rec.Num
		s.recovery.ManifestEdits = rec.EditsApplied
		s.recovery.ManifestTruncated = rec.Truncated
		return state, nil
	case errors.Is(err, manifest.ErrNoManifest):
		return s.bootstrapState()
	default:
		return manifest.State{}, err
	}
}

// bootstrapState decides what a directory with no CURRENT means.
//
// An empty directory is simply a database that does not exist yet. A directory
// that already holds SSTables is a different matter: it is either a Phase 3
// database, or a Phase 4 database whose CURRENT was lost. Nothing on the
// filesystem distinguishes those, and treating the second as the first would
// reduce INV-S6 to a suggestion — delete CURRENT and the engine quietly goes back
// to guessing the file set from the directory, which is the behaviour the MANIFEST
// exists to replace.
//
// So it refuses, and the Phase 3 upgrade is an explicit opt-in that says "these
// files are a Phase 3 database, adopt them". That is a deliberate operator
// decision rather than a silent fallback.
func (s *LSMStore) bootstrapState() (manifest.State, error) {
	nums, err := s.listSSTables()
	if err != nil {
		return manifest.State{}, err
	}
	if len(nums) == 0 {
		s.recovery.Bootstrapped = true
		return manifest.State{NextFileNum: 1}, nil
	}
	if !s.opts.AdoptLegacySSTables {
		return manifest.State{}, fmt.Errorf(
			"lsm: %s holds %d SSTables but no %s; the MANIFEST is the only authority on "+
				"which files are live, so a missing one cannot be worked around by reading the "+
				"directory. If this is a Phase 3 data directory, open it once with "+
				"Options.AdoptLegacySSTables: %w",
			s.dir, len(nums), manifest.CurrentName, ErrCorrupt)
	}

	// The Phase 3 mechanism, run once: fully read each file to recover the
	// sequence range that had nowhere on disk to live before the MANIFEST.
	state := manifest.State{NextFileNum: 1}
	for _, n := range nums {
		r, err := sstable.Open(s.sstPath(n))
		if err != nil {
			return manifest.State{}, err
		}
		stats, err := r.Verify()
		if err != nil {
			_ = r.Close()
			return manifest.State{}, err
		}
		_ = r.Close()
		if stats.NumEntries == 0 {
			return manifest.State{}, fmt.Errorf(
				"lsm: %s holds no entries; a flush never produces an empty table: %w",
				sstName(n), ErrCorrupt)
		}
		state.Files = append(state.Files, manifest.FileMeta{
			Level:       0,
			Num:         n,
			Size:        stats.FileSize,
			NumEntries:  stats.NumEntries,
			SmallestKey: stats.SmallestKey,
			LargestKey:  stats.LargestKey,
			SmallestSeq: stats.SmallestSeq,
			LargestSeq:  stats.LargestSeq,
		})
		state.NextFileNum = n + 1
		if stats.LargestSeq > state.LastSequence {
			state.LastSequence = stats.LargestSeq
		}
	}
	s.recovery.AdoptedLegacy = len(state.Files)
	s.recovery.Bootstrapped = true
	return state, nil
}

// listSSTables returns the SSTable numbers present in the directory, ascending.
func (s *LSMStore) listSSTables() ([]uint64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("lsm: reading %s: %w", s.dir, err)
	}
	var nums []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if n, ok := parseSSTName(e.Name()); ok {
			nums = append(nums, n)
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// parseSSTName parses an SSTable file name. The match is exact — six decimal
// digits and ".sst" — so a stray file cannot be mistaken for table data.
func parseSSTName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, sstSuffix) || strings.HasSuffix(name, sstTempSuffix) {
		return 0, false
	}
	digits := strings.TrimSuffix(name, sstSuffix)
	if len(digits) != sstDigits {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	return n, err == nil
}

// openFiles opens every file the MANIFEST names and cross-checks it.
//
// The cross-check is what makes it defensible to skip the full scan Phase 3 did.
// The MANIFEST records each file's size and entry count, and the file's own footer
// records them independently, so a disagreement means one of the two is damaged
// and is caught here for the price of reading a 48-byte footer.
func (s *LSMStore) openFiles(metas []manifest.FileMeta) ([]*sstFile, error) {
	sorted := append([]manifest.FileMeta(nil), metas...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Num < sorted[j].Num })

	files := make([]*sstFile, 0, len(sorted))
	for _, m := range sorted {
		path := s.sstPath(m.Num)
		r, err := sstable.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return files, fmt.Errorf(
					"lsm: the MANIFEST references %s, which is not on disk; the publication "+
						"protocol never removes a referenced file, so this is not a state a "+
						"crash can produce: %w", sstName(m.Num), ErrCorrupt)
			}
			return files, err
		}
		if got := r.NumEntries(); got != m.NumEntries {
			_ = r.Close()
			return files, fmt.Errorf(
				"lsm: %s holds %d entries but the MANIFEST records %d: %w",
				sstName(m.Num), got, m.NumEntries, ErrCorrupt)
		}
		if got := r.Size(); got != m.Size {
			_ = r.Close()
			return files, fmt.Errorf("lsm: %s is %d bytes but the MANIFEST records %d: %w",
				sstName(m.Num), got, m.Size, ErrCorrupt)
		}

		if s.opts.VerifySSTablesOnOpen {
			stats, verr := r.Verify()
			if verr != nil {
				_ = r.Close()
				return files, verr
			}
			if stats.SmallestSeq != m.SmallestSeq || stats.LargestSeq != m.LargestSeq {
				_ = r.Close()
				return files, fmt.Errorf(
					"lsm: %s covers sequences [%d,%d] but the MANIFEST records [%d,%d]: %w",
					sstName(m.Num), stats.SmallestSeq, stats.LargestSeq,
					m.SmallestSeq, m.LargestSeq, ErrCorrupt)
			}
		}

		files = append(files, &sstFile{meta: m, r: r, path: path})
	}
	s.recovery.FullyVerified = s.opts.VerifySSTablesOnOpen
	return files, nil
}

// checkCoherentFileSet enforces the property the read path depends on: live files
// have pairwise-disjoint sequence ranges.
//
// "First source holding any version of the key wins" is only correct if every
// sequence number in a newer file exceeds every sequence number in an older one.
// Both a flush and a compaction preserve that — a flush's range is above
// everything, and a compaction merges a whole level, whose range is contiguous in
// the global ordering — so a violation means the file set is not one this engine
// produced, and continuing would resolve some keys to the wrong version.
func checkCoherentFileSet(files []*sstFile) error {
	ordered := append([]*sstFile(nil), files...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].meta.LargestSeq > ordered[j].meta.LargestSeq
	})
	for i, f := range ordered {
		if f.meta.NumEntries == 0 {
			return fmt.Errorf("lsm: %s holds no entries; a flush or compaction never "+
				"produces an empty table: %w", sstName(f.meta.Num), ErrCorrupt)
		}
		if f.meta.SmallestSeq > f.meta.LargestSeq {
			return fmt.Errorf("lsm: %s declares an inverted sequence range [%d,%d]: %w",
				sstName(f.meta.Num), f.meta.SmallestSeq, f.meta.LargestSeq, ErrCorrupt)
		}
		if i == 0 {
			continue
		}
		prev := ordered[i-1]
		if f.meta.LargestSeq >= prev.meta.SmallestSeq {
			return fmt.Errorf(
				"lsm: %s covers sequences [%d,%d] which overlaps %s's [%d,%d]; "+
					"live files must not overlap: %w",
				sstName(f.meta.Num), f.meta.SmallestSeq, f.meta.LargestSeq,
				sstName(prev.meta.Num), prev.meta.SmallestSeq, prev.meta.LargestSeq, ErrCorrupt)
		}
	}
	return nil
}

// sweepOrphans deletes what the MANIFEST does not name.
//
// Every category is explicit, and anything unrecognised is left alone
// (docs/MANIFEST.md):
//
//	*.sst.tmp            a flush or compaction interrupted mid-write. Never read.
//	*.sst not in the     an output whose MANIFEST record never landed, or an
//	MANIFEST             input whose deletion never completed. Either way it is
//	                     not part of the database.
//	superseded MANIFESTs  replaced by the one CURRENT names.
//	anything else         ignored. This engine does not own the whole directory.
//
// This runs only after the MANIFEST has been recovered successfully. Deleting a
// file because it is absent from a file set we are not yet sure of would be the
// one way this could lose data.
func (s *LSMStore) sweepOrphans(live []*sstFile) error {
	referenced := make(map[uint64]bool, len(live))
	for _, f := range live {
		referenced[f.meta.Num] = true
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("lsm: reading %s: %w", s.dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case strings.HasSuffix(name, sstTempSuffix):
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
				return fmt.Errorf("lsm: removing partial output %s: %w", name, err)
			}
			s.recovery.TempFilesRemoved++
		default:
			n, ok := parseSSTName(name)
			if !ok || referenced[n] {
				continue
			}
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
				return fmt.Errorf("lsm: removing orphan %s: %w", name, err)
			}
			s.recovery.OrphansRemoved++
		}
	}
	return nil
}

// installManifest writes a fresh manifest holding a snapshot and retires the old
// ones.
func (s *LSMStore) installManifest(state manifest.State) error {
	state.Files = s.cur.metas()
	state.NextFileNum = s.nextNum
	if state.LastSequence < s.cur.maxFlushedSeq() {
		state.LastSequence = s.cur.maxFlushedSeq()
	}

	// The new manifest's number must be above every manifest ON DISK, not merely
	// above the one CURRENT named.
	//
	// Two situations produce a leftover manifest with a number this open would
	// otherwise reuse: an Install interrupted before it could point CURRENT at its
	// output, and a directory whose CURRENT was lost while its manifests remain.
	// Reusing the number fails on the O_EXCL create — Install refuses to overwrite
	// a manifest, which is the right instinct — and the store would not open at
	// all, so the number has to be chosen against the directory.
	existing, err := manifest.List(s.dir)
	if err != nil {
		return err
	}
	num := s.recovery.ManifestNum + 1
	for _, n := range existing {
		if n >= num {
			num = n + 1
		}
	}
	if num == 0 {
		num = 1
	}

	w, err := manifest.Install(s.dir, num, state)
	if err != nil {
		return err
	}
	s.manifest = w

	removed, err := manifest.RemoveObsolete(s.dir, num)
	if err != nil {
		return err
	}
	s.recovery.ManifestsRemoved = removed
	s.recovery.ManifestNum = num
	return nil
}

func (s *LSMStore) closeManifest() {
	if s.manifest != nil {
		_ = s.manifest.Close()
		s.manifest = nil
	}
}

// replayWAL rebuilds the memtable from the portion of the log that is not yet in
// an SSTable.
func (s *LSMStore) replayWAL() error {
	maxFlushed := s.recovery.MaxFlushedSeq

	rec, err := wal.Recover(filepath.Join(s.dir, walDirName), wal.Handler{
		Batch: func(b wal.Batch) error {
			for _, op := range b {
				s.seq++
				if s.seq <= maxFlushed {
					s.recovery.OpsSkipped++
					continue
				}
				kind := ikey.KindValue
				if op.Kind == wal.OpDelete {
					kind = ikey.KindTombstone
				}
				s.cur.mem.Add(s.seq, kind, op.Key, op.Value)
				s.recovery.OpsReplayed++

				// Flush during replay for the same reason as during normal
				// operation: without it, recovering a log larger than memory
				// would need memory proportional to the whole log.
				if s.cur.mem.ApproxSize() >= s.opts.MemTableSize {
					if err := s.flushLocked(); err != nil {
						return err
					}
					s.recovery.FlushesOnReplay++
				}
			}
			return nil
		},
		Applied: func(a wal.AppliedIndex) error {
			s.applied = AppliedIndex{Index: a.Index, Term: a.Term}
			return nil
		},
	})
	if err != nil {
		return err
	}

	if maxFlushed > s.seq {
		// Tables hold sequence numbers the log never contained, so the log was
		// truncated or replaced. Replaying it would produce a state where flushed
		// writes are present but later ones are not, with no way to tell which.
		return fmt.Errorf(
			"lsm: SSTables cover sequences up to %d but the WAL holds only %d mutations; "+
				"the log is shorter than the tables built from it: %w",
			maxFlushed, s.seq, ErrCorrupt)
	}

	s.recovery.SegmentsScanned = rec.SegmentsScanned
	s.recovery.BytesScanned = rec.BytesScanned
	s.recovery.RecordsApplied = rec.RecordsApplied
	s.recovery.BatchesApplied = rec.BatchesApplied
	s.recovery.Sequence = s.seq
	s.recovery.Truncated = rec.Truncated
	s.recovery.TruncatedFile = rec.TruncatedFile
	s.recovery.TruncatedAt = rec.TruncatedAt
	s.recovery.TruncatedBytes = rec.TruncatedBytes
	s.recovery.TruncatedReason = rec.TruncatedReason
	return nil
}

// ---------------------------------------------------------------- write path

// Put implements Store.
func (s *LSMStore) Put(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return opErr("put", key, err)
	}
	if err := s.opts.validateKey("put", key); err != nil {
		return err
	}
	if err := s.opts.validateValue("put", key, value); err != nil {
		return err
	}
	return s.mutate("put", ikey.KindValue, key, value)
}

// Delete implements Store.
//
// Delete writes a tombstone. It performs no read, which is why it is idempotent
// and does not report whether the key existed.
func (s *LSMStore) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return opErr("delete", key, err)
	}
	if err := s.opts.validateKey("delete", key); err != nil {
		return err
	}
	return s.mutate("delete", ikey.KindTombstone, key, nil)
}

func (s *LSMStore) mutate(op string, kind ikey.Kind, key, value []byte) error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return opErr(op, key, ErrClosed)
	}

	var flushed bool
	if err := func() error {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()

		if s.flushErr != nil {
			return classify(op, key, s.flushErr)
		}
		if s.seq >= ikey.MaxSeq {
			return opErr(op, key, fmt.Errorf("%w: the %d-bit sequence space is exhausted",
				ErrIO, 8*ikey.SeqBytes))
		}

		walKind := wal.OpPut
		if kind == ikey.KindTombstone {
			walKind = wal.OpDelete
		}
		if err := s.w.AppendBatch(wal.Batch{{Kind: walKind, Key: key, Value: value}}); err != nil {
			// Nothing is published. The log may hold a partial record, which
			// recovery repairs as a torn tail; either way the caller is told the
			// write did not succeed.
			return classify(op, key, err)
		}

		s.seq++
		s.mu.RLock()
		mem := s.cur.mem
		s.mu.RUnlock()
		mem.Add(s.seq, kind, key, value)

		if mem.ApproxSize() >= s.opts.MemTableSize {
			if err := s.flushLocked(); err != nil {
				// The mutation itself succeeded: it is in the log and visible in
				// memory. The failure latches and the next mutation reports it.
				s.flushErr = err
			} else {
				flushed = true
			}
		}
		return nil
	}(); err != nil {
		return err
	}

	// Signalled outside writeMu: the compactor takes writeMu to publish, and
	// signalling under it would be a lock-ordering hazard for no benefit.
	if flushed {
		s.compactionSignal()
	}
	return nil
}

// allocFileNum reserves the next file number.
func (s *LSMStore) allocFileNum() (uint64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.allocFileNumLocked()
}

func (s *LSMStore) allocFileNumLocked() (uint64, error) {
	if s.nextNum > maxSSTNumber {
		return 0, fmt.Errorf("lsm: SSTable number %d would exceed the %d-digit name format",
			s.nextNum, sstDigits)
	}
	n := s.nextNum
	s.nextNum++
	return n, nil
}

func (s *LSMStore) tmpPath(num uint64) string {
	return filepath.Join(s.dir, sstTempName(num))
}

func (s *LSMStore) writerOptions() sstable.WriterOptions {
	return sstable.WriterOptions{
		BlockSize:     s.opts.BlockSize,
		BitsPerKey:    s.opts.BitsPerKey,
		DisableFilter: s.opts.DisableBloomFilter,
	}
}

// ---------------------------------------------------------------- flush

// Flush writes the current memtable to an SSTable, whatever its size.
func (s *LSMStore) Flush() error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return opErr("flush", nil, ErrClosed)
	}

	if err := func() error {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		if s.flushErr != nil {
			return classify("flush", nil, s.flushErr)
		}
		if err := s.flushLocked(); err != nil {
			s.flushErr = err
			return classify("flush", nil, err)
		}
		return nil
	}(); err != nil {
		return err
	}
	s.compactionSignal()
	return nil
}

// flushLocked turns the current memtable into a level-0 SSTable. writeMu must be
// held.
//
// The order is the crash contract, and each step is chosen for what a crash
// immediately after it leaves behind:
//
//	freeze + swap   the memtable stops taking writes and stays readable as an
//	                immutable source; a fresh one takes the writes.
//	write to .tmp   a crash here leaves a partial file under a name no reader
//	                consults and the MANIFEST does not name. Startup deletes it.
//	fsync the file  the bytes are on the device before any name refers to them.
//	rename          atomic on POSIX.
//	fsync the dir   makes the rename durable.
//	MANIFEST append one record, fsynced. THIS is the instant the file becomes
//	                part of the database. Before it, the file is an orphan.
//	publish         one version swap adds the table and drops the immutable
//	                memtable together.
//
// The difference from Phase 3 is the MANIFEST append. In Phase 3 a complete *.sst
// was adopted at startup because the directory was the authority; now it is
// ignored and deleted unless the MANIFEST names it, and the WAL replays its
// contents instead. Both reconstruct the same logical state — but only one of them
// also works when the file is a compaction output that was never meant to be live.
func (s *LSMStore) flushLocked() error {
	s.mu.RLock()
	cur := s.cur
	s.mu.RUnlock()
	if cur == nil {
		return ErrClosed
	}
	if cur.mem.Empty() {
		return nil
	}

	old := cur.mem
	old.Freeze()
	fresh := memtable.New(s.opts.MemTableSeed)

	// Writers continue against the fresh memtable immediately; readers keep
	// finding the frozen one because it is in imm.
	s.install(newVersion(fresh, append([]*memtable.MemTable{old}, cur.imm...), cur.files))

	num, err := s.allocFileNumLocked()
	if err != nil {
		return err
	}
	tmpPath := s.tmpPath(num)
	finalPath := s.sstPath(num)

	meta, err := sstable.WriteFile(tmpPath, old.NewIterator(), s.writerOptions())
	if err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("lsm: publishing %s: %w", sstName(num), err)
	}
	if err := sstable.SyncDir(s.dir); err != nil {
		return fmt.Errorf("lsm: syncing %s after publishing %s: %w", s.dir, sstName(num), err)
	}

	r, err := sstable.Open(finalPath)
	if err != nil {
		return err
	}

	fm := manifest.FileMeta{
		Level:       0,
		Num:         num,
		Size:        meta.FileSize,
		NumEntries:  meta.NumEntries,
		SmallestKey: meta.SmallestKey,
		LargestKey:  meta.LargestKey,
		SmallestSeq: meta.SmallestSeq,
		LargestSeq:  meta.LargestSeq,
	}
	var edit manifest.Edit
	edit.AddFile(fm)
	edit.SetNextFileNum(s.nextNum)
	edit.SetLastSequence(s.seq)
	if err := s.manifest.Append(&edit); err != nil {
		_ = r.Close()
		return fmt.Errorf("lsm: recording %s in the manifest: %w", sstName(num), err)
	}

	s.mu.RLock()
	cur2 := s.cur
	s.mu.RUnlock()
	if cur2 == nil {
		_ = r.Close()
		return ErrClosed
	}
	s.install(newVersion(cur2.mem, dropMemtable(cur2.imm, old),
		append(append([]*sstFile(nil), cur2.files...), &sstFile{meta: fm, r: r, path: finalPath})))

	// Record the flush's output. This is the flush half of the bytes the engine
	// writes to disk; compaction's half is in CompactionStats. Together they are
	// what a write-amplification measurement divides by the logical bytes the
	// client stored (docs/BENCHMARKS.md §3.8). Recorded only on the success path,
	// so a crashed flush's partial .tmp is never counted.
	s.flushStats.flushes.Add(1)
	s.flushStats.bytes.Add(meta.FileSize)
	s.flushStats.entries.Add(int64(meta.NumEntries))
	return nil
}

func dropMemtable(list []*memtable.MemTable, m *memtable.MemTable) []*memtable.MemTable {
	out := make([]*memtable.MemTable, 0, len(list))
	for _, e := range list {
		if e != m {
			out = append(out, e)
		}
	}
	return out
}

// ---------------------------------------------------------------- read path

// Get implements Store.
func (s *LSMStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, opErr("get", key, err)
	}
	if err := s.opts.validateKey("get", key); err != nil {
		return nil, err
	}

	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return nil, opErr("get", key, ErrClosed)
	}

	v := s.acquire()
	if v == nil {
		return nil, opErr("get", key, ErrClosed)
	}
	defer s.release(v)

	value, kind, found, err := s.lookup(v, key)
	if err != nil {
		return nil, classify("get", key, err)
	}
	if !found || kind == ikey.KindTombstone {
		return nil, opErr("get", key, ErrNotFound)
	}
	return clone(value), nil
}

// lookup searches a version newest-first and stops at the first source holding
// any version of the key.
//
// Stopping at the first source is correct because of two properties that hold
// together: within one source the internal-key ordering puts the newest version
// first, and across sources every sequence number in a newer source is greater
// than every sequence number in an older one. The second is checked at startup
// and maintained by construction — a flush's range is above everything, and a
// compaction merges a contiguous run of the global sequence ordering.
//
// Each SSTable's Bloom filter is consulted inside Reader.Get, before its index
// and blocks, and may only eliminate the file.
func (s *LSMStore) lookup(v *version, key []byte) (value []byte, kind ikey.Kind, found bool, err error) {
	if value, kind, found = v.mem.Get(key, ikey.MaxSeq); found {
		return value, kind, true, nil
	}
	for _, m := range v.imm {
		if value, kind, found = m.Get(key, ikey.MaxSeq); found {
			return value, kind, true, nil
		}
	}
	for _, f := range v.files {
		value, kind, found, err := f.r.Get(key, ikey.MaxSeq)
		if err != nil {
			// Never degrade a damaged file into "key not found". That would turn
			// corruption into an ordinary answer and nobody would ever look.
			return nil, 0, false, fmt.Errorf("lsm: reading %s: %w", sstName(f.meta.Num), err)
		}
		if found {
			return value, kind, true, nil
		}
	}
	return nil, 0, false, nil
}

// ---------------------------------------------------------------- lifecycle

// SetAppliedIndex durably records applied-index metadata.
//
// The WAL remains the authority for it, as in Phase 2 and 3. The MANIFEST carries
// the field too, because docs/DESIGN.md §6 defines it and the snapshot written at
// each open records the recovered value — but nothing reads it back in preference
// to the log, and having two authorities for one number would be worse than
// having the wrong one.
func (s *LSMStore) SetAppliedIndex(ctx context.Context, a AppliedIndex) error {
	if err := ctx.Err(); err != nil {
		return opErr("set-applied-index", nil, err)
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return opErr("set-applied-index", nil, ErrClosed)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.w.AppendAppliedIndex(wal.AppliedIndex{Index: a.Index, Term: a.Term}); err != nil {
		return classify("set-applied-index", nil, err)
	}
	s.mu.Lock()
	s.applied = a
	s.mu.Unlock()
	return nil
}

// AppliedIndex returns the last durably recorded applied index.
func (s *LSMStore) AppliedIndex() AppliedIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// Sync flushes the WAL regardless of the configured sync mode.
func (s *LSMStore) Sync() error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return opErr("sync", nil, ErrClosed)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return classify("sync", nil, s.w.Sync())
}

// stopCompactor stops the background compactor and waits for it to finish.
func (s *LSMStore) stopCompactor() {
	s.stopOnce.Do(func() {
		close(s.compactQuit)
		<-s.compactStopped
	})
}

// Close stops compaction, flushes the WAL, closes every SSTable and releases the
// store. It is idempotent.
//
// Close does NOT flush the memtable. Everything in it is already in the WAL, so a
// clean close and a crash recover through exactly the same path — which means the
// recovery path is exercised by every test that reopens a store.
//
// The compactor is stopped before closeMu is taken for write, not after. A
// compaction holds closeMu for read while it runs, so taking the write lock first
// and then waiting for the compactor would be waiting for a goroutine that is
// waiting for the lock.
func (s *LSMStore) Close() error {
	s.stopCompactor()

	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	err := s.w.Close()
	s.closeManifest()
	s.closeAllReaders()
	return classify("close", nil, err)
}

// ---------------------------------------------------------------- inspection

// Dir returns the data directory.
func (s *LSMStore) Dir() string { return s.dir }

// Recovery returns what opening the store found on disk.
func (s *LSMStore) Recovery() LSMRecovery { return s.recovery }

// WALStats returns the underlying WAL's current shape.
func (s *LSMStore) WALStats() wal.Stats { return s.w.Stats() }

// Sequence returns the last assigned sequence number, which is the number of
// mutations this store has ever accepted.
func (s *LSMStore) Sequence() uint64 {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.seq
}

// SSTableInfo describes one live SSTable.
type SSTableInfo struct {
	Number      uint64
	Level       int
	Entries     uint64
	Blocks      int
	Bytes       int64
	SmallestSeq uint64
	LargestSeq  uint64
	HasFilter   bool
	FilterBytes int
	BlockReads  uint64
	FilterSkips uint64
}

// SSTables returns the live tables, oldest first.
func (s *LSMStore) SSTables() []SSTableInfo {
	v := s.acquire()
	if v == nil {
		return nil
	}
	defer s.release(v)

	out := make([]SSTableInfo, 0, len(v.files))
	for i := len(v.files) - 1; i >= 0; i-- { // v.files is newest-first
		f := v.files[i]
		out = append(out, SSTableInfo{
			Number:      f.meta.Num,
			Level:       f.meta.Level,
			Entries:     f.meta.NumEntries,
			Blocks:      f.r.NumBlocks(),
			Bytes:       f.meta.Size,
			SmallestSeq: f.meta.SmallestSeq,
			LargestSeq:  f.meta.LargestSeq,
			HasFilter:   f.r.HasFilter(),
			FilterBytes: int(f.r.Filter().NumBits() / 8),
			BlockReads:  f.r.BlockReads(),
			FilterSkips: f.r.FilterSkips(),
		})
	}
	return out
}

// flushCounters is the atomic form of FlushStats held by the store.
type flushCounters struct {
	flushes atomic.Int64
	bytes   atomic.Int64
	entries atomic.Int64
}

// FlushStats records what flushing has written since the store was opened: the
// memtable-to-L0-SSTable half of the engine's physical writes. Compaction's half
// is in CompactionStats. Together they let a write-amplification measurement
// (docs/BENCHMARKS.md §3.8) account for every SSTable byte the engine produced,
// not only the ones still live. Counted only on the flush success path.
type FlushStats struct {
	Flushes int64
	Bytes   int64
	Entries int64
}

// FlushStats returns the flush counters.
func (s *LSMStore) FlushStats() FlushStats {
	return FlushStats{
		Flushes: s.flushStats.flushes.Load(),
		Bytes:   s.flushStats.bytes.Load(),
		Entries: s.flushStats.entries.Load(),
	}
}

// ReadCounters aggregates the live readers' work counters, for the Bloom
// measurement in docs/BLOOM.md. They count since each file was opened, so they
// reset when a compaction replaces a file.
type ReadCounters struct {
	BlockReads  uint64
	FilterSkips uint64
	Files       int
	WithFilter  int
}

// ReadCounters returns the aggregated counters.
func (s *LSMStore) ReadCounters() ReadCounters {
	v := s.acquire()
	if v == nil {
		return ReadCounters{}
	}
	defer s.release(v)

	var rc ReadCounters
	rc.Files = len(v.files)
	for _, f := range v.files {
		rc.BlockReads += f.r.BlockReads()
		rc.FilterSkips += f.r.FilterSkips()
		if f.r.HasFilter() {
			rc.WithFilter++
		}
	}
	return rc
}

// LevelSummary reports the file count and total bytes per level.
func (s *LSMStore) LevelSummary() map[int]struct {
	Files int
	Bytes int64
} {
	v := s.acquire()
	if v == nil {
		return nil
	}
	defer s.release(v)

	out := map[int]struct {
		Files int
		Bytes int64
	}{}
	for _, f := range v.files {
		e := out[f.meta.Level]
		e.Files++
		e.Bytes += f.meta.Size
		out[f.meta.Level] = e
	}
	return out
}

// ManifestNumber returns the live manifest's number.
func (s *LSMStore) ManifestNumber() uint64 {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.manifest == nil {
		return 0
	}
	return s.manifest.Num()
}

// MemTableSize returns the current memtable's approximate byte size.
func (s *LSMStore) MemTableSize() int64 {
	s.mu.RLock()
	v := s.cur
	s.mu.RUnlock()
	if v == nil || v.mem == nil {
		return 0
	}
	return v.mem.ApproxSize()
}

// Snapshot returns every live key and its value.
//
// It merges the memtable, any immutable memtables and every SSTable, keeping the
// version with the highest sequence number per user key and dropping tombstoned
// keys. It reads every byte of every SSTable, so it is a diagnostic and a test
// affordance, not part of the Store interface.
func (s *LSMStore) Snapshot() (map[string][]byte, error) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return nil, opErr("snapshot", nil, ErrClosed)
	}
	v := s.acquire()
	if v == nil {
		return nil, opErr("snapshot", nil, ErrClosed)
	}
	defer s.release(v)

	type versioned struct {
		seq   uint64
		kind  ikey.Kind
		value []byte
	}
	newest := make(map[string]versioned)
	offer := func(internal, value []byte) {
		uk := string(ikey.UserKey(internal))
		seq := ikey.Seq(internal)
		if cur, ok := newest[uk]; ok && cur.seq >= seq {
			return
		}
		newest[uk] = versioned{seq: seq, kind: ikey.KindOf(internal), value: clone(value)}
	}

	for _, m := range append([]*memtable.MemTable{v.mem}, v.imm...) {
		it := m.NewIterator()
		for it.Next() {
			offer(it.Key(), it.Value())
		}
	}
	for _, f := range v.files {
		it := f.r.NewIterator()
		for it.Next() {
			offer(it.Key(), it.Value())
		}
		if err := it.Err(); err != nil {
			return nil, classify("snapshot", nil,
				fmt.Errorf("lsm: reading %s: %w", sstName(f.meta.Num), err))
		}
	}

	out := make(map[string][]byte, len(newest))
	for k, v := range newest {
		if v.kind == ikey.KindValue {
			out[k] = v.value
		}
	}
	return out, nil
}

// Len reports the number of live keys. It returns -1 if the merge failed, which
// is a value no caller can mistake for a count.
func (s *LSMStore) Len() int {
	m, err := s.Snapshot()
	if err != nil {
		return -1
	}
	return len(m)
}
