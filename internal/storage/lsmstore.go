package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/adivishall/quorum/internal/storage/ikey"
	"github.com/adivishall/quorum/internal/storage/memtable"
	"github.com/adivishall/quorum/internal/storage/sstable"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// SSTable file naming. Numbers ascend with flush order, so a higher number is
// a newer file. Six digits matches the WAL's segment naming.
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
// It is the Phase 2 WALStore with its Go map replaced by the real thing:
//
//	write:  WAL append  ->  memtable
//	flush:  memtable    ->  SSTable
//	read:   memtable -> immutable memtables -> SSTables, newest first
//
// Every client-visible semantic is unchanged. That is not a claim, it is
// enforced: LSMStore runs the Phase 1 conformance and concurrency suites
// verbatim, including a configuration with a memtable so small that almost
// every operation crosses an SSTable boundary.
//
// # Write path
//
//	validate                    reject bad input before anything is logged
//	assign the next sequence    under writeMu, so log order == sequence order
//	append to the WAL           write(2); flushed per the sync mode
//	publish to the memtable     only after the log write succeeded
//	flush if the memtable is full
//
// Nothing becomes visible in memory before it is in the log. The converse skew
// — a record in the log that was never acknowledged — is inherent to
// write-ahead logging and is stated in docs/CONSISTENCY.md C4 rather than
// papered over.
//
// # Reading
//
// A read takes a consistent snapshot of the three sources under mu, releases
// the lock, and searches them newest-first. The first source that holds ANY
// version of the key decides the answer, including when that version is a
// tombstone — a tombstone is an answer, not an absence, and falling through it
// to an older SSTable is exactly how a deleted key comes back from the dead.
//
// # Locking
//
//	closeMu  held for read by every operation for its whole duration; Close
//	         takes it for write. That is what lets Close close SSTable files
//	         without racing an in-flight read, and it is why an interrupted
//	         operation reports ErrClosed rather than a filesystem error.
//	writeMu  serialises writers across sequence assignment, the log append,
//	         the memtable publish and the flush. Sequence numbers must be
//	         assigned in log order or replay reconstructs a state that never
//	         existed.
//	mu       guards the three version pointers only. Critical sections are
//	         pointer swaps, so a reader never waits on disk I/O and a writer
//	         never waits on a reader's block read.
//
// Order is always closeMu -> writeMu -> mu, and never the reverse.
type LSMStore struct {
	opts Options
	dir  string

	// closeMu is a drain barrier rather than a data lock. See Locking above.
	closeMu sync.RWMutex
	closed  bool

	writeMu sync.Mutex
	seq     uint64 // last assigned sequence number; writeMu
	nextNum uint64 // next SSTable file number; writeMu

	// flushErr latches a failed flush. A flush that fails leaves the data
	// durable in the WAL and visible in memory, so the write that triggered it
	// genuinely succeeded and is reported as such. What must not happen is
	// carrying on: the memtable would grow without bound and the next crash
	// would replay a log nothing had ever compacted. Every subsequent
	// mutation fails with this error instead.
	flushErr error

	mu   sync.RWMutex
	mem  *memtable.MemTable   // mutable; receives writes
	imm  []*memtable.MemTable // frozen, newest first, being flushed
	ssts []*sstFile           // ascending file number: oldest first

	applied AppliedIndex

	w        *wal.WAL
	recovery LSMRecovery
}

var _ Store = (*LSMStore)(nil)

// sstFile is one live SSTable and what startup learned about it.
type sstFile struct {
	num   uint64
	r     *sstable.Reader
	stats sstable.Stats
}

// LSMRecovery summarises what opening the store found on disk.
type LSMRecovery struct {
	// SSTables.
	SSTablesLoaded   int
	SSTableEntries   uint64
	SSTableBytes     int64
	TempFilesRemoved int
	MaxFlushedSeq    uint64

	// WAL replay.
	SegmentsScanned int
	BytesScanned    int64
	RecordsApplied  int64
	BatchesApplied  int64
	OpsReplayed     int64
	OpsSkipped      int64 // already durable in an SSTable
	FlushesOnReplay int

	// Sequence is the last sequence number assigned during replay, which is
	// the total number of mutations the WAL holds.
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
//  1. Delete leftover *.sst.tmp files. A temp file is the signature of a
//     crash during a flush. Its contents are still in the WAL, so deleting it
//     loses nothing, and leaving it would mean carrying a file nothing can
//     classify.
//  2. Open and fully verify every *.sst: footer, index, every block checksum,
//     entry ordering and count. Verification is a complete read of each file.
//     Phase 3 pays that because it has no MANIFEST: the sequence range a file
//     covers lives in the MANIFEST from Phase 4 (docs/DESIGN.md §6) and there
//     is nowhere else on disk to put it now. See docs/LSM.md.
//  3. Check the file set is coherent: no gaps in the numbering, and
//     non-overlapping, ascending sequence ranges. Both are guaranteed by the
//     way flushes are produced, so a violation means a file was removed or
//     substituted, and continuing would silently drop whatever it held.
//  4. Replay the WAL, assigning sequence numbers from 1 in log order —
//     identically to how the original writes were numbered. A mutation whose
//     number is at or below the highest flushed sequence is already in an
//     SSTable and is skipped; everything after it is applied to the memtable.
//
// Step 4 is the whole Phase 3 recovery argument. It works because sequence
// assignment is deterministic: the same committed log prefix produces the same
// numbers, so "seq <= maxFlushedSeq" is exactly "this mutation is already on
// disk in a table".
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
		opts: opts,
		dir:  dir,
		mem:  memtable.New(opts.MemTableSeed),
	}

	removed, err := sweepTempSSTables(dir)
	if err != nil {
		return nil, classify("open", nil, err)
	}
	s.recovery.TempFilesRemoved = removed

	if err := s.loadSSTables(); err != nil {
		s.closeReaders()
		return nil, classify("open", nil, err)
	}

	if err := s.replayWAL(); err != nil {
		s.closeReaders()
		return nil, classify("open", nil, err)
	}

	w, err := wal.Create(filepath.Join(dir, walDirName), opts.WAL)
	if err != nil {
		s.closeReaders()
		return nil, classify("open", nil, err)
	}
	s.w = w
	return s, nil
}

// sweepTempSSTables removes partial flush output left by a crash.
func sweepTempSSTables(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("lsm: reading %s: %w", dir, err)
	}
	var removed int
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), sstTempSuffix) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return removed, fmt.Errorf("lsm: removing partial flush output %s: %w", e.Name(), err)
		}
		removed++
	}
	return removed, nil
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

// loadSSTables opens, verifies and orders every SSTable in the data directory.
func (s *LSMStore) loadSSTables() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("lsm: reading %s: %w", s.dir, err)
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

	for i := 1; i < len(nums); i++ {
		if nums[i] != nums[i-1]+1 {
			// Nothing in Phase 3 deletes an SSTable. A gap means one was
			// removed from underneath us, and every key it held whose
			// sequence number is below the next file's would be skipped
			// during replay and lost without a trace.
			return fmt.Errorf(
				"lsm: SSTable numbering has a gap: %s is followed by %s (%d files missing): %w",
				sstName(nums[i-1]), sstName(nums[i]), nums[i]-nums[i-1]-1, ErrCorrupt)
		}
	}

	for _, n := range nums {
		path := filepath.Join(s.dir, sstName(n))
		r, err := sstable.Open(path)
		if err != nil {
			return err
		}
		stats, err := r.Verify()
		if err != nil {
			_ = r.Close()
			return err
		}
		if stats.NumEntries == 0 {
			_ = r.Close()
			return fmt.Errorf("lsm: %s holds no entries; a flush never produces an empty table: %w",
				sstName(n), ErrCorrupt)
		}
		if last := len(s.ssts); last > 0 {
			if prev := s.ssts[last-1].stats; prev.LargestSeq >= stats.SmallestSeq {
				// Flushes are produced in sequence order under one lock, so
				// file N's sequence numbers are all below file N+1's. An
				// overlap means the file set is not one this engine produced.
				return fmt.Errorf(
					"lsm: %s covers sequences [%d,%d] which overlaps %s's [%d,%d]; "+
						"flushed files must not overlap: %w",
					sstName(n), stats.SmallestSeq, stats.LargestSeq,
					sstName(s.ssts[last-1].num), prev.SmallestSeq, prev.LargestSeq, ErrCorrupt)
			}
		}

		s.ssts = append(s.ssts, &sstFile{num: n, r: r, stats: stats})
		s.recovery.SSTableEntries += stats.NumEntries
		s.recovery.SSTableBytes += stats.FileSize
		s.recovery.MaxFlushedSeq = stats.LargestSeq
	}

	s.recovery.SSTablesLoaded = len(s.ssts)
	s.nextNum = 1
	if len(nums) > 0 {
		s.nextNum = nums[len(nums)-1] + 1
	}
	return nil
}

// replayWAL rebuilds the memtable from the portion of the log that is not yet
// in an SSTable.
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
				s.mem.Add(s.seq, kind, op.Key, op.Value)
				s.recovery.OpsReplayed++

				// Flush during replay for the same reason as during normal
				// operation: without it, recovering a log larger than memory
				// would need memory proportional to the whole log, and the
				// "a dataset larger than RAM fits" claim would hold only
				// until the first restart.
				if s.mem.ApproxSize() >= s.opts.MemTableSize {
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
		// An SSTable holds sequence numbers the log never contained. The log
		// must have been truncated or replaced; replaying it would produce a
		// state where flushed writes are present but later ones are not, with
		// no way to tell which.
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
// Delete writes a tombstone. It performs no read, which is why it is
// idempotent and does not report whether the key existed — the semantic the
// Store interface committed to in Phase 1 precisely so that this phase would
// not have to break it.
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
	s.mem.Add(s.seq, kind, key, value)

	if s.mem.ApproxSize() >= s.opts.MemTableSize {
		if err := s.flushLocked(); err != nil {
			// The mutation itself succeeded: it is in the log and visible in
			// memory. Reporting a failure here would say a write did not
			// happen when it did. The failure latches instead, and the next
			// mutation is the one that reports it.
			s.flushErr = err
		}
	}
	return nil
}

// ---------------------------------------------------------------- flush

// Flush writes the current memtable to an SSTable, whatever its size.
//
// It exists for tests and for an operator who wants the memtable on disk. The
// engine flushes on its own when the memtable reaches Options.MemTableSize;
// nothing about correctness depends on this being called.
func (s *LSMStore) Flush() error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return opErr("flush", nil, ErrClosed)
	}

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
}

// flushLocked turns the current memtable into an SSTable. writeMu must be held.
//
// The order is the crash contract, and each step is chosen for what a crash
// immediately after it leaves behind:
//
//	freeze + swap   the memtable stops taking writes and becomes readable as
//	                an immutable source; a new one takes the writes.
//	write to .tmp   a crash here leaves a partial file under a name no reader
//	                ever consults. Startup deletes it. Nothing is lost,
//	                because the WAL still holds every mutation in it.
//	fsync the file  the bytes are on the device before the name exists.
//	rename          atomic on POSIX: the final name either does not exist or
//	                names a complete, fsynced file. There is no instant at
//	                which a reader can see a partial SSTable.
//	fsync the dir   makes the rename itself durable.
//	publish         one critical section adds the table and drops the
//	                immutable memtable, so every mutation is in exactly one
//	                source at every instant.
//
// A crash between the rename and the publish is harmless: the file is on disk
// and complete, and the WAL still holds its contents, so the next startup
// either uses the file (skipping the replayed prefix) or would have rebuilt
// the same state from the log.
//
// This is synchronous: the writer that triggers it pays for it, and other
// writers wait. Readers do not — that is what the immutable memtable is for.
// Making the flush concurrent is a Phase 5 question with a benchmark attached;
// doing it now would add a scheduler to a phase whose job is to be obviously
// correct.
func (s *LSMStore) flushLocked() error {
	if s.mem.Empty() {
		return nil
	}
	if s.nextNum > maxSSTNumber {
		return fmt.Errorf("lsm: SSTable number %d would exceed the %d-digit name format",
			s.nextNum, sstDigits)
	}

	old := s.mem
	old.Freeze()
	fresh := memtable.New(s.opts.MemTableSeed)

	s.mu.Lock()
	s.mem = fresh
	s.imm = append([]*memtable.MemTable{old}, s.imm...)
	s.mu.Unlock()

	num := s.nextNum
	tmpPath := filepath.Join(s.dir, sstTempName(num))
	finalPath := filepath.Join(s.dir, sstName(num))

	meta, err := sstable.WriteFile(tmpPath, old.NewIterator(),
		sstable.WriterOptions{BlockSize: s.opts.BlockSize})
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

	s.mu.Lock()
	s.ssts = append(s.ssts, &sstFile{
		num: num,
		r:   r,
		stats: sstable.Stats{
			NumEntries:  meta.NumEntries,
			NumBlocks:   meta.NumBlocks,
			FileSize:    meta.FileSize,
			SmallestKey: meta.SmallestKey,
			LargestKey:  meta.LargestKey,
			SmallestSeq: meta.SmallestSeq,
			LargestSeq:  meta.LargestSeq,
		},
	})
	s.imm = dropMemtable(s.imm, old)
	s.mu.Unlock()

	s.nextNum++
	return nil
}

func dropMemtable(list []*memtable.MemTable, m *memtable.MemTable) []*memtable.MemTable {
	out := list[:0]
	for _, e := range list {
		if e != m {
			out = append(out, e)
		}
	}
	return out
}

// ---------------------------------------------------------------- read path

// version is a consistent snapshot of the three places data can live.
type version struct {
	mem  *memtable.MemTable
	imm  []*memtable.MemTable
	ssts []*sstFile
}

// snapshot captures the version pointers. Taking all three in one critical
// section is what guarantees a reader never falls between an immutable
// memtable being dropped and the SSTable that replaced it being added.
func (s *LSMStore) snapshot() version {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return version{mem: s.mem, imm: s.imm, ssts: s.ssts}
}

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

	value, kind, found, err := s.lookup(s.snapshot(), key)
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
// together: within one source the internal-key ordering puts the newest
// version first, and across sources every sequence number in a newer source is
// greater than every sequence number in an older one. The second is checked at
// startup (loadSSTables refuses overlapping files) rather than assumed.
func (s *LSMStore) lookup(v version, key []byte) (value []byte, kind ikey.Kind, found bool, err error) {
	if value, kind, found = v.mem.Get(key, ikey.MaxSeq); found {
		return value, kind, true, nil
	}
	for _, m := range v.imm {
		if value, kind, found = m.Get(key, ikey.MaxSeq); found {
			return value, kind, true, nil
		}
	}
	for i := len(v.ssts) - 1; i >= 0; i-- {
		value, kind, found, err := v.ssts[i].r.Get(key, ikey.MaxSeq)
		if err != nil {
			// Never degrade a damaged file into "key not found". That would
			// turn corruption into an ordinary answer and nobody would ever
			// look.
			return nil, 0, false, fmt.Errorf("lsm: reading %s: %w", sstName(v.ssts[i].num), err)
		}
		if found {
			return value, kind, true, nil
		}
	}
	return nil, 0, false, nil
}

// ---------------------------------------------------------------- lifecycle

// SetAppliedIndex durably records applied-index metadata. See AppliedIndex for
// what it does and does not mean before Phase 9.
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

// Sync flushes the WAL regardless of the configured sync mode. It does not
// flush the memtable; see Flush for that.
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

// Close flushes the WAL, closes every SSTable and releases the store. It is
// idempotent.
//
// Close does NOT flush the memtable to an SSTable. Everything in it is already
// in the WAL, so a clean close and a crash recover through exactly the same
// path — which means the recovery path is exercised by every test that reopens
// a store, not only by the crash tests.
//
// Taking closeMu for write drains in-flight operations first. Without that,
// closing an SSTable's file underneath a reader would surface as a filesystem
// error from a Get, and the contract says a Get racing Close returns ErrClosed.
func (s *LSMStore) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	err := s.w.Close()
	s.closeReaders()

	s.mu.Lock()
	s.mem = nil
	s.imm = nil
	s.ssts = nil
	s.mu.Unlock()

	return classify("close", nil, err)
}

func (s *LSMStore) closeReaders() {
	for _, f := range s.ssts {
		_ = f.r.Close()
	}
}

// ---------------------------------------------------------------- inspection

// Dir returns the data directory.
func (s *LSMStore) Dir() string { return s.dir }

// Recovery returns what opening the store found on disk.
func (s *LSMStore) Recovery() LSMRecovery { return s.recovery }

// WALStats returns the underlying WAL's current shape.
func (s *LSMStore) WALStats() wal.Stats { return s.w.Stats() }

// Sequence returns the last assigned sequence number, which is the number of
// mutations this store has ever accepted (including those replayed from the
// log at startup).
func (s *LSMStore) Sequence() uint64 {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.seq
}

// SSTableInfo describes one live SSTable.
type SSTableInfo struct {
	Number      uint64
	Entries     uint64
	Blocks      int
	Bytes       int64
	SmallestSeq uint64
	LargestSeq  uint64
}

// SSTables returns the live tables, oldest first.
func (s *LSMStore) SSTables() []SSTableInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SSTableInfo, 0, len(s.ssts))
	for _, f := range s.ssts {
		out = append(out, SSTableInfo{
			Number:      f.num,
			Entries:     f.stats.NumEntries,
			Blocks:      f.stats.NumBlocks,
			Bytes:       f.stats.FileSize,
			SmallestSeq: f.stats.SmallestSeq,
			LargestSeq:  f.stats.LargestSeq,
		})
	}
	return out
}

// MemTableSize returns the current memtable's approximate byte size.
func (s *LSMStore) MemTableSize() int64 {
	s.mu.RLock()
	mem := s.mem
	s.mu.RUnlock()
	if mem == nil {
		return 0
	}
	return mem.ApproxSize()
}

// Snapshot returns every live key and its value.
//
// It merges the memtable, any immutable memtables and every SSTable, keeping
// the version with the highest sequence number per user key and dropping
// tombstoned keys. It reads every byte of every SSTable, so it is a
// diagnostic and a test affordance, not part of the Store interface and not
// something to call on a hot path. There are deliberately no range scans in
// the client API (ADR-008); this is not one.
func (s *LSMStore) Snapshot() (map[string][]byte, error) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return nil, opErr("snapshot", nil, ErrClosed)
	}
	v := s.snapshot()

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
	for _, f := range v.ssts {
		it := f.r.NewIterator()
		for it.Next() {
			offer(it.Key(), it.Value())
		}
		if err := it.Err(); err != nil {
			return nil, classify("snapshot", nil, fmt.Errorf("lsm: reading %s: %w", sstName(f.num), err))
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

// Len reports the number of live keys.
//
// Like MemStore.Len it exists for tests and metrics and is not part of the
// Store interface. Unlike MemStore.Len it has to merge every source to answer,
// so it is O(everything). It returns -1 if the merge failed, which is a value
// no caller can mistake for a count — reporting 0 for an unreadable store
// would be exactly the silent degradation this engine refuses elsewhere.
func (s *LSMStore) Len() int {
	m, err := s.Snapshot()
	if err != nil {
		return -1
	}
	return len(m)
}
