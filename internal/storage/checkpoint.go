package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/adivishall/quorum/internal/storage/compaction"
	"github.com/adivishall/quorum/internal/storage/ikey"
	"github.com/adivishall/quorum/internal/storage/manifest"
	"github.com/adivishall/quorum/internal/storage/memtable"
	"github.com/adivishall/quorum/internal/storage/sstable"
	"github.com/adivishall/quorum/internal/vfs"
)

// S4 (docs/STORAGE_INTEGRATION.md §10.3, §10.6): an engine checkpoint — a
// consistent, pinned read view at the durable applied index — and ingest —
// replacing the engine's whole contents with a key-ordered stream, atomically,
// at one MANIFEST edit.

// ---------------------------------------------------------------- checkpoint

// Checkpoint is the engine's state at one applied index, pinned: the version
// (the immutable table set the MANIFEST names, with an empty memtable) that a
// flush left durable at that index. Its reference keeps every table it names
// open and on disk until Close, whatever compaction does meanwhile, so it reads
// the same contents however many times it is iterated (INV-S5). Writes after
// it was taken are not in it.
type Checkpoint struct {
	s       *LSMStore
	v       *version
	applied AppliedIndex
	seq     uint64
	closed  atomic.Bool
}

// ErrInDoubt means an ingest's commit — its one MANIFEST edit — may or may not
// be durable: the engine refuses every read and write until it is reopened,
// when the MANIFEST decides (docs/STORAGE_INTEGRATION.md §10.6).
var ErrInDoubt = errors.New("lsm: the engine's contents are in doubt until it is reopened")

// ErrNotDurable means a checkpoint could not be pinned at a durable applied
// index: after the flush the engine still held unflushed data, or its durable
// watermark was not its applied index. It is an engine bug, never expected.
var ErrNotDurable = errors.New("lsm: the state to checkpoint is not durable")

// Checkpoint makes the engine durable through its applied index and pins its
// state there. Under the write lock it flushes — a table if the memtable holds
// anything, the applied index alone if only that moved (S3) — and then
// acquires the current version, which is exactly the table set the MANIFEST's
// durable Applied describes. The checkpoint's applied index is therefore the
// durable watermark, never above it. The caller closes it; it holds the tables
// open until then.
func (s *LSMStore) Checkpoint() (*Checkpoint, error) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return nil, opErr("checkpoint", nil, ErrClosed)
	}
	if s.inDoubt.Load() {
		return nil, opErr("checkpoint", nil, ErrInDoubt)
	}
	c, flushed, err := func() (*Checkpoint, bool, error) {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		if s.flushErr != nil {
			return nil, false, classify("checkpoint", nil, s.flushErr)
		}
		if err := s.flushLocked(); err != nil {
			s.flushErr = err
			return nil, false, classify("checkpoint", nil, err)
		}
		v := s.acquire()
		if v == nil {
			return nil, false, opErr("checkpoint", nil, ErrClosed)
		}
		s.mu.RLock()
		applied, durable := s.applied, s.flushedApplied
		s.mu.RUnlock()
		if !v.mem.Empty() || len(v.imm) != 0 || applied != durable {
			s.release(v)
			return nil, false, opErr("checkpoint", nil, fmt.Errorf(
				"%w: after the flush the memtable holds %d entries, %d immutable memtables remain, and the applied index %+v is not the durable %+v",
				ErrNotDurable, v.mem.Len(), len(v.imm), applied, durable))
		}
		return &Checkpoint{s: s, v: v, applied: durable, seq: s.seq}, true, nil
	}()
	if flushed {
		s.compactionSignal()
	}
	return c, err
}

// Applied returns the applied index the checkpoint is at: the engine's durable
// applied index when it was taken.
func (c *Checkpoint) Applied() AppliedIndex { return c.applied }

// Tables returns how many tables the checkpoint pins.
func (c *Checkpoint) Tables() int { return len(c.v.files) }

// Iterator returns the checkpoint's live entries in key order: each key once,
// with its newest value; a deleted key is absent. It merges every table the
// checkpoint pins (the compaction merge, bottom-most: the version holds the
// whole database, so a tombstone hides nothing older). Each call starts a new,
// independent iteration over the same contents.
func (c *Checkpoint) Iterator() *CheckpointIterator {
	inputs := make([]compaction.Stream, 0, len(c.v.files))
	for _, f := range c.v.files {
		inputs = append(inputs, f.r.NewIterator())
	}
	return &CheckpointIterator{m: compaction.New(inputs, compaction.Options{BottomMost: true})}
}

// Close releases the checkpoint's tables. It is idempotent, from any number
// of goroutines: the version is released exactly once (a second release could
// drop a live version's count to zero and delete tables the MANIFEST names).
//
// Close the checkpoint before the store. A store closed first closes its
// current version's readers, and a checkpoint still reading that version then
// fails with a read error — never a wrong answer.
func (c *Checkpoint) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.s.release(c.v)
	}
	return nil
}

// CheckpointIterator walks a checkpoint's live entries in key order.
type CheckpointIterator struct {
	m   *compaction.Merger
	err error
}

// Next advances to the next live entry. A false return means the end, or a
// failure: check Err.
func (it *CheckpointIterator) Next() bool {
	if it.err != nil || !it.m.Next() {
		return false
	}
	if ikey.KindOf(it.m.Key()) != ikey.KindValue {
		// A bottom-most merge drops every tombstone; one here is a bug, never
		// an absence to pass over.
		it.err = fmt.Errorf("lsm: a checkpoint produced a tombstone for %s: %w", SafeKey(ikey.UserKey(it.m.Key())), ErrCorrupt)
		return false
	}
	return true
}

// Key returns the current entry's key; valid until the next call to Next.
func (it *CheckpointIterator) Key() []byte { return ikey.UserKey(it.m.Key()) }

// Value returns the current entry's value, with Key's lifetime.
func (it *CheckpointIterator) Value() []byte { return it.m.Value() }

// Err returns what stopped the iteration, if anything did.
func (it *CheckpointIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.m.Err()
}

// memStream adapts a memtable iterator to a merge input (it cannot fail).
type memStream struct{ *memtable.Iterator }

func (memStream) Err() error { return nil }

// Scan calls fn with each live entry of the current state in key order — the
// memtables included — one entry at a time, never the state in memory: the
// LSM machine loads its session records this way at open (S4). It pins one
// version for the scan, so its tables cannot change under it; but its
// memtable is the live one, so a write concurrent with the scan may or may
// not be seen, and a batch may be seen in part. Scan when nothing writes (the
// machine does, at open), or take a Checkpoint. fn's arguments are valid only
// during the call; an error from fn stops the scan and is returned.
func (s *LSMStore) Scan(fn func(key, value []byte) error) error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return opErr("scan", nil, ErrClosed)
	}
	if s.inDoubt.Load() {
		return opErr("scan", nil, ErrInDoubt)
	}
	v := s.acquire()
	if v == nil {
		return opErr("scan", nil, ErrClosed)
	}
	defer s.release(v)
	inputs := make([]compaction.Stream, 0, 1+len(v.imm)+len(v.files))
	for _, m := range append([]*memtable.MemTable{v.mem}, v.imm...) {
		inputs = append(inputs, memStream{m.NewIterator()})
	}
	for _, f := range v.files {
		inputs = append(inputs, f.r.NewIterator())
	}
	m := compaction.New(inputs, compaction.Options{BottomMost: true})
	for m.Next() {
		if ikey.KindOf(m.Key()) != ikey.KindValue {
			return opErr("scan", nil, fmt.Errorf("lsm: a scan produced a tombstone for %s: %w", SafeKey(ikey.UserKey(m.Key())), ErrCorrupt))
		}
		if err := fn(ikey.UserKey(m.Key()), m.Value()); err != nil {
			return err
		}
	}
	if err := m.Err(); err != nil {
		return classify("scan", nil, err)
	}
	return nil
}

// Latched reports the failure that latched the store — a flush, or an ingest
// whose commit is in doubt — after which it refuses every write and must be
// reopened; nil when it has none.
func (s *LSMStore) Latched() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.flushErr
}

// ---------------------------------------------------------------- ingest

// IngestSource is the content an Ingest replaces the store's with: entries in
// strictly ascending key order. Next returning false is the end only if Err
// is nil; a source that ends with an error has not ended (a snapshot's source
// reports its final checks — length, checksum, footer — there).
type IngestSource interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
}

// IngestStats describes a completed ingest.
type IngestStats struct {
	Tables  int    // tables written
	Entries uint64 // entries ingested
	Bytes   int64  // table bytes written
	Level   int    // the level the tables were placed at (0 when there were none)
	Retired int    // live tables the ingest replaced
}

// ErrInvalidIngest means an ingest was refused because of its input — keys out
// of order, a key or value over the engine's limits, an applied index that does
// not advance — before anything changed.
var ErrInvalidIngest = errors.New("invalid ingest")

// stagedTable is a table an ingest wrote and has not yet committed.
type stagedTable struct {
	meta manifest.FileMeta
	r    *sstable.Reader
	path string
}

// Ingest replaces the store's entire contents with the entries src yields, at
// applied index a, atomically (docs/STORAGE_INTEGRATION.md §10.6). Holding the
// close, compaction and write locks — no writer and no compaction runs during
// it:
//
//  1. stage: write the entries into new tables (N.sst.tmp, fsync, rename to
//     N.sst), numbered with fresh sequence numbers above every one the store
//     has used, cut at the memtable size. No MANIFEST edit names them, so
//     they are not part of the store (INV-S6): a crash leaves orphans that
//     the next open sweeps.
//  2. the source must end cleanly (Err nil); any failure so far deletes the
//     staged tables and returns with the store unchanged and usable.
//  3. fsync the directory: the staged names are durable.
//  4. rotate the WAL to a new, empty, durable segment n.
//  5. COMMIT: one MANIFEST edit, fsynced — delete every live table, add every
//     staged table, the log number n, the log start sequence, the last
//     sequence, the applied index a and the applied sequence. Before it the
//     MANIFEST names the old store; after it, only the new tables at a, with
//     a log that begins empty, so no record of the old contents is replayed
//     onto the new.
//  6. switch the version to a fresh memtable and the new tables; the applied
//     index, the durable watermark (DurableApplied) and the sequences are the
//     edit's.
//  7. retire: the old tables are deleted once no reader holds them, the WAL
//     segments below n now. A failure here costs space, never correctness:
//     the next open sweeps both.
//
// A failure in step 4 or 5 is ambiguous — the edit may be durable — so it
// latches, as a failed flush does: the store refuses every later write and
// must be reopened, and the open reads whichever edit is durable, old or new.
// Nothing moves the durable watermark before the append returns success.
//
// a must advance the applied index (as an apply batch's must). The tables go
// to the shallowest level from 1 whose size budget holds them, so the level-0
// trigger does not rewrite them at once; their key ranges and sequence ranges
// are disjoint and ascending, as the read path requires.
func (s *LSMStore) Ingest(src IngestSource, a AppliedIndex) (IngestStats, error) {
	const op = "ingest"
	var stats IngestStats
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return stats, opErr(op, nil, ErrClosed)
	}
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.flushErr != nil {
		return stats, classify(op, nil, s.flushErr)
	}
	if s.w == nil {
		return stats, opErr(op, nil, ErrClosed)
	}
	s.mu.RLock()
	cur := s.applied
	s.mu.RUnlock()
	if a.Index == 0 || a.Term == 0 || a.Index <= cur.Index || a.Term < cur.Term {
		return stats, opErr(op, nil, fmt.Errorf("%w: %w: (%d, %d) after (%d, %d)",
			ErrInvalidIngest, ErrAppliedIndex, a.Index, a.Term, cur.Index, cur.Term))
	}

	// 1–2. Stage.
	staged, seq, err := s.stageIngest(src, &stats)
	discard := func() {
		for _, t := range staged {
			_ = t.r.Close()
			_ = s.fs.Remove(t.path)
		}
	}
	if err != nil {
		discard()
		return stats, err
	}
	// 3. The staged names are durable before anything names them.
	if err := s.fs.SyncDir(s.dir); err != nil {
		discard()
		return stats, classify(op, nil, fmt.Errorf("lsm: syncing %s after staging an ingest: %w", s.dir, err))
	}
	// 4. A fresh, empty, durable segment for the new contents' log.
	n, err := s.w.Rotate()
	if err != nil {
		// The WAL is latched or in doubt; nothing names the staged tables.
		discard()
		s.flushErr = fmt.Errorf("lsm: rotating the WAL to commit an ingest at %d: %w", a.Index, err)
		return stats, classify(op, nil, s.flushErr)
	}
	// 5. The commit point.
	var total int64
	for _, t := range staged {
		total += t.meta.Size
	}
	level := 0
	if len(staged) > 0 {
		level = s.ingestLevel(total)
	}
	s.mu.RLock()
	old := append([]*sstFile(nil), s.cur.files...)
	s.mu.RUnlock()
	var edit manifest.Edit
	for _, f := range old {
		edit.DeleteFile(f.meta.Level, f.meta.Num)
	}
	files := make([]*sstFile, 0, len(staged))
	for _, t := range staged {
		t.meta.Level = level
		edit.AddFile(t.meta)
		files = append(files, &sstFile{meta: t.meta, r: t.r, path: t.path})
	}
	edit.SetNextFileNum(s.nextNum)
	edit.SetLastSequence(seq)
	edit.SetLogNumber(n)
	edit.SetLogStartSeq(seq)
	edit.SetApplied(manifest.Applied{Index: a.Index, Term: a.Term})
	edit.SetAppliedSeq(seq)
	if err := s.manifest.Append(&edit); err != nil {
		// Ambiguous: the edit may be durable, naming the staged tables, so they
		// are kept for the next open to decide. The watermark does not move,
		// and nothing is served — the disk may now hold the new contents while
		// memory holds the old (inDoubt).
		for _, t := range staged {
			_ = t.r.Close()
		}
		s.flushErr = fmt.Errorf("%w: recording an ingest at %d in the manifest: %w", ErrInDoubt, a.Index, err)
		s.inDoubt.Store(true)
		return stats, classify(op, nil, s.flushErr)
	}
	// 6. Switch.
	s.seq = seq
	s.install(newVersion(memtable.New(s.opts.MemTableSeed), nil, files))
	s.mu.Lock()
	s.applied, s.appliedSeq, s.flushedApplied = a, seq, a
	s.mu.Unlock()
	// 7. Retire: the old tables go with the last reader of the old version
	// (reap); the old log now.
	s.truncateWALBelow(n)
	stats.Level, stats.Retired = level, len(old)
	return stats, nil
}

// stageIngest writes src's entries into new tables, returning them and the
// last sequence number assigned. writeMu must be held.
func (s *LSMStore) stageIngest(src IngestSource, stats *IngestStats) ([]*stagedTable, uint64, error) {
	const op = "ingest"
	var (
		staged []*stagedTable
		seq    = s.seq
		prev   []byte
		have   bool

		file    vfs.File
		tw      *sstable.Writer
		num     uint64
		tmp     string
		pending int64 // approximate bytes in the open table
		ik      []byte
	)
	abandon := func() {
		if file != nil {
			_ = file.Close()
			_ = s.fs.Remove(tmp)
			file = nil
		}
	}
	finish := func() error {
		if file == nil {
			return nil
		}
		meta, err := tw.Finish()
		if err == nil {
			err = file.Sync()
		}
		if err != nil {
			abandon()
			return classify(op, nil, fmt.Errorf("lsm: writing ingested %s: %w", sstName(num), err))
		}
		if err := file.Close(); err != nil {
			file = nil
			_ = s.fs.Remove(tmp)
			return classify(op, nil, fmt.Errorf("lsm: closing ingested %s: %w", sstName(num), err))
		}
		file = nil
		final := s.sstPath(num)
		if err := s.fs.Rename(tmp, final); err != nil {
			_ = s.fs.Remove(tmp)
			return classify(op, nil, fmt.Errorf("lsm: staging ingested %s: %w", sstName(num), err))
		}
		r, err := sstable.OpenFS(s.fs, final)
		if err != nil {
			_ = s.fs.Remove(final)
			return classify(op, nil, err)
		}
		staged = append(staged, &stagedTable{path: final, r: r, meta: manifest.FileMeta{
			Num: num, Size: meta.FileSize, NumEntries: meta.NumEntries,
			SmallestKey: meta.SmallestKey, LargestKey: meta.LargestKey,
			SmallestSeq: meta.SmallestSeq, LargestSeq: meta.LargestSeq,
		}})
		stats.Tables++
		stats.Bytes += meta.FileSize
		return nil
	}
	for src.Next() {
		k, v := src.Key(), src.Value()
		if err := s.opts.validateKey(op, k); err != nil {
			abandon()
			return staged, 0, opErr(op, k, fmt.Errorf("%w: %w", ErrInvalidIngest, err))
		}
		if err := s.opts.validateValue(op, k, v); err != nil {
			abandon()
			return staged, 0, opErr(op, k, fmt.Errorf("%w: %w", ErrInvalidIngest, err))
		}
		if have && bytes.Compare(prev, k) >= 0 {
			abandon()
			return staged, 0, opErr(op, k, fmt.Errorf("%w: keys not strictly ascending: %s after %s",
				ErrInvalidIngest, SafeKey(k), SafeKey(prev)))
		}
		prev, have = append(prev[:0], k...), true
		if seq >= ikey.MaxSeq {
			abandon()
			return staged, 0, opErr(op, k, fmt.Errorf("%w: the %d-bit sequence space is exhausted", ErrIO, 8*ikey.SeqBytes))
		}
		seq++
		if file == nil {
			n, err := s.allocFileNumLocked()
			if err != nil {
				return staged, 0, classify(op, nil, err)
			}
			num, tmp = n, s.tmpPath(n)
			f, err := s.fs.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				return staged, 0, classify(op, nil, fmt.Errorf("lsm: creating ingested %s: %w", sstName(num), err))
			}
			file, tw, pending = f, sstable.NewWriter(f, s.writerOptions()), 0
		}
		ik = ikey.Encode(ik[:0], k, seq, ikey.KindValue)
		if err := tw.Add(ik, v); err != nil {
			abandon()
			return staged, 0, classify(op, k, fmt.Errorf("lsm: writing ingested %s: %w", sstName(num), err))
		}
		stats.Entries++
		pending += int64(len(k) + len(v) + ikey.TrailerSize)
		if pending >= s.opts.MemTableSize {
			if err := finish(); err != nil {
				return staged, 0, err
			}
		}
	}
	if err := src.Err(); err != nil {
		abandon()
		return staged, 0, opErr(op, nil, fmt.Errorf("lsm: the ingest source failed: %w", err))
	}
	if err := finish(); err != nil {
		return staged, 0, err
	}
	return staged, seq, nil
}

// ingestLevel is the shallowest level from 1 whose size budget holds total
// bytes, so that ingested tables are not compacted the moment they land.
func (s *LSMStore) ingestLevel(total int64) int {
	for level := 1; level < maxLevel-1; level++ {
		if total <= s.levelMaxBytes(level) {
			return level
		}
	}
	return maxLevel - 1
}
