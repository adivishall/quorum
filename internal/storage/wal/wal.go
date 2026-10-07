package wal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/vfs"
)

// SyncMode selects when the WAL flushes to stable storage. The three modes and
// the guarantee each one provides are specified in docs/DESIGN.md §3 and
// restated in docs/WAL.md; the summary is that they differ only in when fsync
// happens, never in when write(2) happens.
type SyncMode int

// SyncBatch is deliberately the zero value. A caller that forgets to set a
// mode gets the documented default rather than the test-only one; the
// alternative ordering makes "I left the struct empty" silently mean "never
// flush", which is the single worst default a write-ahead log could have.
const (
	// SyncBatch calls fsync periodically, bounded by time and by bytes. This
	// is the default.
	SyncBatch SyncMode = iota
	// SyncOff never calls fsync. Data reaches the kernel but is never flushed
	// on Quorum's initiative. Test-only.
	SyncOff
	// SyncAlways flushes before every append returns.
	SyncAlways
)

func (m SyncMode) String() string {
	switch m {
	case SyncOff:
		return "off"
	case SyncBatch:
		return "batch"
	case SyncAlways:
		return "sync"
	default:
		return fmt.Sprintf("SyncMode(%d)", int(m))
	}
}

// ParseSyncMode parses the configuration spelling of a sync mode.
func ParseSyncMode(s string) (SyncMode, error) {
	switch s {
	case "off":
		return SyncOff, nil
	case "batch":
		return SyncBatch, nil
	case "sync":
		return SyncAlways, nil
	default:
		return 0, fmt.Errorf("wal: unknown sync mode %q (want off, batch or sync)", s)
	}
}

// Defaults.
//
// SegmentSize is not derived from a measurement and is not claimed to be
// optimal. It is a deliberate middle choice: large enough that rotation is
// rare, small enough that one segment is quick to scan during recovery and
// cheap to reclaim once log truncation exists in Phase 4. Phase 5 benchmarks
// are where a number earns the right to be called tuned.
const (
	DefaultSegmentSize  = 16 << 20 // 16 MiB
	DefaultSyncInterval = 100 * time.Millisecond
	DefaultSyncBytes    = 1 << 20 // 1 MiB
)

// ErrClosed is returned by operations on a closed WAL.
var ErrClosed = errors.New("wal: closed")

// ErrRecordTooLarge is returned for an append whose record exceeds the
// framing's maximum (record.MaxRecordSize). Nothing was written and the log
// is unaffected.
var ErrRecordTooLarge = errors.New("wal: record too large")

// Options configures a WAL.
type Options struct {
	// SyncMode selects the durability policy. The zero value is SyncOff, so
	// callers should start from DefaultOptions rather than a bare struct.
	SyncMode SyncMode

	// SegmentSize is the rotation threshold in bytes. A record is never split
	// across segments, so a segment can exceed this by up to one record.
	SegmentSize int64

	// SyncInterval and SyncBytes bound the exposure window in SyncBatch mode:
	// a flush happens when either the interval elapses or that many unflushed
	// bytes accumulate.
	SyncInterval time.Duration
	SyncBytes    int64

	// FS is the filesystem every WAL file operation goes through: segment
	// creation, appends, reads, truncation, fsyncs, the directory listing and
	// directory fsyncs (S1, docs/STORAGE_INTEGRATION.md §7.6). Nil is the real
	// one. Tests substitute internal/fault's crash-consistent MemFS, so the WAL
	// takes part in the process-crash and power-loss model.
	FS vfs.FS
}

// DefaultOptions returns the documented defaults: batch sync, 16 MiB segments.
func DefaultOptions() Options {
	return Options{
		SyncMode:     SyncBatch,
		SegmentSize:  DefaultSegmentSize,
		SyncInterval: DefaultSyncInterval,
		SyncBytes:    DefaultSyncBytes,
	}
}

func (o *Options) applyDefaults() {
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = DefaultSyncInterval
	}
	if o.SyncBytes <= 0 {
		o.SyncBytes = DefaultSyncBytes
	}
}

// WAL is an append-only, segmented write-ahead log.
//
// # Append ordering
//
// A single mutex serialises everything: appends, rotation, flushes from the
// background syncer, and Close. The WAL is a single-writer structure by nature —
// records must land in the file in exactly the order they were accepted, or
// replay reconstructs a state that never existed — so serialising is not a
// missed optimisation, it is the requirement.
//
// The cost is real and is stated rather than hidden: in SyncAlways mode a
// flush is held under the lock, so concurrent writers queue behind it. Phase 5
// is where that gets measured; group commit is the standard answer if the
// measurement justifies it.
//
// # What Append guarantees on return
//
// In every mode, a successful Append has completed a write(2): the bytes are in
// the kernel's page cache and will survive this process being killed. The modes
// differ only in whether they have also been flushed to the device:
//
//	off    - no flush. Survives SIGKILL. Lost on OS crash or power loss.
//	batch  - flushed within SyncInterval or SyncBytes. Survives SIGKILL.
//	         An OS crash or power cut can lose up to that window.
//	sync   - flushed before Append returns. Survives power loss to the extent
//	         the hardware honours the barrier.
//
// The distinction between "survives SIGKILL" and "survives power loss" is the
// whole reason the modes exist, and it is why nothing here buffers records in
// user space: a record held in a bufio.Writer would be lost on SIGKILL, which
// would make the batch-mode guarantee false.
type WAL struct {
	dir  string
	opts Options

	fs       vfs.FS
	mu       sync.Mutex
	f        vfs.File
	w        *record.Writer
	seg      uint64 // active segment number
	segBytes int64  // bytes written to the active segment
	unsynced int64  // bytes written since the last flush
	syncs    int64  // count of fsyncs performed (for measurement; guarded by mu)
	closed   bool

	// syncErr latches a failed flush. A background flush that fails means the
	// WAL can no longer honour its durability promise, and continuing to
	// accept appends as though nothing happened would be the worst possible
	// response — the caller would keep acknowledging writes that may not
	// survive. Every subsequent append fails with this error.
	syncErr error
	// flushFailed is set when the latched failure is a failed flush, rather
	// than a failed write: Close must not flush again after one (see Close).
	flushFailed bool

	// fullSyncOK records whether the platform's strongest flush is available
	// on this file. See supportsFullSync.
	fullSyncOK bool

	stopSyncer chan struct{}
	syncerDone sync.WaitGroup
}

// Seams for this package's own tests: how a segment is flushed, and the
// writer its records reach the file through. Production uses fullSync and the
// file itself; a test fails a flush, or tears a write, at an exact point.
var (
	syncFile   = fullSync
	segmentOut = func(f vfs.File) io.Writer { return f }
)

// fullSync flushes a segment as far as the platform allows: a real file with
// the platform's strongest flush (F_FULLFSYNC on Darwin), any other vfs.File —
// a test's in-memory one — with its own Sync.
func fullSync(f vfs.File) error {
	if osf, ok := f.(*os.File); ok {
		return fullSyncOS(osf)
	}
	return f.Sync()
}

// supportsFullSync reports whether fullSync gives its strongest flush on f.
func supportsFullSync(f vfs.File) bool {
	if osf, ok := f.(*os.File); ok {
		return supportsFullSyncOS(osf)
	}
	return true
}

// Create opens the WAL in dir for appending, creating dir if necessary.
//
// Recover must be called first on any directory that may already contain
// segments: Create appends to the newest segment as it finds it, and it is
// Recover's job to have removed any torn tail so that the append lands on a
// clean record boundary.
func Create(dir string, opts Options) (*WAL, error) {
	opts.applyDefaults()
	fsys := vfs.Or(opts.FS)

	_, statErr := fsys.Stat(dir)
	if err := fsys.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: creating %s: %w", dir, err)
	}
	if errors.Is(statErr, fs.ErrNotExist) && opts.SyncMode != SyncOff {
		// A new directory is not durable until its parent is flushed, any more
		// than a new segment is until this one is (S1, gap (a)): without it a
		// power loss can take the directory, and every segment in it, synced
		// or not.
		if err := fsys.SyncDir(filepath.Dir(filepath.Clean(dir))); err != nil {
			return nil, fmt.Errorf("wal: syncing the parent of %s: %w", dir, err)
		}
	}

	nums, _, err := listSegments(fsys, dir)
	if err != nil {
		return nil, err
	}

	w := &WAL{dir: dir, opts: opts, fs: fsys}

	seg := firstSegment
	if len(nums) > 0 {
		seg = nums[len(nums)-1]
	}
	if err := w.openSegment(seg, len(nums) == 0); err != nil {
		return nil, err
	}

	if opts.SyncMode == SyncBatch {
		w.startSyncer()
	}
	return w, nil
}

// openSegment makes seg the active segment, positioned at its end.
func (w *WAL) openSegment(seg uint64, isNew bool) error {
	path := segmentPath(w.dir, seg)

	f, err := w.fs.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: opening segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("wal: stat %s: %w", path, err)
	}

	if isNew && w.opts.SyncMode != SyncOff {
		// A newly created file is not durable until its parent directory is
		// flushed, or a crash can leave the segment's records in the page
		// cache with no directory entry pointing at them.
		if err := w.fs.SyncDir(w.dir); err != nil {
			_ = f.Close()
			return fmt.Errorf("wal: syncing directory %s: %w", w.dir, err)
		}
	}

	w.f = f
	w.w = record.NewWriter(segmentOut(f))
	w.seg = seg
	w.segBytes = info.Size()
	w.fullSyncOK = supportsFullSync(f)
	return nil
}

// AppendBatch appends one batch of mutations as a single framed record.
//
// The batch is atomic with respect to a crash: its checksum covers every
// operation, so either all of them replay or none of them do.
func (w *WAL) AppendBatch(b Batch) error {
	if len(b) == 0 {
		return fmt.Errorf("wal: refusing to append an empty batch")
	}
	return w.append(KindWriteBatch, b.AppendTo(nil))
}

// AppendApply appends a state-machine application — its mutations and its
// applied index — as ONE record (S1, docs/STORAGE_INTEGRATION.md §7.3). An empty
// Ops is valid: the index alone advances. Whether the index advances is the
// caller's rule to enforce (the store does, and replay re-checks it); this
// refuses only an index or term of zero, which no applied entry has.
func (w *WAL) AppendApply(a ApplyBatch) error {
	if a.Applied.Index == 0 || a.Applied.Term == 0 {
		return fmt.Errorf("wal: refusing an apply batch at (%d, %d): no applied entry has a zero index or term",
			a.Applied.Index, a.Applied.Term)
	}
	return w.append(KindApplyBatch, a.AppendTo(nil))
}

// AppendAppliedIndex appends durable applied-index metadata. See AppliedIndex
// for what this does and does not mean in Phase 2.
func (w *WAL) AppendAppliedIndex(a AppliedIndex) error {
	return w.append(KindAppliedIndex, a.AppendTo(nil))
}

func (w *WAL) append(kind record.Kind, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}
	if w.syncErr != nil {
		return w.syncErr
	}

	// A record the framing cannot hold is refused before anything is written:
	// a caller error, not a failure of the log, so it latches nothing. (The
	// framing's own refusal comes after the call that latches.)
	if len(payload) > record.MaxRecordSize {
		return fmt.Errorf("%w: a record of %d bytes exceeds the %d-byte maximum; nothing was written",
			ErrRecordTooLarge, len(payload), record.MaxRecordSize)
	}

	// Rotate before writing, never in the middle: a record is never split
	// across segments, so replay can treat each segment independently.
	if w.segBytes > 0 && w.segBytes >= w.opts.SegmentSize {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}

	n, err := w.w.Append(kind, payload)
	w.segBytes += int64(n)
	w.unsynced += int64(n)
	if err != nil {
		// The file may now hold a partial record. That is recoverable — it is
		// precisely the torn tail the reader detects — as long as nothing is
		// ever written after it: a later record would turn the torn tail into
		// damage mid-file, which recovery refuses. So the failure latches
		// (audit D10), and the caller is not told the write succeeded.
		w.syncErr = fmt.Errorf("wal: appending to %s: %w", segmentName(w.seg), err)
		return w.syncErr
	}

	switch w.opts.SyncMode {
	case SyncAlways:
		return w.syncLocked()
	case SyncBatch:
		if w.unsynced >= w.opts.SyncBytes {
			return w.syncLocked()
		}
	case SyncOff:
	}
	return nil
}

// rotateLocked closes the active segment and starts the next one.
func (w *WAL) rotateLocked() error {
	if w.seg >= maxSegmentNumber {
		return fmt.Errorf("wal: segment number %d would exceed the %d-digit name format",
			w.seg+1, segmentDigits)
	}
	// Flush the outgoing segment before moving on, so that a segment we will
	// never write to again is not left depending on a future flush.
	if w.opts.SyncMode != SyncOff {
		if err := w.syncLocked(); err != nil {
			return err
		}
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("wal: closing segment %s: %w", segmentName(w.seg), err)
	}
	w.unsynced = 0
	return w.openSegment(w.seg+1, true)
}

// syncLocked flushes the active segment. The caller must hold w.mu.
func (w *WAL) syncLocked() error {
	if w.unsynced == 0 {
		return nil
	}
	if err := syncFile(w.f); err != nil {
		ferr := fmt.Errorf("wal: flushing %s: %w", segmentName(w.seg), err)
		if w.syncErr == nil {
			w.syncErr = ferr // a failed write latched first stays the cause
		}
		w.flushFailed = true
		return ferr
	}
	w.unsynced = 0
	w.syncs++ // count fsyncs so a benchmark can prove a batch flush happened
	return nil
}

// Sync flushes the active segment regardless of the configured mode.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	// After a failed write the records before it are still owed their flush
	// (only a failed flush forbids another); the latched failure is reported
	// all the same, since the log takes no more appends.
	if w.flushFailed {
		return w.syncErr
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	return w.syncErr
}

// startSyncer runs the periodic flush for SyncBatch mode.
func (w *WAL) startSyncer() {
	w.stopSyncer = make(chan struct{})
	w.syncerDone.Add(1)
	go func() {
		defer w.syncerDone.Done()
		t := time.NewTicker(w.opts.SyncInterval)
		defer t.Stop()
		for {
			select {
			case <-w.stopSyncer:
				return
			case <-t.C:
				w.mu.Lock()
				if !w.closed && !w.flushFailed {
					// A failure latches into w.syncErr and is surfaced on the
					// next append; it is not logged and forgotten. A failed
					// write does not stop the flush of what came before it.
					_ = w.syncLocked()
				}
				w.mu.Unlock()
			}
		}
	}()
}

// Close flushes and closes the WAL. It is idempotent.
//
// Close flushes in every mode except SyncOff, where flushing would contradict
// the mode's definition and would make a test that uses it look durable.
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	stop := w.stopSyncer
	w.mu.Unlock()

	// Stop the syncer without holding the mutex it needs.
	if stop != nil {
		close(stop)
		w.syncerDone.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// After a failed flush, no fsync: a second fsync can succeed once the
	// kernel has dropped the pages the first failed to write, and reporting
	// that as durability is the fsyncgate error (audit M11). After a failed
	// write, the records acknowledged before it are still owed their flush;
	// a partial record it makes durable is a torn tail recovery repairs.
	// Either way Close reports the latched failure.
	syncErr := w.syncErr
	if !w.flushFailed && w.opts.SyncMode != SyncOff {
		if err := syncFile(w.f); err != nil && syncErr == nil {
			syncErr = fmt.Errorf("wal: flushing %s on close: %w", segmentName(w.seg), err)
		}
	}
	if err := w.f.Close(); err != nil && syncErr == nil {
		syncErr = fmt.Errorf("wal: closing %s: %w", segmentName(w.seg), err)
	}
	return syncErr
}

// Stats reports the WAL's current shape. It is for tests, logs and the Phase 16
// metrics; nothing in the write path depends on it.
type Stats struct {
	Dir             string
	ActiveSegment   uint64
	ActiveBytes     int64
	UnsyncedBytes   int64
	Syncs           int64 // fsyncs performed since the WAL was opened
	SyncMode        SyncMode
	FullSyncEnabled bool
}

// Stats returns a snapshot of the WAL's state.
func (w *WAL) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Stats{
		Dir:             w.dir,
		ActiveSegment:   w.seg,
		ActiveBytes:     w.segBytes,
		UnsyncedBytes:   w.unsynced,
		Syncs:           w.syncs,
		SyncMode:        w.opts.SyncMode,
		FullSyncEnabled: w.fullSyncOK,
	}
}

// PlatformSyncNote describes, for documentation and diagnostics, what a
// successful flush actually does on this platform.
func PlatformSyncNote() string { return platformSyncNote }
