package wal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/vfs"
)

// Handler receives records during replay, in the exact order they were
// appended. Either function may be nil, in which case records of that kind are
// decoded (so that corruption is still detected) and then discarded.
type Handler struct {
	Batch   func(Batch) error
	Applied func(AppliedIndex) error
	// Apply receives each apply batch (S1): its mutations and its applied
	// index, as the one unit they were written as.
	Apply func(ApplyBatch) error
}

// Recovery describes what Recover found. It is returned even when recovery
// truncated a torn tail, because "we threw away the last 137 bytes of your log"
// is information an operator needs rather than a detail to swallow.
type Recovery struct {
	SegmentsScanned int
	BytesScanned    int64
	RecordsApplied  int64
	BatchesApplied  int64
	ApplyBatches    int64 // apply records replayed (S1)
	OpsApplied      int64
	AppliedIndex    AppliedIndex // the last applied index replayed, from either record kind
	SawAppliedIndex bool

	// SyncedNewest reports that the newest segment was fsynced before it was
	// replayed (S1, gap (b)), so everything recovery handed to the handler is
	// durable.
	SyncedNewest bool

	// IgnoredEntries counts directory entries that are not segment files.
	IgnoredEntries int

	// Truncated records a repaired torn tail.
	Truncated       bool
	TruncatedFile   string
	TruncatedAt     int64
	TruncatedBytes  int64
	TruncatedReason string
}

// Recover replays every WAL segment in dir, in order, into h.
//
// # The policy
//
// A crash during an append leaves a partial record at the very end of the
// newest segment. That is expected, and recovery repairs it by truncating back
// to the last good record boundary. Damage anywhere else is not expected, and
// recovery refuses to continue.
//
//	torn or bad record at the end of the newest segment -> truncate, continue
//	anything wrong anywhere else                        -> ErrCorrupt, refuse
//
// The reason for the asymmetry is that there is no safe way to skip a record.
// The framing has no block structure to resynchronise on, so "continue past the
// bad record" really means "guess where the next one starts". Guessing wrong
// produces a database that opens cleanly and is silently missing committed
// writes, which is far worse than refusing to open. See docs/WAL.md.
//
// Recover is called before Create. It leaves the directory in a state where the
// newest segment ends on a clean record boundary, so that appending to it
// cannot produce a record that starts after garbage.
//
// Recover uses the real filesystem and the default (batch) sync mode; see
// RecoverWith.
func Recover(dir string, h Handler) (Recovery, error) {
	return RecoverWith(dir, DefaultOptions(), h)
}

// RecoverWith is Recover on opts.FS, under opts.SyncMode.
//
// Since S1 it fsyncs the newest segment before replaying it, unless the sync
// mode is off (docs/STORAGE_INTEGRATION.md §7.5, gap (b)). A process killed with
// records still in the page cache leaves them readable but not durable; replay
// hands them to the engine, which may then make a table — and a MANIFEST edit —
// out of them before the kernel writes them back. Syncing first means
// everything replayed is durable: after open, recovered implies durable. Older
// segments were fsynced when they were rotated out.
func RecoverWith(dir string, opts Options, h Handler) (Recovery, error) {
	var rec Recovery
	fsys := vfs.Or(opts.FS)

	if _, err := fsys.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		// A directory that does not exist is an empty log, not an error: this
		// is what a brand-new database looks like.
		return rec, nil
	} else if err != nil {
		return rec, fmt.Errorf("wal: stat %s: %w", dir, err)
	}

	nums, ignored, err := listSegments(fsys, dir)
	if err != nil {
		return rec, err
	}
	rec.IgnoredEntries = ignored

	for i, seg := range nums {
		isFinal := i == len(nums)-1
		if isFinal && opts.SyncMode != SyncOff {
			if err := syncSegment(fsys, segmentPath(dir, seg)); err != nil {
				return rec, err
			}
			rec.SyncedNewest = true
		}
		if err := replaySegment(fsys, dir, seg, isFinal, h, &rec); err != nil {
			return rec, err
		}
		rec.SegmentsScanned++
	}
	return rec, nil
}

// syncSegment fsyncs a segment as it stands, torn tail and all: syncing bytes
// that are about to be truncated is harmless, and the truncation is synced too.
func syncSegment(fsys vfs.FS, path string) error {
	f, err := fsys.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: opening %s to sync it before replay: %w", path, err)
	}
	if err := syncFile(f); err != nil {
		_ = f.Close()
		return fmt.Errorf("wal: syncing %s before replay: %w", path, err)
	}
	return f.Close()
}

func replaySegment(fsys vfs.FS, dir string, seg uint64, isFinal bool, h Handler, rec *Recovery) error {
	path := segmentPath(dir, seg)

	f, err := fsys.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("wal: opening segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("wal: stat %s: %w", path, err)
	}
	size := info.Size()
	rec.BytesScanned += size

	r := record.NewReader(f, path, size)

	var (
		replayErr error
		truncTo   int64 = -1
		reason    string
	)

readLoop:
	for {
		kind, payload, err := r.Next()
		switch {
		case errors.Is(err, io.EOF):
			// A clean end of records. There can still be fewer than a
			// header's worth of trailing bytes from an interrupted append;
			// those are not a record, but they must go, or the next append
			// would begin after garbage.
			if r.NextOffset() < size {
				if !isFinal {
					replayErr = fmt.Errorf(
						"wal: %s has %d trailing bytes after its last record, but it is not the newest segment: %w",
						path, size-r.NextOffset(), ErrCorrupt)
					break readLoop
				}
				truncTo = r.NextOffset()
				reason = fmt.Sprintf("%d trailing bytes from an interrupted append", size-truncTo)
			}
			break readLoop

		case errors.Is(err, record.ErrTornTail):
			if !isFinal {
				// Only the newest segment can be mid-append when a crash
				// happens: an older segment was completed and closed before
				// its successor was created. A torn record in one means
				// something other than a crash damaged it.
				replayErr = fmt.Errorf(
					"wal: torn record in %s, which is not the newest segment (a crash can only damage the newest): %w: %v",
					path, ErrCorrupt, err)
				break readLoop
			}
			var re *record.Error
			if errors.As(err, &re) {
				truncTo = re.Offset
				reason = re.Reason
			} else {
				truncTo = r.NextOffset()
				reason = err.Error()
			}
			break readLoop

		case err != nil:
			// ErrCorrupt, or anything else unexpected. Never repaired.
			replayErr = err
			break readLoop
		}

		if err := dispatch(kind, payload, h, rec, path, r.Offset()); err != nil {
			replayErr = err
			break readLoop
		}
		rec.RecordsApplied++
	}

	if cerr := f.Close(); cerr != nil && replayErr == nil {
		replayErr = fmt.Errorf("wal: closing %s: %w", path, cerr)
	}
	if replayErr != nil {
		return replayErr
	}

	if truncTo >= 0 {
		if err := truncateSegment(fsys, path, truncTo); err != nil {
			return err
		}
		rec.Truncated = true
		rec.TruncatedFile = path
		rec.TruncatedAt = truncTo
		rec.TruncatedBytes = size - truncTo
		rec.TruncatedReason = reason
	}
	return nil
}

// dispatch decodes one record and hands it to the handler.
func dispatch(kind record.Kind, payload []byte, h Handler, rec *Recovery, path string, off int64) error {
	switch kind {
	case KindWriteBatch:
		batch, err := DecodeBatch(payload)
		if err != nil {
			return fmt.Errorf("wal: %s offset %d: %w", path, off, err)
		}
		rec.BatchesApplied++
		rec.OpsApplied += int64(len(batch))
		if h.Batch != nil {
			if err := h.Batch(batch); err != nil {
				return fmt.Errorf("wal: applying batch from %s offset %d: %w", path, off, err)
			}
		}
		return nil

	case KindApplyBatch:
		ab, err := DecodeApplyBatch(payload)
		if err != nil {
			return fmt.Errorf("wal: %s offset %d: %w", path, off, err)
		}
		// The writer refuses an index that does not advance; one in the log
		// was not written by it.
		if !ab.Advances(rec.AppliedIndex) {
			return fmt.Errorf("wal: %s offset %d: apply batch at (%d, %d) does not advance past (%d, %d): %w",
				path, off, ab.Applied.Index, ab.Applied.Term, rec.AppliedIndex.Index, rec.AppliedIndex.Term, ErrCorrupt)
		}
		rec.ApplyBatches++
		rec.OpsApplied += int64(len(ab.Ops))
		if h.Apply != nil {
			if err := h.Apply(ab); err != nil {
				return fmt.Errorf("wal: applying apply batch from %s offset %d: %w", path, off, err)
			}
		}
		rec.AppliedIndex = ab.Applied
		rec.SawAppliedIndex = true
		return nil

	case KindAppliedIndex:
		applied, err := DecodeAppliedIndex(payload)
		if err != nil {
			return fmt.Errorf("wal: %s offset %d: %w", path, off, err)
		}
		rec.AppliedIndex = applied
		rec.SawAppliedIndex = true
		if h.Applied != nil {
			if err := h.Applied(applied); err != nil {
				return fmt.Errorf("wal: applying applied-index from %s offset %d: %w", path, off, err)
			}
		}
		return nil

	default:
		// An unknown kind is not treated as a newer format to skip over.
		// Skipping a record whose meaning is unknown means replaying a state
		// that is missing a mutation, with no indication that anything is
		// wrong.
		return fmt.Errorf("wal: %s offset %d: unknown record kind %#x: %w",
			path, off, uint8(kind), ErrCorrupt)
	}
}

// truncateSegment cuts a segment back to a known-good record boundary and makes
// the truncation durable before anything is appended past it.
func truncateSegment(fsys vfs.FS, path string, at int64) error {
	f, err := fsys.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: opening %s to truncate: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	if err := f.Truncate(at); err != nil {
		return fmt.Errorf("wal: truncating %s to %d: %w", path, at, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: syncing truncation of %s: %w", path, err)
	}
	return nil
}
