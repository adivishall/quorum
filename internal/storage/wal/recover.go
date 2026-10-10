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

	// SegmentsRemoved counts leftover segments below the log number that an
	// interrupted truncation left behind and recovery deleted (S3).
	SegmentsRemoved int

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

// RecoverWith is Recover on opts.FS, under opts.SyncMode. It replays every
// segment present, as before log truncation existed (logNumber 0).
//
// Since S1 it fsyncs the newest segment before replaying it, unless the sync
// mode is off (docs/STORAGE_INTEGRATION.md §7.5, gap (b)). A process killed with
// records still in the page cache leaves them readable but not durable; replay
// hands them to the engine, which may then make a table — and a MANIFEST edit —
// out of them before the kernel writes them back. Syncing first means
// everything replayed is durable: after open, recovered implies durable. Older
// segments were fsynced when they were rotated out.
func RecoverWith(dir string, opts Options, h Handler) (Recovery, error) {
	return RecoverFrom(dir, opts, 0, h)
}

// RecoverFrom replays the WAL from segment logNumber onward (S3,
// docs/STORAGE_INTEGRATION.md §9.7). logNumber is the MANIFEST's LogNumber: the
// segment whose first record is sequence LastSequence+1, so that the caller may
// seed its sequence counter from LastSequence and skip the flushed prefix. A
// logNumber of 0 means no truncation has ever happened — replay from the first
// segment present, exactly as RecoverWith always did (a pre-S3 directory).
//
// With logNumber > 0 it completes an interrupted truncation and detects a lost
// segment:
//   - every segment below logNumber is a leftover from a deletion that a crash
//     interrupted; it is removed (and the removal fsynced), because its records
//     are already in a flushed table and replaying them would double-count;
//   - the lowest segment that remains must be logNumber. A higher one means the
//     segment replay needs is gone, which a crash cannot produce once the
//     segment's creation was fsynced before the MANIFEST named it (§9.5) — so it
//     is ErrCorrupt, not something to work around.
func RecoverFrom(dir string, opts Options, logNumber uint64, h Handler) (Recovery, error) {
	var rec Recovery
	fsys := vfs.Or(opts.FS)

	if _, err := fsys.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		if logNumber > 0 {
			// The MANIFEST names a WAL segment, but the WAL directory is gone.
			// The directory is created once and never removed, and it lives in
			// the durable data directory, so this is damage, not a crash state.
			return rec, fmt.Errorf("wal: the MANIFEST requires segment %s but %s does not exist: %w",
				segmentName(logNumber), dir, ErrCorrupt)
		}
		// A directory that does not exist is an empty log, not an error: this
		// is what a brand-new database looks like.
		return rec, nil
	} else if err != nil {
		return rec, fmt.Errorf("wal: stat %s: %w", dir, err)
	}

	var (
		nums    []uint64
		ignored int
		err     error
	)
	if logNumber > 0 {
		// A truncation may have deleted a prefix of the segments, so list raw
		// (a missing prefix is expected), drop everything below the log number,
		// and only then require what remains to be contiguous.
		nums, ignored, err = listSegmentsRaw(fsys, dir)
		if err != nil {
			return rec, err
		}
		if nums, err = truncateBelow(fsys, opts, dir, logNumber, nums, &rec); err != nil {
			return rec, err
		}
		if err := checkContiguous(nums); err != nil {
			return rec, err
		}
	} else {
		if nums, ignored, err = listSegments(fsys, dir); err != nil {
			return rec, err
		}
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

// truncateBelow removes the leftover segments below logNumber (a deletion a
// crash interrupted), fsyncs the directory, and returns the segments at or above
// logNumber. It refuses a state a crash cannot produce: the lowest remaining
// segment must be logNumber itself, because the segment's creation was fsynced
// before the MANIFEST named it (docs/STORAGE_INTEGRATION.md §9.5).
func truncateBelow(fsys vfs.FS, opts Options, dir string, logNumber uint64, nums []uint64, rec *Recovery) ([]uint64, error) {
	var kept []uint64
	removed := false
	for _, s := range nums {
		if s < logNumber {
			if err := fsys.Remove(segmentPath(dir, s)); err != nil {
				return nil, fmt.Errorf("wal: removing leftover segment %s below log number %s: %w",
					segmentName(s), segmentName(logNumber), err)
			}
			rec.SegmentsRemoved++
			removed = true
			continue
		}
		kept = append(kept, s)
	}
	if removed && opts.SyncMode != SyncOff {
		if err := fsys.SyncDir(dir); err != nil {
			return nil, fmt.Errorf("wal: syncing %s after removing leftover segments: %w", dir, err)
		}
	}
	if len(kept) == 0 || kept[0] != logNumber {
		lowest := "none"
		if len(kept) > 0 {
			lowest = segmentName(kept[0])
		}
		return nil, fmt.Errorf(
			"wal: the MANIFEST requires segment %s but the lowest present at or above it is %s: %w",
			segmentName(logNumber), lowest, ErrCorrupt)
	}
	return kept, nil
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
