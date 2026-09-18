package wal

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/adivishall/distributed-kv/internal/record"
)

// Handler receives records during replay, in the exact order they were
// appended. Either function may be nil, in which case records of that kind are
// decoded (so that corruption is still detected) and then discarded.
type Handler struct {
	Batch   func(Batch) error
	Applied func(AppliedIndex) error
}

// Recovery describes what Recover found. It is returned even when recovery
// truncated a torn tail, because "we threw away the last 137 bytes of your log"
// is information an operator needs rather than a detail to swallow.
type Recovery struct {
	SegmentsScanned int
	BytesScanned    int64
	RecordsApplied  int64
	BatchesApplied  int64
	OpsApplied      int64
	AppliedIndex    AppliedIndex
	SawAppliedIndex bool

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
func Recover(dir string, h Handler) (Recovery, error) {
	var rec Recovery

	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		// A directory that does not exist is an empty log, not an error: this
		// is what a brand-new database looks like.
		return rec, nil
	} else if err != nil {
		return rec, fmt.Errorf("wal: stat %s: %w", dir, err)
	}

	nums, ignored, err := listSegments(dir)
	if err != nil {
		return rec, err
	}
	rec.IgnoredEntries = ignored

	for i, seg := range nums {
		isFinal := i == len(nums)-1
		if err := replaySegment(dir, seg, isFinal, h, &rec); err != nil {
			return rec, err
		}
		rec.SegmentsScanned++
	}
	return rec, nil
}

func replaySegment(dir string, seg uint64, isFinal bool, h Handler, rec *Recovery) error {
	path := segmentPath(dir, seg)

	f, err := os.Open(path)
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
		if err := truncateSegment(path, truncTo); err != nil {
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
func truncateSegment(path string, at int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
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
