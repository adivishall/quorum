// Package raftlog is the durable Raft log for one group (Phase 9, docs/RAFT.md
// §10, ADR-016). It is an append-only stream of records in the shared §2 framing
// (internal/record): Entry records and HardState records.
//
// A conflicting-suffix replacement is APPENDED, not rewritten: recovery replays
// records in file order and applies each Entry at index i as "set index i, drop
// anything above i" — the Phase 8 TruncateAndAppend semantics per record — so the
// final in-memory log is reconstructed from an append-only file and the last
// HardState wins. commitIndex is persisted in HardState as an optimization only.
//
// Crash policy (distinct from the WAL's): a torn final record truncates to the
// last good offset (a crash mid-append whose dependent reply was never sent). Any
// other damage — a checksum mismatch with bytes following, a zero-filled header
// with data after it, an unknown kind, a malformed payload, or an impossible index
// progression — is fatal and refuses to open, never silently skipped.
package raftlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/replication"
)

// Record kinds in the Raft-log namespace (the framing's kind byte is per-log-type,
// internal/record). Distinct from WAL/MANIFEST kinds, which use the same values
// for different meanings.
const (
	kindEntry     record.Kind = 1
	kindHardState record.Kind = 2

	// MaxEntryDataLen bounds one entry's opaque bytes on disk (the 1 MiB value
	// limit, docs/DESIGN.md §1), so a corrupt length cannot drive a wild alloc
	// beyond what record.MaxRecordSize already bounds.
	MaxEntryDataLen = 1 << 20
	// MaxVoteLen bounds a persisted votedFor id.
	MaxVoteLen = 256
)

// Entry and HardState mirror the raft core's durable types without importing it
// (raftlog is a lower layer). The driver converts between these and raft's types.
type (
	Entry     = replication.Entry
	HardState struct {
		Term   uint64
		Vote   replication.NodeID
		Commit uint64
	}
)

// Recovered is the state reconstructed from the durable log on Open.
type Recovered struct {
	Entries   []Entry
	HardState HardState
}

// ErrCorrupt means the log holds damage a crash cannot explain; opening is
// refused. It wraps record.ErrCorrupt where the framing detected it.
var ErrCorrupt = errors.New("raftlog: corrupt log")

// Log is an append-only durable Raft log. It is not safe for concurrent use — the
// driver's single goroutine owns it.
type Log struct {
	path string
	f    *os.File
	w    *record.Writer
	sync bool
}

// Options configures a Log.
type Options struct {
	// Sync fsyncs after every Save. Required for the durability the Raft protocol
	// assumes; tests may disable it for speed where durability is not under test.
	Sync bool
}

// Open opens (creating if absent) the durable log at path and replays it. It
// truncates a torn final record and refuses to open on any other damage. The
// parent directory is fsynced on first creation so the file itself is durable.
func Open(path string, opts Options) (*Log, *Recovered, error) {
	created := false
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		created = true
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if created {
		if err := syncDir(filepath.Dir(path)); err != nil {
			_ = f.Close()
			return nil, nil, err
		}
	}

	rec, err := replay(f, path)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	// Position at the end of the last good record for appending.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	l := &Log{path: path, f: f, w: record.NewWriter(f), sync: opts.Sync}
	return l, rec, nil
}

// replay reads the whole file, reconstructing the final entry set and last
// HardState, and truncates a torn tail.
func replay(f *os.File, path string) (*Recovered, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := record.NewReader(f, path, info.Size())
	rec := &Recovered{}
	for {
		kind, payload, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if errors.Is(err, record.ErrTornTail) {
				break // a crash mid-append; truncate below to the last good offset
			}
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		switch kind {
		case kindEntry:
			e, err := decodeEntry(payload)
			if err != nil {
				return nil, err
			}
			// "set index, drop above": enforce a contiguous, hole-free progression.
			if e.Index == 0 || e.Index > uint64(len(rec.Entries))+1 {
				return nil, fmt.Errorf("%w: entry index %d creates a gap (have %d)", ErrCorrupt, e.Index, len(rec.Entries))
			}
			rec.Entries = rec.Entries[:e.Index-1]
			rec.Entries = append(rec.Entries, e)
		case kindHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return nil, err
			}
			rec.HardState = hs
		default:
			return nil, fmt.Errorf("%w: unknown record kind %d", ErrCorrupt, kind)
		}
	}
	// Truncate away any torn tail or stray trailing bytes past the last good record.
	if r.NextOffset() < info.Size() {
		if err := f.Truncate(r.NextOffset()); err != nil {
			return nil, err
		}
	}
	// Clamp the persisted commit to what the log actually holds (it is an
	// optimization; correctness never trusts it beyond this — docs/DESIGN.md §8.1).
	if rec.HardState.Commit > uint64(len(rec.Entries)) {
		rec.HardState.Commit = uint64(len(rec.Entries))
	}
	return rec, nil
}

// Save durably records a Ready's HardState (if any) and entries in one fsync. On a
// suffix replacement the entries simply extend the file; replay reconstructs the
// truncation. It writes entries in order, then the HardState, so that after a
// crash a HardState referencing a commit index is never durable before the entries
// it covers.
func (l *Log) Save(hs *HardState, entries []Entry) error {
	for _, e := range entries {
		if _, err := l.w.Append(kindEntry, encodeEntry(e)); err != nil {
			return err
		}
	}
	if hs != nil {
		if _, err := l.w.Append(kindHardState, encodeHardState(*hs)); err != nil {
			return err
		}
	}
	if l.sync {
		return l.f.Sync()
	}
	return nil
}

// Sync fsyncs the file.
func (l *Log) Sync() error { return l.f.Sync() }

// Close syncs and closes the file.
func (l *Log) Close() error {
	if l.f == nil {
		return nil
	}
	err := l.f.Sync()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}

// --- payload codecs (bounded, explicit; ADR-003) ---

func encodeEntry(e Entry) []byte {
	buf := make([]byte, 0, 2*binary.MaxVarintLen64+len(e.Data)+binary.MaxVarintLen64)
	buf = appendUvarint(buf, e.Index)
	buf = appendUvarint(buf, e.Term)
	buf = appendUvarint(buf, uint64(len(e.Data)))
	return append(buf, e.Data...)
}

func decodeEntry(p []byte) (Entry, error) {
	r := payloadReader{b: p}
	var e Entry
	var err error
	if e.Index, err = r.uvarint(); err != nil {
		return Entry{}, err
	}
	if e.Term, err = r.uvarint(); err != nil {
		return Entry{}, err
	}
	if e.Data, err = r.bytes(MaxEntryDataLen); err != nil {
		return Entry{}, err
	}
	if err := r.done(); err != nil {
		return Entry{}, err
	}
	return e, nil
}

func encodeHardState(hs HardState) []byte {
	buf := make([]byte, 0, 3*binary.MaxVarintLen64+len(hs.Vote))
	buf = appendUvarint(buf, hs.Term)
	buf = appendUvarint(buf, hs.Commit)
	buf = appendUvarint(buf, uint64(len(hs.Vote)))
	return append(buf, hs.Vote...)
}

func decodeHardState(p []byte) (HardState, error) {
	r := payloadReader{b: p}
	var hs HardState
	var err error
	if hs.Term, err = r.uvarint(); err != nil {
		return HardState{}, err
	}
	if hs.Commit, err = r.uvarint(); err != nil {
		return HardState{}, err
	}
	vote, err := r.bytes(MaxVoteLen)
	if err != nil {
		return HardState{}, err
	}
	hs.Vote = replication.NodeID(vote)
	if err := r.done(); err != nil {
		return HardState{}, err
	}
	return hs, nil
}

func appendUvarint(b []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(b, tmp[:n]...)
}

type payloadReader struct {
	b []byte
	i int
}

func (r *payloadReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.i:])
	if n <= 0 {
		return 0, fmt.Errorf("%w: bad varint", ErrCorrupt)
	}
	r.i += n
	return v, nil
}

func (r *payloadReader) bytes(max int) ([]byte, error) {
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(max) || int(n) > len(r.b)-r.i {
		return nil, fmt.Errorf("%w: length %d out of range", ErrCorrupt, n)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]byte, n)
	copy(out, r.b[r.i:r.i+int(n)])
	r.i += int(n)
	return out, nil
}

func (r *payloadReader) done() error {
	if r.i != len(r.b) {
		return fmt.Errorf("%w: trailing bytes in record payload", ErrCorrupt)
	}
	return nil
}

// syncDir fsyncs a directory so a file creation within it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
