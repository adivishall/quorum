// Package raftlog is the durable Raft log for one group (Phase 9, docs/RAFT.md
// §10, ADR-016). It is an append-only stream of records in the shared §2 framing
// (internal/record): Entry records and HardState records.
//
// A conflicting-suffix replacement is APPENDED, not rewritten: recovery replays
// records in file order and applies each Entry at index i as "set index i, drop
// anything above i" — the Phase 8 TruncateAndAppend semantics per record — so the
// final in-memory log is reconstructed from an append-only file and the last
// HardState wins. commitIndex is persisted in HardState as an optimization only.
// Within one Save the records are ordered so that a crash between any two of
// them leaves a log recovery accepts (SavePlan, Phase 11).
//
// Crash policy (distinct from the WAL's): a torn final record truncates to the
// last good offset (a crash mid-append whose dependent reply was never sent). Any
// other damage — a checksum mismatch with bytes following, a zero-filled header
// with data after it, an unknown kind, a malformed payload, or an impossible index
// progression — is fatal and refuses to open, never silently skipped.
//
// Failure policy (Phase 10, INV-F1): a write or fsync that fails poisons the Log.
// Every later Save returns the original error and touches the file no more,
// because after a failed or short write the file may end in a partial record, and
// appending behind it would turn a recoverable torn tail into mid-log corruption;
// and after a failed fsync, a later "successful" fsync proves nothing about the
// earlier data. Recovery is a fresh Open, which truncates any torn tail.
//
// All file access goes through a vfs.FS (Options.FS; nil is the real OS), which is
// the seam Phase 10 fault injection uses — the code here runs unchanged on it.
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
	"github.com/adivishall/quorum/internal/vfs"
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

// ErrFailed means an earlier write or fsync failed and the Log has refused all
// writes since (see the package's failure policy). The error returned by Save
// wraps both ErrFailed and the original failure.
var ErrFailed = errors.New("raftlog: log failed; no further writes")

// Log is an append-only durable Raft log. It is not safe for concurrent use — the
// driver's single goroutine owns it.
type Log struct {
	path   string
	f      vfs.File
	w      *record.Writer
	sync   bool
	failed error     // the first write/fsync failure; sticky
	hs     HardState // the last HardState durably written (recovered at Open)
}

// Options configures a Log.
type Options struct {
	// Sync fsyncs after every Save. Required for the durability the Raft protocol
	// assumes; tests may disable it for speed where durability is not under test.
	Sync bool
	// FS is the filesystem the log lives on. Nil means the real OS filesystem;
	// tests substitute internal/fault's crash model or fault injector here.
	FS vfs.FS
}

// Open opens (creating if absent) the durable log at path and replays it. It
// truncates a torn final record and refuses to open on any other damage. The
// parent directory is fsynced on first creation so the file itself is durable.
//
// Before returning, Open fsyncs the file: the state it recovered is durable
// before the caller can act on it. That matters after a failed fsync. The
// records a failed Save wrote may still be in the page cache, so a restarted
// process reads them back — e.g. a vote — and would otherwise act on state that a
// power loss could still erase (found by the Phase 10 simulator; docs/FAULTS.md).
// This assumes a successful fsync is honest; a kernel that silently drops pages
// after a write-back error ("fsyncgate") is outside what this can defend against.
func Open(path string, opts Options) (*Log, *Recovered, error) {
	fsys := vfs.Or(opts.FS)
	created := false
	if _, err := fsys.Stat(path); errors.Is(err, os.ErrNotExist) {
		created = true
	}
	f, err := fsys.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if created {
		if err := fsys.SyncDir(filepath.Dir(path)); err != nil {
			_ = f.Close()
			return nil, nil, err
		}
	}

	rec, err := replay(f, path)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	// Make the recovered state (and any tail repair) durable before it is used.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	// Position at the end of the last good record for appending.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	l := &Log{path: path, f: f, w: record.NewWriter(f), sync: opts.Sync, hs: rec.HardState}
	return l, rec, nil
}

// replay reads the whole file, reconstructing the final entry set and last
// HardState, and truncates a torn tail (Open then fsyncs the result).
func replay(f vfs.File, path string) (*Recovered, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	rec, next, err := readRecords(f, path, info.Size())
	if err != nil {
		return nil, err
	}
	// Truncate away any torn tail or stray trailing bytes past the last good
	// record, so nothing is ever appended behind garbage.
	if next < info.Size() {
		if err := f.Truncate(next); err != nil {
			return nil, err
		}
	}
	return rec, nil
}

// readRecords reconstructs the recovered state from a record stream of exactly
// size bytes and returns it together with the offset just past the last good
// record (the safe truncation/append point). It does not mutate the stream. A
// torn final record stops the read (recoverable); any other damage is fatal.
func readRecords(r io.Reader, name string, size int64) (*Recovered, int64, error) {
	rd := record.NewReader(r, name, size)
	rec := &Recovered{}
	for {
		kind, payload, err := rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if errors.Is(err, record.ErrTornTail) {
				break // a crash mid-append; the last good offset is the append point
			}
			return nil, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		switch kind {
		case kindEntry:
			e, err := decodeEntry(payload)
			if err != nil {
				return nil, 0, err
			}
			// "set index, drop above": enforce a contiguous, hole-free progression.
			if e.Index == 0 || e.Index > uint64(len(rec.Entries))+1 {
				return nil, 0, fmt.Errorf("%w: entry index %d creates a gap (have %d)", ErrCorrupt, e.Index, len(rec.Entries))
			}
			rec.Entries = rec.Entries[:e.Index-1]
			rec.Entries = append(rec.Entries, e)
		case kindHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return nil, 0, err
			}
			rec.HardState = hs
		default:
			return nil, 0, fmt.Errorf("%w: unknown record kind %d", ErrCorrupt, kind)
		}
	}
	// Clamp the persisted commit to what the log actually holds (it is an
	// optimization; correctness never trusts it beyond this — docs/DESIGN.md §8.1).
	if rec.HardState.Commit > uint64(len(rec.Entries)) {
		rec.HardState.Commit = uint64(len(rec.Entries))
	}
	return rec, rd.NextOffset(), nil
}

// Inspect reopens a durable log read-only and returns its recovered state WITHOUT
// truncating or otherwise modifying the file. It is for tests and tooling that
// need to read persisted Entries/HardState (e.g. after a process was killed),
// including while another process holds the file open. It applies the same crash
// policy as Open (a torn tail stops the read; other damage is fatal).
func Inspect(path string) (*Recovered, error) { return InspectFS(nil, path) }

// InspectFS is Inspect on an explicit filesystem (nil means the real OS).
func InspectFS(fsys vfs.FS, path string) (*Recovered, error) {
	f, err := vfs.Or(fsys).OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	rec, _, err := readRecords(f, path, info.Size())
	return rec, err
}

// Save durably records a Ready's HardState (if any) and entries in one fsync. On a
// suffix replacement the entries simply extend the file; replay reconstructs the
// truncation.
//
// The record order is chosen so that a crash between ANY two records of a Save
// leaves a log recovery accepts without repair (Phase 11, docs/CRASH_RECOVERY.md
// §5; found by the crash matrix). Two constraints pull in opposite directions:
// the current term must never be below the term of an entry in the log (a
// leader of that term existed), so a term change must precede entries of the new
// term; and a commit index must never be durable before the entries it covers —
// on a suffix replacement it would otherwise cover the OLD, conflicting entries
// still on disk. So SavePlan writes a changed term/vote FIRST, carrying the
// previously durable commit, then the entries, then the HardState with the new
// commit if it changed. A Save with no entries is one HardState record; one
// with an unchanged term and vote is the entries then the HardState.
//
// If any write or the fsync fails, the Log is failed from then on: this and every
// later Save return an error wrapping ErrFailed and the original cause, and no
// further byte is written (INV-F1).
func (l *Log) Save(hs *HardState, entries []Entry) error {
	if l.failed != nil {
		return l.failed
	}
	lead, trail := SavePlan(l.hs, hs, entries)
	if lead != nil {
		if _, err := l.w.Append(kindHardState, encodeHardState(*lead)); err != nil {
			return l.fail(err)
		}
	}
	for _, e := range entries {
		if _, err := l.w.Append(kindEntry, encodeEntry(e)); err != nil {
			return l.fail(err)
		}
	}
	if trail != nil {
		if _, err := l.w.Append(kindHardState, encodeHardState(*trail)); err != nil {
			return l.fail(err)
		}
	}
	if l.sync {
		if err := l.f.Sync(); err != nil {
			return l.fail(err)
		}
	}
	if hs != nil {
		l.hs = *hs
	}
	return nil
}

// SavePlan is the record order of a Save, as a pure function: given the last
// durably written HardState prev, it returns the HardState record written BEFORE
// the entries (lead) and the one written AFTER them (trail); either may be nil.
// The records of the Save are therefore: lead?, entries..., trail?. It is
// exported so the deterministic simulator's model of what a crash may leave on
// disk (INV-F2) is derived from the same rule the log writes by, never a copy.
func SavePlan(prev HardState, hs *HardState, entries []Entry) (lead, trail *HardState) {
	if hs == nil {
		return nil, nil
	}
	if len(entries) > 0 && (hs.Term != prev.Term || hs.Vote != prev.Vote) {
		lead = &HardState{Term: hs.Term, Vote: hs.Vote, Commit: prev.Commit}
		if lead.Commit == hs.Commit {
			return lead, nil // the leading record is already the final state
		}
	}
	trail = &HardState{Term: hs.Term, Vote: hs.Vote, Commit: hs.Commit}
	return lead, trail
}

// fail latches the first durability failure and returns it.
func (l *Log) fail(err error) error {
	l.failed = fmt.Errorf("%w: %w", ErrFailed, err)
	return l.failed
}

// Err returns the latched durability failure, or nil if the Log is healthy.
func (l *Log) Err() error { return l.failed }

// Close closes the file. A healthy Log is fsynced first; a failed one is closed
// without another fsync, which could not vouch for the data anyway.
func (l *Log) Close() error {
	if l.f == nil {
		return nil
	}
	var err error
	if l.failed == nil {
		err = l.f.Sync()
	}
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
