// Package raftlog is the durable Raft log for one group (Phase 9, docs/RAFT.md
// §10, ADR-016). It is an append-only stream of records in the shared §2 framing
// (internal/record): Entry records, HardState records and — since Phase 14 —
// Boundary records.
//
// A conflicting-suffix replacement is APPENDED, not rewritten: recovery replays
// records in file order and applies each Entry at index i as "set index i, drop
// anything above i" — the Phase 8 TruncateAndAppend semantics per record — so the
// final in-memory log is reconstructed from an append-only file and the last
// HardState wins. commitIndex is persisted in HardState as an optimization only.
// Within one Save the records are ordered so that a crash between any two of
// them leaves a log recovery accepts (SavePlan, Phase 11).
//
// Compaction (Phase 14, docs/SNAPSHOTS.md): a Boundary record (index, term) says
// a durable snapshot covers the log through index, whose term was term. Replay
// applies it by Raft's install rule (§7): if the log holds index with that term,
// the entries after it are kept, otherwise every entry is discarded; the entries
// through index are dropped either way, and the persisted commit is raised to
// index. Install appends one (a follower installing a leader's snapshot).
// Compact rewrites the file instead — the boundary record, the HardState, the
// entries after the boundary — into a temporary file that is fsynced and renamed
// over the log, then the directory is fsynced: the log file stays bounded, and a
// crash at any point leaves either the whole old file or the whole new one.
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
	kindBoundary  record.Kind = 3 // Phase 14
	// kindEntryTyped is an entry with a type (Phase 15): a configuration entry.
	// A kind-1 record is an EntryNormal — every entry before Phase 15 — so
	// normal entries keep their encoding; only typed ones use this kind.
	kindEntryTyped record.Kind = 4

	// MaxEntryDataLen bounds one entry's opaque bytes on disk: the system's one
	// entry-size limit, replication.MaxEntryDataLen. Save refuses to write a
	// larger entry and replay refuses to read one — the same bound on both
	// sides, so the log never persists what its own recovery would refuse — and
	// a corrupt length cannot drive a wild allocation.
	MaxEntryDataLen = replication.MaxEntryDataLen
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

// Boundary is the log's compaction boundary (Phase 14): the index and term of
// the last entry a durable snapshot covers and the log no longer holds. The zero
// Boundary is an uncompacted log.
type Boundary struct {
	Index, Term uint64
}

// Recovered is the state reconstructed from the durable log on Open. Entries
// are the entries after the boundary: Entries[i].Index == Boundary.Index+1+i.
// HardState.Commit is clamped to [Boundary.Index, the last entry].
type Recovered struct {
	Boundary  Boundary
	Entries   []Entry
	HardState HardState
}

// LastIndex is the index of the last recovered entry (the boundary if none).
func (r *Recovered) LastIndex() uint64 { return r.Boundary.Index + uint64(len(r.Entries)) }

// ErrCorrupt means the log holds damage a crash cannot explain; opening is
// refused. It wraps record.ErrCorrupt where the framing detected it.
var ErrCorrupt = errors.New("raftlog: corrupt log")

// ErrFailed means an earlier write or fsync failed and the Log has refused all
// writes since (see the package's failure policy). The error returned by Save
// wraps both ErrFailed and the original failure.
var ErrFailed = errors.New("raftlog: log failed; no further writes")

// ErrBoundary is a Compact or Install the log refuses because it would record a
// boundary the log's state contradicts (Phase 14): below the current boundary,
// past the durable commit, at an entry the log does not hold, or at a term above
// the durable term. Nothing is written, and the Log is not failed.
var ErrBoundary = errors.New("raftlog: invalid compaction boundary")

// Log is an append-only durable Raft log. It is not safe for concurrent use — the
// driver's single goroutine owns it.
type Log struct {
	path     string
	fs       vfs.FS
	f        vfs.File
	w        *record.Writer
	sync     bool
	failed   error     // the first write/fsync failure; sticky
	hs       HardState // the last HardState durably written (recovered at Open)
	boundary Boundary  // the durable boundary (recovered at Open)
	size     int64     // the file's length: the end of its last whole record
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
// truncates a torn final record and refuses to open on any other damage.
//
// Before returning, Open fsyncs the file: the state it recovered is durable
// before the caller can act on it. That matters after a failed fsync. The
// records a failed Save wrote may still be in the page cache, so a restarted
// process reads them back — e.g. a vote — and would otherwise act on state that a
// power loss could still erase (found by the Phase 10 simulator; docs/FAULTS.md).
// This assumes a successful fsync is honest; a kernel that silently drops pages
// after a write-back error ("fsyncgate") is outside what this can defend against.
//
// For the same reason Open fsyncs the parent directory — always, not only when
// it creates the file. A compaction renames a new file over the log; a process
// crash after the rename but before its directory fsync leaves the new file
// visible yet not durably named, and appends to it would be lost with the name
// on a power loss (found by TestCompactIsAtomicUnderEveryCrash, Phase 14). The
// directory fsync also covers anything else renamed in the same directory, such
// as a published snapshot (internal/snapshot keeps its files next to the log).
//
// A temporary file left by a compaction that crashed before its rename is
// removed: the log it would have replaced is intact.
func Open(path string, opts Options) (*Log, *Recovered, error) {
	fsys := vfs.Or(opts.FS)
	if _, err := fsys.Stat(TmpPath(path)); err == nil {
		if err := fsys.Remove(TmpPath(path)); err != nil {
			return nil, nil, err
		}
	}
	f, err := fsys.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if err := fsys.SyncDir(filepath.Dir(path)); err != nil {
		_ = f.Close()
		return nil, nil, err
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
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	l := &Log{path: path, fs: fsys, f: f, w: record.NewWriter(f), sync: opts.Sync, hs: rec.HardState, boundary: rec.Boundary, size: end}
	return l, rec, nil
}

// TmpPath is where Compact writes the rewritten log before renaming it over the
// log at path.
func TmpPath(path string) string { return path + ".tmp" }

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
		case kindEntry, kindEntryTyped:
			var e Entry
			var err error
			if kind == kindEntry {
				e, err = decodeEntry(payload)
			} else {
				e, err = decodeTypedEntry(payload)
			}
			if err != nil {
				return nil, 0, err
			}
			// "set index, drop above": enforce a contiguous, hole-free progression
			// after the boundary. Nothing at or below the boundary is ever written
			// after it: those entries are committed and covered by a snapshot.
			base := rec.Boundary.Index
			if e.Index <= base || e.Index > rec.LastIndex()+1 {
				return nil, 0, fmt.Errorf("%w: entry index %d creates a gap (boundary %d, last %d)", ErrCorrupt, e.Index, base, rec.LastIndex())
			}
			rec.Entries = rec.Entries[:e.Index-base-1]
			rec.Entries = append(rec.Entries, e)
		case kindHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return nil, 0, err
			}
			rec.HardState = hs
		case kindBoundary:
			b, err := decodeBoundary(payload)
			if err != nil {
				return nil, 0, err
			}
			if err := rec.install(b); err != nil {
				return nil, 0, err
			}
		default:
			return nil, 0, fmt.Errorf("%w: unknown record kind %d", ErrCorrupt, kind)
		}
	}
	// Clamp the persisted commit to what the log actually holds (it is an
	// optimization; correctness never trusts it beyond this — docs/DESIGN.md §8.1),
	// and raise it to the boundary: everything a snapshot covers is committed.
	rec.HardState.Commit = min(rec.HardState.Commit, rec.LastIndex())
	rec.HardState.Commit = max(rec.HardState.Commit, rec.Boundary.Index)
	return rec, rd.NextOffset(), nil
}

// install applies a Boundary record: Raft's install rule (§7) on the replayed
// log. A boundary that moves backwards, repeats with another term, or would
// replace an entry the persisted commit covers is corruption: no sequence of
// Install and Compact calls writes one.
func (rec *Recovered) install(b Boundary) error {
	cur := rec.Boundary
	switch {
	case b.Index < cur.Index:
		return fmt.Errorf("%w: boundary moves back from %d to %d", ErrCorrupt, cur.Index, b.Index)
	case b.Index == cur.Index && b.Term != cur.Term:
		return fmt.Errorf("%w: boundary %d repeated with term %d (was %d)", ErrCorrupt, b.Index, b.Term, cur.Term)
	case b.Index == cur.Index:
		return nil
	}
	if b.Index <= rec.LastIndex() {
		if rec.Entries[b.Index-cur.Index-1].Term == b.Term {
			rec.Entries = append([]Entry(nil), rec.Entries[b.Index-cur.Index:]...)
			rec.Boundary = b
			return nil
		}
		if min(rec.HardState.Commit, rec.LastIndex()) >= b.Index {
			return fmt.Errorf("%w: boundary (%d, term %d) replaces a committed entry of term %d", ErrCorrupt, b.Index, b.Term, rec.Entries[b.Index-cur.Index-1].Term)
		}
	}
	rec.Entries = nil
	rec.Boundary = b
	return nil
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
//
// An entry longer than MaxEntryDataLen — which replay would refuse to read — is
// never written: Save checks every entry before writing any byte, and refuses
// the whole Save with an error wrapping replication.ErrEntryTooLarge. That fails
// the Log too, because its caller already holds the entry and cannot go on as
// if it were durable; but nothing reached the file, so the log on disk stays
// exactly what it was and the node restarts from it.
func (l *Log) Save(hs *HardState, entries []Entry) error {
	if l.failed != nil {
		return l.failed
	}
	for _, e := range entries {
		if len(e.Data) > MaxEntryDataLen {
			return l.fail(fmt.Errorf("%w: entry %d of %d bytes, the limit is %d", replication.ErrEntryTooLarge, e.Index, len(e.Data), MaxEntryDataLen))
		}
	}
	lead, trail := SavePlan(l.hs, hs, entries)
	if lead != nil {
		if err := l.append(kindHardState, encodeHardState(*lead)); err != nil {
			return l.fail(err)
		}
	}
	for _, e := range entries {
		if err := l.append(entryKind(e), encodeAnyEntry(e)); err != nil {
			return l.fail(err)
		}
	}
	if trail != nil {
		if err := l.append(kindHardState, encodeHardState(*trail)); err != nil {
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

// HardState returns the last HardState durably written (or recovered).
func (l *Log) HardState() HardState { return l.hs }

// Boundary returns the log's durable compaction boundary.
func (l *Log) Boundary() Boundary { return l.boundary }

// Install durably records that a snapshot at (index, term) has been installed
// (Phase 14, Raft §7; docs/SNAPSHOTS.md §8): it appends one Boundary record and
// fsyncs. On the next Open the log keeps its entries after index if it holds
// index with that term, and discards them otherwise — exactly what the core did
// in memory. The snapshot must already be published (durable), and its term
// already durable in the HardState: a log whose last term exceeds the durable
// currentTerm is one the core refuses. So Install refuses (ErrBoundary, nothing
// written) a term above the durable term, an index at or below the durable
// boundary, and an index at or below the durable commit — the core never
// installs a snapshot its commit already covers.
func (l *Log) Install(index, term uint64) error {
	if l.failed != nil {
		return l.failed
	}
	switch {
	case index == 0 || term == 0:
		return fmt.Errorf("%w: install at index %d term %d", ErrBoundary, index, term)
	case term > l.hs.Term:
		return fmt.Errorf("%w: snapshot term %d above the durable term %d", ErrBoundary, term, l.hs.Term)
	case index <= l.boundary.Index:
		return fmt.Errorf("%w: install at %d, boundary already %d", ErrBoundary, index, l.boundary.Index)
	case index <= l.hs.Commit:
		return fmt.Errorf("%w: install at %d, durable commit already %d", ErrBoundary, index, l.hs.Commit)
	}
	if err := l.append(kindBoundary, encodeBoundary(Boundary{index, term})); err != nil {
		return l.fail(err)
	}
	if l.sync {
		if err := l.f.Sync(); err != nil {
			return l.fail(err)
		}
	}
	l.boundary = Boundary{index, term}
	return nil
}

// Compact durably discards the entries through index, which a published
// snapshot now covers (Phase 14, docs/SNAPSHOTS.md §5): it rewrites the log as
// a Boundary record (index, term), the durable HardState and the entries after
// index, into TmpPath; fsyncs it; renames it over the log; fsyncs the
// directory. Until the rename the old file is the log; after it, the new one;
// the directory fsync makes the rename survive a power loss. The rewrite is
// read back from the file itself, so it carries exactly what is durable.
//
// The log must hold index with that term, and index must not exceed the durable
// commit: an uncommitted entry is never compacted. A violation is ErrBoundary
// and writes nothing; compacting to the current boundary is a no-op. Any I/O
// failure fails the Log (INV-F1): after a failed directory fsync the log's own
// name is no longer known to be durable.
func (l *Log) Compact(index, term uint64) error {
	if l.failed != nil {
		return l.failed
	}
	rec, err := l.reread()
	if err != nil {
		return l.fail(err)
	}
	b := rec.Boundary
	switch {
	case index == b.Index && term == b.Term:
		return nil
	case index <= b.Index:
		return fmt.Errorf("%w: compact to %d, boundary already %d", ErrBoundary, index, b.Index)
	case index > rec.LastIndex():
		return fmt.Errorf("%w: compact to %d, log ends at %d", ErrBoundary, index, rec.LastIndex())
	case rec.Entries[index-b.Index-1].Term != term:
		return fmt.Errorf("%w: compact to %d at term %d, the log holds term %d", ErrBoundary, index, term, rec.Entries[index-b.Index-1].Term)
	case index > rec.HardState.Commit:
		return fmt.Errorf("%w: compact to %d, durable commit is %d", ErrBoundary, index, rec.HardState.Commit)
	}
	tmp := TmpPath(l.path)
	f, err := l.fs.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return l.fail(err)
	}
	w := record.NewWriter(f)
	var size int64
	write := func(k record.Kind, p []byte) error {
		n, err := w.Append(k, p)
		size += int64(n)
		return err
	}
	err = write(kindBoundary, encodeBoundary(Boundary{index, term}))
	if err == nil {
		err = write(kindHardState, encodeHardState(rec.HardState))
	}
	for _, e := range rec.Entries[index-b.Index:] {
		if err == nil {
			err = write(entryKind(e), encodeAnyEntry(e))
		}
	}
	if err == nil && l.sync {
		err = f.Sync()
	}
	if err == nil {
		err = l.fs.Rename(tmp, l.path)
	}
	if err == nil && l.sync {
		err = l.fs.SyncDir(filepath.Dir(l.path))
	}
	if err != nil {
		_ = f.Close()
		return l.fail(err)
	}
	_ = l.f.Close()
	l.f, l.w, l.boundary, l.size = f, w, Boundary{index, term}, size
	return nil
}

// append writes one record and accounts for its bytes.
func (l *Log) append(kind record.Kind, payload []byte) error {
	n, err := l.w.Append(kind, payload)
	l.size += int64(n)
	return err
}

// Size is the length of the log's file in bytes: what Open recovered, plus
// every record appended since, or what a compaction rewrote it to (Phase 16,
// docs/OBSERVABILITY.md). It is the log's growth between compactions.
func (l *Log) Size() int64 { return l.size }

// reread replays the log's own file (every record in it is whole: Open
// truncated any torn tail and every append since succeeded) and leaves the file
// positioned at its end for the next append.
func (l *Log) reread() (*Recovered, error) {
	info, err := l.f.Stat()
	if err != nil {
		return nil, err
	}
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	rec, _, err := readRecords(l.f, l.path, info.Size())
	if _, serr := l.f.Seek(0, io.SeekEnd); err == nil {
		err = serr
	}
	return rec, err
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

// entryKind is the record kind an entry is written as: the Phase 9 kind for a
// normal entry, the typed kind for any other.
func entryKind(e Entry) record.Kind {
	if e.Type == replication.EntryNormal {
		return kindEntry
	}
	return kindEntryTyped
}

// encodeAnyEntry encodes an entry in the payload of its record kind.
func encodeAnyEntry(e Entry) []byte {
	if e.Type == replication.EntryNormal {
		return encodeEntry(e)
	}
	return encodeTypedEntry(e)
}

// encodeTypedEntry is a typed entry's payload: type, then the entry as
// encodeEntry writes it.
func encodeTypedEntry(e Entry) []byte {
	return append([]byte{byte(e.Type)}, encodeEntry(e)...)
}

// decodeTypedEntry decodes a typed entry. A configuration entry must carry a
// configuration that decodes — the core relies on it — and an unknown type is
// corruption; recovery refuses either rather than guessing.
func decodeTypedEntry(p []byte) (Entry, error) {
	if len(p) < 1 {
		return Entry{}, fmt.Errorf("%w: empty typed entry", ErrCorrupt)
	}
	e, err := decodeEntry(p[1:])
	if err != nil {
		return Entry{}, err
	}
	e.Type = replication.EntryType(p[0])
	switch e.Type {
	case replication.EntryNormal:
		return Entry{}, fmt.Errorf("%w: a normal entry written as a typed record", ErrCorrupt)
	case replication.EntryConfig:
		if _, err := replication.DecodeConfigurationEntry(e.Data); err != nil {
			return Entry{}, fmt.Errorf("%w: configuration entry %d: %v", ErrCorrupt, e.Index, err)
		}
	default:
		return Entry{}, fmt.Errorf("%w: entry %d has unknown type %d", ErrCorrupt, e.Index, e.Type)
	}
	return e, nil
}

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

func encodeBoundary(b Boundary) []byte {
	buf := appendUvarint(nil, b.Index)
	return appendUvarint(buf, b.Term)
}

func decodeBoundary(p []byte) (Boundary, error) {
	r := payloadReader{b: p}
	var b Boundary
	var err error
	if b.Index, err = r.uvarint(); err != nil {
		return Boundary{}, err
	}
	if b.Term, err = r.uvarint(); err != nil {
		return Boundary{}, err
	}
	if err := r.done(); err != nil {
		return Boundary{}, err
	}
	if b.Index == 0 || b.Term == 0 {
		return Boundary{}, fmt.Errorf("%w: boundary at index %d term %d", ErrCorrupt, b.Index, b.Term)
	}
	return b, nil
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
