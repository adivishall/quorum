// Package manifest implements Quorum's MANIFEST — the authoritative record of
// which SSTables constitute the database (docs/DESIGN.md §6, docs/MANIFEST.md).
//
// # What problem this solves
//
// Phase 3 inferred the live file set by listing the directory and reading every
// *.sst it found. That works only while nothing ever removes a file: the moment
// compaction can delete a table and publish a replacement, "what is on disk" and
// "what the database consists of" stop being the same question. A crash midway
// through a compaction leaves both the inputs and the output on disk, and
// directory listing cannot tell which set is the database — it has to guess, and
// either guess loses data or resurrects it.
//
// The MANIFEST removes the guess. It is an append-only log of version edits; the
// live file set is whatever replaying it produces. A file on disk that the
// MANIFEST does not name is not part of the database, however complete it looks.
//
// # Layout
//
//	<data-dir>/CURRENT           the name of the live manifest, one line
//	<data-dir>/MANIFEST-000001   an append-only record stream of version edits
//
// CURRENT is replaced atomically: write CURRENT.tmp, fsync it, rename over
// CURRENT, fsync the directory. rename(2) is atomic on POSIX, so a reader sees
// either the old name or the new one and never a partial one.
//
// # One record is one edit
//
// docs/DESIGN.md §6 lists AddFile, DeleteFile and the Set* operations as record
// kinds, and describes the compaction commit as appending "one manifest record
// group". A group of separate records is not atomic: the framing in
// docs/DESIGN.md §2 has no grouping primitive, so a crash between the AddFile
// and the DeleteFile of one compaction would leave a manifest in which the new
// file is live and the old ones are live too — a state no version of the
// database was ever in.
//
// So an edit is exactly one framed record, and its CRC covers the whole edit.
// Either the entire version change replays or none of it does. This is the same
// reasoning, and the same solution, as the WAL's write batch (docs/WAL.md §2).
// DESIGN's operation numbers are preserved as field tags inside the edit
// payload. See ADR-010.
//
// # Recovery policy
//
// Identical to the WAL's, because the failure is identical (docs/WAL.md §7,
// INV-S8):
//
//   - A torn or bad-checksum record at the very end of the manifest is a crash
//     during the append of that edit. The edit was never fsynced, so it never
//     took effect, and nothing that depended on it was acknowledged. Truncate to
//     the last good boundary and continue.
//   - Damage anywhere else is not explainable by a crash. Refuse to open. The
//     alternative — skipping an edit in the middle — would silently apply a
//     version history that never happened.
package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/adivishall/quorum/internal/record"
)

// File names.
const (
	// CurrentName is the file naming the live manifest.
	CurrentName = "CURRENT"
	// currentTempName is the staging name CURRENT is renamed from.
	currentTempName = "CURRENT.tmp"
	// manifestPrefix and manifestDigits give MANIFEST-%06d.
	manifestPrefix = "MANIFEST-"
	manifestDigits = 6
)

// KindVersionEdit is the only record kind a manifest contains.
//
// The namespace is local to this file type, as docs/DESIGN.md §2 specifies: 0x01
// means WriteBatch in a WAL and a version edit here.
const KindVersionEdit record.Kind = 0x01

// Field tags inside an edit payload. The numbers are docs/DESIGN.md §6's
// operation numbers, preserved deliberately even though they are now tags within
// a record rather than record kinds of their own.
const (
	tagAddFile        = 0x01
	tagDeleteFile     = 0x02
	tagNextFileNum    = 0x03
	tagLastSequence   = 0x04
	tagLogNumber      = 0x05
	tagApplied        = 0x06
	tagMaxKnown       = tagApplied
	maxFilesPerEdit   = 1 << 20 // bounds an allocation driven by a decoded count
	maxKeyLenInEdit   = 1 << 20 // a key is at most 4 KiB; this is slack, not a limit
	maxLevelsPerEdit  = 64      // a level number this large means damage
	maxManifestNumber = 999999
)

// Errors. Callers must branch with errors.Is.
var (
	// ErrCorrupt means the manifest could not be trusted. It is deliberately
	// the same value as record.ErrCorrupt so that one sentinel covers damage
	// wherever it is found.
	ErrCorrupt = record.ErrCorrupt

	// ErrNoManifest means the data directory has no CURRENT file.
	//
	// For an empty directory that is simply a database that does not exist yet.
	// For a directory that contains SSTables it is a much more serious
	// condition, and distinguishing the two is the caller's job, not this
	// package's — see storage.OpenLSMStore.
	ErrNoManifest = errors.New("manifest: no CURRENT file")
)

// FileMeta describes one SSTable, and is exactly what docs/DESIGN.md §6's
// AddFile carries.
//
// The sequence range is the field that earns the MANIFEST its keep on the
// startup path: Phase 3 had to read every byte of every SSTable to recover it,
// because there was nowhere on disk to write it down.
type FileMeta struct {
	Level       int
	Num         uint64
	Size        int64
	NumEntries  uint64
	SmallestKey []byte // internal key
	LargestKey  []byte // internal key
	SmallestSeq uint64
	LargestSeq  uint64
}

// Clone returns a deep copy, so that a State handed to a caller cannot be
// mutated through its key slices.
func (f FileMeta) Clone() FileMeta {
	g := f
	g.SmallestKey = append([]byte(nil), f.SmallestKey...)
	g.LargestKey = append([]byte(nil), f.LargestKey...)
	return g
}

// DeletedFile identifies a file being retired.
type DeletedFile struct {
	Level int
	Num   uint64
}

// Applied is durable Raft progress metadata, carried here for the same reason
// the WAL carries it: so the on-disk format does not change in Phase 9.
type Applied struct {
	Index uint64
	Term  uint64
}

// Edit is one atomic change to the file set. It is encoded as exactly one record.
type Edit struct {
	Added   []FileMeta
	Deleted []DeletedFile

	HasNextFileNum bool
	NextFileNum    uint64

	HasLastSequence bool
	LastSequence    uint64

	HasLogNumber bool
	LogNumber    uint64

	HasApplied bool
	Applied    Applied
}

// Empty reports whether the edit would change nothing.
func (e *Edit) Empty() bool {
	return len(e.Added) == 0 && len(e.Deleted) == 0 &&
		!e.HasNextFileNum && !e.HasLastSequence && !e.HasLogNumber && !e.HasApplied
}

// AddFile records a new live file.
func (e *Edit) AddFile(f FileMeta) { e.Added = append(e.Added, f) }

// DeleteFile records a file being retired.
func (e *Edit) DeleteFile(level int, num uint64) {
	e.Deleted = append(e.Deleted, DeletedFile{Level: level, Num: num})
}

// SetNextFileNum records the next unused file number.
func (e *Edit) SetNextFileNum(n uint64) { e.HasNextFileNum, e.NextFileNum = true, n }

// SetLastSequence records the highest assigned sequence number.
func (e *Edit) SetLastSequence(n uint64) { e.HasLastSequence, e.LastSequence = true, n }

// SetLogNumber records the oldest WAL segment the database still needs.
//
// Phase 4 writes it so that the field exists and round-trips, and does not act
// on it: retiring WAL segments is a later concern, and doing it here would mean
// deleting a log on the strength of a field nothing yet tests. See
// docs/LIMITATIONS.md.
func (e *Edit) SetLogNumber(n uint64) { e.HasLogNumber, e.LogNumber = true, n }

// SetApplied records Raft progress metadata.
func (e *Edit) SetApplied(a Applied) { e.HasApplied, e.Applied = true, a }

// AppendTo encodes the edit onto dst.
//
// Integers are uvarints and byte strings are uvarint-length-prefixed, matching
// the WAL's payload convention (docs/DESIGN.md §3, "all lengths are uvarint")
// rather than introducing a second style in the same codebase.
func (e *Edit) AppendTo(dst []byte) []byte {
	for _, f := range e.Added {
		dst = binary.AppendUvarint(dst, tagAddFile)
		dst = binary.AppendUvarint(dst, uint64(f.Level))
		dst = binary.AppendUvarint(dst, f.Num)
		dst = binary.AppendUvarint(dst, uint64(f.Size))
		dst = binary.AppendUvarint(dst, f.NumEntries)
		dst = appendBytes(dst, f.SmallestKey)
		dst = appendBytes(dst, f.LargestKey)
		dst = binary.AppendUvarint(dst, f.SmallestSeq)
		dst = binary.AppendUvarint(dst, f.LargestSeq)
	}
	for _, d := range e.Deleted {
		dst = binary.AppendUvarint(dst, tagDeleteFile)
		dst = binary.AppendUvarint(dst, uint64(d.Level))
		dst = binary.AppendUvarint(dst, d.Num)
	}
	if e.HasNextFileNum {
		dst = binary.AppendUvarint(dst, tagNextFileNum)
		dst = binary.AppendUvarint(dst, e.NextFileNum)
	}
	if e.HasLastSequence {
		dst = binary.AppendUvarint(dst, tagLastSequence)
		dst = binary.AppendUvarint(dst, e.LastSequence)
	}
	if e.HasLogNumber {
		dst = binary.AppendUvarint(dst, tagLogNumber)
		dst = binary.AppendUvarint(dst, e.LogNumber)
	}
	if e.HasApplied {
		dst = binary.AppendUvarint(dst, tagApplied)
		dst = binary.AppendUvarint(dst, e.Applied.Index)
		dst = binary.AppendUvarint(dst, e.Applied.Term)
	}
	return dst
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// DecodeEdit parses an edit payload.
//
// Strict throughout, for the reason wal.DecodeBatch is strict: a lenient decoder
// in a log that defines what the database consists of turns damage into a
// plausible-looking file set, and the operator sees a database that started
// cleanly and is quietly missing data. An unknown tag is an error, not a field
// to skip — a manifest written by a future version is not something this version
// can safely half-understand.
func DecodeEdit(payload []byte) (Edit, error) {
	var (
		e Edit
		p = payload
	)
	for len(p) > 0 {
		tag, rest, err := takeUvarint(p, "field tag")
		if err != nil {
			return Edit{}, err
		}
		p = rest

		switch tag {
		case tagAddFile:
			var f FileMeta
			level, rest, err := takeUvarint(p, "added file level")
			if err != nil {
				return Edit{}, err
			}
			if level > maxLevelsPerEdit {
				return Edit{}, fmt.Errorf("manifest: added file names level %d, above the %d-level maximum: %w",
					level, maxLevelsPerEdit, ErrCorrupt)
			}
			f.Level = int(level)
			if f.Num, rest, err = takeUvarint(rest, "added file number"); err != nil {
				return Edit{}, err
			}
			size, rest2, err := takeUvarint(rest, "added file size")
			if err != nil {
				return Edit{}, err
			}
			if size > 1<<62 {
				return Edit{}, fmt.Errorf("manifest: added file declares an impossible size %d: %w",
					size, ErrCorrupt)
			}
			f.Size = int64(size)
			rest = rest2
			if f.NumEntries, rest, err = takeUvarint(rest, "added file entry count"); err != nil {
				return Edit{}, err
			}
			if f.SmallestKey, rest, err = takeBytes(rest, "added file smallest key"); err != nil {
				return Edit{}, err
			}
			if f.LargestKey, rest, err = takeBytes(rest, "added file largest key"); err != nil {
				return Edit{}, err
			}
			if f.SmallestSeq, rest, err = takeUvarint(rest, "added file smallest sequence"); err != nil {
				return Edit{}, err
			}
			if f.LargestSeq, rest, err = takeUvarint(rest, "added file largest sequence"); err != nil {
				return Edit{}, err
			}
			if f.SmallestSeq > f.LargestSeq {
				return Edit{}, fmt.Errorf(
					"manifest: file %d declares sequence range [%d,%d], which is inverted: %w",
					f.Num, f.SmallestSeq, f.LargestSeq, ErrCorrupt)
			}
			if len(e.Added) >= maxFilesPerEdit {
				return Edit{}, fmt.Errorf("manifest: edit adds more than %d files: %w",
					maxFilesPerEdit, ErrCorrupt)
			}
			// Keys alias the payload buffer, which the record reader reuses.
			f.SmallestKey = append([]byte(nil), f.SmallestKey...)
			f.LargestKey = append([]byte(nil), f.LargestKey...)
			e.Added = append(e.Added, f)
			p = rest

		case tagDeleteFile:
			level, rest, err := takeUvarint(p, "deleted file level")
			if err != nil {
				return Edit{}, err
			}
			if level > maxLevelsPerEdit {
				return Edit{}, fmt.Errorf("manifest: deleted file names level %d: %w", level, ErrCorrupt)
			}
			num, rest, err := takeUvarint(rest, "deleted file number")
			if err != nil {
				return Edit{}, err
			}
			if len(e.Deleted) >= maxFilesPerEdit {
				return Edit{}, fmt.Errorf("manifest: edit deletes more than %d files: %w",
					maxFilesPerEdit, ErrCorrupt)
			}
			e.Deleted = append(e.Deleted, DeletedFile{Level: int(level), Num: num})
			p = rest

		case tagNextFileNum:
			v, rest, err := takeUvarint(p, "next file number")
			if err != nil {
				return Edit{}, err
			}
			e.SetNextFileNum(v)
			p = rest

		case tagLastSequence:
			v, rest, err := takeUvarint(p, "last sequence")
			if err != nil {
				return Edit{}, err
			}
			e.SetLastSequence(v)
			p = rest

		case tagLogNumber:
			v, rest, err := takeUvarint(p, "log number")
			if err != nil {
				return Edit{}, err
			}
			e.SetLogNumber(v)
			p = rest

		case tagApplied:
			idx, rest, err := takeUvarint(p, "applied index")
			if err != nil {
				return Edit{}, err
			}
			term, rest, err := takeUvarint(rest, "applied term")
			if err != nil {
				return Edit{}, err
			}
			e.SetApplied(Applied{Index: idx, Term: term})
			p = rest

		default:
			return Edit{}, fmt.Errorf(
				"manifest: unknown field tag %#x (this version understands tags up to %#x): %w",
				tag, tagMaxKnown, ErrCorrupt)
		}
	}
	return e, nil
}

func takeUvarint(p []byte, what string) (uint64, []byte, error) {
	v, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, nil, fmt.Errorf("manifest: unreadable %s: %w", what, ErrCorrupt)
	}
	return v, p[n:], nil
}

func takeBytes(p []byte, what string) ([]byte, []byte, error) {
	n, read := binary.Uvarint(p)
	if read <= 0 {
		return nil, nil, fmt.Errorf("manifest: unreadable %s length: %w", what, ErrCorrupt)
	}
	p = p[read:]
	if n > maxKeyLenInEdit {
		return nil, nil, fmt.Errorf("manifest: %s declares %d bytes, above the %d-byte maximum: %w",
			what, n, maxKeyLenInEdit, ErrCorrupt)
	}
	if n > uint64(len(p)) {
		return nil, nil, fmt.Errorf("manifest: %s declares %d bytes but only %d remain: %w",
			what, n, len(p), ErrCorrupt)
	}
	return p[:n], p[n:], nil
}

// State is the database version: the live file set plus the scalars that
// describe it.
type State struct {
	Files        []FileMeta // live files; ordering is not significant here
	NextFileNum  uint64
	LastSequence uint64
	LogNumber    uint64
	Applied      Applied
}

// Clone returns a deep copy.
func (s State) Clone() State {
	out := s
	out.Files = make([]FileMeta, 0, len(s.Files))
	for _, f := range s.Files {
		out.Files = append(out.Files, f.Clone())
	}
	return out
}

// Apply folds one edit into the state.
//
// Deletions are applied before additions, so an edit may retire a file number
// and reuse it, and every inconsistency is an error rather than something to
// work around: deleting a file the state does not hold, or adding one it already
// does, means the manifest does not describe a history this engine could have
// produced.
func (s *State) Apply(e Edit) error {
	for _, d := range e.Deleted {
		idx := -1
		for i := range s.Files {
			if s.Files[i].Num == d.Num {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("manifest: edit deletes file %06d, which is not live: %w", d.Num, ErrCorrupt)
		}
		s.Files = append(s.Files[:idx], s.Files[idx+1:]...)
	}
	for _, f := range e.Added {
		for i := range s.Files {
			if s.Files[i].Num == f.Num {
				return fmt.Errorf("manifest: edit adds file %06d, which is already live: %w",
					f.Num, ErrCorrupt)
			}
		}
		s.Files = append(s.Files, f.Clone())
	}
	if e.HasNextFileNum {
		s.NextFileNum = e.NextFileNum
	}
	if e.HasLastSequence {
		s.LastSequence = e.LastSequence
	}
	if e.HasLogNumber {
		s.LogNumber = e.LogNumber
	}
	if e.HasApplied {
		s.Applied = e.Applied
	}
	return nil
}

// Snapshot returns a single edit that reproduces this state from nothing.
//
// It is what gets written as the first record of a freshly installed manifest,
// so that recovery never has to replay a history longer than the current state.
func (s State) Snapshot() Edit {
	var e Edit
	files := append([]FileMeta(nil), s.Files...)
	// Deterministic order, so two installs of the same state produce identical
	// bytes and a manifest diff means a real change.
	sort.Slice(files, func(i, j int) bool { return files[i].Num < files[j].Num })
	for _, f := range files {
		e.AddFile(f)
	}
	e.SetNextFileNum(s.NextFileNum)
	e.SetLastSequence(s.LastSequence)
	e.SetLogNumber(s.LogNumber)
	e.SetApplied(s.Applied)
	return e
}

// ---------------------------------------------------------------- file naming

// Name returns the manifest file name for a number.
func Name(n uint64) string {
	return fmt.Sprintf("%s%0*d", manifestPrefix, manifestDigits, n)
}

// ParseName parses a manifest file name. The match is exact, so a stray file
// cannot be mistaken for a manifest.
func ParseName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, manifestPrefix) {
		return 0, false
	}
	digits := strings.TrimPrefix(name, manifestPrefix)
	if len(digits) != manifestDigits {
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

// List returns the manifest numbers present in dir, ascending.
func List(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("manifest: reading %s: %w", dir, err)
	}
	var out []uint64
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if n, ok := ParseName(ent.Name()); ok {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// ReadCurrent returns the manifest number CURRENT names.
func ReadCurrent(dir string) (uint64, error) {
	raw, err := os.ReadFile(filepath.Join(dir, CurrentName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, ErrNoManifest
		}
		return 0, fmt.Errorf("manifest: reading %s: %w", CurrentName, err)
	}
	name := strings.TrimSpace(string(raw))
	n, ok := ParseName(name)
	if !ok {
		// CURRENT is one short line this package writes. Anything else in it is
		// damage, and guessing which manifest was meant is exactly the kind of
		// reconstruction the MANIFEST exists to make unnecessary.
		return 0, fmt.Errorf("manifest: %s names %q, which is not a manifest file name: %w",
			CurrentName, truncateForMessage(name), ErrCorrupt)
	}
	return n, nil
}

// writeCurrent points CURRENT at a manifest, atomically and durably.
//
// The sequence is docs/DESIGN.md §6's: write the temporary file, fsync it so its
// contents exist before any name refers to them, rename over CURRENT (atomic on
// POSIX), then fsync the directory so the rename itself survives a crash.
func writeCurrent(dir string, num uint64) error {
	tmp := filepath.Join(dir, currentTempName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("manifest: creating %s: %w", currentTempName, err)
	}
	if _, err := f.WriteString(Name(num) + "\n"); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("manifest: writing %s: %w", currentTempName, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("manifest: syncing %s: %w", currentTempName, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("manifest: closing %s: %w", currentTempName, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, CurrentName)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("manifest: installing %s: %w", CurrentName, err)
	}
	return SyncDir(dir)
}

// SyncDir fsyncs a directory, making a rename within it durable.
func SyncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

func truncateForMessage(s string) string {
	if len(s) <= 64 {
		return s
	}
	return s[:64] + "..."
}

// ---------------------------------------------------------------- writer

// Writer appends version edits to the live manifest.
//
// Append fsyncs before returning. That is not tunable: an edit that is not on
// disk has not happened, and the publication protocol in docs/MANIFEST.md
// depends on the fsync having completed before the new file set is treated as
// live.
type Writer struct {
	f      *os.File
	dir    string
	num    uint64
	offset int64
	closed bool
}

// Install writes a brand-new manifest holding a snapshot of state, then points
// CURRENT at it.
//
// This runs on every open, which is what bounds a manifest's length: recovery
// replays one manifest that starts from a full snapshot, rather than every edit
// the database has ever made. A crash partway through leaves the previous
// CURRENT and previous manifest untouched and the new manifest unreferenced, so
// the old version stays authoritative and the new file is an orphan.
func Install(dir string, num uint64, state State) (*Writer, error) {
	if num == 0 || num > maxManifestNumber {
		return nil, fmt.Errorf("manifest: number %d is outside [1,%d]", num, maxManifestNumber)
	}
	path := filepath.Join(dir, Name(num))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("manifest: creating %s: %w", Name(num), err)
	}
	w := &Writer{f: f, dir: dir, num: num}

	snap := state.Snapshot()
	if err := w.Append(&snap); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := writeCurrent(dir, num); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return w, nil
}

// Reopen returns a Writer appending to an existing manifest at the given offset,
// which must be the end of the last good record.
func Reopen(dir string, num uint64, offset int64) (*Writer, error) {
	path := filepath.Join(dir, Name(num))
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("manifest: opening %s: %w", Name(num), err)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("manifest: seeking %s to %d: %w", Name(num), offset, err)
	}
	return &Writer{f: f, dir: dir, num: num, offset: offset}, nil
}

// Num returns the manifest's number.
func (w *Writer) Num() uint64 { return w.num }

// Offset returns the byte offset after the last appended record.
func (w *Writer) Offset() int64 { return w.offset }

// Append writes one edit as one record and fsyncs it.
func (w *Writer) Append(e *Edit) error {
	if w.closed {
		return fmt.Errorf("manifest: writer is closed")
	}
	if e.Empty() {
		return fmt.Errorf("manifest: refusing to append an edit that changes nothing")
	}
	buf, err := record.Encode(nil, KindVersionEdit, e.AppendTo(nil))
	if err != nil {
		return fmt.Errorf("manifest: encoding edit: %w", err)
	}
	n, err := w.f.Write(buf)
	w.offset += int64(n)
	if err != nil {
		return fmt.Errorf("manifest: appending to %s: %w", Name(w.num), err)
	}
	if n != len(buf) {
		return fmt.Errorf("manifest: short write to %s: %w", Name(w.num), io.ErrShortWrite)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("manifest: syncing %s: %w", Name(w.num), err)
	}
	return nil
}

// Close releases the manifest file.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.f.Close()
}

// ---------------------------------------------------------------- recovery

// Recovery reports what reading the manifest found.
type Recovery struct {
	Num          uint64 // the manifest CURRENT named
	EditsApplied int
	BytesRead    int64
	EndOffset    int64 // end of the last good record; the append point

	// Truncated reports a repaired torn tail: a crash during the append of the
	// final edit. That edit never took effect.
	Truncated       bool
	TruncatedAt     int64
	TruncatedBytes  int64
	TruncatedReason string
}

// Recover reads CURRENT, replays the manifest it names, and returns the state.
//
// A torn final record is repaired by truncating the file, so the next append
// does not follow garbage. Damage anywhere else is refused.
func Recover(dir string) (State, Recovery, error) {
	num, err := ReadCurrent(dir)
	if err != nil {
		return State{}, Recovery{}, err
	}
	rec := Recovery{Num: num}

	path := filepath.Join(dir, Name(num))
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// CURRENT names a manifest that is not there. This is the one case
			// the protocol cannot produce: CURRENT is only ever pointed at a
			// manifest that was already fsynced.
			return State{}, rec, fmt.Errorf(
				"manifest: %s names %s, which does not exist: %w",
				CurrentName, Name(num), ErrCorrupt)
		}
		return State{}, rec, fmt.Errorf("manifest: opening %s: %w", Name(num), err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return State{}, rec, fmt.Errorf("manifest: stat %s: %w", Name(num), err)
	}
	size := info.Size()
	rec.BytesRead = size

	var state State
	r := record.NewReader(f, Name(num), size)
	for {
		kind, payload, rerr := r.Next()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			// A torn tail is a crash during the final append. Anything else is
			// damage that a crash cannot explain.
			if errors.Is(rerr, record.ErrTornTail) {
				rec.Truncated = true
				rec.TruncatedAt = r.NextOffset()
				rec.TruncatedBytes = size - r.NextOffset()
				rec.TruncatedReason = rerr.Error()
				break
			}
			return State{}, rec, fmt.Errorf("manifest: %w", rerr)
		}
		if kind != KindVersionEdit {
			return State{}, rec, fmt.Errorf(
				"manifest %s: record at offset %d has unknown kind %#x: %w",
				Name(num), r.Offset(), uint8(kind), ErrCorrupt)
		}
		edit, derr := DecodeEdit(payload)
		if derr != nil {
			return State{}, rec, fmt.Errorf("manifest %s at offset %d: %w",
				Name(num), r.Offset(), derr)
		}
		if aerr := state.Apply(edit); aerr != nil {
			return State{}, rec, fmt.Errorf("manifest %s at offset %d: %w",
				Name(num), r.Offset(), aerr)
		}
		rec.EditsApplied++
	}
	rec.EndOffset = r.NextOffset()

	if rec.EditsApplied == 0 {
		// A manifest always begins with a snapshot edit, written and fsynced by
		// Install before CURRENT could name it. An empty one means the file was
		// replaced or emptied.
		return State{}, rec, fmt.Errorf(
			"manifest %s: holds no usable edits; a manifest always begins with a snapshot: %w",
			Name(num), ErrCorrupt)
	}

	if rec.Truncated {
		if err := truncateAt(path, rec.TruncatedAt); err != nil {
			return State{}, rec, err
		}
	}
	return state, rec, nil
}

func truncateAt(path string, at int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("manifest: opening %s to repair: %w", filepath.Base(path), err)
	}
	if err := f.Truncate(at); err != nil {
		_ = f.Close()
		return fmt.Errorf("manifest: truncating %s to %d: %w", filepath.Base(path), at, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("manifest: syncing %s after repair: %w", filepath.Base(path), err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest: closing %s after repair: %w", filepath.Base(path), err)
	}
	return SyncDir(filepath.Dir(path))
}

// RemoveObsolete deletes every manifest in dir other than keep.
//
// Safe because CURRENT is the only thing that makes a manifest live, and it is
// switched atomically: any other manifest is either a superseded one or one an
// interrupted Install left behind.
func RemoveObsolete(dir string, keep uint64) (int, error) {
	nums, err := List(dir)
	if err != nil {
		return 0, err
	}
	var removed int
	for _, n := range nums {
		if n == keep {
			continue
		}
		if err := os.Remove(filepath.Join(dir, Name(n))); err != nil {
			return removed, fmt.Errorf("manifest: removing obsolete %s: %w", Name(n), err)
		}
		removed++
	}
	// A leftover CURRENT.tmp is an interrupted writeCurrent. It is never read,
	// so removing it cannot lose anything.
	if err := os.Remove(filepath.Join(dir, currentTempName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return removed, fmt.Errorf("manifest: removing %s: %w", currentTempName, err)
	}
	return removed, nil
}
