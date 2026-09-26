// Package snapshot is the durable form of a Raft group's state snapshot (Phase
// 14, docs/SNAPSHOTS.md): its file format, its crash-safe publication, loading
// with full validation, the chunk codec that carries it to a follower
// (transport kind InstallSnapshot), and the receiver that reassembles one.
//
// It makes no Raft decision and knows nothing of the state it carries: the
// state bytes are opaque (the key-value store encodes and validates them), and
// which snapshot to create, offer or install is the core's and the driver's
// business. All file access goes through vfs.FS, so the fault models of
// internal/fault — process crash, power loss, injected I/O errors and crash
// points at every write, fsync and rename — apply to it unchanged.
//
// # Format
//
// A snapshot file is a sequence of records in the shared framing
// (internal/record: CRC-32C per record), in this package's kind namespace:
//
//	Header (kind 1): magic "QSNP" | version | members | index | term | dataLen | SHA-256(data)
//	Data   (kind 2): up to MaxDataRecord bytes of state, in order   (zero or more)
//	Footer (kind 3): index | term                                    (end marker)
//
// Integers are canonical uvarints; the members (the group identity) are
// length-prefixed, non-empty and strictly ascending. A file is valid only if it
// is exactly one header, data records whose concatenation is dataLen bytes with
// the header's SHA-256, and one footer repeating the header's index and term —
// nothing torn, nothing after. There is no repair: a published snapshot was
// fsynced before it was published, so any damage is corruption. The encoding is
// deterministic — no timestamp, random id or host name — so the same state and
// metadata always produce the same bytes.
package snapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/adivishall/quorum/internal/record"
)

// Version is the snapshot format version this package writes and reads.
const Version = 1

const (
	magic = "QSNP"

	kindHeader record.Kind = 1
	kindData   record.Kind = 2
	kindFooter record.Kind = 3

	// MaxDataRecord bounds one data record, keeping records far below the
	// framing's 64 MiB limit and reads incremental.
	MaxDataRecord = 1 << 20
	// MaxData bounds a snapshot's state bytes. The state is held in memory while
	// it is written, sent and received; this is the documented Phase 14 bound
	// (docs/SNAPSHOTS.md), not a claim that larger states are supported.
	MaxData = 512 << 20
	// MaxMembers and MaxMemberLen bound the group identity.
	MaxMembers   = 64
	MaxMemberLen = 256
)

// Errors. ErrCorrupt covers every structural or checksum failure; ErrVersion a
// file of another format version; ErrWrongGroup a valid snapshot of a different
// group.
var (
	ErrCorrupt    = errors.New("snapshot: corrupt")
	ErrVersion    = errors.New("snapshot: unsupported format version")
	ErrWrongGroup = errors.New("snapshot: belongs to a different group")
	ErrTooLarge   = errors.New("snapshot: state exceeds the size bound")
)

// Meta identifies a snapshot: the group it belongs to and the last log entry it
// covers.
type Meta struct {
	Members []string // the group's member ids, strictly ascending
	Index   uint64   // last entry the snapshot covers
	Term    uint64   // that entry's term
}

// Group returns the canonical group identity for a member list: the ids,
// sorted. Two nodes of one group always compute the same identity.
func Group(members []string) []string {
	out := append([]string(nil), members...)
	sort.Strings(out)
	return out
}

// SameGroup reports whether a and b name the same group.
func SameGroup(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (m Meta) validate() error {
	if m.Index == 0 || m.Term == 0 {
		return fmt.Errorf("%w: snapshot at index %d term %d", ErrCorrupt, m.Index, m.Term)
	}
	if len(m.Members) == 0 || len(m.Members) > MaxMembers {
		return fmt.Errorf("%w: %d members", ErrCorrupt, len(m.Members))
	}
	for i, id := range m.Members {
		if id == "" || len(id) > MaxMemberLen {
			return fmt.Errorf("%w: member id of %d bytes", ErrCorrupt, len(id))
		}
		if i > 0 && m.Members[i-1] >= id {
			return fmt.Errorf("%w: members not strictly ascending", ErrCorrupt)
		}
	}
	return nil
}

// Encode returns the complete file for a snapshot of data described by m.
func Encode(m Meta, data []byte) ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	if len(data) > MaxData {
		return nil, fmt.Errorf("%w: %d bytes", ErrTooLarge, len(data))
	}
	sum := sha256.Sum256(data)
	h := []byte(magic)
	h = binary.AppendUvarint(h, Version)
	h = binary.AppendUvarint(h, uint64(len(m.Members)))
	for _, id := range m.Members {
		h = binary.AppendUvarint(h, uint64(len(id)))
		h = append(h, id...)
	}
	h = binary.AppendUvarint(h, m.Index)
	h = binary.AppendUvarint(h, m.Term)
	h = binary.AppendUvarint(h, uint64(len(data)))
	h = append(h, sum[:]...)
	out, err := record.Encode(nil, kindHeader, h)
	if err != nil {
		return nil, err
	}
	for off := 0; off < len(data); off += MaxDataRecord {
		end := min(off+MaxDataRecord, len(data))
		if out, err = record.Encode(out, kindData, data[off:end]); err != nil {
			return nil, err
		}
	}
	f := binary.AppendUvarint(nil, m.Index)
	f = binary.AppendUvarint(f, m.Term)
	return record.Encode(out, kindFooter, f)
}

// Decode validates a complete snapshot file and returns its metadata and state
// bytes. Any deviation from the format — a torn or damaged record, a wrong
// magic, a missing or repeated header or footer, a data length or hash mismatch,
// a footer disagreeing with the header, bytes after the footer, a non-canonical
// integer — is ErrCorrupt; a file of another version is ErrVersion.
func Decode(b []byte) (Meta, []byte, error) {
	rd := record.NewReader(bytes.NewReader(b), "snapshot", int64(len(b)))
	next := func() (record.Kind, []byte, error) {
		k, p, err := rd.Next()
		if err == io.EOF {
			return 0, nil, fmt.Errorf("%w: ends before its footer", ErrCorrupt)
		}
		if err != nil {
			return 0, nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		return k, p, nil
	}
	kind, p, err := next()
	if err != nil {
		return Meta{}, nil, err
	}
	if kind != kindHeader {
		return Meta{}, nil, fmt.Errorf("%w: first record is kind %d, not a header", ErrCorrupt, kind)
	}
	m, dataLen, sum, err := decodeHeader(p)
	if err != nil {
		return Meta{}, nil, err
	}
	data := make([]byte, 0, dataLen)
	for {
		kind, p, err := next()
		if err != nil {
			return Meta{}, nil, err
		}
		switch kind {
		case kindData:
			if len(p) == 0 || len(p) > MaxDataRecord || uint64(len(data)+len(p)) > dataLen {
				return Meta{}, nil, fmt.Errorf("%w: data record of %d bytes (have %d of %d)", ErrCorrupt, len(p), len(data), dataLen)
			}
			data = append(data, p...)
			continue
		case kindFooter:
			d := decoder{b: p}
			idx, term := d.uint(), d.uint()
			if err := d.done(); err != nil {
				return Meta{}, nil, err
			}
			if idx != m.Index || term != m.Term {
				return Meta{}, nil, fmt.Errorf("%w: footer (%d,%d) disagrees with header (%d,%d)", ErrCorrupt, idx, term, m.Index, m.Term)
			}
		default:
			return Meta{}, nil, fmt.Errorf("%w: unexpected record kind %d", ErrCorrupt, kind)
		}
		break
	}
	// Nothing may follow the footer. The framing's reader reports a remainder
	// shorter than a record header as a clean end of file (the logs' policy for
	// a torn header); a snapshot is never torn, so compare offsets instead (found
	// by FuzzDecode: one stray byte after the footer was accepted).
	if rd.NextOffset() != int64(len(b)) {
		return Meta{}, nil, fmt.Errorf("%w: %d bytes after the footer", ErrCorrupt, int64(len(b))-rd.NextOffset())
	}
	if uint64(len(data)) != dataLen {
		return Meta{}, nil, fmt.Errorf("%w: %d state bytes, header says %d", ErrCorrupt, len(data), dataLen)
	}
	if sha256.Sum256(data) != sum {
		return Meta{}, nil, fmt.Errorf("%w: state checksum mismatch", ErrCorrupt)
	}
	return m, data, nil
}

func decodeHeader(p []byte) (Meta, uint64, [32]byte, error) {
	var sum [32]byte
	if len(p) < len(magic) || string(p[:len(magic)]) != magic {
		return Meta{}, 0, sum, fmt.Errorf("%w: not a snapshot (bad magic)", ErrCorrupt)
	}
	d := decoder{b: p, i: len(magic)}
	if v := d.uint(); d.err == nil && v != Version {
		return Meta{}, 0, sum, fmt.Errorf("%w: version %d (this build reads %d)", ErrVersion, v, Version)
	}
	n := d.uint()
	if d.err == nil && (n == 0 || n > MaxMembers) {
		return Meta{}, 0, sum, fmt.Errorf("%w: %d members", ErrCorrupt, n)
	}
	var m Meta
	for i := uint64(0); i < n && d.err == nil; i++ {
		m.Members = append(m.Members, string(d.bytes(MaxMemberLen)))
	}
	m.Index, m.Term = d.uint(), d.uint()
	dataLen := d.uint()
	if d.err == nil && dataLen > MaxData {
		return Meta{}, 0, sum, fmt.Errorf("%w: header declares %d state bytes", ErrTooLarge, dataLen)
	}
	if d.err == nil && len(p)-d.i != len(sum) {
		return Meta{}, 0, sum, fmt.Errorf("%w: header of %d bytes", ErrCorrupt, len(p))
	}
	if d.err != nil {
		return Meta{}, 0, sum, d.err
	}
	copy(sum[:], p[d.i:])
	if err := m.validate(); err != nil {
		return Meta{}, 0, sum, err
	}
	return m, dataLen, sum, nil
}

// decoder reads canonical uvarints and bounded byte strings, remembering the
// first error (as internal/kv's).
type decoder struct {
	b   []byte
	i   int
	err error
}

func (d *decoder) uint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b[d.i:])
	if n <= 0 || n != uvarintLen(v) {
		d.err = fmt.Errorf("%w: bad or non-canonical integer", ErrCorrupt)
		return 0
	}
	d.i += n
	return v
}

func (d *decoder) bytes(max int) []byte {
	n := d.uint()
	if d.err != nil {
		return nil
	}
	if n == 0 || n > uint64(max) || n > uint64(len(d.b)-d.i) {
		d.err = fmt.Errorf("%w: byte string of %d", ErrCorrupt, n)
		return nil
	}
	out := append([]byte(nil), d.b[d.i:d.i+int(n)]...)
	d.i += int(n)
	return out
}

func (d *decoder) done() error {
	if d.err == nil && d.i != len(d.b) {
		d.err = fmt.Errorf("%w: trailing bytes", ErrCorrupt)
	}
	return d.err
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
