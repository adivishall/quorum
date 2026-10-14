package snapshot

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/replication"
)

// Streaming (S4, docs/STORAGE_INTEGRATION.md §10.4–§10.5). The format is the
// one Encode writes and Decode reads, byte for byte; these are the same
// encoder and decoder for a state that should never be held in memory whole.

// Source is a snapshot's state, produced by writing it. WriteTo must write
// the same bytes every time it is called: Write calls it twice, once to
// measure and hash the state for the header and once to write it.
type Source interface {
	WriteTo(w io.Writer) (int64, error)
}

// BytesSource is a Source over a state already in memory.
type BytesSource []byte

// WriteTo writes the state.
func (b BytesSource) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b)
	return int64(n), err
}

// ErrSourceChanged is a Source whose second pass differed from its first: the
// state it measured is not the state it wrote. Nothing written with it may be
// published.
var ErrSourceChanged = errors.New("snapshot: the source did not reproduce its state")

// errOverLimit stops a measuring pass at MaxData.
var errOverLimit = errors.New("snapshot: over the state bound")

// measurer is the measuring pass: the state's length and SHA-256, refusing
// past MaxData without hashing gigabytes first.
type measurer struct {
	h hash.Hash
	n uint64
}

func (m *measurer) Write(p []byte) (int, error) {
	if m.n+uint64(len(p)) > MaxData {
		return 0, errOverLimit
	}
	m.h.Write(p)
	m.n += uint64(len(p))
	return len(p), nil
}

// encodeHeader returns the header record's payload.
func encodeHeader(m Meta, dataLen uint64, sum [32]byte) []byte {
	h := []byte(magic)
	h = binary.AppendUvarint(h, Version)
	h = binary.AppendUvarint(h, uint64(m.Group))
	conf := replication.EncodeConfiguration(m.Conf)
	h = binary.AppendUvarint(h, uint64(len(conf)))
	h = append(h, conf...)
	h = binary.AppendUvarint(h, m.Index)
	h = binary.AppendUvarint(h, m.Term)
	h = binary.AppendUvarint(h, dataLen)
	return append(h, sum[:]...)
}

// encodeFooter returns the footer record's payload.
func encodeFooter(m Meta) []byte {
	f := binary.AppendUvarint(nil, m.Index)
	return binary.AppendUvarint(f, m.Term)
}

// Write writes the snapshot file of src's state, described by m, to w — the
// bytes Encode would return for the same state — and returns how many it
// wrote. A state over MaxData is ErrTooLarge, found by the measuring pass
// before anything is written. A source whose second pass differs from its
// first is ErrSourceChanged; w then holds a file that does not validate.
func Write(w io.Writer, m Meta, src Source) (int64, error) {
	if err := m.validate(); err != nil {
		return 0, err
	}
	meas := &measurer{h: sha256.New()}
	if _, err := src.WriteTo(meas); err != nil {
		if errors.Is(err, errOverLimit) {
			return 0, fmt.Errorf("%w: more than %d bytes", ErrTooLarge, MaxData)
		}
		return 0, fmt.Errorf("snapshot: measuring the state: %w", err)
	}
	var sum [32]byte
	copy(sum[:], meas.h.Sum(nil))

	out := &recordWriter{w: w, h: sha256.New(), buf: make([]byte, 0, MaxDataRecord)}
	if err := out.record(kindHeader, encodeHeader(m, meas.n, sum)); err != nil {
		return out.written, err
	}
	if _, err := src.WriteTo(out); err != nil {
		return out.written, fmt.Errorf("snapshot: writing the state: %w", err)
	}
	if err := out.flushData(); err != nil {
		return out.written, err
	}
	var again [32]byte
	copy(again[:], out.h.Sum(nil))
	if out.data != meas.n || again != sum {
		return out.written, fmt.Errorf("%w: measured %d bytes, wrote %d", ErrSourceChanged, meas.n, out.data)
	}
	if err := out.record(kindFooter, encodeFooter(m)); err != nil {
		return out.written, err
	}
	return out.written, nil
}

// recordWriter cuts the state into data records of MaxDataRecord bytes, as
// Encode does, hashing what it writes.
type recordWriter struct {
	w       io.Writer
	h       hash.Hash
	buf     []byte // the data record being filled
	enc     []byte // reused record encoding
	data    uint64 // state bytes accepted
	written int64  // file bytes written
}

func (r *recordWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		take := min(MaxDataRecord-len(r.buf), len(p))
		r.buf = append(r.buf, p[:take]...)
		p, n = p[take:], n+take
		if len(r.buf) == MaxDataRecord {
			if err := r.flushData(); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// flushData writes the buffered state bytes as one data record (none if the
// buffer is empty: a data record is never empty).
func (r *recordWriter) flushData() error {
	if len(r.buf) == 0 {
		return nil
	}
	r.h.Write(r.buf)
	r.data += uint64(len(r.buf))
	err := r.record(kindData, r.buf)
	r.buf = r.buf[:0]
	return err
}

func (r *recordWriter) record(kind record.Kind, payload []byte) error {
	var err error
	if r.enc, err = record.Encode(r.enc[:0], kind, payload); err != nil {
		return err
	}
	n, err := r.w.Write(r.enc)
	r.written += int64(n)
	if err == nil && n != len(r.enc) {
		err = io.ErrShortWrite
	}
	return err
}

// Reader streams the state of a snapshot file, validating as it goes: exactly
// the checks Decode makes, in the same order, so a file Decode refuses is
// refused here, with ErrCorrupt (or ErrVersion, ErrTooLarge from the header).
// NewReader reads and checks the header; Read then returns the state's bytes
// in order, and returns io.EOF only once the whole file is proven valid —
// every record's checksum, no data past the declared length, the footer
// repeating the header's index and term, nothing after the footer, the length
// and the SHA-256. Until then a caller must treat what it has read as
// unvalidated. It holds one record at a time.
type Reader struct {
	rd   *record.Reader
	size int64

	meta    Meta
	dataLen uint64
	sum     [32]byte
	h       hash.Hash
	got     uint64

	buf  []byte // unread bytes of the current data record
	err  error  // latched: once invalid, always invalid
	done bool   // the footer was read and everything checked
}

// NewReader reads the header of the snapshot file r, which must hold exactly
// size bytes.
func NewReader(r io.Reader, size int64) (*Reader, error) {
	rd := &Reader{rd: record.NewReader(r, "snapshot", size), size: size, h: sha256.New()}
	kind, p, err := rd.next()
	if err != nil {
		return nil, err
	}
	if kind != kindHeader {
		return nil, fmt.Errorf("%w: first record is kind %d, not a header", ErrCorrupt, kind)
	}
	if rd.meta, rd.dataLen, rd.sum, err = decodeHeader(p); err != nil {
		return nil, err
	}
	return rd, nil
}

func (r *Reader) next() (record.Kind, []byte, error) {
	k, p, err := r.rd.Next()
	if err == io.EOF {
		return 0, nil, fmt.Errorf("%w: ends before its footer", ErrCorrupt)
	}
	if err != nil {
		return 0, nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return k, p, nil
}

// Meta returns the snapshot's metadata, from its header.
func (r *Reader) Meta() Meta { return r.meta }

// DataLen returns the state's length, from its header.
func (r *Reader) DataLen() uint64 { return r.dataLen }

// Read returns the next bytes of the state.
func (r *Reader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.done {
			return 0, io.EOF
		}
		r.err = r.advance()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// advance reads the next record: a data record's bytes into buf, or the
// footer, after which every whole-file check is made.
func (r *Reader) advance() error {
	kind, p, err := r.next()
	if err != nil {
		return err
	}
	switch kind {
	case kindData:
		if len(p) == 0 || len(p) > MaxDataRecord || r.got+uint64(len(p)) > r.dataLen {
			return fmt.Errorf("%w: data record of %d bytes (have %d of %d)", ErrCorrupt, len(p), r.got, r.dataLen)
		}
		r.got += uint64(len(p))
		r.h.Write(p)
		r.buf = p
		return nil
	case kindFooter:
		d := decoder{b: p}
		idx, term := d.uint(), d.uint()
		if err := d.done(); err != nil {
			return err
		}
		if idx != r.meta.Index || term != r.meta.Term {
			return fmt.Errorf("%w: footer (%d,%d) disagrees with header (%d,%d)", ErrCorrupt, idx, term, r.meta.Index, r.meta.Term)
		}
		if r.rd.NextOffset() != r.size {
			return fmt.Errorf("%w: %d bytes after the footer", ErrCorrupt, r.size-r.rd.NextOffset())
		}
		if r.got != r.dataLen {
			return fmt.Errorf("%w: %d state bytes, header says %d", ErrCorrupt, r.got, r.dataLen)
		}
		var sum [32]byte
		copy(sum[:], r.h.Sum(nil))
		if sum != r.sum {
			return fmt.Errorf("%w: state checksum mismatch", ErrCorrupt)
		}
		r.done = true
		return nil
	default:
		return fmt.Errorf("%w: unexpected record kind %d", ErrCorrupt, kind)
	}
}

// Verify reads the rest of the state, discarding it, and reports whether the
// file is valid. It is what a caller that does not need the state — a
// receiver checking a transfer, a node checking its published snapshot at
// startup — uses to prove the whole file.
func (r *Reader) Verify() error {
	_, err := io.Copy(io.Discard, r)
	return err
}
