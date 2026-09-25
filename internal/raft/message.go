package raft

import "encoding/binary"

// Bounds for the hand-written, reflection-free message codec (ADR-003, ADR-016).
// Every decoder checks a declared length against the bytes remaining and against
// a maximum before allocating, and returns ErrMalformedMessage rather than
// panicking on hostile input.
const (
	// MaxEntriesPerMessage bounds the entry count in one AppendEntries, so a
	// declared count cannot size an unbounded allocation. Far beyond any single
	// AppendEntries this implementation sends.
	MaxEntriesPerMessage = 1 << 16
	// MaxEntryDataLen bounds one entry's opaque command bytes (the 1 MiB value
	// limit, docs/DESIGN.md §1).
	MaxEntryDataLen = 1 << 20
)

// Marshal encodes a message payload deterministically. It encodes the type and
// the type-specific fields; From/To are NOT encoded — the receiver attributes a
// message to the connection's handshake identity, never to a payload field
// (INV-T4). The driver fills From/To from the transport envelope.
func (m Message) Marshal() []byte {
	w := msgWriter{}
	w.byte(byte(m.Type))
	w.uvarint(m.Term)
	switch m.Type {
	case MsgVoteRequest:
		w.uvarint(m.LastLogIndex)
		w.uvarint(m.LastLogTerm)
	case MsgVoteResponse:
		w.bool(m.VoteGranted)
	case MsgAppendRequest:
		w.uvarint(m.PrevLogIndex)
		w.uvarint(m.PrevLogTerm)
		w.uvarint(m.LeaderCommit)
		w.uvarint(uint64(len(m.Entries)))
		for _, e := range m.Entries {
			w.uvarint(e.Index)
			w.uvarint(e.Term)
			w.bytes(e.Data)
		}
		w.uvarint(m.Seq)
	case MsgAppendResponse:
		w.bool(m.Success)
		w.uvarint(m.MatchIndex)
		w.uvarint(m.ConflictTerm)
		w.uvarint(m.ConflictIndex)
		w.uvarint(m.Seq)
	}
	return w.b
}

// Unmarshal decodes a message payload. From/To are left empty for the driver to
// fill from the transport envelope. It consumes exactly the payload and rejects
// trailing bytes.
func Unmarshal(payload []byte) (Message, error) {
	r := msgReader{b: payload}
	t, err := r.byte()
	if err != nil {
		return Message{}, err
	}
	m := Message{Type: MessageType(t)}
	if m.Term, err = r.uvarint(); err != nil {
		return Message{}, err
	}
	switch m.Type {
	case MsgVoteRequest:
		if m.LastLogIndex, err = r.uvarint(); err != nil {
			return Message{}, err
		}
		if m.LastLogTerm, err = r.uvarint(); err != nil {
			return Message{}, err
		}
	case MsgVoteResponse:
		if m.VoteGranted, err = r.bool(); err != nil {
			return Message{}, err
		}
	case MsgAppendRequest:
		if m.PrevLogIndex, err = r.uvarint(); err != nil {
			return Message{}, err
		}
		if m.PrevLogTerm, err = r.uvarint(); err != nil {
			return Message{}, err
		}
		if m.LeaderCommit, err = r.uvarint(); err != nil {
			return Message{}, err
		}
		n, err := r.uvarint()
		if err != nil {
			return Message{}, err
		}
		if n > MaxEntriesPerMessage {
			return Message{}, ErrMalformedMessage
		}
		var entries []Entry
		for i := uint64(0); i < n; i++ {
			var e Entry
			if e.Index, err = r.uvarint(); err != nil {
				return Message{}, err
			}
			if e.Term, err = r.uvarint(); err != nil {
				return Message{}, err
			}
			if e.Data, err = r.bytes(MaxEntryDataLen); err != nil {
				return Message{}, err
			}
			entries = append(entries, e)
		}
		m.Entries = entries
		if m.Seq, err = r.uvarint(); err != nil {
			return Message{}, err
		}
	case MsgAppendResponse:
		if m.Success, err = r.bool(); err != nil {
			return Message{}, err
		}
		if m.MatchIndex, err = r.uvarint(); err != nil {
			return Message{}, err
		}
		if m.ConflictTerm, err = r.uvarint(); err != nil {
			return Message{}, err
		}
		if m.ConflictIndex, err = r.uvarint(); err != nil {
			return Message{}, err
		}
		if m.Seq, err = r.uvarint(); err != nil {
			return Message{}, err
		}
	default:
		return Message{}, ErrUnknownMessageType
	}
	if err := r.done(); err != nil {
		return Message{}, err
	}
	return m, nil
}

// --- writer ---

type msgWriter struct {
	b   []byte
	buf [binary.MaxVarintLen64]byte
}

func (w *msgWriter) byte(v byte) { w.b = append(w.b, v) }
func (w *msgWriter) bool(v bool) {
	if v {
		w.b = append(w.b, 1)
	} else {
		w.b = append(w.b, 0)
	}
}
func (w *msgWriter) uvarint(v uint64) {
	n := binary.PutUvarint(w.buf[:], v)
	w.b = append(w.b, w.buf[:n]...)
}
func (w *msgWriter) bytes(p []byte) {
	w.uvarint(uint64(len(p)))
	w.b = append(w.b, p...)
}

// --- reader ---

type msgReader struct {
	b []byte
	i int
}

func (r *msgReader) byte() (byte, error) {
	if r.i+1 > len(r.b) {
		return 0, ErrMalformedMessage
	}
	v := r.b[r.i]
	r.i++
	return v, nil
}

func (r *msgReader) bool() (bool, error) {
	v, err := r.byte()
	if err != nil {
		return false, err
	}
	if v > 1 {
		return false, ErrMalformedMessage // a bool is exactly 0 or 1
	}
	return v == 1, nil
}

func (r *msgReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.i:])
	if n <= 0 {
		return 0, ErrMalformedMessage
	}
	r.i += n
	return v, nil
}

// bytes reads a uvarint length then that many bytes, rejecting a length over max
// or over the bytes remaining. It returns a fresh copy (no aliasing of payload).
func (r *msgReader) bytes(max int) ([]byte, error) {
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(max) || int(n) > len(r.b)-r.i {
		return nil, ErrMalformedMessage
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]byte, n)
	copy(out, r.b[r.i:r.i+int(n)])
	r.i += int(n)
	return out, nil
}

func (r *msgReader) done() error {
	if r.i != len(r.b) {
		return ErrMalformedMessage
	}
	return nil
}
