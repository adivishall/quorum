// Package wal implements Quorum's storage-engine write-ahead log.
//
// The WAL is the only thing standing between an acknowledged write and a
// process that stops existing. Every mutation is appended here, as one framed
// record, before it becomes visible in memory; on restart the log is replayed
// to reconstruct the state that was durable at the moment of the crash.
//
// Phase 2 scope: this is a write-ahead log and nothing else. There is no
// memtable, no SSTable, no compaction and no log truncation — replay always
// reads every segment. Those arrive in Phases 3 and 4.
package wal

import (
	"encoding/binary"
	"fmt"

	"github.com/adivishall/quorum/internal/record"
)

// Record kinds within a WAL file (docs/DESIGN.md §3). The namespace is local to
// this file type: kind 0x01 means something else entirely in a Raft log.
const (
	// KindWriteBatch carries one atomic group of mutations.
	KindWriteBatch record.Kind = 0x01
	// KindAppliedIndex carries durable Raft progress metadata.
	KindAppliedIndex record.Kind = 0x02
)

// OpKind distinguishes a write from a delete. The values match the internal-key
// value types in docs/DESIGN.md §1, so the two encodings agree when the LSM
// engine arrives in Phase 3.
type OpKind uint8

const (
	// OpDelete is a tombstone: the key is removed. No value is encoded.
	OpDelete OpKind = 0x00
	// OpPut sets a key to a value.
	OpPut OpKind = 0x01
)

func (k OpKind) String() string {
	switch k {
	case OpDelete:
		return "delete"
	case OpPut:
		return "put"
	default:
		return fmt.Sprintf("OpKind(%#x)", uint8(k))
	}
}

// ErrCorrupt is returned when a WAL payload cannot be decoded.
//
// It is deliberately the same error value as record.ErrCorrupt, so a caller
// gets one sentinel to test regardless of whether the damage was caught by the
// framing checksum or by payload decoding. Both mean the same thing: this log
// cannot be trusted and must not be silently skipped past.
var ErrCorrupt = record.ErrCorrupt

// Op is a single mutation.
//
// Key and Value alias the caller's memory on the encode path and the decoder's
// buffer on the decode path; neither is retained past the call.
type Op struct {
	Kind  OpKind
	Key   []byte
	Value []byte // always nil for OpDelete
}

// Batch is a group of mutations that is atomic with respect to a crash.
//
// A batch is encoded as exactly one framed record, so its checksum covers every
// operation in it. Either the whole batch replays or none of it does; there is
// no state in which half a batch survived.
type Batch []Op

// AppliedIndex is durable metadata describing the last Raft entry applied to
// the state machine.
//
// Phase 2 has no Raft. This record exists now so that the on-disk format does
// not change when Phase 9 arrives, and so that the replay path that will
// eventually reconcile the engine against the Raft log is exercised from the
// start. It is written and replayed as opaque metadata and carries no
// consensus meaning yet: nothing in Phase 2 produces a non-zero value except a
// caller that explicitly sets one.
type AppliedIndex struct {
	Index uint64
	Term  uint64
}

// appliedIndexSize is the exact encoded size: two little-endian uint64s.
const appliedIndexSize = 16

// AppendTo encodes the batch onto dst and returns the extended slice.
func (b Batch) AppendTo(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	for _, op := range b {
		dst = append(dst, byte(op.Kind))
		dst = binary.AppendUvarint(dst, uint64(len(op.Key)))
		dst = append(dst, op.Key...)
		if op.Kind == OpPut {
			dst = binary.AppendUvarint(dst, uint64(len(op.Value)))
			dst = append(dst, op.Value...)
		}
	}
	return dst
}

// DecodeBatch decodes a WriteBatch payload.
//
// Decoding is strict on purpose. Every field is checked against the bytes that
// actually remain, and any leftover bytes at the end are an error. A lenient
// decoder in a write-ahead log converts corruption into plausible-looking
// state, which is strictly worse than refusing to start: the operator sees a
// database that came up fine and is quietly wrong.
func DecodeBatch(payload []byte) (Batch, error) {
	p := payload

	count, n := binary.Uvarint(p)
	if n <= 0 {
		return nil, fmt.Errorf("wal: batch: unreadable operation count: %w", ErrCorrupt)
	}
	p = p[n:]

	if count == 0 {
		return nil, fmt.Errorf("wal: batch: operation count is zero; an empty batch is never written: %w", ErrCorrupt)
	}
	// Each operation costs at least three bytes (kind, key length, one key
	// byte), so a count larger than the remaining bytes is impossible and must
	// be rejected before it is used to size an allocation.
	if count > uint64(len(p)) {
		return nil, fmt.Errorf("wal: batch: operation count %d exceeds the %d bytes remaining: %w",
			count, len(p), ErrCorrupt)
	}

	batch := make(Batch, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(p) < 1 {
			return nil, fmt.Errorf("wal: batch: truncated at operation %d of %d: %w", i, count, ErrCorrupt)
		}
		kind := OpKind(p[0])
		p = p[1:]

		if kind != OpPut && kind != OpDelete {
			return nil, fmt.Errorf("wal: batch: operation %d has unknown kind %#x: %w", i, uint8(kind), ErrCorrupt)
		}

		key, rest, err := takeBytes(p, "key", i)
		if err != nil {
			return nil, err
		}
		p = rest
		if len(key) == 0 {
			// An empty key is rejected by the Store contract, so one can never
			// have been written. Seeing it means the payload is not what it
			// claims to be.
			return nil, fmt.Errorf("wal: batch: operation %d has an empty key: %w", i, ErrCorrupt)
		}

		op := Op{Kind: kind, Key: key}
		if kind == OpPut {
			value, rest, err := takeBytes(p, "value", i)
			if err != nil {
				return nil, err
			}
			p = rest
			op.Value = value
		}
		batch = append(batch, op)
	}

	if len(p) != 0 {
		return nil, fmt.Errorf("wal: batch: %d unconsumed bytes after %d operations: %w",
			len(p), count, ErrCorrupt)
	}
	return batch, nil
}

// takeBytes reads a uvarint-prefixed byte string.
func takeBytes(p []byte, what string, opIndex uint64) (value, rest []byte, err error) {
	n, read := binary.Uvarint(p)
	if read <= 0 {
		return nil, nil, fmt.Errorf("wal: batch: operation %d has an unreadable %s length: %w",
			opIndex, what, ErrCorrupt)
	}
	p = p[read:]
	if n > uint64(len(p)) {
		return nil, nil, fmt.Errorf("wal: batch: operation %d declares a %d-byte %s but only %d bytes remain: %w",
			opIndex, n, what, len(p), ErrCorrupt)
	}
	return p[:n], p[n:], nil
}

// AppendTo encodes the applied index onto dst and returns the extended slice.
func (a AppliedIndex) AppendTo(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, a.Index)
	dst = binary.LittleEndian.AppendUint64(dst, a.Term)
	return dst
}

// DecodeAppliedIndex decodes an AppliedIndex payload. The encoding is fixed
// width, so any other length is corruption rather than a newer format.
func DecodeAppliedIndex(payload []byte) (AppliedIndex, error) {
	if len(payload) != appliedIndexSize {
		return AppliedIndex{}, fmt.Errorf("wal: applied index: payload is %d bytes, want exactly %d: %w",
			len(payload), appliedIndexSize, ErrCorrupt)
	}
	return AppliedIndex{
		Index: binary.LittleEndian.Uint64(payload[0:8]),
		Term:  binary.LittleEndian.Uint64(payload[8:16]),
	}, nil
}
