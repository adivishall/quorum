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
	// KindApplyBatch carries a state-machine application (S1): the mutations
	// of a span of applied entries AND the applied index they bring the state
	// machine to, in one record, so a crash keeps both or neither
	// (docs/STORAGE_INTEGRATION.md §7.3).
	KindApplyBatch record.Kind = 0x03
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
func (b Batch) AppendTo(dst []byte) []byte { return b.appendOps(dst) }

// appendOps encodes the operation count and the operations: a WriteBatch's
// whole payload, and an ApplyBatch's after its header.
func (b Batch) appendOps(dst []byte) []byte {
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

	count, n := uvarint(p)
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

	return decodeOps(p, count, "batch")
}

// decodeOps decodes count operations, which must consume p exactly. what names
// the record for the error.
func decodeOps(p []byte, count uint64, what string) (Batch, error) {
	batch := make(Batch, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(p) < 1 {
			return nil, fmt.Errorf("wal: %s: truncated at operation %d of %d: %w", what, i, count, ErrCorrupt)
		}
		kind := OpKind(p[0])
		p = p[1:]

		if kind != OpPut && kind != OpDelete {
			return nil, fmt.Errorf("wal: %s: operation %d has unknown kind %#x: %w", what, i, uint8(kind), ErrCorrupt)
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
			return nil, fmt.Errorf("wal: %s: operation %d has an empty key: %w", what, i, ErrCorrupt)
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
		return nil, fmt.Errorf("wal: %s: %d unconsumed bytes after %d operations: %w",
			what, len(p), count, ErrCorrupt)
	}
	return batch, nil
}

// takeBytes reads a uvarint-prefixed byte string.
func takeBytes(p []byte, what string, opIndex uint64) (value, rest []byte, err error) {
	n, read := uvarint(p)
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

// ApplyBatch is a state-machine application (S1, docs/STORAGE_INTEGRATION.md
// §7): the mutations a span of applied entries produced, and the applied index
// they bring the state machine to. It is encoded as ONE record, so its checksum
// covers both: either recovery sees every mutation together with the index, or
// none of the mutations and the index as it was before. Ops may be empty — an
// entry with no effect on the data (a Raft no-op, a configuration change) still
// advances the index.
type ApplyBatch struct {
	Applied AppliedIndex
	Ops     Batch
}

// applyBatchVersion is the payload version this package writes and reads.
const applyBatchVersion = 1

// applyHeaderSize is the fixed prefix: a version byte, the index and the term.
const applyHeaderSize = 1 + appliedIndexSize

// AppendTo encodes the apply batch onto dst and returns the extended slice:
// version, index and term (little-endian), then the operations exactly as a
// WriteBatch encodes them.
func (a ApplyBatch) AppendTo(dst []byte) []byte {
	dst = append(dst, applyBatchVersion)
	dst = a.Applied.AppendTo(dst)
	return a.Ops.appendOps(dst)
}

// DecodeApplyBatch decodes an ApplyBatch payload, strictly: an unknown version,
// an index or term of zero (no applied entry has either), a count the bytes
// cannot hold, a malformed operation or a trailing byte is corruption.
func DecodeApplyBatch(payload []byte) (ApplyBatch, error) {
	if len(payload) < applyHeaderSize {
		return ApplyBatch{}, fmt.Errorf("wal: apply batch: payload is %d bytes, shorter than its %d-byte header: %w",
			len(payload), applyHeaderSize, ErrCorrupt)
	}
	if v := payload[0]; v != applyBatchVersion {
		return ApplyBatch{}, fmt.Errorf("wal: apply batch: unknown version %d: %w", v, ErrCorrupt)
	}
	applied, err := DecodeAppliedIndex(payload[1:applyHeaderSize])
	if err != nil {
		return ApplyBatch{}, err
	}
	if applied.Index == 0 || applied.Term == 0 {
		return ApplyBatch{}, fmt.Errorf("wal: apply batch: applied index (%d, %d) names no entry: %w",
			applied.Index, applied.Term, ErrCorrupt)
	}
	p := payload[applyHeaderSize:]
	count, n := uvarint(p)
	if n <= 0 {
		return ApplyBatch{}, fmt.Errorf("wal: apply batch: unreadable operation count: %w", ErrCorrupt)
	}
	p = p[n:]
	// As for a WriteBatch: each operation costs at least three bytes, so a
	// count above the bytes left is impossible.
	if count > uint64(len(p)) {
		return ApplyBatch{}, fmt.Errorf("wal: apply batch: operation count %d exceeds the %d bytes remaining: %w",
			count, len(p), ErrCorrupt)
	}
	ops, err := decodeOps(p, count, "apply batch")
	if err != nil {
		return ApplyBatch{}, err
	}
	return ApplyBatch{Applied: applied, Ops: ops}, nil
}

// Advances reports whether applying a moves the state machine forward from
// prev, the applied index in effect before it: a higher index, in a term no
// lower. It is the rule the writer enforces and replay re-checks.
func (a ApplyBatch) Advances(prev AppliedIndex) bool {
	return a.Applied.Index > prev.Index && a.Applied.Term >= prev.Term
}

// uvarint is binary.Uvarint restricted to the minimal (canonical) encoding: a
// count or length written in more bytes than it needs (0 as 0x80 0x00) is
// refused (n = 0), as the key-value codecs refuse it (kv.uvarint), so every
// payload these decoders accept has exactly one meaning and is exactly what
// this package would encode for it. Found by fuzzing apply batches (S1); the
// encoder has only ever written the minimal form, so no log it wrote is
// refused.
func uvarint(p []byte) (uint64, int) {
	v, n := binary.Uvarint(p)
	if n > 0 && n != len(binary.AppendUvarint(nil, v)) {
		return 0, 0
	}
	return v, n
}
