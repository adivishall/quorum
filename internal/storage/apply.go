package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/adivishall/quorum/internal/storage/ikey"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// Apply batches (S1, docs/STORAGE_INTEGRATION.md §7): the engine's half of R1.
// A replicated state machine applies a span of committed Raft entries; the
// mutations they produce and the applied index they bring it to are recorded
// as ONE WAL record, so recovery observes both or neither. Raft decides the
// order; the engine only makes the result of applying it durable, and never
// numbers anything itself beyond its own sequence numbers.

// MutationKind is what a Mutation does.
type MutationKind uint8

const (
	// MutationPut stores Value under Key.
	MutationPut MutationKind = iota + 1
	// MutationDelete removes Key; Value must be empty.
	MutationDelete
)

// Mutation is one write of an apply batch.
type Mutation struct {
	Kind  MutationKind
	Key   []byte
	Value []byte
}

// Apply records muts and the applied index they bring the store to as one WAL
// record, then makes the mutations visible, then the index.
//
// Guaranteed (docs/STORAGE_INTEGRATION.md §7.2):
//
//   - Crash atomicity. After a process crash or a power loss, recovery observes
//     the batch whole — every mutation and its index — or not at all: no
//     mutation, and the index as it was. Batches are recovered as a prefix of
//     the order they were applied in.
//   - Durability on return is the WAL sync mode's, as for Put: every mode
//     survives a process crash; sync survives a power loss; batch does once a
//     flush has covered it (Sync forces one).
//   - The index advances: applied.Index above the current one, applied.Term
//     not below it, neither zero — else ErrAppliedIndex, and nothing is
//     written. An empty muts is valid and advances the index alone (a Raft no-op
//     or configuration entry).
//   - Publication order: the mutations are visible to Get before AppliedIndex
//     reports the batch, so a reader that sees index i sees every mutation up
//     to it.
//   - A failed append leaves the store's state, its sequence and its applied
//     index unchanged. The WAL latches the failure; whether the batch is
//     recovered after a crash is then unknown — but never in part.
//
// Not guaranteed: isolation. A Get running while the batch is inserted may see
// some of its mutations and not others; the atomicity is with respect to a
// crash, not a transaction. And it does not make Put, Delete or SetAppliedIndex
// atomic with anything: they remain separate records.
//
// Each mutation takes the next sequence number, in batch order; the applied
// index takes none.
func (s *LSMStore) Apply(ctx context.Context, muts []Mutation, applied AppliedIndex) error {
	const op = "apply"
	if err := ctx.Err(); err != nil {
		return opErr(op, nil, err)
	}
	ops := make(wal.Batch, len(muts))
	for i, m := range muts {
		if err := s.opts.validateKey(op, m.Key); err != nil {
			return err
		}
		switch m.Kind {
		case MutationPut:
			if err := s.opts.validateValue(op, m.Key, m.Value); err != nil {
				return err
			}
			ops[i] = wal.Op{Kind: wal.OpPut, Key: m.Key, Value: m.Value}
		case MutationDelete:
			if len(m.Value) != 0 {
				return opErr(op, m.Key, fmt.Errorf("%w: mutation %d deletes and carries a value", ErrInvalidBatch, i))
			}
			ops[i] = wal.Op{Kind: wal.OpDelete, Key: m.Key}
		default:
			return opErr(op, m.Key, fmt.Errorf("%w: mutation %d has unknown kind %d", ErrInvalidBatch, i, m.Kind))
		}
	}
	ab := wal.ApplyBatch{Applied: wal.AppliedIndex{Index: applied.Index, Term: applied.Term}, Ops: ops}

	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return opErr(op, nil, ErrClosed)
	}

	var flushed bool
	if err := func() error {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()

		if s.flushErr != nil {
			return classify(op, nil, s.flushErr)
		}
		cur := wal.AppliedIndex{Index: s.applied.Index, Term: s.applied.Term}
		if applied.Index == 0 || applied.Term == 0 || !ab.Advances(cur) {
			return opErr(op, nil, fmt.Errorf("%w: (%d, %d) after (%d, %d)",
				ErrAppliedIndex, applied.Index, applied.Term, cur.Index, cur.Term))
		}
		if uint64(len(ops)) > ikey.MaxSeq-s.seq {
			return opErr(op, nil, fmt.Errorf("%w: the %d-bit sequence space cannot hold %d more mutations",
				ErrIO, 8*ikey.SeqBytes, len(ops)))
		}

		if err := s.w.AppendApply(ab); err != nil {
			// Nothing is published. A record too large for the framing was
			// refused before anything was written and latches nothing; any
			// other failure latches the WAL, which may hold a torn tail.
			if errors.Is(err, wal.ErrRecordTooLarge) {
				return opErr(op, nil, fmt.Errorf("%w: %v", ErrInvalidBatch, err))
			}
			return classify(op, nil, err)
		}

		// The record is in the log: publish the mutations, then the index.
		s.mu.RLock()
		mem := s.cur.mem
		s.mu.RUnlock()
		for _, o := range ops {
			s.seq++
			kind := ikey.KindValue
			if o.Kind == wal.OpDelete {
				kind = ikey.KindTombstone
			}
			mem.Add(s.seq, kind, o.Key, o.Value)
		}
		s.mu.Lock()
		s.applied = applied
		s.appliedSeq = s.seq
		s.mu.Unlock()

		if mem.ApproxSize() >= s.opts.MemTableSize {
			if err := s.flushLocked(); err != nil {
				// The batch itself succeeded: it is in the log and visible.
				// The failure latches and the next write reports it.
				s.flushErr = err
			} else {
				flushed = true
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	if flushed {
		s.compactionSignal()
	}
	return nil
}

// AppliedSequence returns the sequence number of the last mutation the last
// apply batch held: the data the applied index covers is exactly the mutations
// numbered up to it (S1). It is 0 before any apply batch, and does not move for
// Put, Delete or SetAppliedIndex.
func (s *LSMStore) AppliedSequence() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedSeq
}
