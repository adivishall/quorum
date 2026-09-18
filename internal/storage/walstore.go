package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/adivishall/quorum/internal/storage/wal"
)

// walDirName is the WAL subdirectory of the data directory. Phase 3 adds
// siblings (MANIFEST, CURRENT, *.sst) alongside it.
const walDirName = "wal"

// AppliedIndex is durable metadata describing the last Raft entry applied to
// this store.
//
// Phase 2 has no Raft, and nothing here produces a non-zero value on its own.
// It exists now so that the on-disk format does not have to change when Phase 9
// arrives, and so that the replay path that will eventually reconcile the
// engine against the Raft log is exercised from the start. Treat it as opaque
// metadata that survives restart; it carries no consensus meaning yet.
type AppliedIndex struct {
	Index uint64
	Term  uint64
}

// WALStore is a durable, single-node Store.
//
// It is not an LSM engine. The authoritative state lives in a write-ahead log
// on disk; the in-memory map is a cache of the log's effect, rebuilt by replay
// on every open. That means memory use is proportional to the live data set and
// recovery time is proportional to the whole log, neither of which is
// acceptable long term — Phase 3's memtable and SSTables fix both. What this
// buys now is the property Phase 1 lacked entirely: an acknowledged write
// survives the process dying.
//
// # Write path
//
// The order is fixed and is the reason recovery can be trusted:
//
//	validate                      -- reject bad input before anything is logged
//	encode the mutation as a batch
//	append to the WAL             -- write(2); the bytes reach the kernel
//	flush per the sync mode       -- fsync now, later, or never
//	publish to the in-memory map
//	return nil
//
// The in-memory state is updated only after the log write has succeeded, never
// before. Reversing those two steps would let a failed or interrupted log write
// leave a value visible in memory that the log does not contain, so a restart
// would silently lose a write the client was told had succeeded.
//
// The converse skew is possible and is harmless: a crash between the append and
// the publish leaves a record in the log that was never acknowledged. Replay
// applies it. That is inherent to write-ahead logging — a client whose request
// was interrupted genuinely cannot know whether it took effect — and it is
// stated in docs/CONSISTENCY.md C4 rather than papered over.
//
// # Locking
//
// Two locks, always acquired in this order and never the reverse:
//
//	writeMu  serialises writers. Held across BOTH the log append and the
//	         in-memory publish, so that the order mutations reach the log is
//	         exactly the order they reach memory. Without that, two concurrent
//	         Puts to one key could be logged A-then-B but applied B-then-A, and
//	         the state after a restart would differ from the state before it.
//	mu       guards the map. Writers hold it only for the publish itself, so a
//	         reader never waits for an fsync.
type WALStore struct {
	opts Options
	dir  string

	writeMu sync.Mutex

	mu      sync.RWMutex
	data    map[string][]byte
	applied AppliedIndex
	closed  bool

	w        *wal.WAL
	recovery Recovery
}

var _ Store = (*WALStore)(nil)

// Recovery summarises what opening the store found in the log.
type Recovery struct {
	SegmentsScanned int
	BytesScanned    int64
	RecordsApplied  int64
	BatchesApplied  int64
	OpsApplied      int64
	KeysRecovered   int

	// Truncated reports that a torn tail was repaired. This is expected after
	// a crash and is surfaced rather than swallowed, because "the last 137
	// bytes of your log were discarded" is something an operator needs to be
	// told.
	Truncated       bool
	TruncatedFile   string
	TruncatedAt     int64
	TruncatedBytes  int64
	TruncatedReason string
}

// OpenWALStore opens or creates a durable store in dir.
//
// Recovery happens here: the WAL is replayed in full, a torn tail left by a
// crash is repaired, and any damage that cannot be explained by a crash causes
// the open to fail with ErrCorrupt rather than to succeed with a state that is
// silently missing writes.
func OpenWALStore(dir string, opts Options) (*WALStore, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if dir == "" {
		return nil, opErr("open", nil, fmt.Errorf("%w: data directory is empty", ErrInvalidOptions))
	}

	s := &WALStore{
		opts: opts,
		dir:  dir,
		data: make(map[string][]byte),
	}
	walDir := filepath.Join(dir, walDirName)

	rec, err := wal.Recover(walDir, wal.Handler{
		Batch: func(b wal.Batch) error {
			// Single-threaded here — the store is not published yet — but the
			// lock is taken anyway so that replay and the live write path go
			// through exactly one apply implementation.
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, op := range b {
				s.applyLocked(op)
			}
			return nil
		},
		Applied: func(a wal.AppliedIndex) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.applied = AppliedIndex{Index: a.Index, Term: a.Term}
			return nil
		},
	})
	if err != nil {
		return nil, classify("open", nil, err)
	}

	w, err := wal.Create(walDir, opts.WAL)
	if err != nil {
		return nil, classify("open", nil, err)
	}
	s.w = w
	s.recovery = Recovery{
		SegmentsScanned: rec.SegmentsScanned,
		BytesScanned:    rec.BytesScanned,
		RecordsApplied:  rec.RecordsApplied,
		BatchesApplied:  rec.BatchesApplied,
		OpsApplied:      rec.OpsApplied,
		KeysRecovered:   len(s.data),
		Truncated:       rec.Truncated,
		TruncatedFile:   rec.TruncatedFile,
		TruncatedAt:     rec.TruncatedAt,
		TruncatedBytes:  rec.TruncatedBytes,
		TruncatedReason: rec.TruncatedReason,
	}
	return s, nil
}

// applyLocked applies one operation to the in-memory map. The caller holds mu.
//
// This is the single apply implementation: replay and the live write path both
// go through it, so recovered state cannot drift from live state through two
// subtly different code paths.
func (s *WALStore) applyLocked(op wal.Op) {
	switch op.Kind {
	case wal.OpPut:
		s.data[string(op.Key)] = clone(op.Value)
	case wal.OpDelete:
		delete(s.data, string(op.Key))
	}
}

// Put implements Store.
func (s *WALStore) Put(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return opErr("put", key, err)
	}
	if err := s.opts.validateKey("put", key); err != nil {
		return err
	}
	if err := s.opts.validateValue("put", key, value); err != nil {
		return err
	}
	return s.mutate("put", wal.Op{Kind: wal.OpPut, Key: key, Value: value})
}

// Delete implements Store.
//
// As in Phase 1, Delete is idempotent and performs no read. It appends a
// tombstone unconditionally — which is not merely allowed here but is what the
// LSM engine will do literally, so the semantics carry forward unchanged.
func (s *WALStore) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return opErr("delete", key, err)
	}
	if err := s.opts.validateKey("delete", key); err != nil {
		return err
	}
	return s.mutate("delete", wal.Op{Kind: wal.OpDelete, Key: key})
}

// mutate performs the documented write path for a single operation.
func (s *WALStore) mutate(op string, o wal.Op) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// Close also holds writeMu, so the store cannot become closed between this
	// check and the publish below.
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return opErr(op, o.Key, ErrClosed)
	}

	if err := s.w.AppendBatch(wal.Batch{o}); err != nil {
		// Nothing is published. The log may hold a partial record, which
		// recovery will repair as a torn tail; either way the caller is told
		// the write did not succeed.
		return classify(op, o.Key, err)
	}

	s.mu.Lock()
	s.applyLocked(o)
	s.mu.Unlock()
	return nil
}

// Get implements Store.
func (s *WALStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, opErr("get", key, err)
	}
	if err := s.opts.validateKey("get", key); err != nil {
		return nil, err
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, opErr("get", key, ErrClosed)
	}
	v, ok := s.data[string(key)]
	if !ok {
		s.mu.RUnlock()
		return nil, opErr("get", key, ErrNotFound)
	}
	out := clone(v)
	s.mu.RUnlock()
	return out, nil
}

// SetAppliedIndex durably records applied-index metadata.
//
// It is ordered with respect to mutations: the record lands in the log after
// every write accepted before this call and before every write accepted after
// it, which is what makes it meaningful as a marker. See AppliedIndex for what
// it does and does not mean in Phase 2.
func (s *WALStore) SetAppliedIndex(ctx context.Context, a AppliedIndex) error {
	if err := ctx.Err(); err != nil {
		return opErr("set-applied-index", nil, err)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return opErr("set-applied-index", nil, ErrClosed)
	}

	if err := s.w.AppendAppliedIndex(wal.AppliedIndex{Index: a.Index, Term: a.Term}); err != nil {
		return classify("set-applied-index", nil, err)
	}

	s.mu.Lock()
	s.applied = a
	s.mu.Unlock()
	return nil
}

// AppliedIndex returns the last durably recorded applied index.
func (s *WALStore) AppliedIndex() AppliedIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// Sync flushes the WAL regardless of the configured sync mode.
//
// After it returns nil, every write acknowledged before the call is on stable
// storage to the extent described in docs/WAL.md — that is, as far as
// fsync/F_FULLFSYNC and the hardware's honesty allow.
func (s *WALStore) Sync() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return opErr("sync", nil, ErrClosed)
	}
	return classify("sync", nil, s.w.Sync())
}

// Close flushes and closes the store. It is idempotent.
func (s *WALStore) Close() error {
	// writeMu first, then mu — the invariant lock order. Holding writeMu means
	// no mutation can be between its log append and its publish.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.data = nil
	s.mu.Unlock()

	return classify("close", nil, s.w.Close())
}

// Len reports the number of live keys. Like MemStore.Len it exists for tests
// and metrics and is not part of the Store interface.
func (s *WALStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Dir returns the data directory.
func (s *WALStore) Dir() string { return s.dir }

// Recovery returns what opening the store found in the log.
func (s *WALStore) Recovery() Recovery { return s.recovery }

// WALStats returns the underlying WAL's current shape.
func (s *WALStore) WALStats() wal.Stats { return s.w.Stats() }
