package storage

import (
	"context"
	"sync"
)

// MemStore is an in-memory Store. It is the Phase 1 implementation and it is
// not durable: everything it holds is lost when the process exits. Phase 2
// adds a write-ahead log and Phase 3 replaces this type with the LSM engine.
//
// Concurrency design: a single sync.RWMutex guards the map and the closed flag.
// Reads take the read lock and so proceed in parallel; writes are exclusive.
// This is the simplest structure that satisfies the atomicity contract on
// Store, and "simplest thing that is provably correct" is the right trade here
// — a sharded or lock-free map would buy throughput this phase cannot measure
// against anything, at the cost of a much harder correctness argument. If the
// Phase 5 benchmarks show the lock dominating, that is the moment to revisit,
// with a number in hand.
type MemStore struct {
	opts Options

	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// compile-time assertion that MemStore satisfies the interface.
var _ Store = (*MemStore)(nil)

// NewMemStore creates an empty in-memory store.
func NewMemStore(opts Options) (*MemStore, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	return &MemStore{
		opts: opts,
		data: make(map[string][]byte),
	}, nil
}

// Put implements Store.
func (s *MemStore) Put(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return opErr("put", key, err)
	}
	if err := s.opts.validateKey("put", key); err != nil {
		return err
	}
	if err := s.opts.validateValue("put", key, value); err != nil {
		return err
	}

	// Copy before taking the lock: the caller's slice must not be retained,
	// and the copy does not need mutual exclusion.
	stored := clone(value)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return opErr("put", key, ErrClosed)
	}
	s.data[string(key)] = stored
	return nil
}

// Get implements Store.
func (s *MemStore) Get(ctx context.Context, key []byte) ([]byte, error) {
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
	// m[string(byteSlice)] is recognised by the compiler and does not allocate
	// a string for the lookup.
	v, ok := s.data[string(key)]
	if !ok {
		s.mu.RUnlock()
		return nil, opErr("get", key, ErrNotFound)
	}
	// Copy while still holding the read lock. Doing it after unlocking would
	// race with a concurrent Put that replaced the map entry.
	out := clone(v)
	s.mu.RUnlock()
	return out, nil
}

// Delete implements Store.
func (s *MemStore) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return opErr("delete", key, err)
	}
	if err := s.opts.validateKey("delete", key); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return opErr("delete", key, ErrClosed)
	}
	// Idempotent by construction: deleting an absent map entry is a no-op and
	// the existence of the key is never consulted. See the Store.Delete doc
	// comment for why this is required rather than merely convenient.
	delete(s.data, string(key))
	return nil
}

// Close implements Store. It is idempotent.
func (s *MemStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.data = nil
	return nil
}

// Len reports the number of keys currently stored. It exists for tests and for
// the Phase 16 metrics; it is not part of the Store interface because an LSM
// engine cannot answer it without a full scan.
func (s *MemStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
