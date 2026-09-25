package kv

import (
	"fmt"
	"sync"
)

// Store is the deterministic key-value state machine a Raft group replicates:
// an in-memory map applied from committed log entries in index order. It
// implements replication.StateMachine (raftnode.StateMachine). Its semantics are
// exactly the Phase 1 storage contract (docs/INVARIANTS.md INV-A1..A5): keys are
// opaque bytes compared bytewise; an empty value is a present key; DELETE is
// idempotent and reports nothing; Get returns a fresh copy. Apply is called by
// the node's actor goroutine and Get by client goroutines, so the map is
// guarded by a mutex; the state itself is purely a function of the applied
// commands, which is what lets every replica hold byte-identical state.
//
// The state is volatile: a fresh Store is empty, and a restarted node rebuilds
// it by re-applying its recovered committed prefix (docs/CRASH_RECOVERY.md §6).
type Store struct {
	mu      sync.RWMutex
	m       map[string][]byte
	applied uint64 // highest index applied, for observability
}

// NewStore returns an empty store.
func NewStore() *Store { return &Store{m: map[string][]byte{}} }

// Apply applies one committed log entry. A nil or empty command (the election
// no-op) applies nothing; a command that does not decode is refused with
// ErrMalformedCommand and the store is unchanged.
func (s *Store) Apply(index uint64, command []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(command) == 0 {
		s.applied = index
		return nil
	}
	c, err := Decode(command)
	if err != nil {
		return fmt.Errorf("kv: apply index %d: %w", index, err)
	}
	switch c.Op {
	case OpPut:
		s.m[string(c.Key)] = append([]byte{}, c.Value...)
	case OpDelete:
		delete(s.m, string(c.Key))
	}
	s.applied = index
	return nil
}

// Get returns a copy of the value under key and whether the key is present. An
// empty value is present (ok true, zero-length non-nil slice).
func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte{}, v...), true
}

// Applied returns the highest index applied to this store.
func (s *Store) Applied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// Len returns the number of present keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// Snapshot returns a copy of the whole map (tests compare replicas with it).
func (s *Store) Snapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]byte, len(s.m))
	for k, v := range s.m {
		out[k] = append([]byte{}, v...)
	}
	return out
}
