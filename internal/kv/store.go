package kv

import (
	"fmt"
	"sort"
	"sync"
)

// Store is the deterministic key-value state machine a Raft group replicates:
// an in-memory map applied from committed log entries in index order, and —
// since Phase 13 — the session table that makes identified writes happen at
// most once (docs/DEDUP.md). It implements replication.StateMachine and
// raftnode.ResultStateMachine. Its key-value semantics are exactly the Phase 1
// storage contract (INV-A1..A5): keys are opaque bytes compared bytewise; an
// empty value is a present key; DELETE is idempotent and reports nothing; Get
// returns a fresh copy. Apply is called by the node's actor goroutine and Get by
// client goroutines, so the state is guarded by a mutex; the state itself —
// map AND session table — is purely a function of the applied commands, which
// is what lets every replica hold byte-identical state.
//
// The state is volatile: a fresh Store is empty, and a restarted node rebuilds
// it — session table included — by re-applying its recovered committed prefix
// (docs/CRASH_RECOVERY.md §6, docs/DEDUP.md §4).
type Store struct {
	mu       sync.RWMutex
	m        map[string][]byte
	applied  uint64 // highest index applied, for observability
	sessions sessionTable
}

// sessionTable is the session half of the replicated state: every session's
// recency, watermark and remembered results, the limits that bound it, and the
// decision function over it (docs/CLIENT_SEMANTICS.md §4). Both machines hold
// one — it is the one decision function (S2, docs/STORAGE_INTEGRATION.md §8.2)
// — and differ only in where they record what it decided: the in-memory store
// applies an executed command to its map at once; the LSM machine stages the
// command's write and the session records the decision changed, and records
// them with the cycle.
type sessionTable struct {
	limits   Limits
	sessions map[uint64]*session
	stats    ApplyStats

	// Phase 16: counters outside the replicated state (Metrics.Observe); a
	// restore does not reset them. Nil: none.
	observe        func(Decision)
	observeEvicted func()
}

// effect is what deciding one command changed in the table, so a durable
// machine can record exactly that: the session whose record changed (0: none)
// and the sessions evicted.
type effect struct {
	touched uint64
	evicted []uint64
}

func newSessionTable(l Limits) sessionTable {
	return sessionTable{limits: l, sessions: map[uint64]*session{}}
}

// setObserve installs the decision counters (Machine).
func (s *Store) setObserve(observe func(Decision), evicted func()) {
	s.mu.Lock()
	s.sessions.observe, s.sessions.observeEvicted = observe, evicted
	s.mu.Unlock()
}

// Limits bound the session table (docs/CLIENT_SEMANTICS.md §8). They are part
// of the state machine's definition: every replica of a group MUST use the same
// limits, or their decisions — and so their states — would diverge.
type Limits struct {
	MaxSessions int // sessions kept; registering one more evicts the least recently used
	MaxUnacked  int // remembered results per session; a new request beyond it is refused
}

// DefaultLimits are the limits dkvd runs with: at most 1024 sessions × 128
// unacknowledged results, each result a 32-byte fingerprint and an index —
// a worst case of a few MiB (docs/DEDUP.md §6).
var DefaultLimits = Limits{MaxSessions: 1024, MaxUnacked: 128}

// session is one client session: the log index of its last command (its
// recency, for eviction), its acknowledgement watermark, and the results of its
// executed requests at or above the watermark.
type session struct {
	last       uint64
	ackedBelow uint64
	results    map[uint64]execution
}

// execution is what the table remembers of an executed request: which command it
// was, and where it took effect. The command's result needs no storing — every
// executed PUT or DELETE succeeds.
type execution struct {
	fp    [32]byte
	index uint64
}

// Decision is what applying one command did (docs/CLIENT_SEMANTICS.md §4).
type Decision uint8

const (
	Executed   Decision = iota + 1 // a new request (or an anonymous write) changed the state
	Duplicate                      // the request had executed with the same command: no change
	Conflict                       // the request id had executed with a different command: no change
	Stale                          // below the session's AckedBelow: no change
	Expired                        // no such session (never registered, or evicted): no change
	Limit                          // the session holds MaxUnacked results: no change
	Registered                     // a new session, whose id is the entry's index
)

var decisionNames = map[Decision]string{Executed: "executed", Duplicate: "duplicate", Conflict: "conflict",
	Stale: "stale", Expired: "expired", Limit: "limit", Registered: "registered"}

func (d Decision) String() string {
	if s, ok := decisionNames[d]; ok {
		return s
	}
	return fmt.Sprintf("decision(%d)", d)
}

// Result is what applying one committed command produced — handed to the
// client whose proposal it was (raftnode.ResultStateMachine). Index is the log
// index of the execution it reports: this entry's for Executed, the ORIGINAL
// execution's for Duplicate, and for Registered the new session id.
type Result struct {
	Decision Decision
	Index    uint64
}

// ApplyStats counts decisions, for observability and tests.
type ApplyStats struct {
	Executed, Duplicate, Conflict, Stale, Expired, Limit, Registered, Evicted int
}

// NewStore returns an empty store with DefaultLimits.
func NewStore() *Store { return NewStoreWithLimits(DefaultLimits) }

// NewStoreWithLimits returns an empty store with the given limits (tests use
// small ones to exercise eviction). Both limits must be at least 1.
func NewStoreWithLimits(l Limits) *Store {
	if l.MaxSessions < 1 || l.MaxUnacked < 1 {
		panic("kv: session limits must be at least 1")
	}
	return &Store{m: map[string][]byte{}, sessions: newSessionTable(l)}
}

// Apply applies one committed log entry (replication.StateMachine). A nil or
// empty command (the election no-op) applies nothing; a command that does not
// decode is refused with ErrMalformedCommand and the store is unchanged.
func (s *Store) Apply(index uint64, command []byte) error {
	_, err := s.ApplyResult(index, command)
	return err
}

// ApplyResult applies one committed log entry and returns its Result (nil for
// the no-op). Every decision is a function of the command and the store's state
// alone, so every replica decides identically (docs/CLIENT_SEMANTICS.md §4):
//
//	REGISTER                         → Registered: session id = index; LRU eviction beyond MaxSessions
//	anonymous PUT/DELETE             → Executed
//	identified, no such session      → Expired
//	identified: first raise the session's AckedBelow to min(AckedBelow, RequestID)
//	  and forget results below it; then
//	  RequestID < AckedBelow         → Stale
//	  result recorded, same command  → Duplicate (the original index)
//	  result recorded, other command → Conflict
//	  MaxUnacked results held        → Limit
//	  otherwise                      → Executed, and recorded
//
// Any identified command naming an existing session marks it used at index.
func (s *Store) ApplyResult(index uint64, command []byte) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(command) == 0 {
		s.applied = index
		return nil, nil
	}
	c, err := Decode(command)
	if err != nil {
		return nil, fmt.Errorf("kv: apply index %d: %w", index, err)
	}
	s.applied = index
	r, _ := s.sessions.decide(index, c)
	s.sessions.count(r.Decision)
	if r.Decision == Executed {
		s.write(c)
	}
	return r, nil
}

// decide decides one command against the table and applies the decision's
// session-table effects — a session registered (and the least recently used
// evicted beyond the limit), marked used, its watermark raised and results
// below it forgotten, a result recorded — returning the Result and what it
// changed. An Executed result means the caller must apply the command's write;
// nothing else changes the data. Every replica, decided from the same command
// and table, decides the same.
func (t *sessionTable) decide(index uint64, c Command) (Result, effect) {
	if c.Op == OpRegister {
		t.sessions[index] = &session{last: index, ackedBelow: 1, results: map[uint64]execution{}}
		var ef effect
		ef.touched = index
		for len(t.sessions) > t.limits.MaxSessions {
			var lru uint64
			for id, ss := range t.sessions {
				if lru == 0 || ss.last < t.sessions[lru].last {
					lru = id
				}
			}
			delete(t.sessions, lru)
			ef.evicted = append(ef.evicted, lru)
			t.stats.Evicted++
			if t.observeEvicted != nil {
				t.observeEvicted()
			}
		}
		return Result{Decision: Registered, Index: index}, ef
	}
	if c.ClientID == 0 {
		return Result{Decision: Executed, Index: index}, effect{}
	}
	ss := t.sessions[c.ClientID]
	if ss == nil {
		return Result{Decision: Expired}, effect{}
	}
	ef := effect{touched: c.ClientID}
	ss.last = index
	w := c.AckedBelow
	if w > c.RequestID {
		w = c.RequestID // Validate forbids it; clamp so a replica never depends on that
	}
	if w > ss.ackedBelow {
		ss.ackedBelow = w
		for rid := range ss.results {
			if rid < w {
				delete(ss.results, rid)
			}
		}
	}
	if c.RequestID < ss.ackedBelow {
		return Result{Decision: Stale}, ef
	}
	fp := c.Fingerprint()
	if rec, ok := ss.results[c.RequestID]; ok {
		if rec.fp == fp {
			return Result{Decision: Duplicate, Index: rec.index}, ef
		}
		return Result{Decision: Conflict}, ef
	}
	if len(ss.results) >= t.limits.MaxUnacked {
		return Result{Decision: Limit}, ef
	}
	ss.results[c.RequestID] = execution{fp: fp, index: index}
	return Result{Decision: Executed, Index: index}, ef
}

// count counts a decision into the stats and the observer.
func (t *sessionTable) count(d Decision) {
	if t.observe != nil {
		t.observe(d)
	}
	switch d {
	case Executed:
		t.stats.Executed++
	case Duplicate:
		t.stats.Duplicate++
	case Conflict:
		t.stats.Conflict++
	case Stale:
		t.stats.Stale++
	case Expired:
		t.stats.Expired++
	case Limit:
		t.stats.Limit++
	case Registered:
		t.stats.Registered++
	}
}

// snapshot copies the table, for Sessions.
func (t *sessionTable) snapshot() map[uint64]SessionState {
	out := make(map[uint64]SessionState, len(t.sessions))
	for id, ss := range t.sessions {
		st := SessionState{Last: ss.last, AckedBelow: ss.ackedBelow}
		for rid := range ss.results {
			st.Requests = append(st.Requests, rid)
		}
		sort.Slice(st.Requests, func(i, j int) bool { return st.Requests[i] < st.Requests[j] })
		out[id] = st
	}
	return out
}

func (s *Store) write(c Command) {
	switch c.Op {
	case OpPut:
		s.m[string(c.Key)] = append([]byte{}, c.Value...)
	case OpDelete:
		delete(s.m, string(c.Key))
	}
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

// Lookup is Get (Machine); the in-memory store cannot fail to read.
func (s *Store) Lookup(key []byte) ([]byte, bool, error) {
	v, ok := s.Get(key)
	return v, ok, nil
}

// Contents is Snapshot (Machine).
func (s *Store) Contents() (map[string][]byte, error) { return s.Snapshot(), nil }

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

// SessionState is one session as the table holds it, for diffing replicas and
// the reference model.
type SessionState struct {
	Last       uint64
	AckedBelow uint64
	Requests   []uint64 // remembered request ids, ascending
}

// Sessions returns a copy of the session table.
func (s *Store) Sessions() map[uint64]SessionState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions.snapshot()
}

// Stats returns the decision counters.
func (s *Store) Stats() ApplyStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions.stats
}
