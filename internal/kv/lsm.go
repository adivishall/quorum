package kv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/vfs"
)

// LSMMachine is the replicated state machine held in the storage engine (S2,
// docs/STORAGE_INTEGRATION.md §8). It decides every command exactly as Store
// does — the two share sessionTable, the one decision function — and differs
// only in where the decisions' effects go: the user writes and the session
// records a cycle's entries change are staged, and EndCycle records them with
// the applied index of the cycle's last entry as ONE engine apply batch, S1's
// recovery unit. Raft decides the order; the engine makes the result of
// applying it durable; nothing here invents an order.
//
// The session table is mirrored in memory, because decisions read it on every
// identified command, and loaded from the engine's session records at open;
// the engine's copy is the durable one. A cycle's staged writes are visible to
// Lookup before they are recorded (a read released by its barrier mid-cycle
// must see the entry it waited for), under the same mutex the staging holds.
//
// Recovery (§8.6): the engine's applied index is the durable authority. An
// entry at or below it is skipped by ApplyEntry — its effects are in the
// engine, and applying them again would reset a session or make a request a
// duplicate of itself — and a published snapshot below it changes nothing.
type LSMMachine struct {
	dir  string
	opts storage.Options

	mu       sync.RWMutex
	db       *storage.LSMStore
	sessions sessionTable
	engine   storage.AppliedIndex // what the engine holds, durable as its sync mode says
	applied  uint64               // the newest index applied: staged, recorded or recovered
	closed   bool

	// The cycle in progress: what ApplyEntry staged since the last EndCycle.
	staged      []storage.Mutation
	stagedBytes int
	overlay     map[string]overlayValue // user key -> this cycle's last write to it
	touched     map[uint64]bool         // sessions whose records changed
	evicted     map[uint64]bool         // sessions evicted
	last        storage.AppliedIndex    // the last staged entry; Index 0: nothing staged
}

// overlayValue is a staged write to a user key: a value, or a deletion.
type overlayValue struct {
	value   []byte
	present bool
}

// cycleFlushBytes bounds a batch: a cycle whose staged writes reach it is
// recorded in more than one batch, at entry boundaries, so a catch-up of many
// large entries never exceeds one WAL record (64 MiB). With entries of at most
// 1 MiB and every session record at most ~6 KiB, a batch stays well below it.
const cycleFlushBytes = 32 << 20

// ErrMachineClosed is returned by every operation of a closed machine.
var ErrMachineClosed = errors.New("kv: the state machine is closed")

var _ Machine = (*LSMMachine)(nil)

// OpenLSMMachine opens the machine's engine in dir (creating it) under limits
// — the same limits every replica of the group uses — and engine options opts,
// whose key and value limits are set here: a user key is stored one tag byte
// longer. It loads the session table from the engine's records and refuses a
// record it did not write (ErrSessionRecord).
func OpenLSMMachine(dir string, limits Limits, opts storage.Options) (*LSMMachine, error) {
	if limits.MaxSessions < 1 || limits.MaxUnacked < 1 {
		return nil, errors.New("kv: session limits must be at least 1")
	}
	opts.MaxKeySize = MaxKeyLen + 1
	opts.MaxValueSize = MaxValueLen
	db, err := storage.OpenLSMStore(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("kv: opening the engine in %s: %w", dir, err)
	}
	m := &LSMMachine{dir: dir, opts: opts, db: db, sessions: newSessionTable(limits)}
	if err := m.loadSessions(); err != nil {
		_ = db.Close()
		return nil, err
	}
	m.engine = db.AppliedIndex()
	m.applied = m.engine.Index
	m.resetCycle()
	return m, nil
}

// loadSessions rebuilds the session mirror from the engine's session records.
// It reads the engine's whole state (S2: the engine has no range scan yet,
// docs/STORAGE_INTEGRATION.md §8.8); the records themselves are few and small.
func (m *LSMMachine) loadSessions() error {
	all, err := m.db.Snapshot()
	if err != nil {
		return fmt.Errorf("kv: reading the engine's session records: %w", err)
	}
	for k, v := range all {
		id, ok := parseSessionKey([]byte(k))
		if !ok {
			continue
		}
		ss, err := decodeSession(id, v, m.sessions.limits)
		if err != nil {
			return err
		}
		m.sessions.sessions[id] = ss
	}
	if n := len(m.sessions.sessions); n > m.sessions.limits.MaxSessions {
		return fmt.Errorf("%w: %d sessions recorded, limit %d", ErrSessionRecord, n, m.sessions.limits.MaxSessions)
	}
	return nil
}

func (m *LSMMachine) resetCycle() {
	m.staged, m.stagedBytes = nil, 0
	m.overlay = map[string]overlayValue{}
	m.touched, m.evicted = map[uint64]bool{}, map[uint64]bool{}
	m.last = storage.AppliedIndex{}
}

// Dir returns the engine's directory.
func (m *LSMMachine) Dir() string { return m.dir }

// EngineApplied returns the applied index the engine holds (tests and the
// operator read it; the cycle in progress may be above it).
func (m *LSMMachine) EngineApplied() storage.AppliedIndex {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.engine
}

// ApplyEntry decides one committed entry and stages its effects
// (raftnode.CycleStateMachine). An entry at or below the engine's applied
// index is already in the engine and is skipped, with no result. A nil or
// empty command (the election no-op, a configuration entry) stages nothing;
// the cycle still advances the index. A command that does not decode is
// refused with ErrMalformedCommand, as Store refuses it.
func (m *LSMMachine) ApplyEntry(index, term uint64, command []byte) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrMachineClosed
	}
	if index <= m.engine.Index {
		return nil, nil
	}
	if m.stagedBytes >= cycleFlushBytes {
		if err := m.flushLocked(); err != nil {
			return nil, err
		}
	}
	if len(command) == 0 {
		m.last, m.applied = storage.AppliedIndex{Index: index, Term: term}, index
		return nil, nil
	}
	c, err := Decode(command)
	if err != nil {
		return nil, fmt.Errorf("kv: apply index %d: %w", index, err)
	}
	m.last, m.applied = storage.AppliedIndex{Index: index, Term: term}, index
	r, ef := m.sessions.decide(index, c)
	m.sessions.count(r.Decision)
	if r.Decision == Executed {
		m.stageWrite(c)
	}
	if ef.touched != 0 {
		m.touched[ef.touched] = true
	}
	for _, id := range ef.evicted {
		m.evicted[id] = true
		delete(m.touched, id)
	}
	return r, nil
}

// stageWrite stages an executed command's write. Decode handed over fresh
// copies of the key and value, so nothing aliases the entry.
func (m *LSMMachine) stageWrite(c Command) {
	ek := userKey(c.Key)
	switch c.Op {
	case OpPut:
		m.staged = append(m.staged, storage.Mutation{Kind: storage.MutationPut, Key: ek, Value: c.Value})
		m.overlay[string(c.Key)] = overlayValue{value: c.Value, present: true}
		m.stagedBytes += 2 + 10 + len(ek) + 10 + len(c.Value)
	case OpDelete:
		m.staged = append(m.staged, storage.Mutation{Kind: storage.MutationDelete, Key: ek})
		m.overlay[string(c.Key)] = overlayValue{}
		m.stagedBytes += 2 + 10 + len(ek)
	}
}

// EndCycle records the cycle's staged effects and its applied index as one
// engine apply batch (raftnode.CycleStateMachine). A failure leaves them
// staged and the machine's mirror ahead of its engine: the node fail-stops on
// it, and a restart reads the engine back.
func (m *LSMMachine) EndCycle() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrMachineClosed
	}
	return m.flushLocked()
}

// flushLocked is EndCycle under the lock. The batch is the staged user writes
// in entry order, then the record of every session touched (as the mirror
// holds it now) and the deletion of every session evicted, in id order.
func (m *LSMMachine) flushLocked() error {
	if m.last.Index == 0 {
		return nil
	}
	muts := m.staged
	for _, id := range sortedIDs(m.touched) {
		ss := m.sessions.sessions[id]
		if ss == nil {
			return fmt.Errorf("kv: session %d touched this cycle is not in the table", id)
		}
		muts = append(muts, storage.Mutation{Kind: storage.MutationPut, Key: sessionKey(id), Value: encodeSession(ss)})
	}
	for _, id := range sortedIDs(m.evicted) {
		muts = append(muts, storage.Mutation{Kind: storage.MutationDelete, Key: sessionKey(id)})
	}
	if err := m.db.Apply(context.Background(), muts, m.last); err != nil {
		return fmt.Errorf("kv: recording the cycle through index %d: %w", m.last.Index, err)
	}
	m.engine = m.last
	m.resetCycle()
	return nil
}

func sortedIDs(set map[uint64]bool) []uint64 {
	ids := make([]uint64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Apply applies one entry as a cycle of its own (replication.StateMachine),
// with the term of the newest entry the machine knows: a caller that has the
// entry's term — raftnode does — uses ApplyEntry and EndCycle.
func (m *LSMMachine) Apply(index uint64, command []byte) error {
	m.mu.RLock()
	term := max(m.last.Term, m.engine.Term, 1)
	m.mu.RUnlock()
	if _, err := m.ApplyEntry(index, term, command); err != nil {
		return err
	}
	return m.EndCycle()
}

// Lookup returns a copy of the value under key and whether the key is present
// (Machine): a write staged this cycle, else what the engine holds. An engine
// failure is reported, never presented as absence (INV-L7).
func (m *LSMMachine) Lookup(key []byte) ([]byte, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, false, ErrMachineClosed
	}
	if ov, ok := m.overlay[string(key)]; ok {
		if !ov.present {
			return nil, false, nil
		}
		return append([]byte{}, ov.value...), true, nil
	}
	v, err := m.db.Get(context.Background(), userKey(key))
	switch {
	case err == nil:
		return v, true, nil
	case errors.Is(err, storage.ErrNotFound):
		return nil, false, nil
	default:
		return nil, false, err
	}
}

// Applied returns the newest index applied: staged, recorded or recovered.
func (m *LSMMachine) Applied() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.applied
}

// Sessions returns a copy of the session table.
func (m *LSMMachine) Sessions() map[uint64]SessionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions.snapshot()
}

// Stats returns the decision counters.
func (m *LSMMachine) Stats() ApplyStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions.stats
}

func (m *LSMMachine) setObserve(observe func(Decision), evicted func()) {
	m.mu.Lock()
	m.sessions.observe, m.sessions.observeEvicted = observe, evicted
	m.mu.Unlock()
}

// Contents returns a copy of every present user key: the engine's, with this
// cycle's staged writes applied, as Lookup would answer. It reads the engine's
// whole state.
func (m *LSMMachine) Contents() (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrMachineClosed
	}
	return m.contentsLocked()
}

func (m *LSMMachine) contentsLocked() (map[string][]byte, error) {
	all, err := m.db.Snapshot()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(all))
	for k, v := range all {
		if len(k) > 0 && k[0] == tagUser {
			out[k[1:]] = v
		}
	}
	for k, ov := range m.overlay {
		if ov.present {
			out[k] = append([]byte{}, ov.value...)
		} else {
			delete(out, k)
		}
	}
	return out, nil
}

// EncodeSnapshot returns the state at the engine's applied index, in the
// snapshot format Store writes (raftnode.SnapshotStateMachine). The driver
// calls it between cycles, so nothing is staged and the engine's index is the
// state's.
func (m *LSMMachine) EncodeSnapshot() (uint64, []byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return 0, nil, ErrMachineClosed
	}
	if m.last.Index != 0 {
		return 0, nil, fmt.Errorf("kv: a snapshot inside a cycle (entries through %d staged)", m.last.Index)
	}
	user, err := m.contentsLocked()
	if err != nil {
		return 0, nil, err
	}
	return m.engine.Index, encodeState(m.engine.Index, m.sessions.limits, user, m.sessions.sessions), nil
}

// ValidateSnapshot reports whether RestoreSnapshot would accept data as the
// state at index: Store's rules, and that the state fits one engine apply
// batch — the one record a restore is (S2; S4's ingest lifts it).
func (m *LSMMachine) ValidateSnapshot(index uint64, data []byte) error {
	_, err := m.checkSnapshot(index, data)
	return err
}

func (m *LSMMachine) checkSnapshot(index uint64, data []byte) (*state, error) {
	st, err := decodeState(data, m.sessions.limits)
	if err != nil {
		return nil, err
	}
	if st.applied != index {
		return nil, fmt.Errorf("%w: state at index %d, snapshot at %d", ErrSnapshotState, st.applied, index)
	}
	if n := restoreBatchBytes(st); n > maxRestoreBytes {
		return nil, fmt.Errorf("%w: a state of about %d bytes exceeds the %d the LSM machine installs as one batch (S2)", ErrSnapshotState, n, maxRestoreBytes)
	}
	return st, nil
}

// maxRestoreBytes is what a restore's batch may encode to: one WAL record,
// less room for the record's own framing and the estimate's slack.
const maxRestoreBytes = record.MaxRecordSize - 1<<20

// restoreBatchBytes bounds the batch a state restores as, from above.
func restoreBatchBytes(st *state) int {
	n := 64
	for k, v := range st.m {
		n += 1 + 10 + 1 + len(k) + 10 + len(v)
	}
	for _, ss := range st.sessions {
		n += 1 + 10 + sessionKeyLen + 10 + 1 + 16 + 10 + len(ss.results)*sessionResultLen
	}
	return n
}

// RestoreSnapshot replaces the state with a snapshot's, taken at (index, term)
// (raftnode.SnapshotStateMachine). If the engine's applied index is at or
// above index, the engine's state is at least as new, and nothing changes.
// Otherwise the engine is replaced: closed, its directory removed, opened
// fresh, and the whole state — every user key, every session record — recorded
// as ONE apply batch at (index, term). Under R1 that is atomic: after a crash
// the engine holds the batch whole, or is empty at index 0, below the published
// snapshot, which the next open restores again.
func (m *LSMMachine) RestoreSnapshot(index, term uint64, data []byte) error {
	st, err := m.checkSnapshot(index, data)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrMachineClosed
	}
	if m.engine.Index >= index {
		return nil
	}
	if m.last.Index != 0 {
		return fmt.Errorf("kv: a restore inside a cycle (entries through %d staged)", m.last.Index)
	}
	if err := m.db.Close(); err != nil {
		return fmt.Errorf("kv: closing the engine to restore: %w", err)
	}
	if err := os.RemoveAll(m.dir); err != nil {
		return fmt.Errorf("kv: removing the engine to restore: %w", err)
	}
	db, err := storage.OpenLSMStore(m.dir, m.opts)
	if err != nil {
		return fmt.Errorf("kv: reopening the engine to restore: %w", err)
	}
	m.db = db
	keys := make([]string, 0, len(st.m))
	for k := range st.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	muts := make([]storage.Mutation, 0, len(keys)+len(st.sessions))
	for _, k := range keys {
		muts = append(muts, storage.Mutation{Kind: storage.MutationPut, Key: userKey([]byte(k)), Value: st.m[k]})
	}
	ids := make([]uint64, 0, len(st.sessions))
	for id := range st.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		muts = append(muts, storage.Mutation{Kind: storage.MutationPut, Key: sessionKey(id), Value: encodeSession(st.sessions[id])})
	}
	applied := storage.AppliedIndex{Index: index, Term: term}
	if err := db.Apply(context.Background(), muts, applied); err != nil {
		return fmt.Errorf("kv: recording the restored state at %d: %w", index, err)
	}
	// The new directory's entry replaces the old one's in the parent; a
	// restored state the engine fsynced is not durable until that is.
	if err := (vfs.OS{}).SyncDir(filepath.Dir(filepath.Clean(m.dir))); err != nil {
		return fmt.Errorf("kv: syncing the engine's parent after a restore: %w", err)
	}
	m.sessions.sessions, m.sessions.stats = st.sessions, ApplyStats{}
	m.engine, m.applied = applied, index
	m.resetCycle()
	return nil
}

// Close closes the engine. It is idempotent.
func (m *LSMMachine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	return m.db.Close()
}
