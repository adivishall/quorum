package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/storage"
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

var (
	_ Machine                       = (*LSMMachine)(nil)
	_ raftnode.DurableApplier       = (*LSMMachine)(nil)
	_ raftnode.SnapshotStateMachine = (*LSMMachine)(nil)
)

// ErrLimits is a session limit the LSM machine cannot run under: a MaxUnacked
// whose fullest session record would not fit one engine value.
var ErrLimits = errors.New("kv: session limits the engine cannot store")

// OpenLSMMachine opens the machine's engine in dir (creating it) under limits
// — the same limits every replica of the group uses — and engine options opts,
// whose key and value limits are set here: a user key is stored one tag byte
// longer. It loads the session table from the engine's records and refuses a
// record it did not write (ErrSessionRecord), or a key under no tag it writes.
//
// It refuses a MaxUnacked whose fullest session record would not fit one
// engine value (ErrLimits): the machine could run until a session filled up,
// then fail every cycle that flushes it — and every restart, replaying the
// same entry (S4 closeout review; the in-memory store has no such bound). At
// the 1 MiB value limit, 21,844 results fit.
func OpenLSMMachine(dir string, limits Limits, opts storage.Options) (*LSMMachine, error) {
	if limits.MaxSessions < 1 || limits.MaxUnacked < 1 {
		return nil, errors.New("kv: session limits must be at least 1")
	}
	opts.MaxKeySize = MaxKeyLen + 1
	opts.MaxValueSize = MaxValueLen
	if n := maxSessionRecordLen(limits); n > opts.MaxValueSize {
		return nil, fmt.Errorf("%w: a session may hold %d results, a record of %d bytes; the engine stores values of at most %d bytes (%d results fit)",
			ErrLimits, limits.MaxUnacked, n, opts.MaxValueSize, maxUnackedFor(opts.MaxValueSize))
	}
	// A restore that a crash interrupted may have left the old engine renamed
	// aside (RestoreSnapshot); it is never read again.
	if err := removeAll(asidePath(dir)); err != nil {
		return nil, fmt.Errorf("kv: removing the engine a restore set aside: %w", err)
	}
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

// loadSessions rebuilds the session mirror from the engine's session records,
// scanning the engine one entry at a time (S4: never its whole state in a
// map); the records themselves are few and small. A key under no tag the
// machine writes is refused here, as the snapshot source refuses it — not at
// the first snapshot, where it would fail the node at every start.
func (m *LSMMachine) loadSessions() error {
	err := m.db.Scan(func(k, v []byte) error {
		if len(k) > 0 && k[0] == tagUser {
			return nil
		}
		id, ok := parseSessionKey(k)
		if !ok {
			return fmt.Errorf("%w: an engine key %q under no tag the machine writes", ErrSessionRecord, k)
		}
		ss, err := decodeSession(id, v, m.sessions.limits)
		if err != nil {
			return err
		}
		m.sessions.sessions[id] = ss
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrSessionRecord) {
			return err
		}
		return fmt.Errorf("kv: reading the engine's session records: %w", err)
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

// DurableAppliedIndex returns the applied index the engine recovers after a
// crash without the Raft log (raftnode.DurableApplier, R2): the engine's flushed
// applied index, which the Raft log is never compacted past. A closed machine
// vouches for nothing and reports 0 (compact nothing).
func (m *LSMMachine) DurableAppliedIndex() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return 0
	}
	return m.db.DurableApplied().Index
}

// SyncApplied flushes the engine so its durable applied index reaches the
// applied index (raftnode.DurableApplier, R2). The driver calls it when creating
// a snapshot, so the log may be compacted behind that snapshot. The driver calls
// it between cycles, so nothing is staged and the engine's applied index is the
// machine's. The engine's Flush records that index even when its memtable is
// empty — a cycle of election no-ops and configuration entries advances it
// without a write.
func (m *LSMMachine) SyncApplied() error {
	m.mu.RLock()
	db, closed := m.db, m.closed
	m.mu.RUnlock()
	if closed {
		return ErrMachineClosed
	}
	return db.Flush()
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
// snapshot format Store writes: the snapshot source's bytes (OpenSnapshot),
// gathered in memory. It is for tests and small states; the driver streams.
func (m *LSMMachine) EncodeSnapshot() (uint64, []byte, error) {
	src, err := m.OpenSnapshot()
	if err != nil {
		return 0, nil, err
	}
	defer src.Close()
	var buf bytes.Buffer
	if _, err := src.WriteTo(&buf); err != nil {
		return 0, nil, err
	}
	return src.Index(), buf.Bytes(), nil
}

// OpenSnapshot returns the state at the engine's applied index as a snapshot
// source (raftnode.SnapshotStateMachine, S4, docs/STORAGE_INTEGRATION.md
// §10.3–§10.4): an engine checkpoint — the engine flushed and pinned at its
// durable applied index — which WriteTo streams in the snapshot format Store
// writes, byte for byte, without the state in memory. The driver calls it
// between cycles, so nothing is staged and the engine's index is the state's.
//
// The format's header needs the key count, so opening the source makes one
// counting pass over the checkpoint, and checks on it that every engine key
// is a user key or a session record the machine wrote and that the session
// records are exactly the machine's session table: a checkpoint that is not
// the machine's state is never published.
func (m *LSMMachine) OpenSnapshot() (raftnode.SnapshotSource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrMachineClosed
	}
	if m.last.Index != 0 {
		return nil, fmt.Errorf("kv: a snapshot inside a cycle (entries through %d staged)", m.last.Index)
	}
	ck, err := m.db.Checkpoint()
	if err != nil {
		return nil, fmt.Errorf("kv: checkpointing the engine: %w", err)
	}
	src := &lsmSource{ck: ck, limits: m.sessions.limits}
	if a := ck.Applied(); a != m.engine {
		_ = ck.Close()
		return nil, fmt.Errorf("kv: the engine's checkpoint is at %+v, the machine at %+v", a, m.engine)
	}
	it := ck.Iterator()
	for it.Next() {
		k := it.Key()
		switch {
		case len(k) > 0 && k[0] == tagUser:
			src.nKeys++
		default:
			id, ok := parseSessionKey(k)
			if !ok {
				_ = ck.Close()
				return nil, fmt.Errorf("%w: an engine key %q under no tag the machine writes", ErrSessionRecord, k)
			}
			ss, err := decodeSession(id, it.Value(), m.sessions.limits)
			if err != nil {
				_ = ck.Close()
				return nil, err
			}
			if !equalSession(ss, m.sessions.sessions[id]) {
				_ = ck.Close()
				return nil, fmt.Errorf("kv: the engine's record of session %d is not the machine's", id)
			}
			src.nSessions++
		}
	}
	if err := it.Err(); err != nil {
		_ = ck.Close()
		return nil, fmt.Errorf("kv: reading the engine's checkpoint: %w", err)
	}
	if src.nSessions != uint64(len(m.sessions.sessions)) {
		_ = ck.Close()
		return nil, fmt.Errorf("kv: the engine records %d sessions, the machine holds %d", src.nSessions, len(m.sessions.sessions))
	}
	return src, nil
}

// equalSession reports whether two sessions are the same table entry.
func equalSession(a, b *session) bool {
	if a == nil || b == nil || a.last != b.last || a.ackedBelow != b.ackedBelow || len(a.results) != len(b.results) {
		return false
	}
	for rid, e := range a.results {
		if f, ok := b.results[rid]; !ok || f != e {
			return false
		}
	}
	return true
}

// lsmSource is the LSM machine's snapshot source: a pinned engine checkpoint
// and the counts its encoding's header needs.
type lsmSource struct {
	ck               *storage.Checkpoint
	limits           Limits
	nKeys, nSessions uint64
}

func (s *lsmSource) Index() uint64 { return s.ck.Applied().Index }
func (s *lsmSource) Close() error  { return s.ck.Close() }

// WriteTo writes the state in the snapshot format (encodeState's bytes for the
// same state): the header, every user key in order, then every session in id
// order — the engine's own key order under the machine's tags.
func (s *lsmSource) WriteTo(w io.Writer) (int64, error) {
	var n int64
	put := func(b []byte) error {
		k, err := w.Write(b)
		n += int64(k)
		return err
	}
	b := binary.AppendUvarint(nil, SnapshotVersion)
	b = binary.AppendUvarint(b, s.Index())
	b = binary.AppendUvarint(b, uint64(s.limits.MaxSessions))
	b = binary.AppendUvarint(b, uint64(s.limits.MaxUnacked))
	b = binary.AppendUvarint(b, s.nKeys)
	if err := put(b); err != nil {
		return n, err
	}
	var keys, sessions uint64
	sessionsBegun := false
	it := s.ck.Iterator()
	for it.Next() {
		k, v := it.Key(), it.Value()
		b = b[:0]
		if len(k) > 0 && k[0] == tagUser {
			if sessionsBegun {
				return n, fmt.Errorf("kv: a user key after the session records in the engine's order")
			}
			keys++
			b = appendBytes(b, k[1:])
			b = appendBytes(b, v)
		} else {
			id, ok := parseSessionKey(k)
			if !ok {
				return n, fmt.Errorf("%w: an engine key %q under no tag the machine writes", ErrSessionRecord, k)
			}
			ss, err := decodeSession(id, v, s.limits)
			if err != nil {
				return n, err
			}
			if !sessionsBegun {
				sessionsBegun = true
				b = binary.AppendUvarint(b, s.nSessions)
			}
			sessions++
			b = appendSession(b, id, ss)
		}
		if err := put(b); err != nil {
			return n, err
		}
	}
	if err := it.Err(); err != nil {
		return n, fmt.Errorf("kv: reading the engine's checkpoint: %w", err)
	}
	if !sessionsBegun {
		if err := put(binary.AppendUvarint(b[:0], s.nSessions)); err != nil {
			return n, err
		}
	}
	if keys != s.nKeys || sessions != s.nSessions {
		return n, fmt.Errorf("kv: the checkpoint yielded %d keys and %d sessions, counted %d and %d", keys, sessions, s.nKeys, s.nSessions)
	}
	return n, nil
}

// ValidateSnapshot reports whether RestoreSnapshot would accept data as the
// state at index: Store's rules (the one decoder, stateReader). Since S4 there
// is no size bound beyond the snapshot format's: a restore is an ingest, not
// one WAL record.
func (m *LSMMachine) ValidateSnapshot(index uint64, data []byte) error {
	return m.ValidateSnapshotFrom(index, bytes.NewReader(data))
}

// ValidateSnapshotFrom reads a snapshot's whole state from r, streaming, and
// reports whether RestoreSnapshotFrom would accept it as the state at index
// (raftnode.SnapshotStateMachine). It holds one key or session at a time.
func (m *LSMMachine) ValidateSnapshotFrom(index uint64, r io.Reader) error {
	sr, err := newStateReader(r, stateSize(r), m.sessions.limits)
	if err != nil {
		return err
	}
	if sr.applied != index {
		return fmt.Errorf("%w: state at index %d, snapshot at %d", ErrSnapshotState, sr.applied, index)
	}
	for {
		if _, _, ok := sr.nextKey(); !ok {
			break
		}
	}
	for {
		id, ss, ok := sr.nextSession()
		if !ok {
			break
		}
		if err := m.fitsTheEngine(id, ss); err != nil {
			return err
		}
	}
	return sr.end()
}

// fitsTheEngine refuses a session whose record would not fit one engine value.
// The decoder's rules bound a session by the limits, but a group run with a
// very large MaxUnacked (above ~21,800 at a 1 MiB value limit) can hold a
// session the engine cannot store: validating it, then failing to restore it at
// every start, would leave a node that cannot start (S4 review). The in-memory
// store has no such bound.
func (m *LSMMachine) fitsTheEngine(id uint64, ss *session) error {
	if n := sessionRecordLen(ss); n > m.opts.MaxValueSize {
		return fmt.Errorf("%w: session %d's record is %d bytes, over the engine's %d-byte value limit (MaxUnacked %d is too large for the LSM machine)",
			ErrSnapshotState, id, n, m.opts.MaxValueSize, m.sessions.limits.MaxUnacked)
	}
	return nil
}

// sessionRecordLen is len(encodeSession(ss)), without encoding it.
func sessionRecordLen(ss *session) int {
	return sessionRecordLenOf(len(ss.results))
}

// sessionRecordLenOf is the length of a session record holding n results.
func sessionRecordLenOf(n int) int {
	return 1 + 16 + uvarintLen(uint64(n)) + n*sessionResultLen
}

// maxSessionRecordLen is the length of the fullest session record limits allow.
func maxSessionRecordLen(l Limits) int { return sessionRecordLenOf(l.MaxUnacked) }

// maxUnackedFor is the largest MaxUnacked whose fullest session record fits a
// value of valueLimit bytes.
func maxUnackedFor(valueLimit int) int {
	n := (valueLimit - 1 - 16 - binary.MaxVarintLen64) / sessionResultLen
	for sessionRecordLenOf(n+1) <= valueLimit {
		n++
	}
	return n
}

// RestoreSnapshot restores data as the state at (index, term): the stream
// restore over the bytes.
func (m *LSMMachine) RestoreSnapshot(index, term uint64, data []byte) error {
	return m.RestoreSnapshotFrom(index, term, bytes.NewReader(data))
}

// RestoreSnapshotFrom replaces the state with a snapshot's, read from r and
// taken at (index, term) (raftnode.SnapshotStateMachine, S4,
// docs/STORAGE_INTEGRATION.md §10.6). If the engine's applied index is at or
// above index, the engine's state is at least as new, and nothing changes —
// decided first, before anything is read: at a restart raftnode restores the
// published snapshot, which for a node's own snapshot the engine already
// holds (S3).
//
// Otherwise the engine INGESTS the state: the decoder streams the snapshot's
// keys and sessions — every rule Store applies, checked as they pass — into
// new tables, and one MANIFEST edit replaces the engine's whole contents with
// them at (index, term). Nothing is held in memory beyond one entry, the
// session table being rebuilt, and the table being written; there is no size
// bound but the snapshot format's. Before the commit the old engine is intact:
// a state that does not validate, or a staging failure, changes nothing and
// the machine stays as it was. After it the engine holds the new state whole.
// A failure that leaves the commit in doubt latches the engine, and the
// machine fails stop — closed, every operation ErrMachineClosed, never
// absence served from an engine whose contents it cannot vouch for (INV-L7) —
// and the next open reads whichever state is durable, old or new; raftnode
// restores the published snapshot again if it is the old.
func (m *LSMMachine) RestoreSnapshotFrom(index, term uint64, r io.Reader) (err error) {
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
	sr, err := newStateReader(r, stateSize(r), m.sessions.limits)
	if err != nil {
		return err
	}
	if sr.applied != index {
		return fmt.Errorf("%w: state at index %d, snapshot at %d", ErrSnapshotState, sr.applied, index)
	}
	src := &restoreSource{sr: sr, sessions: map[uint64]*session{}, fits: m.fitsTheEngine}
	applied := storage.AppliedIndex{Index: index, Term: term}
	if _, err := m.db.Ingest(src, applied); err != nil {
		if m.db.Latched() != nil {
			_ = m.db.Close()
			m.closed = true
		}
		return fmt.Errorf("kv: restoring the snapshot at %d: %w", index, err)
	}
	m.sessions.sessions, m.sessions.stats = src.sessions, ApplyStats{}
	m.engine, m.applied = applied, index
	m.resetCycle()
	return nil
}

// restoreSource is a snapshot's state as engine entries, in the engine's key
// order: each user key under its tag, then each session's record. It keeps the
// sessions it passes — the machine's new session table.
type restoreSource struct {
	sr       *stateReader
	fits     func(uint64, *session) error
	sessions map[uint64]*session
	key, val []byte
	inUsers  bool
	started  bool
	err      error
}

func (s *restoreSource) Next() bool {
	if s.err != nil {
		return false
	}
	if !s.started {
		s.started, s.inUsers = true, true
	}
	if s.inUsers {
		if k, v, ok := s.sr.nextKey(); ok {
			s.key = append(append(s.key[:0], tagUser), k...)
			s.val = v
			return true
		}
		if s.sr.err != nil {
			s.err = s.sr.err
			return false
		}
		s.inUsers = false
	}
	if id, ss, ok := s.sr.nextSession(); ok {
		if err := s.fits(id, ss); err != nil {
			s.err = err
			return false
		}
		s.sessions[id] = ss
		s.key = sessionKey(id)
		s.val = encodeSession(ss)
		return true
	}
	// The end: nothing may follow the last session, and the stream must report
	// its own end — for a snapshot file, its checksum and footer proven.
	s.err = s.sr.end()
	return false
}

func (s *restoreSource) Key() []byte   { return s.key }
func (s *restoreSource) Value() []byte { return s.val }
func (s *restoreSource) Err() error    { return s.err }

// asidePath is where RestoreSnapshot sets the old engine aside before deleting
// it: a sibling of the engine's directory, so the rename never crosses a
// filesystem.
func asidePath(dir string) string { return filepath.Clean(dir) + ".restore-aside" }

// removeAll deletes a directory tree; a test replaces it to crash part-way.
var removeAll = os.RemoveAll

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
