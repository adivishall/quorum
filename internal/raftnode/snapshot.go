package raftnode

import (
	"errors"
	"fmt"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/snapshot"
)

// Phase 14: snapshots and log compaction in the driver (docs/SNAPSHOTS.md). The
// core decides WHEN a follower needs a snapshot and what installing one does to
// the log; this file owns the bytes and the durable orderings — creation,
// publication, compaction, installation and recovery — as functions shared by
// the node's actor and the deterministic simulator (ADR-017).

// ErrSnapshot wraps a snapshot the driver cannot use, or a state the driver
// refuses to act on; during recovery the node does not start.
var ErrSnapshot = errors.New("raftnode: snapshot")

// ErrSuperseded completes a write whose log index a snapshot from the leader
// replaced before this node applied it: the snapshot holds the entry's effect,
// if the entry was the write's at all, but not which entry it was. The outcome
// is UNKNOWN to the client, who retries with the same request id; the session
// table, which the snapshot carries, answers the retry (docs/CLIENT_SEMANTICS.md).
var ErrSuperseded = errors.New("raftnode: outcome unknown: a snapshot replaced the log at the proposal's index")

// SnapshotStateMachine is a state machine whose whole state a snapshot can
// capture and restore (kv.Store). The encoding is the state machine's own; the
// driver treats it as opaque bytes.
type SnapshotStateMachine interface {
	StateMachine
	// EncodeSnapshot returns the state at the applied index, encoded.
	EncodeSnapshot() (index uint64, data []byte, err error)
	// ValidateSnapshot reports whether RestoreSnapshot would accept the state.
	ValidateSnapshot(index uint64, data []byte) error
	// RestoreSnapshot replaces the whole state with the snapshot's, taken at
	// (index, term), or changes nothing on error. The in-memory store has no
	// use for the term; a durable engine records it beside the index (S2).
	RestoreSnapshot(index, term uint64, data []byte) error
}

// LogStore is the durable Raft log as the driver uses it: *raftlog.Log, or the
// simulator's recording wrapper around one.
type LogStore interface {
	Storage
	HardState() raftlog.HardState
	Install(index, term uint64) error
	Compact(index, term uint64) error
}

var _ LogStore = (*raftlog.Log)(nil)

// Installer is a Storage that can make a snapshot the core installed durable
// and active. DrainReadyAt requires one for a Ready that carries a snapshot.
type Installer interface {
	InstallSnapshot(meta raft.SnapshotMeta, hs *raftlog.HardState, at Hook) error
}

// Snapshots is one node's snapshot state: its published snapshot, the state
// machine, the policy, and a leader's snapshot being received. It is owned by
// the node's actor goroutine (or the simulator).
type Snapshots struct {
	Files snapshot.Files
	SM    SnapshotStateMachine // nil: this node neither creates nor installs snapshots
	Group replication.GroupID  // the group's identity (Phase 15)
	// Every is the snapshot trigger: a snapshot is created once the applied
	// index is Every entries past the published one (0: never). It reads only
	// indexes — never a clock — so it is deterministic (docs/SNAPSHOTS.md §4).
	Every uint64
	// Retain is how many entries below a new snapshot's index the log keeps,
	// so a follower slightly behind catches up by entries, not a snapshot.
	Retain uint64
	// Installed, if non-nil, learns the index of each snapshot installed from a
	// leader, once it is durable and restored.
	Installed func(index uint64)

	meta   snapshot.Meta // the published snapshot; Index 0 means none
	file   []byte        // its bytes, loaded when first sent
	skip   uint64        // no new snapshot below this applied index (a state too large)
	recv   *snapshot.Receiver
	staged *snapshot.Received // complete and validated, awaiting the core
}

// Published returns the published snapshot's metadata (Index 0: none).
func (s *Snapshots) Published() snapshot.Meta { return s.meta }

// SendFile returns the published snapshot and its file, for a transfer to a
// follower. It loads (and validates) the file on first use.
func (s *Snapshots) SendFile() (snapshot.Meta, []byte, error) {
	if s.meta.Index == 0 {
		return snapshot.Meta{}, nil, fmt.Errorf("%w: no published snapshot to send", ErrSnapshot)
	}
	if s.file == nil {
		m, _, file, found, err := s.Files.Load()
		if err != nil {
			return snapshot.Meta{}, nil, err
		}
		if !found || m.Index != s.meta.Index || m.Term != s.meta.Term {
			return snapshot.Meta{}, nil, fmt.Errorf("%w: the published file is not snapshot (%d,%d)", ErrSnapshot, s.meta.Index, s.meta.Term)
		}
		s.file = file
	}
	return s.meta, s.file, nil
}

// Receive takes one chunk of a snapshot a peer is sending (transport kind
// InstallSnapshot). When the chunk completes a transfer that validates — the
// file, the group, and the state itself (ValidateSnapshot) — the snapshot is
// staged and Receive returns the MsgSnapshot to step into the core, carrying the
// snapshot's configuration (Phase 15: the node's base configuration once
// installed); the core then installs it or ignores it (docs/SNAPSHOTS.md §8). Otherwise it returns
// nil: an incomplete transfer, or one refused (the error says why; the leader
// offers again). The staged snapshot is consumed by InstallSnapshot within the
// same Ready cycle, or dropped by Unstage.
func (s *Snapshots) Receive(from NodeID, payload []byte) (*raft.Message, error) {
	if s.SM == nil {
		return nil, fmt.Errorf("%w: this node's state machine cannot install snapshots", ErrSnapshot)
	}
	c, err := snapshot.UnmarshalChunk(payload)
	if err != nil {
		return nil, err
	}
	if s.recv == nil {
		s.recv = snapshot.NewReceiver(s.Files)
	}
	got, err := s.recv.Accept(string(from), c)
	if err != nil || got == nil {
		return nil, err
	}
	if got.Meta.Group != s.Group {
		return nil, fmt.Errorf("%w: a snapshot of group %d, this node is of group %d", snapshot.ErrWrongGroup, got.Meta.Group, s.Group)
	}
	if err := s.SM.ValidateSnapshot(got.Meta.Index, got.Data); err != nil {
		return nil, err
	}
	s.staged = got
	conf := got.Meta.Conf.Clone()
	return &raft.Message{Type: raft.MsgSnapshot, From: from, Term: got.Term,
		SnapshotIndex: got.Meta.Index, SnapshotTerm: got.Meta.Term, Conf: &conf}, nil
}

// Unstage drops a staged snapshot the core did not install (it already had
// the state). Its staging file is overwritten by the next transfer or removed
// at startup; it is never published.
func (s *Snapshots) Unstage() { s.staged = nil }

// Durable is a node's durable state as the Ready cycle drives it: the Raft log
// and the snapshots. It is the Storage the node and the simulator drain Readies
// through, so the orderings below have one implementation.
type Durable struct {
	Log  LogStore
	Snap *Snapshots
}

var _ Installer = (*Durable)(nil)

// Save persists a Ready's HardState and entries (the log's Save).
func (d *Durable) Save(hs *raftlog.HardState, entries []raftlog.Entry) error {
	return d.Log.Save(hs, entries)
}

// InstallSnapshot makes the snapshot the core just installed durable and
// active, in the order recovery depends on (docs/SNAPSHOTS.md §6, §8):
//
//  1. a changed term and vote, alone — carrying the previously durable
//     commit — so the durable term is never below the snapshot's (a log whose
//     last term exceeds currentTerm is one the core refuses);
//  2. publication of the staged file: from here the snapshot is the durable
//     record of the log through its index;
//  3. the log's boundary record: the durable log is reset to the snapshot;
//  4. the state machine's restore (validated when the transfer completed).
//
// The Ready's own HardState (the new commit) and entries are saved after this,
// and its messages — the response to the leader — leave only after that. A
// crash between any two steps recovers (§6): before 2 the node is as it was;
// after 2 recovery completes the install itself.
func (d *Durable) InstallSnapshot(meta raft.SnapshotMeta, hs *raftlog.HardState, at Hook) error {
	s := d.Snap
	st := s.staged
	if st == nil || st.Meta.Index != meta.Index || st.Meta.Term != meta.Term {
		return fmt.Errorf("%w: the core installed (%d,%d), and no such snapshot is staged", ErrSnapshot, meta.Index, meta.Term)
	}
	if hs != nil {
		if cur := d.Log.HardState(); hs.Term != cur.Term || hs.Vote != cur.Vote {
			if err := d.Log.Save(&raftlog.HardState{Term: hs.Term, Vote: hs.Vote, Commit: cur.Commit}, nil); err != nil {
				return err
			}
		}
	}
	if err := at.hit(BeforeInstallPublish, meta.Index); err != nil {
		return err
	}
	if err := s.Files.PublishReceived(); err != nil {
		return err
	}
	s.meta, s.file = st.Meta, nil
	if err := at.hit(AfterInstallPublish, meta.Index); err != nil {
		return err
	}
	if err := d.Log.Install(meta.Index, meta.Term); err != nil {
		return err
	}
	if err := at.hit(AfterInstallBoundary, meta.Index); err != nil {
		return err
	}
	if err := s.SM.RestoreSnapshot(meta.Index, meta.Term, st.Data); err != nil {
		return fmt.Errorf("%w: restoring a validated snapshot: %w", ErrSnapshot, err)
	}
	s.staged = nil
	if s.Installed != nil {
		s.Installed(meta.Index)
	}
	return nil
}

// MaybeSnapshot creates a snapshot if the trigger says so: the applied index
// is Every entries past the published snapshot (docs/SNAPSHOTS.md §4). It is
// called after the entries of a cycle are applied.
func (d *Durable) MaybeSnapshot(core *raft.Raft, at Hook) error {
	s := d.Snap
	if s == nil || s.SM == nil || s.Every == 0 {
		return nil
	}
	applied := core.AppliedIndex()
	if applied < s.meta.Index+s.Every || applied < s.skip {
		return nil
	}
	return d.Snapshot(core, at)
}

// Snapshot creates a snapshot of the state machine at the applied index,
// publishes it, and compacts the log behind it, in this order
// (docs/SNAPSHOTS.md §5): encode (memory); publish (temporary file, fsync,
// rename, directory fsync); rewrite the durable log without the prefix; discard
// the prefix in the core. The log is never compacted before the snapshot that
// covers it is durable, so a crash anywhere leaves a state recovery accepts.
//
// A state larger than snapshot.MaxData is not snapshotted: the error wraps
// snapshot.ErrTooLarge, nothing is written, and no new attempt is made until
// Every more entries are applied — the log then keeps growing (documented, not
// hidden). Nor is a state whose configuration the node does not know (Phase 15:
// a joiner that has not yet applied its group's first configuration entry it
// received; raft.ErrConfUnknown): the snapshot must carry the configuration, so
// it waits for the next applied entry. Any other failure is a durability
// failure: the node stops.
func (d *Durable) Snapshot(core *raft.Raft, at Hook) error {
	s := d.Snap
	if s.SM == nil {
		return fmt.Errorf("%w: this node's state machine cannot snapshot", ErrSnapshot)
	}
	idx, data, err := s.SM.EncodeSnapshot()
	if err != nil {
		return fmt.Errorf("%w: encoding the state: %w", ErrSnapshot, err)
	}
	switch {
	case idx > core.AppliedIndex():
		// A machine whose durable state holds more than the core has applied
		// (S2, docs/STORAGE_INTEGRATION.md §8.6): after a restart the core
		// catches up through entries the machine already holds and skips. In
		// practice the first cycle applies the whole committed backlog, and
		// the engine never holds past the durable commit (INV-CR3), so by the
		// time a trigger fires they agree; this guard keeps a snapshot from
		// ever being taken at an index the core has not reached, which would
		// compact a log the core still reads. None is taken until they agree.
		return nil
	case idx < core.AppliedIndex():
		return fmt.Errorf("%w: the state machine is at %d, the core applied %d", ErrSnapshot, idx, core.AppliedIndex())
	}
	if idx <= s.meta.Index {
		return nil // nothing applied since the published snapshot
	}
	term, err := core.TermAt(idx)
	if err != nil {
		return fmt.Errorf("%w: the term of applied entry %d: %w", ErrSnapshot, idx, err)
	}
	conf, err := core.ConfAt(idx)
	if errors.Is(err, raft.ErrConfUnknown) {
		s.skip = idx + 1
		return fmt.Errorf("%w: at %d: %w", ErrSnapshot, idx, err)
	}
	if err != nil {
		return fmt.Errorf("%w: the configuration at %d: %w", ErrSnapshot, idx, err)
	}
	meta := snapshot.Meta{Group: s.Group, Conf: conf, Index: idx, Term: term}
	file, err := snapshot.Encode(meta, data)
	if errors.Is(err, snapshot.ErrTooLarge) {
		s.skip = idx + max(s.Every, 1)
		return err
	}
	if err != nil {
		return err
	}
	if err := at.hit(BeforeSnapshotPublish, idx); err != nil {
		return err
	}
	if err := s.Files.Publish(file); err != nil {
		return err
	}
	s.meta, s.file = meta, file
	if err := at.hit(AfterSnapshotPublish, idx); err != nil {
		return err
	}
	return d.compact(core, at)
}

// compact discards the log through the published snapshot's index less
// Retain: durably first, then in the core. The core then offers a snapshot to
// any follower that needs a discarded entry.
func (d *Durable) compact(core *raft.Raft, at Hook) error {
	s := d.Snap
	c := s.meta.Index - min(s.meta.Index, s.Retain)
	if b, _ := core.Boundary(); c <= b {
		return nil
	}
	term, err := core.TermAt(c)
	if err != nil {
		return fmt.Errorf("%w: the term of compaction point %d: %w", ErrSnapshot, c, err)
	}
	if err := d.Log.Compact(c, term); err != nil {
		return err
	}
	if err := at.hit(AfterLogCompact, c); err != nil {
		return err
	}
	return core.Compact(c)
}

// reconcile decides how a published snapshot and the recovered log fit
// together (docs/SNAPSHOTS.md §6). It returns repair=true when the log does not
// yet record the snapshot — an install that crashed after publishing it and
// before its boundary record — which recovery then completes with the same
// boundary record. Every other mismatch is refused: the log was compacted past
// the only snapshot, or two different entries are committed at one index.
func reconcile(s snapshot.Meta, rec *raftlog.Recovered) (repair bool, err error) {
	b := rec.Boundary
	switch {
	case b.Index > s.Index:
		return false, fmt.Errorf("%w: the log is compacted through %d, past the published snapshot at %d", ErrSnapshot, b.Index, s.Index)
	case b.Index == s.Index && b.Term != s.Term:
		return false, fmt.Errorf("%w: the log's boundary (%d, term %d) contradicts the snapshot's term %d", ErrSnapshot, b.Index, b.Term, s.Term)
	case b.Index == s.Index:
		return false, nil
	}
	if s.Index <= rec.LastIndex() {
		t := rec.Entries[s.Index-b.Index-1].Term
		if t == s.Term {
			return false, nil // the log holds the snapshot's entry: it continues it
		}
		if rec.HardState.Commit >= s.Index {
			return false, fmt.Errorf("%w: entry %d is committed with term %d, the snapshot has term %d", ErrSnapshot, s.Index, t, s.Term)
		}
	}
	return true, nil
}
