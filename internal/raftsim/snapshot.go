package raftsim

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/snapshot"
)

// Snapshots in the simulator (Phase 14, docs/SNAPSHOTS.md). Nodes create,
// publish, compact, send, receive, install and restore snapshots through the
// driver's own functions (raftnode.Durable, raftnode.Snapshots, raftnode.
// Recover); this file holds the state-machine adapter and the snapshot
// invariants, checked at the instant each is defined.

// snapSM is a node's state machine as the snapshot driver sees it: the node's
// kv.Store, observed at the two moments a state is captured or replaced.
// Entries are applied through simSM, never through this.
type snapSM struct {
	c *Cluster
	n *node
}

var _ raftnode.SnapshotStateMachine = (*snapSM)(nil)

func (s *snapSM) Apply(uint64, []byte) error {
	return errors.New("raftsim: entries are applied through simSM")
}

func (s *snapSM) EncodeSnapshot() (uint64, []byte, error) {
	idx, data, err := s.n.store.EncodeSnapshot()
	if err == nil {
		s.c.checkSnapshotState(s.n, "created", idx, data)
	}
	return idx, data, err
}

func (s *snapSM) ValidateSnapshot(index uint64, data []byte) error {
	return s.n.store.ValidateSnapshot(index, data)
}

func (s *snapSM) RestoreSnapshot(index uint64, data []byte) error {
	if err := s.n.store.RestoreSnapshot(index, data); err != nil {
		return err
	}
	s.n.applied = index
	s.c.checkRestored(s.n, index, data)
	return nil
}

// checkSnapshotState is INV-SN1 with determinism: a snapshot at index holds
// exactly the reference session model folded over the committed log through
// index — the key-value map and the session table, which an independent
// implementation computes — it covers only committed entries, and every
// snapshot at one index, on any node, in any incarnation, is the same bytes
// (the state at an index is a function of the committed prefix, and the
// encoding is canonical).
func (c *Cluster) checkSnapshotState(n *node, what string, index uint64, data []byte) {
	if c.viol != nil {
		return
	}
	if index > uint64(len(c.chk.committed)) {
		c.violate("INV-SN1", "%s %s a snapshot at index %d, beyond the %d entries committed", n.id, what, index, len(c.chk.committed))
		return
	}
	sum := sha256.Sum256(data)
	if prev, ok := c.chk.snapStates[index]; ok && prev != sum {
		c.violate("INV-SN1", "%s %s a snapshot at index %d whose state differs from an earlier snapshot at the same index", n.id, what, index)
		return
	}
	c.chk.snapStates[index] = sum
	model, keys := c.modelThrough(index)
	if diffs := diffStore(n.store, model, keys); len(diffs) > 0 {
		c.violate("INV-SN1", "%s %s a snapshot at index %d that differs from the reference model over the committed log: %s",
			n.id, what, index, strings.Join(diffs, "; "))
	}
}

// checkRestored is INV-SN2 and INV-SN1 at a restore: the state a node restores
// — at a restart or installing a leader's snapshot — is its published
// snapshot, whole and valid, and that snapshot is the replicated state at its
// index.
func (c *Cluster) checkRestored(n *node, index uint64, data []byte) {
	if c.viol != nil {
		return
	}
	m, published, _, found, err := (snapshot.Files{FS: n.disk, Base: logPath}).Load()
	switch {
	case err != nil || !found:
		c.violate("INV-SN2", "%s restored a snapshot at %d but its published snapshot is not valid (found=%v err=%v)", n.id, index, found, err)
		return
	case m.Index != index || string(published) != string(data):
		c.violate("INV-SN2", "%s restored a state at %d that is not its published snapshot (%d)", n.id, index, m.Index)
		return
	}
	c.checkSnapshotState(n, "restored", index, data)
}

// checkSN3 is INV-SN3: compaction never discards the only record of committed
// state — a node's durable log boundary is never above the index of its
// durable snapshot (the one a power loss right now would leave).
func (c *Cluster) checkSN3(n *node) {
	b := n.shadow.persisted.b
	if b.Index == 0 || c.viol != nil {
		return
	}
	file, ok := n.disk.Durable(logPath + ".snap")
	if !ok {
		c.violate("INV-SN3", "%s's log is durably compacted through %d and no snapshot is durable", n.id, b.Index)
		return
	}
	m, _, err := snapshot.Decode(file)
	if err != nil || m.Index < b.Index {
		c.violate("INV-SN3", "%s's log is durably compacted through %d, its durable snapshot is at %d (%v)", n.id, b.Index, m.Index, err)
	}
}

// modelThrough folds the reference session model over the committed log
// through index, and returns the keys that prefix wrote.
func (c *Cluster) modelThrough(index uint64) (*lincheck.SessionModel, map[string]bool) {
	l := c.kvLimits()
	model := lincheck.NewSessionModel(lincheck.SessionLimits{MaxSessions: l.MaxSessions, MaxUnacked: l.MaxUnacked})
	keys := map[string]bool{}
	for i := uint64(0); i < index && i < uint64(len(c.chk.committed)); i++ {
		cmd, err := kv.Decode(c.chk.committed[i].e.Data)
		if err != nil {
			continue // the no-op and plain Propose commands
		}
		mc := lincheck.SessionCommand{Index: i + 1, Register: cmd.Op == kv.OpRegister, ClientID: cmd.ClientID,
			RequestID: cmd.RequestID, AckedBelow: cmd.AckedBelow, Kind: lincheck.Put, Key: string(cmd.Key), Value: string(cmd.Value)}
		if cmd.Op == kv.OpDelete {
			mc.Kind = lincheck.Delete
		}
		model.Apply(mc)
		if cmd.Op != kv.OpRegister {
			keys[string(cmd.Key)] = true
		}
	}
	return model, keys
}

// diffStore lists how a store differs from the model: every key the
// committed log wrote, every key the store holds, every session.
func diffStore(store *kv.Store, model *lincheck.SessionModel, keys map[string]bool) []string {
	snap := store.Snapshot()
	var diffs []string
	for k := range keys {
		st := model.State(k)
		v, ok := snap[k]
		if ok != st.Present || (ok && string(v) != st.Value) {
			diffs = append(diffs, fmt.Sprintf("%q: store (%v,%q) model (%v,%q)", k, ok, v, st.Present, st.Value))
		}
	}
	for k := range snap {
		if !keys[k] {
			diffs = append(diffs, fmt.Sprintf("%q present in the store, never written in the committed log", k))
		}
	}
	table := store.Sessions()
	ids := model.Sessions()
	if len(ids) != len(table) {
		diffs = append(diffs, fmt.Sprintf("the store holds %d sessions, the model %d", len(table), len(ids)))
	}
	for _, sid := range ids {
		acked, rids, _ := model.SessionInfo(sid)
		sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })
		st, ok := table[sid]
		if !ok || st.AckedBelow != acked || fmt.Sprint(st.Requests) != fmt.Sprint(rids) {
			diffs = append(diffs, fmt.Sprintf("session %d: store %+v (present %v), model acked=%d rids=%v", sid, st, ok, acked, rids))
		}
	}
	sort.Strings(diffs)
	return diffs
}

// matchesAfter reports whether a recovered log is one of the candidates with a
// boundary record appended — recovery completing an install that crashed after
// publishing its snapshot (docs/SNAPSHOTS.md §6).
func (s *shadowStore) matchesAfter(rec *raftlog.Recovered, b raftlog.Boundary) bool {
	for _, cand := range s.candidates() {
		cand.install(b)
		if sameDurable(cand, rec) {
			return true
		}
	}
	return false
}
