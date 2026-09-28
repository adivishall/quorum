package raftnode

import (
	"context"
	"errors"
	"fmt"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/replication"
)

// Phase 15: membership changes in the driver (docs/MEMBERSHIP.md §7). The core
// decides every rule — one change at a time, joint consensus for voters, the
// leader's step-down — and the driver only submits a change to it and reports
// when the change is complete. The configuration itself is never held
// anywhere else: it is the core's, derived from the log.

// ErrConfLost means the configuration entry a membership change appended was
// overwritten by a different entry at its index — the proposing leader lost
// its leadership before the entry committed, and the next leader did not have
// it. The change did not happen; the configuration is whatever the log says.
// Definite.
var ErrConfLost = errors.New("raftnode: the membership change was lost: its configuration entry was overwritten")

type confReq struct {
	cc     raft.ConfChange
	result chan confOutcome
}

type confOutcome struct {
	conf  replication.Configuration
	index uint64
	err   error
}

// confWait is an accepted membership change: its first configuration entry is
// at since, appended in term.
type confWait struct {
	since, term uint64
	result      chan confOutcome
}

// ChangeMembership submits one membership operation to this node, which must be
// the leader, and waits until the change is complete (docs/MEMBERSHIP.md §7):
// its configuration entry — for Promote and RemoveVoter, the joint entry and
// then the final one — committed, with no change under way. It returns the
// configuration reached and the index of its entry. Errors:
//
//   - raft.ErrNotLeader, raft.ErrConfChangeInProgress, raft.ErrInvalidConfChange:
//     nothing was appended. Definite.
//   - ErrConfLost: the change's entry was overwritten; it did not happen.
//     Definite.
//   - ctx.Err(), raft.ErrStopped: the outcome is UNKNOWN — the entry may still
//     commit, and a new leader completes a committed joint configuration on its
//     own. Read the configuration (Status) to learn it.
//
// A leader that removes itself leads the change to its end and then steps down;
// its ChangeMembership still returns success. A node that loses its leadership
// meanwhile keeps waiting, and returns success if the next leader completes the
// change from the same entry.
func (n *Node) ChangeMembership(ctx context.Context, cc raft.ConfChange) (replication.Configuration, uint64, error) {
	req := confReq{cc: cc, result: make(chan confOutcome, 1)}
	select {
	case n.confCh <- req:
	case <-n.ctx.Done():
		return replication.Configuration{}, 0, raft.ErrStopped
	case <-ctx.Done():
		return replication.Configuration{}, 0, ctx.Err()
	}
	select {
	case out := <-req.result:
		return out.conf, out.index, out.err
	case <-n.ctx.Done():
		return replication.Configuration{}, 0, raft.ErrStopped
	case <-ctx.Done():
		return replication.Configuration{}, 0, ctx.Err()
	}
}

// settleChanges completes the accepted membership changes that have finished
// (or were lost). It runs on the actor after every cycle.
func (n *Node) settleChanges() {
	if len(n.changes) == 0 {
		return
	}
	conf, idx := n.core.Conf()
	boundary, _ := n.core.Boundary()
	kept := n.changes[:0]
	for _, w := range n.changes {
		ours := true // the entry at w.since is still the one this change appended
		if w.since > boundary {
			t, err := n.core.TermAt(w.since)
			ours = err == nil && t == w.term
		}
		// (At or below the boundary the entry was applied, hence committed, and
		// a committed entry is never overwritten.)
		// Log Matching makes a lost entry lost for good: only this node, leading
		// w.term, ever appended an entry of that term at w.since, so a leader
		// whose log conflicts with ours at or below it does not hold it.
		switch {
		case !ours:
			w.result <- confOutcome{err: fmt.Errorf("%w: index %d", ErrConfLost, w.since)}
		case !n.core.ConfPending() && (idx >= w.since || w.since <= boundary):
			// Complete: the change's entry is committed and so is every entry
			// it led to. (The configuration may include later changes.)
			w.result <- confOutcome{conf: conf, index: idx}
		default:
			kept = append(kept, w)
		}
	}
	for i := len(kept); i < len(n.changes); i++ {
		n.changes[i] = nil
	}
	n.changes = kept
}

// noteConf records a change of the node's configuration, logging it
// (event=raft_conf) and, once a committed stable configuration no longer
// includes this node, event=raft_removed — the signal the operator (or the
// multi-Raft host) stops the node on (docs/MEMBERSHIP.md §5). It reports
// whether the configuration changed.
func (n *Node) noteConf() bool {
	conf, idx := n.core.Conf()
	changed := !n.confLogged || idx != n.confSeenIdx || !conf.Equal(n.confSeen)
	if changed {
		n.confSeen, n.confSeenIdx, n.confLogged = conf, idx, true
		n.logf("event=raft_conf node=%s group=%d index=%d pending=%v voter=%v conf=%q", n.cfg.ID, n.cfg.Group, idx, n.core.ConfPending(), conf.IsVoter(n.cfg.ID), conf.String())
	}
	if !n.removed && !conf.Empty() && !conf.IsMember(n.cfg.ID) && !n.core.ConfPending() {
		n.removed = true
		n.logf("event=raft_removed node=%s group=%d index=%d conf=%q", n.cfg.ID, n.cfg.Group, idx, conf.String())
	}
	return changed
}

// Removed reports whether this node has seen a committed configuration that
// does not include it: it is no longer a member of its group and may be
// stopped (docs/MEMBERSHIP.md §5).
func (n *Node) Removed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.status.Removed
}
