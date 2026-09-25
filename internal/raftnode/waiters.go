package raftnode

import (
	"errors"

	"github.com/adivishall/quorum/internal/raft"
)

// ErrLost means a proposal was accepted into a leader's log but a DIFFERENT entry
// was later committed and applied at its index: the leader lost its term before
// the entry could commit, and the new leader's log did not contain it. The write
// definitely had no effect (a committed entry is never replaced), so the client
// may safely retry it as a new operation.
var ErrLost = errors.New("raftnode: proposal lost: a different entry was committed at its index")

// Outcome is what a Waiter learns: the index and term of the entry applied at
// its index, or an error.
type Outcome struct {
	Index uint64
	Term  uint64
	Err   error
}

// Waiters is the table of client requests waiting for the state machine to reach
// a log index (Phase 12, docs/LINEARIZABILITY.md §3): a write waits for its own
// entry — success only if the entry applied at that index carries the term the
// proposal was made in, ErrLost otherwise; a read barrier (term 0) waits for the
// applied index to reach a confirmed read index. It is pure bookkeeping owned by
// the node's actor goroutine (no locking) and shared with the deterministic
// simulator, so the completion rule has exactly one implementation.
type Waiters struct {
	byIndex map[uint64][]waiter
	n       int
}

type waiter struct {
	term uint64
	ch   chan Outcome
}

// NewWaiters returns an empty table.
func NewWaiters() *Waiters { return &Waiters{byIndex: map[uint64][]waiter{}} }

// Add registers a waiter for index and returns the channel its Outcome arrives
// on (buffered, so completion never blocks the actor). term != 0 is a write
// waiting for its own entry; term == 0 is a barrier satisfied by any entry
// reaching index. applied is the state machine's current applied index: a
// barrier at or below it is satisfied immediately (a write never is — its entry
// cannot have been applied before it was registered).
func (w *Waiters) Add(index, term, applied uint64) <-chan Outcome {
	ch := make(chan Outcome, 1)
	if term == 0 && index <= applied {
		ch <- Outcome{Index: index}
		return ch
	}
	w.byIndex[index] = append(w.byIndex[index], waiter{term: term, ch: ch})
	w.n++
	return ch
}

// Applied reports that the entry (index, term) has been applied: every waiter
// at that index is completed — a write with its term matched succeeds, a write
// with another term is ErrLost, a barrier succeeds — and, since application is
// in order, so is any barrier registered at a lower index that was somehow
// missed. Call it after AppliedTo, so a response can never precede the record
// of the application it reports.
func (w *Waiters) Applied(index, term uint64) {
	ws, ok := w.byIndex[index]
	if !ok {
		return
	}
	delete(w.byIndex, index)
	w.n -= len(ws)
	for _, wt := range ws {
		switch {
		case wt.term == 0 || wt.term == term:
			wt.ch <- Outcome{Index: index, Term: term}
		default:
			wt.ch <- Outcome{Index: index, Term: term, Err: ErrLost}
		}
	}
}

// FailAll completes every waiter with err (the node stopped or fail-stopped:
// the outcome of each request is UNKNOWN to its client).
func (w *Waiters) FailAll(err error) {
	for idx, ws := range w.byIndex {
		for _, wt := range ws {
			wt.ch <- Outcome{Index: idx, Err: err}
		}
	}
	w.byIndex = map[uint64][]waiter{}
	w.n = 0
}

// Len is the number of waiters registered and not yet completed.
func (w *Waiters) Len() int { return w.n }

// pendingRead is a ReadIndex request the core has registered but not yet
// confirmed: it remembers the term it was registered in, so a leadership change
// before confirmation fails it with ErrNotLeader (the core drops the read).
type pendingRead struct {
	term uint64
	ch   chan Outcome
}

// Reads tracks the node's unconfirmed ReadIndex requests by id (owned by the
// actor). Once the core confirms one (Ready.ReadStates) it becomes a barrier in
// Waiters at the confirmed index.
type Reads struct {
	byID map[uint64]pendingRead
}

// NewReads returns an empty table.
func NewReads() *Reads { return &Reads{byID: map[uint64]pendingRead{}} }

// Add registers a read the core accepted in term and returns its channel.
func (r *Reads) Add(id, term uint64) <-chan Outcome {
	ch := make(chan Outcome, 1)
	r.byID[id] = pendingRead{term: term, ch: ch}
	return ch
}

// Confirmed moves a confirmed read into the waiters as a barrier at rs.Index.
// A read the table no longer knows (already failed) is ignored.
func (r *Reads) Confirmed(rs raft.ReadState, w *Waiters, applied uint64) {
	p, ok := r.byID[rs.ID]
	if !ok {
		return
	}
	delete(r.byID, rs.ID)
	if rs.Index <= applied {
		p.ch <- Outcome{Index: rs.Index}
		return
	}
	w.byIndex[rs.Index] = append(w.byIndex[rs.Index], waiter{term: 0, ch: p.ch})
	w.n++
}

// DropStale fails every unconfirmed read that was registered in a term other
// than term, or when the node no longer leads: the core has dropped them and
// they will never be confirmed.
func (r *Reads) DropStale(term uint64, leader bool) {
	for id, p := range r.byID {
		if !leader || p.term != term {
			p.ch <- Outcome{Err: raft.ErrNotLeader}
			delete(r.byID, id)
		}
	}
}

// FailAll fails every unconfirmed read with err.
func (r *Reads) FailAll(err error) {
	for id, p := range r.byID {
		p.ch <- Outcome{Err: err}
		delete(r.byID, id)
	}
}

// Len is the number of unconfirmed reads.
func (r *Reads) Len() int { return len(r.byID) }
