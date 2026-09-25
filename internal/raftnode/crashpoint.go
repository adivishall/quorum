package raftnode

import (
	"errors"
	"fmt"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
)

// Point is a crash point: a boundary in the driver's persist → send → advance →
// apply cycle at which a test may kill the node (Phase 11, docs/CRASH_RECOVERY.md).
// Points name the boundaries of the driver's own steps, so the same point means
// the same thing in the deterministic simulator (internal/raftsim), in an
// in-process driver test, and in a real dkvd process that kills itself there
// (`dkvd -crash-at`). The finer, I/O-level boundaries inside one Save (between
// record writes, before the fsync) are addressed at the vfs seam instead
// (fault.Injection.At), because they are properties of the durable log, not of
// the driver.
type Point uint8

const (
	// BeforeSave: the Ready has a HardState or entries to persist and none of it
	// has been written. Nothing of this Ready survives a crash here.
	BeforeSave Point = iota + 1
	// AfterSave: Save returned, so the whole Ready is durable (fsynced); no
	// message of it has been handed to the network.
	AfterSave
	// AfterSend: one message of the Ready has been handed to the network (in the
	// driver: enqueued to its peer's outbox). Arg is the message's 0-based ordinal
	// within the Ready; messages after it are never sent.
	AfterSend
	// BeforeAdvance: every message has been handed off; Advance has not run.
	BeforeAdvance
	// AfterAdvance: the Ready cycle is complete.
	AfterAdvance
	// BeforeApply: a committed entry (Arg = its index) is about to be applied.
	BeforeApply
	// AfterApply: the state machine applied the entry (Arg = its index) but
	// AppliedTo has not recorded it. A restart re-applies it (docs/CRASH_RECOVERY.md).
	AfterApply
	// AfterAppliedTo: AppliedTo recorded the entry (Arg = its index).
	AfterAppliedTo
)

var pointNames = map[Point]string{
	BeforeSave: "before-save", AfterSave: "after-save", AfterSend: "after-send",
	BeforeAdvance: "before-advance", AfterAdvance: "after-advance",
	BeforeApply: "before-apply", AfterApply: "after-apply", AfterAppliedTo: "after-applied-to",
}

// Points lists every crash point, in cycle order.
var Points = []Point{BeforeSave, AfterSave, AfterSend, BeforeAdvance, AfterAdvance, BeforeApply, AfterApply, AfterAppliedTo}

// String renders a point in the syntax ParsePoint reads back.
func (p Point) String() string {
	if s, ok := pointNames[p]; ok {
		return s
	}
	return fmt.Sprintf("point(%d)", p)
}

// ParsePoint parses a point name such as "after-save".
func ParsePoint(s string) (Point, bool) {
	for p, name := range pointNames {
		if name == s {
			return p, true
		}
	}
	return 0, false
}

// Hook observes the driver's crash points. It is called at each Point with the
// point's argument (see the Point constants). Returning a non-nil error aborts
// the current cycle at that exact boundary — nothing after it happens — which is
// how a test crashes the node there; the driver then stops as it does for a
// persistence failure. A nil Hook (production) costs nothing.
type Hook func(p Point, arg uint64) error

// ErrApply wraps a state-machine Apply failure returned by ApplyCommitted, so a
// caller can tell it from a hook's abort.
var ErrApply = errors.New("raftnode: state machine apply failed")

// DrainReady performs every pending Ready of core in the order the Raft
// persistence contract requires (ADR-016, INV-R6): persist the Ready's HardState
// and entries through st (which fsyncs), THEN hand each of its Messages to send,
// THEN Advance. It returns the first persistence failure WITHOUT sending that
// Ready's messages and without advancing; the caller must then treat the node as
// failed and never drive core again (INV-F1). This is the single implementation of
// the ordering, shared by the node's actor loop and the deterministic simulator.
func DrainReady(core *raft.Raft, st Storage, send func(raft.Message)) error {
	return DrainReadyAt(core, st, send, nil, nil)
}

// DrainReadyAt is DrainReady with crash points and confirmed reads: at observes
// each boundary of every cycle and may abort there (see Hook); reads, if
// non-nil, receives each ReadIndex request the core confirmed in this Ready
// (Phase 12) — after the Ready's persistence, before its messages; a confirmed
// read depends on nothing being persisted. A nil at and nil reads is DrainReady.
func DrainReadyAt(core *raft.Raft, st Storage, send func(raft.Message), reads func(raft.ReadState), at Hook) error {
	for core.HasReady() {
		rd := core.Ready()
		var hs *raftlog.HardState
		if rd.HardState != nil {
			hs = &raftlog.HardState{Term: rd.HardState.Term, Vote: rd.HardState.Vote, Commit: rd.HardState.Commit}
		}
		if hs != nil || len(rd.Entries) > 0 {
			if err := at.hit(BeforeSave, 0); err != nil {
				return err
			}
			if err := st.Save(hs, rd.Entries); err != nil {
				return err
			}
			if err := at.hit(AfterSave, 0); err != nil {
				return err
			}
		}
		if reads != nil {
			for _, rs := range rd.ReadStates {
				reads(rs)
			}
		}
		for i, m := range rd.Messages {
			send(m)
			if err := at.hit(AfterSend, uint64(i)); err != nil {
				return err
			}
		}
		if err := at.hit(BeforeAdvance, 0); err != nil {
			return err
		}
		core.Advance()
		if err := at.hit(AfterAdvance, 0); err != nil {
			return err
		}
	}
	return nil
}

// ApplyCommitted feeds the committed-but-unapplied entries to sm in index order,
// recording each through AppliedTo only after its Apply returned nil (so an apply
// failure never advances appliedIndex — INV-R7's driver half). A nil sm applies
// nothing but still advances. applied, if non-nil, is told of each entry AFTER it
// has been applied and recorded (Phase 12: this is where a client's write or read
// barrier completes — never before the application it reports). It returns a
// state-machine failure wrapped in ErrApply (the remaining entries are left
// unapplied for the next cycle), or a hook's abort as-is. It is shared by the
// node's actor loop and the simulator.
func ApplyCommitted(core *raft.Raft, sm StateMachine, at Hook, applied func(raft.Entry)) error {
	for _, e := range core.NextApply() {
		if err := at.hit(BeforeApply, e.Index); err != nil {
			return err
		}
		if sm != nil {
			if err := sm.Apply(e.Index, e.Data); err != nil {
				return fmt.Errorf("%w: index %d: %w", ErrApply, e.Index, err)
			}
		}
		if err := at.hit(AfterApply, e.Index); err != nil {
			return err
		}
		if err := core.AppliedTo(e.Index); err != nil {
			return fmt.Errorf("%w: AppliedTo(%d): %w", ErrApply, e.Index, err)
		}
		if err := at.hit(AfterAppliedTo, e.Index); err != nil {
			return err
		}
		if applied != nil {
			applied(e)
		}
	}
	return nil
}

// hit calls the hook if there is one.
func (h Hook) hit(p Point, arg uint64) error {
	if h == nil {
		return nil
	}
	return h(p, arg)
}
