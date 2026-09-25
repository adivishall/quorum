package raftnode

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/replication"
)

// memStorage is a Storage that records every Save it was asked for.
type memStorage struct {
	saves []string
}

func (m *memStorage) Save(hs *raftlog.HardState, entries []raftlog.Entry) error {
	s := fmt.Sprintf("save(entries=%d", len(entries))
	if hs != nil {
		s += fmt.Sprintf(",term=%d,vote=%s,commit=%d", hs.Term, hs.Vote, hs.Commit)
	}
	m.saves = append(m.saves, s+")")
	return nil
}

// singleNodeCore returns a fresh single-node core, which elects itself and
// appends its no-op on the first election timeout: one cycle holds a HardState
// (term 1, vote self), an entry (the no-op), no messages, then a commit.
func singleNodeCore(t *testing.T) *raft.Raft {
	t.Helper()
	core, err := raft.New(raft.Config{ID: "a", Peers: []NodeID{"a"}, Rand: rand.New(rand.NewSource(1)), Log: replication.NewMemoryLog()})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2*raft.DefaultElectionTicks && core.Role() != raft.Leader; i++ {
		core.Tick()
	}
	if core.Role() != raft.Leader {
		t.Fatal("single-node core did not elect itself")
	}
	return core
}

// threeNodeCandidate returns a core of a three-node group that has just become a
// candidate: its cycle holds a HardState (term 1, vote self) and two RequestVote
// messages, and nothing to apply.
func threeNodeCandidate(t *testing.T) *raft.Raft {
	t.Helper()
	core, err := raft.New(raft.Config{ID: "a", Peers: []NodeID{"a", "b", "c"}, Rand: rand.New(rand.NewSource(1)), Log: replication.NewMemoryLog()})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2*raft.DefaultElectionTicks && core.Role() != raft.Candidate; i++ {
		core.Tick()
	}
	if core.Role() != raft.Candidate {
		t.Fatal("core did not become a candidate")
	}
	return core
}

// drive runs one full driver cycle (DrainReadyAt then ApplyCommitted) with a
// recording hook, storage, send and state machine, returning the interleaved
// record of everything that happened in order.
func drive(core *raft.Raft, at func(p Point, arg uint64) error) (events []string, err error) {
	st := &memStorage{}
	sm := &recSM{}
	hook := func(p Point, arg uint64) error {
		events = append(events, fmt.Sprintf("%s:%d", p, arg))
		if at == nil {
			return nil
		}
		return at(p, arg)
	}
	send := func(m raft.Message) { events = append(events, "send:"+m.Type.String()+">"+string(m.To)) }
	err = DrainReadyAt(core, storageTap{st, func() { events = append(events, st.saves[len(st.saves)-1]) }}, send, hook)
	if err != nil {
		return events, err
	}
	err = ApplyCommitted(core, smTap{sm, func(i uint64) { events = append(events, fmt.Sprintf("apply:%d", i)) }}, hook)
	return events, err
}

type storageTap struct {
	st *memStorage
	on func()
}

func (s storageTap) Save(hs *raftlog.HardState, entries []raftlog.Entry) error {
	err := s.st.Save(hs, entries)
	s.on()
	return err
}

type smTap struct {
	sm *recSM
	on func(uint64)
}

func (s smTap) Apply(i uint64, cmd []byte) error {
	s.on(i)
	return s.sm.Apply(i, cmd)
}

// TestCrashPointsBracketEveryStepInOrder pins where each point sits relative to
// the driver's real steps: BeforeSave/AfterSave around the Save, AfterSend after
// each hand-off, BeforeAdvance/AfterAdvance around Advance, and the three apply
// points around Apply and AppliedTo — for a candidate's cycle (a Save then
// messages) and a single-node leader's (a Save, no messages, then a commit that
// is persisted and applied).
func TestCrashPointsBracketEveryStepInOrder(t *testing.T) {
	got, err := drive(threeNodeCandidate(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"before-save:0", "save(entries=0,term=1,vote=a,commit=0)", "after-save:0",
		"send:VoteRequest>b", "after-send:0", "send:VoteRequest>c", "after-send:1",
		"before-advance:0", "after-advance:0",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("candidate cycle:\n got %v\nwant %v", got, want)
	}

	got, err = drive(singleNodeCore(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	// One cycle: a single node is its own quorum, so the election commits the
	// no-op in the same step that appends it — term, vote, the entry and the
	// commit are one Save; then the no-op (index 1) is applied.
	want = []string{
		"before-save:0", "save(entries=1,term=1,vote=a,commit=1)", "after-save:0", "before-advance:0", "after-advance:0",
		"before-apply:1", "apply:1", "after-apply:1", "after-applied-to:1",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("single-node cycle:\n got %v\nwant %v", got, want)
	}
}

// TestCrashPointAbortStopsExactlyThere proves a hook's abort ends the cycle at
// that boundary: everything before the point happened, nothing after it did, and
// the abort is returned unchanged (never mistaken for an apply failure).
func TestCrashPointAbortStopsExactlyThere(t *testing.T) {
	boom := errors.New("crash here")
	cases := []struct {
		at   string
		want []string // the complete record, the abort's own point last
	}{
		{"before-save:0", []string{"before-save:0"}},
		{"after-save:0", []string{"before-save:0", "save(entries=0,term=1,vote=a,commit=0)", "after-save:0"}},
		{"after-send:0", []string{"before-save:0", "save(entries=0,term=1,vote=a,commit=0)", "after-save:0", "send:VoteRequest>b", "after-send:0"}},
		{"after-send:1", []string{"before-save:0", "save(entries=0,term=1,vote=a,commit=0)", "after-save:0", "send:VoteRequest>b", "after-send:0", "send:VoteRequest>c", "after-send:1"}},
		{"before-advance:0", []string{"before-save:0", "save(entries=0,term=1,vote=a,commit=0)", "after-save:0", "send:VoteRequest>b", "after-send:0", "send:VoteRequest>c", "after-send:1", "before-advance:0"}},
	}
	for _, c := range cases {
		core := threeNodeCandidate(t)
		got, err := drive(core, func(p Point, arg uint64) error {
			if fmt.Sprintf("%s:%d", p, arg) == c.at {
				return boom
			}
			return nil
		})
		if !errors.Is(err, boom) || errors.Is(err, ErrApply) {
			t.Fatalf("abort at %s returned %v, want the abort itself", c.at, err)
		}
		if strings.Join(got, " ") != strings.Join(c.want, " ") {
			t.Fatalf("abort at %s:\n got %v\nwant %v", c.at, got, c.want)
		}
		if c.at != "before-save:0" && !core.HasReady() {
			// Nothing was advanced, so the Ready is still pending — a restarted
			// node would not need it (its state is durable), a continuing one would.
			t.Fatalf("abort at %s: the core no longer has the Ready pending", c.at)
		}
	}

	// Apply points: an abort after Apply but before AppliedTo leaves the entry
	// unrecorded, so it would be applied again — the documented at-least-once.
	core := singleNodeCore(t)
	got, err := drive(core, func(p Point, arg uint64) error {
		if p == AfterApply {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("abort after apply returned %v", err)
	}
	if last := got[len(got)-1]; last != "after-apply:1" || got[len(got)-2] != "apply:1" {
		t.Fatalf("abort after apply: record ends %v, want apply:1 then after-apply:1", got[len(got)-2:])
	}
	if core.AppliedIndex() != 0 {
		t.Fatalf("applied index advanced to %d although the abort preceded AppliedTo", core.AppliedIndex())
	}
	if n := len(core.NextApply()); n != 1 {
		t.Fatalf("%d entries left to apply, want the aborted one still pending", n)
	}
}

// TestApplyFailureIsWrappedAndLeavesTheRestPending proves a state machine's
// failure is reported as ErrApply (distinct from a crash point), AppliedTo is not
// called for it, and later entries are not applied.
func TestApplyFailureIsWrappedAndLeavesTheRestPending(t *testing.T) {
	core := singleNodeCore(t)
	if err := DrainReady(core, &memStorage{}, func(raft.Message) {}); err != nil {
		t.Fatal(err)
	}
	// Two more proposals, so three entries are committed and pending application.
	_ = core.Propose([]byte("x"))
	_ = core.Propose([]byte("y"))
	if err := DrainReady(core, &memStorage{}, func(raft.Message) {}); err != nil {
		t.Fatal(err)
	}
	if core.CommitIndex() != 3 {
		t.Fatalf("setup: commit = %d, want 3", core.CommitIndex())
	}
	failAt := uint64(2)
	sm := failingSM{at: failAt}
	err := ApplyCommitted(core, sm, nil)
	if !errors.Is(err, ErrApply) {
		t.Fatalf("apply failure returned %v, want ErrApply", err)
	}
	if core.AppliedIndex() != failAt-1 {
		t.Fatalf("applied index = %d after a failure at %d, want %d", core.AppliedIndex(), failAt, failAt-1)
	}
}

type failingSM struct{ at uint64 }

func (f failingSM) Apply(i uint64, _ []byte) error {
	if i == f.at {
		return errors.New("refused")
	}
	return nil
}
