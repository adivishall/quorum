package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
)

// newCore builds a single core over a fresh log for unit-level tests.
func newCore(t *testing.T, id NodeID, peers []NodeID, seed int64) (*Raft, *replication.MemoryLog) {
	t.Helper()
	lg := replication.NewMemoryLog()
	r, err := New(Config{ID: id, Peers: peers, Rand: rand.New(rand.NewSource(seed)), Log: lg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r, lg
}

// TestConfigValidation covers the construction guards.
func TestConfigValidation(t *testing.T) {
	good := func() Config {
		return Config{ID: "a", Peers: ids(3), Rand: rand.New(rand.NewSource(1)), Log: replication.NewMemoryLog()}
	}
	cases := []struct {
		name  string
		mut   func(*Config)
		wantE error
	}{
		{"ok", func(c *Config) {}, nil},
		{"no id", func(c *Config) { c.ID = "" }, ErrNoID},
		{"id not in peers", func(c *Config) { c.ID = "z" }, ErrIDNotInPeers},
		{"nil rand", func(c *Config) { c.Rand = nil }, ErrNoRand},
		{"nil log", func(c *Config) { c.Log = nil }, ErrNoLog},
		{"dup peer", func(c *Config) { c.Peers = []NodeID{"a", "a", "b"} }, ErrDuplicatePeer},
		{"empty peer", func(c *Config) { c.Peers = []NodeID{"a", "", "b"} }, ErrEmptyPeer},
		{"bad ticks", func(c *Config) { c.ElectionTicks = 2; c.HeartbeatTicks = 5 }, ErrInvalidTicks},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := good()
			tc.mut(&cfg)
			_, err := New(cfg)
			if tc.wantE == nil {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if err == nil || !errorsIs(err, tc.wantE) {
				t.Fatalf("err = %v, want %v", err, tc.wantE)
			}
		})
	}
}

func errorsIs(err, target error) bool { return err == target }

// TestInitialState proves a fresh node starts as a follower at its recovered term.
func TestInitialState(t *testing.T) {
	r, _ := newCore(t, "a", ids(3), 1)
	if r.Role() != Follower || r.Term() != 0 || r.LeaderID() != "" {
		t.Fatalf("initial: role=%s term=%d leader=%q, want Follower/0/\"\"", r.Role(), r.Term(), r.LeaderID())
	}
	if r.CommitIndex() != 0 || r.LastIndex() != 0 {
		t.Fatalf("initial commit=%d last=%d, want 0/0", r.CommitIndex(), r.LastIndex())
	}
}

// TestProposeOnNonLeaderRejected proves Propose fails on a follower.
func TestProposeOnNonLeaderRejected(t *testing.T) {
	r, _ := newCore(t, "a", ids(3), 1)
	if err := r.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("Propose on follower = %v, want ErrNotLeader", err)
	}
}

// TestCoreDeterminism proves the core is replayable: two cores with the same seed
// and the same input sequence produce identical roles, terms, and message bytes at
// every step (docs/RAFT.md §12, ADR-002).
func TestCoreDeterminism(t *testing.T) {
	script := func() []Message {
		s := rand.New(rand.NewSource(42))
		var out []Message
		for i := 0; i < 60; i++ {
			out = append(out, Message{
				Type: MessageType(1 + s.Intn(4)),
				From: ids(3)[s.Intn(3)],
				Term: uint64(s.Intn(5)),
			})
		}
		return out
	}()

	trace := func() string {
		r, _ := newCore(t, "a", ids(3), 99)
		var b bytes.Buffer
		for step, m := range script {
			if step%7 == 0 {
				r.Tick()
			}
			_ = r.Step(m)
			rd := r.Ready()
			fmt.Fprintf(&b, "s%d role=%s term=%d hs=%v", step, r.Role(), r.Term(), rd.HardState != nil)
			for _, om := range rd.Messages {
				b.Write(om.Marshal())
			}
			r.Advance()
		}
		return b.String()
	}
	if trace() != trace() {
		t.Fatal("core is not deterministic under identical seed and inputs")
	}
}

// --- reference model for the vote decision (differential, §38) ---

// refShouldGrant is an INDEPENDENT reimplementation of the vote rule (§5.2/§5.4.1),
// deliberately not sharing code with the core, so a mirrored bug is detectable.
func refShouldGrant(curTerm uint64, votedFor NodeID, myLastIdx, myLastTerm uint64, m Message) bool {
	if m.Term < curTerm {
		return false
	}
	effVote := votedFor
	if m.Term > curTerm {
		effVote = "" // a higher term clears the vote before deciding
	}
	if effVote != "" && effVote != m.From {
		return false
	}
	upToDate := m.LastLogTerm > myLastTerm || (m.LastLogTerm == myLastTerm && m.LastLogIndex >= myLastIdx)
	return upToDate
}

// TestVoteAgainstReferenceModel drives generated RequestVotes into a fresh core and
// compares the grant decision against the independent reference.
func TestVoteAgainstReferenceModel(t *testing.T) {
	s := rand.New(rand.NewSource(7))
	for iter := 0; iter < 500; iter++ {
		// A node with a random pre-existing log and term/vote.
		lg := replication.NewMemoryLog()
		n := s.Intn(4)
		term := uint64(0)
		for i := 1; i <= n; i++ {
			if s.Intn(3) == 0 {
				term++
			}
			_ = lg.Append(Entry{Index: uint64(i), Term: term})
		}
		curTerm := term + uint64(s.Intn(3))
		vote := NodeID("")
		if s.Intn(2) == 0 {
			vote = "b"
		}
		r, err := New(Config{ID: "a", Peers: ids(3), Rand: rand.New(rand.NewSource(1)), Log: lg, Term: curTerm, Vote: vote})
		if err != nil {
			t.Fatal(err)
		}
		myLast := lg.LastIndex()
		myLastTerm, _ := lg.Term(myLast)

		m := Message{
			Type: MsgVoteRequest, From: NodeID([]string{"b", "c"}[s.Intn(2)]), To: "a",
			Term:         curTerm + uint64(s.Intn(3)) - 1, // may be below/equal/above
			LastLogIndex: uint64(s.Intn(n + 2)),
			LastLogTerm:  uint64(s.Intn(int(term) + 2)),
		}
		if m.Term == 0 {
			m.Term = 1
		}
		want := refShouldGrant(curTerm, vote, myLast, myLastTerm, m)

		_ = r.Step(m)
		rd := r.Ready()
		var granted bool
		for _, om := range rd.Messages {
			if om.Type == MsgVoteResponse {
				granted = om.VoteGranted
			}
		}
		if granted != want {
			t.Fatalf("iter %d: granted=%v want=%v (curTerm=%d vote=%q myLast=(%d,t%d) msg=%+v)",
				iter, granted, want, curTerm, vote, myLast, myLastTerm, m)
		}
	}
}

// FuzzRaftEvents drives a single core with an arbitrary event program and
// establishes: no panic, internal coherence (applied<=commit<=lastIndex) after
// every step, and deterministic replay. Inputs are bounded so allocation is bounded.
func FuzzRaftEvents(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 0, 1, 2, 3, 0, 1})
	f.Add(bytes.Repeat([]byte{0x02, 0x11, 0x03}, 40))

	run := func(data []byte) (role Role, term, last, commit uint64) {
		r, _ := newCoreNoT("a", ids(3), 5)
		i := 0
		for ops := 0; i < len(data) && ops < 300; ops++ {
			op := data[i]
			i++
			switch op % 4 {
			case 0:
				r.Tick()
			case 1:
				_ = r.Propose([]byte{op})
			default:
				m := Message{
					Type: MessageType(1 + int(next(data, &i))%4),
					From: ids(3)[int(next(data, &i))%3],
					To:   "a",
					Term: uint64(next(data, &i)) % 6,
				}
				m.PrevLogTerm = uint64(next(data, &i)) % 6
				m.LastLogTerm = m.PrevLogTerm
				_ = r.Step(m)
			}
			// Coherence after every step.
			if r.AppliedIndex() > r.CommitIndex() || r.CommitIndex() > r.LastIndex() {
				panic("invariant violated during fuzz")
			}
			// Drain effects so accumulators stay bounded.
			_ = r.Ready()
			r.Advance()
		}
		return r.Role(), r.Term(), r.LastIndex(), r.CommitIndex()
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		r1, t1, l1, c1 := run(data)
		r2, t2, l2, c2 := run(data)
		if r1 != r2 || t1 != t2 || l1 != l2 || c1 != c2 {
			t.Fatalf("non-deterministic: (%s,%d,%d,%d) vs (%s,%d,%d,%d)", r1, t1, l1, c1, r2, t2, l2, c2)
		}
	})
}

// newCoreNoT builds a core outside a *testing.T context (for fuzz run closures).
func newCoreNoT(id NodeID, peers []NodeID, seed int64) (*Raft, *replication.MemoryLog) {
	lg := replication.NewMemoryLog()
	r, err := New(Config{ID: id, Peers: peers, Rand: rand.New(rand.NewSource(seed)), Log: lg})
	if err != nil {
		panic(err)
	}
	return r, lg
}

func next(data []byte, i *int) byte {
	if *i >= len(data) {
		return 0
	}
	b := data[*i]
	*i++
	return b
}
