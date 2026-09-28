package raftsim

import (
	"fmt"
	"testing"

	"github.com/adivishall/quorum/internal/raft"
)

func selectedMultiProfiles(t *testing.T) []MultiProfile {
	if *flagProfile == "" {
		return MultiProfiles
	}
	p, ok := MultiProfileByName(*flagProfile)
	if !ok {
		t.Skipf("-raftsim.profile=%s is not a multi-group profile", *flagProfile)
	}
	return []MultiProfile{p}
}

// TestMultiGroupSchedules runs seeded multi-group chaos: node-level faults
// fanned out to every group, each group's own traffic, membership changes and
// session clients, then stabilization. Every group's continuous invariants
// hold throughout, every group converges, every group commits, and every
// group's client history is linearizable on its own.
func TestMultiGroupSchedules(t *testing.T) {
	for _, p := range selectedMultiProfiles(t) {
		for _, seed := range selectedSeeds() {
			p, seed := p, seed
			t.Run(fmt.Sprintf("%s/seed=%d", p.Name, seed), func(t *testing.T) {
				r := RunMulti(p, seed)
				if r.Violation != nil {
					c := r.Multi.Group(r.Group)
					t.Fatalf("group %d: %v\n--- reproduce ---\ngo test ./internal/raftsim -run 'TestMultiGroupSchedules/%s/seed=%d$' -raftsim.profile=%s -raftsim.seed=%d -count=1 -v\n--- last trace lines of the group ---\n%s",
						r.Group, r.Violation, p.Name, seed, p.Name, seed, joinLines(c.Trace().Tail(40)))
				}
				crashes := 0
				for g := 0; g < r.Multi.Groups(); g++ {
					c := r.Multi.Group(g)
					st := c.Stats()
					crashes += st.ProcessCrashes + st.PointCrashes
					if st.MaxCommit <= 1 || st.LeaderElections == 0 {
						t.Fatalf("group %d proved nothing: %+v", g, st)
					}
					if h := c.History(); len(h.Ops) > 0 {
						if ok, lr := linearizable(h); !ok {
							t.Fatalf("group %d: NOT LINEARIZABLE: %s", g, lr.Reason)
						}
					}
				}
				if crashes == 0 {
					t.Fatalf("no node crashed in any group: the run injected no node-level fault")
				}
			})
		}
	}
}

func joinLines(ls []string) string {
	out := ""
	for _, l := range ls {
		out += l + "\n"
	}
	return out
}

// TestMultiSameSeedSameTrace: a multi-group run is a pure function of its
// profile and seed — same seed, same script and trace; the recorded script
// replays to the same trace; another seed differs.
func TestMultiSameSeedSameTrace(t *testing.T) {
	for _, p := range MultiProfiles {
		a, b := RunMulti(p, 7), RunMulti(p, 7)
		if a.TraceHash != b.TraceHash || len(a.Script) != len(b.Script) {
			t.Fatalf("%s: same seed, different runs", p.Name)
		}
		for i := range a.Script {
			if a.Script[i].String() != b.Script[i].String() {
				t.Fatalf("%s: scripts differ at event %d: %s vs %s", p.Name, i, a.Script[i], b.Script[i])
			}
		}
		re := ReplayMulti(p.Config(7), a.Script)
		re.Stabilize(400)
		for g := 0; g < re.Groups(); g++ {
			if c := re.Group(g); c.kv != nil && len(c.kv.clients) > 0 {
				c.SettleClients(p.Group.Keys, 200)
			}
		}
		if re.TraceHash() != a.TraceHash {
			t.Fatalf("%s: replaying the recorded script diverged", p.Name)
		}
		if c := RunMulti(p, 8); c.TraceHash == a.TraceHash {
			t.Fatalf("%s: seeds 7 and 8 produced the same trace", p.Name)
		}
	}
}

// TestMultiGroupIsolation (INV-M9): in a multi-group run, each group's trace
// is reproduced exactly by replaying, on a lone Cluster, the events that group
// received — its own and the node-level ones fanned out to it. A group's
// behaviour is a function of its own inputs alone: nothing another group did
// reached it except through the node-level faults every group suffers.
func TestMultiGroupIsolation(t *testing.T) {
	for _, p := range MultiProfiles {
		for _, seed := range []int64{1, 2, 3} {
			r := RunMulti(p, seed)
			if r.Violation != nil {
				t.Fatalf("%s seed %d: %v", p.Name, seed, r.Violation)
			}
			for g := 0; g < r.Multi.Groups(); g++ {
				c := r.Multi.Group(g)
				alone := Replay(r.Multi.GroupConfig(g), c.Script())
				if alone.TraceHash != c.Trace().Hash() {
					t.Fatalf("%s seed %d: group %d replayed alone gives trace %s, in the multi-group run %s",
						p.Name, seed, g, alone.TraceHash[:16], c.Trace().Hash()[:16])
				}
			}
		}
	}
}

// TestMultiBreakOneGroupTheOtherContinues is the isolation scenario of the
// phase: two groups on three nodes; group 0's durable log fails on two of its
// three nodes — each fail-stops that group only, as the cluster-mode host
// does — so group 0 loses its quorum and commits nothing; group 1, on the same
// nodes, keeps electing and committing by its own quorum; restarted, group 0
// recovers from its own files.
func TestMultiBreakOneGroupTheOtherContinues(t *testing.T) {
	m, err := NewMulti(MultiConfig{Nodes: 3, Groups: 2, RF: 3, Seed: 11})
	if err != nil {
		t.Fatal(err)
	}
	g0, g1 := m.Group(0), m.Group(1)
	for _, c := range []*Cluster{g0, g1} {
		c.OnViolation = func(v *Violation) { t.Fatalf("%v\n%s", v, joinLines(c.Trace().Tail(40))) }
	}
	elect := func(g int, id NodeID) {
		c := m.Group(g)
		for i := 0; i < 4*raft.DefaultElectionTicks && c.State(id).Role != raft.Leader; i++ {
			m.Apply(MultiEvent{Group: g, Event: Event{Kind: Tick, Node: id}})
			c.DeliverAll()
		}
		if c.State(id).Role != raft.Leader {
			t.Fatalf("group %d: %s did not win", g, id)
		}
	}
	commit := func(g int, leader NodeID, data string) bool {
		c := m.Group(g)
		m.Apply(MultiEvent{Group: g, Event: Event{Kind: Propose, Node: leader, Data: data}})
		idx := c.State(leader).LastIndex
		for i := 0; i < 20; i++ {
			c.DeliverAll()
			if c.State(leader).Commit >= idx {
				return true
			}
			for k := 0; k < raft.DefaultHeartbeatTicks; k++ {
				m.Apply(MultiEvent{Group: g, Event: Event{Kind: Tick, Node: leader}})
			}
		}
		return false
	}
	elect(0, "n1")
	elect(1, "n1")
	if !commit(0, "n1", "g0-a") || !commit(1, "n1", "g1-a") {
		t.Fatal("premise: both groups commit")
	}
	// Break group 0: its log fails on n2 and n3.
	for _, id := range []NodeID{"n2", "n3"} {
		m.Apply(MultiEvent{Group: 0, Event: Event{Kind: FailPersist, Node: id, Op: FailSync}})
	}
	commit(0, "n1", "g0-lost")
	if g0.Up("n2") || g0.Up("n3") {
		t.Fatalf("group 0's failing replicas did not fail-stop: n2 %v n3 %v", g0.Up("n2"), g0.Up("n3"))
	}
	if !g1.Up("n2") || !g1.Up("n3") {
		t.Fatal("group 0's failure stopped group 1's replicas")
	}
	before := g0.State("n1").Commit
	for i := 0; i < 10; i++ {
		if !commit(1, "n1", fmt.Sprintf("g1-%d", i)) {
			t.Fatalf("group 1 stopped committing while group 0 is broken (round %d)", i)
		}
	}
	if g0.State("n1").Commit != before {
		t.Fatalf("group 0 committed without its quorum: %d -> %d", before, g0.State("n1").Commit)
	}
	// Group 0 recovers from its own files.
	for _, id := range []NodeID{"n2", "n3"} {
		m.Apply(MultiEvent{Group: 0, Event: Event{Kind: Restart, Node: id}})
	}
	m.Stabilize(400)
	if v, g := m.Violation(); v != nil {
		t.Fatalf("group %d: %v", g, v)
	}
}
