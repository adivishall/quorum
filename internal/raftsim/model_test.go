package raftsim

import (
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/raft"
)

var flagModelDepth = flag.Int("raftsim.model-depth", 3, "sequence length of the bounded membership model (TestMembershipBoundedModel)")

// The bounded membership model (Phase 15): every sequence of up to depth steps
// over a small alphabet of membership changes and faults, on four nodes of
// which three are the genesis voters. Each step is applied, then the group
// runs a few rounds; every continuous invariant — the INV-R, INV-F, INV-SN and
// INV-M series — is checked after every event; at the end the group is
// stabilized and must converge with no change under way. A step that does not
// apply in the state it meets (no leader to ask, a change already under way,
// nothing to promote) is a no-op, exactly as the Member event is refused.

type modelStep struct {
	name string
	do   func(c *Cluster)
}

func leaderOf(c *Cluster) NodeID {
	if ls := c.Leaders(); len(ls) == 1 {
		return ls[0]
	}
	return ""
}

func memberOp(op string, pick func(c *Cluster, l NodeID, conf raft.Configuration) NodeID) func(c *Cluster) {
	return func(c *Cluster) {
		l := leaderOf(c)
		if l == "" {
			return
		}
		conf := c.State(l).Conf
		if target := pick(c, l, conf); target != "" {
			c.Apply(Event{Kind: Member, Node: l, Data: op, To: target})
		}
	}
}

func firstFollowerVoter(c *Cluster, l NodeID, conf raft.Configuration) NodeID {
	for _, id := range conf.VoterIDs() {
		if id != l {
			return id
		}
	}
	return ""
}

var modelSteps = []modelStep{
	{"add", memberOp("addlearner", func(c *Cluster, _ NodeID, conf raft.Configuration) NodeID {
		for _, id := range c.IDs() {
			if !conf.IsMember(id) {
				return id
			}
		}
		return ""
	})},
	{"promote", memberOp("promote", func(_ *Cluster, _ NodeID, conf raft.Configuration) NodeID {
		if len(conf.Learners) > 0 {
			return conf.Learners[0].ID
		}
		return ""
	})},
	{"remove-leader", memberOp("removevoter", func(_ *Cluster, l NodeID, conf raft.Configuration) NodeID {
		if len(conf.Voters) > 1 {
			return l
		}
		return ""
	})},
	{"remove-follower", memberOp("removevoter", firstFollowerVoter)},
	{"crash-leader", func(c *Cluster) {
		if l := leaderOf(c); l != "" {
			c.Apply(Event{Kind: Crash, Node: l, Power: true, N: 7})
		}
	}},
	{"crash-follower", func(c *Cluster) {
		if l := leaderOf(c); l != "" {
			if f := firstFollowerVoter(c, l, c.State(l).Conf); f != "" {
				c.Apply(Event{Kind: Crash, Node: f})
			}
		}
	}},
	{"restart", func(c *Cluster) {
		for _, id := range c.IDs() {
			if !c.Up(id) {
				c.Apply(Event{Kind: Restart, Node: id})
			}
		}
	}},
	{"snapshot", func(c *Cluster) {
		for _, id := range c.IDs() {
			if c.Up(id) {
				c.Apply(Event{Kind: SnapshotNow, Node: id})
			}
		}
	}},
	{"partition-leader", func(c *Cluster) {
		if l := leaderOf(c); l != "" {
			c.Apply(Event{Kind: Isolate, Node: l})
		}
	}},
	{"heal", func(c *Cluster) { c.Apply(Event{Kind: HealAll}) }},
	{"propose", func(c *Cluster) {
		if l := leaderOf(c); l != "" {
			c.Apply(Event{Kind: Propose, Node: l, Data: fmt.Sprintf("p%d", c.Step())})
		}
	}},
}

// rounds ticks every running node and delivers everything, n times.
func rounds(c *Cluster, n int) {
	for r := 0; r < n && c.Violation() == nil; r++ {
		for _, id := range c.IDs() {
			if c.Up(id) && !c.Paused(id) {
				c.Apply(Event{Kind: Tick, Node: id})
			}
		}
		c.DeliverAll()
	}
}

func runModelSequence(t *testing.T, seq []int) {
	t.Helper()
	c, err := New(Config{Nodes: 4, Genesis: 3, Seed: 5, SnapshotEvery: 6, SnapshotRetain: 1})
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(seq))
	for i, k := range seq {
		names[i] = modelSteps[k].name
	}
	// A leader first, and a committed entry: the state every sequence starts
	// from.
	rounds(c, 3*raft.DefaultElectionTicks)
	if l := leaderOf(c); l != "" {
		c.Apply(Event{Kind: Propose, Node: l, Data: "start"})
	}
	rounds(c, 4)
	for _, k := range seq {
		modelSteps[k].do(c)
		rounds(c, 2*raft.DefaultElectionTicks)
	}
	c.Stabilize(400)
	if v := c.Violation(); v != nil {
		t.Fatalf("sequence [%s]: %v\n--- last trace lines ---\n%s\n--- script ---\n%s",
			strings.Join(names, " "), v, strings.Join(c.Trace().Tail(60), "\n"), FormatScript(c.Script()))
	}
}

// TestMembershipBoundedModel runs every sequence of length depth (and, through
// the shorter prefixes' runs, every shorter one) over the alphabet above.
func TestMembershipBoundedModel(t *testing.T) {
	depth := *flagModelDepth
	if testing.Short() && depth > 2 {
		depth = 2
	}
	n := len(modelSteps)
	total := 1
	for i := 0; i < depth; i++ {
		total *= n
	}
	seq := make([]int, depth)
	for x := 0; x < total; x++ {
		v := x
		for i := range seq {
			seq[i] = v % n
			v /= n
		}
		runModelSequence(t, seq)
	}
	t.Logf("bounded membership model: %d sequences of %d steps over %d steps", total, depth, n)
}
