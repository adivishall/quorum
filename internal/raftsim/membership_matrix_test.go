package raftsim

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
)

var flagMembershipMatrixOut = flag.String("raftsim.membership-matrix.out", "", "write the membership crash matrix report (JSON) to this file")

// membershipScenario is the crash matrix's first story (docs/MEMBERSHIP.md):
// a spare joins — by a snapshot, the log being compacted — is promoted, a
// follower snapshots, and the leader removes itself; the remaining voters
// elect a leader, and the removed node restarts. Every configuration entry is
// proposed, persisted, replicated, committed and applied; a joint one is
// finalized; a snapshot is created and installed; nodes restart.
func membershipScenario(t *testing.T, cfg Config) []Event {
	t.Helper()
	s := newSimWith(t, cfg)
	s.electLeader("n1")
	for i := 0; i < 6; i++ {
		s.commit("n1", fmt.Sprintf("a%d", i), "n1", "n2", "n3")
	}
	s.member("n1", "addlearner", "n4")
	s.settleConf("n1")
	s.commit("n1", "b", "n1", "n2", "n3", "n4")
	s.member("n1", "promote", "n4")
	s.settleConf("n1")
	s.do(Event{Kind: SnapshotNow, Node: "n2"})
	s.member("n1", "removevoter", "n1")
	for i := 0; i < 50 && s.State("n1").Role == raft.Leader; i++ {
		s.heartbeat("n1")
	}
	l := s.electAmong("n2", "n3", "n4")
	s.commit(l, "c", "n2", "n3", "n4")
	s.do(Event{Kind: Crash, Node: "n1"})
	s.restart("n1")
	s.commit(l, "end", "n2", "n3", "n4")
	return s.Script()
}

// replacementScenario is the second: the old member n3 is partitioned away
// and replaced by the spare n4 while it is cut off; healed, it campaigns on
// its stale configuration, and the group carries on.
func replacementScenario(t *testing.T, cfg Config) []Event {
	t.Helper()
	s := newSimWith(t, cfg)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: Isolate, Node: "n3"})
	s.member("n1", "addlearner", "n4")
	s.settleConf("n1", "n3")
	s.member("n1", "promote", "n4")
	s.settleConf("n1", "n3")
	s.member("n1", "removevoter", "n3")
	s.settleConf("n1", "n3")
	s.do(Event{Kind: HealAll})
	s.tick("n3", 3*10)
	s.DeliverAll()
	s.commit("n1", "end", "n1", "n2", "n4")
	return s.Script()
}

// lostChangeScenario is the third: two changes that never happen. First the
// leader's Save of a configuration entry tears (a short write): it fail-stops,
// and its restart truncates the torn record — the configuration is as if the
// change was never proposed. Then a leader proposes a change that reaches no
// one else and is cut off; the others elect a leader that overwrites the
// entry, and the old leader's configuration reverts when it rejoins
// (docs/MEMBERSHIP.md §2). The new leader then makes the change for real.
func lostChangeScenario(t *testing.T, cfg Config) []Event {
	t.Helper()
	s := newSimWith(t, cfg)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: FailPersist, Node: "n1", Op: ShortWrite, N: 6})
	s.member("n1", "addlearner", "n4")
	s.requireDown("n1", "the torn Save of the configuration entry did not fail-stop n1")
	s.restart("n1")
	if c := s.State("n1").Conf; c.IsMember("n4") {
		t.Fatalf("premise: n1 recovered the torn configuration entry: %s", c)
	}
	l := s.electAmong("n1", "n2", "n3")
	s.commit(l, "b", "n1", "n2", "n3")
	s.member(l, "addlearner", "n4")
	s.dropAll(func(raft.Message) bool { return true })
	s.do(Event{Kind: Isolate, Node: l})
	var rest []NodeID
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		if id != l {
			rest = append(rest, id)
		}
	}
	l2 := s.electAmong(rest...)
	s.commit(l2, "c", rest...)
	s.do(Event{Kind: HealAll})
	s.settleConf(l2)
	if c := s.State(l).Conf; c.IsMember("n4") {
		t.Fatalf("premise: %s kept its overwritten configuration %s", l, c)
	}
	s.member(l2, "addlearner", "n4")
	s.settleConf(l2)
	s.commit(l2, "end", "n1", "n2", "n3", "n4")
	return s.Script()
}

// TestMembershipCrashMatrix is the bounded membership crash matrix (Phase 15):
// every crash point every node reaches in each scenario — the old members,
// the new one, the leader — at every driver point and I/O boundary of the
// proposal, persistence, replication, commit, application, snapshot,
// installation and finalization of configuration entries, in every crash
// mode; each crashed node restarts at once, the run stabilizes, every
// invariant (the INV-M series included) holds throughout, and the group
// converges with no change left under way. Each row records the group, the
// transition under way, the configuration before, the one reached and the
// one the crashed node recovered (-raftsim.membership-matrix.out writes the
// report as JSON).
func TestMembershipCrashMatrix(t *testing.T) {
	cfg := Config{Nodes: 4, Genesis: 3, Seed: 71, SnapshotEvery: 5, SnapshotRetain: 1, ChunkSize: 64}
	noSnap := Config{Nodes: 4, Genesis: 3, Seed: 72} // caught up by entries: the overwritten entry is truncated
	var all MatrixReport
	all.Nodes, all.Seed = cfg.Nodes, cfg.Seed
	for _, sc := range []struct {
		name  string
		cfg   Config
		build func(*testing.T, Config) []Event
	}{
		{"add-promote-remove-leader", cfg, membershipScenario},
		{"replace-partitioned-member", cfg, replacementScenario},
		{"lost-change-overwritten", noSnap, lostChangeScenario},
	} {
		name, cfg, build := sc.name, sc.cfg, sc.build
		scenario := build(t, cfg)
		rep, err := RunCrashMatrix(cfg, scenario, DefaultCrashModes)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		t.Logf("%s:\n%s", name, rep.Summary())
		if fails := rep.Failures(); len(fails) > 0 {
			shown := fails
			if len(shown) > 12 {
				shown = shown[:12]
			}
			t.Fatalf("%s: %d of %d crashes were not survived (seed=%d; reproduce a row by arming its event first):\n%s",
				name, len(fails), len(rep.Rows), cfg.Seed, (&MatrixReport{Rows: shown}).Text())
		}
		all.Rows = append(all.Rows, rep.Rows...)
		all.Scenario += rep.Scenario
	}
	byPoint, byMode := all.Coverage()
	for _, p := range raftnode.Points {
		if byPoint[p.String()] == 0 {
			t.Errorf("the membership scenarios never reached driver point %s", p)
		}
	}
	for _, p := range append(append([]string(nil), IOPoints...), SnapshotIOPoints...) {
		if byPoint[p] == 0 {
			t.Errorf("the membership scenarios never reached I/O point %s", p)
		}
	}
	if len(byMode) != len(DefaultCrashModes) {
		t.Errorf("modes covered: %v", byMode)
	}
	byTransition, byNode := map[string]int{}, map[string]int{}
	for _, row := range all.Rows {
		byTransition[row.Transition]++
		byNode[row.Node]++
	}
	for _, tr := range []string{"stable", "change proposed", "joint proposed", "joint committed, final pending"} {
		if byTransition[tr] == 0 {
			t.Errorf("no crash during transition %q (%v)", tr, byTransition)
		}
	}
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		if byNode[id] == 0 {
			t.Errorf("no crash on %s", id)
		}
	}
	var keys []string
	for k := range byTransition {
		keys = append(keys, fmt.Sprintf("%s=%d", k, byTransition[k]))
	}
	sort.Strings(keys)
	t.Logf("membership crash matrix: %d crashes; by transition: %s", len(all.Rows), strings.Join(keys, ", "))
	if *flagMembershipMatrixOut != "" {
		js, err := all.JSON()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(*flagMembershipMatrixOut, js, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
