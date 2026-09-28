package raftnode

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/transport"
)

// Phase 15 driver tests: the group identity file, the group envelope, and
// membership changes on real nodes over TCP (docs/MEMBERSHIP.md §7).

// join starts id as a joiner of the running group: a new address, known to
// every running transport (the multi-Raft host's job in production), and a
// node started with Join — no genesis, no configuration until the leader adds
// it.
func (c *snapCluster) join(id NodeID) {
	c.t.Helper()
	c.addrs[id] = freeAddr(c.t)
	for _, tr := range c.trs {
		if tr != nil {
			if err := tr.AddPeer(transport.NodeID(id), c.addrs[id]); err != nil {
				c.t.Fatal(err)
			}
		}
	}
	c.ids = append(c.ids, id)
	if c.joiners == nil {
		c.joiners = map[NodeID]bool{}
	}
	c.joiners[id] = true
	c.mu.Lock() // the running nodes' Logf reads the map
	c.logs[id] = &strings.Builder{}
	c.mu.Unlock()
	c.start(id, nil)
}

// change runs one membership change on whichever of among leads, following
// leadership, until it completes.
func (c *snapCluster) change(among []NodeID, cc raft.ConfChange) replication.Configuration {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		conf, _, err := c.nodes[c.leader(among)].ChangeMembership(ctx, cc)
		cancel()
		if err == nil {
			return conf
		}
		if time.Now().After(deadline) || !(errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrConfChangeInProgress)) {
			c.t.Fatalf("%s: %v", cc, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitStatus waits until id's status satisfies ok.
func (c *snapCluster) waitStatus(id NodeID, what string, ok func(Status) bool) Status {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if st := c.nodes[id].Status(); ok(st) {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("%s never %s: %+v", id, what, c.nodes[id].Status())
	return Status{}
}

// TestAddPromoteAndRemoveTheLeader grows a three-node group by a joiner — first
// a learner that replicates and never votes, then a voter by joint consensus —
// and then removes the group's leader: it leads the change to its end, steps
// down and reports raft_removed, and the remaining voters elect a leader among
// themselves and keep committing. Every node's state machine applies the same
// sequence throughout.
func TestAddPromoteAndRemoveTheLeader(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 3, 0, 0)
	c.propose(c.ids, cmds("a", 5))
	c.converged(c.ids)

	c.join("n3")
	if st := c.nodes["n3"].Status(); st.Voter || !st.Conf.Empty() || st.Role != raft.Follower {
		t.Fatalf("a joiner before it is added: %+v", st)
	}
	conf := c.change(c.ids, raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "n3"}})
	if !conf.IsLearner("n3") || len(conf.Voters) != 3 {
		t.Fatalf("after AddLearner: %s", conf)
	}
	c.propose(c.ids, cmds("b", 5))
	c.converged(c.ids)
	c.waitStatus("n3", "learned it is a learner", func(st Status) bool { return st.Conf.IsLearner("n3") && !st.Voter })

	conf = c.change(c.ids, raft.ConfChange{Type: raft.Promote, Member: raft.Member{ID: "n3"}})
	if conf.Joint() || !conf.IsVoter("n3") || len(conf.Voters) != 4 {
		t.Fatalf("after Promote: %s", conf)
	}
	c.waitStatus("n3", "became a voter", func(st Status) bool { return st.Voter && !st.ConfPending })

	ld := c.leader(c.ids)
	conf = c.change(c.ids, raft.ConfChange{Type: raft.RemoveVoter, Member: raft.Member{ID: ld}})
	if conf.IsMember(ld) || len(conf.Voters) != 3 {
		t.Fatalf("after removing the leader %s: %s", ld, conf)
	}
	c.waitStatus(ld, "saw its removal", func(st Status) bool { return st.Removed && st.Role != raft.Leader })
	if !strings.Contains(c.log(ld), "event=raft_removed node="+string(ld)) {
		t.Fatalf("%s did not log its removal", ld)
	}
	rest := without(c.ids, ld)
	c.down(ld)
	c.propose(rest, cmds("c", 5))
	c.converged(rest)
	for _, id := range rest {
		if st := c.nodes[id].Status(); st.Conf.IsMember(ld) || st.ConfPending {
			t.Fatalf("%s: %+v", id, st)
		}
	}
}

// TestMembershipSurvivesRestarts: after a joiner is added and promoted, every
// node — restarted with no genesis named (the identity file supplies it), or
// the joiner with Join again — recovers the same configuration from its log;
// with snapshots every few entries the configuration comes back from the
// snapshot and the entries after it.
func TestMembershipSurvivesRestarts(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 3, 8, 2)
	c.propose(c.ids, cmds("a", 5))
	c.join("n3")
	c.change(c.ids, raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "n3"}})
	want := c.change(c.ids, raft.ConfChange{Type: raft.Promote, Member: raft.Member{ID: "n3"}})
	c.propose(c.ids, cmds("b", 20)) // enough to snapshot past the configuration entries
	c.converged(c.ids)
	for _, id := range c.ids {
		c.down(id)
	}
	for _, id := range c.ids {
		c.genesisless = true
		c.start(id, nil)
	}
	c.converged(c.ids)
	for _, id := range c.ids {
		st := c.nodes[id].Status()
		if !st.Conf.Equal(want) || st.ConfPending {
			t.Fatalf("%s recovered %+v, want %s", id, st, want)
		}
		if st.Snapshot == 0 {
			t.Fatalf("%s never snapshotted: the configuration came only from the log", id)
		}
	}
	c.propose(c.ids, cmds("c", 3))
	c.converged(c.ids)
}

// TestJoinerCatchesUpBySnapshotAndLearnsItsConfiguration: a joiner added after
// the group compacted past its first entries can only catch up by a snapshot;
// it adopts the snapshot's configuration (which already names it a learner)
// and, restarted, recovers it from the snapshot it installed.
func TestJoinerCatchesUpBySnapshotAndLearnsItsConfiguration(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 3, 8, 2)
	c.propose(c.ids, cmds("a", 30))
	c.converged(c.ids)
	c.compactedPast(c.ids, 5)
	c.join("n3")
	conf := c.change(c.ids, raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "n3"}})
	c.propose(c.ids, cmds("b", 12))
	c.converged(c.ids)
	c.waitStatus("n3", "installed a snapshot naming it", func(st Status) bool { return st.Snapshot > 0 && st.Conf.IsLearner("n3") })
	if _, _, restores := c.sms["n3"].state(); restores < 1 {
		t.Fatal("the joiner did not catch up by a snapshot")
	}
	// The snapshot it installed may predate its own addition: a configuration
	// without it, committed. That is a joiner not yet reached by its addition,
	// not a removed member — it must never be reported removed (the host would
	// retire the group under it; found by the real-process tests).
	if st := c.nodes["n3"].Status(); st.Removed || strings.Contains(c.log("n3"), "event=raft_removed") {
		t.Fatalf("a joiner was reported removed: %+v\n%s", st, c.log("n3"))
	}
	c.down("n3")
	c.start("n3", nil)
	c.propose(c.ids, cmds("c", 3))
	c.converged(c.ids)
	if st := c.nodes["n3"].Status(); !st.Conf.Equal(conf) || st.Voter {
		t.Fatalf("the restarted joiner: %+v, want %s", st, conf)
	}
}

// TestChangeMembershipRefusals: on a follower, ErrNotLeader; an operation that
// does not apply, ErrInvalidConfChange — and neither appends anything.
func TestChangeMembershipRefusals(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 3, 0, 0)
	c.propose(c.ids, cmds("a", 2))
	c.converged(c.ids)
	ld := c.leader(c.ids)
	f := without(c.ids, ld)[0]
	before := c.nodes[ld].Status().LastIndex
	if _, _, err := c.nodes[f].ChangeMembership(ctx, raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "n9"}}); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("on a follower: %v", err)
	}
	for _, cc := range []raft.ConfChange{
		{Type: raft.Promote, Member: raft.Member{ID: "n9"}},     // not a learner
		{Type: raft.RemoveLearner, Member: raft.Member{ID: f}},  // a voter, not a learner
		{Type: raft.AddLearner, Member: raft.Member{ID: f}},     // already a member
		{Type: raft.RemoveVoter, Member: raft.Member{ID: "n9"}}, // not a member
	} {
		if _, _, err := c.nodes[ld].ChangeMembership(ctx, cc); !errors.Is(err, raft.ErrInvalidConfChange) {
			t.Fatalf("%s: %v", cc, err)
		}
	}
	if after := c.nodes[ld].Status().LastIndex; after != before {
		t.Fatalf("a refused change appended: last index %d -> %d", before, after)
	}
}

// TestNodeDropsFramesOfOtherGroups: a frame whose envelope names another group,
// or does not decode, is dropped (logged) and never stepped; one of the node's
// own group is stepped.
func TestNodeDropsFramesOfOtherGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := &preloadedTransport{id: "a", ch: make(chan transport.Envelope, 4)}
	msg := raft.Message{Type: raft.MsgAppendRequest, Term: 9, From: "b", To: "a"}.Marshal()
	tr.ch <- transport.Envelope{Peer: "b", Kind: transport.MsgAppendEntries, Payload: WrapGroup(3, msg)}
	tr.ch <- transport.Envelope{Peer: "b", Kind: transport.MsgAppendEntries, Payload: []byte{0x80}}
	var logs strings.Builder
	var mu = make(chan struct{}, 1)
	mu <- struct{}{}
	n, err := Start(ctx, Config{ID: "a", Group: 2, Peers: []NodeID{"a", "b", "c"}, Transport: tr,
		LogPath: filepath.Join(t.TempDir(), "a.log"), TickInterval: time.Hour, DisableSync: true,
		Logf: func(f string, a ...any) { <-mu; logs.WriteString(f + "\n"); mu <- struct{}{} }})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	// Another group's frame, then a malformed one, then the node's own.
	tr.ch <- transport.Envelope{Peer: "b", Kind: transport.MsgAppendEntries, Payload: WrapGroup(2, msg)}
	deadline := time.Now().Add(5 * time.Second)
	for n.Status().Term != 9 {
		if time.Now().After(deadline) {
			t.Fatalf("the node's own frame was never stepped: %+v", n.Status())
		}
		time.Sleep(time.Millisecond)
	}
	<-mu
	got := strings.Count(logs.String(), "event=raft_frame_dropped")
	mu <- struct{}{}
	if got != 2 {
		t.Fatalf("%d frames dropped, want 2", got)
	}
}

// TestEnvelopeRoundTripAndRefusals: every group id round-trips; an empty
// frame, a truncated or non-canonical varint and an id over 32 bits are
// refused.
func TestEnvelopeRoundTripAndRefusals(t *testing.T) {
	for _, g := range []replication.GroupID{0, 1, 127, 128, 1 << 20, 1<<32 - 1} {
		w := WrapGroup(g, []byte("payload"))
		got, p, err := UnwrapGroup(w)
		if err != nil || got != g || string(p) != "payload" {
			t.Fatalf("group %d: %d %q %v", g, got, p, err)
		}
	}
	for name, b := range map[string][]byte{
		"empty":         {},
		"truncated":     {0x80},
		"non-canonical": {0x80, 0x00, 'x'},
		"over 32 bits":  {0x80, 0x80, 0x80, 0x80, 0x10},
	} {
		if _, _, err := UnwrapGroup(b); !errors.Is(err, ErrEnvelope) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// FuzzUnwrapGroup: the envelope decoder is total and canonical.
func FuzzUnwrapGroup(f *testing.F) {
	f.Add(WrapGroup(7, []byte("x")))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		g, p, err := UnwrapGroup(b)
		if err != nil {
			return
		}
		if !bytes.Equal(WrapGroup(g, p), b) {
			t.Fatal("accepted a non-canonical envelope")
		}
	})
}

// TestIdentityFileRules pins the group identity file (docs/MEMBERSHIP.md §2):
// a first start records it; a restart naming no genesis, or the same one,
// reads it; a restart naming another genesis, another group, or a joiner's
// empty genesis over a bootstrap member's is refused; durable state without
// the file is refused; a first start naming no genesis is refused; naming two
// is refused; an orphaned temporary is removed.
func TestIdentityFileRules(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "n0.log")
	start := func(cfg Config) error {
		t.Helper()
		cfg.ID, cfg.LogPath, cfg.Rand = "n0", path, newRand()
		rc, err := Recover(cfg)
		if err == nil {
			rc.Log.Close()
		}
		return err
	}
	if err := start(Config{}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("a first start naming no genesis: %v", err)
	}
	if err := start(Config{Peers: []NodeID{"n0"}, Join: true}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("naming two geneses: %v", err)
	}
	boot := replication.Configuration{Voters: []replication.Member{{ID: "n0", Addr: "h:0"}, {ID: "n1", Addr: "h:1"}}}
	if err := start(Config{Group: 4, Bootstrap: &boot}); err != nil {
		t.Fatalf("first start: %v", err)
	}
	id, found, err := LoadIdentity(nil, path)
	if err != nil || !found || id.Group != 4 || !id.Genesis.Equal(boot) {
		t.Fatalf("recorded identity: %+v %v %v", id, found, err)
	}
	if err := start(Config{Group: 4}); err != nil {
		t.Fatalf("a restart naming no genesis: %v", err)
	}
	if err := start(Config{Group: 4, Bootstrap: &boot}); err != nil {
		t.Fatalf("a restart naming the same genesis: %v", err)
	}
	other := replication.Configuration{Voters: boot.Voters[:1]}
	for name, cfg := range map[string]Config{
		"another genesis": {Group: 4, Bootstrap: &other},
		"another group":   {Group: 5},
		"as a joiner":     {Group: 4, Join: true},
		"as Peers":        {Group: 4, Peers: []NodeID{"n0", "n1"}}, // same ids, no addresses: another configuration
	} {
		if err := start(cfg); !errors.Is(err, ErrIdentity) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A log without the identity file is refused.
	if err := os.Remove(identityPath(path)); err != nil {
		t.Fatal(err)
	}
	if err := start(Config{Group: 4, Bootstrap: &boot}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("a log without the identity file: %v", err)
	}
	// An orphaned temporary is removed, and does not count as an identity.
	fresh := filepath.Join(dir, "n1.log")
	if err := os.WriteFile(identityTmpPath(fresh), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	rc, err := Recover(Config{ID: "n0", LogPath: fresh, Rand: newRand(), Join: true})
	if err != nil {
		t.Fatal(err)
	}
	rc.Log.Close()
	if _, err := os.Stat(identityTmpPath(fresh)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the orphan survived: %v", err)
	}
	if c, _ := rc.Core.Conf(); !c.Empty() || rc.Core.IsVoter() {
		t.Fatalf("a joiner's configuration: %s", c)
	}
}

// TestIdentityFileCorruptionIsRefused: every truncation and every bit flip of
// an identity file, and a file of a future version, refuse to start.
func TestIdentityFileCorruptionIsRefused(t *testing.T) {
	good, err := EncodeIdentity(Identity{Group: 9, Genesis: replication.Configuration{Voters: []replication.Member{{ID: "n0", Addr: "h:0"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeIdentity(good); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(good); n++ {
		if _, err := DecodeIdentity(good[:n]); !errors.Is(err, ErrIdentity) {
			t.Fatalf("truncated to %d of %d: %v", n, len(good), err)
		}
	}
	for i := range good {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), good...)
			bad[i] ^= 1 << bit
			if _, err := DecodeIdentity(bad); !errors.Is(err, ErrIdentity) {
				t.Fatalf("bit %d of byte %d: %v", bit, i, err)
			}
		}
	}
	future, _ := record.Encode(nil, identityKind, append([]byte(identityMagic), 2, 9, 0))
	trailing := append(append([]byte(nil), good...), 0)
	for name, b := range map[string][]byte{"future version": future, "trailing byte": trailing} {
		if _, err := DecodeIdentity(b); !errors.Is(err, ErrIdentity) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// And through Recover, on a MemFS.
	fs := fault.NewMemFS()
	if err := writeIdentity(fs, "/n/raft.log", Identity{Genesis: replication.VotersOf([]NodeID{"n0"})}); err != nil {
		t.Fatal(err)
	}
	f, err := fs.OpenFile(identityPath("/n/raft.log"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xff}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := Recover(Config{ID: "n0", LogPath: "/n/raft.log", FS: fs, Rand: newRand()}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("Recover over a corrupt identity file: %v", err)
	}
}

// FuzzDecodeIdentity: the identity decoder is total, and whatever it accepts
// re-encodes to the same bytes.
func FuzzDecodeIdentity(f *testing.F) {
	good, _ := EncodeIdentity(Identity{Group: 3, Genesis: replication.VotersOf([]NodeID{"a", "b"})})
	f.Add(good)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		id, err := DecodeIdentity(b)
		if err != nil {
			return
		}
		again, err := EncodeIdentity(id)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("accepted a non-canonical identity: %v", err)
		}
	})
}

// TestJoinerInstallingASnapshotThatPredatesItIsNotRemoved pins the bug the
// real-process tests found: the only snapshot the leader can offer a joiner
// predates the joiner's addition (snapshots every 20 entries; the addition is
// entry ~32), so the joiner installs a committed configuration that does not
// name it — a joiner not yet reached by its addition, NOT a removed member.
// Reporting it removed made the host retire the group under it, and the
// joiner never caught up. It is never reported removed; it learns it is a
// learner from the entries after the snapshot, and catches up.
func TestJoinerInstallingASnapshotThatPredatesItIsNotRemoved(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 3, 20, 2)
	c.propose(c.ids, cmds("a", 30))
	c.converged(c.ids)
	c.compactedPast(c.ids, 10)
	for _, id := range c.ids {
		if s := c.nodes[id].Status().Snapshot; s >= 30 {
			t.Fatalf("premise: %s's snapshot %d is not below the addition", id, s)
		}
	}
	c.join("n3")
	c.change(c.ids, raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "n3"}})
	c.propose(c.ids, cmds("b", 3))
	c.converged(c.ids)
	st := c.waitStatus("n3", "installed a snapshot and learned it is a learner", func(st Status) bool {
		return st.Snapshot > 0 && st.Conf.IsLearner("n3")
	})
	if st.Snapshot >= 30 {
		t.Fatalf("premise: the joiner installed snapshot %d, which names it already", st.Snapshot)
	}
	if st.Removed || strings.Contains(c.log("n3"), "event=raft_removed") {
		t.Fatalf("a joiner was reported removed: %+v\n%s", st, c.log("n3"))
	}
}

// TestChangeMembershipReportsALostChange: a leader cut off from its group
// accepts a change — its configuration entry cannot commit — while the others
// elect a leader and overwrite that index. When the cut-off node rejoins, its
// entry is truncated and its ChangeMembership answers ErrConfLost: a definite
// "did not happen", never success, and the configuration is the group's.
func TestChangeMembershipReportsALostChange(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 3, 0, 0)
	c.propose(c.ids, cmds("a", 3))
	c.converged(c.ids)
	old := c.leader(c.ids)
	rest := without(c.ids, old)
	cut := func(on bool) {
		for _, a := range c.ids {
			for _, b := range c.ids {
				if a == b || (a != old && b != old) {
					continue
				}
				if on {
					_ = c.trs[a].RemovePeer(transport.NodeID(b))
				} else if err := c.trs[a].AddPeer(transport.NodeID(b), c.addrs[b]); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	cut(true)
	result := make(chan error, 1)
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, _, err := c.nodes[old].ChangeMembership(cctx, raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "n9"}})
		result <- err
	}()
	c.waitStatus(old, "appended the change", func(st Status) bool { return st.Conf.IsLearner("n9") && st.ConfPending })
	c.propose(rest, cmds("b", 3)) // the others elect a leader and write over the index
	cut(false)
	select {
	case err := <-result:
		if !errors.Is(err, ErrConfLost) {
			t.Fatalf("the overwritten change answered %v, want ErrConfLost", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the overwritten change never answered")
	}
	c.converged(c.ids)
	for _, id := range c.ids {
		if st := c.nodes[id].Status(); st.Conf.IsMember("n9") {
			t.Fatalf("%s holds the lost configuration %s", id, st.Conf)
		}
	}
}
