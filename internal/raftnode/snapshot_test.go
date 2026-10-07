package raftnode

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/snapshot"
	"github.com/adivishall/quorum/internal/transport"
)

// Phase 14: snapshots in the real driver — real files, the real actor, real
// TCP (docs/SNAPSHOTS.md). The exhaustive crash matrix is the simulator's
// (internal/raftsim); these prove the running node honours the same orderings.

// snapSM is a state machine with snapshots whose state is every command it
// has applied, in order. Apply insists on the next index, so a skipped or
// repeated entry around a snapshot, a restore or an install fails loudly.
type snapSM struct {
	mu       sync.Mutex
	index    uint64
	cmds     []string
	restores int
}

func (s *snapSM) Apply(index uint64, command []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index != s.index+1 {
		return fmt.Errorf("snapSM: apply %d after %d", index, s.index)
	}
	s.index = index
	s.cmds = append(s.cmds, string(command))
	return nil
}

func (s *snapSM) EncodeSnapshot() (uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := binary.AppendUvarint(nil, s.index)
	for _, c := range s.cmds {
		b = binary.AppendUvarint(b, uint64(len(c)))
		b = append(b, c...)
	}
	return s.index, b, nil
}

func decodeSnapSM(index uint64, data []byte) ([]string, error) {
	got, n := binary.Uvarint(data)
	if n <= 0 || got != index {
		return nil, fmt.Errorf("snapSM: state at %d, snapshot at %d", got, index)
	}
	data = data[n:]
	var cmds []string
	for len(data) > 0 {
		l, n := binary.Uvarint(data)
		if n <= 0 || uint64(len(data)-n) < l {
			return nil, errors.New("snapSM: malformed")
		}
		cmds = append(cmds, string(data[n:n+int(l)]))
		data = data[n+int(l):]
	}
	if uint64(len(cmds)) != index {
		return nil, fmt.Errorf("snapSM: %d commands at index %d", len(cmds), index)
	}
	return cmds, nil
}

func (s *snapSM) ValidateSnapshot(index uint64, data []byte) error {
	_, err := decodeSnapSM(index, data)
	return err
}

func (s *snapSM) RestoreSnapshot(index, _ uint64, data []byte) error {
	cmds, err := decodeSnapSM(index, data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index, s.cmds = index, cmds
	s.restores++
	return nil
}

func (s *snapSM) state() (uint64, []string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.index, append([]string(nil), s.cmds...), s.restores
}

func newRand() *rand.Rand { return rand.New(rand.NewSource(1)) }

// userCommands are the non-empty commands (the leaders' no-ops are empty).
func userCommands(cmds []string) []string {
	var out []string
	for _, c := range cmds {
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

// TestReconcileTable is docs/SNAPSHOTS.md §6, row by row.
func TestReconcileTable(t *testing.T) {
	e := func(i, term uint64) raftlog.Entry { return raftlog.Entry{Index: i, Term: term} }
	snap := snapshot.Meta{Index: 10, Term: 3}
	for _, tc := range []struct {
		name   string
		rec    raftlog.Recovered
		repair bool
		refuse bool
	}{
		{"boundary at the snapshot", raftlog.Recovered{Boundary: raftlog.Boundary{Index: 10, Term: 3}, Entries: []raftlog.Entry{e(11, 3)}}, false, false},
		{"log holds the snapshot's entry (retained prefix)", raftlog.Recovered{Boundary: raftlog.Boundary{Index: 7, Term: 2}, Entries: []raftlog.Entry{e(8, 2), e(9, 3), e(10, 3), e(11, 3)}}, false, false},
		{"uncompacted log holds it", raftlog.Recovered{Entries: []raftlog.Entry{e(1, 1), e(2, 1), e(3, 1), e(4, 1), e(5, 1), e(6, 1), e(7, 1), e(8, 1), e(9, 3), e(10, 3)}}, false, false},
		{"log does not reach it: complete the install", raftlog.Recovered{Entries: []raftlog.Entry{e(1, 1), e(2, 1)}, HardState: raftlog.HardState{Commit: 2}}, true, false},
		{"another term there, uncommitted: complete the install", raftlog.Recovered{Boundary: raftlog.Boundary{Index: 8, Term: 2}, Entries: []raftlog.Entry{e(9, 2), e(10, 2)}, HardState: raftlog.HardState{Commit: 9}}, true, false},
		{"another term there, committed: refuse", raftlog.Recovered{Boundary: raftlog.Boundary{Index: 8, Term: 2}, Entries: []raftlog.Entry{e(9, 2), e(10, 2)}, HardState: raftlog.HardState{Commit: 10}}, false, true},
		{"compacted past the snapshot: refuse", raftlog.Recovered{Boundary: raftlog.Boundary{Index: 11, Term: 3}}, false, true},
		{"boundary at the snapshot with another term: refuse", raftlog.Recovered{Boundary: raftlog.Boundary{Index: 10, Term: 2}}, false, true},
	} {
		repair, err := reconcile(snap, &tc.rec)
		if tc.refuse != (err != nil) || (err == nil && repair != tc.repair) {
			t.Errorf("%s: repair=%v err=%v", tc.name, repair, err)
		}
		if err != nil && !errors.Is(err, ErrSnapshot) {
			t.Errorf("%s: %v does not wrap ErrSnapshot", tc.name, err)
		}
	}
}

// startSnapSingle starts a durable single-node group with snapshots.
func startSnapSingle(t *testing.T, ctx context.Context, logPath string, sm *snapSM, every, retain uint64, hook Hook, logf func(string, ...any)) (*Node, *transport.TCPTransport) {
	t.Helper()
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "n0", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := Start(ctx, Config{
		ID: "n0", Peers: []NodeID{"n0"}, Transport: tr, LogPath: logPath,
		StateMachine: sm, TickInterval: 10 * time.Millisecond, Hook: hook, Logf: logf,
		SnapshotEvery: every, SnapshotRetain: retain,
	})
	if err != nil {
		tr.Close()
		t.Fatal(err)
	}
	return n, tr
}

func waitApplied(t *testing.T, n *Node, idx uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().Applied >= idx {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("applied %d, want %d", n.Status().Applied, idx)
}

// TestRecoverFromSnapshotAndSuffix: a node snapshots every 10 entries and
// compacts its log behind each snapshot; a restart restores the latest
// snapshot and applies only the suffix after it, reaching the identical
// state; the durable log holds only the suffix.
func TestRecoverFromSnapshotAndSuffix(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "raft.log")
	sm := &snapSM{}
	n, tr := startSnapSingle(t, ctx, path, sm, 10, 0, nil, nil)
	waitLeaderNode(t, n)
	for i := 0; i < 35; i++ {
		if err := n.Propose(ctx, []byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	last := n.Status().LastIndex
	waitApplied(t, n, last)
	st := n.Status()
	if st.Snapshot < 30 || st.Boundary != st.Snapshot {
		t.Fatalf("status %+v: want a snapshot at ≥ 30 and the log compacted to it", st)
	}
	_, want, _ := sm.state()
	n.Close()
	tr.Close()

	rec, err := raftlog.Inspect(path)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Boundary.Index != st.Snapshot || rec.LastIndex() != last {
		t.Fatalf("durable log: boundary %+v last %d, want boundary %d last %d", rec.Boundary, rec.LastIndex(), st.Snapshot, last)
	}

	sm2 := &snapSM{}
	n2, tr2 := startSnapSingle(t, ctx, path, sm2, 10, 0, nil, nil)
	defer tr2.Close()
	defer n2.Close()
	waitApplied(t, n2, last)
	_, got, restores := sm2.state()
	if restores != 1 {
		t.Fatalf("restored %d times, want once", restores)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("recovered state differs:\n got %v\nwant %v", got, want)
	}
}

// TestSnapshotCrashPointsRecover crashes the node at each boundary of snapshot
// creation (docs/SNAPSHOTS.md §5) and restarts it: every committed command is
// in the recovered state exactly once and in order (snapSM refuses anything
// else), and the node keeps working and snapshotting.
func TestSnapshotCrashPointsRecover(t *testing.T) {
	for _, p := range []Point{BeforeSnapshotPublish, AfterSnapshotPublish, AfterLogCompact} {
		t.Run(p.String(), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "raft.log")
			hook, fired := crashHook(p, 2) // the second snapshot: one is already published
			sm := &snapSM{}
			n, tr := startSnapSingle(t, ctx, path, sm, 8, 2, hook, nil)
			waitLeaderNode(t, n)
			var proposed []string
		loop:
			for i := 0; i < 40; i++ {
				c := fmt.Sprintf("c%d", i)
				select {
				case <-fired:
					break loop
				default:
				}
				if err := n.Propose(ctx, []byte(c)); err != nil {
					break // the crash point fired during this proposal's cycle
				}
				proposed = append(proposed, c)
			}
			select {
			case <-fired:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s never reached", p)
			}
			<-n.Done()
			n.Close()
			tr.Close()

			sm2 := &snapSM{}
			n2, tr2 := startSnapSingle(t, ctx, path, sm2, 8, 2, nil, nil)
			defer tr2.Close()
			defer n2.Close()
			waitLeaderNode(t, n2)
			if err := n2.Propose(ctx, []byte("after")); err != nil {
				t.Fatal(err)
			}
			waitApplied(t, n2, n2.Status().LastIndex)
			_, got, _ := sm2.state()
			user := userCommands(got)
			// Every acknowledged proposal survived, in order; "after" follows.
			if len(user) < len(proposed)+1 || strings.Join(user[:len(proposed)], ",") != strings.Join(proposed, ",") || user[len(user)-1] != "after" {
				t.Fatalf("recovered %v, proposed %v then after", user, proposed)
			}
			if n2.Status().Snapshot == 0 {
				t.Fatal("no snapshot survived the crash")
			}
		})
	}
}

// snapCluster is a TCP cluster whose nodes snapshot, and can be restarted.
type snapCluster struct {
	t             *testing.T
	ctx           context.Context
	dir           string
	ids           []NodeID
	addrs         map[NodeID]string
	trs           map[NodeID]*transport.TCPTransport
	nodes         map[NodeID]*Node
	sms           map[NodeID]*snapSM
	every, retain uint64
	logs          map[NodeID]*strings.Builder
	mu            sync.Mutex
	// Phase 15: the genesis voters; nodes started as joiners; and whether a
	// (re)start names no genesis at all, relying on the identity file.
	genesis     []NodeID
	joiners     map[NodeID]bool
	genesisless bool
	// Phase 16: when set, each node runs with its own registry (a process
	// each, as in production), kept across restarts.
	regs map[NodeID]*metrics.Registry
	mets map[NodeID]*Metrics
}

// withMetrics gives every node started from now on a registry of its own.
func (c *snapCluster) withMetrics() {
	c.regs, c.mets = map[NodeID]*metrics.Registry{}, map[NodeID]*Metrics{}
}

// metricsFor returns id's Metrics (nil without withMetrics).
func (c *snapCluster) metricsFor(id NodeID) *Metrics {
	if c.regs == nil {
		return nil
	}
	if c.mets[id] == nil {
		c.regs[id] = metrics.NewRegistry()
		c.mets[id] = NewMetrics(c.regs[id])
	}
	return c.mets[id]
}

func startSnapCluster(t *testing.T, ctx context.Context, n int, every, retain uint64) *snapCluster {
	t.Helper()
	c := &snapCluster{t: t, ctx: ctx, dir: t.TempDir(), addrs: map[NodeID]string{}, trs: map[NodeID]*transport.TCPTransport{},
		nodes: map[NodeID]*Node{}, sms: map[NodeID]*snapSM{}, every: every, retain: retain, logs: map[NodeID]*strings.Builder{}}
	for i := 0; i < n; i++ {
		id := NodeID(fmt.Sprintf("n%d", i))
		c.ids = append(c.ids, id)
		c.addrs[id] = freeAddr(t)
	}
	c.genesis = append([]NodeID(nil), c.ids...)
	t.Cleanup(c.stop)
	for _, id := range c.ids {
		c.logs[id] = &strings.Builder{}
		c.start(id, nil)
	}
	return c
}

// start starts id's node — and its transport, if it is down — on the node's
// durable log.
func (c *snapCluster) start(id NodeID, hook Hook) {
	c.t.Helper()
	if c.trs[id] == nil {
		peers := map[transport.NodeID]string{}
		for _, other := range c.ids {
			if other != id {
				peers[transport.NodeID(other)] = c.addrs[other]
			}
		}
		tr, err := transport.NewTCPTransport(transport.Config{NodeID: transport.NodeID(id), ListenAddr: c.addrs[id], Peers: peers})
		if err != nil {
			c.t.Fatal(err)
		}
		c.trs[id] = tr
	}
	sm := &snapSM{}
	var peers []NodeID
	switch {
	case c.joiners[id], c.genesisless:
	default:
		peers = c.genesis
	}
	// The node's goroutines log through this closure while the test goroutine
	// goes on adding nodes to c.logs; a map read and a map write race even on
	// different keys (CI, main at 5f8ca4b). So the builder is resolved here,
	// on the test goroutine, and the node never touches the map. c.mu still
	// orders the writes against log()'s reads of the builder.
	lg := c.logs[id]
	node, err := Start(c.ctx, Config{
		ID: id, Peers: peers, Join: c.joiners[id], Transport: c.trs[id], LogPath: filepath.Join(c.dir, string(id)+".log"),
		StateMachine: sm, TickInterval: 15 * time.Millisecond, DisableSync: true, Hook: hook,
		SnapshotEvery: c.every, SnapshotRetain: c.retain, Metrics: c.metricsFor(id),
		Logf: func(f string, a ...any) {
			c.mu.Lock()
			defer c.mu.Unlock()
			fmt.Fprintf(lg, f+"\n", a...)
		},
	})
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.nodes[id], c.sms[id] = node, sm
}

// down stops id as a process stops: its node and its transport, so nothing
// the others send meanwhile waits to be delivered when it starts again — a
// restarted follower sees only what its peers send it from then on.
func (c *snapCluster) down(id NodeID) {
	c.t.Helper()
	_ = c.nodes[id].Close()
	if err := c.trs[id].Close(); err != nil {
		c.t.Fatal(err)
	}
	c.trs[id] = nil
}

// compactedPast waits until every node in ids has compacted its log past
// index. A follower holding nothing beyond index can then catch up only by a
// snapshot: Propose returns once an entry is durable in the leader's log, not
// once it is committed, applied and snapshotted, so the tests that need a
// snapshot transfer wait for it here.
func (c *snapCluster) compactedPast(ids []NodeID, index uint64) {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			ok = ok && c.nodes[id].Status().Boundary > index
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range ids {
		c.t.Logf("%s: %+v", id, c.nodes[id].Status())
	}
	c.t.Fatalf("the logs were not compacted past %d", index)
}

func (c *snapCluster) log(id NodeID) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.logs[id].String()
}

func (c *snapCluster) stop() {
	for _, n := range c.nodes {
		_ = n.Close()
	}
	for _, tr := range c.trs {
		if tr != nil {
			_ = tr.Close()
		}
	}
}

// leader returns the node the running nodes agree leads, waiting for one.
func (c *snapCluster) leader(among []NodeID) NodeID {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range among {
			if c.nodes[id].Role() == raft.Leader {
				return id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return ""
}

// propose submits cmds through whichever of among leads, following leader
// changes; each is acknowledged durable in a leader's log before the next.
func (c *snapCluster) propose(among []NodeID, cmds []string) {
	c.t.Helper()
	for _, cmd := range cmds {
		deadline := time.Now().Add(10 * time.Second)
		for {
			err := c.nodes[c.leader(among)].Propose(c.ctx, []byte(cmd))
			if err == nil {
				break
			}
			if !errors.Is(err, raft.ErrNotLeader) || time.Now().After(deadline) {
				c.t.Fatalf("propose %s: %v", cmd, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// converged waits until every node in ids has applied the same commands as
// the leader, through the leader's last index.
func (c *snapCluster) converged(ids []NodeID) []string {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ld := c.leader(ids)
		want := c.nodes[ld].Status().LastIndex
		_, ref, _ := c.sms[ld].state()
		ok := uint64(len(ref)) == want
		for _, id := range ids {
			idx, cmds, _ := c.sms[id].state()
			ok = ok && idx == want && strings.Join(cmds, ",") == strings.Join(ref, ",")
		}
		if ok {
			return ref
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range ids {
		c.t.Logf("%s: %+v", id, c.nodes[id].Status())
	}
	c.t.Fatal("the nodes did not converge")
	return nil
}

func cmds(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

// TestLaggingFollowerCatchesUpBySnapshot: a follower down while the others
// commit, snapshot and compact past everything it has can only catch up by
// installing the leader's snapshot over kind InstallSnapshot; it then applies
// the suffix by entries and holds exactly the leader's state. It proves the
// snapshot was sent and installed (not vacuous), and that a later restart of
// the follower recovers from the snapshot it installed.
func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 3, 16, 4)
	c.propose(c.ids, cmds("a", 5))
	c.converged(c.ids)
	ld := c.leader(c.ids)
	lag := without(c.ids, ld)[0]
	up := without(c.ids, lag)
	last := c.nodes[lag].Status().LastIndex
	c.down(lag)

	c.propose(up, cmds("b", 80))
	c.compactedPast(up, last)
	c.start(lag, nil)
	c.propose(c.ids, cmds("c", 3))
	c.converged(c.ids)
	// At least one install: an offer retried while a transfer is under way
	// may bring a newer snapshot after the first — each above the commit
	// index at the time, as the install rule allows.
	_, _, restores := c.sms[lag].state()
	if restores < 1 || !strings.Contains(c.log(lag), "event=raft_snapshot_received") {
		t.Fatalf("the lagging follower did not catch up by a snapshot (restores %d):\n%s", restores, c.log(lag))
	}
	if st := c.nodes[lag].Status(); st.Snapshot == 0 || st.Boundary == 0 {
		t.Fatalf("follower status after install: %+v", st)
	}

	// Restart the follower: it recovers from the snapshot it installed.
	c.down(lag)
	before := len(c.log(lag))
	c.start(lag, nil)
	c.propose(c.ids, cmds("d", 3))
	c.converged(c.ids)
	if _, _, restores := c.sms[lag].state(); restores < 1 || !strings.Contains(c.log(lag)[before:], "event=raft_snapshot_restored") {
		t.Fatalf("the restarted follower did not start from its own snapshot (restores %d)", restores)
	}
}

// TestInstallCrashPointsRecover crashes a lagging follower at each boundary of
// installing the leader's snapshot (docs/SNAPSHOTS.md §8) and restarts it: it
// recovers — completing the install itself after the snapshot was published
// (the repair of §6) — and converges to the leader's state. The follower is
// down, transport and all, until the others have compacted past its log, so
// its first install is of a snapshot whose entry it does not hold: the install
// the repair has to complete.
func TestInstallCrashPointsRecover(t *testing.T) {
	for _, p := range []Point{BeforeInstallPublish, AfterInstallPublish, AfterInstallBoundary} {
		t.Run(p.String(), func(t *testing.T) {
			ctx := context.Background()
			c := startSnapCluster(t, ctx, 3, 16, 0)
			c.propose(c.ids, cmds("a", 3))
			c.converged(c.ids)
			ld := c.leader(c.ids)
			lag := without(c.ids, ld)[0]
			up := without(c.ids, lag)
			last := c.nodes[lag].Status().LastIndex
			c.down(lag)
			c.propose(up, cmds("b", 50))
			c.compactedPast(up, last)

			hook, fired := crashHook(p, 1)
			c.start(lag, hook)
			select {
			case <-fired:
			case <-time.After(15 * time.Second):
				t.Fatalf("%s never reached:\n%s", p, c.log(lag))
			}
			<-c.nodes[lag].Done()
			c.down(lag)

			c.start(lag, nil)
			c.propose(c.ids, cmds("c", 3))
			ref := c.converged(c.ids)
			if len(userCommands(ref)) != 56 {
				t.Fatalf("converged on %d commands, want 56", len(userCommands(ref)))
			}
			if p == AfterInstallPublish && !strings.Contains(c.log(lag), "repaired=true") {
				t.Fatalf("a crash after publishing did not make recovery complete the install:\n%s", c.log(lag))
			}
		})
	}
}

// TestWaitersSettleOnInstall: an installed snapshot completes the waiters it
// covers — reads succeed, writes are ErrSuperseded (outcome unknown) — and
// leaves the ones above it waiting.
func TestWaitersSettleOnInstall(t *testing.T) {
	w := NewWaiters()
	write := w.Add(5, 2, 0)
	read := w.Add(7, 0, 0)
	above := w.Add(9, 2, 0)
	w.Installed(8)
	if o := <-write; !errors.Is(o.Err, ErrSuperseded) {
		t.Fatalf("write below the snapshot: %+v", o)
	}
	if o := <-read; o.Err != nil || o.Index != 7 {
		t.Fatalf("read below the snapshot: %+v", o)
	}
	select {
	case o := <-above:
		t.Fatalf("a waiter above the snapshot completed: %+v", o)
	default:
	}
	if w.Len() != 1 {
		t.Fatalf("%d waiters left, want 1", w.Len())
	}
}

// TestRecoverRefusesAContradictedSnapshot: a published snapshot recovery
// cannot reconcile with the log refuses to start — never a silent fallback
// (docs/SNAPSHOTS.md §7) — and so does a compacted log with no snapshot, a
// corrupt snapshot, and a snapshot of another group.
func TestRecoverRefusesAContradictedSnapshot(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "raft.log")
	sm := &snapSM{}
	n, tr := startSnapSingle(t, ctx, path, sm, 5, 0, nil, nil)
	waitLeaderNode(t, n)
	for i := 0; i < 12; i++ {
		if err := n.Propose(ctx, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	waitApplied(t, n, n.Status().LastIndex)
	n.Close()
	tr.Close()
	snapPath := path + ".snap"
	good, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	recoverState := func() error {
		t.Helper()
		rc, err := Recover(Config{ID: "n0", Peers: []NodeID{"n0"}, LogPath: path, StateMachine: &snapSM{}, Rand: newRand()})
		if err == nil {
			rc.Log.Close()
		}
		return err
	}
	if err := recoverState(); err != nil {
		t.Fatalf("the intact state: %v", err)
	}
	bad := append([]byte(nil), good...)
	bad[len(bad)/2] ^= 0x40
	for name, mutate := range map[string]func() error{
		"corrupt snapshot":           func() error { return os.WriteFile(snapPath, bad, 0o644) },
		"compacted log, no snapshot": func() error { return os.Remove(snapPath) },
	} {
		if err := mutate(); err != nil {
			t.Fatal(err)
		}
		if err := recoverState(); !errors.Is(err, ErrSnapshot) {
			t.Errorf("%s: %v", name, err)
		}
		if err := os.WriteFile(snapPath, good, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Phase 15: a restart that names another genesis is refused by the group
	// identity file before anything is read; one configured for another group
	// likewise; and a published snapshot of another group (the same members,
	// another group id) is refused by the snapshot's identity.
	if _, err := Recover(Config{ID: "n0", Peers: []NodeID{"n0", "n1", "n2"}, LogPath: path, StateMachine: &snapSM{}, Rand: newRand()}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("another genesis: %v", err)
	}
	if _, err := Recover(Config{ID: "n0", Group: 5, LogPath: path, StateMachine: &snapSM{}, Rand: newRand()}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("another group's data: %v", err)
	}
	meta, state, err := snapshot.Decode(good)
	if err != nil {
		t.Fatal(err)
	}
	meta.Group = 5
	other, err := snapshot.Encode(meta, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapPath, other, 0o644); err != nil {
		t.Fatal(err)
	}
	rc, err := Recover(Config{ID: "n0", LogPath: path, StateMachine: &snapSM{}, Rand: newRand()})
	if !errors.Is(err, snapshot.ErrWrongGroup) {
		if err == nil {
			rc.Log.Close()
		}
		t.Fatalf("another group's snapshot: %v", err)
	}
	if err := os.WriteFile(snapPath, good, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Recover(Config{ID: "n0", Peers: []NodeID{"n0"}, LogPath: path, StateMachine: &recSM{}, Rand: newRand()}); !errors.Is(err, ErrSnapshot) {
		t.Fatalf("a state machine that cannot restore: %v", err)
	}
}

// TestInstallOrderIsTermPublishBoundary pins the I/O order of installing a
// leader's snapshot (docs/SNAPSHOTS.md §8): the new term is written and
// fsynced before the snapshot is published (rename + directory fsync), and the
// publication completes before the log's boundary record is written. It also
// refuses to install a snapshot that is not the one staged.
func TestInstallOrderIsTermPublishBoundary(t *testing.T) {
	const path = "/node/raft.log"
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	lg, _, err := raftlog.Open(path, raftlog.Options{Sync: true, FS: inj})
	if err != nil {
		t.Fatal(err)
	}
	if err := lg.Save(&raftlog.HardState{Term: 1, Commit: 2}, []raftlog.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	conf := replication.VotersOf([]NodeID{"n0", "n1", "n2"})
	snaps := &Snapshots{Files: snapshot.Files{FS: inj, Base: path}, SM: &snapSM{}}
	d := &Durable{Log: lg, Snap: snaps}
	if err := d.InstallSnapshot(raft.SnapshotMeta{Index: 10, Term: 2}, &raftlog.HardState{Term: 2, Commit: 10}, nil); !errors.Is(err, ErrSnapshot) {
		t.Fatalf("install with nothing staged: %v", err)
	}

	src := &snapSM{}
	for i := 1; i <= 10; i++ {
		_ = src.Apply(uint64(i), []byte(fmt.Sprintf("c%d", i)))
	}
	_, data, _ := src.EncodeSnapshot()
	meta := snapshot.Meta{Conf: conf, Index: 10, Term: 2}
	file, err := snapshot.Encode(meta, data)
	if err != nil {
		t.Fatal(err)
	}
	var m *raft.Message
	for _, c := range snapshot.Split(2, meta, file) {
		if m, err = snaps.Receive("n1", c.Marshal()); err != nil {
			t.Fatal(err)
		}
	}
	if m == nil || m.Type != raft.MsgSnapshot || m.SnapshotIndex != 10 || m.SnapshotTerm != 2 || m.From != "n1" || m.Term != 2 {
		t.Fatalf("the completed transfer stepped %+v", m)
	}
	if err := d.InstallSnapshot(raft.SnapshotMeta{Index: 10, Term: 3}, &raftlog.HardState{Term: 3, Commit: 10}, nil); !errors.Is(err, ErrSnapshot) {
		t.Fatalf("install of a snapshot other than the staged one: %v", err)
	}

	before := len(inj.Ops())
	if err := d.InstallSnapshot(raft.SnapshotMeta{Index: 10, Term: 2}, &raftlog.HardState{Term: 2, Commit: 10}, nil); err != nil {
		t.Fatal(err)
	}
	var seq []string
	for _, o := range inj.Ops()[before:] {
		seq = append(seq, fmt.Sprintf("%s %s", o.Op, filepath.Base(o.Path)))
	}
	want := []string{"write raft.log", "fsync raft.log", "rename raft.log.snap", "syncdir node", "write raft.log", "fsync raft.log"}
	if strings.Join(seq, "; ") != strings.Join(want, "; ") {
		t.Fatalf("install I/O:\n got %v\nwant %v", seq, want)
	}
	if idx, cmds, _ := snaps.SM.(*snapSM).state(); idx != 10 || len(cmds) != 10 {
		t.Fatalf("state machine after install: index %d, %d commands", idx, len(cmds))
	}
	lg.Close()
	mem.CrashPowerLoss(0)
	rec, err := raftlog.InspectFS(mem, path)
	if err != nil || rec.Boundary != (raftlog.Boundary{Index: 10, Term: 2}) || rec.HardState.Term != 2 || rec.HardState.Commit != 10 {
		t.Fatalf("after a power loss: %+v %v", rec, err)
	}
}

// TestRecoverRefusesASnapshotWithoutItsLogOrWithAStateItCannotRestore covers
// recovery cases C and M of docs/SNAPSHOTS.md §6: a published snapshot whose
// log is gone is refused — the durable term and vote went with the log, and a
// node that forgot its vote must not rejoin — and so is a snapshot file that
// is well formed but whose state the state machine refuses.
func TestRecoverRefusesASnapshotWithoutItsLogOrWithAStateItCannotRestore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "raft.log")
	n, tr := startSnapSingle(t, ctx, path, &snapSM{}, 4, 0, nil, nil)
	waitLeaderNode(t, n)
	for i := 0; i < 6; i++ {
		if err := n.Propose(ctx, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	waitApplied(t, n, n.Status().LastIndex)
	n.Close()
	tr.Close()
	recoverAt := func() error {
		rc, err := Recover(Config{ID: "n0", Peers: []NodeID{"n0"}, LogPath: path, StateMachine: &snapSM{}, Rand: newRand()})
		if err == nil {
			rc.Log.Close()
		}
		return err
	}
	logBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := recoverAt(); !errors.Is(err, ErrSnapshot) {
		t.Fatalf("case C, a snapshot without its log: %v", err)
	}
	if err := os.WriteFile(path, logBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := recoverAt(); err != nil {
		t.Fatalf("the restored log: %v", err)
	}
	m, _, _, _, err := (snapshot.Files{Base: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	bad, err := snapshot.Encode(m, []byte("a state the state machine refuses"))
	if err != nil {
		t.Fatal(err)
	}
	if err := (snapshot.Files{Base: path}).Publish(bad); err != nil {
		t.Fatal(err)
	}
	if err := recoverAt(); !errors.Is(err, ErrSnapshot) {
		t.Fatalf("case M, a state the state machine refuses: %v", err)
	}
}

// startSnapOn starts a durable single node with snapshots on an explicit
// filesystem (a fault injector over MemFS).
func startSnapOn(t *testing.T, fsys *fault.InjectFS, sm *snapSM) (*Node, *transport.TCPTransport) {
	t.Helper()
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "n0", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := Start(context.Background(), Config{
		ID: "n0", Peers: []NodeID{"n0"}, Transport: tr, LogPath: "/n/raft.log", FS: fsys,
		StateMachine: sm, TickInterval: 10 * time.Millisecond, SnapshotEvery: 5,
	})
	if err != nil {
		tr.Close()
		t.Fatal(err)
	}
	return n, tr
}

// TestSnapshotCreationUnderAStalledDisk: while the snapshot's fsync stalls,
// nothing is published and nothing compacted — the log still holds the whole
// prefix — and once the disk resumes, the snapshot is published and the log
// compacted behind it.
func TestSnapshotCreationUnderAStalledDisk(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	inj.Arm(fault.Injection{Op: fault.OpSync, Path: "/n/raft.log.snap.tmp", Gate: gate})
	n, tr := startSnapOn(t, inj, &snapSM{})
	defer tr.Close()
	defer n.Close()
	defer release() // before Close on every path: the actor may be blocked on the gate
	waitLeaderNode(t, n)
	// Propose until the trigger fires and the snapshot's fsync stalls. A
	// proposal is answered only after its whole Ready cycle — including a
	// snapshot the cycle triggers — so each gets a deadline.
	deadline := time.Now().Add(5 * time.Second)
	for !stalledOnSnapshot(inj) && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_ = n.Propose(ctx, []byte("x"))
		cancel()
	}
	if !stalledOnSnapshot(inj) {
		t.Fatal("premise: the snapshot's fsync was never reached")
	}
	if rec, err := raftlog.InspectFS(mem, "/n/raft.log"); err != nil || rec.Boundary.Index != 0 {
		t.Fatalf("while the snapshot is not durable the log was compacted: %+v %v", rec, err)
	}
	if _, ok := mem.Cached("/n/raft.log.snap"); ok {
		t.Fatal("a snapshot was published before its fsync completed")
	}
	release()
	for n.Status().Boundary == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if st := n.Status(); st.Boundary == 0 || st.Snapshot == 0 {
		t.Fatalf("after the disk resumed: %+v", st)
	}
}

// stalledOnSnapshot reports whether the snapshot temporary's fsync has been
// reached and is stalled: the gate fired (an injection is spent when it fires,
// so none is armed), the temporary was written, and its fsync — recorded only
// once it completes — is not in the op log.
func stalledOnSnapshot(inj *fault.InjectFS) bool {
	wrote, synced := false, false
	for _, o := range inj.Ops() {
		if o.Path == "/n/raft.log.snap.tmp" {
			wrote = wrote || o.Op == fault.OpWrite
			synced = synced || o.Op == fault.OpSync
		}
	}
	return wrote && !synced && inj.Armed() == 0
}

// TestSnapshotCreationUnderAFailedDisk: an fsync of the snapshot that fails
// fail-stops the node, like any durability failure — nothing published,
// nothing compacted — and the restarted node recovers from its full log and
// snapshots again.
func TestSnapshotCreationUnderAFailedDisk(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	inj.Arm(fault.Injection{Op: fault.OpSync, Path: "/n/raft.log.snap.tmp"})
	sm := &snapSM{}
	n, tr := startSnapOn(t, inj, sm)
	waitLeaderNode(t, n)
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := n.Propose(ctx, []byte(fmt.Sprintf("c%d", i)))
		cancel()
		if err != nil {
			break
		}
	}
	select {
	case <-n.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the node did not fail-stop on the snapshot's failed fsync")
	}
	if err := n.Err(); !errors.Is(err, fault.ErrInjected) {
		t.Fatalf("fail-stop cause: %v", err)
	}
	n.Close()
	tr.Close()
	if rec, err := raftlog.InspectFS(mem, "/n/raft.log"); err != nil || rec.Boundary.Index != 0 {
		t.Fatalf("a failed snapshot compacted the log: %+v %v", rec, err)
	}
	if _, ok := mem.Cached("/n/raft.log.snap"); ok {
		t.Fatal("a failed snapshot was published")
	}
	mem.CrashProcess()
	sm2 := &snapSM{}
	n2, tr2 := startSnapOn(t, fault.NewInjectFS(mem), sm2)
	defer tr2.Close()
	defer n2.Close()
	waitLeaderNode(t, n2)
	for i := 0; i < 6; i++ {
		if err := n2.Propose(context.Background(), []byte("again")); err != nil {
			t.Fatal(err)
		}
	}
	waitApplied(t, n2, n2.Status().LastIndex)
	if st := n2.Status(); st.Snapshot == 0 {
		t.Fatalf("the restarted node never snapshotted: %+v", st)
	}
}

// TestReceiveRefusesWhatCannotBeInstalled: a complete, well-formed transfer
// that is another group's snapshot, or whose state the state machine refuses,
// is refused before the core ever sees it — nothing is staged, so nothing can
// be installed — while a valid one is accepted. An install of a refused
// snapshot would be durable, and a node whose published snapshot its state
// machine refuses cannot start (docs/SNAPSHOTS.md §7).
func TestReceiveRefusesWhatCannotBeInstalled(t *testing.T) {
	conf := replication.VotersOf([]NodeID{"n0", "n1", "n2"})
	snaps := &Snapshots{Files: snapshot.Files{FS: fault.NewMemFS(), Base: "/n/raft.log"}, SM: &snapSM{}, Group: 3}
	src := &snapSM{}
	for i := 1; i <= 3; i++ {
		_ = src.Apply(uint64(i), []byte("c"))
	}
	_, state, _ := src.EncodeSnapshot()
	send := func(meta snapshot.Meta) (*raft.Message, error) {
		t.Helper()
		file, err := snapshot.Encode(meta, state)
		if err != nil {
			t.Fatal(err)
		}
		var m *raft.Message
		for _, c := range snapshot.Split(2, meta, file) {
			if m, err = snaps.Receive("n1", c.Marshal()); err != nil {
				return m, err
			}
		}
		return m, nil
	}
	// Phase 15: the group id is the identity — the same members in another
	// group are another group.
	if m, err := send(snapshot.Meta{Group: 4, Conf: conf, Index: 3, Term: 1}); m != nil || !errors.Is(err, snapshot.ErrWrongGroup) {
		t.Fatalf("another group's snapshot: %v %v", m, err)
	}
	if m, err := send(snapshot.Meta{Group: 3, Conf: conf, Index: 4, Term: 1}); m != nil || err == nil {
		t.Fatalf("a state the state machine refuses (state at 3, snapshot at 4): %v %v", m, err)
	}
	d := &Durable{Snap: snaps}
	if err := d.InstallSnapshot(raft.SnapshotMeta{Index: 4, Term: 1}, nil, nil); !errors.Is(err, ErrSnapshot) {
		t.Fatalf("a refused snapshot was left installable: %v", err)
	}
	withLearner := replication.Configuration{Voters: conf.Voters, Learners: []replication.Member{{ID: "n3", Addr: "h:3"}}}
	m, err := send(snapshot.Meta{Group: 3, Conf: withLearner, Index: 3, Term: 1})
	if m == nil || err != nil {
		t.Fatalf("the valid snapshot: %v %v", m, err)
	}
	// The core learns the snapshot's configuration with it (Phase 15).
	if m.Conf == nil || !m.Conf.Equal(withLearner) {
		t.Fatalf("the MsgSnapshot carries configuration %v, want %s", m.Conf, withLearner)
	}
}
