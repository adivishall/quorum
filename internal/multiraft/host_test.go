package multiraft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/transport"
)

// recSM records the commands a group applies, in order.
type recSM struct {
	mu   sync.Mutex
	cmds []string
}

func (s *recSM) Apply(index uint64, command []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(command) > 0 {
		s.cmds = append(s.cmds, string(command))
	}
	return nil
}

func (s *recSM) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.cmds...)
}

// hostCluster is a set of hosts over real TCP transports, each with its own
// data directory.
type hostCluster struct {
	t     *testing.T
	ctx   context.Context
	ids   []NodeID
	addrs map[NodeID]string
	dirs  map[NodeID]string
	trs   map[NodeID]*transport.TCPTransport
	hosts map[NodeID]*Host
	logs  map[NodeID]*strings.Builder
	mu    sync.Mutex
}

func newHostCluster(t *testing.T, ids ...NodeID) *hostCluster {
	t.Helper()
	c := &hostCluster{t: t, ctx: context.Background(), addrs: map[NodeID]string{}, dirs: map[NodeID]string{},
		trs: map[NodeID]*transport.TCPTransport{}, hosts: map[NodeID]*Host{}, logs: map[NodeID]*strings.Builder{}}
	t.Cleanup(c.close)
	for _, id := range ids {
		c.add(id)
	}
	return c
}

// add makes id's transport (listening) and data directory; startHost starts
// its host.
func (c *hostCluster) add(id NodeID) {
	c.t.Helper()
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: transport.NodeID(id), ListenAddr: "127.0.0.1:0", DialRetryInterval: 20 * time.Millisecond})
	if err != nil {
		c.t.Fatal(err)
	}
	c.ids = append(c.ids, id)
	c.trs[id], c.addrs[id], c.dirs[id] = tr, tr.LocalAddr().String(), filepath.Join(c.t.TempDir(), string(id))
	c.mu.Lock()
	c.logs[id] = &strings.Builder{}
	c.mu.Unlock()
}

// static is every other known node, as static peers of id.
func (c *hostCluster) static(id NodeID) map[NodeID]string {
	peers := map[NodeID]string{}
	for _, o := range c.ids {
		if o != id {
			peers[o] = c.addrs[o]
		}
	}
	return peers
}

// startHost starts id's host, with every other known node as a static peer
// when static is true.
func (c *hostCluster) startHost(id NodeID, static bool) *Host {
	c.t.Helper()
	peers := map[NodeID]string{}
	if static {
		peers = c.static(id)
	}
	h, err := Start(c.ctx, Config{
		ID: id, DataDir: c.dirs[id], Transport: c.trs[id], StaticPeers: peers,
		NewStateMachine: func(GroupID) raftnode.StateMachine { return &recSM{} },
		TickInterval:    10 * time.Millisecond, DisableSync: true,
		Logf: func(f string, a ...any) {
			c.mu.Lock()
			defer c.mu.Unlock()
			fmt.Fprintf(c.logs[id], f+"\n", a...)
		},
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.hosts[id] = h
	return h
}

func (c *hostCluster) log(id NodeID) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.logs[id].String()
}

func (c *hostCluster) close() {
	for _, h := range c.hosts {
		_ = h.Close()
	}
	for _, tr := range c.trs {
		_ = tr.Close()
	}
}

// genesis is the configuration of ids as voters, with their addresses.
func (c *hostCluster) genesis(ids ...NodeID) *replication.Configuration {
	var m []replication.Member
	for _, id := range ids {
		m = append(m, replication.Member{ID: id, Addr: c.addrs[id]})
	}
	conf, err := replication.NewConfiguration(m, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	return &conf
}

// create starts group g on each of ids with the genesis ids.
func (c *hostCluster) create(g GroupID, ids ...NodeID) {
	c.t.Helper()
	gen := c.genesis(ids...)
	for _, id := range ids {
		if _, err := c.hosts[id].Create(g, gen); err != nil {
			c.t.Fatalf("create group %d on %s: %v", g, id, err)
		}
	}
}

// leader waits for a leader of group g among ids.
func (c *hostCluster) leader(g GroupID, ids ...NodeID) NodeID {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			if grp := c.hosts[id].Group(g); grp != nil && grp.Node.Role() == raft.Leader {
				return id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("group %d has no leader among %v", g, ids)
	return ""
}

// write commits cmd in group g through its leader among ids.
func (c *hostCluster) write(g GroupID, cmd string, ids ...NodeID) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
		_, _, _, err := c.hosts[c.leader(g, ids...)].Group(g).Node.Write(ctx, []byte(cmd))
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("write %s to group %d: %v", cmd, g, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// applied waits until group g on every one of ids applied exactly want.
func (c *hostCluster) applied(g GroupID, want []string, ids ...NodeID) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			grp := c.hosts[id].Group(g)
			ok = ok && grp != nil && strings.Join(grp.SM.(*recSM).got(), ",") == strings.Join(want, ",")
		}
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, id := range ids {
		if grp := c.hosts[id].Group(g); grp != nil {
			c.t.Logf("%s group %d: %v %+v", id, g, grp.SM.(*recSM).got(), grp.Node.Status())
		}
	}
	c.t.Fatalf("group %d did not apply %v on %v", g, want, ids)
}

// TestTwoGroupsOnThreeNodesAreIndependent: two groups over the same three
// processes and transports each elect their own leader, commit their own
// commands, and apply nothing of the other's; each lives in its own
// directory with its own log and identity file.
func TestTwoGroupsOnThreeNodesAreIndependent(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c")
	for _, id := range c.ids {
		c.startHost(id, true)
	}
	c.create(1, "a", "b", "c")
	c.create(2, "a", "b", "c")
	for i := 0; i < 5; i++ {
		c.write(1, fmt.Sprintf("one-%d", i), c.ids...)
		c.write(2, fmt.Sprintf("two-%d", i), c.ids...)
	}
	c.applied(1, []string{"one-0", "one-1", "one-2", "one-3", "one-4"}, c.ids...)
	c.applied(2, []string{"two-0", "two-1", "two-2", "two-3", "two-4"}, c.ids...)
	for _, id := range c.ids {
		for _, g := range []GroupID{1, 2} {
			ident, found, err := raftnode.LoadIdentity(nil, LogPath(c.dirs[id], g))
			if err != nil || !found || ident.Group != g {
				t.Fatalf("%s group %d identity: %+v %v %v", id, g, ident, found, err)
			}
		}
		if got := c.hosts[id].Groups(); len(got) != 2 {
			t.Fatalf("%s hosts %v", id, got)
		}
	}
}

// TestBreakingOneGroupLeavesTheOtherRunning is the isolation test
// (docs/MULTI_RAFT.md §8): group 1 loses its quorum (stopped on two of its
// three nodes) while group 2, on the same processes and connections, keeps
// electing and committing by its own quorum. Group 1 commits nothing
// meanwhile, and recovers when its nodes return.
func TestBreakingOneGroupLeavesTheOtherRunning(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c")
	for _, id := range c.ids {
		c.startHost(id, true)
	}
	c.create(1, "a", "b", "c")
	c.create(2, "a", "b", "c")
	c.write(1, "one-0", c.ids...)
	c.write(2, "two-0", c.ids...)
	c.applied(1, []string{"one-0"}, c.ids...)
	for _, id := range []NodeID{"a", "b"} {
		if err := c.hosts[id].Stop(1); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 10; i++ {
		c.write(2, fmt.Sprintf("two-%d", i), c.ids...)
	}
	want2 := []string{"two-0"}
	for i := 1; i <= 10; i++ {
		want2 = append(want2, fmt.Sprintf("two-%d", i))
	}
	c.applied(2, want2, c.ids...)
	// Group 1's last member cannot commit alone.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, _, _, err := c.hosts["c"].Group(1).Node.Write(ctx, []byte("one-lost"))
	cancel()
	if err == nil {
		t.Fatal("group 1 committed with one of its three members")
	}
	// Bring group 1 back: it recovers from its own files and commits again.
	for _, id := range []NodeID{"a", "b"} {
		if _, err := c.hosts[id].Create(1, c.genesis("a", "b", "c")); err != nil {
			t.Fatal(err)
		}
	}
	c.write(1, "one-1", c.ids...)
	grp := c.hosts["a"].Group(1)
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(strings.Join(grp.SM.(*recSM).got(), ","), "one-1") {
		if time.Now().After(deadline) {
			t.Fatalf("group 1 did not recover: %v", grp.SM.(*recSM).got())
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, cmd := range grp.SM.(*recSM).got() {
		if strings.HasPrefix(cmd, "two-") {
			t.Fatalf("group 1 applied group 2's command %q", cmd)
		}
	}
}

// TestAGroupThatFailsToRecoverDoesNotBlockTheOthers: a group whose durable
// state is corrupt is reported (Failed, event=group_failed) and left down on
// restart; every other group of the host recovers and serves.
func TestAGroupThatFailsToRecoverDoesNotBlockTheOthers(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c")
	for _, id := range c.ids {
		c.startHost(id, true)
	}
	c.create(1, "a", "b", "c")
	c.create(2, "a", "b", "c")
	c.write(1, "one-0", c.ids...)
	c.write(2, "two-0", c.ids...)
	c.applied(1, []string{"one-0"}, c.ids...)
	c.applied(2, []string{"two-0"}, c.ids...)
	if err := c.hosts["a"].Close(); err != nil {
		t.Fatal(err)
	}
	// Corrupt group 1's identity file on a.
	idPath := LogPath(c.dirs["a"], 1) + ".group"
	b, err := os.ReadFile(idPath)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xff
	if err := os.WriteFile(idPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	h := c.startHost("a", true)
	if err := h.Failed()[1]; !errors.Is(err, raftnode.ErrIdentity) {
		t.Fatalf("group 1 on a: %v", err)
	}
	if h.Group(1) != nil || h.Group(2) == nil {
		t.Fatalf("a hosts %v", h.Groups())
	}
	if !strings.Contains(c.log("a"), "event=group_failed node=a group=1") {
		t.Fatal("no group_failed event")
	}
	c.write(2, "two-1", c.ids...)
	c.applied(2, []string{"two-0", "two-1"}, c.ids...)
}

// TestHostRestartRecoversEveryGroup: a host restarted with nothing but its
// data directory finds its groups (docs/MULTI_RAFT.md §5) and each resumes
// from its own files.
func TestHostRestartRecoversEveryGroup(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c")
	for _, id := range c.ids {
		c.startHost(id, true)
	}
	for g := GroupID(1); g <= 4; g++ {
		c.create(g, "a", "b", "c")
		c.write(g, fmt.Sprintf("g%d-0", g), c.ids...)
	}
	for _, id := range c.ids {
		if err := c.hosts[id].Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.ids {
		h := c.startHost(id, true)
		if got := h.Groups(); len(got) != 4 {
			t.Fatalf("%s recovered %v", id, got)
		}
	}
	for g := GroupID(1); g <= 4; g++ {
		c.write(g, fmt.Sprintf("g%d-1", g), c.ids...)
		c.applied(g, []string{fmt.Sprintf("g%d-0", g), fmt.Sprintf("g%d-1", g)}, c.ids...)
	}
}

// TestJoinerAddedThroughTheHost: a fourth process joins group 1 only. Its
// host creates the group as a joiner; the leader adds it as a learner with its
// address, which the members' hosts add to their transports from the
// configuration alone; it catches up, is promoted, and group 2 never hears of
// it.
func TestJoinerAddedThroughTheHost(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c", "d")
	for _, id := range []NodeID{"a", "b", "c"} {
		c.startHost(id, false) // no static peers: the configurations name them
	}
	c.create(1, "a", "b", "c")
	c.create(2, "a", "b", "c")
	members := []NodeID{"a", "b", "c"}
	c.write(1, "one-0", members...)
	c.write(2, "two-0", members...)
	// d knows a, b and c statically: it is the one joining.
	c.startHost("d", true)
	if _, err := c.hosts["d"].Create(1, nil); err != nil {
		t.Fatal(err)
	}
	ld := c.leader(1, members...)
	if _, _, err := c.hosts[ld].Group(1).Node.ChangeMembership(context.Background(),
		raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "d", Addr: c.addrs["d"]}}); err != nil {
		t.Fatal(err)
	}
	c.write(1, "one-1", members...)
	c.applied(1, []string{"one-0", "one-1"}, "a", "b", "c", "d")
	ld = c.leader(1, members...)
	conf, _, err := c.hosts[ld].Group(1).Node.ChangeMembership(context.Background(), raft.ConfChange{Type: raft.Promote, Member: raft.Member{ID: "d"}})
	if err != nil || !conf.IsVoter("d") {
		t.Fatalf("promote: %s %v", conf, err)
	}
	c.write(1, "one-2", c.ids...)
	c.applied(1, []string{"one-0", "one-1", "one-2"}, c.ids...)
	if peers := c.trs["a"].Peers(); peers["d"] != c.addrs["d"] {
		t.Fatalf("a's transport peers: %v", peers)
	}
	if c.hosts["d"].Group(2) != nil {
		t.Fatal("d hosts group 2")
	}
	for _, id := range members {
		if st := c.hosts[id].Group(2).Node.Status(); st.Conf.IsMember("d") {
			t.Fatalf("%s's group 2 names d: %s", id, st.Conf)
		}
	}
}

// TestRemovedLeaderRetiresItsGroup: a leader removed from group 1 leads the
// change to its end, sees its removal committed and its host stops the group
// (event=group_retired); its other group runs on.
func TestRemovedLeaderRetiresItsGroup(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c")
	for _, id := range c.ids {
		c.startHost(id, true)
	}
	c.create(1, "a", "b", "c")
	c.create(2, "a", "b", "c")
	c.write(1, "one-0", c.ids...)
	ld := c.leader(1, c.ids...)
	if _, _, err := c.hosts[ld].Group(1).Node.ChangeMembership(context.Background(),
		raft.ConfChange{Type: raft.RemoveVoter, Member: raft.Member{ID: ld}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for c.hosts[ld].Group(1) != nil {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not retire group 1", ld)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(c.log(ld), "event=group_retired node="+string(ld)+" group=1 reason=removed") {
		t.Fatal("no group_retired event")
	}
	var rest []NodeID
	for _, id := range c.ids {
		if id != ld {
			rest = append(rest, id)
		}
	}
	c.write(1, "one-1", rest...)
	c.applied(1, []string{"one-0", "one-1"}, rest...)
	c.write(2, "two-0", c.ids...)
	c.applied(2, []string{"two-0"}, c.ids...)
}

// chanTransport is a transport whose inbound frames the test injects.
type chanTransport struct {
	id   transport.NodeID
	ch   chan transport.Envelope
	once sync.Once
}

func (c *chanTransport) Send(context.Context, transport.NodeID, transport.MsgKind, []byte) error {
	return nil
}
func (c *chanTransport) Receive() <-chan transport.Envelope { return c.ch }
func (c *chanTransport) LocalID() transport.NodeID          { return c.id }
func (c *chanTransport) Close() error                       { c.once.Do(func() { close(c.ch) }); return nil }

// TestFramesReachExactlyTheirGroup (INV-MB6): a frame is delivered to the group
// its envelope names and to no other; an unknown group's, a stopped group's
// and a malformed frame are dropped and counted; a group created afterwards
// receives its frames; a full inbox drops rather than stalling the others.
func TestFramesReachExactlyTheirGroup(t *testing.T) {
	tr := &chanTransport{id: "a", ch: make(chan transport.Envelope, 16)}
	h, err := Start(context.Background(), Config{
		ID: "a", DataDir: t.TempDir(), Transport: tr, InboxSize: 4,
		NewStateMachine: func(GroupID) raftnode.StateMachine { return &recSM{} },
		TickInterval:    time.Hour, DisableSync: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	gen := replication.VotersOf([]NodeID{"a", "b", "c"})
	for _, g := range []GroupID{1, 2} {
		if _, err := h.Create(g, &gen); err != nil {
			t.Fatal(err)
		}
	}
	appendAt := func(term uint64) []byte {
		return raft.Message{Type: raft.MsgAppendRequest, Term: term, From: "b", To: "a"}.Marshal()
	}
	send := func(payload []byte) {
		tr.ch <- transport.Envelope{Peer: "b", Kind: transport.MsgAppendEntries, Payload: payload}
	}
	waitTerm := func(g GroupID, term uint64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for h.Group(g).Node.Term() != term {
			if time.Now().After(deadline) {
				t.Fatalf("group %d at term %d, want %d", g, h.Group(g).Node.Term(), term)
			}
			time.Sleep(time.Millisecond)
		}
	}
	send(raftnode.WrapGroup(1, appendAt(5)))
	waitTerm(1, 5)
	if h.Group(2).Node.Term() != 0 {
		t.Fatal("group 1's frame moved group 2")
	}
	send(raftnode.WrapGroup(2, appendAt(7)))
	waitTerm(2, 7)
	if h.Group(1).Node.Term() != 5 {
		t.Fatal("group 2's frame moved group 1")
	}
	waitDropped := func(reason string, n uint64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for h.Dropped()[reason] < n {
			if time.Now().After(deadline) {
				t.Fatalf("dropped %v, want %d %s", h.Dropped(), n, reason)
			}
			time.Sleep(time.Millisecond)
		}
	}
	send(raftnode.WrapGroup(9, appendAt(50))) // unknown
	send([]byte{0x80})                        // malformed envelope
	waitDropped("unknown_group", 1)
	waitDropped("malformed", 1)
	if err := h.Stop(2); err != nil {
		t.Fatal(err)
	}
	send(raftnode.WrapGroup(2, appendAt(60))) // stopped
	waitDropped("unknown_group", 2)
	if h.Group(1).Node.Term() != 5 {
		t.Fatalf("a dropped frame reached group 1: term %d", h.Group(1).Node.Term())
	}
	// A group created now receives its frames.
	if _, err := h.Create(3, &gen); err != nil {
		t.Fatal(err)
	}
	send(raftnode.WrapGroup(3, appendAt(3)))
	waitTerm(3, 3)
	// Duplicated frames are delivered as duplicates to their group only.
	for i := 0; i < 3; i++ {
		send(raftnode.WrapGroup(3, appendAt(4)))
	}
	waitTerm(3, 4)
	if h.Group(1).Node.Term() != 5 {
		t.Fatal("group 3's frames moved group 1")
	}
}
