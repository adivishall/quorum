package integration

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/routing"
)

// The Phase 15 real-process harness: dkvd processes in -cluster mode, one Raft
// group per shard of the routing, each node with a client port and an admin
// port. Every node that may ever run — the genesis nodes and the spares that
// join later — has its addresses and its per-link TCP proxies from the start
// (so a partition can cut any link), and every node's -peers names them all.
// The routing's node list is the genesis nodes, on every process, spares
// included. Every fact a test relies on is read from the nodes themselves —
// the admin protocol's status — never assumed from elapsed time.

type mcluster struct {
	t       *testing.T
	bin     string
	ids     []string // every node: genesis first, then spares
	genesis []string
	shards  int
	rf      int
	addrs   map[string]string
	kvAddrs map[string]string
	admAddr map[string]string
	dirs    map[string]string
	proxies map[[2]string]*tcpProxy
	procs   map[string]*dkvNode
	every   []string
	assign  *multiraft.Assignment
	cluster string // the cluster id its nodes' data directories record
}

// newMCluster starts genesis nodes (n1..nG) hosting shards groups, with
// spares more nodes addressable but not started.
func newMCluster(t *testing.T, genesis, spares, shards int, every ...string) *mcluster {
	t.Helper()
	c := &mcluster{t: t, bin: buildDkvd(t), shards: shards, rf: min(3, genesis),
		addrs: map[string]string{}, kvAddrs: map[string]string{}, admAddr: map[string]string{}, dirs: map[string]string{},
		proxies: map[[2]string]*tcpProxy{}, procs: map[string]*dkvNode{}, every: every, cluster: newClusterID()}
	root := t.TempDir()
	var rnodes []routing.NodeID
	for i := 1; i <= genesis+spares; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		if i <= genesis {
			c.genesis = append(c.genesis, id)
			rnodes = append(rnodes, routing.NodeID(id))
		}
		c.addrs[id], c.kvAddrs[id], c.admAddr[id] = freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
		c.dirs[id] = filepath.Join(root, id)
	}
	a, err := multiraft.NewAssignment(routing.Config{ShardCount: shards, ReplicationFactor: c.rf, Nodes: rnodes})
	if err != nil {
		t.Fatal(err)
	}
	c.assign = a
	for i, x := range c.ids {
		for _, y := range c.ids[i+1:] {
			c.proxies[[2]string{x, y}] = startProxy(t, c.addrs[y])
		}
	}
	t.Cleanup(c.killAll)
	for _, id := range c.genesis {
		c.start(id)
	}
	for _, id := range c.genesis {
		waitForLine(t, c.procs[id], "event=admin_ready", 20*time.Second)
	}
	return c
}

// start launches (or relaunches) a node; extra flags apply to this start only.
func (c *mcluster) start(id string, extra ...string) {
	c.t.Helper()
	var peers []string
	for _, o := range c.ids {
		switch {
		case o == id:
		case id < o:
			peers = append(peers, o+"="+c.proxies[[2]string{id, o}].Addr())
		default:
			peers = append(peers, o+"="+c.addrs[o])
		}
	}
	args := []string{"-id", id, "-listen", c.addrs[id], "-peers", strings.Join(peers, ","),
		"-cluster", "-shards", fmt.Sprint(c.shards), "-rf", fmt.Sprint(c.rf), "-nodes", strings.Join(c.genesis, ","),
		"-data-dir", c.dirs[id], "-tick-interval", "25ms", "-client-listen", c.kvAddrs[id], "-admin-listen", c.admAddr[id]}
	args = append(append(append(args, initFlags(c.dirs[id], c.cluster)...), c.every...), extra...)
	buf := &safeBuf{}
	cmd := exec.Command(c.bin, args...)
	cmd.Stdout, cmd.Stderr = buf, buf
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.procs[id] = &dkvNode{id: id, addr: c.addrs[id], cmd: cmd, out: buf}
}

// startReady starts a node and waits for its admin port.
func (c *mcluster) startReady(id string, extra ...string) {
	c.t.Helper()
	c.start(id, extra...)
	waitForLine(c.t, c.procs[id], "event=admin_ready", 20*time.Second)
}

// kill SIGKILLs a node: no shutdown path runs.
func (c *mcluster) kill(id string) {
	c.t.Helper()
	if p := c.procs[id]; p != nil {
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
		_ = p.cmd.Wait()
		c.procs[id] = nil
	}
}

func (c *mcluster) killAll() {
	for _, p := range c.procs {
		if p != nil {
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
	}
}

func (c *mcluster) isolate(id string) {
	for k, p := range c.proxies {
		if k[0] == id || k[1] == id {
			p.Cut()
		}
	}
}

func (c *mcluster) healAll() {
	for _, p := range c.proxies {
		p.Heal()
	}
}

func (c *mcluster) running() []string {
	var out []string
	for _, id := range c.ids {
		if c.procs[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

// adminCall sends an admin request to id (a transport failure is returned).
func (c *mcluster) adminCall(id string, req multiraft.AdminRequest) (multiraft.AdminResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return multiraft.AdminCall(ctx, c.admAddr[id], req)
}

// status returns id's groups, by group id (nil if the node cannot answer).
func (c *mcluster) status(id string) map[multiraft.GroupID]multiraft.GroupStatus {
	if c.procs[id] == nil {
		return nil
	}
	resp, err := c.adminCall(id, multiraft.AdminRequest{Op: "status"})
	if err != nil || !resp.OK {
		return nil
	}
	out := map[multiraft.GroupID]multiraft.GroupStatus{}
	for _, g := range resp.Groups {
		out[multiraft.GroupID(g.Group)] = g
	}
	return out
}

// leaderOf returns group g's confirmed leader among the running nodes: of the
// nodes reporting themselves leader, the one of the highest term that another
// running node reports following in that term (or that is the only voter of
// its configuration). Terms nobody confirms — a stale node campaigning alone
// on an old configuration, or an isolated leader — never count.
func (c *mcluster) leaderOf(g multiraft.GroupID) (string, uint64, bool) {
	sts := map[string]multiraft.GroupStatus{}
	for _, id := range c.running() {
		if st, ok := c.status(id)[g]; ok {
			sts[id] = st
		}
	}
	best, bestTerm := "", uint64(0)
	for id, st := range sts {
		if st.Role != "Leader" || st.Term <= bestTerm {
			continue
		}
		confirmed := len(st.Conf.Voters) == 1 && st.Conf.Voters[0].ID == id && len(st.Conf.Outgoing) == 0
		for o, os := range sts {
			if o != id && os.Leader == id && os.Term == st.Term {
				confirmed = true
			}
		}
		if confirmed {
			best, bestTerm = id, st.Term
		}
	}
	return best, bestTerm, best != ""
}

func (c *mcluster) waitLeaderOf(g multiraft.GroupID) (string, uint64) {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if l, tm, ok := c.leaderOf(g); ok {
			return l, tm
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("group %d has no confirmed leader\n%s", g, c.statusDump())
	return "", 0
}

// change submits a membership operation to group g's leader, following
// leadership changes, until it completes.
func (c *mcluster) change(g multiraft.GroupID, op, id, addr string) multiraft.AdminResponse {
	c.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		l, _ := c.waitLeaderOf(g)
		resp, err := c.adminCall(l, multiraft.AdminRequest{Op: op, Group: uint32(g), ID: id, Addr: addr, Timeout: 15000})
		if err == nil && resp.OK {
			return resp
		}
		if err == nil && !strings.Contains(resp.Error, "not leader") && !strings.Contains(resp.Error, "in progress") && !strings.Contains(resp.Error, "context deadline") {
			c.t.Fatalf("%s %s in group %d at %s: %s", op, id, g, l, resp.Error)
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("%s %s in group %d did not complete\n%s", op, id, g, c.statusDump())
	return multiraft.AdminResponse{}
}

// waitConf waits until every node of among reports group g with no change
// under way and a configuration ok accepts.
func (c *mcluster) waitConf(g multiraft.GroupID, among []string, ok func(multiraft.ConfStatus) bool) {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range among {
			st, has := c.status(id)[g]
			all = all && has && !st.ConfPending && ok(st.Conf)
		}
		if all {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("group %d's configuration did not settle on %v\n%s", g, among, c.statusDump())
}

// waitCaughtUp waits until id has applied group g through its leader's commit
// index as of now.
func (c *mcluster) waitCaughtUp(g multiraft.GroupID, id string) {
	c.t.Helper()
	l, _ := c.waitLeaderOf(g)
	target := c.status(l)[g].Commit
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if st, ok := c.status(id)[g]; ok && st.Applied >= target {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("%s did not catch up group %d to %d\n%s", id, g, target, c.statusDump())
}

func (c *mcluster) statusDump() string {
	var b strings.Builder
	for _, id := range c.ids {
		sts := c.status(id)
		if sts == nil {
			fmt.Fprintf(&b, "%s: -\n", id)
			continue
		}
		var gs []int
		for g := range sts {
			gs = append(gs, int(g))
		}
		sort.Ints(gs)
		for _, g := range gs {
			st := sts[multiraft.GroupID(g)]
			fmt.Fprintf(&b, "%s g%d %s t%d leader=%s commit=%d applied=%d snap=%d voters=%v outgoing=%v learners=%v pending=%v removed=%v\n",
				id, g, st.Role, st.Term, st.Leader, st.Commit, st.Applied, st.Snapshot,
				multiraft.IDs(st.Conf.Voters), multiraft.IDs(st.Conf.Outgoing), multiraft.IDs(st.Conf.Learners), st.ConfPending, st.Removed)
		}
	}
	return b.String()
}

// isVoter, hasMember: configuration predicates for waitConf.
func isVoter(id string) func(multiraft.ConfStatus) bool {
	return func(cs multiraft.ConfStatus) bool {
		for _, m := range cs.Voters {
			if m.ID == id {
				return true
			}
		}
		return false
	}
}

func notMember(id string) func(multiraft.ConfStatus) bool {
	return func(cs multiraft.ConfStatus) bool {
		for _, list := range [][]multiraft.Member{cs.Voters, cs.Outgoing, cs.Learners} {
			for _, m := range list {
				if m.ID == id {
					return false
				}
			}
		}
		return true
	}
}

func (c *mcluster) endpoints() []kv.Doer {
	var out []kv.Doer
	for _, id := range c.ids {
		out = append(out, &procEndpoint{node: id, addr: c.kvAddrs[id]})
	}
	return out
}

// sharded returns a session client of the cluster: keys routed to their
// group, one session per group.
func (c *mcluster) sharded() *kv.Sharded {
	return kv.NewSharded(c.endpoints(), kv.SessionOptions{AttemptTimeout: 2 * time.Second, MaxAttempts: 30}, c.assign.GroupOf)
}

// keyIn returns a key of group g (the i-th found scanning key-0, key-1, ...).
func (c *mcluster) keyIn(g multiraft.GroupID, i int) string {
	n := 0
	for k := 0; ; k++ {
		key := fmt.Sprintf("key-%d", k)
		if c.assign.GroupOf([]byte(key)) == g {
			if n == i {
				return key
			}
			n++
		}
	}
}

// write puts key=value through a fresh sharded client and requires success.
func (c *mcluster) write(cl *kv.Sharded, key, value string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if out := cl.Put(ctx, []byte(key), []byte(value), nil); out.Err != nil {
		c.t.Fatalf("put %s: %v\n%s", key, out.Err, c.statusDump())
	}
}

func (c *mcluster) read(cl *kv.Sharded, key string) (string, bool) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out := cl.Get(ctx, []byte(key), nil)
	if out.Err != nil {
		c.t.Fatalf("get %s: %v\n%s", key, out.Err, c.statusDump())
	}
	return string(out.Response.Value), out.Response.Status == kv.StatusOK
}

// --- a recorded session workload ---

// shardedWorkload runs clients of the cluster, each with its own sessions,
// issuing puts and gets over keys of every group, recording every operation
// and every attempt for the linearizability checker.
type shardedWorkload struct {
	c    *mcluster
	rec  *lincheck.Recorder
	keys []string
	stop chan struct{}
	wg   sync.WaitGroup
}

func (c *mcluster) startWorkload(clients int, keysPerGroup int, seed int64) *shardedWorkload {
	w := &shardedWorkload{c: c, rec: lincheck.NewRecorder(), stop: make(chan struct{})}
	for g := 0; g < c.shards; g++ {
		for i := 0; i < keysPerGroup; i++ {
			w.keys = append(w.keys, c.keyIn(multiraft.GroupID(g), i))
		}
	}
	for i := 0; i < clients; i++ {
		name := fmt.Sprintf("w%d", i)
		rng := rand.New(rand.NewSource(seed + int64(i)))
		cl := c.sharded()
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for n := 0; ; n++ {
				select {
				case <-w.stop:
					return
				default:
				}
				key := w.keys[rng.Intn(len(w.keys))]
				if rng.Intn(2) == 0 {
					w.op(cl, name, lincheck.Put, key, []byte(fmt.Sprintf("%s.%d", name, n)))
				} else {
					w.op(cl, name, lincheck.Get, key, nil)
				}
			}
		}()
	}
	return w
}

// op runs one logical request of client name, recording each attempt.
func (w *shardedWorkload) op(cl *kv.Sharded, name string, kind lincheck.Kind, key string, value []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	g := cl.Group([]byte(key))
	s, err := cl.Session(ctx, g)
	if err != nil {
		return // no session yet: nothing was requested
	}
	rid := s.Reserve()
	defer s.Release(rid)
	// A session id is group-local (docs/CLIENT_SEMANTICS.md): two groups
	// number their sessions independently, so the same id in two groups names
	// two unrelated sessions. The history's identity is therefore the pair
	// (group, session id), packed into one number.
	id := w.rec.BeginRequest(name, kind, key, value, uint64(g)<<48|s.ID(), rid)
	op := kv.ReqGet
	if kind == lincheck.Put {
		op = kv.ReqPut
	}
	hook := func(node string) func(kv.Response, error) {
		a := w.rec.Attempt(id, node)
		return func(resp kv.Response, err error) {
			w.rec.AttemptDone(id, a, err == nil, fmt.Sprintf("%v %s", resp.Status, resp.Message), resp.Term)
		}
	}
	out := s.Send(ctx, rid, op, []byte(key), value, hook)
	switch {
	case out.Err == nil && out.Response.Status == kv.StatusNotFound:
		w.rec.End(id, lincheck.NotFound, nil, out.Response.Node, out.Response.Term, out.Response.Index)
	case out.Err == nil:
		w.rec.End(id, lincheck.OK, out.Response.Value, out.Response.Node, out.Response.Term, out.Response.Index)
	case !out.Known:
		w.rec.End(id, lincheck.Incomplete, nil, "", 0, 0)
	default:
		w.rec.End(id, lincheck.Rejected, nil, out.Response.Node, out.Response.Term, 0)
	}
}

// finish stops the clients and checks the history: linearizable, and not
// vacuous.
func (w *shardedWorkload) finish() lincheck.Counts {
	w.c.t.Helper()
	close(w.stop)
	w.wg.Wait()
	h := w.rec.History()
	res := lincheck.Check(h, lincheck.Options{Minimize: true})
	switch {
	case res.Unchecked:
		w.c.t.Fatalf("checker budget exceeded: %s", res.Reason)
	case !res.OK:
		w.c.t.Fatalf("NOT LINEARIZABLE\n%s\n--- minimized counterexample ---\n%s", res.Reason, lincheck.Format(res.Counterexample))
	}
	sum := h.Summary()
	if sum.OK+sum.NotFound == 0 {
		w.c.t.Fatal("no operation completed: the history proves nothing")
	}
	w.c.t.Logf("linearizable: %d ops (%d ok, %d notfound, %d rejected, %d incomplete) over %d keys",
		sum.Total, sum.OK, sum.NotFound, sum.Rejected, sum.Incomplete, sum.Keys)
	return sum
}

// completed counts the operations that completed so far.
func (w *shardedWorkload) completed() int {
	s := w.rec.History().Summary()
	return s.OK + s.NotFound
}

// waitProgress waits until at least n more operations complete.
func (w *shardedWorkload) waitProgress(n int) {
	w.c.t.Helper()
	target := w.completed() + n
	deadline := time.Now().Add(60 * time.Second)
	for w.completed() < target {
		if time.Now().After(deadline) {
			w.c.t.Fatalf("the workload stalled at %d completed operations\n%s", w.completed(), w.c.statusDump())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
