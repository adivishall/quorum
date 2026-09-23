package integration

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raftlog"
)

// Phase 10 real-process fault tests (docs/FAULTS.md). Real cmd/dkvd processes run
// Raft over real TCP; faults are real: SIGKILL (process death), SIGSTOP/SIGCONT (a
// frozen process — a GC pause or a hung host), restart on the same data directory,
// and network partitions made by cutting test-owned TCP proxies on the node-to-node
// links. Timing is real, so these are not replayable from a seed (the simulator
// is); what they assert must hold under any timing:
//
//   - election safety across processes: no two processes ever announce leadership
//     of the same term (their raft_leader events, each derived from one consistent
//     status snapshot). Leadership shorter than dkvd's 20 ms status poll can be
//     missed, so this check can miss a violation but can never invent one;
//   - durability of committed entries across real crashes: the committed prefix
//     read from a node's log before a fault is a prefix of every node's log after;
//   - log matching and convergence of the durable logs on disk after the run.

var (
	reLeader   = regexp.MustCompile(`event=raft_leader node=(\S+) term=(\d+)`)
	reFollower = regexp.MustCompile(`event=raft_follower node=(\S+) term=(\d+) leader=(\S+)`)
	reCommit   = regexp.MustCompile(`event=raft_commit node=(\S+) index=(\d+)`)
)

// rcluster is a group of real dkvd -raft processes whose links run through proxies.
type rcluster struct {
	t       *testing.T
	bin     string
	ids     []string
	addrs   map[string]string
	dirs    map[string]string
	proxies map[[2]string]*tcpProxy // keyed by (dialer, accepter), dialer < accepter
	procs   map[string]*dkvNode     // the current process of each node (nil = not running)
	history []*dkvNode              // every process ever started, for election-safety checks
}

func newRCluster(t *testing.T, n int) *rcluster {
	t.Helper()
	c := &rcluster{
		t: t, bin: buildDkvd(t), addrs: map[string]string{}, dirs: map[string]string{},
		proxies: map[[2]string]*tcpProxy{}, procs: map[string]*dkvNode{},
	}
	root := t.TempDir()
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.addrs[id] = freeTCPAddr(t)
		c.dirs[id] = filepath.Join(root, id)
	}
	for i, a := range c.ids {
		for _, b := range c.ids[i+1:] {
			c.proxies[[2]string{a, b}] = startProxy(t, c.addrs[b])
		}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(c.killAll)
	return c
}

// start launches (or relaunches) a node on its data dir and listen address. Its
// peer address for every higher id is that link's proxy; lower ids dial it, so
// their entry is only a placeholder the transport validates and never dials.
func (c *rcluster) start(id string) {
	c.t.Helper()
	var peers []string
	for _, other := range c.ids {
		switch {
		case other == id:
		case id < other:
			peers = append(peers, other+"="+c.proxies[[2]string{id, other}].Addr())
		default:
			peers = append(peers, other+"="+c.addrs[other])
		}
	}
	buf := &safeBuf{}
	cmd := exec.Command(c.bin, "-id", id, "-listen", c.addrs[id], "-peers", strings.Join(peers, ","),
		"-raft", "-data-dir", c.dirs[id], "-tick-interval", "25ms")
	cmd.Stdout, cmd.Stderr = buf, buf
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	p := &dkvNode{id: id, addr: c.addrs[id], cmd: cmd, out: buf}
	c.procs[id] = p
	c.history = append(c.history, p)
}

// kill SIGKILLs a node and reaps it: no shutdown path runs.
func (c *rcluster) kill(id string) {
	c.t.Helper()
	p := c.procs[id]
	if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		c.t.Fatalf("kill %s: %v", id, err)
	}
	_ = p.cmd.Wait()
	c.procs[id] = nil
}

func (c *rcluster) signal(id string, sig syscall.Signal) {
	c.t.Helper()
	if err := c.procs[id].cmd.Process.Signal(sig); err != nil {
		c.t.Fatalf("signal %v to %s: %v", sig, id, err)
	}
}

func (c *rcluster) killAll() {
	for _, p := range c.procs {
		if p != nil {
			_ = p.cmd.Process.Signal(syscall.SIGCONT)
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
	}
}

// isolate cuts every link touching id; healAll restores every link.
func (c *rcluster) isolate(id string) {
	for k, p := range c.proxies {
		if k[0] == id || k[1] == id {
			p.Cut()
		}
	}
}

func (c *rcluster) healAll() {
	for _, p := range c.proxies {
		p.Heal()
	}
}

func (c *rcluster) cutAll() {
	for _, p := range c.proxies {
		p.Cut()
	}
}

// running returns the ids with a live process, in order.
func (c *rcluster) running() []string {
	var out []string
	for _, id := range c.ids {
		if c.procs[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

// latestLeader returns the current leader among ids, if one is confirmed: the
// highest term any of their current processes has seen (in a leader OR follower
// event) must have been announced as led by one of ids, and another node among
// ids must report following it in that term (a quorum of a 3-node group agrees).
// Taking the maximum over follower events too means a survivor's stale leadership
// of an old term is never mistaken for the current leader.
func (c *rcluster) latestLeader(ids []string) (string, uint64, bool) {
	var maxTerm uint64
	leaderOf := map[uint64]string{}
	for _, id := range ids {
		p := c.procs[id]
		if p == nil {
			continue
		}
		out := p.out.String()
		for _, m := range reLeader.FindAllStringSubmatch(out, -1) {
			tm, _ := strconv.ParseUint(m[2], 10, 64)
			leaderOf[tm] = id
			if tm > maxTerm {
				maxTerm = tm
			}
		}
		for _, m := range reFollower.FindAllStringSubmatch(out, -1) {
			if tm, _ := strconv.ParseUint(m[2], 10, 64); tm > maxTerm {
				maxTerm = tm
			}
		}
	}
	leader, ok := leaderOf[maxTerm]
	if !ok || maxTerm == 0 {
		return "", 0, false
	}
	want := fmt.Sprintf("term=%d leader=%s", maxTerm, leader)
	for _, id := range ids {
		if p := c.procs[id]; id != leader && p != nil && strings.Contains(p.out.String(), "event=raft_follower node="+id+" "+want) {
			return leader, maxTerm, true
		}
	}
	return "", 0, false
}

// waitLeader waits for a confirmed leader among ids in a term above minTerm.
func (c *rcluster) waitLeader(ids []string, minTerm uint64, d time.Duration) (string, uint64) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if l, tm, ok := c.latestLeader(ids); ok && tm > minTerm {
			return l, tm
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no confirmed leader among %v above term %d within %s\n%s", ids, minTerm, d, c.outputs())
	return "", 0
}

// commitOf is the highest commit index a node's current process reported.
func (c *rcluster) commitOf(id string) uint64 {
	var max uint64
	if p := c.procs[id]; p != nil {
		for _, m := range reCommit.FindAllStringSubmatch(p.out.String(), -1) {
			if v, _ := strconv.ParseUint(m[2], 10, 64); v > max {
				max = v
			}
		}
	}
	return max
}

// waitCommit waits until every id has reported committing at least idx.
func (c *rcluster) waitCommit(ids []string, idx uint64, d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			if c.commitOf(id) < idx {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("%v did not all commit through %d within %s\n%s", ids, idx, d, c.outputs())
}

// waitFollows waits until id's current process reports following leader in a term
// of at least term.
func (c *rcluster) waitFollows(id, leader string, term uint64, d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, m := range reFollower.FindAllStringSubmatch(c.procs[id].out.String(), -1) {
			if tm, _ := strconv.ParseUint(m[2], 10, 64); m[3] == leader && tm >= term {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("%s never followed %s at term >= %d within %s\n%s", id, leader, term, d, c.outputs())
}

// liveLog reads a node's durable log (read-only; safe while the process runs).
func (c *rcluster) liveLog(id string) *raftlog.Recovered {
	c.t.Helper()
	rec, err := raftlog.Inspect(filepath.Join(c.dirs[id], "raft-"+id+".log"))
	if err != nil {
		c.t.Fatalf("inspect %s: %v", id, err)
	}
	return rec
}

// committedPrefix is a node's log up to the commit index it reported.
func (c *rcluster) committedPrefix(id string) []raftlog.Entry {
	entries := c.liveLog(id).Entries
	commit := c.commitOf(id)
	if commit > uint64(len(entries)) {
		c.t.Fatalf("%s reports commit %d but its durable log holds %d entries", id, commit, len(entries))
	}
	return entries[:commit]
}

// finish stops every process with SIGTERM (requiring a clean exit 0) and then
// checks the whole run: election safety across every process that ever ran, log
// matching across the durable logs, and that every log starts with each prefix in
// mustKeep (entries known committed before a fault).
func (c *rcluster) finish(mustKeep ...[]raftlog.Entry) {
	c.t.Helper()
	for _, id := range c.running() {
		c.signal(id, syscall.SIGCONT)
		c.signal(id, syscall.SIGTERM)
	}
	for _, id := range c.running() {
		if err := waitExit(c.procs[id], 15*time.Second); err != nil {
			c.t.Fatalf("%s did not exit cleanly: %v\n%s", id, err, c.procs[id].out.String())
		}
		c.procs[id] = nil
	}
	c.checkElectionSafety()
	logs := map[string][]raftlog.Entry{}
	for _, id := range c.ids {
		logs[id] = c.liveLog(id).Entries
	}
	for i, a := range c.ids {
		for _, b := range c.ids[i+1:] {
			checkLogMatching(c.t, a, logs[a], b, logs[b])
		}
	}
	for _, prefix := range mustKeep {
		for _, id := range c.ids {
			if len(logs[id]) < len(prefix) {
				c.t.Fatalf("%s lost committed entries: holds %d, %d were committed", id, len(logs[id]), len(prefix))
			}
			for i, e := range prefix {
				if g := logs[id][i]; g.Term != e.Term || string(g.Data) != string(e.Data) {
					c.t.Fatalf("%s index %d is (t%d) but (t%d) was committed there", id, i+1, g.Term, e.Term)
				}
			}
		}
	}
}

// checkElectionSafety fails if two processes ever announced leadership of one term.
func (c *rcluster) checkElectionSafety() {
	c.t.Helper()
	leaders := map[uint64]map[string]bool{}
	for _, p := range c.history {
		for _, m := range reLeader.FindAllStringSubmatch(p.out.String(), -1) {
			tm, _ := strconv.ParseUint(m[2], 10, 64)
			if leaders[tm] == nil {
				leaders[tm] = map[string]bool{}
			}
			leaders[tm][m[1]] = true
		}
	}
	for tm, set := range leaders {
		if len(set) > 1 {
			var who []string
			for id := range set {
				who = append(who, id)
			}
			sort.Strings(who)
			c.t.Fatalf("INV-R1 violated across real processes: %v all led term %d", who, tm)
		}
	}
	if len(leaders) == 0 {
		c.t.Fatal("no process ever announced leadership; the run proved nothing")
	}
}

// checkLogMatching: if two logs hold an entry with the same index and term, they
// are identical up to it (INV-R3, on the bytes on disk).
func checkLogMatching(t *testing.T, a string, la []raftlog.Entry, b string, lb []raftlog.Entry) {
	t.Helper()
	hi := len(la)
	if len(lb) < hi {
		hi = len(lb)
	}
	for i := hi - 1; i >= 0; i-- {
		if la[i].Term != lb[i].Term {
			continue
		}
		for j := 0; j <= i; j++ {
			if la[j].Term != lb[j].Term || string(la[j].Data) != string(lb[j].Data) {
				t.Fatalf("durable logs of %s and %s share (index %d, term %d) but differ at %d", a, b, i+1, la[i].Term, j+1)
			}
		}
		return
	}
}

func (c *rcluster) outputs() string {
	var b strings.Builder
	for _, id := range c.ids {
		if p := c.procs[id]; p != nil {
			fmt.Fprintf(&b, "--- %s ---\n%s\n", id, p.out.String())
		}
	}
	return b.String()
}

func others(ids []string, x string) []string {
	var out []string
	for _, id := range ids {
		if id != x {
			out = append(out, id)
		}
	}
	return out
}

// TestRealLeaderCrashAndReelection (scenario C): SIGKILL the leader of a real
// 3-process group after it has committed; the survivors elect a leader in a higher
// term and commit; the killed node restarts on its data dir and follows. Every
// entry committed before the crash is in every log afterwards.
func TestRealLeaderCrashAndReelection(t *testing.T) {
	c := newRCluster(t, 3)
	l1, t1 := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit(c.ids, 1, 20*time.Second)
	committed := c.committedPrefix(l1)

	c.kill(l1)
	l2, t2 := c.waitLeader(others(c.ids, l1), t1, 20*time.Second)
	c.waitCommit(others(c.ids, l1), uint64(len(committed))+1, 20*time.Second) // l2's no-op

	c.start(l1)
	c.waitFollows(l1, l2, t2, 20*time.Second)
	c.waitCommit(c.ids, c.commitOf(l2), 20*time.Second)
	c.finish(committed, c.committedPrefix(l2))
}

// TestRealFollowerCrashAndCatchUp (scenario B): a follower is SIGKILLed; while it
// is down the rest of the group moves to a new term (the leader is restarted, and
// a new election appends and commits a no-op the dead follower never saw); the
// follower restarts on its data dir and catches up.
func TestRealFollowerCrashAndCatchUp(t *testing.T) {
	c := newRCluster(t, 3)
	l1, t1 := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit(c.ids, 1, 20*time.Second)
	f := others(c.ids, l1)[0]
	g := others(c.ids, l1)[1]

	c.kill(f)
	c.kill(l1)
	c.start(l1)
	l2, t2 := c.waitLeader([]string{l1, g}, t1, 20*time.Second)
	c.waitCommit([]string{l1, g}, 2, 20*time.Second)
	behind := c.committedPrefix(l2)
	if got := len(c.liveLog(f).Entries); got >= len(behind) {
		t.Fatalf("setup: the dead follower already holds %d of %d committed entries", got, len(behind))
	}

	c.start(f)
	c.waitFollows(f, l2, t2, 20*time.Second)
	c.waitCommit(c.ids, uint64(len(behind)), 20*time.Second)
	c.finish(behind)
}

// TestRealFrozenLeaderStepsDown (scenario A, process level): the leader is frozen
// with SIGSTOP — it keeps its sockets and its belief that it leads, but runs no
// code. The others elect a new leader in a higher term. On SIGCONT the old leader
// processes the backlog, learns the higher term, and follows; never two leaders in
// one term.
func TestRealFrozenLeaderStepsDown(t *testing.T) {
	c := newRCluster(t, 3)
	l1, t1 := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit(c.ids, 1, 20*time.Second)
	committed := c.committedPrefix(l1)

	c.signal(l1, syscall.SIGSTOP)
	l2, t2 := c.waitLeader(others(c.ids, l1), t1, 20*time.Second)
	c.waitCommit(others(c.ids, l1), uint64(len(committed))+1, 20*time.Second)
	c.signal(l1, syscall.SIGCONT)
	c.waitFollows(l1, l2, t2, 20*time.Second)
	c.finish(committed, c.committedPrefix(l2))
}

// TestRealIsolatedLeaderRejoinsAfterPartition (scenario A, network level): every
// link of the leader is cut at the TCP level while its process keeps running.
// The others elect a leader in a higher term; the old leader cannot commit
// anything new; after the links heal (the transport redials), it follows.
func TestRealIsolatedLeaderRejoinsAfterPartition(t *testing.T) {
	c := newRCluster(t, 3)
	l1, t1 := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit(c.ids, 1, 20*time.Second)
	committed := c.committedPrefix(l1)
	before := c.commitOf(l1)

	c.isolate(l1)
	l2, t2 := c.waitLeader(others(c.ids, l1), t1, 20*time.Second)
	c.waitCommit(others(c.ids, l1), uint64(len(committed))+1, 20*time.Second)
	if got := c.commitOf(l1); got != before {
		t.Fatalf("the isolated leader's commit moved from %d to %d without a quorum", before, got)
	}

	c.healAll()
	c.waitFollows(l1, l2, t2, 20*time.Second)
	c.waitCommit(c.ids, c.commitOf(l2), 20*time.Second)
	c.finish(committed)
}

// TestRealConnectionFlapping repeatedly severs and restores every link while the
// processes keep running — connection loss, redial, and in-flight messages lost
// mid-election — then requires the group to settle on one leader that everyone
// follows, with no process crashed and every durable log consistent.
func TestRealConnectionFlapping(t *testing.T) {
	c := newRCluster(t, 3)
	_, t1 := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit(c.ids, 1, 20*time.Second)
	for i := 0; i < 8; i++ {
		c.cutAll()
		time.Sleep(time.Duration(100+40*i) * time.Millisecond)
		c.healAll()
		time.Sleep(300 * time.Millisecond)
	}
	for _, id := range c.ids {
		if err := c.procs[id].cmd.Process.Signal(syscall.Signal(0)); err != nil {
			t.Fatalf("%s died during connection flapping (%v)\n%s", id, err, c.procs[id].out.String())
		}
	}
	l, tl := c.waitLeader(c.ids, t1-1, 30*time.Second)
	for _, id := range others(c.ids, l) {
		c.waitFollows(id, l, tl, 20*time.Second)
	}
	c.waitCommit(c.ids, c.commitOf(l), 20*time.Second)
	c.finish(c.committedPrefix(l))
}

// TestRealRestartWhileIsolated (scenario I): a follower is cut off, killed, and
// restarted while still cut off, so it campaigns in vain and inflates its term.
// Meanwhile the others move to a new term and commit an entry it lacks. When the
// links heal, its higher term forces a new election that it cannot win with a
// stale log; the group converges and nothing committed is lost.
func TestRealRestartWhileIsolated(t *testing.T) {
	c := newRCluster(t, 3)
	l1, t1 := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit(c.ids, 1, 20*time.Second)
	f := others(c.ids, l1)[0]
	g := others(c.ids, l1)[1]

	c.isolate(f)
	c.kill(f)
	c.kill(l1) // force a new term among the reachable pair
	c.start(l1)
	l2, t2 := c.waitLeader([]string{l1, g}, t1, 20*time.Second)
	c.waitCommit([]string{l1, g}, 2, 20*time.Second)
	committed := c.committedPrefix(l2)

	c.start(f) // restarts still isolated
	time.Sleep(1500 * time.Millisecond)
	c.healAll()
	l3, t3 := c.waitLeader(c.ids, t2-1, 30*time.Second)
	for _, id := range others(c.ids, l3) {
		c.waitFollows(id, l3, t3, 20*time.Second)
	}
	c.waitCommit(c.ids, uint64(len(committed)), 20*time.Second)
	c.finish(committed)
}

// TestRealRepeatedCrashRestart (scenario H): every node in turn is SIGKILLed and
// restarted, twice around, with a leader elected and committing between crashes.
// Every entry committed at any checkpoint must survive in every final log.
func TestRealRepeatedCrashRestart(t *testing.T) {
	c := newRCluster(t, 3)
	_, term := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit(c.ids, 1, 20*time.Second)
	var checkpoints [][]raftlog.Entry
	for round := 0; round < 6; round++ {
		victim := c.ids[round%3]
		c.kill(victim)
		l, tl := c.waitLeader(others(c.ids, victim), 0, 20*time.Second)
		if tl < term {
			t.Fatalf("leader term went backwards: %d after %d", tl, term)
		}
		term = tl
		checkpoints = append(checkpoints, c.committedPrefix(l))
		c.start(victim)
		c.waitCommit([]string{victim}, uint64(len(checkpoints[len(checkpoints)-1])), 20*time.Second)
	}
	l, tl := c.waitLeader(c.ids, 0, 20*time.Second)
	for _, id := range others(c.ids, l) {
		c.waitFollows(id, l, tl, 20*time.Second)
	}
	c.finish(checkpoints...)
}
