package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/snapshot"
)

// Phase 14 on real dkvd processes (docs/SNAPSHOTS.md): snapshots created,
// published, compacted behind, streamed over the InstallSnapshot transport
// kind, installed and restored, by processes with real files that are killed
// with SIGKILL at the exact boundaries the simulator crashes at. Every test
// ends with the whole-run checks: election safety across every process,
// log matching on the durable logs, and every node's durable state — its
// published snapshot plus the log suffix after it, read off disk — identical
// on every node and holding every acknowledged write.

var (
	reSnapshotEv = regexp.MustCompile(`event=raft_snapshot node=(\S+) index=(\d+) term=(\d+) boundary=(\d+)`)
	reRestored   = regexp.MustCompile(`event=raft_snapshot_restored node=(\S+) index=(\d+) term=(\d+) repaired=(\S+)`)
)

// snapEvery are the flags every process of these tests runs with: a snapshot
// every 16 applied entries, 4 entries kept behind it.
var snapEvery = []string{"-snapshot-every", "16", "-snapshot-retain", "4"}

func newSnapCluster(t *testing.T, extra ...string) *rcluster {
	t.Helper()
	c := newRClusterEvery(t, 3, snapEvery, extra...)
	c.waitClientReady(10 * time.Second)
	return c
}

func (c *rcluster) snapFiles(id string) snapshot.Files {
	return snapshot.Files{Base: filepath.Join(c.dirs[id], "raft-"+id+".log")}
}

// published loads and fully validates a node's published snapshot (read-only;
// safe while the process runs: publication is an atomic rename).
func (c *rcluster) published(id string) (snapshot.Meta, []byte, bool) {
	c.t.Helper()
	m, data, _, found, err := c.snapFiles(id).Load()
	if err != nil {
		c.t.Fatalf("%s's published snapshot: %v", id, err)
	}
	return m, data, found
}

// waitPublished waits until a node's published snapshot is at index ≥ min.
func (c *rcluster) waitPublished(id string, min uint64, d time.Duration) snapshot.Meta {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if m, _, ok := c.published(id); ok && m.Index >= min {
			return m
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("%s never published a snapshot at ≥ %d within %s\n%s", id, min, d, c.outputs())
	return snapshot.Meta{}
}

// writer writes keys through a session with retries under the same identity,
// so every acknowledged write took effect exactly once whatever the faults.
type writer struct {
	c     *rcluster
	s     *kv.Session
	acked map[string]string
	last  uint64 // the highest index an acknowledged write executed at
}

func (c *rcluster) newWriter(first ...string) *writer {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := kv.Register(ctx, c.doers(first...), kv.SessionOptions{AttemptTimeout: 2 * time.Second, MaxAttempts: 40})
	if err != nil {
		c.t.Fatalf("register: %v\n%s", err, c.outputs())
	}
	return &writer{c: c, s: s, acked: map[string]string{}}
}

func (w *writer) put(key, value string) kv.Response {
	w.c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out := w.s.Put(ctx, []byte(key), []byte(value), nil)
	if out.Err != nil {
		w.c.t.Fatalf("put %s=%s: %v (known %v, %d attempts)\n%s", key, value, out.Err, out.Known, out.Attempts, w.c.outputs())
	}
	w.acked[key] = value
	w.last = max(w.last, out.Response.Index)
	return out.Response
}

func (w *writer) putN(prefix string, n int) {
	w.c.t.Helper()
	for i := 0; i < n; i++ {
		w.put(fmt.Sprintf("%s%03d", prefix, i), fmt.Sprintf("v-%s-%d", prefix, i))
	}
}

// do sends one identified request, retrying the SAME request until an answer
// is definite: safe for a write precisely because of deduplication.
func (c *rcluster) do(req kv.Request) kv.Response {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		ep := c.doers()[i%len(c.ids)]
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req.Timeout = 2 * time.Second
		resp, err := ep.Do(ctx, req)
		cancel()
		if err == nil && resp.Status != kv.StatusNotLeader && resp.Status != kv.StatusUnavailable && resp.Status != kv.StatusUnknown {
			return resp
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no definite answer to %+v\n%s", req, c.outputs())
	return kv.Response{}
}

// durableStore rebuilds a node's state from its files: the published
// snapshot restored, then the log's entries after it through index applied.
func (c *rcluster) durableStore(id string, through uint64) *kv.Store {
	c.t.Helper()
	store := kv.NewStore()
	var at uint64
	if m, data, ok := c.published(id); ok {
		if m.Index > through {
			c.t.Fatalf("%s's snapshot at %d is past %d", id, m.Index, through)
		}
		if err := store.RestoreSnapshot(m.Index, m.Term, data); err != nil {
			c.t.Fatalf("%s's snapshot does not restore: %v", id, err)
		}
		at = m.Index
	}
	rec := c.liveLog(id)
	if rec.Boundary.Index > at {
		c.t.Fatalf("INV-SN3 on disk: %s's log is compacted through %d, its snapshot is at %d", id, rec.Boundary.Index, at)
	}
	for _, e := range rec.Entries {
		if e.Index <= at || e.Index > through {
			continue
		}
		if e.Index != at+1 {
			c.t.Fatalf("%s: entry %d follows %d", id, e.Index, at)
		}
		if _, err := store.ApplyResult(e.Index, e.Data); err != nil {
			c.t.Fatalf("%s: replaying %d: %v", id, e.Index, err)
		}
		at = e.Index
	}
	if at != through {
		c.t.Fatalf("%s's files reach index %d, not %d", id, at, through)
	}
	return store
}

// settleDurable waits until every node's durable commit reaches the leader's
// last index (no writes are running), and returns it.
func (c *rcluster) settleDurable(d time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		l, _, ok := c.latestLeader(c.running())
		if ok {
			last := c.liveLog(l).LastIndex()
			done := true
			for _, id := range c.ids {
				if rec := c.liveLog(id); rec.HardState.Commit < last || rec.LastIndex() < last {
					done = false
				}
			}
			if done {
				return last
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatalf("the durable logs did not converge within %s\n%s", d, c.outputs())
	return 0
}

// finishSnapshots is the whole-run check: every node's durable state, rebuilt
// from its snapshot and log, is identical, and holds every acknowledged write;
// then a clean shutdown, election safety and log matching.
func (c *rcluster) finishSnapshots(w *writer) {
	c.t.Helper()
	through := c.settleDurable(30 * time.Second)
	var ref *kv.Store
	for _, id := range c.ids {
		st := c.durableStore(id, through)
		if ref == nil {
			ref = st
			for k, v := range w.acked {
				if got, ok := st.Get([]byte(k)); !ok || string(got) != v {
					c.t.Fatalf("acknowledged %s=%s, %s's durable state has %q (%v)", k, v, id, got, ok)
				}
			}
			continue
		}
		if fmt.Sprint(st.Snapshot()) != fmt.Sprint(ref.Snapshot()) || fmt.Sprint(st.Sessions()) != fmt.Sprint(ref.Sessions()) {
			c.t.Fatalf("%s's durable state differs from %s's at index %d", id, c.ids[0], through)
		}
	}
	c.finish()
}

// waitBoundary waits until a node's durable log is compacted past index: a
// node compacts in the cycle that applies the entry that triggers its
// snapshot, which can come just after a write is acknowledged.
func (c *rcluster) waitBoundary(id string, past uint64, d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for c.liveLog(id).Boundary.Index <= past && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if b := c.liveLog(id).Boundary.Index; b <= past {
		c.t.Fatalf("premise: %s's log boundary %d does not pass %d\n%s", id, b, past, c.outputs())
	}
}

// lagFollower kills a follower, writes n keys, and waits until every other
// node's log is compacted past everything the follower holds — so whichever
// of them leads when it returns can bring it up to date only by a snapshot.
func (c *rcluster) lagFollower(w *writer, n int) (leader, lag string) {
	c.t.Helper()
	leader, _ = c.waitStable(c.ids, 0, 20*time.Second)
	lag = others(c.ids, leader)[0]
	c.kill(lag)
	behind := c.liveLog(lag).LastIndex()
	w.putN("lag", n)
	for _, id := range others(c.ids, lag) {
		c.waitBoundary(id, behind, 20*time.Second)
	}
	return leader, lag
}

// 1. The leader creates a snapshot: published, whole and valid, holding the
// acknowledged writes; the log compacted behind it; the log file shorter.
func TestRealLeaderCreatesSnapshot(t *testing.T) {
	c := newSnapCluster(t)
	w := c.newWriter()
	w.putN("a", 40)
	l, _ := c.waitStable(c.ids, 0, 20*time.Second)
	m := c.waitPublished(l, 32, 20*time.Second)
	c.waitBoundary(l, m.Index-5, 10*time.Second) // compaction follows publication
	rec := c.liveLog(l)
	if rec.Boundary.Index == 0 || rec.Boundary.Index > m.Index || rec.Boundary.Index+4 < m.Index {
		t.Fatalf("leader boundary %+v, snapshot %d (retain 4)", rec.Boundary, m.Index)
	}
	_, data, _ := c.published(l)
	st := kv.NewStore()
	if err := st.RestoreSnapshot(m.Index, m.Term, data); err != nil {
		t.Fatal(err)
	}
	if v, ok := st.Get([]byte("a000")); !ok || string(v) != "v-a-0" {
		t.Fatalf("the snapshot does not hold the first write: %q %v", v, ok)
	}
	if !reSnapshotEv.MatchString(c.procs[l].out.String()) {
		t.Fatal("no event=raft_snapshot")
	}
	c.finishSnapshots(w)
}

// 2. A follower down while the others compact receives the leader's snapshot
// over InstallSnapshot: it logs the transfer, publishes the snapshot, and its
// durable log records the boundary.
func TestRealFollowerReceivesSnapshot(t *testing.T) {
	c := newSnapCluster(t)
	w := c.newWriter()
	w.putN("a", 5)
	leader, lag := c.lagFollower(w, 60)
	c.start(lag)
	waitForLine(t, c.procs[lag], "event=raft_snapshot_received", 20*time.Second)
	m := c.waitPublished(lag, c.liveLog(leader).Boundary.Index, 20*time.Second)
	c.waitBoundary(lag, 0, 10*time.Second) // the boundary record follows publication
	if b := c.liveLog(lag).Boundary; b.Index == 0 || b.Index > m.Index {
		t.Fatalf("%s's durable log after the install: boundary %+v, snapshot %d", lag, b, m.Index)
	}
	if !strings.Contains(c.procs[leader].out.String(), "event=raft_snapshot_sent node="+leader+" to="+lag) {
		t.Fatalf("%s did not stream the snapshot", leader)
	}
	c.finishSnapshots(w)
}

// 3. After the install, the follower resumes normal replication: later
// writes reach its log as entries after the snapshot, and its state is the
// leader's.
func TestRealFollowerCatchesUpAfterCompaction(t *testing.T) {
	c := newSnapCluster(t)
	w := c.newWriter()
	w.putN("a", 5)
	_, lag := c.lagFollower(w, 60)
	c.start(lag)
	waitForLine(t, c.procs[lag], "event=raft_snapshot_received", 20*time.Second)
	w.putN("after", 6)
	c.waitCommit([]string{lag}, w.last, 20*time.Second)
	rec := c.liveLog(lag)
	if rec.LastIndex() < w.last || len(rec.Entries) == 0 {
		t.Fatalf("%s did not continue by entries after its snapshot: %+v last %d", lag, rec.Boundary, rec.LastIndex())
	}
	c.finishSnapshots(w)
}

// 4. The leader dies (SIGKILL, its own crash point) right after publishing a
// snapshot, before compacting its log: on disk, the new snapshot and the
// uncompacted log — coherent; its restart restores the snapshot and keeps
// the log after it, and the group continues.
func TestRealLeaderCrashesAfterSnapshotPublication(t *testing.T) {
	c := newSnapCluster(t, "-crash-at", "after-snapshot-publish:1", "-crash-armed-by-signal")
	w := c.newWriter()
	w.putN("a", 3)
	l, _ := c.waitStable(c.ids, 0, 20*time.Second)
	c.signal(l, syscall.SIGUSR1)
	waitForLine(t, c.procs[l], "event=crash_armed", 10*time.Second)
	before := uint64(0)
	if m, _, ok := c.published(l); ok {
		before = m.Index
	}
	// Writes run while the leader heads for its crash point; the session
	// retries through the other nodes once it is gone. The goroutine touches
	// only the session and its own map — never the cluster.
	acked := map[string]string{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 40; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			key := fmt.Sprintf("b%03d", i)
			out := w.s.Put(ctx, []byte(key), []byte("x"), nil)
			cancel()
			if out.Err != nil {
				return
			}
			acked[key] = "x"
		}
	}()
	point, _ := c.waitKilledAtPoint(l, 30*time.Second)
	<-done
	for k, v := range acked {
		w.acked[k] = v
	}
	if point != "after-snapshot-publish" {
		t.Fatalf("died at %s", point)
	}
	m, _, ok := c.published(l)
	if !ok || m.Index <= before {
		t.Fatalf("premise: the crash came after publishing a new snapshot (%d, had %d)", m.Index, before)
	}
	rec := c.liveLog(l)
	if rec.Boundary.Index >= m.Index-4 || rec.LastIndex() < m.Index {
		t.Fatalf("on disk after the crash: log boundary %d last %d, snapshot %d — the log should still hold the snapshot's prefix", rec.Boundary.Index, rec.LastIndex(), m.Index)
	}
	c.start(l)
	waitForLine(t, c.procs[l], fmt.Sprintf("event=raft_snapshot_restored node=%s index=%d", l, m.Index), 10*time.Second)
	w.putN("c", 5)
	c.finishSnapshots(w)
}

// 5. A follower dies installing the leader's snapshot — after publishing it,
// before its log records the boundary: on disk, the leader's snapshot and
// the follower's old log. Its restart completes the install itself
// (repaired=true) and it converges.
func TestRealFollowerCrashesDuringSnapshotInstallation(t *testing.T) {
	c := newSnapCluster(t)
	w := c.newWriter()
	w.putN("a", 5)
	_, lag := c.lagFollower(w, 60)
	behind := c.liveLog(lag)
	c.startWith(lag, "-crash-at", "after-install-publish:1")
	if point, _ := c.waitKilledAtPoint(lag, 30*time.Second); point != "after-install-publish" {
		t.Fatalf("died at %s", point)
	}
	m, _, ok := c.published(lag)
	rec := c.liveLog(lag)
	if !ok || rec.Boundary.Index >= m.Index || rec.LastIndex() >= m.Index {
		t.Fatalf("on disk after the crash: snapshot %d (found %v), log boundary %d last %d (was %d)", m.Index, ok, rec.Boundary.Index, rec.LastIndex(), behind.LastIndex())
	}
	c.start(lag)
	waitForLine(t, c.procs[lag], fmt.Sprintf("event=raft_snapshot_restored node=%s index=%d term=%d repaired=true", lag, m.Index, m.Term), 10*time.Second)
	if b := c.liveLog(lag).Boundary; b.Index != m.Index {
		t.Fatalf("the repaired log's boundary is %d, want %d", b.Index, m.Index)
	}
	w.putN("c", 5)
	c.finishSnapshots(w)
}

// 6. Every process is killed at once; each restarts from its snapshot and
// the log suffix after it, and the group serves reads and writes again.
func TestRealFullClusterRestartFromSnapshot(t *testing.T) {
	c := newSnapCluster(t)
	w := c.newWriter()
	w.putN("a", 50)
	for _, id := range c.ids {
		c.waitPublished(id, 32, 20*time.Second)
	}
	for _, id := range c.ids {
		c.kill(id)
	}
	snaps := map[string]uint64{}
	for _, id := range c.ids {
		m, _, _ := c.published(id)
		snaps[id] = m.Index
		c.start(id)
	}
	for _, id := range c.ids {
		waitForLine(t, c.procs[id], fmt.Sprintf("event=raft_snapshot_restored node=%s index=%d", id, snaps[id]), 10*time.Second)
	}
	c.waitClientReady(10 * time.Second)
	resp := c.do(kv.Request{Op: kv.ReqGet, Key: []byte("a000")})
	if resp.Status != kv.StatusOK || string(resp.Value) != "v-a-0" {
		t.Fatalf("a read after the restart: %+v", resp)
	}
	w.putN("b", 5)
	c.finishSnapshots(w)
}

// identified sets up the Phase 13 retry case across a snapshot: a session's
// request R (id 1) writes k=v and executes at index I; request 2 writes k=w;
// enough writes follow that every node snapshots and compacts past I; then
// every process is killed and restarted, so the session table can come only
// from the snapshots.
func identifiedAcrossSnapshots(t *testing.T) (c *rcluster, w *writer, clientID, rIndex uint64) {
	c = newSnapCluster(t)
	w = c.newWriter()
	reg := c.do(kv.Request{Op: kv.ReqRegister})
	if reg.Status != kv.StatusOK || reg.ClientID == 0 {
		t.Fatalf("register: %+v", reg)
	}
	clientID = reg.ClientID
	r := c.do(kv.Request{Op: kv.ReqPut, ClientID: clientID, RequestID: 1, AckedBelow: 1, Key: []byte("k"), Value: []byte("v")})
	if r.Status != kv.StatusOK || r.Duplicate {
		t.Fatalf("R: %+v", r)
	}
	rIndex = r.Index
	if r2 := c.do(kv.Request{Op: kv.ReqPut, ClientID: clientID, RequestID: 2, AckedBelow: 1, Key: []byte("k"), Value: []byte("w")}); r2.Status != kv.StatusOK {
		t.Fatalf("request 2: %+v", r2)
	}
	w.acked["k"] = "w"
	w.putN("fill", 50)
	c.settleDurable(20 * time.Second)
	for _, id := range c.ids {
		c.waitBoundary(id, rIndex, 20*time.Second) // R's entry is gone from every log
	}
	for _, id := range c.ids {
		c.kill(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	c.waitClientReady(10 * time.Second)
	return c, w, clientID, rIndex
}

// 7. The retry of an already-executed request, after its entry was compacted
// into snapshots on every node and every process restarted from them, is
// answered as a duplicate of its original index — and does not execute
// again (the key keeps the later write).
func TestRealRetryAfterSnapshotIsADuplicate(t *testing.T) {
	c, w, clientID, rIndex := identifiedAcrossSnapshots(t)
	retry := c.do(kv.Request{Op: kv.ReqPut, ClientID: clientID, RequestID: 1, AckedBelow: 1, Key: []byte("k"), Value: []byte("v")})
	if retry.Status != kv.StatusOK || !retry.Duplicate || retry.Index != rIndex {
		t.Fatalf("the retry of R: %+v, want OK duplicate of index %d", retry, rIndex)
	}
	if got := c.do(kv.Request{Op: kv.ReqGet, Key: []byte("k")}); got.Status != kv.StatusOK || string(got.Value) != "w" {
		t.Fatalf("k after the retry: %+v (the retry executed again?)", got)
	}
	c.finishSnapshots(w)
}

// 8. A different command under R's identity, after the snapshots and restarts,
// is a conflict — no effect — as it was before.
func TestRealConflictingRequestIDAfterSnapshot(t *testing.T) {
	c, w, clientID, _ := identifiedAcrossSnapshots(t)
	conflict := c.do(kv.Request{Op: kv.ReqPut, ClientID: clientID, RequestID: 1, AckedBelow: 1, Key: []byte("k"), Value: []byte("other")})
	if conflict.Status != kv.StatusConflict {
		t.Fatalf("a different command under R's identity: %+v, want CONFLICT", conflict)
	}
	if got := c.do(kv.Request{Op: kv.ReqGet, Key: []byte("k")}); got.Status != kv.StatusOK || string(got.Value) != "w" {
		t.Fatalf("k after the conflict: %+v", got)
	}
	c.finishSnapshots(w)
}

// 9. The leader is killed after snapshots; a new leader — whose own log is
// compacted — continues, snapshots again, and brings the old leader back by
// a snapshot when it rejoins behind the new leader's boundary.
func TestRealSnapshotAndLeaderChange(t *testing.T) {
	c := newSnapCluster(t)
	w := c.newWriter()
	w.putN("a", 30)
	l1, t1 := c.waitStable(c.ids, 0, 20*time.Second)
	for _, id := range c.ids {
		c.waitBoundary(id, 0, 20*time.Second) // every log compacted, the next leader's too
	}
	c.kill(l1)
	behind := c.liveLog(l1).LastIndex()
	l2, _ := c.waitStable(others(c.ids, l1), t1, 20*time.Second)
	w.putN("b", 40)
	c.waitBoundary(l2, behind, 20*time.Second)
	c.start(l1)
	waitForLine(t, c.procs[l1], "event=raft_snapshot_received", 20*time.Second)
	w.putN("c", 5)
	c.finishSnapshots(w)
}

// 10. A follower cut off by a partition while the majority commits and
// compacts past it rejoins when the partition heals and catches up by a
// snapshot; a request retried across the partition is still deduplicated.
func TestRealSnapshotPartitionAndRecovery(t *testing.T) {
	c := newSnapCluster(t)
	w := c.newWriter()
	w.putN("a", 5)
	leader, _ := c.waitStable(c.ids, 0, 20*time.Second)
	cut := others(c.ids, leader)[0]
	c.isolate(cut)
	behind := c.liveLog(cut).LastIndex()
	w.putN("p", 60)
	c.waitBoundary(leader, behind, 20*time.Second)
	c.healAll()
	waitForLine(t, c.procs[cut], "event=raft_snapshot_received", 30*time.Second)
	w.putN("h", 5)
	c.finishSnapshots(w)
}

// sanity for the parsing helpers used above.
func TestSnapshotEventPatterns(t *testing.T) {
	m := reRestored.FindStringSubmatch("event=raft_snapshot_restored node=n2 index=48 term=3 repaired=true")
	if m == nil || m[1] != "n2" || m[4] != "true" {
		t.Fatalf("%v", m)
	}
	if i, _ := strconv.Atoi(reSnapshotEv.FindStringSubmatch("event=raft_snapshot node=n1 index=32 term=2 boundary=28")[2]); i != 32 {
		t.Fatal(i)
	}
}
