package integration

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/storage"
)

// S4 on real dkvd processes (docs/STORAGE_INTEGRATION.md §10): a follower
// installs a snapshot larger than the ~63 MiB the S3 LSM machine could install
// as one engine batch — streamed from the leader's published file, validated
// by streaming, ingested into new tables and committed by one MANIFEST edit —
// restarts on it, converges, and keeps its sessions; and a follower that dies
// during such an install recovers. Run on both machines like every real test
// (QUORUM_STATE_MACHINE): on the in-memory machine the same snapshots go
// through the same streamed plumbing. Every test ends with the whole-run
// checks of finishSnapshots and, on the LSM machine, every node's ENGINE read
// back offline and compared with its durable state.

// s3InstallLimit is the most the S3 LSM machine installed: one WAL record
// less room for its framing.
const s3InstallLimit = record.MaxRecordSize - 1<<20

// bigValue is one large write: well inside one command (kv.MaxValueLen).
const bigValue = 1000 << 10

var reReceivedFrom = regexp.MustCompile(`event=raft_snapshot_received node=(\S+) from=(\S+) index=(\d+)`)

// largeSnapEvery: a snapshot every 32 entries, so that the snapshots are
// large ones rather than many.
var largeSnapEvery = []string{"-snapshot-every", "32", "-snapshot-retain", "4"}

// snapSize is the size of a node's published snapshot file (0: none).
func (c *rcluster) snapSize(id string) int64 {
	info, err := os.Stat(c.snapFiles(id).Path())
	if errors.Is(err, fs.ErrNotExist) {
		return 0
	}
	if err != nil {
		c.t.Fatal(err)
	}
	return info.Size()
}

// growSnapshots writes big values until every node of ids has published a
// snapshot larger than min bytes, and returns how many it wrote.
func (c *rcluster) growSnapshots(w *writer, min int64, ids []string) int {
	c.t.Helper()
	val := strings.Repeat("v", bigValue)
	n := 0
	for {
		small := false
		for _, id := range ids {
			if c.snapSize(id) <= min {
				small = true
			}
		}
		if !small {
			return n
		}
		if n > 300 {
			c.t.Fatalf("premise: 300 MiB written and a snapshot is still not above %d bytes\n%s", min, c.outputs())
		}
		w.put(fmt.Sprintf("big%04d", n), val[:len(val)-n%101])
		n++
	}
}

// lagForLargeSnapshot registers a session and executes request R through it,
// kills a follower, grows every other node's published snapshot past the S3
// limit and compacts past the follower: whichever node leads when it returns
// can bring it back only by a snapshot above that limit.
func lagForLargeSnapshot(t *testing.T) (c *rcluster, w *writer, lag string, clientID, rIndex uint64) {
	c = newRClusterEvery(t, 3, largeSnapEvery)
	c.waitClientReady(10 * time.Second)
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
	w.acked["k"] = "v"
	w.putN("a", 3)
	leader, _ := c.waitStable(c.ids, 0, 20*time.Second)
	lag = others(c.ids, leader)[0]
	c.kill(lag)
	behind := c.liveLog(lag).LastIndex()
	start := time.Now()
	n := c.growSnapshots(w, s3InstallLimit+8<<20, others(c.ids, lag))
	for _, id := range others(c.ids, lag) {
		c.waitBoundary(id, behind, 30*time.Second)
	}
	t.Logf("%s down at index %d; %d writes of ~%d KiB later (%.1fs) every other node's snapshot is %v bytes",
		lag, behind, n, bigValue>>10, time.Since(start).Seconds(),
		[]int64{c.snapSize(others(c.ids, lag)[0]), c.snapSize(others(c.ids, lag)[1])})
	return c, w, lag, clientID, rIndex
}

// finishLarge is finishSnapshots's whole-run check for large states — every
// node's durable state (its published snapshot and the log after it) is the
// same, compared by the SHA-256 of its canonical encoding rather than as
// formatted maps, and holds every acknowledged write; then a clean shutdown,
// election safety and log matching — and then, on the LSM machine, every
// node's ENGINE opened offline: it must hold exactly that durable state at the
// engine's own applied index, sessions included, durable at or above its
// published snapshot.
func (c *rcluster) finishLarge(w *writer) {
	c.t.Helper()
	through := c.settleDurable(60 * time.Second)
	sums := map[string][32]byte{}
	for i, id := range c.ids {
		st := c.durableStore(id, through)
		if i == 0 {
			for k, v := range w.acked {
				if got, ok := st.Get([]byte(k)); !ok || string(got) != v {
					c.t.Fatalf("acknowledged %s (%d bytes), %s's durable state has %d bytes (%v)", k, len(v), id, len(got), ok)
				}
			}
		}
		_, enc, _ := st.EncodeSnapshot()
		sums[id] = sha256.Sum256(enc)
		if sums[id] != sums[c.ids[0]] {
			c.t.Fatalf("%s's durable state differs from %s's at index %d", id, c.ids[0], through)
		}
	}
	c.finish()
	for _, id := range c.ids {
		dir := filepath.Join(c.dirs[id], "lsm")
		if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
			continue // the in-memory machine
		}
		m, err := kv.OpenLSMMachine(dir, kv.DefaultLimits, storage.DefaultOptions())
		if err != nil {
			c.t.Fatalf("%s's engine does not open: %v", id, err)
		}
		e, durable := m.Applied(), m.DurableAppliedIndex()
		snap, _, _ := c.published(id)
		if e < snap.Index || durable < snap.Index {
			c.t.Fatalf("%s's engine is at %d, durable through %d, below its published snapshot %d", id, e, durable, snap.Index)
		}
		want, ok := sums[id]
		if e != through {
			_, enc, _ := c.durableStore(id, e).EncodeSnapshot()
			want, ok = sha256.Sum256(enc), true
		}
		gi, got, err := m.EncodeSnapshot()
		if err != nil || !ok || gi != e || sha256.Sum256(got) != want {
			c.t.Fatalf("%s's engine at %d is not its durable state there (%v)", id, gi, err)
		}
		_ = m.Close()
		c.t.Logf("%s's engine: applied %d (the durable state's), durable through %d, published snapshot %d",
			id, e, durable, snap.Index)
	}
}

// waitInstalled waits for a node's install of a snapshot from a leader and
// returns the sender and the snapshot's index, from the node's own record.
func waitInstalled(t *testing.T, c *rcluster, id string) (from string, index uint64) {
	t.Helper()
	waitForLine(t, c.procs[id], "event=raft_snapshot_installed node="+id, 120*time.Second)
	got := reReceivedFrom.FindAllStringSubmatch(c.procs[id].out.String(), -1)
	if len(got) == 0 {
		t.Fatalf("%s installed a snapshot it never logged receiving", id)
	}
	last := got[len(got)-1]
	index, _ = strconv.ParseUint(last[3], 10, 64)
	return last[2], index
}

// sentBytes is the size of the snapshot the sender logged streaming to a node
// at an index, waiting for the line (the sender logs once the stream completes).
func sentBytes(t *testing.T, c *rcluster, from, to string, index uint64) int64 {
	t.Helper()
	p := c.procs[from]
	if p == nil {
		t.Fatalf("%s received a snapshot from %s, which is not running", to, from)
	}
	waitForLine(t, p, fmt.Sprintf("event=raft_snapshot_sent node=%s to=%s index=%d", from, to, index), 30*time.Second)
	m := regexp.MustCompile(fmt.Sprintf(`event=raft_snapshot_sent node=%s to=%s index=%d term=\d+ bytes=(\d+)`, from, to, index)).
		FindStringSubmatch(p.out.String())
	if m == nil {
		t.Fatalf("%s's send log carries no size", from)
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	if !strings.Contains(p.out.String(), "event=raft_leader node="+from+" ") {
		t.Fatalf("%s streamed a snapshot to %s but was never leader", from, to)
	}
	return n
}

// TestRealFollowerInstallsASnapshotAboveTheS3Limit: the S4 exit criterion on
// real processes. A follower returns behind a boundary every other node has
// compacted past, and the leader of the moment streams it a snapshot above
// the S3 limit; the follower installs it — on the LSM machine without its heap
// ever reaching half the snapshot's size — publishes it, restarts on it — its
// own snapshot, which its engine already holds — converges with the group,
// serves writes after it, and a retry of a request the snapshot covers is a
// duplicate of its original index.
func TestRealFollowerInstallsASnapshotAboveTheS3Limit(t *testing.T) {
	c, w, lag, clientID, rIndex := lagForLargeSnapshot(t)
	// The follower serves /metrics: its live heap is sampled while it installs,
	// and its peak resident set read once it has (getrusage's maximum over the
	// process, which starts behind the snapshot).
	metricsAt := freeTCPAddr(t)
	c.startWith(lag, "-metrics-listen", metricsAt)
	out := c.procs[lag].out
	heap := sampleHeap(t, metricsAt, func() string {
		if strings.Contains(out.String(), "event=raft_snapshot_received node="+lag) {
			return "install"
		}
		return "receive"
	})
	start := time.Now()
	from, index := waitInstalled(t, c, lag)
	installedIn := time.Since(start)
	mostHeap := heap.stop()
	ss, err := scrapeHTTP(t, metricsAt)
	if err != nil {
		t.Fatalf("%s's metrics: %v", lag, err)
	}
	peakRSS, _ := ss.Get("process_max_rss_bytes")
	size := sentBytes(t, c, from, lag, index)
	if size <= s3InstallLimit || c.snapSize(lag) <= s3InstallLimit {
		t.Fatalf("premise: the snapshot %s installed is %d bytes (sent %d), not above the S3 limit %d",
			lag, c.snapSize(lag), size, s3InstallLimit)
	}
	if m, _, ok := c.published(lag); !ok || m.Index != index {
		t.Fatalf("%s installed %d but publishes %+v", lag, index, m)
	}
	sent, failed := c.transfersTo(lag)
	t.Logf("%s installed snapshot %d (%d MiB) from %s, %.1fs after it started; its heap at most %d MiB receiving and proving it, %d MiB installing it (sampled); its peak resident set %d MiB; transfers to it by then: %d sent, %d failed",
		lag, index, size>>20, from, installedIn.Seconds(), int64(mostHeap["receive"])>>20, int64(mostHeap["install"])>>20, int64(peakRSS)>>20, sent, failed)
	if mostHeap["receive"] == 0 || mostHeap["install"] == 0 || peakRSS == 0 {
		t.Fatalf("premise: %s's metrics were not sampled in both phases (%v) or report no resident set (%v)", lag, mostHeap, peakRSS)
	}
	// On the LSM machine nothing holds the state: it streams through the
	// receiver, the proofs and the ingest a record at a time, and a leader's
	// repeated offer, sent while the follower is still busy, is dropped as it
	// arrives. Half the snapshot is far above what that costs (8–18 MiB of
	// heap for ~85 MiB, measured under CPU contention) and far below one copy
	// of the state — which a repeated offer queued whole used to add. The
	// in-memory machine holds the state by design; its figures are logged.
	if stateMachineKind() == "lsm" {
		for _, phase := range []string{"receive", "install"} {
			if most := int64(mostHeap[phase]); most >= size/2 {
				t.Fatalf("%s's heap reached %d MiB in the %s phase of a %d MiB snapshot: half a copy of the state or more", lag, most>>20, phase, size>>20)
			}
		}
	}

	// The restart: the node's own snapshot, which its engine already holds.
	c.kill(lag)
	c.start(lag)
	waitForLine(t, c.procs[lag], fmt.Sprintf("event=raft_snapshot_restored node=%s index=", lag), 60*time.Second)
	w.putN("after", 5)
	c.waitCommit([]string{lag}, w.last, 60*time.Second)

	retry := c.do(kv.Request{Op: kv.ReqPut, ClientID: clientID, RequestID: 1, AckedBelow: 1, Key: []byte("k"), Value: []byte("v")})
	if retry.Status != kv.StatusOK || !retry.Duplicate || retry.Index != rIndex {
		t.Fatalf("the retry of R after the install: %+v, want OK duplicate of index %d", retry, rIndex)
	}
	c.finishLarge(w)
}

// TestRealFollowerDiesBetweenALargeInstallsBoundaryAndItsRestore: the
// follower is killed (its own crash point) after publishing the large snapshot
// and recording the log's boundary — the Raft-level install is durable — but
// before its state machine's restore, the ingest, has run: on disk the new
// snapshot over the old engine. Its restart restores the published snapshot —
// for the LSM machine, ingests it — and it converges.
func TestRealFollowerDiesBetweenALargeInstallsBoundaryAndItsRestore(t *testing.T) {
	c, w, lag, _, _ := lagForLargeSnapshot(t)
	c.startWith(lag, "-crash-at", "after-install-boundary:1")
	if point, _ := c.waitKilledAtPoint(lag, 120*time.Second); point != "after-install-boundary" {
		t.Fatalf("died at %s", point)
	}
	m, _, ok := c.published(lag)
	if !ok || c.snapSize(lag) <= s3InstallLimit {
		t.Fatalf("premise: a published snapshot above the S3 limit on disk (found %v, %d bytes)", ok, c.snapSize(lag))
	}
	if b := c.liveLog(lag).Boundary; b.Index != m.Index {
		t.Fatalf("premise: the log's boundary %d is the snapshot's %d", b.Index, m.Index)
	}
	c.start(lag)
	waitForLine(t, c.procs[lag], fmt.Sprintf("event=raft_snapshot_restored node=%s index=%d", lag, m.Index), 120*time.Second)
	w.putN("after", 5)
	c.waitCommit([]string{lag}, w.last, 60*time.Second)
	c.finishLarge(w)
}

// TestRealFollowerKilledDuringALargeInstallRecovers: the follower is SIGKILLed
// the moment it has received and validated a large snapshot — while it
// publishes it, records the boundary, or ingests it, wherever the kill lands.
// Whatever that leaves on disk — the old state with a staged file, the new
// snapshot over the old engine, staged tables, or the new engine — its
// restart recovers, and it converges.
func TestRealFollowerKilledDuringALargeInstallRecovers(t *testing.T) {
	c, w, lag, _, _ := lagForLargeSnapshot(t)
	c.start(lag)
	waitForLine(t, c.procs[lag], "event=raft_snapshot_received node="+lag, 120*time.Second)
	c.kill(lag)
	before, _, had := c.published(lag)
	installed := false
	for _, p := range c.history {
		if p.id == lag && strings.Contains(p.out.String(), "event=raft_snapshot_installed node="+lag) {
			installed = true
		}
	}
	t.Logf("killed: published snapshot %d (found %v), install completed before the kill: %v", before.Index, had, installed)
	c.start(lag)
	waitInstalledOrRestored(t, c, lag)
	w.putN("after", 5)
	c.waitCommit([]string{lag}, w.last, 60*time.Second)
	c.finishLarge(w)
}

// TestRealFollowerKilledMidTransferOfALargeSnapshotRecovers: the follower is
// SIGKILLed while a snapshot above the S3 limit is still streaming into it —
// part of it in its .snap.recv, none of it proven. The partial transfer never
// becomes state: nothing new is published, and the engine is still the
// follower's own old state; its restart discards the partial file, a leader
// streams the snapshot again from its start, and the follower installs it and
// converges.
func TestRealFollowerKilledMidTransferOfALargeSnapshotRecovers(t *testing.T) {
	withPremise(t, func() {
		c, w, lag, _, _ := lagForLargeSnapshot(t)
		behind := c.liveLog(lag).LastIndex()
		before, _, had := c.published(lag)
		c.start(lag)
		out, recv := c.procs[lag].out, c.snapFiles(lag).RecvPath()
		var partial int64
		for deadline := time.Now().Add(120 * time.Second); partial == 0 && time.Now().Before(deadline); {
			if info, err := os.Stat(recv); err == nil && info.Size() >= 4<<20 {
				partial = info.Size()
			} else {
				time.Sleep(2 * time.Millisecond)
			}
		}
		c.kill(lag)
		if partial == 0 {
			t.Fatalf("%s never received 4 MiB of a snapshot within 120s\n%s", lag, c.outputs())
		}
		if strings.Contains(out.String(), "event=raft_snapshot_received node="+lag) {
			c.killAll()
			panic(premiseFailed{"the transfer was proven before the kill landed"})
		}
		info, err := os.Stat(recv)
		if err != nil {
			t.Fatalf("premise: the partial transfer is on disk: %v", err)
		}
		if after, _, has := c.published(lag); has != had || after.Index != before.Index {
			t.Fatalf("a partial transfer changed what %s publishes: %+v (had %v) -> %+v (has %v)", lag, before, had, after, has)
		}
		if dir := filepath.Join(c.dirs[lag], "lsm"); stateMachineKind() == "lsm" {
			m, err := kv.OpenLSMMachine(dir, kv.DefaultLimits, storage.DefaultOptions())
			if err != nil {
				t.Fatalf("%s's engine does not open after a kill mid-transfer: %v", lag, err)
			}
			applied, durable := m.Applied(), m.DurableAppliedIndex()
			_ = m.Close()
			if applied > behind || durable > behind {
				t.Fatalf("%s's engine is at %d (durable %d) after a partial transfer; it went down at %d", lag, applied, durable, behind)
			}
		}
		t.Logf("%s killed with %d MiB of a snapshot unproven in its %s: nothing published, its state still its own at ≤ %d",
			lag, info.Size()>>20, filepath.Base(recv), behind)
		c.start(lag)
		from, index := waitInstalled(t, c, lag)
		size := sentBytes(t, c, from, lag, index)
		if size <= s3InstallLimit {
			t.Fatalf("premise: the snapshot sent again is %d bytes, not above the S3 limit", size)
		}
		t.Logf("restarted, %s installed snapshot %d (%d MiB) from %s", lag, index, size>>20, from)
		w.putN("after", 5)
		c.waitCommit([]string{lag}, w.last, 60*time.Second)
		c.finishLarge(w)
	})
}

// transfersTo counts the snapshot transfers to id that the other running
// nodes have finished — streamed whole, or failed — from their own logs.
func (c *rcluster) transfersTo(id string) (sent, failed int) {
	for _, other := range others(c.ids, id) {
		if p := c.procs[other]; p != nil {
			out := p.out.String()
			sent += strings.Count(out, "event=raft_snapshot_sent node="+other+" to="+id+" ")
			failed += strings.Count(out, "event=raft_snapshot_send_failed node="+other+" to="+id+" ")
		}
	}
	return sent, failed
}

// heapSampler polls a node's /metrics while it runs and keeps, for each phase
// the phase function names at the sample, the most heap it reported
// (go_heap_objects_bytes: live objects and dead ones not yet swept): a sampled
// maximum, not the true peak, and unlike the resident set it excludes the
// race detector's shadow memory, so it measures what the code holds.
type heapSampler struct {
	stopc chan struct{}
	done  chan map[string]float64
}

func sampleHeap(t *testing.T, addr string, phase func() string) *heapSampler {
	h := &heapSampler{stopc: make(chan struct{}), done: make(chan map[string]float64, 1)}
	go func() {
		most := map[string]float64{}
		for {
			p := phase()
			if ss, err := scrapeHTTP(t, addr); err == nil {
				if v, ok := ss.Get("go_heap_objects_bytes"); ok && v > most[p] {
					most[p] = v
				}
			}
			select {
			case <-h.stopc:
				h.done <- most
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	return h
}

// stop ends the sampling and returns the most heap seen in each phase.
func (h *heapSampler) stop() map[string]float64 {
	close(h.stopc)
	return <-h.done
}

// waitInstalledOrRestored waits until a restarted node holds a large snapshot:
// restored from its own published one, or installed again from a leader.
func waitInstalledOrRestored(t *testing.T, c *rcluster, id string) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		out := c.procs[id].out.String()
		if strings.Contains(out, "event=raft_snapshot_installed node="+id) ||
			(strings.Contains(out, "event=raft_snapshot_restored node="+id) && c.snapSize(id) > s3InstallLimit) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s neither restored nor installed a large snapshot after its restart\n%s", id, c.outputs())
}
