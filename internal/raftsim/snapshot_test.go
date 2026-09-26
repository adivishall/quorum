package raftsim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
)

// Phase 14 in the deterministic simulator: snapshots created, published,
// compacted behind, sent, received, installed and restored through the
// driver's own functions, under the exact faults each scenario names — with
// every continuous invariant running, the snapshot ones included (INV-SN1:
// every snapshot is the reference model's state at its index, and every
// snapshot at one index is the same bytes; INV-SN2: a restore is always of the
// published, valid snapshot; INV-SN3: no durable log is compacted past its
// durable snapshot).

// snapCfg snapshots every 4 entries, keeps 1 entry behind each, and sends
// snapshots in 64-byte chunks.
func snapCfg(seed int64) Config {
	return Config{Nodes: 3, Seed: seed, SnapshotEvery: 4, SnapshotRetain: 1, ChunkSize: 64}
}

func newSnapSim(t *testing.T, cfg Config) *kvSim { return &kvSim{newSimWith(t, cfg)} }

// settle heartbeats the leader until every up node has committed and
// applied the leader's last index (installing a snapshot where entries are
// gone), within a bound that covers the core's snapshot retry.
func (s *kvSim) settle(leader NodeID) {
	s.t.Helper()
	for i := 0; i < 400; i++ {
		last := s.State(leader).LastIndex
		done := true
		for _, id := range s.IDs() {
			if st := s.State(id); st.Up && (st.Commit < last || st.Applied < last) {
				done = false
			}
		}
		if done {
			return
		}
		s.heartbeat(leader)
	}
	var b strings.Builder
	for _, id := range s.IDs() {
		fmt.Fprintf(&b, "%s: %+v\n", id, s.State(id))
	}
	s.t.Fatalf("the cluster did not settle on %s's log\n%s%s", leader, b.String(), strings.Join(s.Trace().Tail(40), "\n"))
}

// lagBehindCompaction isolates n3 and commits enough on n1 and n2 that both
// compact their logs past everything n3 holds.
func (s *kvSim) lagBehindCompaction(leader NodeID, n int) {
	s.t.Helper()
	s.do(Event{Kind: Isolate, Node: "n3"})
	for i := 0; i < n; i++ {
		s.commit(leader, fmt.Sprintf("lag%d-%d", s.Step(), i), leader, "n2")
	}
	if b := s.State(leader).Boundary; b <= s.State("n3").LastIndex {
		s.t.Fatalf("premise: the leader's boundary %d does not pass n3's last index %d", b, s.State("n3").LastIndex)
	}
}

// chunksTo returns the in-flight chunk flights to a node, oldest first, with
// their link positions.
func (s *kvSim) chunksTo(to NodeID) []chunkAt {
	var out []chunkAt
	pos := map[[2]NodeID]int{}
	for _, f := range s.flights {
		k := [2]NodeID{f.msg.From, f.msg.To}
		if f.chunk != nil && f.msg.To == to {
			out = append(out, chunkAt{f: f, pos: pos[k]})
		}
		pos[k]++
	}
	return out
}

type chunkAt struct {
	f   *flight
	pos int
}

// offerTo ticks the leader, delivering everything except what is addressed
// to target, until a snapshot transfer to target is in flight.
func (s *kvSim) offerTo(leader, target NodeID) []chunkAt {
	s.t.Helper()
	for i := 0; i < 400; i++ {
		if cs := s.chunksTo(target); len(cs) > 0 {
			return cs
		}
		s.tick(leader, 1)
		s.deliverWhere(func(m raft.Message) bool { return m.To != target })
	}
	s.t.Fatalf("%s never offered %s a snapshot", leader, target)
	return nil
}

func requireSameStore(t *testing.T, s *kvSim, a, b NodeID) {
	t.Helper()
	sa, sb := s.Store(a), s.Store(b)
	if fmt.Sprint(sa.Snapshot()) != fmt.Sprint(sb.Snapshot()) || fmt.Sprint(sa.Sessions()) != fmt.Sprint(sb.Sessions()) {
		t.Fatalf("%s and %s hold different states:\n%v %v\n%v %v", a, b, sa.Snapshot(), sa.Sessions(), sb.Snapshot(), sb.Sessions())
	}
}

// TestSimLaggingFollowerInstallsASnapshot: a follower cut off while the others
// commit and compact past everything it holds catches up by installing the
// leader's snapshot — sent as chunks, reassembled, validated, published,
// recorded in its log and restored — then continues by entries, and holds
// exactly the leader's state, sessions included (INV-SN6).
func TestSimLaggingFollowerInstallsASnapshot(t *testing.T) {
	s := newSnapSim(t, snapCfg(1))
	s.electLeader("n1")
	s.register("n1", "c1")
	for i := 0; i < 3; i++ {
		s.put("n1", "c1", "k0", fmt.Sprintf("a%d", i))
		s.DeliverAll()
		s.heartbeat("n1")
	}
	s.lagBehindCompaction("n1", 10)
	installs := s.Stats().Installs
	s.do(Event{Kind: HealAll})
	s.settle("n1")
	if s.Stats().Installs == installs {
		t.Fatal("n3 caught up without installing a snapshot")
	}
	if st := s.State("n3"); st.Snapshot == 0 || st.Boundary == 0 {
		t.Fatalf("n3 after the install: %+v", st)
	}
	s.commit("n1", "after", "n1", "n2", "n3")
	requireSameStore(t, s, "n1", "n3")
	s.do(Event{Kind: CheckConverged})
}

// TestSimCorruptedChunkIsRefusedAndRetried: a snapshot transfer with one
// corrupted byte completes but does not validate — refused, nothing
// published, nothing installed — and the leader's next offer succeeds.
func TestSimCorruptedChunkIsRefusedAndRetried(t *testing.T) {
	s := newSnapSim(t, snapCfg(2))
	s.electLeader("n1")
	s.lagBehindCompaction("n1", 10)
	s.do(Event{Kind: HealAll})
	cs := s.offerTo("n1", "n3")
	last := cs[len(cs)-1]
	s.do(Event{Kind: CorruptChunk, From: "n1", To: "n3", Pos: last.pos, N: len(last.f.chunk) - 1})
	refused, installs := s.Stats().ChunksRefused, s.Stats().Installs
	s.deliverWhere(link("n1", "n3"))
	if s.Stats().ChunksRefused == refused || s.Stats().Installs != installs {
		t.Fatalf("the corrupted transfer: refused %d -> %d, installs %d -> %d", refused, s.Stats().ChunksRefused, installs, s.Stats().Installs)
	}
	if st := s.State("n3"); st.Snapshot != 0 {
		t.Fatalf("n3 published a snapshot from a corrupted transfer: %+v", st)
	}
	s.settle("n1")
	if s.Stats().Installs == installs {
		t.Fatal("the leader's retry was never installed")
	}
	s.do(Event{Kind: CheckConverged})
}

// TestSimDuplicatedAndReorderedTransfers: a transfer delivered twice installs
// once (the second completion is covered by the commit index: ignored); a
// transfer delivered newest-chunk-first installs nothing (chunks are accepted
// in order only); the next in-order offer installs.
func TestSimDuplicatedAndReorderedTransfers(t *testing.T) {
	s := newSnapSim(t, snapCfg(3))
	s.electLeader("n1")
	s.lagBehindCompaction("n1", 10)
	s.do(Event{Kind: HealAll})
	cs := s.offerTo("n1", "n3")
	if len(cs) < 2 {
		t.Fatalf("premise: a transfer of %d chunks", len(cs))
	}
	// Newest first: nothing completes.
	installs := s.Stats().Installs
	for i := len(cs) - 1; i >= 0; i-- {
		s.Apply(Event{Kind: Deliver, From: "n1", To: "n3", Pos: cs[i].pos})
	}
	if s.Stats().Installs != installs {
		t.Fatal("a transfer delivered newest chunk first was installed")
	}
	// The next offer, duplicated whole: one install.
	cs = s.offerTo("n1", "n3")
	for _, c := range cs {
		s.Apply(Event{Kind: Duplicate, From: "n1", To: "n3", Pos: c.pos})
	}
	s.deliverWhere(link("n1", "n3"))
	if got := s.Stats().Installs - installs; got != 1 {
		t.Fatalf("a duplicated transfer installed %d times, want once", got)
	}
	s.settle("n1")
	s.do(Event{Kind: CheckConverged})
}

// TestSimStaleSnapshotAfterANewerInstallIsIgnored: a transfer delayed until
// after the follower installed a later offer and moved on by entries is,
// when it finally completes, covered by the commit index — the core ignores
// it, nothing is published or restored, the follower's state is unchanged.
func TestSimStaleSnapshotAfterANewerInstallIsIgnored(t *testing.T) {
	s := newSnapSim(t, snapCfg(4))
	s.electLeader("n1")
	s.lagBehindCompaction("n1", 10)
	s.do(Event{Kind: HealAll})
	for _, c := range s.offerTo("n1", "n3") {
		s.Apply(Event{Kind: Delay, From: "n1", To: "n3", Pos: c.pos, N: 100000})
	}
	s.settle("n1") // a later offer is installed
	for i := 0; i < 6; i++ {
		s.commit("n1", fmt.Sprintf("more%d", i), "n1", "n2", "n3")
	}
	before, installs, snap := s.State("n3"), s.Stats().Installs, s.State("n3").Snapshot
	s.do(Event{Kind: Release})
	s.deliverWhere(link("n1", "n3"))
	after := s.State("n3")
	if s.Stats().Installs != installs || after.Snapshot != snap || after.Commit != before.Commit || after.LastIndex != before.LastIndex {
		t.Fatalf("a stale snapshot changed n3: %+v -> %+v (installs %d -> %d)", before, after, installs, s.Stats().Installs)
	}
	s.settle("n1")
	s.do(Event{Kind: CheckConverged})
}

// TestSimRestartFromSnapshotAfterPowerLoss: every node loses power at once;
// each recovers from its snapshot and the log suffix after it — the restored
// state checked against the reference model at the snapshot's index — and the
// cluster converges to states equal to the model over the whole committed log.
func TestSimRestartFromSnapshotAfterPowerLoss(t *testing.T) {
	s := newSnapSim(t, snapCfg(5))
	s.electLeader("n1")
	s.register("n1", "c1")
	for i := 0; i < 6; i++ {
		s.put("n1", "c1", fmt.Sprintf("k%d", i%2), fmt.Sprintf("v%d", i))
		s.DeliverAll()
		s.heartbeat("n1")
	}
	s.settle("n1")
	restores := s.Stats().Restores
	for i, id := range s.IDs() {
		s.do(Event{Kind: Crash, Node: id, Power: true, N: 7 * i})
	}
	for _, id := range s.IDs() {
		s.restart(id)
	}
	if got := s.Stats().Restores - restores; got != 3 {
		t.Fatalf("%d nodes restored a snapshot at restart, want all 3", got)
	}
	s.electLeader("n2")
	s.put("n2", "c1", "k0", "after")
	s.DeliverAll()
	s.settle("n2")
	s.checkStores()
	s.do(Event{Kind: CheckConverged})
	s.requireLinearizable()
}

// TestSimCorruptPublishedSnapshotRefusesToStart: a node whose published
// snapshot is damaged on disk does not start — loud and total, never a
// silent fallback to an empty or older state — and the rest of the group
// carries on without it.
func TestSimCorruptPublishedSnapshotRefusesToStart(t *testing.T) {
	s := newSnapSim(t, snapCfg(6))
	s.electLeader("n1")
	for i := 0; i < 8; i++ {
		s.commit("n1", fmt.Sprintf("x%d", i), "n1", "n2", "n3")
	}
	if s.State("n2").Snapshot == 0 {
		t.Fatal("premise: n2 has a snapshot")
	}
	s.do(Event{Kind: Crash, Node: "n2"}, Event{Kind: CorruptSnapshot, Node: "n2", N: 40})
	failures := s.Stats().RestartFailures
	s.Apply(Event{Kind: Restart, Node: "n2"})
	if s.Up("n2") || s.Stats().RestartFailures != failures+1 {
		t.Fatal("n2 started from a corrupt snapshot")
	}
	tail := strings.Join(s.Trace().Tail(5), "\n")
	if !strings.Contains(tail, "restart-failed n2") || !strings.Contains(tail, "snapshot: corrupt") {
		t.Fatalf("the refusal does not name the corruption:\n%s", tail)
	}
	s.commit("n1", "without-n2", "n1", "n3")
}

// TestSimDedupSurvivesSnapshotCompactionAndRestart is the Phase 14 statement
// of Phase 13's guarantee: a session's request R executes and is applied, its
// client never hears (the leader dies after applying it); every node then
// snapshots and compacts the entry away, every node restarts from its
// snapshot, and the client's retry of R — same identity — is answered as a
// duplicate of R's original index, from the session table the snapshots
// carried. R executed once; the history of logical requests is linearizable.
func TestSimDedupSurvivesSnapshotCompactionAndRestart(t *testing.T) {
	s := newSnapSim(t, Config{Nodes: 3, Seed: 7, SnapshotEvery: 3, SnapshotRetain: 0, ChunkSize: 64})
	s.electLeader("n1")
	s.register("n1", "c1")
	s.heartbeat("n1")
	s.do(Event{Kind: CrashAt, Node: "n1", Point: "after-applied-to", Nth: 1})
	s.put("n1", "c1", "k", "A")
	s.DeliverAll()
	s.heartbeat("n1")
	if s.Up("n1") || !s.Busy("c1") {
		t.Fatal("premise: n1 died after applying the write, and c1 has no answer")
	}
	var rIndex uint64
	for _, e := range s.State("n2").Log {
		if strings.Contains(string(e.Data), "A") {
			rIndex = e.Index
		}
	}
	if rIndex == 0 {
		t.Fatal("premise: n2 holds R's entry")
	}
	s.do(Event{Kind: KVTimeout, Client: "c1"})
	s.electLeader("n2")
	for i := 0; i < 4; i++ {
		s.commit("n2", fmt.Sprintf("y%d", i), "n2", "n3")
	}
	s.do(Event{Kind: SnapshotNow, Node: "n2"}, Event{Kind: SnapshotNow, Node: "n3"})
	s.restart("n1")
	s.settle("n2")
	for _, id := range s.IDs() {
		if st := s.State(id); st.Boundary < rIndex {
			t.Fatalf("premise: %s's log still holds R's entry %d (boundary %d)", id, rIndex, st.Boundary)
		}
	}
	for _, id := range s.IDs() {
		s.do(Event{Kind: Crash, Node: id, Power: true})
	}
	restores := s.Stats().Restores
	for _, id := range s.IDs() {
		s.restart(id)
	}
	if s.Stats().Restores-restores != 3 {
		t.Fatal("premise: every node restarted from a snapshot")
	}
	s.electLeader("n3")
	s.heartbeat("n3")
	s.Apply(Event{Kind: KVRetry, Node: "n3", Client: "c1"})
	s.DeliverAll()
	s.settle("n3")
	op := s.last("c1")
	want := fmt.Sprintf("duplicate of index %d", rIndex)
	if op.Outcome != lincheck.OK || op.Attempts[len(op.Attempts)-1].Result != want {
		t.Fatalf("the retry after snapshots, compaction and restarts: %s %+v, want %q", op, op.Attempts, want)
	}
	s.get("n3", "c2", "k")
	s.DeliverAll()
	s.settle("n3")
	if op := s.last("c2"); op.Outcome != lincheck.OK || string(op.Output) != "A" {
		t.Fatalf("read after the retry: %s", op)
	}
	s.requireLinearizable()
	s.checkStores()
}

// TestSimSuccessiveSnapshotsWithCrashesBetween: snapshots at about 100, 200
// and 300 applied entries, with a different crash between each — a
// follower's power loss, the leader's process crash, every node's power loss
// — and every recovery starts from the latest snapshot the node published.
func TestSimSuccessiveSnapshotsWithCrashesBetween(t *testing.T) {
	s := newSnapSim(t, Config{Nodes: 3, Seed: 8, SnapshotEvery: 100, SnapshotRetain: 0, ChunkSize: 512})
	leader := NodeID("n1")
	s.electLeader(leader)
	for round := 1; round <= 3; round++ {
		for s.State(leader).Applied < uint64(100*round)+2 {
			s.commit(leader, fmt.Sprintf("r%d-%d", round, s.Step()), s.IDs()...)
		}
		s.settle(leader)
		for _, id := range s.IDs() {
			if st := s.State(id); st.Snapshot < uint64(100*round) || st.Snapshot > uint64(100*round)+3 {
				t.Fatalf("round %d: %s's snapshot is at %d", round, id, st.Snapshot)
			}
		}
		switch round {
		case 1:
			s.do(Event{Kind: Crash, Node: "n2", Power: true, N: 5})
			s.restart("n2")
		case 2:
			s.do(Event{Kind: Crash, Node: leader})
			s.electLeader("n2")
			s.restart(leader)
			leader = "n2"
		case 3:
			for _, id := range s.IDs() {
				s.do(Event{Kind: Crash, Node: id, Power: true})
			}
			for _, id := range s.IDs() {
				s.restart(id)
			}
			s.electLeader("n3")
			leader = "n3"
		}
		s.settle(leader)
		for _, id := range s.IDs() {
			if st := s.State(id); st.Snapshot < uint64(100*round) || st.Applied < st.Snapshot {
				t.Fatalf("round %d after the crash: %s %+v", round, id, st)
			}
		}
	}
	s.commit(leader, "end", s.IDs()...)
	s.do(Event{Kind: CheckConverged})
	s.checkStores()
}

// snapshotScenario is the crash matrix's snapshot scenario: every node
// snapshots and compacts; a session writes; n3 falls behind the compaction
// and catches up by an install; n3 fail-stops on a torn write and restarts
// from its snapshot (recovery's torn-tail truncate); n2 restarts from its
// snapshot; an explicit snapshot on the leader. It reaches every driver crash
// point and every I/O boundary, renames and directory fsyncs included.
func snapshotScenario(t *testing.T, cfg Config) []Event {
	t.Helper()
	s := newSnapSim(t, cfg)
	s.electLeader("n1")
	s.register("n1", "c1")
	for i := 0; i < 3; i++ {
		s.put("n1", "c1", "k0", fmt.Sprintf("a%d", i))
		s.DeliverAll()
		s.heartbeat("n1")
	}
	s.lagBehindCompaction("n1", 8)
	s.do(Event{Kind: HealAll})
	s.settle("n1")
	s.do(Event{Kind: FailPersist, Node: "n3", Op: ShortWrite, N: 6})
	s.Apply(Event{Kind: Propose, Node: "n1", Data: "torn"})
	for i := 0; i < 4 && s.Up("n3"); i++ {
		s.heartbeat("n1")
	}
	s.requireDown("n3", "the torn write did not fail-stop n3")
	s.restart("n3")
	s.do(Event{Kind: Crash, Node: "n2"})
	s.restart("n2")
	s.settle("n1")
	s.do(Event{Kind: SnapshotNow, Node: "n1"})
	s.commit("n1", "end", "n1", "n2", "n3")
	return s.Script()
}

// TestSnapshotCrashMatrix is the bounded exhaustive crash matrix of Phase 14:
// a crash at every point the snapshot scenario reaches — every driver point,
// including snapshot creation, publication, compaction and installation, and
// every write, fsync, truncate, rename and directory fsync of every file — in
// every crash mode, each followed by an immediate restart, the rest of the
// scenario and convergence; zero failures, and every point covered.
func TestSnapshotCrashMatrix(t *testing.T) {
	cfg := snapCfg(61)
	scenario := snapshotScenario(t, cfg)
	rep, err := RunCrashMatrix(cfg, scenario, DefaultCrashModes)
	if err != nil {
		t.Fatal(err)
	}
	byPoint, byMode := rep.Coverage()
	for _, p := range raftnode.Points {
		if byPoint[p.String()] == 0 {
			t.Errorf("the snapshot scenario never reached driver point %s", p)
		}
	}
	for _, p := range append(append([]string(nil), IOPoints...), SnapshotIOPoints...) {
		if byPoint[p] == 0 {
			t.Errorf("the snapshot scenario never reached I/O point %s", p)
		}
	}
	if len(byMode) != len(DefaultCrashModes) {
		t.Errorf("modes covered: %v", byMode)
	}
	t.Log(rep.Summary())
	if fails := rep.Failures(); len(fails) > 0 {
		shown := fails
		if len(shown) > 12 {
			shown = shown[:12]
		}
		t.Fatalf("%d of %d crashes were not survived (seed=%d; reproduce a row by arming its event first):\n%s",
			len(fails), len(rep.Rows), cfg.Seed, (&MatrixReport{Rows: shown}).Text())
	}
}
