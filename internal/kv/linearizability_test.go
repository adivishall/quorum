package kv_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/kv/workload"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/transport"
)

func run(t *testing.T, c *cluster, opts workload.Options) (*lincheck.Recorder, workload.Stats) {
	t.Helper()
	rec := lincheck.NewRecorder()
	ctx, cancel := context.WithTimeout(c.ctx, 120*time.Second)
	defer cancel()
	st := workload.Run(ctx, c.endpoints(), opts, rec)
	return rec, st
}

// TestSequentialBaselineIsLinearizableAndMatchesTheModel is scenario A on the real
// driver: one client, PUT/GET/PUT/GET/DELETE/GET, checked by the linearizability
// checker AND diffed against the sequential reference model.
func TestSequentialBaselineIsLinearizableAndMatchesTheModel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, false)
	l := c.waitLeader(0, 10*time.Second)
	srv, _ := c.eps[l].current()
	rec := lincheck.NewRecorder()
	do := func(kind lincheck.Kind, key, val string) {
		id := rec.Begin("c1", kind, key, []byte(val))
		a := rec.Attempt(id, string(l))
		octx, ocancel := context.WithTimeout(ctx, 5*time.Second)
		defer ocancel()
		switch kind {
		case lincheck.Put:
			m, err := srv.Put(octx, []byte(key), []byte(val))
			if err != nil {
				t.Fatalf("put: %v", err)
			}
			rec.AttemptDone(id, a, true, "ok", m.Term)
			rec.End(id, lincheck.OK, nil, string(l), m.Term, m.Index)
		case lincheck.Delete:
			m, err := srv.Delete(octx, []byte(key))
			if err != nil {
				t.Fatalf("delete: %v", err)
			}
			rec.AttemptDone(id, a, true, "ok", m.Term)
			rec.End(id, lincheck.OK, nil, string(l), m.Term, m.Index)
		case lincheck.Get:
			v, m, err := srv.Get(octx, []byte(key))
			switch {
			case err == nil:
				rec.AttemptDone(id, a, true, "ok", m.Term)
				rec.End(id, lincheck.OK, v, string(l), m.Term, m.Index)
			case err == kv.ErrNotFound:
				rec.AttemptDone(id, a, true, "notfound", m.Term)
				rec.End(id, lincheck.NotFound, nil, string(l), m.Term, m.Index)
			default:
				t.Fatalf("get: %v", err)
			}
		}
	}
	do(lincheck.Put, "k", "A")
	do(lincheck.Get, "k", "")
	do(lincheck.Put, "k", "B")
	do(lincheck.Get, "k", "")
	do(lincheck.Delete, "k", "")
	do(lincheck.Get, "k", "")
	h := rec.History()
	if r := lincheck.Check(h, lincheck.Options{}); !r.OK {
		t.Fatalf("not linearizable: %s\n%s", r.Reason, h)
	}
	if _, bad := lincheck.Sequential(h.Ops); bad != -1 {
		t.Fatalf("the sequential reference model contradicts op %d:\n%s", h.Ops[bad].ID, h)
	}
	want := []lincheck.Outcome{lincheck.OK, lincheck.OK, lincheck.OK, lincheck.OK, lincheck.OK, lincheck.NotFound}
	for i, op := range h.Ops {
		if op.Outcome != want[i] {
			t.Fatalf("op %d outcome %s, want %s", op.ID, op.Outcome, want[i])
		}
	}
	if string(h.Ops[1].Output) != "A" || string(h.Ops[3].Output) != "B" {
		t.Fatalf("reads returned %q and %q, want A and B", h.Ops[1].Output, h.Ops[3].Output)
	}
}

// TestConcurrentClientsAreLinearizable: 1, 2, 4 and 8 clients over independent
// and shared keys, no faults. Scenarios B, C, D, E and G at once, judged by the
// checker rather than by a hand-written expectation.
func TestConcurrentClientsAreLinearizable(t *testing.T) {
	for _, clients := range []int{1, 2, 4, 8} {
		t.Run(itoa(clients)+"-clients", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := startCluster(t, ctx, 3, false)
			c.waitLeader(0, 10*time.Second)
			rec, st := run(t, c, workload.Options{Clients: clients, OpsPerClient: 40, Keys: 3, Timeout: 5 * time.Second, Seed: int64(clients), GetPct: 40, DeletePct: 10})
			check(t, rec, st)
		})
	}
}

// TestHotKeyConcurrentWritesAndReads: scenario C/F under pressure — eight clients
// on one key, writes and reads interleaved. Every read must be explained by one
// order of the concurrent writes, and a read after a completed write must see it.
func TestHotKeyConcurrentWritesAndReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, false)
	c.waitLeader(0, 10*time.Second)
	rec, st := run(t, c, workload.Options{Clients: 8, OpsPerClient: 40, Keys: 1, Timeout: 5 * time.Second, Seed: 99, GetPct: 50, DeletePct: 10})
	r := check(t, rec, st)
	if r.Stats.Complete < 200 {
		t.Fatalf("too few completed ops to mean anything: %d", r.Stats.Complete)
	}
}

// untilDone runs schedule in the background and returns a workload Stop
// predicate that ends the clients once the schedule has finished — so the
// faults intersect the workload by construction, not by luck of timing.
func untilDone(schedule func()) func() bool {
	var done atomic.Bool
	go func() {
		schedule()
		done.Store(true)
	}()
	return done.Load
}

// TestLinearizableUnderMessageFaults: scenario I(1–3) — delay (hold and release
// in reverse), drop and duplication of Raft traffic while eight clients run.
func TestLinearizableUnderMessageFaults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, true)
	c.waitLeader(0, 10*time.Second)
	c.net.AddRule(fault.Rule{Action: fault.Duplicate, Copies: 1})
	stop := untilDone(func() {
		for i := 0; i < 6; i++ {
			hold := c.net.AddRule(fault.Rule{Kinds: []transport.MsgKind{transport.MsgAppendEntries}, Action: fault.Hold})
			time.Sleep(40 * time.Millisecond)
			c.net.RemoveRule(hold)
			c.net.Release(true) // newest first: reordering
			drop := c.net.AddRule(fault.Rule{Action: fault.Drop, Count: 5})
			time.Sleep(20 * time.Millisecond)
			c.net.RemoveRule(drop)
		}
	})
	rec, st := run(t, c, workload.Options{Clients: 8, OpsPerClient: 1 << 20, Keys: 3, Timeout: 3 * time.Second, Seed: 5, GetPct: 40, DeletePct: 10, MaxAttempts: 6, Stop: stop})
	c.net.ClearRules()
	c.net.Release(false)
	check(t, rec, st)
	if s := c.net.Stats(); s.Duplicated == 0 || s.Dropped == 0 || s.Held == 0 || s.Released == 0 {
		t.Fatalf("faults did not engage: %+v", s)
	}
}

// TestLinearizableAcrossLeaderCrashAndRestart: scenario H/J — the leader is
// stopped mid-workload (its clients get unknown or unavailable), the others
// elect, it restarts on its log with a fresh state machine and rejoins. Every
// completed operation must still be explained.
func TestLinearizableAcrossLeaderCrashAndRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, false)
	l := c.waitLeader(0, 10*time.Second)
	stop := untilDone(func() {
		time.Sleep(150 * time.Millisecond)
		c.crash(l)
		time.Sleep(400 * time.Millisecond)
		c.startNode(l)
		time.Sleep(300 * time.Millisecond)
	})
	rec, st := run(t, c, workload.Options{Clients: 6, OpsPerClient: 1 << 20, Keys: 3, Timeout: 2 * time.Second, Seed: 11, GetPct: 40, DeletePct: 10, MaxAttempts: 6, Pace: 2 * time.Millisecond, Stop: stop})
	check(t, rec, st)
	if st.Unknown+st.Unavailable+st.Redirects == 0 {
		t.Fatalf("the crash did not intersect the workload: %s", st)
	}
	// The restarted node has rebuilt its store from the log: after the run,
	// once it has caught up, its store equals the leader's for every key.
	l2 := c.waitLeader(0, 10*time.Second)
	srvL, _ := c.eps[l2].current()
	srvR, _ := c.eps[l].current()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if srvR.Node().Status().Applied >= srvL.Node().Status().Commit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up: applied %d, leader commit %d", srvR.Node().Status().Applied, srvL.Node().Status().Commit)
		}
		time.Sleep(10 * time.Millisecond)
	}
	a, b := srvL.Store().Snapshot(), srvR.Store().Snapshot()
	if len(a) != len(b) {
		t.Fatalf("stores differ in size: %d vs %d", len(a), len(b))
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || string(v) != string(w) {
			t.Fatalf("stores differ at %q: %q vs %q", k, v, w)
		}
	}
}

// TestLinearizableAcrossPartitionAndHeal: scenario I(5) — the leader is cut off
// mid-workload; the majority elects and serves; the old leader can complete
// nothing (its clients see deadlines: unknown); the partition heals. "Eventual
// success after heal" is not the property — every completed op must linearize.
func TestLinearizableAcrossPartitionAndHeal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, true)
	l := c.waitLeader(0, 10*time.Second)
	stop := untilDone(func() {
		time.Sleep(150 * time.Millisecond)
		c.net.Isolate(string(l), c.members())
		time.Sleep(700 * time.Millisecond)
		c.net.HealAll()
		time.Sleep(300 * time.Millisecond)
	})
	rec, st := run(t, c, workload.Options{Clients: 6, OpsPerClient: 1 << 20, Keys: 3, Timeout: 1500 * time.Millisecond, Seed: 17, GetPct: 40, DeletePct: 10, MaxAttempts: 6, Pace: 2 * time.Millisecond, Stop: stop})
	check(t, rec, st)
	if st.Unknown+st.Redirects == 0 {
		t.Fatalf("the partition did not intersect the workload: %s", st)
	}
}

// TestFollowerLocalReadIsCaughtAsNonLinearizable is the bug hunt made concrete:
// a read served from a follower's local store, WITHOUT ReadIndex, while a write
// completed on the leader that the follower has not yet applied. The checker must
// reject the history — which is why Phase 12 never offers follower reads, and
// why a mutant that let a follower serve ReadIndex would be killed.
func TestFollowerLocalReadIsCaughtAsNonLinearizable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, true)
	l := c.waitLeader(0, 10*time.Second)
	var follower *endpoint
	for _, id := range c.ids {
		if id != l {
			follower = c.eps[id]
			break
		}
	}
	srvL, _ := c.eps[l].current()
	srvF, _ := follower.current()
	rec := lincheck.NewRecorder()
	// Establish a value everyone has.
	id := rec.Begin("c1", lincheck.Put, "k", []byte("old"))
	m, err := srvL.Put(ctx, []byte("k"), []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	rec.End(id, lincheck.OK, nil, string(l), m.Term, m.Index)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, ok := srvF.Store().Get([]byte("k")); ok && string(v) == "old" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower never applied the first write")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Cut the follower's inbound traffic so it cannot learn the next write.
	c.net.Block(string(l), follower.Name())
	id = rec.Begin("c1", lincheck.Put, "k", []byte("new"))
	if m, err = srvL.Put(ctx, []byte("k"), []byte("new")); err != nil {
		t.Fatalf("write with one follower blocked (the other forms a quorum): %v", err)
	}
	rec.End(id, lincheck.OK, nil, string(l), m.Term, m.Index)
	// A local read on the blocked follower — bypassing ReadIndex — after the
	// write completed.
	id = rec.Begin("c2", lincheck.Get, "k", nil)
	v, ok := srvF.Store().Get([]byte("k"))
	if !ok {
		t.Fatal("follower lost the key")
	}
	rec.End(id, lincheck.OK, v, follower.Name(), 0, 0)
	c.net.HealAll()
	h := rec.History()
	r := lincheck.Check(h, lincheck.Options{Minimize: true})
	if r.OK {
		t.Fatalf("a stale follower read was accepted as linearizable:\n%s", h)
	}
	t.Logf("caught, as it must be:\n%s", r.Reason)
	// And the same read through ReadIndex on the follower is refused, not served.
	if _, _, err := srvF.Get(ctx, []byte("k")); err == nil {
		t.Fatal("a follower served a linearizable read")
	}
}

func itoa(n int) string { return fmtInt(n) }

func fmtInt(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
