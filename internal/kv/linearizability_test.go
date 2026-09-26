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
	"github.com/adivishall/quorum/internal/raftnode"
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

// served counts the operations the cluster answered (OK or NotFound).
func served(rec *lincheck.Recorder) int {
	s := rec.History().Summary()
	return s.OK + s.NotFound
}

// waitServed is a synchronization point on client progress: it returns once n
// more operations than now have been served. Fault schedules inject at these
// points, never after a guessed sleep.
func waitServed(t *testing.T, rec *lincheck.Recorder, n int, d time.Duration) {
	t.Helper()
	want := served(rec) + n
	deadline := time.Now().Add(d)
	for served(rec) < want {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d operations served within %s", served(rec), want, d)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitFor polls a condition (a fault engaging, a leader appearing) — a
// synchronization point, bounded by d.
func waitFor(t testing.TB, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, d)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// runFaults runs the workload in the background while schedule runs on the
// test goroutine (so its waits can fail the test); the clients stop once the
// schedule returns.
func runFaults(t *testing.T, c *cluster, opts workload.Options, schedule func(rec *lincheck.Recorder)) (*lincheck.Recorder, workload.Stats) {
	t.Helper()
	rec := lincheck.NewRecorder()
	ctx, cancel := context.WithTimeout(c.ctx, 120*time.Second)
	defer cancel()
	var stop atomic.Bool
	opts.Stop = stop.Load
	done := make(chan workload.Stats, 1)
	go func() { done <- workload.Run(ctx, c.endpoints(), opts, rec) }()
	schedule(rec)
	stop.Store(true)
	return rec, <-done
}

// TestLinearizableUnderMessageFaults: scenario I(1–3) — delay (hold and release
// in reverse), drop and duplication of Raft traffic while eight clients run.
// Each fault is held until it has demonstrably engaged (the network's own
// counters), and the clients make progress between rounds.
func TestLinearizableUnderMessageFaults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, true)
	c.waitLeader(0, 10*time.Second)
	c.net.AddRule(fault.Rule{Action: fault.Duplicate, Copies: 1})
	rec, st := runFaults(t, c, workload.Options{Clients: 8, OpsPerClient: 1 << 20, Keys: 3, Timeout: 3 * time.Second, Seed: 5, GetPct: 40, DeletePct: 10, MaxAttempts: 6, Backoff: 5 * time.Millisecond}, func(rec *lincheck.Recorder) {
		waitServed(t, rec, 20, 30*time.Second)
		for i := 0; i < 6; i++ {
			held := c.net.Stats().Held
			hold := c.net.AddRule(fault.Rule{Kinds: []transport.MsgKind{transport.MsgAppendEntries}, Action: fault.Hold})
			waitFor(t, "holding AppendEntries", 10*time.Second, func() bool { return c.net.Stats().Held >= held+4 })
			c.net.RemoveRule(hold)
			c.net.Release(true) // newest first: reordering
			dropped := c.net.Stats().Dropped
			drop := c.net.AddRule(fault.Rule{Action: fault.Drop, Count: 5})
			waitFor(t, "dropping five messages", 10*time.Second, func() bool { return c.net.Stats().Dropped >= dropped+5 })
			c.net.RemoveRule(drop)
			waitServed(t, rec, 10, 30*time.Second)
		}
	})
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
	t1 := c.node(l).Status().Term
	rec, st := runFaults(t, c, workload.Options{Clients: 6, OpsPerClient: 1 << 20, Keys: 3, Timeout: 2 * time.Second, Seed: 11, GetPct: 40, DeletePct: 10, MaxAttempts: 6, Backoff: 5 * time.Millisecond}, func(rec *lincheck.Recorder) {
		waitServed(t, rec, 40, 30*time.Second)
		c.crash(l)
		c.waitLeader(t1, 10*time.Second)
		waitServed(t, rec, 30, 30*time.Second)
		c.startNode(l)
		waitServed(t, rec, 30, 30*time.Second)
	})
	check(t, rec, st)
	if st.Unknown+st.Unavailable+st.Redirects == 0 {
		t.Fatalf("the crash did not intersect the workload: %s", st)
	}
	// The restarted node has rebuilt its store from the log: after the run,
	// once it has caught up, its store equals the leader's for every key.
	l2 := c.waitLeader(0, 10*time.Second)
	srvL, _ := c.eps[l2].current()
	srvR, _ := c.eps[l].current()
	waitFor(t, "the restarted node catching up", 10*time.Second, func() bool {
		return srvR.Node().Status().Applied >= srvL.Node().Status().Commit
	})
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
	t1 := c.node(l).Status().Term
	rec, st := runFaults(t, c, workload.Options{Clients: 6, OpsPerClient: 1 << 20, Keys: 3, Timeout: 1500 * time.Millisecond, Seed: 17, GetPct: 40, DeletePct: 10, MaxAttempts: 6, Backoff: 5 * time.Millisecond}, func(rec *lincheck.Recorder) {
		waitServed(t, rec, 40, 30*time.Second)
		c.net.Isolate(string(l), c.members())
		c.waitLeader(t1, 10*time.Second)
		waitServed(t, rec, 40, 30*time.Second)
		c.net.HealAll()
		waitFor(t, "the old leader stepping down", 10*time.Second, func() bool { return c.node(l).Status().Term > t1 })
		waitServed(t, rec, 30, 30*time.Second)
	})
	check(t, rec, st)
	if st.Unknown+st.Redirects == 0 {
		t.Fatalf("the partition did not intersect the workload: %s", st)
	}
}

// TestSessionClientsUnderFaultsRetryAndStayLinearizable is Phase 13 under
// Phase 10's faults: session clients that RETRY every unknown write under its
// identity, with deliberate concurrent duplicates, through held-and-reversed,
// dropped and duplicated traffic (forwards included), a leader crash and
// restart, and a leader partition and heal. The checker judges LOGICAL
// operations — a retried request is one operation, its sends attempts — and
// the history must be linearizable; the clients must actually have retried
// unknown writes and met duplicates, or the run proved nothing.
func TestSessionClientsUnderFaultsRetryAndStayLinearizable(t *testing.T) {
	opts := workload.Options{Clients: 6, OpsPerClient: 1 << 20, Keys: 2, Timeout: 1 * time.Second, Seed: 13,
		GetPct: 35, DeletePct: 15, MaxAttempts: 30, Backoff: 5 * time.Millisecond, Sessions: true, DupPct: 20}
	t.Run("messages", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := startCluster(t, ctx, 3, true)
		c.waitLeader(0, 10*time.Second)
		dup := c.net.AddRule(fault.Rule{Action: fault.Duplicate, Copies: 1})
		rec, st := runFaults(t, c, opts, func(rec *lincheck.Recorder) {
			waitServed(t, rec, 20, 30*time.Second)
			for i := 0; i < 5; i++ {
				held := c.net.Stats().Held
				hold := c.net.AddRule(fault.Rule{Kinds: []transport.MsgKind{transport.MsgAppendEntries, transport.MsgForwardResponse}, Action: fault.Hold})
				waitFor(t, "holding replication and forward responses", 10*time.Second, func() bool { return c.net.Stats().Held >= held+4 })
				c.net.RemoveRule(hold)
				c.net.Release(true)
				dropped := c.net.Stats().Dropped
				drop := c.net.AddRule(fault.Rule{Kinds: []transport.MsgKind{transport.MsgForward, transport.MsgForwardResponse, transport.MsgAppendEntries}, Action: fault.Drop, Count: 6})
				waitFor(t, "dropping six messages", 10*time.Second, func() bool { return c.net.Stats().Dropped >= dropped+6 })
				c.net.RemoveRule(drop)
				// And exactly one forward RESPONSE: the leader executed the
				// request, the forwarder never hears — UNKNOWN — and the
				// client must retry the same request. (Drops that Raft heals
				// by retransmission alone do not guarantee an unknown outcome.)
				// The duplicate rule is suspended meanwhile: it applies first,
				// so a response would travel as two copies and dropping one
				// would lose nothing (fault.Rule: duplication, then the
				// terminal rules, per copy).
				c.net.RemoveRule(dup)
				dropped = c.net.Stats().Dropped
				lost := c.net.AddRule(fault.Rule{Kinds: []transport.MsgKind{transport.MsgForwardResponse}, Action: fault.Drop, Count: 1})
				waitFor(t, "dropping a forward response", 10*time.Second, func() bool { return c.net.Stats().Dropped > dropped })
				c.net.RemoveRule(lost)
				dup = c.net.AddRule(fault.Rule{Action: fault.Duplicate, Copies: 1})
				waitServed(t, rec, 10, 30*time.Second)
			}
		})
		c.net.ClearRules()
		c.net.Release(false)
		check(t, rec, st)
		requireSessionEffects(t, st)
		if st.Unknown == 0 {
			t.Fatalf("five forward responses were lost, yet no attempt was unknown: %s", st)
		}
	})
	t.Run("leader-crash", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := startCluster(t, ctx, 3, false)
		l := c.waitLeader(0, 10*time.Second)
		t1 := c.node(l).Status().Term
		rec, st := runFaults(t, c, opts, func(rec *lincheck.Recorder) {
			waitServed(t, rec, 30, 30*time.Second)
			// The leader dies right after applying a client's write, before
			// answering it (not at an arbitrary instant, when no request may
			// be in flight there): that client's outcome is unknown, and its
			// retry at the new leader is a duplicate.
			fired := c.armCrash(l, raftnode.AfterAppliedTo, 0)
			fired.Wait()
			c.setHook(nil)
			c.crash(l)
			c.waitLeader(t1, 10*time.Second)
			waitServed(t, rec, 30, 30*time.Second)
			c.startNode(l)
			waitServed(t, rec, 30, 30*time.Second)
		})
		check(t, rec, st)
		requireSessionEffects(t, st)
		if st.Unknown == 0 {
			t.Fatalf("the leader died after applying a client's write, yet no attempt was unknown: %s", st)
		}
	})
	t.Run("leader-partition", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := startCluster(t, ctx, 3, true)
		l := c.waitLeader(0, 10*time.Second)
		t1 := c.node(l).Status().Term
		rec, st := runFaults(t, c, opts, func(rec *lincheck.Recorder) {
			waitServed(t, rec, 30, 30*time.Second)
			c.net.Isolate(string(l), c.members())
			c.waitLeader(t1, 10*time.Second)
			waitServed(t, rec, 30, 30*time.Second)
			c.net.HealAll()
			waitFor(t, "the old leader stepping down", 10*time.Second, func() bool { return c.node(l).Status().Term > t1 })
			waitServed(t, rec, 30, 30*time.Second)
		})
		check(t, rec, st)
		requireSessionEffects(t, st)
	})
}

// requireSessionEffects fails a session run that never retried an unknown
// write or never saw a duplicate answered — it would have tested nothing new.
func requireSessionEffects(t *testing.T, st workload.Stats) {
	t.Helper()
	if st.Sessions == 0 || st.DupSends == 0 || st.Duplicates == 0 || st.WriteRetries+st.Unknown == 0 {
		t.Fatalf("the run did not exercise retries and duplicates: %s", st)
	}
	t.Logf("sessions: %s", st)
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
	// And the same read sent to the follower through the API is never served
	// from the follower's state: in redirect-only mode it is refused; with
	// forwarding (Phase 13) the leader serves it — through ReadIndex — and the
	// answer is the fresh value, labelled with the leader that served it.
	srvF.SetForwarding(false)
	if _, _, err := srvF.Get(ctx, []byte("k")); err == nil {
		t.Fatal("a follower served a linearizable read")
	}
	srvF.SetForwarding(true)
	resp, _ := srvF.Do(ctx, kv.Request{Op: kv.ReqGet, Key: []byte("k")})
	if resp.Status != kv.StatusOK || string(resp.Value) != "new" || resp.Node != string(l) || resp.Via != follower.Name() {
		t.Fatalf("a forwarded read must be the leader's fresh answer: %+v", resp)
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
