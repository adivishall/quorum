package integration

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/kv/workload"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raftlog"
)

// Phase 13 on real processes (docs/CLIENT_SEMANTICS.md, docs/DEDUP.md): session
// clients speak wire protocol v2 to dkvd -raft processes, retry every unknown
// outcome under the SAME request identity, and are recorded as one logical
// operation per request with every send an attempt; the checker judges the
// history of logical operations. Independently of the history, every run's
// committed log — the durable log the processes wrote — is replayed through a
// fresh kv.Store and through the lincheck session model, which must decide
// every entry identically, and no request identity may execute at two indexes.

// doers returns the endpoints as kv.Doers: the named nodes first, in order,
// then the rest — a session's first attempt goes to the first one.
func (c *rcluster) doers(first ...string) []kv.Doer {
	var out []kv.Doer
	seen := map[string]bool{}
	for _, id := range append(append([]string(nil), first...), c.ids...) {
		if !seen[id] {
			seen[id] = true
			out = append(out, &procEndpoint{node: id, addr: c.kvAddrs[id]})
		}
	}
	return out
}

// session registers a session and returns a scripted recording client on it
// whose first attempt goes to first[0].
func (r *linRun) session(name string, opts kv.SessionOptions, first ...string) *workload.SessionClient {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reg, err := kv.Register(ctx, r.c.doers(first...), kv.SessionOptions{})
	if err != nil {
		r.fail("register %s: %v", name, err)
	}
	r.event("%s registered session %d", name, reg.ID())
	return workload.NewSessionClient(name, kv.ResumeSession(r.c.doers(first...), opts, reg.ID(), 1), r.rec)
}

// resume is a client that restarted with its persisted session (ClientID and
// next RequestID): a new session object, the same identity.
func (r *linRun) resume(name string, old *kv.Session, opts kv.SessionOptions, first ...string) *workload.SessionClient {
	return workload.NewSessionClient(name, kv.ResumeSession(r.c.doers(first...), opts, old.ID(), old.Next()), r.rec)
}

// sessionAttemptServedElsewhere reports whether an op's first attempt was not
// executed by its target: refused as not leader, or forwarded — either proves
// the target did not lead when the request arrived.
func sessionAttemptServedElsewhere(op lincheck.Op) bool {
	if len(op.Attempts) == 0 {
		return false
	}
	res := op.Attempts[0].Result
	return strings.HasPrefix(res, "not_leader") || strings.Contains(res, " via ")
}

// dedupLog is what a committed log shows about request identity.
type dedupLog struct {
	identified int                  // identified PUT/DELETE entries
	duplicates int                  // of them, decided Duplicate
	executedAt map[[2]uint64]uint64 // identity → the index where it executed
	entries    map[[2]uint64]int    // identity → entries carrying it
}

// dedupEvidence replays a node's durable committed prefix through a fresh
// kv.Store and, independently, the lincheck session model: both must decide
// every entry identically (INV-X11 on the real log), and no identity may
// execute at two indexes (INV-X2).
func (c *rcluster) dedupEvidence(id string, limits kv.Limits) dedupLog {
	c.t.Helper()
	rec := c.liveLog(id)
	commit := min(rec.HardState.Commit, uint64(len(rec.Entries)))
	store := kv.NewStoreWithLimits(limits)
	model := lincheck.NewSessionModel(lincheck.SessionLimits{MaxSessions: limits.MaxSessions, MaxUnacked: limits.MaxUnacked})
	ev := dedupLog{executedAt: map[[2]uint64]uint64{}, entries: map[[2]uint64]int{}}
	for _, e := range rec.Entries[:commit] {
		got, err := store.ApplyResult(e.Index, e.Data)
		if err != nil {
			c.t.Fatalf("%s: replaying index %d: %v", id, e.Index, err)
		}
		cmd, derr := kv.Decode(e.Data)
		if derr != nil {
			if got != nil {
				c.t.Fatalf("%s: index %d is not a command, yet the store decided %v", id, e.Index, got)
			}
			continue
		}
		mc := lincheck.SessionCommand{Index: e.Index, Register: cmd.Op == kv.OpRegister, ClientID: cmd.ClientID,
			RequestID: cmd.RequestID, AckedBelow: cmd.AckedBelow, Kind: lincheck.Put, Key: string(cmd.Key), Value: string(cmd.Value)}
		if cmd.Op == kv.OpDelete {
			mc.Kind = lincheck.Delete
		}
		want, wantIdx := model.Apply(mc)
		r, _ := got.(kv.Result)
		if r.Decision.String() != want.String() || r.Index != wantIdx {
			c.t.Fatalf("%s: index %d decided %s@%d by the store, %s@%d by the session model", id, e.Index, r.Decision, r.Index, want, wantIdx)
		}
		if cmd.ClientID == 0 || cmd.Op == kv.OpRegister {
			continue
		}
		key := [2]uint64{cmd.ClientID, cmd.RequestID}
		ev.identified++
		ev.entries[key]++
		switch r.Decision {
		case kv.Duplicate:
			ev.duplicates++
		case kv.Executed:
			if at, ok := ev.executedAt[key]; ok {
				c.t.Fatalf("%s: request (cid=%d, rid=%d) executed at index %d and again at %d", id, key[0], key[1], at, e.Index)
			}
			ev.executedAt[key] = e.Index
		}
	}
	return ev
}

// requireDedupEvidence checks every node's durable committed log and returns
// the longest one's evidence.
func (c *rcluster) requireDedupEvidence(limits kv.Limits) dedupLog {
	c.t.Helper()
	var best dedupLog
	for _, id := range c.ids {
		if ev := c.dedupEvidence(id, limits); ev.identified >= best.identified {
			best = ev
		}
	}
	return best
}

// --- concurrent session workloads under faults ---

// sessionWorkload is faultWorkload under the Phase 13 policy: every client
// registers a session and retries unknown outcomes under the same identity,
// and 15% of writes are also sent concurrently through another node.
func sessionWorkload(seed int64) workload.Options {
	o := faultWorkload(seed)
	o.Sessions, o.DupPct = true, 15
	o.MaxAttempts, o.Backoff = 20, 50*time.Millisecond // enough to ride out an election
	return o
}

// TestRealSessionWorkloadsUnderFaults: six session clients run while the
// leader is SIGKILLed, the leader is partitioned, or every node is restarted in
// turn. The history of logical requests must be linearizable; the clients must
// have retried writes whose attempts went unanswered and sent concurrent
// duplicates; and the committed log must show every identity executed at most
// once, with every replica and the session model agreeing on every decision.
func TestRealSessionWorkloadsUnderFaults(t *testing.T) {
	schedules := []struct {
		name  string
		seed  int64
		sched func(c *rcluster, r *linRun)
	}{
		{"leader-kill", 131, func(c *rcluster, r *linRun) {
			r.waitServed(60, 60*time.Second)
			l, tm := c.waitStable(c.ids, 0, 30*time.Second)
			r.event("kill leader %s (term %d)", l, tm)
			c.kill(l)
			l2, t2 := c.waitLeader(others(c.ids, l), tm, 20*time.Second)
			r.event("%s leads term %d", l2, t2)
			r.waitServed(40, 60*time.Second)
			c.start(l)
			waitForLine(t, c.procs[l], "event=client_ready", 20*time.Second)
			r.event("restarted %s", l)
			c.waitStable(c.ids, t2-1, 30*time.Second)
			r.waitServed(40, 60*time.Second)
		}},
		{"leader-partition", 141, func(c *rcluster, r *linRun) {
			r.waitServed(60, 60*time.Second)
			l, tm := c.waitStable(c.ids, 0, 30*time.Second)
			r.event("isolate leader %s (term %d)", l, tm)
			c.isolate(l)
			l2, t2 := c.waitLeader(others(c.ids, l), tm, 20*time.Second)
			r.event("%s leads term %d", l2, t2)
			r.waitServed(60, 60*time.Second)
			c.healAll()
			r.event("heal")
			c.waitStable(c.ids, t2-1, 30*time.Second)
			r.waitServed(60, 60*time.Second)
		}},
		{"rolling-restart", 151, func(c *rcluster, r *linRun) {
			r.waitServed(40, 60*time.Second)
			for _, id := range c.ids {
				r.event("kill %s", id)
				c.kill(id)
				l, tm := c.waitLeader(others(c.ids, id), 0, 20*time.Second)
				r.event("%s leads term %d", l, tm)
				r.waitServed(30, 60*time.Second)
				c.start(id)
				waitForLine(t, c.procs[id], "event=client_ready", 20*time.Second)
				r.event("restarted %s", id)
				c.waitStable(c.ids, 0, 30*time.Second)
			}
			r.waitServed(30, 60*time.Second)
		}},
	}
	for _, sc := range schedules {
		t.Run(sc.name, func(t *testing.T) {
			c := newRCluster(t, 3)
			c.waitLeader(c.ids, 0, 20*time.Second)
			c.waitClientReady(20 * time.Second)
			r := newLinRun(t, c)
			opts := sessionWorkload(sc.seed)
			st := r.workload(opts, func() { sc.sched(c, r) })
			r.check()
			if st.Sessions != opts.Clients || st.DupSends == 0 || st.Duplicates == 0 {
				r.fail("sessions were not exercised: %s", st)
			}
			if st.WriteRetries == 0 {
				r.fail("the fault never left a session write unanswered, so nothing was retried: %s", st)
			}
			c.finish()
			ev := c.requireDedupEvidence(kv.DefaultLimits)
			if ev.duplicates == 0 {
				t.Fatalf("the committed log holds no duplicate entry: %+v", ev)
			}
			t.Logf("%s; committed log: %d identified writes, %d duplicate entries answered from their original, %d identities executed once each",
				st, ev.identified, ev.duplicates, len(ev.executedAt))
		})
	}
}

// --- the hardest case at every crash window ---

// TestRealSessionRetryAcrossCrashWindows is the hardest retry case on real
// processes, at every point of a write's life. Session W sends PUT(k, A) —
// request (W, 1) — to the leader, which is SIGKILLed at the point; W hears
// nothing (or, after the reply, OK). A new leader is elected; another session
// writes B; W's process "restarts" with its persisted session and retries
// request (W, 1), the SAME identity, at the new leader. The contract:
//
//	the original committed  → the retry is a DUPLICATE, answered with the
//	                          original's index; the key keeps B
//	it did not              → the retry executes (once); the key becomes A
//
// Either way request (W, 1) executed exactly once — which the committed log
// is replayed to prove — and the history of logical requests, in which the
// original send and the retry are one operation, is linearizable. The victim's
// durable log is inspected to establish which case each point is in.
func TestRealSessionRetryAcrossCrashWindows(t *testing.T) {
	cases := []crashCase{
		{"before-save:1", false, "no", "old"},
		{"after-save:1", true, "no", "any"},
		{"after-save:2", true, "yes", "new"},
		{"before-apply:1", true, "yes", "new"},
		{"after-apply:1", true, "yes", "new"},
		{"after-applied-to:1", true, "yes", "new"},
		{"before-reply:1", true, "yes", "new"},
		{"after-reply:1", true, "yes", "new"},
	}
	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			withPremise(t, func() { sessionCrashWindow(t, tc) })
		})
	}
}

func sessionCrashWindow(t *testing.T, tc crashCase) {
	c := newRClusterArgs(t, 3, "-crash-at", tc.spec, "-crash-armed-by-signal")
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	ctx := context.Background()
	l, tm := c.waitStable(c.ids, 0, 30*time.Second)
	patient := kv.SessionOptions{AttemptTimeout: 3 * time.Second, MaxAttempts: 20, Backoff: 50 * time.Millisecond}
	setup := r.session("setup", patient, l)
	if op, out := setup.Put(ctx, "k", []byte("old")); out.Err != nil {
		r.fail("setup: %s %v", op, out.Err)
	}
	w := r.session("W", kv.SessionOptions{AttemptTimeout: 10 * time.Second, MaxAttempts: 1}, l)
	c2 := r.session("c2", patient, others(c.ids, l)...)
	// Quiesce: every node has applied the setup, so after arming the only
	// Save, apply and reply the leader performs are W's write's.
	l, tm2 := c.waitStable(c.ids, 0, 30*time.Second)
	r.premise(tm2 == tm, "leadership moved during setup (term %d → %d)", tm, tm2)
	c.waitCommit(c.ids, c.commitOf(l), 20*time.Second)

	c.armCrash(l)
	r.event("armed %s on leader %s (term %d)", tc.spec, l, tm)
	rid := w.Session().Reserve()
	first, firstOut := w.Send(ctx, rid, lincheck.Put, "k", []byte("A"))
	r.premise(!sessionAttemptServedElsewhere(first), "the armed node %s did not lead when the write arrived: %+v", l, first.Attempts)
	point, nth := c.waitKilledAtPoint(l, 30*time.Second)
	r.event("%s died at %s#%d", l, point, nth)
	if want := strings.SplitN(tc.spec, ":", 2)[0]; point != want {
		r.fail("died at %s, want %s", point, want)
	}
	switch {
	case tc.spec == "after-reply:1" && firstOut.Known:
		t.Logf("the response left before the SIGKILL: W knows (%s)", firstOut.Response.Status)
	case firstOut.Known:
		r.fail("W must not know the outcome of a write whose leader died at %s: %+v", tc.spec, firstOut)
	}

	held, err := raftlog.Inspect(filepath.Join(c.dirs[l], "raft-"+l+".log"))
	if err != nil {
		r.fail("inspect %s: %v", l, err)
	}
	r.premise(held.HardState.Term == tm, "%s's term moved from %d to %d between arming and the crash", l, tm, held.HardState.Term)
	idx := entryHolding(held, "k", "A")
	switch {
	case tc.inLog && idx == 0:
		r.fail("premise: at %s the entry must be on the leader's disk", tc.spec)
	case !tc.inLog && idx != 0:
		r.fail("premise: at %s the entry must NOT be on the leader's disk, but index %d holds it", tc.spec, idx)
	case tc.committed == "yes" && held.HardState.Commit < idx:
		r.fail("premise: at %s the leader's durable commit (%d) must cover the entry (%d)", tc.spec, held.HardState.Commit, idx)
	case tc.committed == "no" && idx != 0 && held.HardState.Commit >= idx:
		r.fail("premise: at %s the entry (%d) must not yet be committed (durable commit %d)", tc.spec, idx, held.HardState.Commit)
	}
	r.event("victim's disk: entry index %d, durable commit %d", idx, held.HardState.Commit)

	l2, t2 := c.waitLeader(others(c.ids, l), tm, 20*time.Second)
	r.event("%s leads term %d", l2, t2)
	// A read before B: a committed original is visible though W does not know
	// it. It also lets the checker ALONE see a second execution — A, then B,
	// then A again cannot come from one PUT(A).
	pre := r.session("pre-reader", patient, l2)
	preRead, preOut := pre.Get(ctx, "k")
	switch {
	case preOut.Err != nil:
		r.fail("read before B: %s %v", preRead, preOut.Err)
	case tc.read == "new" && string(preRead.Output) != "A":
		r.fail("the write was committed when the leader died at %s, but a later read returned %q: a committed write was lost", tc.spec, preRead.Output)
	case tc.read == "old" && string(preRead.Output) != "old":
		r.fail("the write never left the leader at %s, but a later read returned %q", tc.spec, preRead.Output)
	}
	if op, out := c2.Put(ctx, "k", []byte("B")); out.Err != nil {
		r.fail("c2's PUT(B): %s %v", op, out.Err)
	}
	c.start(l)
	waitForLine(t, c.procs[l], "event=client_ready", 20*time.Second)
	r.event("restarted %s: it rebuilt its store, session table included, by replay", l)
	c.waitStable(c.ids, t2-1, 30*time.Second)

	// W restarts with its persisted session and retries the same request.
	w2 := r.resume("W", w.Session(), patient, l2)
	retry, out := w2.Send(ctx, rid, lincheck.Put, "k", []byte("A"))
	w.Session().Release(rid)
	if out.Err != nil || out.Response.Status != kv.StatusOK {
		r.fail("the retry must reach a definite OK: %s %+v", retry, out)
	}
	reader := r.session("reader", patient, l2)
	got, gotOut := reader.Get(ctx, "k")
	if gotOut.Err != nil {
		r.fail("read: %s %v", got, gotOut.Err)
	}
	r.check() // first: the history of logical requests alone
	dup := out.Response.Duplicate
	switch {
	case tc.committed == "yes" && !dup:
		r.fail("the original was committed when the leader died at %s, but the retry executed again: %+v", tc.spec, out.Response)
	case dup && (idx == 0 || out.Response.Index != idx):
		r.fail("the retry was answered as a duplicate of index %d, but the original's entry is index %d", out.Response.Index, idx)
	case dup && string(got.Output) != "B":
		r.fail("the retry was a duplicate, yet the key is %q, not B: it executed again", got.Output)
	case !dup && string(got.Output) != "A":
		r.fail("the retry executed, yet the key is %q, not A", got.Output)
	}
	c.finish()
	ev := c.dedupEvidence(l2, kv.DefaultLimits)
	id := [2]uint64{w.Session().ID(), rid}
	if _, ok := ev.executedAt[id]; !ok {
		t.Fatalf("request (W, %d) never executed in the committed log: %+v", rid, ev)
	}
	t.Logf("crash at %s: W %s; a read before B saw %q; the retry was %s; the key reads %q; (W, %d) has %d committed entries and executed once, at index %d",
		tc.spec, map[bool]string{true: "knew", false: "did not know"}[firstOut.Known], preRead.Output,
		map[bool]string{true: fmt.Sprintf("a duplicate of index %d", out.Response.Index), false: "the first execution"}[dup],
		got.Output, rid, ev.entries[id], ev.executedAt[id])
}

// --- forwarding ---

// TestRealForwarderDiesBeforeRelaying: a session's write goes to a follower,
// which forwards it one hop to the leader; the leader executes it and answers;
// the follower is SIGKILLed before it writes the relayed answer to the client
// (before-reply). The client's attempt is unknown; the session retries the same
// request at the next node and is answered from the original execution. (Had
// the follower become the leader meanwhile, it would have executed the write
// itself before dying at the same point: the retry is a duplicate either way.)
func TestRealForwarderDiesBeforeRelaying(t *testing.T) {
	c := newRClusterArgs(t, 3, "-crash-at", "before-reply:1", "-crash-armed-by-signal")
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	l, tm := c.waitStable(c.ids, 0, 30*time.Second)
	f := others(c.ids, l)[0]
	r := newLinRun(t, c)
	w := r.session("W", kv.SessionOptions{AttemptTimeout: 3 * time.Second, MaxAttempts: 20, Backoff: 50 * time.Millisecond}, f, l)
	c.armCrash(f)
	r.event("armed before-reply:1 on follower %s (leader %s, term %d)", f, l, tm)
	op, out := w.Put(context.Background(), "k", []byte("A"))
	c.waitKilledAtPoint(f, 30*time.Second)
	r.event("%s died before relaying", f)
	if len(op.Attempts) < 2 || op.Attempts[0].Node != f || op.Attempts[0].Complete != 0 {
		r.fail("want an unanswered attempt at %s, then a retry: %+v", f, op.Attempts)
	}
	if out.Err != nil || !out.Response.Duplicate {
		r.fail("the retry of a write the leader executed must be answered as its duplicate: %s %+v", op, out.Response)
	}
	r.check()
	c.start(f)
	waitForLine(t, c.procs[f], "event=client_ready", 20*time.Second)
	c.waitStable(c.ids, 0, 30*time.Second)
	c.finish()
	ev := c.requireDedupEvidence(kv.DefaultLimits)
	t.Logf("attempts %+v; committed log: %d identified, %d duplicate", op.Attempts, ev.identified, ev.duplicates)
}

// TestRealConcurrentDuplicatesThroughEveryNode: the same request is sent at
// once to all three nodes — the leader executes one copy, each follower
// forwards its copy — round after round: 20 PUT rounds, 5 GET rounds, 5 DELETE
// rounds. For a write exactly one copy executes and every other is answered as
// its duplicate, with the same index; a read is never deduplicated — every copy
// really reads, and all see the same value.
func TestRealConcurrentDuplicatesThroughEveryNode(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	c.waitStable(c.ids, 0, 30*time.Second) // every follower knows the leader it forwards to
	r := newLinRun(t, c)
	ctx := context.Background()
	opts := kv.SessionOptions{AttemptTimeout: 5 * time.Second, MaxAttempts: 20, Backoff: 50 * time.Millisecond}
	base := r.session("W", opts)
	sess := base.Session()
	round := func(n int, kind lincheck.Kind, value []byte) []kv.Outcome {
		rid := sess.Reserve()
		outs := make([]kv.Outcome, len(c.ids))
		var wg sync.WaitGroup
		for i, id := range c.ids {
			sess.Hold(rid)
			cl := r.resume(fmt.Sprintf("W@%s", id), sess, kv.SessionOptions{AttemptTimeout: 5 * time.Second, MaxAttempts: 1}, id)
			cl.Session().Hold(rid)
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				defer sess.Release(rid)
				_, outs[i] = cl.Send(ctx, rid, kind, "k", value)
			}(i)
		}
		wg.Wait()
		sess.Release(rid)
		for i, o := range outs {
			if o.Err != nil {
				r.fail("%s round %d: the copy sent to %s: %+v", kind, n, c.ids[i], o)
			}
		}
		return outs
	}
	writes := func(n int, kind lincheck.Kind, value []byte) {
		outs := round(n, kind, value)
		executed := 0
		for _, o := range outs {
			if !o.Response.Duplicate {
				executed++
			}
			if o.Response.Index != outs[0].Response.Index {
				r.fail("%s round %d: copies answered from different indexes: %+v", kind, n, outs)
			}
		}
		if executed != 1 {
			r.fail("%s round %d: %d copies executed, want exactly 1: %+v", kind, n, executed, outs)
		}
	}
	for n := 0; n < 20; n++ {
		writes(n, lincheck.Put, []byte(fmt.Sprintf("v%d", n)))
	}
	for n := 0; n < 5; n++ {
		for _, o := range round(n, lincheck.Get, nil) {
			if o.Response.Duplicate || o.Response.Status != kv.StatusOK || string(o.Response.Value) != "v19" {
				r.fail("GET round %d: every copy must really read v19: %+v", n, o.Response)
			}
		}
	}
	for n := 0; n < 5; n++ {
		writes(n, lincheck.Delete, nil)
	}
	r.check()
	c.finish()
	ev := c.requireDedupEvidence(kv.DefaultLimits)
	if len(ev.executedAt) != 25 || ev.duplicates < 50 {
		t.Fatalf("want 25 write identities executed once and at least 50 duplicate entries: %+v", ev)
	}
	t.Logf("committed log: %d identified entries, %d duplicates, %d identities executed once each", ev.identified, ev.duplicates, len(ev.executedAt))
}

// TestRealConcurrentRequestsFromOneSession: eight goroutines share one session
// and send distinct requests at once (through the session's endpoints — its
// first requests to any node, then following the leader hint). Every request
// executes once — none is refused stale or answered as a duplicate: each
// carries the lowest id still in flight as its watermark, so no request's
// result is forgotten while it is outstanding — and the history is
// linearizable.
func TestRealConcurrentRequestsFromOneSession(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	c.waitStable(c.ids, 0, 30*time.Second)
	r := newLinRun(t, c)
	ctx := context.Background()
	opts := kv.SessionOptions{AttemptTimeout: 5 * time.Second, MaxAttempts: 20, Backoff: 50 * time.Millisecond}
	sess := r.session("S", opts).Session()
	var wg sync.WaitGroup
	errs := make(chan string, 80)
	for g := 0; g < 8; g++ {
		cl := workload.NewSessionClient(fmt.Sprintf("S-g%d", g), sess, r.rec)
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				op, out := cl.Put(ctx, fmt.Sprintf("k%d", g), []byte(fmt.Sprint(i)))
				if out.Err != nil || out.Response.Duplicate {
					errs <- fmt.Sprintf("goroutine %d request %d: %s %+v", g, i, op, out)
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		r.fail("%s", e)
	}
	rd := r.session("reader", opts)
	for g := 0; g < 8; g++ {
		if got, out := rd.Get(ctx, fmt.Sprintf("k%d", g)); out.Err != nil || string(got.Output) != "9" {
			r.fail("k%d = %q (%v)", g, got.Output, out.Err)
		}
	}
	r.check()
	c.finish()
	ev := c.requireDedupEvidence(kv.DefaultLimits)
	if len(ev.executedAt) != 80 {
		t.Fatalf("want 80 requests executed once each: %+v", ev)
	}
}

// TestRealRedirectOnlyModeWithSessions: with -client-forwarding=false, a
// session's request to a follower is refused NOT_LEADER with the leader named,
// and the session follows the hint; nothing is forwarded.
func TestRealRedirectOnlyModeWithSessions(t *testing.T) {
	withPremise(t, func() {
		c := newRClusterEvery(t, 3, []string{"-client-forwarding=false"})
		l, _ := c.waitLeader(c.ids, 0, 20*time.Second)
		c.waitClientReady(20 * time.Second)
		l, _ = c.waitStable(c.ids, 0, 30*time.Second)
		f := others(c.ids, l)[0]
		r := newLinRun(t, c)
		w := r.session("W", kv.SessionOptions{MaxAttempts: 8}, f)
		op, out := w.Put(context.Background(), "k", []byte("A"))
		if out.Err != nil {
			r.fail("%s %v", op, out.Err)
		}
		r.premise(op.Attempts[0].Node == f && op.Attempts[0].Result == "not_leader("+l+")", "the first attempt was not refused by follower %s naming %s: %+v", f, l, op.Attempts)
		for _, a := range op.Attempts {
			if strings.Contains(a.Result, " via ") {
				r.fail("redirect-only mode forwarded: %+v", op.Attempts)
			}
		}
		if last := op.Attempts[len(op.Attempts)-1]; last.Node != l {
			r.fail("the session did not follow the hint to %s: %+v", l, op.Attempts)
		}
		r.check()
		c.finish()
	})
}

// --- the contract's refusals and recovery of the session table ---

// TestRealSessionContractSurvivesFullClusterRestart exercises every refusal of
// the contract on real processes with small limits (-session-max 2,
// -session-max-unacked 2 on every start), then SIGKILLs every process,
// restarts them all (each rebuilds its store and session table by replay) and
// requires the answers to hold afterwards:
//
//	(A, 2) retried                  → a duplicate
//	(A, 2) with another command     → REQUEST_CONFLICT, no effect
//	(A, 1) after (A, 2) acked it    → REQUEST_STALE, no effect
//	a third unacknowledged request  → SESSION_LIMIT, no effect
//	a session evicted by LRU        → SESSION_EXPIRED, no effect — and still
//	                                  expired after the restart, never revived
//	(B, 1) retried after restart    → a duplicate; with another command, a
//	                                  conflict
func TestRealSessionContractSurvivesFullClusterRestart(t *testing.T) {
	limits := kv.Limits{MaxSessions: 2, MaxUnacked: 2}
	c := newRClusterEvery(t, 3, []string{"-session-max", "2", "-session-max-unacked", "2"})
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	ctx := context.Background()
	opts := kv.SessionOptions{AttemptTimeout: 3 * time.Second, MaxAttempts: 20, Backoff: 50 * time.Millisecond}

	a := r.session("A", opts)
	if put1, out := a.Put(ctx, "k", []byte("A1")); out.Err != nil {
		r.fail("(A,1): %s %v", put1, out.Err)
	}
	// (A,2) carries AckedBelow 2: (A,1)'s result may be forgotten.
	if _, out := a.Put(ctx, "k2", []byte("A2")); out.Err != nil {
		r.fail("(A,2): %v", out.Err)
	}
	if _, out := a.Send(ctx, 1, lincheck.Put, "k", []byte("A1")); !errors.Is(out.Err, kv.ErrStale) || !out.Known {
		r.fail("(A,1) below A's watermark must be REQUEST_STALE: %+v", out)
	}
	if _, out := a.Send(ctx, 2, lincheck.Put, "k2", []byte("other")); !errors.Is(out.Err, kv.ErrConflict) || !out.Known {
		r.fail("(A,2) reused with another command must be REQUEST_CONFLICT: %+v", out)
	}
	if _, out := a.Send(ctx, 2, lincheck.Put, "k2", []byte("A2")); out.Err != nil || !out.Response.Duplicate {
		r.fail("(A,2) retried must be a duplicate: %+v", out)
	}

	// SESSION_LIMIT: (A,3) and (A,4) complete but are never acknowledged
	// (their ids stay reserved), so A holds two unacknowledged results.
	r3, r4 := a.Session().Reserve(), a.Session().Reserve()
	for _, rid := range []uint64{r3, r4} {
		if _, out := a.Send(ctx, rid, lincheck.Put, "k3", []byte(fmt.Sprint("A", rid))); out.Err != nil {
			r.fail("(A,%d): %v", rid, out.Err)
		}
	}
	r5 := a.Session().Reserve()
	limited, lo := a.Send(ctx, r5, lincheck.Put, "k5", []byte("never"))
	if !errors.Is(lo.Err, kv.ErrSessionLimit) || !lo.Known {
		r.fail("a third unacknowledged request must be SESSION_LIMIT: %s %+v", limited, lo)
	}
	a.Session().Release(r3)
	a.Session().Release(r4)
	a.Session().Release(r5)

	// Eviction: with room for two sessions, registering B and then C evicts
	// the least recently used — A (its last entry is older than B's REGISTER).
	b := r.session("B", opts)
	if _, out := b.Put(ctx, "kb", []byte("B1")); out.Err != nil {
		r.fail("(B,1): %v", out.Err)
	}
	r.session("C", opts)
	expired, eo := a.Put(ctx, "k", []byte("after-eviction"))
	if !errors.Is(eo.Err, kv.ErrSessionExpired) || !eo.Known {
		r.fail("A was evicted: its next request must be SESSION_EXPIRED: %s %+v", expired, eo)
	}
	r.check()

	// Every process dies at once and restarts on its log.
	for _, id := range c.ids {
		c.kill(id)
	}
	r.event("SIGKILLed every process")
	for _, id := range c.ids {
		c.start(id)
	}
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r.event("restarted every process")
	if _, out := b.Send(ctx, 1, lincheck.Put, "kb", []byte("B1")); out.Err != nil || !out.Response.Duplicate {
		r.fail("after the restart, B's executed request retried must still be a duplicate: %+v", out)
	}
	if _, out := b.Send(ctx, 1, lincheck.Put, "kb", []byte("other")); !errors.Is(out.Err, kv.ErrConflict) {
		r.fail("after the restart, B's request reused with another command must still conflict: %+v", out)
	}
	if _, out := a.Put(ctx, "k", []byte("after-restart")); !errors.Is(out.Err, kv.ErrSessionExpired) {
		r.fail("after the restart, the evicted session must still be expired — never revived: %+v", out)
	}
	rd := r.session("reader", opts)
	for key, want := range map[string]string{"k": "A1", "k2": "A2", "kb": "B1"} {
		if got, out := rd.Get(ctx, key); out.Err != nil || string(got.Output) != want {
			r.fail("no refused request may have taken effect: %s = %q (%v), want %q", key, got.Output, out.Err, want)
		}
	}
	if got, out := rd.Get(ctx, "k5"); !(out.Err == nil && out.Response.Status == kv.StatusNotFound) {
		r.fail("the request refused with SESSION_LIMIT took effect: k5 %s %+v", got, out)
	}
	r.check()
	c.finish()
	ev := c.requireDedupEvidence(limits)
	t.Logf("committed log (limits %+v): %d identified entries, %d duplicates, %d identities executed once each", limits, ev.identified, ev.duplicates, len(ev.executedAt))
}
