package kv_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/transport"
)

// Phase 13 retry semantics on the real driver (docs/CLIENT_SEMANTICS.md §4, §7,
// §9): real raftnode actors over real TCP, kv.Server per node, sessions that
// retry under one identity — and crashes at exact driver points (Phase 11's
// seam), dropped forward responses and held replication to make every retry
// case happen exactly, not by luck of timing.

var errCrash = errors.New("test: crash point")

func startClusterWith(t *testing.T, ctx context.Context, n int, faults bool, limits kv.Limits) *cluster {
	t.Helper()
	c := &cluster{t: t, ctx: ctx, dir: t.TempDir(), addrs: map[raftnode.NodeID]string{}, limits: limits,
		trs: map[raftnode.NodeID]*transport.TCPTransport{}, nodes: map[raftnode.NodeID]*raftnode.Node{}, eps: map[raftnode.NodeID]*endpoint{}}
	if faults {
		c.net = fault.NewNetwork()
	}
	for i := 0; i < n; i++ {
		id := raftnode.NodeID(fmt.Sprintf("n%d", i+1))
		c.ids = append(c.ids, id)
		c.addrs[id] = freeAddr(t)
		c.eps[id] = &endpoint{name: string(id)}
	}
	for _, id := range c.ids {
		c.startNode(id)
	}
	t.Cleanup(c.stop)
	return c
}

func (c *cluster) doers(order ...raftnode.NodeID) []kv.Doer {
	if len(order) == 0 {
		order = c.ids
	}
	var out []kv.Doer
	for _, id := range order {
		out = append(out, c.eps[id])
	}
	return out
}

func (c *cluster) server(id raftnode.NodeID) *kv.Server {
	s, err := c.eps[id].current()
	if err != nil {
		c.t.Fatalf("%s: %v", id, err)
	}
	return s
}

func register(t *testing.T, c *cluster, opts kv.SessionOptions, order ...raftnode.NodeID) *kv.Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
	defer cancel()
	s, err := kv.Register(ctx, c.doers(order...), opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// quiesce waits until every live node has applied everything the leader has
// committed: from here on, the leader's next entry is the next client write.
func (c *cluster) quiesce(leader raftnode.NodeID) uint64 {
	c.t.Helper()
	ln := c.node(leader)
	waitFor(c.t, "the group applying the leader's log", 10*time.Second, func() bool {
		st := ln.Status()
		if st.Commit != st.LastIndex || st.Applied != st.Commit {
			return false
		}
		for _, id := range c.ids {
			if n := c.node(id); n != nil && n.Status().Applied < st.Commit {
				return false
			}
		}
		return true
	})
	return ln.Status().LastIndex
}

// armCrash aborts node id's actor the first time it reaches point p (for an
// apply point, at entry index idx; 0 matches any). The node stops as it does on
// a fail-stop: every request waiting on it is answered UNKNOWN.
func (c *cluster) armCrash(id raftnode.NodeID, p raftnode.Point, idx uint64) *sync.WaitGroup {
	var fired sync.WaitGroup
	fired.Add(1)
	var once sync.Once
	c.setHook(func(n raftnode.NodeID, pt raftnode.Point, arg uint64) error {
		if n != id || pt != p || (idx != 0 && arg != idx) {
			return nil
		}
		err := error(nil)
		once.Do(func() { err = errCrash; fired.Done() })
		return err
	})
	return &fired
}

// restart replaces a crashed node: close what is left of it and start it on
// its log with a fresh store (rebuilt by replay).
func (c *cluster) restart(id raftnode.NodeID) {
	c.crash(id)
	c.startNode(id)
}

func get(t *testing.T, c *cluster, key string) (string, bool) {
	t.Helper()
	l := c.waitLeader(0, 10*time.Second)
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	resp, _ := c.server(l).Do(ctx, kv.Request{Op: kv.ReqGet, Key: []byte(key)})
	switch resp.Status {
	case kv.StatusOK:
		return string(resp.Value), true
	case kv.StatusNotFound:
		return "", false
	}
	t.Fatalf("read %s: %+v", key, resp)
	return "", false
}

// TestUnknownWriteRetriedAfterLeaderCrashIsOneRequest is the hardest case of
// the phase, exactly: the request commits and is applied on the leader, the
// leader dies before replying (the client hears nothing), a new leader is
// elected, another client overwrites the key, and the first client retries the
// SAME request. The retry must be answered OK as a duplicate, reporting the
// index of the ORIGINAL execution, with no second state transition — the key
// keeps the other client's value.
func TestUnknownWriteRetriedAfterLeaderCrashIsOneRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, false, kv.Limits{})
	l := c.waitLeader(0, 10*time.Second)
	t1 := c.node(l).Status().Term
	s := register(t, c, kv.SessionOptions{AttemptTimeout: 3 * time.Second, MaxAttempts: 1}, l)
	idx := c.quiesce(l) + 1
	fired := c.armCrash(l, raftnode.AfterAppliedTo, idx)
	rid := s.Reserve()
	first := s.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte("A"), nil)
	fired.Wait()
	c.setHook(nil)
	if first.Known || !errors.Is(first.Err, kv.ErrUnknown) {
		t.Fatalf("the client of a leader that died before replying must not know the outcome: %+v", first)
	}
	c.restart(l)
	l2 := c.waitLeader(t1, 10*time.Second)
	other := register(t, c, kv.SessionOptions{}, l2)
	if o := other.Put(ctx, []byte("k"), []byte("B"), nil); o.Err != nil {
		t.Fatalf("the other client's write: %+v", o)
	}
	retry := kv.ResumeSession(c.doers(), kv.SessionOptions{AttemptTimeout: 3 * time.Second}, s.ID(), s.Next())
	retry.Hold(rid)
	again := retry.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte("A"), nil)
	if again.Err != nil || !again.Response.Duplicate || again.Response.Index != idx {
		t.Fatalf("the retry must be answered as the original execution at index %d, not re-executed: %+v", idx, again)
	}
	if v, _ := get(t, c, "k"); v != "B" {
		t.Fatalf("the retry changed the state again: k = %q, want B", v)
	}
}

// TestRetryAtEveryCrashPointOfAWrite crashes the leader at every driver point
// of one identified write's life and lets the session retry through the
// election, to a definite answer. Exactly one execution, always: a write
// committed before the crash is answered as a duplicate of that execution; a
// write that never left the leader is executed by the retry; and the key's
// final value agrees with which one it was.
func TestRetryAtEveryCrashPointOfAWrite(t *testing.T) {
	for _, tc := range []struct {
		point     raftnode.Point
		atIndex   bool   // an apply point: match the write's own index
		duplicate string // "yes", "no", "either"
	}{
		{raftnode.BeforeSave, false, "no"},
		{raftnode.AfterSave, false, "either"},
		{raftnode.AfterSend, false, "either"},
		{raftnode.BeforeAdvance, false, "either"},
		{raftnode.BeforeApply, true, "yes"},
		{raftnode.AfterApply, true, "yes"},
		{raftnode.AfterAppliedTo, true, "yes"},
	} {
		t.Run(tc.point.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := startClusterWith(t, ctx, 3, false, kv.Limits{})
			l := c.waitLeader(0, 10*time.Second)
			s := register(t, c, kv.SessionOptions{AttemptTimeout: 2 * time.Second, MaxAttempts: 20, Backoff: 20 * time.Millisecond})
			if o := s.Put(ctx, []byte("k"), []byte("old"), nil); o.Err != nil {
				t.Fatal(o.Err)
			}
			l = c.waitLeader(0, 10*time.Second)
			idx := c.quiesce(l) + 1
			var at uint64
			if tc.atIndex {
				at = idx
			}
			fired := c.armCrash(l, tc.point, at)
			restarted := make(chan struct{})
			go func() {
				fired.Wait()
				c.setHook(nil)
				c.restart(l) // the crashed node comes back while the client retries
				close(restarted)
			}()
			out := s.Put(ctx, []byte("k"), []byte("new"), nil)
			<-restarted
			if out.Err != nil {
				t.Fatalf("the retried write did not complete: %+v", out)
			}
			if out.Attempts < 2 {
				t.Fatalf("the first attempt should have died with the leader: %+v", out)
			}
			switch {
			case tc.duplicate == "yes" && !out.Response.Duplicate:
				t.Fatalf("committed before the crash at %s, yet the retry executed it again: %+v", tc.point, out.Response)
			case tc.duplicate == "yes" && out.Response.Index != idx:
				t.Fatalf("the duplicate must report the original execution at %d: %+v", idx, out.Response)
			case tc.duplicate == "no" && out.Response.Duplicate:
				t.Fatalf("the write never left the leader at %s, yet the retry was answered as a duplicate: %+v", tc.point, out.Response)
			}
			if v, _ := get(t, c, "k"); v != "new" {
				t.Fatalf("k = %q", v)
			}
			// Exactly one execution anywhere: every replica — restarted ones
			// rebuilt theirs by replaying the log — executed exactly two
			// writes, the setup "old" and the one logical request "new";
			// every other entry carrying the request was a duplicate.
			commit := c.node(c.waitLeader(0, 10*time.Second)).Status().Commit
			for _, id := range c.ids {
				c.waitApplied(id, commit)
				if st := c.server(id).Store().Stats(); st.Executed != 2 {
					t.Fatalf("%s executed %d writes (%+v): the request took effect more than once, or never", id, st.Executed, st)
				}
			}
			t.Logf("crash at %s: %d attempts; duplicate=%v, executed at index %d", tc.point, out.Attempts, out.Response.Duplicate, out.Response.Index)
		})
	}
}

func (c *cluster) waitApplied(id raftnode.NodeID, idx uint64) {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("%s applying through %d", id, idx), 10*time.Second, func() bool {
		n := c.node(id)
		return n != nil && n.Status().Applied >= idx
	})
}

// TestForwardedRequestWhoseAnswerIsLostIsRetriedSafely: a follower forwards
// the write, the leader executes it, and the forward's response is dropped on
// the way back — the forwarder answers UNKNOWN; the session retries through
// the OTHER follower, which forwards it again. The second forward is a
// duplicate: one execution.
func TestForwardedRequestWhoseAnswerIsLostIsRetriedSafely(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, true, kv.Limits{})
	l := c.waitLeader(0, 10*time.Second)
	var fs []raftnode.NodeID
	for _, id := range c.ids {
		if id != l {
			fs = append(fs, id)
		}
	}
	s := register(t, c, kv.SessionOptions{AttemptTimeout: time.Second, MaxAttempts: 6}, fs...)
	drop := c.net.AddRule(fault.Rule{Kinds: []transport.MsgKind{transport.MsgForwardResponse}, Action: fault.Drop, Count: 1})
	out := s.Put(ctx, []byte("k"), []byte("A"), nil)
	c.net.RemoveRule(drop)
	if out.Err != nil || !out.Response.Duplicate || out.Response.Via == "" || out.Attempts < 2 {
		t.Fatalf("want OK as a forwarded duplicate after a lost forward response: %+v", out)
	}
	if c.net.Stats().Dropped == 0 {
		t.Fatal("the forward response was never dropped: the test proved nothing")
	}
	if v, _ := get(t, c, "k"); v != "A" {
		t.Fatalf("k = %q", v)
	}
}

// TestConcurrentDuplicatesAtTwoNodes: the same request sent at the same moment
// to both followers, which both forward it — two log entries, one execution:
// exactly one answer is an execution and the other a duplicate of it. Many
// rounds, for the race detector.
func TestConcurrentDuplicatesAtTwoNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, false, kv.Limits{})
	l := c.waitLeader(0, 10*time.Second)
	var fs []raftnode.NodeID
	for _, id := range c.ids {
		if id != l {
			fs = append(fs, id)
		}
	}
	s := register(t, c, kv.SessionOptions{}, l)
	for round := 0; round < 30; round++ {
		rid := s.Reserve()
		value := []byte(fmt.Sprintf("v%d", round))
		outs := make([]kv.Outcome, 2)
		var wg sync.WaitGroup
		for i, f := range fs {
			s2 := kv.ResumeSession(c.doers(f), kv.SessionOptions{MaxAttempts: 1}, s.ID(), rid+1)
			s2.Hold(rid)
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				outs[i] = s2.Send(ctx, rid, kv.ReqPut, []byte("k"), value, nil)
			}(i)
		}
		wg.Wait()
		s.Release(rid)
		if outs[0].Err != nil || outs[1].Err != nil {
			t.Fatalf("round %d: %+v / %+v", round, outs[0], outs[1])
		}
		if outs[0].Response.Duplicate == outs[1].Response.Duplicate || outs[0].Response.Index != outs[1].Response.Index {
			t.Fatalf("round %d: want exactly one execution and one duplicate of it: %+v / %+v", round, outs[0].Response, outs[1].Response)
		}
	}
}

// TestDuplicateSentBeforeTheOriginalCommits: the followers' acknowledgements
// are held so the original cannot commit; its duplicate is sent meanwhile; both
// entries then commit, in order — the original executes, the duplicate is
// answered from it. (The ACKS are held, not the AppendEntries: holding those
// would silence the heartbeats too, and a follower that hears none campaigns
// and deposes the leader the test is about.)
func TestDuplicateSentBeforeTheOriginalCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, true, kv.Limits{})
	l := c.waitLeader(0, 10*time.Second)
	s := register(t, c, kv.SessionOptions{AttemptTimeout: 300 * time.Millisecond, MaxAttempts: 1}, l)
	c.quiesce(l)
	held := c.net.Stats().Held
	hold := c.net.AddRule(fault.Rule{Kinds: []transport.MsgKind{transport.MsgAppendEntriesResponse}, Action: fault.Hold})
	rid := s.Reserve()
	first := s.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte("A"), nil)
	if first.Known {
		t.Fatalf("a write that cannot commit was answered: %+v", first)
	}
	waitFor(t, "the acknowledgements to be held", 5*time.Second, func() bool { return c.net.Stats().Held > held })
	dup := kv.ResumeSession(c.doers(l), kv.SessionOptions{AttemptTimeout: 10 * time.Second, MaxAttempts: 1}, s.ID(), s.Next())
	dup.Hold(rid)
	done := make(chan kv.Outcome, 1)
	go func() { done <- dup.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte("A"), nil) }()
	waitFor(t, "the duplicate to be appended behind the original", 5*time.Second, func() bool {
		return c.node(l).Status().LastIndex >= c.node(l).Status().Commit+2
	})
	c.net.RemoveRule(hold)
	c.net.Release(false)
	out := <-done
	if out.Err != nil || !out.Response.Duplicate {
		t.Fatalf("the duplicate must be answered from the original's execution: %+v", out)
	}
	if st := c.node(l).Status(); st.Role != raft.Leader {
		t.Fatalf("premise: %s must have led throughout, it is now %s in term %d", l, st.Role, st.Term)
	}
}

// TestRetryAfterEveryNodeRestarts: the request executes, its reply is lost,
// then EVERY node restarts — every store is rebuilt from the log, session table
// included. The retry is still a duplicate.
func TestRetryAfterEveryNodeRestarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, false, kv.Limits{})
	l := c.waitLeader(0, 10*time.Second)
	s := register(t, c, kv.SessionOptions{MaxAttempts: 1, AttemptTimeout: 3 * time.Second}, l)
	idx := c.quiesce(l) + 1
	fired := c.armCrash(l, raftnode.AfterAppliedTo, idx)
	rid := s.Reserve()
	if o := s.Send(ctx, rid, kv.ReqDelete, []byte("k"), nil, nil); o.Known {
		t.Fatalf("want an unknown outcome: %+v", o)
	}
	fired.Wait()
	c.setHook(nil)
	for _, id := range c.ids {
		c.restart(id)
	}
	c.waitLeader(0, 10*time.Second)
	retry := kv.ResumeSession(c.doers(), kv.SessionOptions{MaxAttempts: 20, Backoff: 20 * time.Millisecond}, s.ID(), s.Next())
	retry.Hold(rid)
	out := retry.Send(ctx, rid, kv.ReqDelete, []byte("k"), nil, nil)
	if out.Err != nil || !out.Response.Duplicate || out.Response.Index != idx {
		t.Fatalf("after every node restarted the retry must still be the original at %d: %+v", idx, out)
	}
}

// TestConflictingReuseAndIdentityScope: the same RequestID with a different
// command is refused and changes nothing; RequestIDs are per client; a client
// that restarts and reuses an old id with the same command is answered as the
// same request, with another command refused, and below its acknowledged
// watermark as stale.
func TestConflictingReuseAndIdentityScope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, false, kv.Limits{})
	l := c.waitLeader(0, 10*time.Second)
	s := register(t, c, kv.SessionOptions{}, l)
	rid := s.Reserve()
	if o := s.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte("A"), nil); o.Err != nil || o.Response.Duplicate {
		t.Fatal(o)
	}
	if o := s.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte("B"), nil); !errors.Is(o.Err, kv.ErrConflict) {
		t.Fatalf("same id, different value: %+v", o)
	}
	if o := s.Send(ctx, rid, kv.ReqDelete, []byte("k"), nil, nil); !errors.Is(o.Err, kv.ErrConflict) {
		t.Fatalf("same id, a delete: %+v", o)
	}
	if v, _ := get(t, c, "k"); v != "A" {
		t.Fatalf("a refused conflict changed the state: %q", v)
	}
	s.Release(rid)
	// Another client's request 1 is unrelated.
	other := register(t, c, kv.SessionOptions{}, l)
	orid := other.Reserve()
	if orid != rid {
		t.Fatalf("setup: both sessions should start at request id %d", rid)
	}
	if o := other.Send(ctx, orid, kv.ReqPut, []byte("k"), []byte("C"), nil); o.Err != nil || o.Response.Duplicate {
		t.Fatalf("another client's request %d must execute: %+v", orid, o)
	}
	other.Release(orid)
	if v, _ := get(t, c, "k"); v != "C" {
		t.Fatalf("k = %q", v)
	}
	// The first client "restarts" and forgets its counter: request 1 again.
	amnesiac := kv.ResumeSession(c.doers(l), kv.SessionOptions{}, s.ID(), rid)
	r1 := amnesiac.Reserve()
	if o := amnesiac.Send(ctx, r1, kv.ReqPut, []byte("k"), []byte("A"), nil); o.Err != nil || !o.Response.Duplicate {
		t.Fatalf("an old id re-sent with the same command is the same request: %+v", o)
	}
	if v, _ := get(t, c, "k"); v != "C" {
		t.Fatalf("a re-sent old request executed again: k = %q", v)
	}
	amnesiac.Release(r1)
	// Once the client has acknowledged it (a later request raises the
	// watermark), the old id is stale.
	r2 := amnesiac.Reserve()
	if o := amnesiac.Send(ctx, r2, kv.ReqPut, []byte("j"), []byte("x"), nil); o.Err != nil {
		t.Fatal(o)
	}
	amnesiac.Release(r2)
	stale := kv.ResumeSession(c.doers(l), kv.SessionOptions{}, s.ID(), rid)
	r3 := stale.Reserve()
	if o := stale.Send(ctx, r3, kv.ReqPut, []byte("k"), []byte("A"), nil); !errors.Is(o.Err, kv.ErrStale) {
		t.Fatalf("an acknowledged id must be stale: %+v", o)
	}
}

// TestEvictedSessionIsRefusedNotReexecuted: with room for two sessions, a third
// registration evicts the least recently used one; that session's retry of a
// request it had executed is refused as SESSION_EXPIRED — never executed again
// — and the client learns only that its session is gone.
func TestEvictedSessionIsRefusedNotReexecuted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, false, kv.Limits{MaxSessions: 2, MaxUnacked: 8})
	l := c.waitLeader(0, 10*time.Second)
	old := register(t, c, kv.SessionOptions{}, l)
	rid := old.Reserve()
	if o := old.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte("A"), nil); o.Err != nil {
		t.Fatal(o)
	}
	b := register(t, c, kv.SessionOptions{}, l)
	if o := b.Put(ctx, []byte("k"), []byte("B"), nil); o.Err != nil {
		t.Fatal(o)
	}
	register(t, c, kv.SessionOptions{}, l) // evicts `old`, the least recently used
	o := old.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte("A"), nil)
	if !errors.Is(o.Err, kv.ErrSessionExpired) || o.Response.Status != kv.StatusSessionExpired {
		t.Fatalf("an evicted session's retry must be refused: %+v", o)
	}
	if v, _ := get(t, c, "k"); v != "B" {
		t.Fatalf("an evicted session's retry executed again: k = %q", v)
	}
	if st := c.server(l).Store().Stats(); st.Evicted != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

// TestSessionLimitRefusesRatherThanForgets: a session that keeps more requests
// unacknowledged than the limit is refused the next NEW request (SESSION_LIMIT,
// no effect) — the server never forgets a result a retry might still need —
// and succeeds once it acknowledges.
func TestSessionLimitRefusesRatherThanForgets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, false, kv.Limits{MaxSessions: 8, MaxUnacked: 2})
	l := c.waitLeader(0, 10*time.Second)
	s := register(t, c, kv.SessionOptions{MaxAttempts: 1}, l)
	a, b := s.Reserve(), s.Reserve() // both stay in flight: the watermark stays at a
	for _, rid := range []uint64{a, b} {
		if o := s.Send(ctx, rid, kv.ReqPut, []byte("k"), []byte(fmt.Sprint(rid)), nil); o.Err != nil {
			t.Fatal(o)
		}
	}
	third := s.Reserve()
	if o := s.Send(ctx, third, kv.ReqPut, []byte("k"), []byte("3"), nil); !errors.Is(o.Err, kv.ErrSessionLimit) {
		t.Fatalf("want SESSION_LIMIT: %+v", o)
	}
	if v, _ := get(t, c, "k"); v != fmt.Sprint(b) {
		t.Fatalf("a refused request changed the state: %q", v)
	}
	// Retries of the remembered requests are still answered.
	if o := s.Send(ctx, a, kv.ReqPut, []byte("k"), []byte(fmt.Sprint(a)), nil); o.Err != nil || !o.Response.Duplicate {
		t.Fatalf("a remembered request: %+v", o)
	}
	s.Release(a)
	s.Release(b)
	if o := s.Send(ctx, third, kv.ReqPut, []byte("k"), []byte("3"), nil); o.Err != nil || o.Response.Duplicate {
		t.Fatalf("after acknowledging, the request executes: %+v", o)
	}
}

// TestForwardedRequestIsNeverForwardedAgain: a forward that reaches a node that
// is not the leader is answered NOT_LEADER by it — never forwarded onwards —
// so forwarding cannot loop, whatever the nodes believe.
func TestForwardedRequestIsNeverForwardedAgain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, false, kv.Limits{})
	l := c.waitLeader(0, 10*time.Second)
	var fs []raftnode.NodeID
	for _, id := range c.ids {
		if id != l {
			fs = append(fs, id)
		}
	}
	c.quiesce(l)
	resp := c.server(fs[0]).ForwardTo(ctx, string(fs[1]), kv.Request{Op: kv.ReqPut, Key: []byte("k"), Value: []byte("A")})
	if resp.Status != kv.StatusNotLeader || resp.Node != string(fs[1]) || resp.Leader != string(l) {
		t.Fatalf("a follower receiving a forward must refuse it, naming the leader: %+v", resp)
	}
	if _, ok := get(t, c, "k"); ok {
		t.Fatal("the refused forward executed")
	}
}

// TestConcurrentRequestsFromOneSession: one session, sixteen goroutines, many
// requests in flight at once — the acknowledgement watermark never runs ahead
// of an unanswered request (no STALE, no LIMIT under the default limits), and
// every request executes once.
func TestConcurrentRequestsFromOneSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(t, ctx, 3, false, kv.Limits{})
	c.waitLeader(0, 10*time.Second)
	s := register(t, c, kv.SessionOptions{})
	var wg sync.WaitGroup
	errs := make(chan error, 16*10)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				o := s.Put(ctx, []byte(fmt.Sprintf("k%d", g)), []byte(fmt.Sprint(i)), nil)
				if o.Err != nil || o.Response.Duplicate {
					errs <- fmt.Errorf("goroutine %d op %d: %+v", g, i, o)
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for g := 0; g < 16; g++ {
		if v, _ := get(t, c, fmt.Sprintf("k%d", g)); v != "9" {
			t.Fatalf("k%d = %q", g, v)
		}
	}
}
