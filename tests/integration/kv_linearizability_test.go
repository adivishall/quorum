package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/kv/workload"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raftlog"
)

// Phase 12 real-process linearizability (docs/LINEARIZABILITY.md §7). Real
// dkvd -raft processes serve the Phase 12 operation protocol on their client
// ports (-client-listen); test clients speak the real wire protocol to them
// and record every invocation, every attempt (the node contacted, what came
// back) and every completion into a lincheck.History; the checker consumes
// that client-visible history and nothing else — never a Raft log, a commit
// index or a node's internal state. Faults are real: SIGKILL, restart on the
// data directory (a fresh state machine rebuilt by replay), TCP partitions at
// the test-owned proxies between nodes, and — through dkvd's crash seam — a
// SIGKILL at an exact point of a write's life, armed by a signal the test
// sends at a moment it chooses.
//
// Timing is real, so a run is not replayable from a seed; what is replayable
// is its evidence. Every run records the workload seed and options (the client
// schedule: each client's operation sequence is a function of them), every
// fault and node event placed in the history's own order (a position from the
// same counter as the operations), each node's output, and the history. A
// failure writes all of it to a directory it names and prints the minimized
// counterexample; `go run ./cmd/lincheck DIR/history.txt` re-checks the saved
// history offline. Faults are injected at synchronization points — a leader
// confirmed in the nodes' own output, a client port reported ready, a number
// of completed client operations — never after a guessed sleep.

// procEndpoint dials a process's client port for each request, so every
// workload goroutine has its own connection. A dead process is ErrUnavailable
// before a request is sent (definite: nothing executed), ErrUnknown if it dies
// mid-request (the request may have executed).
type procEndpoint struct {
	node string
	addr string
}

func (e *procEndpoint) Name() string { return e.node }
func (e *procEndpoint) dial() (*kv.Client, error) {
	c, err := kv.Dial(e.node, e.addr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", kv.ErrUnavailable, err)
	}
	return c, nil
}
func (e *procEndpoint) Put(ctx context.Context, key, value []byte) (kv.Meta, error) {
	c, err := e.dial()
	if err != nil {
		return kv.Meta{Node: e.node}, err
	}
	defer c.Close()
	return c.Put(ctx, key, value)
}
func (e *procEndpoint) Get(ctx context.Context, key []byte) ([]byte, kv.Meta, error) {
	c, err := e.dial()
	if err != nil {
		return nil, kv.Meta{Node: e.node}, err
	}
	defer c.Close()
	return c.Get(ctx, key)
}
func (e *procEndpoint) Delete(ctx context.Context, key []byte) (kv.Meta, error) {
	c, err := e.dial()
	if err != nil {
		return kv.Meta{Node: e.node}, err
	}
	defer c.Close()
	return c.Delete(ctx, key)
}

// Do sends one request of the Phase 13 protocol (kv.Doer) on its own
// connection.
func (e *procEndpoint) Do(ctx context.Context, req kv.Request) (kv.Response, error) {
	c, err := e.dial()
	if err != nil {
		return kv.Response{Node: e.node}, err
	}
	defer c.Close()
	return c.Do(ctx, req)
}

func (c *rcluster) endpoints() []workload.Endpoint {
	var out []workload.Endpoint
	for _, id := range c.ids {
		out = append(out, &procEndpoint{node: id, addr: c.kvAddrs[id]})
	}
	return out
}

// waitClientReady waits until every running process has opened its client port.
func (c *rcluster) waitClientReady(d time.Duration) {
	c.t.Helper()
	for _, id := range c.running() {
		waitForLine(c.t, c.procs[id], "event=client_ready", d)
	}
}

// linRun is one recorded real-process run: the history, the fault and node
// events placed in its order, the workload's seed and options, and the
// artifact written when it fails.
type linRun struct {
	t      *testing.T
	c      *rcluster
	rec    *lincheck.Recorder
	mu     sync.Mutex
	events []string
	notes  []string // seed, options, anything else that reproduces the schedule
}

func newLinRun(t *testing.T, c *rcluster) *linRun {
	return &linRun{t: t, c: c, rec: lincheck.NewRecorder()}
}

// event records a fault or node event at the next position of the history's
// clock, so the artifact shows exactly which operations it fell between.
func (r *linRun) event(format string, args ...any) {
	pos := r.rec.Mark()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, fmt.Sprintf("pos=%d %s", pos, fmt.Sprintf(format, args...)))
}

func (r *linRun) note(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notes = append(r.notes, fmt.Sprintf(format, args...))
}

// served is how many operations have completed with an answer the cluster
// actually served (OK or NotFound) — progress, as opposed to refusals.
func (r *linRun) served() int {
	s := r.rec.History().Summary()
	return s.OK + s.NotFound
}

// waitServed is a synchronization point on client progress: it returns once at
// least n more operations than now have been served. Faults are injected at
// these points — never after a guessed sleep — so every fault falls inside a
// window where the cluster demonstrably serves clients.
func (r *linRun) waitServed(n int, d time.Duration) {
	r.t.Helper()
	want := r.served() + n
	deadline := time.Now().Add(d)
	for r.served() < want {
		if time.Now().After(deadline) {
			r.fail("only %d of %d served operations within %s", r.served(), want, d)
		}
		time.Sleep(5 * time.Millisecond)
	}
	r.event("%d operations served", r.served())
}

// client returns a scripted history-recording client.
func (r *linRun) client(name string, timeout time.Duration, attempts int) *workload.Client {
	return workload.NewClient(name, r.c.endpoints(), workload.Options{Timeout: timeout, MaxAttempts: attempts}, r.rec)
}

// workload runs opts' concurrent clients in the background while schedule runs
// on the test goroutine (so its waits may fail the test); the clients stop once
// schedule returns. It returns what the clients saw.
func (r *linRun) workload(opts workload.Options, schedule func()) workload.Stats {
	r.t.Helper()
	r.note("workload seed=%d clients=%d keys=%d get%%=%d delete%%=%d timeout=%s attempts=%d", opts.Seed, opts.Clients, opts.Keys, opts.GetPct, opts.DeletePct, opts.Timeout, opts.MaxAttempts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	r.t.Cleanup(cancel)
	var stop atomic.Bool
	if schedule != nil {
		opts.Stop = stop.Load
	}
	done := make(chan workload.Stats, 1)
	go func() { done <- workload.Run(ctx, r.c.endpoints(), opts, r.rec) }()
	if schedule != nil {
		func() {
			defer stop.Store(true) // also when a premise aborts the attempt
			schedule()
		}()
	}
	st := <-done
	r.event("workload ended: %s", st)
	return st
}

// check checks the recorded history; on a violation it writes the artifact and
// fails with the minimized counterexample.
func (r *linRun) check() lincheck.Result {
	r.t.Helper()
	h := r.rec.History()
	if err := h.Validate(); err != nil {
		r.fail("malformed history: %v", err)
	}
	res := lincheck.Check(h, lincheck.Options{Minimize: true})
	switch {
	case res.Unchecked:
		r.fail("checker budget exceeded: %s", res.Reason)
	case !res.OK:
		r.fail("NOT LINEARIZABLE\n%s\n--- minimized counterexample ---\n%s", res.Reason, lincheck.Format(res.Counterexample))
	}
	c := h.Summary()
	if c.OK+c.NotFound == 0 {
		r.fail("no operation completed; the history proves nothing")
	}
	r.t.Logf("linearizable: %d ops (%d ok, %d notfound, %d rejected, %d incomplete, %d attempts) over %d keys; checker %d states in %s",
		c.Total, c.OK, c.NotFound, c.Rejected, c.Incomplete, c.Attempts, c.Keys, res.Stats.States, res.Stats.Duration)
	return res
}

// fail writes the run's artifact — notes, events, history, every node's
// output — to a directory that outlives the test and fails with its path.
func (r *linRun) fail(format string, args ...any) {
	r.t.Helper()
	dir, err := os.MkdirTemp("", "dkv-lin-"+strings.ReplaceAll(r.t.Name(), "/", "_")+"-")
	msg := fmt.Sprintf(format, args...)
	if err == nil {
		r.mu.Lock()
		var hdr strings.Builder
		fmt.Fprintf(&hdr, "# %s\n", r.t.Name())
		for _, n := range r.notes {
			fmt.Fprintf(&hdr, "# %s\n", n)
		}
		for _, e := range r.events {
			fmt.Fprintf(&hdr, "# event %s\n", e)
		}
		r.mu.Unlock()
		_ = os.WriteFile(filepath.Join(dir, "history.txt"), []byte(hdr.String()+r.rec.History().String()), 0o644)
		for _, p := range r.c.history {
			_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s-%d.log", p.id, p.cmd.Process.Pid)), []byte(p.out.String()), 0o644)
		}
		msg += fmt.Sprintf("\n--- artifact: %s (history.txt, node logs); re-check with: go run ./cmd/lincheck %s ---", dir, filepath.Join(dir, "history.txt"))
	}
	r.mu.Lock()
	msg += "\n--- schedule ---\n" + strings.Join(r.notes, "\n") + "\n" + strings.Join(r.events, "\n")
	r.mu.Unlock()
	r.t.Fatal(msg)
}

// premiseFailed aborts one attempt of a scenario (not the test): real timing
// violated a precondition the scenario verifies — typically, the node it armed
// or cut off had already been deposed by a spurious election before the action,
// which the node's own "not leader" refusal proves. Such an attempt tests
// nothing, so the scenario starts over on a fresh cluster (withPremise). Only a
// premise is ever retried; an assertion that fails fails the test at once.
type premiseFailed struct{ why string }

// premise aborts the attempt when cond is false.
func (r *linRun) premise(cond bool, format string, args ...any) {
	r.t.Helper()
	if cond {
		return
	}
	why := fmt.Sprintf(format, args...)
	r.event("premise not met: %s", why)
	r.c.killAll()
	panic(premiseFailed{why})
}

// withPremise runs attempt until its premises hold, at most maxPremiseAttempts
// times, logging every attempt whose premise real timing violated.
func withPremise(t *testing.T, attempt func()) {
	t.Helper()
	if err := runWithPremise(attempt, t.Logf); err != nil {
		t.Fatal(err)
	}
}

const maxPremiseAttempts = 3

// runWithPremise is withPremise's loop: it returns nil once an attempt
// completes, and an error if every attempt's premise failed. Any other panic
// propagates, and a test failure inside an attempt (t.Fatal: runtime.Goexit)
// ends the test as it always does.
func runWithPremise(attempt func(), logf func(string, ...any)) error {
	for i := 1; i <= maxPremiseAttempts; i++ {
		var failed *premiseFailed
		func() {
			defer func() {
				if v := recover(); v != nil {
					pf, ok := v.(premiseFailed)
					if !ok {
						panic(v)
					}
					failed = &pf
				}
			}()
			attempt()
		}()
		if failed == nil {
			return nil
		}
		logf("attempt %d: premise not met (%s); starting over on a fresh cluster", i, failed.why)
	}
	return fmt.Errorf("the scenario's premise was not met in %d attempts", maxPremiseAttempts)
}

// leftTerm reports whether a node's current process has reported following, or
// leading, a term above t — i.e. it stopped believing it leads term t.
func (c *rcluster) leftTerm(id string, t uint64) bool {
	out := c.procs[id].out.String()
	for _, re := range []*regexp.Regexp{reFollower, reLeader} {
		for _, m := range re.FindAllStringSubmatch(out, -1) {
			if tm, _ := strconv.ParseUint(m[2], 10, 64); m[1] == id && tm > t {
				return true
			}
		}
	}
	return false
}

// refusedAsNotLeader reports whether an operation's first request was refused
// by its target as "not leader" — proof the target did not lead when it arrived.
func refusedAsNotLeader(op lincheck.Op) bool {
	return len(op.Attempts) > 0 && strings.HasPrefix(op.Attempts[0].Result, "not-leader")
}

// require asserts an outcome of one scripted operation.
func (r *linRun) require(op lincheck.Op, outcome lincheck.Outcome, output string) {
	r.t.Helper()
	if op.Outcome != outcome || (outcome == lincheck.OK && op.Kind == lincheck.Get && string(op.Output) != output) {
		r.fail("op %s: want %s %q", op, outcome, output)
	}
}

// --- healthy cluster ---

// TestRealSequentialBaselineMatchesTheModel is scenario A on real processes:
// one client, PUT/GET/PUT(empty)/GET/DELETE/GET/DELETE, checked by the checker
// AND diffed op by op against the sequential reference model.
func TestRealSequentialBaselineMatchesTheModel(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	cl := r.client("c1", 10*time.Second, 4)
	ctx := context.Background()
	r.require(cl.Put(ctx, "k", []byte("A")), lincheck.OK, "")
	r.require(cl.Get(ctx, "k"), lincheck.OK, "A")
	r.require(cl.Put(ctx, "k", []byte{}), lincheck.OK, "")
	r.require(cl.Get(ctx, "k"), lincheck.OK, "")
	r.require(cl.Delete(ctx, "k"), lincheck.OK, "")
	r.require(cl.Get(ctx, "k"), lincheck.NotFound, "")
	r.require(cl.Delete(ctx, "k"), lincheck.OK, "")
	h := r.rec.History()
	if _, bad := lincheck.Sequential(h.Ops); bad != -1 {
		r.fail("the reference model contradicts op %s", h.Ops[bad])
	}
	r.check()
	c.finish()
}

// TestRealConcurrentClientsAreLinearizable: 2, 4 and 8 concurrent clients on
// shared keys against a healthy 3-process group.
func TestRealConcurrentClientsAreLinearizable(t *testing.T) {
	for _, clients := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("%d-clients", clients), func(t *testing.T) {
			c := newRCluster(t, 3)
			c.waitLeader(c.ids, 0, 20*time.Second)
			c.waitClientReady(20 * time.Second)
			r := newLinRun(t, c)
			r.workload(workload.Options{Clients: clients, OpsPerClient: 60, Keys: 3, Timeout: 10 * time.Second, Seed: int64(clients), GetPct: 40, DeletePct: 10}, nil)
			r.check()
			c.finish()
		})
	}
}

// TestRealSameKeyWritesReadsAndDeletes: eight clients on ONE key — concurrent
// same-key writes, reads overlapping writes, deletes racing puts. Every read
// must be explained by one order of the writes around it.
func TestRealSameKeyWritesReadsAndDeletes(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	st := r.workload(workload.Options{Clients: 8, OpsPerClient: 50, Keys: 1, Timeout: 10 * time.Second, Seed: 77, GetPct: 40, DeletePct: 25}, nil)
	res := r.check()
	if st.OK < 200 || res.Stats.Keys != 1 {
		r.fail("too little same-key contention to mean anything: %s", st)
	}
}

// TestRealCompletedWriteIsSeenByEveryLaterRead is the completed-write →
// later-read rule asserted directly, not only through the checker: after
// PUT(k, v) returns OK, a GET invoked afterwards — by another client, sent
// first to EACH node in turn, followers included — returns v. A follower
// never answers with its own state: it forwards the read to the leader
// (Phase 13), which serves it through ReadIndex, and the answer records both.
func TestRealCompletedWriteIsSeenByEveryLaterRead(t *testing.T) {
	c := newRCluster(t, 3)
	l, _ := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	w := r.client("writer", 10*time.Second, 4)
	rd := r.client("reader", 10*time.Second, 4)
	ctx := context.Background()
	redirected := 0
	for i := 0; i < 5; i++ {
		v := fmt.Sprintf("v%d", i)
		w.Prefer(l)
		r.require(w.Put(ctx, "k", []byte(v)), lincheck.OK, "")
		for _, id := range c.ids {
			rd.Prefer(id)
			op := rd.Get(ctx, "k")
			r.require(op, lincheck.OK, v)
			// Whoever first received the read either served it (it led),
			// forwarded it to the leader that did, or refused it — a node that
			// did not serve never answers with its own state.
			if first := op.Attempts[0]; first.Node != op.Node {
				if !strings.HasPrefix(first.Result, "not-leader") && !strings.HasSuffix(first.Result, "via "+first.Node) {
					r.fail("%s did not serve the read yet neither forwarded nor refused it: %+v", first.Node, op.Attempts)
				}
				redirected++
			}
		}
	}
	if redirected < 5 {
		r.fail("only %d of 15 reads were sent to a follower and served by the leader; the scenario did not exercise followers", redirected)
	}
	r.check()
	c.finish()
}

// --- reads across leadership changes: the stale-read attacks ---

// TestRealStaleLeaderNeverServesARead: the leader L1 completes PUT(A) and is
// then cut off from its peers at the proxies — its client port stays
// reachable and it still believes it leads (it hears no higher term). The
// others elect L2, which completes PUT(B). A GET sent to L1 must NOT return A:
// L1 registers a ReadIndex it can never confirm (no quorum can acknowledge a
// heartbeat it sends after the read), so the client's deadline expires and
// the read is recorded Incomplete. After the partition heals, L1 learns the
// higher term, and a GET sent to it is refused (not leader) and redirected to
// the leader, which returns B. With a read path that skipped ReadIndex — a
// local read on the leader that believes it leads — the GET would return A
// after PUT(B) completed, and the checker would reject the history (the
// Phase 12 mutant "Server.Get bypasses ReadIndex" dies here).
func TestRealStaleLeaderNeverServesARead(t *testing.T) {
	withPremise(t, func() {
		c := newRCluster(t, 3)
		c.waitLeader(c.ids, 0, 20*time.Second)
		c.waitClientReady(20 * time.Second)
		r := newLinRun(t, c)
		ctx := context.Background()
		w := r.client("writer", 10*time.Second, 4)
		r.require(w.Put(ctx, "k", []byte("A")), lincheck.OK, "")
		l1, t1 := c.waitStable(c.ids, 0, 30*time.Second)

		r.event("isolate %s (leader of term %d)", l1, t1)
		c.isolate(l1)
		l2, t2 := c.waitLeader(others(c.ids, l1), t1, 20*time.Second)
		r.event("%s leads term %d", l2, t2)
		w.Prefer(l2)
		r.require(w.Put(ctx, "k", []byte("B")), lincheck.OK, "")

		// The read on the stale leader: one attempt, no redirect, a short deadline.
		stale := r.client("stale-reader", 2*time.Second, 1)
		stale.Prefer(l1)
		op := stale.Get(ctx, "k")
		// Cut off, l1 can learn nothing: a "not leader" answer means it had been
		// deposed BEFORE the cut — the attempt's premise, not a verdict.
		r.premise(!refusedAsNotLeader(op), "%s had been deposed before it was cut off: %s", l1, op)
		// With forwarding, a node deposed before the cut would forward the read
		// rather than refuse it; its own output settles whether it still
		// believed it led term t1 (a node cut off learns no higher term).
		r.premise(!c.leftTerm(l1, t1), "%s had left term %d before the read: %s", l1, t1, op)
		if op.Outcome == lincheck.OK || op.Outcome == lincheck.NotFound {
			r.fail("the isolated leader %s SERVED a read after %s completed PUT(B) in term %d: %s", l1, l2, t2, op)
		}
		if op.Outcome != lincheck.Incomplete {
			r.fail("the isolated leader %s cannot know it was deposed, so its read can only time out: %s", l1, op)
		}

		r.event("heal")
		c.healAll()
		c.waitFollows(l1, l2, t2, 20*time.Second)
		rd := r.client("reader", 10*time.Second, 4)
		rd.Prefer(l1)
		op = rd.Get(ctx, "k")
		r.require(op, lincheck.OK, "B")
		// The deposed leader never serves the read itself: it forwards it to
		// the leader (Phase 13) — or, in redirect-only mode, refuses it.
		if op.Attempts[0].Node != l1 || op.Node == l1 ||
			!(strings.HasPrefix(op.Attempts[0].Result, "not-leader") || strings.HasSuffix(op.Attempts[0].Result, "via "+l1)) {
			r.fail("the deposed leader %s must not serve the read itself: %+v, served by %s", l1, op.Attempts, op.Node)
		}
		r.check()
		c.waitStable(c.ids, 0, 30*time.Second)
		c.finish()
	})
}

// TestRealMinorityLeaderWithAFollowerNeverServesARead is the stale-leader attack
// at its sharpest, on five real processes: the group splits {L1, F} | {the
// other three} while L1 leads. F never hears of the new term, so it keeps
// acknowledging L1's heartbeats — L1 receives fresh acknowledgements, in its
// own term, sent after the read, from a live follower. Two of five is still
// not a quorum: the read must time out, never return A after the majority's
// leader completed PUT(B). (Isolating L1 completely, as the test above does,
// would leave it with no acknowledgements at all — a weaker attack that a
// broken quorum rule survives; this one it does not.)
func TestRealMinorityLeaderWithAFollowerNeverServesARead(t *testing.T) {
	withPremise(t, func() {
		c := newRCluster(t, 5)
		c.waitLeader(c.ids, 0, 20*time.Second)
		c.waitClientReady(20 * time.Second)
		r := newLinRun(t, c)
		ctx := context.Background()
		w := r.client("writer", 10*time.Second, 6)
		r.require(w.Put(ctx, "k", []byte("A")), lincheck.OK, "")
		l1, t1 := c.waitStable(c.ids, 0, 30*time.Second)
		c.waitCommit(c.ids, c.commitOf(l1), 20*time.Second)

		f := others(c.ids, l1)[0]
		minority, majority := []string{l1, f}, others(others(c.ids, l1), f)
		r.event("split %v | %v (leader %s of term %d)", minority, majority, l1, t1)
		c.split(minority, majority)
		l2, t2 := c.waitLeader(majority, t1, 20*time.Second)
		r.event("%s leads term %d", l2, t2)
		w.Prefer(l2)
		r.require(w.Put(ctx, "k", []byte("B")), lincheck.OK, "")

		stale := r.client("stale-reader", 3*time.Second, 1)
		stale.Prefer(l1)
		op := stale.Get(ctx, "k")
		// Premises, checked after the fact: l1 still led when the read arrived (a
		// "not leader" answer proves it had been deposed before the split, or by
		// its own follower campaigning), and the follower stayed in l1's term.
		r.premise(!refusedAsNotLeader(op), "%s did not lead when the read arrived: %s", l1, op)
		r.premise(!c.leftTerm(l1, t1), "%s had left term %d before the read: %s", l1, t1, op)
		for _, m := range reFollower.FindAllStringSubmatch(c.procs[f].out.String(), -1) {
			tm, _ := strconv.ParseUint(m[2], 10, 64)
			r.premise(tm < t2, "%s left the minority leader's term before the read (it followed %s in term %d)", f, m[3], tm)
		}
		if op.Outcome != lincheck.Incomplete {
			r.fail("the minority leader %s (with follower %s) answered a read after %s completed PUT(B) in term %d: %s", l1, f, l2, t2, op)
		}

		r.event("heal")
		c.healAll()
		c.waitFollows(l1, l2, t2, 20*time.Second)
		rd := r.client("reader", 10*time.Second, 6)
		rd.Prefer(l1)
		r.require(rd.Get(ctx, "k"), lincheck.OK, "B")
		r.check()
		c.waitStable(c.ids, 0, 30*time.Second)
		c.finish()
	})
}

// TestRealReadsAcrossLeaderChanges: GET, then a leadership change, then GET —
// by leader crash (SIGKILL) and by leader partition. After each change the
// next read, by a different client, must return the latest completed write,
// never an older value (the new leader's read index is at least its election
// no-op, which covers every entry committed before it).
func TestRealReadsAcrossLeaderChanges(t *testing.T) {
	for _, how := range []string{"crash", "partition"} {
		t.Run(how, func(t *testing.T) {
			c := newRCluster(t, 3)
			l, tm := c.waitLeader(c.ids, 0, 20*time.Second)
			c.waitClientReady(20 * time.Second)
			r := newLinRun(t, c)
			ctx := context.Background()
			w := r.client("writer", 10*time.Second, 6)
			for round := 0; round < 2; round++ {
				v := fmt.Sprintf("%s-%d", how, round)
				w.Prefer(l)
				r.require(w.Put(ctx, "k", []byte(v)), lincheck.OK, "")
				before := r.client(fmt.Sprintf("before%d", round), 10*time.Second, 6)
				before.Prefer(l)
				r.require(before.Get(ctx, "k"), lincheck.OK, v)
				if how == "crash" {
					r.event("kill %s (leader of term %d)", l, tm)
					c.kill(l)
				} else {
					r.event("isolate %s (leader of term %d)", l, tm)
					c.isolate(l)
				}
				rest := others(c.ids, l)
				l2, t2 := c.waitLeader(rest, tm, 20*time.Second)
				r.event("%s leads term %d", l2, t2)
				// A fresh client, starting at a survivor that may or may not
				// lead: it must see v, never anything older.
				after := r.client(fmt.Sprintf("after%d", round), 10*time.Second, 6)
				after.Prefer(rest[0])
				r.require(after.Get(ctx, "k"), lincheck.OK, v)
				if how == "crash" {
					c.start(l)
					waitForLine(t, c.procs[l], "event=client_ready", 20*time.Second)
					r.event("restarted %s", l)
				} else {
					c.healAll()
					r.event("heal")
				}
				l, tm = c.waitStable(c.ids, t2-1, 30*time.Second)
				r.event("stable: %s leads term %d", l, tm)
			}
			r.check()
			c.finish()
		})
	}
}

// --- concurrent workloads under faults ---

// faultWorkload is the common shape: a concurrent workload, and a fault
// schedule synchronized on client progress.
func faultWorkload(seed int64) workload.Options {
	return workload.Options{Clients: 6, OpsPerClient: 1 << 20, Keys: 3, Timeout: 2 * time.Second, Seed: seed, GetPct: 40, DeletePct: 10, MaxAttempts: 6, Backoff: 20 * time.Millisecond}
}

// TestRealLeaderKilledDuringWorkload: SIGKILL the leader while six clients run;
// the survivors elect; the old leader restarts on its data directory (a fresh
// state machine rebuilt by replaying its committed prefix) and rejoins.
func TestRealLeaderKilledDuringWorkload(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	st := r.workload(faultWorkload(21), func() {
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
	})
	r.check()
	if st.Unavailable+st.Unknown+st.Redirects == 0 {
		r.fail("the kill did not intersect the workload: %s", st)
	}
	c.finish()
}

// TestRealFollowerKilledDuringWorkload: a follower dies while six clients run
// and the leader keeps serving with the one follower left (a quorum of two):
// operations are served during the downtime window by construction — the
// restart waits for them. The follower restarts on its log with an empty state
// machine, catches up by replay, and a read sent to it afterwards is refused
// (not leader) and redirected — it never answers from its rebuilt store.
func TestRealFollowerKilledDuringWorkload(t *testing.T) {
	withPremise(t, func() {
		c := newRCluster(t, 3)
		c.waitLeader(c.ids, 0, 20*time.Second)
		c.waitClientReady(20 * time.Second)
		r := newLinRun(t, c)
		r.workload(faultWorkload(31), func() {
			r.waitServed(60, 60*time.Second)
			l, _ := c.waitStable(c.ids, 0, 30*time.Second)
			f := others(c.ids, l)[0]
			r.event("kill follower %s", f)
			c.kill(f)
			r.waitServed(60, 60*time.Second) // served with f down
			c.start(f)
			waitForLine(t, c.procs[f], "event=client_ready", 20*time.Second)
			r.event("restarted %s", f)
			l2, _ := c.waitStable(c.ids, 0, 30*time.Second)
			// The restarted node may legitimately win an election; the read
			// below is about a restarted FOLLOWER.
			r.premise(l2 != f, "the restarted %s became the leader", f)
			rd := r.client("follower-reader", 10*time.Second, 6)
			rd.Prefer(f)
			op := rd.Get(context.Background(), "k0")
			if op.Outcome != lincheck.OK && op.Outcome != lincheck.NotFound {
				r.fail("read redirected from the restarted follower: %s", op)
			}
			if first := op.Attempts[0]; first.Node != f || op.Node == f ||
				!(strings.HasPrefix(first.Result, "not-leader") || strings.HasSuffix(first.Result, "via "+f)) {
				// f served it itself only if it had become leader since
				// waitStable: a premise, not a verdict.
				r.premise(op.Node != f, "%s became the leader before the read arrived", f)
				r.fail("the restarted follower %s must not serve the read itself (refuse, or forward to the leader): %+v", f, op.Attempts)
			}
			r.waitServed(40, 60*time.Second)
		})
		r.check()
		c.finish()
	})
}

// TestRealLeaderPartitionedDuringWorkload: the leader's links are cut while six
// clients run. It keeps its client port and believes it leads: its reads can
// never confirm and its writes can never commit, so clients there see
// deadlines (unknown) — never a value. The majority elects and serves. Then
// the partition heals, the old leader's uncommitted entries are overwritten
// (their writers, if still waiting, learn they were lost), and the workload
// continues across the heal.
func TestRealLeaderPartitionedDuringWorkload(t *testing.T) {
	withPremise(t, func() {
		c := newRCluster(t, 3)
		c.waitLeader(c.ids, 0, 20*time.Second)
		c.waitClientReady(20 * time.Second)
		r := newLinRun(t, c)
		st := r.workload(faultWorkload(41), func() {
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
		})
		r.check() // every attempt's history is checked, whatever its premise
		// Non-vacuity is this scenario's premise: if the leader had been deposed
		// before the cut, no client waits on a stale leader and nothing was tested.
		r.premise(st.Unknown > 0, "no client ever waited on the isolated leader: %s", st)
		c.finish()
	})
}

// TestRealLeaderChangesWithoutCrashes: leadership moves repeatedly — the leader
// is isolated until the others elect, then healed at once — so clients meet
// deposed leaders that are alive and reachable, not dead ones.
func TestRealLeaderChangesWithoutCrashes(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	st := r.workload(faultWorkload(61), func() {
		r.waitServed(40, 60*time.Second)
		for i := 0; i < 3; i++ {
			l, tm := c.waitStable(c.ids, 0, 30*time.Second)
			r.event("isolate leader %s (term %d)", l, tm)
			c.isolate(l)
			l2, t2 := c.waitLeader(others(c.ids, l), tm, 20*time.Second)
			c.healAll()
			r.event("heal; %s leads term %d", l2, t2)
			r.waitServed(40, 60*time.Second)
		}
	})
	r.check()
	// A client meets a deposed leader as a redirect, a forwarded answer (a
	// deposed leader forwards to the new one, Phase 13) or a lost write.
	if st.Redirects+st.Forwarded+st.Lost == 0 {
		r.fail("no client ever met a deposed leader: %s", st)
	}
	c.finish()
}

// TestRealRollingRestartDuringWorkload: every process in turn is SIGKILLed and
// restarted while clients run — repeated leader changes, catch-up by replay.
func TestRealRollingRestartDuringWorkload(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	st := r.workload(faultWorkload(51), func() {
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
	})
	r.check()
	if st.Unavailable+st.Unknown == 0 {
		r.fail("the restarts did not intersect the workload: %s", st)
	}
	c.finish()
}

// --- write completion: a SIGKILL at every point of one write's life ---

// entryHolding returns the index of the entry in a durable log whose command is
// PUT(key, value), or 0.
func entryHolding(rec *raftlog.Recovered, key, value string) uint64 {
	for _, e := range rec.Entries {
		if cmd, err := kv.Decode(e.Data); err == nil && cmd.Op == kv.OpPut && string(cmd.Key) == key && string(cmd.Value) == value {
			return e.Index
		}
	}
	return 0
}

// armCrash sends the crash seam's arming signal to a node and waits for it to
// confirm: from here on, the crash point counts occurrences.
func (c *rcluster) armCrash(id string) {
	c.t.Helper()
	c.signal(id, syscall.SIGUSR1)
	waitForLine(c.t, c.procs[id], "event=crash_armed", 10*time.Second)
}

// TestRealWriteCrashWindows kills the leader with SIGKILL at each point of one
// PUT's life on a real process — before its entry is persisted, after (only
// the leader has it), after the commit is persisted, before and after it is
// applied, before the response is written and after — and establishes, per
// point, the two facts Phase 12 keeps separate: what the CLIENT knows (its
// history) and what the SYSTEM did (the durable log the SIGKILL left behind,
// inspected to prove the point's premise rather than assume it). Then a new
// leader is elected, the victim restarts on its log, and a fresh client reads
// the key through ReadIndex:
//
//	point            client hears   entry on victim's disk   later read
//	before-save      nothing        absent                   old (never left the leader)
//	after-save:1     nothing        present, uncommitted     old or new (whoever wins next)
//	after-save:2     nothing        present, committed       new (committed ⇒ durable)
//	before-apply     nothing        present, committed       new
//	after-apply      nothing        present, committed       new
//	after-applied-to nothing        present, committed       new
//	before-reply     nothing        present, committed       new
//	after-reply      OK (or not)    present, committed       new
//
// "Nothing" is recorded as an Incomplete operation: the checker may place it
// or omit it — and a committed write the client never heard of is still
// committed, which the explicit later-read assertion requires. The history is
// checked too.
func TestRealWriteCrashWindows(t *testing.T) {
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
			withPremise(t, func() { crashWindow(t, tc) })
		})
	}
}

// crashCase is one row of TestRealWriteCrashWindows.
type crashCase struct {
	spec      string
	inLog     bool   // the entry must be on the victim's disk
	committed string // "yes", "no", "any": the victim's durable commit covers it
	read      string // "new", "old", "any"
}

// crashWindow is one attempt at one row of TestRealWriteCrashWindows.
func crashWindow(t *testing.T, tc crashCase) {
	c := newRClusterArgs(t, 3, "-crash-at", tc.spec, "-crash-armed-by-signal")
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	ctx := context.Background()
	setup := r.client("setup", 10*time.Second, 4)
	r.require(setup.Put(ctx, "k", []byte("old")), lincheck.OK, "")
	// Quiesce: every node has applied "old", so after arming the only
	// Save, apply and reply the leader performs are the next write's.
	l, tm := c.waitStable(c.ids, 0, 30*time.Second)
	c.waitCommit(c.ids, c.commitOf(l), 20*time.Second)

	c.armCrash(l)
	r.event("armed %s at %s on leader %s (term %d)", tc.spec, tc.spec, l, tm)
	w := r.client("writer", 10*time.Second, 1) // one attempt: no retry, no redirect
	w.Prefer(l)
	op := w.Put(ctx, "k", []byte("new"))
	r.premise(!refusedAsNotLeader(op), "the armed node %s had been deposed before the write arrived: %s", l, op)
	point, nth := c.waitKilledAtPoint(l, 30*time.Second)
	r.event("%s died at %s#%d", l, point, nth)
	if want := strings.SplitN(tc.spec, ":", 2)[0]; point != want {
		r.fail("died at %s, want %s", point, want)
	}
	switch {
	case tc.spec == "after-reply:1" && op.Outcome == lincheck.OK:
		t.Logf("the response left before the SIGKILL: the client knows")
	case op.Outcome != lincheck.Incomplete:
		r.fail("the writer must hear nothing from a leader that died at %s: %s", tc.spec, op)
	}

	// What the system did: the victim's durable log.
	held, err := raftlog.Inspect(filepath.Join(c.dirs[l], "raft-"+l+".log"))
	if err != nil {
		r.fail("inspect %s: %v", l, err)
	}
	// The crash point belongs to this write only if no election
	// intervened between arming and the crash.
	r.premise(held.HardState.Term == tm, "%s's term moved from %d to %d between arming and the crash: the point may belong to another operation", l, tm, held.HardState.Term)
	idx := entryHolding(held, "k", "new")
	switch {
	case tc.inLog && idx == 0:
		r.fail("premise: at %s the entry must be on the leader's disk; it holds %d entries, commit %d", tc.spec, len(held.Entries), held.HardState.Commit)
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
	c.start(l)
	waitForLine(t, c.procs[l], "event=client_ready", 20*time.Second)
	r.event("restarted %s", l)
	c.waitStable(c.ids, 0, 30*time.Second)
	rd := r.client("reader", 10*time.Second, 6)
	got := rd.Get(ctx, "k")
	if got.Outcome != lincheck.OK {
		r.fail("read after recovery: %s", got)
	}
	switch saw := string(got.Output); {
	case tc.read == "new" && saw != "new":
		r.fail("the write was committed when the leader died at %s, but a later read returned %q: a committed write was lost", tc.spec, saw)
	case tc.read == "old" && saw != "old":
		r.fail("the write never left the leader at %s, but a later read returned %q", tc.spec, saw)
	default:
		t.Logf("crash at %s: client outcome %s; entry %d on disk (durable commit %d); later read %q", tc.spec, op.Outcome, idx, held.HardState.Commit, saw)
	}
	r.check()
	c.finish()
}

// --- incomplete operations and retries ---

// TestRealIncompleteWriteThenRetry separates "the server committed it" from
// "the client knows it", and shows what a retry means without deduplication.
// c1's PUT(A) is committed and applied on the leader, which is then SIGKILLed
// before replying: c1 records an Incomplete op. A reader then sees A (it IS
// committed — the later-read assertion is explicit). c2 completes PUT(B); the
// reader sees B. c1 now RETRIES PUT(A) — as a new operation, as the Phase 12
// contract requires — which applies A a second time; the reader sees A again.
//
// Recorded honestly (two operations for c1) the history is linearizable: the
// first attempt before the first read, the retry after B. Collapsed into ONE
// logical PUT(A) — what a client library hiding its retries would report —
// the same events are NOT linearizable, and the checker says so. That is the
// precise gap Phase 13's request ids and deduplication close: with them the
// retry would be recognized, not re-applied, and the single-operation view
// would become true.
func TestRealIncompleteWriteThenRetry(t *testing.T) {
	withPremise(t, func() {
		c := newRClusterArgs(t, 3, "-crash-at", "after-applied-to:1", "-crash-armed-by-signal")
		c.waitLeader(c.ids, 0, 20*time.Second)
		c.waitClientReady(20 * time.Second)
		r := newLinRun(t, c)
		ctx := context.Background()
		l, tm := c.waitStable(c.ids, 0, 30*time.Second)
		c.waitCommit(c.ids, c.commitOf(l), 20*time.Second)
		c.armCrash(l)
		c1 := r.client("c1", 10*time.Second, 1)
		c1.Prefer(l)
		first := c1.Put(ctx, "k", []byte("A"))
		r.premise(!refusedAsNotLeader(first), "the armed node %s had been deposed before the write arrived: %s", l, first)
		c.waitKilledAtPoint(l, 30*time.Second)
		held, err := raftlog.Inspect(filepath.Join(c.dirs[l], "raft-"+l+".log"))
		if err != nil {
			r.fail("inspect %s: %v", l, err)
		}
		r.premise(held.HardState.Term == tm, "%s's term moved from %d to %d between arming and the crash", l, tm, held.HardState.Term)
		r.event("%s died after applying c1's PUT(A), before replying", l)
		if first.Outcome != lincheck.Incomplete {
			r.fail("c1 must hear nothing: %s", first)
		}
		l2, t2 := c.waitLeader(others(c.ids, l), tm, 20*time.Second)
		r.event("%s leads term %d", l2, t2)
		reader := r.client("reader", 10*time.Second, 6)
		r.require(reader.Get(ctx, "k"), lincheck.OK, "A") // committed, though c1 does not know
		c2 := r.client("c2", 10*time.Second, 6)
		r.require(c2.Put(ctx, "k", []byte("B")), lincheck.OK, "")
		r.require(reader.Get(ctx, "k"), lincheck.OK, "B")
		c1 = r.client("c1", 10*time.Second, 6) // the same client, retrying
		retry := c1.Put(ctx, "k", []byte("A"))
		r.require(retry, lincheck.OK, "")
		r.require(reader.Get(ctx, "k"), lincheck.OK, "A")
		r.check()

		// The same history with c1's attempts collapsed into one operation.
		h := r.rec.History()
		var collapsed lincheck.History
		for _, op := range h.Ops {
			switch op.ID {
			case first.ID:
				op.Outcome, op.Complete, op.Node, op.Term, op.Index = lincheck.OK, retry.Complete, retry.Node, retry.Term, retry.Index
			case retry.ID:
				continue
			}
			collapsed.Ops = append(collapsed.Ops, op)
		}
		if res := lincheck.Check(collapsed, lincheck.Options{Minimize: true}); res.OK {
			r.fail("collapsing an unknown write and its retry into one operation must NOT be linearizable here:\n%s", collapsed)
		} else {
			t.Logf("collapsed into one PUT(A), the history is rejected, as it must be:\n%s", res.Reason)
		}
		c.start(l)
		waitForLine(t, c.procs[l], "event=client_ready", 20*time.Second)
		c.waitStable(c.ids, 0, 30*time.Second)
		c.finish()
	})
}

// TestRealWriteToPartitionedLeaderNeverTakesEffect:
//
//	C1 ---- PUT(new) ----X      (its leader is cut off: the entry cannot commit)
//	C2 ---- GET -------------   (at the new leader: old)
//	heal; C3 ---- GET --------   (old: the entry never left the old leader, so
//	                              the new leader's log overwrote it)
//
// C1's operation is Incomplete (its deadline passed). The checker would allow
// it to take effect later; the explicit assertion is stronger and specific to
// this schedule: the entry never reached another node, so it never commits.
func TestRealWriteToPartitionedLeaderNeverTakesEffect(t *testing.T) {
	withPremise(t, func() {
		c := newRCluster(t, 3)
		c.waitLeader(c.ids, 0, 20*time.Second)
		c.waitClientReady(20 * time.Second)
		r := newLinRun(t, c)
		ctx := context.Background()
		setup := r.client("setup", 10*time.Second, 4)
		r.require(setup.Put(ctx, "k", []byte("old")), lincheck.OK, "")
		l, tm := c.waitStable(c.ids, 0, 30*time.Second)
		c.waitCommit(c.ids, c.commitOf(l), 20*time.Second)

		r.event("isolate %s (leader of term %d)", l, tm)
		c.isolate(l)
		c1 := r.client("c1", 2*time.Second, 1)
		c1.Prefer(l)
		op := c1.Put(ctx, "k", []byte("new"))
		r.premise(!refusedAsNotLeader(op), "%s had been deposed before it was cut off: %s", l, op)
		if op.Outcome != lincheck.Incomplete {
			r.fail("a write to an isolated leader cannot complete: %s", op)
		}
		if entryHolding(c.liveLog(l), "k", "new") == 0 {
			r.fail("premise: the isolated leader must have appended the write")
		}
		l2, t2 := c.waitLeader(others(c.ids, l), tm, 20*time.Second)
		r.event("%s leads term %d", l2, t2)
		c2 := r.client("c2", 10*time.Second, 6)
		c2.Prefer(l2)
		r.require(c2.Get(ctx, "k"), lincheck.OK, "old")

		r.event("heal")
		c.healAll()
		c.waitFollows(l, l2, t2, 20*time.Second)
		c3 := r.client("c3", 10*time.Second, 6)
		c3.Prefer(l)
		r.require(c3.Get(ctx, "k"), lincheck.OK, "old")
		c.waitCommit(c.ids, c.commitOf(l2), 20*time.Second)
		if idx := entryHolding(c.liveLog(l), "k", "new"); idx != 0 {
			r.fail("the old leader's uncommitted entry survived the new leader's log at index %d", idx)
		}
		r.check()
		c.finish()
	})
}

// TestRealMalformedAndAbandonedRequestsDoNotCorruptTheHistory: the wire protocol
// facts that could affect client-visible semantics, on a real process — a
// request frame that is not the protocol closes that connection only (other
// clients are unaffected), and a client that disconnects mid-request leaves
// its operation Incomplete without the server ever answering it on another
// connection. The concurrent workload around them must stay linearizable.
func TestRealMalformedAndAbandonedRequestsDoNotCorruptTheHistory(t *testing.T) {
	c := newRCluster(t, 3)
	l, _ := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	r := newLinRun(t, c)
	st := r.workload(workload.Options{Clients: 4, OpsPerClient: 1 << 20, Keys: 2, Timeout: 5 * time.Second, Seed: 81, GetPct: 40, DeletePct: 10, MaxAttempts: 4}, func() {
		r.waitServed(30, 60*time.Second)
		// Garbage, then a truncated frame, on raw connections.
		for _, junk := range [][]byte{[]byte("GET / HTTP/1.1\r\n\r\n"), {0x01, 0x02}} {
			conn, err := net.DialTimeout("tcp", c.kvAddrs[l], 3*time.Second)
			if err != nil {
				r.fail("dial: %v", err)
			}
			_, _ = conn.Write(junk)
			_ = conn.Close()
		}
		r.event("sent malformed frames to %s", l)
		// An abandoned request: the client gives up at once.
		ab := r.client("abandoner", time.Millisecond, 1)
		ab.Prefer(l)
		op := ab.Put(context.Background(), "k0", []byte("abandoned"))
		r.event("abandoned request: %s", op)
		if op.Outcome == lincheck.Rejected {
			r.fail("an abandoned request is unknown, never a definite rejection: %s", op)
		}
		r.waitServed(60, 60*time.Second)
	})
	r.check()
	if st.OK == 0 {
		r.fail("workload made no progress: %s", st)
	}
	c.finish()
}

// TestWithPremiseRetriesOnlyThePremise pins the harness rule the real-timing
// scenarios rely on: an attempt whose premise fails is abandoned and started
// over (at most three times), an attempt that completes ends the loop, a
// premise that never holds is an error (never a pass), and any other panic is
// not swallowed.
func TestWithPremiseRetriesOnlyThePremise(t *testing.T) {
	r := &linRun{t: t, c: &rcluster{t: t}, rec: lincheck.NewRecorder()}
	attempts := 0
	if err := runWithPremise(func() {
		attempts++
		r.premise(attempts >= 3, "attempt %d: the leader was deposed", attempts)
	}, t.Logf); err != nil || attempts != 3 {
		t.Fatalf("ran %d attempts (%v), want 3: two premise failures, then success", attempts, err)
	}
	attempts = 0
	if err := runWithPremise(func() {
		attempts++
		r.premise(false, "never holds")
	}, t.Logf); err == nil || attempts != maxPremiseAttempts {
		t.Fatalf("a premise that never holds must be an error after %d attempts: %v after %d", maxPremiseAttempts, err, attempts)
	}
	defer func() {
		if v := recover(); v != "boom" {
			t.Fatalf("a panic that is not a premise must propagate, got %v", v)
		}
	}()
	_ = runWithPremise(func() { panic("boom") }, t.Logf)
	t.Fatal("unreachable: the panic must propagate")
}
