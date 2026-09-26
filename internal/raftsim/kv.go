package raftsim

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
)

// The deterministic client tier (Phase 12, docs/LINEARIZABILITY.md §8).
//
// Every node's state machine is a kv.Store — the same code dkvd runs — and
// client operations are events in the script: KVPut, KVGet and KVDelete
// invoke an operation on a node, KVTimeout makes a client give up on the one it
// is waiting for. A write is proposed through the core and completes through
// the driver's own raftnode.Waiters (success only when applied in its term,
// ErrLost when a different entry is applied at its index); a read registers a
// ReadIndex with the core, is confirmed through the driver's own
// raftnode.Reads as the Ready's ReadStates are drained by the real
// DrainReadyAt, and is served from the node's store once applied through it.
// Nothing about completion is re-implemented here, so what this tier checks is
// the code the real node runs, under a simulated clock and network.
//
// Every invocation, attempt and completion is recorded, in event order, into a
// lincheck.Recorder: the run's client-visible history, which is checked for
// linearizability after the run — and, independently of that checker, three
// implementation invariants are checked at the instant each operation
// completes (INV-X5..X7, docs/INVARIANTS.md). A run with clients is still a
// pure function of (Config, script), so a non-linearizable history replays
// exactly and its script can be minimized.
//
// A client has at most one operation outstanding (a real client that is still
// waiting cannot issue its next request). A request to a node that is down is
// a definite rejection (connection refused: nothing was sent). A request to a
// paused node stalls until the client times out (the request sits unread in a
// frozen process). A not-leader answer is followed by one redirect to the
// leader it names, as the Phase 12 workload does, and every attempt is
// recorded. A write whose node dies before answering is Incomplete: it may
// still commit. Nothing is ever retried after an unknown outcome.

// maxKVAttempts bounds one send's redirects.
const maxKVAttempts = 3

// KVStats counts client-visible outcomes.
type KVStats struct {
	Ops, OK, NotFound, Rejected, Incomplete int
	Redirects, Lost, Unavailable, Stalled   int
	ReadsServed, WritesAcked                int
	// Phase 13 sessions. Resolved counts session requests that had an
	// unanswered send and still ended with a definite OK; ResolvedDup those
	// among them whose answer was a duplicate — an unanswered send had
	// executed, and the retry was deduplicated against it.
	Registered, Retries, DupSends, Duplicates, Expired int
	Resolved, ResolvedDup                              int
}

// simClient is one client. An anonymous client (session 0) has Phase 12
// semantics: one send per operation, and an unknown outcome ends the operation.
// A session client (Phase 13, after kvregister) keeps its open logical request
// across timeouts, crashes and lost entries until a definite answer: kvretry
// re-sends it with the same identity, kvdup sends a concurrent duplicate, and
// every send is an attempt of the one recorded operation.
type simClient struct {
	name    string
	session uint64 // 0: anonymous
	nextRID uint64

	op       int // recorder op id of the open logical request; 0 = none
	register bool
	kind     lincheck.Kind
	key      string
	value    []byte
	rid      uint64
	unknown  bool // some send of the open request went unanswered
	sends    []*simSend
}

// simSend is one outstanding send of a client's open request.
type simSend struct {
	node    *node
	inc     int // the incarnation of node that holds it
	attempt int // recorder attempt index (-1 for REGISTER, which is not recorded)
	stalled bool
	write   <-chan raftnode.Outcome
	read    <-chan raftnode.Outcome
	index   uint64 // the write's entry index
	term    uint64 // the term the write was proposed in / the read registered in
	data    []byte // the write's encoded command
	// regCommitted is the highest index committed anywhere in the cluster when
	// the read was registered: INV-X7 requires the read to be served at or
	// above it.
	regCommitted uint64
}

func (cl *simClient) busy() bool { return cl.op != 0 || cl.register }

// kvState is the cluster's client-side state.
type kvState struct {
	rec     *lincheck.Recorder
	clients map[string]*simClient
	stats   KVStats
	// LocalReads is a deliberately BROKEN read path for tests of this tier
	// itself: a Get is served from the contacted node's store at once, with no
	// ReadIndex and no leadership check. It must produce non-linearizable
	// histories under faults — proof that the tier can see a stale read.
	localReads bool
	// freshRetryIDs is a deliberately BROKEN session client for tests of this
	// tier: a retry is sent under a NEW request id, so the server cannot
	// deduplicate it against an unanswered send that executed. Every replica
	// and the session model agree on every decision (each id is new), so only
	// the linearizability check over logical operations can see the double
	// execution — which it must.
	freshRetryIDs bool

	// The request-identity reference (Phase 13): the session model fed with
	// the committed log in index order, and its decision for every index —
	// every replica's store must decide the same, at the instant it applies
	// (INV-X11), and no identity may execute twice (INV-X2).
	model     *lincheck.SessionModel
	decisions []decision
	executed  map[[2]uint64]uint64 // identity → the index where it executed
}

type decision struct {
	data  string
	d     lincheck.Decision // 0: not a key-value command
	index uint64
}

func (c *Cluster) kvInit() {
	l := c.kvLimits()
	c.kv = &kvState{rec: lincheck.NewRecorder(), clients: map[string]*simClient{},
		model:    lincheck.NewSessionModel(lincheck.SessionLimits{MaxSessions: l.MaxSessions, MaxUnacked: l.MaxUnacked}),
		executed: map[[2]uint64]uint64{}}
}

func (c *Cluster) kvLimits() kv.Limits {
	if c.cfg.KVLimits == (kv.Limits{}) {
		return kv.DefaultLimits
	}
	return c.cfg.KVLimits
}

// UnsafeLocalReads switches every later Get to the broken local-read path (see
// kvState.localReads). Only tests of the tier itself use it.
func (c *Cluster) UnsafeLocalReads() { c.kv.localReads = true }

// UnsafeFreshRetryIDs switches session retries to the broken fresh-id client
// (see kvState.freshRetryIDs). Only tests of the tier itself use it.
func (c *Cluster) UnsafeFreshRetryIDs() { c.kv.freshRetryIDs = true }

// History returns the client-visible history recorded so far (operations still
// waiting are Incomplete).
func (c *Cluster) History() lincheck.History { return c.kv.rec.History() }

// KVStats returns the client outcome counters.
func (c *Cluster) KVStats() KVStats { return c.kv.stats }

// Store returns a node's current state machine (nil while it is down).
func (c *Cluster) Store(id NodeID) *kv.Store {
	n := c.nodes[id]
	if n == nil || !n.up {
		return nil
	}
	return n.store
}

// Busy reports whether a client is waiting for an operation.
func (c *Cluster) Busy(client string) bool {
	cl := c.kv.clients[client]
	return cl != nil && cl.busy()
}

// Session reports a client's session id (0: anonymous or not registered).
func (c *Cluster) Session(client string) uint64 {
	if cl := c.kv.clients[client]; cl != nil {
		return cl.session
	}
	return 0
}

func (c *Cluster) client(name string) *simClient {
	cl := c.kv.clients[name]
	if cl == nil {
		cl = &simClient{name: name}
		c.kv.clients[name] = cl
	}
	return cl
}

// kvStart invokes a client operation (KVPut, KVGet, KVDelete). A session
// client's request carries its identity. It returns the node the event touched.
func (c *Cluster) kvStart(e Event) *node {
	cl := c.client(e.Client)
	if cl.busy() || e.Key == "" || len(e.Key) > kv.MaxKeyLen {
		c.skip(e)
		return nil
	}
	var value []byte
	switch e.Kind {
	case KVPut:
		cl.kind, value = lincheck.Put, []byte(e.Data)
	case KVGet:
		cl.kind = lincheck.Get
	default:
		cl.kind = lincheck.Delete
	}
	c.kv.stats.Ops++
	cl.key, cl.value, cl.unknown, cl.sends = e.Key, value, false, nil
	if cl.session != 0 {
		cl.rid = cl.nextRID
		cl.nextRID++
		cl.op = c.kv.rec.BeginRequest(cl.name, cl.kind, e.Key, value, cl.session, cl.rid)
	} else {
		cl.rid = 0
		cl.op = c.kv.rec.Begin(cl.name, cl.kind, e.Key, value)
	}
	c.trace.add(c.step, "kv-invoke %s op=%d %s %q %q cid=%d rid=%d at %s", cl.name, cl.op, cl.kind, e.Key, value, cl.session, cl.rid, e.Node)
	return c.kvSend(cl, e.Node, maxKVAttempts)
}

// kvRegister starts a client's REGISTER (not recorded in the history: it has
// no key and no effect on the key-value state).
func (c *Cluster) kvRegister(e Event) *node {
	cl := c.client(e.Client)
	if cl.busy() || cl.session != 0 {
		c.skip(e)
		return nil
	}
	cl.register, cl.unknown, cl.sends = true, false, nil
	c.trace.add(c.step, "kv-register %s at %s", cl.name, e.Node)
	return c.kvSend(cl, e.Node, maxKVAttempts)
}

// kvRetry re-sends a session client's open request — the same identity — to a
// node, once its previous sends are all over.
func (c *Cluster) kvRetry(e Event) *node {
	cl := c.kv.clients[e.Client]
	if cl == nil || cl.session == 0 || cl.op == 0 || len(cl.sends) > 0 {
		c.skip(e)
		return nil
	}
	c.kv.stats.Retries++
	if c.kv.freshRetryIDs {
		cl.rid = cl.nextRID
		cl.nextRID++
	}
	c.trace.add(c.step, "kv-retry %s op=%d rid=%d at %s", cl.name, cl.op, cl.rid, e.Node)
	return c.kvSend(cl, e.Node, maxKVAttempts)
}

// kvDup sends a concurrent duplicate of a session client's open write — a
// second connection, another node (following its not-leader redirects, as any
// send does) — while a send is still outstanding.
func (c *Cluster) kvDup(e Event) *node {
	cl := c.kv.clients[e.Client]
	if cl == nil || cl.session == 0 || cl.op == 0 || cl.kind == lincheck.Get || len(cl.sends) == 0 {
		c.skip(e)
		return nil
	}
	c.kv.stats.DupSends++
	c.trace.add(c.step, "kv-dup %s op=%d rid=%d at %s", cl.name, cl.op, cl.rid, e.Node)
	return c.kvSend(cl, e.Node, maxKVAttempts)
}

// kvSend sends the client's open request to target, following one not-leader
// redirect per remaining attempt. For an anonymous client a definite refusal
// ends the operation (Phase 12); for a session client it ends only this send.
func (c *Cluster) kvSend(cl *simClient, target NodeID, left int) *node {
	sd := &simSend{attempt: -1}
	if cl.op != 0 {
		sd.attempt = c.kv.rec.Attempt(cl.op, string(target))
	}
	n := c.nodes[target]
	switch {
	case n == nil || !n.up:
		c.attemptDone(cl, sd, true, "unavailable", 0)
		c.kv.stats.Unavailable++
		c.refused(cl, "unavailable")
		return nil
	case n.paused:
		sd.node, sd.inc, sd.stalled = n, n.inc, true
		cl.sends = append(cl.sends, sd)
		c.kv.stats.Stalled++
		c.trace.add(c.step, "kv-stalled %s at paused %s", cl.name, n.id)
		return nil
	}
	sd.node, sd.inc = n, n.inc
	var err error
	switch {
	case cl.op != 0 && cl.kind == lincheck.Get && c.kv.localReads:
		v, ok := n.store.Get([]byte(cl.key))
		c.attemptDone(cl, sd, true, "local", n.core.Term())
		if ok {
			c.kvEnd(cl, lincheck.OK, v, n.id, n.core.Term(), n.core.AppliedIndex(), "local read")
		} else {
			c.kvEnd(cl, lincheck.NotFound, nil, n.id, n.core.Term(), n.core.AppliedIndex(), "local read")
		}
		return nil
	case cl.op != 0 && cl.kind == lincheck.Get:
		var rs raft.ReadState
		if rs, err = n.core.ReadIndex(); err == nil {
			sd.term = n.core.Term()
			sd.regCommitted = uint64(len(c.chk.committed))
			sd.read = n.reads.Add(rs.ID, sd.term)
			c.trace.add(c.step, "kv-read %s op=%d at %s id=%d index=%d term=%d committed=%d", cl.name, cl.op, n.id, rs.ID, rs.Index, sd.term, sd.regCommitted)
		}
	default:
		var cmd kv.Command
		switch {
		case cl.register:
			cmd = kv.Command{Op: kv.OpRegister}
		case cl.kind == lincheck.Put:
			cmd = kv.Command{Op: kv.OpPut, Key: []byte(cl.key), Value: cl.value}
		default:
			cmd = kv.Command{Op: kv.OpDelete, Key: []byte(cl.key)}
		}
		if cl.session != 0 && !cl.register {
			// One request at a time: every earlier one is acknowledged.
			cmd.ClientID, cmd.RequestID, cmd.AckedBelow = cl.session, cl.rid, cl.rid
		}
		data := cmd.Encode()
		if err = n.core.Propose(data); err == nil {
			sd.index, sd.term, sd.data = n.core.LastIndex(), n.core.Term(), data
			c.chk.proposed(string(data), c.step)
			c.chk.kvCommands[string(data)] = true
			sd.write = n.waiters.Add(sd.index, sd.term, n.core.AppliedIndex())
			c.trace.add(c.step, "kv-write %s op=%d at %s idx=%d term=%d", cl.name, cl.op, n.id, sd.index, sd.term)
		}
	}
	if err != nil {
		hint := n.core.LeaderID()
		c.attemptDone(cl, sd, true, "not-leader("+string(hint)+")", n.core.Term())
		c.kv.stats.Redirects++
		c.trace.add(c.step, "kv-redirect %s op=%d from %s to %q", cl.name, cl.op, n.id, hint)
		if left > 1 && hint != "" && hint != n.id {
			return c.kvSend(cl, hint, left-1) // a new attempt of the same request
		}
		c.refused(cl, "not leader")
		return nil
	}
	cl.sends = append(cl.sends, sd)
	c.drain(n)
	return n
}

// attemptDone records how a send ended (REGISTER sends are not recorded).
func (c *Cluster) attemptDone(cl *simClient, sd *simSend, complete bool, result string, term uint64) {
	if cl.op != 0 && sd.attempt >= 0 {
		c.kv.rec.AttemptDone(cl.op, sd.attempt, complete, result, term)
	}
}

// refused handles a definite no-effect answer to a send: it ends an anonymous
// operation (Rejected, as in Phase 12) or a REGISTER; a session client's
// request stays open for a retry.
func (c *Cluster) refused(cl *simClient, why string) {
	switch {
	case cl.register:
		if len(cl.sends) == 0 {
			cl.register = false
			c.trace.add(c.step, "kv-register-failed %s (%s)", cl.name, why)
		}
	case cl.session == 0 && cl.op != 0:
		c.kvEnd(cl, lincheck.Rejected, nil, "", 0, 0, why)
	}
}

// kvTimeout makes a waiting client give up on its outstanding sends. An
// anonymous operation is then Incomplete — for a write, it may still take
// effect. A session client's request stays open, its outcome now unknown: it
// will be retried under the same identity.
func (c *Cluster) kvTimeout(e Event) {
	cl := c.kv.clients[e.Client]
	if cl == nil || !cl.busy() {
		c.skip(e)
		return
	}
	for _, sd := range cl.sends {
		c.attemptDone(cl, sd, false, "timeout", 0)
	}
	cl.sends = nil
	switch {
	case cl.register:
		cl.register = false
		c.trace.add(c.step, "kv-register-timeout %s", cl.name)
	case cl.session == 0:
		c.kvEnd(cl, lincheck.Incomplete, nil, "", 0, 0, "timeout")
	default:
		cl.unknown = true
		c.trace.add(c.step, "kv-timeout %s op=%d rid=%d (open: retry)", cl.name, cl.op, cl.rid)
	}
}

// kvPoll completes every send that has an outcome, in client-name order (so the
// history is a pure function of the script). It runs after every event, once
// the event's invariant checks have run — so the global committed record is
// current when INV-X5..X7 are checked.
func (c *Cluster) kvPoll() {
	if len(c.kv.clients) == 0 || c.viol != nil {
		return
	}
	names := make([]string, 0, len(c.kv.clients))
	for name := range c.kv.clients {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		cl := c.kv.clients[name]
		for i := 0; i < len(cl.sends) && c.viol == nil; {
			sd := cl.sends[i]
			if sd.stalled {
				i++
				continue
			}
			var out raftnode.Outcome
			ch := sd.write
			if ch == nil {
				ch = sd.read
			}
			select {
			case out = <-ch:
			default:
				i++
				continue
			}
			cl.sends = append(cl.sends[:i], cl.sends[i+1:]...)
			if c.completeSend(cl, sd, out) {
				break // the request ended: its other sends were abandoned
			}
		}
	}
}

// completeSend applies one send's outcome; it reports whether the client's
// request (or REGISTER) ended.
func (c *Cluster) completeSend(cl *simClient, sd *simSend, out raftnode.Outcome) bool {
	n := sd.node
	alive := n.up && n.inc == sd.inc
	switch {
	case out.Err == nil && !alive:
		// The outcome was reached, then the process died before the response
		// left it: the client heard nothing.
		c.attemptDone(cl, sd, false, "died before replying", 0)
		return c.unanswered(cl, "no reply")
	case out.Err == nil && sd.write != nil:
		if !c.committedAs(sd.index, sd.term, sd.data) {
			c.violate("INV-X5", "%s op %d: write acknowledged at index %d term %d, but that entry is not the committed one there", cl.name, cl.op, sd.index, sd.term)
			return true
		}
		r, _ := out.Result.(kv.Result)
		if cl.register {
			if r.Decision != kv.Registered {
				c.violate("harness", "%s: REGISTER applied as %s", cl.name, r.Decision)
				return true
			}
			cl.session, cl.nextRID, cl.register, cl.sends = r.Index, 1, false, nil
			c.kv.stats.Registered++
			c.trace.add(c.step, "kv-registered %s session=%d", cl.name, cl.session)
			return true
		}
		switch r.Decision {
		case kv.Executed, kv.Duplicate:
			result := "ok"
			if r.Decision == kv.Duplicate {
				result = fmt.Sprintf("duplicate of index %d", r.Index)
				c.kv.stats.Duplicates++
			}
			if cl.unknown {
				c.kv.stats.Resolved++
				if r.Decision == kv.Duplicate {
					c.kv.stats.ResolvedDup++
				}
			}
			c.attemptDone(cl, sd, true, result, sd.term)
			c.kv.stats.WritesAcked++
			c.kvEnd(cl, lincheck.OK, nil, n.id, sd.term, r.Index, result)
		case kv.Expired:
			c.attemptDone(cl, sd, true, "session expired", sd.term)
			c.kv.stats.Expired++
			o := lincheck.Rejected
			if cl.unknown {
				o = lincheck.Incomplete // an earlier send may have executed it
			}
			cl.session = 0 // it must register again
			c.kvEnd(cl, o, nil, n.id, sd.term, sd.index, "session expired")
		default:
			// Conflict, stale or limit: a well-behaved simulated client never
			// provokes them — surface it rather than guess.
			c.violate("harness", "%s op %d: identified write applied as %s", cl.name, cl.op, r.Decision)
		}
		return true
	case out.Err == nil:
		if out.Index < sd.regCommitted || n.core.AppliedIndex() < sd.regCommitted {
			c.violate("INV-X7", "%s op %d: read served at read index %d (applied %d) on %s, but index %d was committed in the cluster when it was registered",
				cl.name, cl.op, out.Index, n.core.AppliedIndex(), n.id, sd.regCommitted)
			return true
		}
		v, ok := n.store.Get([]byte(cl.key))
		c.attemptDone(cl, sd, true, "ok", sd.term)
		c.kv.stats.ReadsServed++
		if ok {
			c.kvEnd(cl, lincheck.OK, v, n.id, sd.term, out.Index, "read")
		} else {
			c.kvEnd(cl, lincheck.NotFound, nil, n.id, sd.term, out.Index, "read")
		}
		return true
	case errors.Is(out.Err, raftnode.ErrLost):
		if c.committedAs(sd.index, sd.term, sd.data) {
			c.violate("INV-X6", "%s op %d: write reported lost, but its entry (index %d, term %d) is committed", cl.name, cl.op, sd.index, sd.term)
			return true
		}
		c.attemptDone(cl, sd, true, "lost", sd.term)
		c.kv.stats.Lost++
		if cl.session == 0 && cl.op != 0 {
			c.kvEnd(cl, lincheck.Rejected, nil, n.id, sd.term, sd.index, "lost")
			return true
		}
		c.refused(cl, "lost")
		return !cl.busy()
	case errors.Is(out.Err, raft.ErrNotLeader):
		// A read the core dropped when it stopped leading: a definite
		// no-effect (reads have none anyway).
		c.attemptDone(cl, sd, true, "not-leader", sd.term)
		c.kv.stats.Redirects++
		if cl.session == 0 {
			c.kvEnd(cl, lincheck.Rejected, nil, "", 0, 0, "read dropped: no longer leader")
			return true
		}
		return false
	default:
		c.attemptDone(cl, sd, false, "unknown: "+out.Err.Error(), 0)
		return c.unanswered(cl, "unknown: "+out.Err.Error())
	}
}

// unanswered handles a send whose outcome the client never learns: an
// anonymous operation ends Incomplete (Phase 12); a session client's request
// stays open with an unknown outcome, to be retried; a REGISTER is abandoned
// once none of its sends is left.
func (c *Cluster) unanswered(cl *simClient, why string) bool {
	switch {
	case cl.register:
		if len(cl.sends) == 0 {
			cl.register = false
			c.trace.add(c.step, "kv-register-lost %s (%s)", cl.name, why)
			return true
		}
		return false
	case cl.session == 0:
		c.kvEnd(cl, lincheck.Incomplete, nil, "", 0, 0, why)
		return true
	default:
		cl.unknown = true
		return false
	}
}

// committedAs reports whether the global committed record holds exactly this
// entry at index.
func (c *Cluster) committedAs(index, term uint64, data []byte) bool {
	if index == 0 || index > uint64(len(c.chk.committed)) {
		return false
	}
	e := c.chk.committed[index-1].e
	return e.Term == term && string(e.Data) == string(data)
}

func (c *Cluster) kvEnd(cl *simClient, o lincheck.Outcome, output []byte, node NodeID, term, index uint64, why string) {
	// Sends still outstanding are abandoned: the request is over.
	for _, sd := range cl.sends {
		c.attemptDone(cl, sd, false, "abandoned: request ended", 0)
	}
	c.kv.rec.End(cl.op, o, output, string(node), term, index)
	switch o {
	case lincheck.OK:
		c.kv.stats.OK++
	case lincheck.NotFound:
		c.kv.stats.NotFound++
	case lincheck.Rejected:
		c.kv.stats.Rejected++
	default:
		c.kv.stats.Incomplete++
	}
	if o == lincheck.OK && cl.kind == lincheck.Get {
		c.trace.add(c.step, "kv-complete %s op=%d %s %q (%s) at %s index=%d", cl.name, cl.op, o, output, why, node, index)
	} else {
		c.trace.add(c.step, "kv-complete %s op=%d %s (%s) at %s index=%d", cl.name, cl.op, o, why, node, index)
	}
	cl.op, cl.sends, cl.unknown, cl.rid, cl.value = 0, nil, false, 0, nil
}

// checkDecision runs at the instant a node's store decides an entry (Phase 13):
// the decision must be the reference session model's decision for that index
// of the committed log (INV-X11) — the model is fed the log in index order by
// whichever node applies an index first, and every later application of it,
// including every replay after a restart, must agree — and an identity may
// execute at one index only (INV-X2).
func (c *Cluster) checkDecision(n *node, e raft.Entry, result any) {
	i := int(e.Index)
	if i == len(c.kv.decisions)+1 {
		d := decision{data: string(e.Data)}
		if cmd, err := kv.Decode(e.Data); err == nil {
			mc := lincheck.SessionCommand{Index: e.Index, Register: cmd.Op == kv.OpRegister, ClientID: cmd.ClientID,
				RequestID: cmd.RequestID, AckedBelow: cmd.AckedBelow, Kind: lincheck.Put, Key: string(cmd.Key), Value: string(cmd.Value)}
			if cmd.Op == kv.OpDelete {
				mc.Kind = lincheck.Delete
			}
			d.d, d.index = c.kv.model.Apply(mc)
		}
		c.kv.decisions = append(c.kv.decisions, d)
	}
	if i > len(c.kv.decisions) {
		c.violate("harness", "%s applied index %d before any node applied %d", n.id, i, len(c.kv.decisions)+1)
		return
	}
	want := c.kv.decisions[i-1]
	if want.data != string(e.Data) {
		return // INV-R5 reports a different entry at a committed index
	}
	r, isResult := result.(kv.Result)
	switch {
	case want.d == 0 && !isResult:
		return
	case want.d == 0 || !isResult:
		c.violate("INV-X11", "%s decided index %d as %v, the reference model as %v", n.id, i, result, want.d)
		return
	}
	if r.Decision.String() != want.d.String() || r.Index != want.index {
		c.violate("INV-X11", "%s decided index %d as %s@%d, the reference model as %s@%d", n.id, i, r.Decision, r.Index, want.d, want.index)
		return
	}
	if r.Decision == kv.Executed {
		if cmd, _ := kv.Decode(e.Data); cmd.ClientID != 0 {
			id := [2]uint64{cmd.ClientID, cmd.RequestID}
			if at, ok := c.kv.executed[id]; ok && at != e.Index {
				c.violate("INV-X2", "request (cid=%d, rid=%d) executed at index %d and again at index %d", id[0], id[1], at, e.Index)
				return
			}
			c.kv.executed[id] = e.Index
		}
	}
}

// kvNodeUp gives a booting node a fresh, empty state machine and empty request
// tables: a restarted node rebuilds its store by re-applying its recovered
// committed prefix (docs/CRASH_RECOVERY.md §6), and no request survives a
// process.
func (n *node) kvNodeUp(l kv.Limits) {
	n.store = kv.NewStoreWithLimits(l)
	n.waiters = raftnode.NewWaiters()
	n.reads = raftnode.NewReads()
}

// kvNodeDown fails every request the dying process held (their clients hear
// nothing: unknown).
func (n *node) kvNodeDown() {
	if n.waiters != nil {
		n.waiters.FailAll(raft.ErrStopped)
		n.reads.FailAll(raft.ErrStopped)
	}
	n.store, n.waiters, n.reads = nil, nil, nil
}

// applyToStore applies a committed entry to the node's store: the election no-op
// and the non-key-value commands of plain Propose events apply as no-ops.
func (n *node) applyToStore(index uint64, data []byte) (any, error) {
	if !kv.IsCommand(data) {
		data = nil
	}
	return n.store.ApplyResult(index, data)
}

// --- seeded client workloads ---

// KVProfiles are the built-in client workload mixes: clients against a cluster
// under each family of faults. Their runs are checked for linearizability
// (TestKVSeededHistoriesAreLinearizable) on top of every Raft invariant.
var KVProfiles = []Profile{
	{Name: "kv-steady", Nodes: 3, Steps: 1500, Tick: 300, Deliver: 700,
		KVPut: 60, KVGet: 60, KVDelete: 15, Clients: 4, Keys: 3, FIFOPercent: 90},
	{Name: "kv-partitions", Nodes: 5, Steps: 2500, Tick: 300, Deliver: 600, Partition: 12, Heal: 12,
		KVPut: 50, KVGet: 60, KVDelete: 10, KVTimeout: 8, Clients: 6, Keys: 2, FIFOPercent: 80},
	// Two-sided splits of a 5-node group: the minority side can keep a leader
	// AND a follower acknowledging its heartbeats in the old term — the
	// sharpest stale-leader case for ReadIndex's quorum rule.
	{Name: "kv-splits", Nodes: 5, Steps: 2500, Tick: 300, Deliver: 600, Split: 3, Heal: 1,
		KVPut: 50, KVGet: 70, KVDelete: 10, KVTimeout: 8, Clients: 6, Keys: 2, FIFOPercent: 80},
	{Name: "kv-crashes", Nodes: 3, Steps: 2500, Tick: 300, Deliver: 600, Crash: 10, Restart: 40,
		KVPut: 50, KVGet: 60, KVDelete: 10, KVTimeout: 8, Clients: 5, Keys: 2, FIFOPercent: 80, PowerLossPercent: 40, MaxTorn: 128},
	{Name: "kv-messages", Nodes: 5, Steps: 2500, Tick: 300, Deliver: 600, Drop: 50, Duplicate: 50, Delay: 50, Pause: 4, Resume: 30,
		KVPut: 50, KVGet: 60, KVDelete: 10, KVTimeout: 8, Clients: 6, Keys: 2, FIFOPercent: 30, MaxDelay: 60},
	{Name: "kv-crashpoints", Nodes: 3, Steps: 2500, Tick: 300, Deliver: 600, CrashAt: 12, Restart: 40,
		KVPut: 60, KVGet: 60, KVDelete: 10, KVTimeout: 8, Clients: 5, Keys: 2, FIFOPercent: 80, PowerLossPercent: 40, MaxTorn: 128},
	{Name: "kv-mixed", Nodes: 5, Steps: 3000, Tick: 300, Deliver: 600, Drop: 20, Duplicate: 20, Delay: 20,
		Partition: 6, Heal: 15, Crash: 5, Restart: 30, Pause: 5, Resume: 30, FailPersist: 3, CrashAt: 4,
		KVPut: 50, KVGet: 60, KVDelete: 10, KVTimeout: 6, Clients: 8, Keys: 3, FIFOPercent: 60, PowerLossPercent: 40, MaxDelay: 40, MaxTorn: 64},

	// Phase 13: session clients that retry every unknown write under its
	// identity and send concurrent duplicates, under each fault family.
	{Name: "kv-sessions-crashes", Nodes: 3, Steps: 2500, Tick: 300, Deliver: 600, Crash: 10, Restart: 40,
		KVPut: 50, KVGet: 40, KVDelete: 15, KVTimeout: 12, KVSessions: true, KVRegister: 30, KVRetry: 40, KVDup: 12,
		Clients: 5, Keys: 2, FIFOPercent: 80, PowerLossPercent: 40, MaxTorn: 128},
	{Name: "kv-sessions-crashpoints", Nodes: 3, Steps: 2500, Tick: 300, Deliver: 600, CrashAt: 12, Restart: 40,
		KVPut: 50, KVGet: 40, KVDelete: 15, KVTimeout: 12, KVSessions: true, KVRegister: 30, KVRetry: 40, KVDup: 12,
		Clients: 5, Keys: 2, FIFOPercent: 80, PowerLossPercent: 40, MaxTorn: 128},
	{Name: "kv-sessions-partitions", Nodes: 5, Steps: 2500, Tick: 300, Deliver: 600, Partition: 8, Split: 3, Heal: 10,
		KVPut: 50, KVGet: 40, KVDelete: 15, KVTimeout: 12, KVSessions: true, KVRegister: 30, KVRetry: 40, KVDup: 12,
		Clients: 6, Keys: 2, FIFOPercent: 80},
	{Name: "kv-sessions-messages", Nodes: 5, Steps: 2500, Tick: 300, Deliver: 600, Drop: 50, Duplicate: 50, Delay: 50, Pause: 4, Resume: 30,
		KVPut: 50, KVGet: 40, KVDelete: 15, KVTimeout: 12, KVSessions: true, KVRegister: 30, KVRetry: 40, KVDup: 12,
		Clients: 6, Keys: 2, FIFOPercent: 30, MaxDelay: 60},
	// Tiny limits: sessions are evicted all the time, so retries meet
	// SESSION_EXPIRED and clients register again — never a second execution.
	{Name: "kv-sessions-evict", Nodes: 3, Steps: 2500, Tick: 300, Deliver: 600, Crash: 6, Restart: 40,
		KVPut: 50, KVGet: 30, KVDelete: 15, KVTimeout: 12, KVSessions: true, KVRegister: 40, KVRetry: 30, KVDup: 10,
		KVLimits: kv.Limits{MaxSessions: 3, MaxUnacked: 2}, Clients: 6, Keys: 2, FIFOPercent: 80},
	{Name: "kv-sessions-mixed", Nodes: 5, Steps: 3000, Tick: 300, Deliver: 600, Drop: 20, Duplicate: 20, Delay: 20,
		Partition: 6, Heal: 15, Crash: 5, Restart: 30, Pause: 5, Resume: 30, FailPersist: 3, CrashAt: 4,
		KVPut: 50, KVGet: 40, KVDelete: 15, KVTimeout: 12, KVSessions: true, KVRegister: 30, KVRetry: 40, KVDup: 12,
		Clients: 8, Keys: 3, FIFOPercent: 60, PowerLossPercent: 40, MaxDelay: 40, MaxTorn: 64},
}

// KVProfileByName returns the named client profile.
func KVProfileByName(name string) (Profile, bool) {
	for _, p := range KVProfiles {
		if p.Name == name {
			return p, true
		}
	}
	return Profile{}, false
}

// kvEvents adds the client events a profile weights to the generator's options.
func (c *Cluster) kvEvents(rng *rand.Rand, p Profile, add func(int, bool, func() Event)) {
	if p.Clients == 0 {
		return
	}
	var idle, busy, unregistered, open, dupable []string
	for i := 1; i <= p.Clients; i++ {
		name := fmt.Sprintf("c%d", i)
		cl := c.kv.clients[name]
		switch {
		case p.KVSessions && (cl == nil || (cl.session == 0 && !cl.busy())):
			unregistered = append(unregistered, name)
		case cl != nil && cl.busy():
			busy = append(busy, name)
			if cl.session != 0 && cl.op != 0 && len(cl.sends) == 0 {
				open = append(open, name)
			}
			if cl.session != 0 && cl.op != 0 && cl.kind != lincheck.Get && len(cl.sends) > 0 {
				dupable = append(dupable, name)
			}
		default:
			idle = append(idle, name)
		}
	}
	// Clients mostly go to a node that believes it leads — possibly a deposed
	// one, which is exactly the stale-leader case ReadIndex must survive — and
	// otherwise anywhere, including down and paused nodes.
	target := func() NodeID {
		if ls := c.Leaders(); len(ls) > 0 && rng.Intn(100) < 75 {
			return ls[rng.Intn(len(ls))]
		}
		return c.ids[rng.Intn(len(c.ids))]
	}
	key := func() string { return fmt.Sprintf("k%d", rng.Intn(max(p.Keys, 1))) }
	pick := func(s []string) string { return s[rng.Intn(len(s))] }
	add(p.KVPut, len(idle) > 0, func() Event {
		cl := pick(idle)
		return Event{Kind: KVPut, Node: target(), Client: cl, Key: key(), Data: fmt.Sprintf("%s.%d", cl, c.step+1)}
	})
	add(p.KVGet, len(idle) > 0, func() Event {
		return Event{Kind: KVGet, Node: target(), Client: pick(idle), Key: key()}
	})
	add(p.KVDelete, len(idle) > 0, func() Event {
		return Event{Kind: KVDelete, Node: target(), Client: pick(idle), Key: key()}
	})
	add(p.KVTimeout, len(busy) > 0, func() Event { return Event{Kind: KVTimeout, Client: pick(busy)} })
	add(p.KVRegister, len(unregistered) > 0, func() Event {
		return Event{Kind: KVRegister, Node: target(), Client: pick(unregistered)}
	})
	add(p.KVRetry, len(open) > 0, func() Event { return Event{Kind: KVRetry, Node: target(), Client: pick(open)} })
	add(p.KVDup, len(dupable) > 0, func() Event {
		return Event{Kind: KVDup, Node: c.ids[rng.Intn(len(c.ids))], Client: pick(dupable)}
	})
}

// RunKV executes a seeded client workload: Steps events drawn from the profile
// (faults, scheduling and client operations), then stabilization, then every
// waiting client is allowed to finish — a session client's open request is
// retried at the leader until it has a definite answer — and finally a fresh
// client reads every key through the leader, so the history ends with the
// converged state observed through the linearizable read path. It is a pure
// function of (profile, seed); the Result carries the history.
func RunKV(p Profile, seed int64) *Result {
	c, err := New(Config{Nodes: p.Nodes, Seed: seed, KVLimits: p.KVLimits})
	if err != nil {
		return &Result{Seed: seed, Profile: p.Name, Violation: &Violation{Invariant: "harness", Detail: err.Error()}}
	}
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < p.Steps && c.viol == nil; i++ {
		c.Apply(c.generate(rng, p))
	}
	if c.viol == nil {
		c.Stabilize(400)
	}
	if c.viol == nil {
		c.SettleClients(p.Keys, 200)
	}
	if c.viol == nil {
		c.checkStores()
	}
	return c.result(seed, p.Name)
}

// SettleClients runs fair rounds until no client is waiting: a send stalled in
// a node that was paused times out, and a session client's open request with
// no send outstanding is retried at the leader — so every session request ends
// with a definite answer. Then a fresh client reads every key k0..k{keys-1}
// through the leader and waits for those reads. Every action is an event, so a
// replay reproduces it.
func (c *Cluster) SettleClients(keys, maxRounds int) {
	round := func() {
		for _, id := range c.ids {
			c.Apply(Event{Kind: Tick, Node: id})
		}
		c.deliverAllFIFO()
	}
	names := func() []string {
		var out []string
		for name := range c.kv.clients {
			out = append(out, name)
		}
		sort.Strings(out)
		return out
	}
	for r := 0; r < maxRounds && c.viol == nil && c.anyBusy(); r++ {
		for _, name := range names() {
			cl := c.kv.clients[name]
			stalled := false
			for _, sd := range cl.sends {
				stalled = stalled || sd.stalled
			}
			if cl.busy() && stalled {
				c.Apply(Event{Kind: KVTimeout, Client: name})
			}
			if cl.session != 0 && cl.op != 0 && len(cl.sends) == 0 {
				if ls := c.Leaders(); len(ls) == 1 {
					c.Apply(Event{Kind: KVRetry, Node: ls[0], Client: name})
				}
			}
		}
		round()
	}
	for _, name := range names() {
		if c.Busy(name) {
			c.Apply(Event{Kind: KVTimeout, Client: name})
		}
	}
	for k := 0; k < keys && c.viol == nil; k++ {
		ls := c.Leaders()
		if len(ls) != 1 {
			round()
			ls = c.Leaders()
		}
		if len(ls) != 1 {
			continue
		}
		cl := fmt.Sprintf("final%d", k)
		c.Apply(Event{Kind: KVGet, Node: ls[0], Client: cl, Key: fmt.Sprintf("k%d", k)})
		for r := 0; r < maxRounds && c.viol == nil && c.Busy(cl); r++ {
			round()
		}
	}
}

func (c *Cluster) anyBusy() bool {
	for _, cl := range c.kv.clients {
		if cl.busy() {
			return true
		}
	}
	return false
}

// checkStores is the reference-model differential after convergence: every
// node's kv.Store — its key-value map and its session table — equals the
// lincheck session model folded over the committed log (two independent
// implementations of the Phase 1 semantics and of request identity). The model
// decides each command exactly as it was decided on every replica at apply
// time (INV-X11), so a duplicate, a conflict or an expired session's command
// has no effect in it either.
func (c *Cluster) checkStores() {
	l := c.kvLimits()
	model := lincheck.NewSessionModel(lincheck.SessionLimits{MaxSessions: l.MaxSessions, MaxUnacked: l.MaxUnacked})
	keys := map[string]bool{}
	for i, ce := range c.chk.committed {
		cmd, err := kv.Decode(ce.e.Data)
		if err != nil {
			continue // the no-op and plain Propose commands
		}
		mc := lincheck.SessionCommand{Index: uint64(i + 1), Register: cmd.Op == kv.OpRegister, ClientID: cmd.ClientID,
			RequestID: cmd.RequestID, AckedBelow: cmd.AckedBelow, Kind: lincheck.Put, Key: string(cmd.Key), Value: string(cmd.Value)}
		if cmd.Op == kv.OpDelete {
			mc.Kind = lincheck.Delete
		}
		model.Apply(mc)
		if cmd.Op != kv.OpRegister {
			keys[string(cmd.Key)] = true
		}
	}
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.up {
			continue
		}
		snap := n.store.Snapshot()
		var diffs []string
		for k := range keys {
			st := model.State(k)
			v, ok := snap[k]
			if ok != st.Present || (ok && string(v) != st.Value) {
				diffs = append(diffs, fmt.Sprintf("%q: store (%v,%q) model (%v,%q)", k, ok, v, st.Present, st.Value))
			}
		}
		for k := range snap {
			if !keys[k] {
				diffs = append(diffs, fmt.Sprintf("%q present in the store, never written in the committed log", k))
			}
		}
		table := n.store.Sessions()
		ids := model.Sessions()
		if len(ids) != len(table) {
			diffs = append(diffs, fmt.Sprintf("the store holds %d sessions, the model %d", len(table), len(ids)))
		}
		for _, sid := range ids {
			acked, rids, _ := model.SessionInfo(sid)
			sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })
			st, ok := table[sid]
			if !ok || st.AckedBelow != acked || fmt.Sprint(st.Requests) != fmt.Sprint(rids) {
				diffs = append(diffs, fmt.Sprintf("session %d: store %+v (present %v), model acked=%d rids=%v", sid, st, ok, acked, rids))
			}
		}
		if len(diffs) > 0 {
			sort.Strings(diffs)
			c.violate("INV-X8", "%s's store differs from the reference model over the committed log: %s", id, strings.Join(diffs, "; "))
			return
		}
	}
}
