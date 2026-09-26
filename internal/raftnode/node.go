// Package raftnode is the node driver for one Raft group (Phase 9, docs/RAFT.md
// §1, ADR-016). It is the impure layer around the pure internal/raft core: it owns
// the actor goroutine, the tick source, the durable log (internal/raftlog), the
// transport adapter, and the apply loop. The core owns none of these.
//
// A single goroutine (the actor loop) owns the core and the log, so the core needs
// no locks (ADR-002). External callers reach it only through channels (Propose) or
// a mutex-guarded status snapshot (Status). The driver honours the Ready contract:
// it persists a Ready's HardState and Entries durably BEFORE handing its Messages
// to the network, which is what makes durability-before-reply hold (INV-R6). That
// ordering lives in one function, DrainReady, which the deterministic simulator
// (internal/raftsim) calls too, as it does Recover, the startup path.
//
// Phase 10 hardening (docs/FAULTS.md, ADR-017):
//
//   - Fail-stop on a persistence failure (INV-F1): if a Save fails, the actor
//     sends none of that Ready's messages, never drives the core again, records
//     the error (Err) and stops (Done). The durable log also refuses every later
//     write (raftlog's latch), so a torn record can never end up mid-log.
//   - Per-peer outboxes: the actor never blocks on the network. Messages are
//     handed to a bounded per-peer queue drained by that peer's sender goroutine,
//     so one wedged peer delays only its own messages, never heartbeats to others.
//   - Status returns one consistent snapshot, so an observer can never combine
//     the role of one moment with the term of another.
//
// Phase 11 crash points (docs/CRASH_RECOVERY.md, ADR-018): the two loops above —
// DrainReadyAt (persist, send, advance) and ApplyCommitted (apply, record) — have
// named crash points (Point) at every boundary, observed through an optional Hook
// (Config.Hook; nil in production). The deterministic simulator, the in-process
// crash tests and `dkvd -crash-at` all crash at the same points.
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/transport"
	"github.com/adivishall/quorum/internal/vfs"
)

// DefaultTickInterval is the wall-clock duration of one logical tick
// (docs/DESIGN.md §8.3). The core only counts ticks; this is where they come from.
const DefaultTickInterval = 50 * time.Millisecond

// OutboxSize bounds each peer's queue of messages awaiting transmission. When a
// peer's queue is full the newest message to it is dropped (and logged): Raft
// retransmits on the next heartbeat, so a bounded loss is safe, whereas an
// unbounded queue behind a wedged peer is not.
const OutboxSize = 256

// StateMachine applies committed commands in log order (the Phase 8 seam). It may
// be nil, in which case commands are applied as a no-op (Phase 9 needs only to
// exercise the apply path; the LSM-backed machine is a later phase).
type StateMachine = replication.StateMachine

// Config constructs a Node.
type Config struct {
	ID        NodeID
	Peers     []NodeID // fixed group membership including ID (ADR-005)
	Transport transport.Transport
	LogPath   string // durable raft log file for this group

	StateMachine   StateMachine // optional
	TickInterval   time.Duration
	ElectionTicks  int
	HeartbeatTicks int

	// DisableSync turns OFF the fsync of the durable Raft log on every Save. It is
	// UNSAFE and exists only for tests and benchmarks where durability is not under
	// test. The zero value (false) is the durable, production path: every Save
	// fsyncs before the driver sends the dependent reply, which is what the
	// persistence contract (INV-R6) requires. Production code leaves this false.
	DisableSync bool

	// FS is the filesystem the durable log lives on. Nil — the production value —
	// is the real OS filesystem. Tests substitute internal/fault's crash model or
	// I/O fault injector here, underneath the unchanged driver and log code.
	FS vfs.FS

	Rand *rand.Rand // optional; defaults to a seed derived from ID
	Logf func(string, ...any)

	// Hook, if non-nil, observes the driver's crash points and may abort a cycle
	// at one of them (see Point and Hook; Phase 11, docs/CRASH_RECOVERY.md). It
	// is a test seam: production leaves it nil, and a nil Hook costs nothing.
	Hook Hook
}

// raftlogOptions returns the durable-log options this config implies. The default
// (zero-value) config is durable (Sync: true); only an explicit DisableSync turns
// fsync off. Start uses this exact function, so a test that pins its result pins
// the real durability default.
func (c Config) raftlogOptions() raftlog.Options {
	return raftlog.Options{Sync: !c.DisableSync, FS: c.FS}
}

// NodeID re-exported for callers.
type NodeID = raft.NodeID

// Storage is the durable-log operation the Ready cycle depends on. *raftlog.Log
// implements it.
type Storage interface {
	Save(hs *raftlog.HardState, entries []raftlog.Entry) error
}

// Recovered is a core rebuilt from its durable log, together with the open log.
type Recovered struct {
	Core  *raft.Raft
	Log   *raftlog.Log
	Mem   *replication.MemoryLog // the core's in-memory log
	State *raftlog.Recovered     // exactly what was read from disk
}

// Recover opens the durable log at cfg.LogPath (on cfg.FS) and rebuilds the core
// from it: the in-memory log from the durable entries, the commit index from the
// persisted (clamped) value, and the core at the recovered term and vote, as a
// Follower. It is the exact startup path of Start, exported so the deterministic
// simulator restarts nodes through the same code. It uses ID, Peers, LogPath,
// FS, DisableSync, Rand (required here), ElectionTicks and HeartbeatTicks.
func Recover(cfg Config) (*Recovered, error) {
	if cfg.Rand == nil {
		return nil, raft.ErrNoRand
	}
	lg, rec, err := raftlog.Open(cfg.LogPath, cfg.raftlogOptions())
	if err != nil {
		return nil, err
	}
	mlog := replication.NewMemoryLog()
	if len(rec.Entries) > 0 {
		if err := mlog.Append(rec.Entries...); err != nil {
			_ = lg.Close()
			return nil, fmt.Errorf("raftnode: recovered log is inconsistent: %w", err)
		}
	}
	if rec.HardState.Commit > 0 {
		if err := mlog.Commit(rec.HardState.Commit); err != nil {
			_ = lg.Close()
			return nil, fmt.Errorf("raftnode: recovered commit invalid: %w", err)
		}
	}
	core, err := raft.New(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers, Rand: cfg.Rand, Log: mlog,
		ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks,
		Term: rec.HardState.Term, Vote: rec.HardState.Vote,
	})
	if err != nil {
		_ = lg.Close()
		return nil, err
	}
	return &Recovered{Core: core, Log: lg, Mem: mlog, State: rec}, nil
}

// Node is a running Raft group driver.
type Node struct {
	cfg  Config
	core *raft.Raft
	log  *raftlog.Log
	tr   transport.Transport
	sm   StateMachine

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{} // closed when the actor loop exits, for any reason

	recvCh    chan raft.Message
	proposeCh chan proposal
	writeCh   chan writeReq
	readCh    chan readReq
	outboxes  map[NodeID]chan raft.Message
	waiters   *Waiters // requests waiting for an apply (actor-owned)
	reads     *Reads   // unconfirmed ReadIndex requests (actor-owned)

	mu     sync.Mutex
	status Status
	err    error // the fail-stop cause; nil if running or cleanly closed

	app atomic.Pointer[AppHandler] // Phase 13: application messages (forwarding)
}

// AppHandler receives the application messages this node's peers send it —
// every frame kind the Raft driver does not own (Phase 13: request forwarding,
// transport.MsgForward and MsgForwardResponse). It runs on the node's receive
// goroutine, so it must not block: hand the work to another goroutine.
type AppHandler func(peer NodeID, kind transport.MsgKind, payload []byte)

// SetAppHandler installs the application-message handler (nil removes it).
// Messages that arrive with no handler installed are dropped, as they were
// before Phase 13.
func (n *Node) SetAppHandler(h AppHandler) {
	if h == nil {
		n.app.Store(nil)
		return
	}
	n.app.Store(&h)
}

// SendApp sends an application message to a peer over the node's transport —
// the same connections, framing and fault injection as Raft traffic. An error
// means the message was not handed to the connection (e.g. the peer is not
// connected): nothing was sent.
func (n *Node) SendApp(ctx context.Context, peer NodeID, kind transport.MsgKind, payload []byte) error {
	return n.tr.Send(ctx, transport.NodeID(peer), kind, payload)
}

type proposal struct {
	data   []byte
	result chan error
}

// writeReq is a Write: the actor answers with the entry's index and term and the
// channel its apply Outcome will arrive on, or an error.
type writeReq struct {
	data   []byte
	result chan writeAccepted
}

type writeAccepted struct {
	index, term uint64
	done        <-chan Outcome
	err         error
}

// readReq is a ReadIndex: the actor answers with the channel the confirmed,
// applied read index will arrive on, or an error.
type readReq struct {
	result chan readAccepted
}

type readAccepted struct {
	done <-chan Outcome
	err  error
}

// Status is one consistent snapshot of a node's Raft state, taken by the actor
// after it finished processing an event.
type Status struct {
	Role      raft.Role
	Term      uint64
	Leader    NodeID
	Commit    uint64
	LastIndex uint64
	Applied   uint64
}

// Start recovers durable state, constructs the core, and launches the actor,
// receive, and per-peer sender goroutines. The caller owns the transport's
// lifecycle (Start does not close it); Close stops the driver and the durable log.
func Start(ctx context.Context, cfg Config) (*Node, error) {
	if cfg.TickInterval == 0 {
		cfg.TickInterval = DefaultTickInterval
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(seedFromID(cfg.ID)))
	}
	rc, err := Recover(cfg)
	if err != nil {
		return nil, err
	}

	nctx, cancel := context.WithCancel(ctx)
	n := &Node{
		cfg: cfg, core: rc.Core, log: rc.Log, tr: cfg.Transport, sm: cfg.StateMachine,
		ctx: nctx, cancel: cancel, done: make(chan struct{}),
		recvCh:    make(chan raft.Message, 256),
		proposeCh: make(chan proposal),
		writeCh:   make(chan writeReq),
		readCh:    make(chan readReq),
		outboxes:  map[NodeID]chan raft.Message{},
		waiters:   NewWaiters(),
		reads:     NewReads(),
	}
	n.snapshotStatus()

	for _, p := range cfg.Peers {
		if p == cfg.ID {
			continue
		}
		ch := make(chan raft.Message, OutboxSize)
		n.outboxes[p] = ch
		n.wg.Add(1)
		go n.senderLoop(ch)
	}
	// Read what the start line reports while this goroutine still owns the
	// core: once the actor runs, only it may touch the core (it may be
	// stepping a message already).
	term, last := rc.Core.Term(), rc.Core.LastIndex()
	n.wg.Add(2)
	go n.receiveLoop()
	go n.actorLoop()
	n.logf("event=raft_started node=%s peers=%d term=%d lastIndex=%d", cfg.ID, len(cfg.Peers), term, last)
	return n, nil
}

// Propose submits a command to be appended and replicated. It returns nil once the
// entry is in the leader's log AND durably persisted there (so Status already
// reflects it); raft.ErrNotLeader if this node is not the leader; the persistence
// failure (wrapping raftlog.ErrFailed) if the entry could not be made durable, in
// which case the node has fail-stopped; and raft.ErrStopped once the node has
// stopped. It does not wait for commitment (there is no client commit-wait yet).
//
// If ctx ends first, Propose returns ctx.Err() — and the outcome is then UNKNOWN:
// the node may already have appended the entry. That is the same ambiguity any
// client timeout has (docs/CONSISTENCY.md C4); it is never reported as a failure.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	p := proposal{data: append([]byte(nil), data...), result: make(chan error, 1)}
	select {
	case n.proposeCh <- p:
	case <-n.ctx.Done():
		return raft.ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-p.result:
		return err
	case <-n.ctx.Done():
		return raft.ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Write proposes a client command and returns only once the entry has been
// COMMITTED and APPLIED on this node in the term it was proposed in — the Phase
// 12 write-completion rule (docs/LINEARIZABILITY.md §3, docs/ARCHITECTURE.md §8
// step 8): success means every later linearizable read, from any client, sees
// the write. It returns the entry's index and term, and the state machine's
// result for that entry (Phase 13: for the key-value store, whether the entry
// executed its request or was a duplicate or a conflict — nil for a state
// machine without results). Errors:
//
//   - raft.ErrNotLeader: this node did not accept the proposal; nothing was
//     appended. The client should retry at the leader (LeaderID). Definite.
//   - ErrLost: the entry was appended but a DIFFERENT entry was committed at its
//     index (this node lost leadership first). The write had no effect. Definite.
//   - ctx.Err(): the outcome is UNKNOWN — the entry may still commit and apply.
//     A client must treat it exactly as a timeout (docs/CONSISTENCY.md C4).
//   - raft.ErrStopped, or a persistence failure: the node stopped. If the
//     proposal had been accepted the outcome is likewise unknown.
func (n *Node) Write(ctx context.Context, data []byte) (index, term uint64, result any, err error) {
	req := writeReq{data: append([]byte(nil), data...), result: make(chan writeAccepted, 1)}
	select {
	case n.writeCh <- req:
	case <-n.ctx.Done():
		return 0, 0, nil, raft.ErrStopped
	case <-ctx.Done():
		return 0, 0, nil, ctx.Err()
	}
	var acc writeAccepted
	select {
	case acc = <-req.result:
	case <-n.ctx.Done():
		return 0, 0, nil, raft.ErrStopped
	case <-ctx.Done():
		return 0, 0, nil, ctx.Err()
	}
	if acc.err != nil {
		return 0, 0, nil, acc.err
	}
	select {
	case out := <-acc.done:
		return acc.index, acc.term, out.Result, out.Err
	case <-ctx.Done():
		return acc.index, acc.term, nil, ctx.Err()
	}
}

// ReadIndex performs the ReadIndex protocol (docs/DESIGN.md §8.5) on this node
// and returns once it is safe to serve a linearizable read from the local state
// machine: the read index was confirmed by a quorum acknowledging this leader
// after the read was registered, and the state machine has applied through it.
// The returned index is that read index. Errors: raft.ErrNotLeader (this node is
// not the leader, or stopped leading before the read was confirmed — retry at
// the leader; a read has no effect either way), ctx.Err() (give up; no effect),
// raft.ErrStopped.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	req := readReq{result: make(chan readAccepted, 1)}
	select {
	case n.readCh <- req:
	case <-n.ctx.Done():
		return 0, raft.ErrStopped
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	var acc readAccepted
	select {
	case acc = <-req.result:
	case <-n.ctx.Done():
		return 0, raft.ErrStopped
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if acc.err != nil {
		return 0, acc.err
	}
	select {
	case out := <-acc.done:
		return out.Index, out.Err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Status returns one consistent snapshot of the node's state. Use it (not the
// single-field accessors) whenever more than one field is needed together.
func (n *Node) Status() Status { n.mu.Lock(); defer n.mu.Unlock(); return n.status }

// Role, Term, LeaderID, CommitIndex return single fields of the latest snapshot.
func (n *Node) Role() raft.Role     { return n.Status().Role }
func (n *Node) Term() uint64        { return n.Status().Term }
func (n *Node) LeaderID() NodeID    { return n.Status().Leader }
func (n *Node) CommitIndex() uint64 { return n.Status().Commit }

// Done is closed when the node stops processing — after Close, or after a
// fail-stop. Err then distinguishes the two.
func (n *Node) Done() <-chan struct{} { return n.done }

// Err returns the reason the node failed (a persistence failure, INV-F1), or nil
// if it is running or was closed cleanly.
func (n *Node) Err() error { n.mu.Lock(); defer n.mu.Unlock(); return n.err }

// Close stops the driver: it cancels the actor, receive and sender loops, waits
// for them, and closes the durable log. It is idempotent. It does not close the
// transport. Close does not report a prior fail-stop; Err does.
func (n *Node) Close() error {
	n.cancel()
	n.wg.Wait()
	return n.log.Close()
}

// actorLoop is the single goroutine that owns the core and the log. It exits on
// cancellation, or immediately after a persistence failure — so a failed node's
// core is never driven again and nothing it computed after the failure can leave.
func (n *Node) actorLoop() {
	defer n.wg.Done()
	defer close(n.done)
	defer func() {
		// Whoever is still waiting learns nothing more from this incarnation.
		n.waiters.FailAll(raft.ErrStopped)
		n.reads.FailAll(raft.ErrStopped)
	}()
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		var accepted *proposal // a proposal the core appended, answered once durable
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.core.Tick()
		case m := <-n.recvCh:
			_ = n.core.Step(m)
		case p := <-n.proposeCh:
			if err := n.core.Propose(p.data); err != nil {
				p.result <- err
			} else {
				accepted = &p
			}
		case w := <-n.writeCh:
			if err := n.core.Propose(w.data); err != nil {
				w.result <- writeAccepted{err: err}
			} else {
				// The entry is the log's tail, in the current term; it completes
				// when that index is applied — with this term, or as ErrLost.
				idx, term := n.core.LastIndex(), n.core.Term()
				w.result <- writeAccepted{index: idx, term: term, done: n.waiters.Add(idx, term, n.core.AppliedIndex())}
			}
		case r := <-n.readCh:
			rs, err := n.core.ReadIndex()
			if err != nil {
				r.result <- readAccepted{err: err}
			} else {
				r.result <- readAccepted{done: n.reads.Add(rs.ID, n.core.Term())}
			}
		}
		if err := n.processReady(); err != nil {
			if accepted != nil {
				accepted.result <- err
			}
			n.fail(err)
			return
		}
		if accepted != nil {
			accepted.result <- nil
		}
	}
}

// processReady performs the core's pending effects in the required order — persist
// (fsync), then hand messages to the outboxes (DrainReadyAt) — then applies
// committed entries (ApplyCommitted) and publishes a status snapshot. A persistence
// failure is returned before anything of that Ready is sent or applied; so is a
// crash-point abort (Config.Hook). A state-machine failure is logged and left for
// the next cycle: appliedIndex does not advance past it, and the node keeps running.
func (n *Node) processReady() error {
	if err := DrainReadyAt(n.core, n.log, n.enqueue, n.confirmRead, n.cfg.Hook); err != nil {
		return err
	}
	if err := ApplyCommitted(n.core, n.sm, n.cfg.Hook, n.applied); err != nil {
		if !errors.Is(err, ErrApply) {
			return err // a crash point fired
		}
		n.logf("event=raft_apply_failed node=%s err=%v", n.cfg.ID, err)
	}
	// A read registered in a term this node no longer leads will never be
	// confirmed (the core dropped it): tell its client to go elsewhere.
	n.reads.DropStale(n.core.Term(), n.core.Role() == raft.Leader)
	n.snapshotStatus()
	return nil
}

// confirmRead is DrainReadyAt's hand-off of a confirmed ReadIndex: the read now
// waits only for the state machine to reach its index.
func (n *Node) confirmRead(rs raft.ReadState) {
	n.reads.Confirmed(rs, n.waiters, n.core.AppliedIndex())
}

// applied is ApplyCommitted's hand-off after an entry is applied and recorded:
// the write that proposed it (or a read barrier at its index) completes now,
// with the state machine's result.
func (n *Node) applied(e raft.Entry, result any) {
	n.waiters.Applied(e.Index, e.Term, result)
}

// fail records a persistence failure and stops every goroutine of the node. The
// actor has already sent nothing of the failed Ready and will not run again; the
// durable log refuses further writes on its own (raftlog's latch).
func (n *Node) fail(err error) {
	n.mu.Lock()
	if n.err == nil {
		n.err = err
	}
	n.mu.Unlock()
	n.logf("event=raft_persist_failed node=%s err=%v", n.cfg.ID, err)
	n.waiters.FailAll(err)
	n.reads.FailAll(err)
	n.cancel()
}

// enqueue hands a message (already persisted-for, by DrainReady's ordering) to its
// peer's outbox without ever blocking the actor. A full outbox drops the message;
// Raft's heartbeats retransmit whatever it carried.
func (n *Node) enqueue(m raft.Message) {
	ch, ok := n.outboxes[m.To]
	if !ok {
		n.logf("event=raft_send_dropped node=%s to=%s type=%s reason=unknown_peer", n.cfg.ID, m.To, m.Type)
		return
	}
	select {
	case ch <- m:
	default:
		n.logf("event=raft_send_dropped node=%s to=%s type=%s reason=outbox_full", n.cfg.ID, m.To, m.Type)
	}
}

// senderLoop transmits one peer's messages in order. It is the only goroutine that
// can block on that peer's connection.
func (n *Node) senderLoop(ch <-chan raft.Message) {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case m := <-ch:
			n.sendMessage(m)
		}
	}
}

// sendMessage marshals a core message and sends it over the transport. A peer that
// is not currently connected is expected (the dialer reconnects); the message is
// dropped and Raft retransmits on the next tick.
func (n *Node) sendMessage(m raft.Message) {
	kind, ok := kindForType(m.Type)
	if !ok {
		return
	}
	if err := n.tr.Send(n.ctx, transport.NodeID(m.To), kind, m.Marshal()); err != nil {
		// ErrPeerNotConnected / a write error: fine, Raft is retransmission-based.
		n.logf("event=raft_send_failed node=%s to=%s type=%s err=%v", n.cfg.ID, m.To, m.Type, err)
	}
}

// receiveLoop decodes inbound transport frames into core messages and feeds them
// to the actor. The message's sender is the connection's handshake identity
// (env.Peer), never a payload field (INV-T4).
func (n *Node) receiveLoop() {
	defer n.wg.Done()
	rc := n.tr.Receive()
	for {
		select {
		case <-n.ctx.Done():
			return
		case env, ok := <-rc:
			if !ok {
				return // transport closed
			}
			mt, ok := typeForKind(env.Kind)
			if !ok {
				// Not a Raft message: an application message (Phase 13
				// forwarding) if a handler is installed; otherwise (e.g. a
				// Probe) ignored.
				if h := n.app.Load(); h != nil {
					(*h)(NodeID(env.Peer), env.Kind, env.Payload)
				}
				continue
			}
			m, err := raft.Unmarshal(env.Payload)
			if err != nil {
				n.logf("event=raft_decode_failed node=%s from=%s err=%v", n.cfg.ID, env.Peer, err)
				continue
			}
			m.Type = mt                    // trust the frame kind for the type
			m.From = raft.NodeID(env.Peer) // trust the handshake identity for the sender
			m.To = n.cfg.ID
			select {
			case n.recvCh <- m:
			case <-n.ctx.Done():
				return
			}
		}
	}
}

func (n *Node) snapshotStatus() {
	n.mu.Lock()
	n.status = Status{
		Role: n.core.Role(), Term: n.core.Term(), Leader: n.core.LeaderID(),
		Commit: n.core.CommitIndex(), LastIndex: n.core.LastIndex(), Applied: n.core.AppliedIndex(),
	}
	n.mu.Unlock()
}

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logf != nil {
		n.cfg.Logf(format, args...)
	}
}

// seedFromID derives a deterministic rand seed from a node id, so different nodes
// get different election timeouts without a global clock.
func seedFromID(id NodeID) int64 {
	var h int64 = 1469598103934665603 // FNV-1a 64 offset basis (low bits)
	for _, b := range []byte(id) {
		h ^= int64(b)
		h *= 1099511628211
	}
	if h < 0 {
		h = -h
	}
	return h + 1
}
