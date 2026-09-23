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
package raftnode

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
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

// DrainReady performs every pending Ready of core in the order the Raft
// persistence contract requires (ADR-016, INV-R6): persist the Ready's HardState
// and entries through st (which fsyncs), THEN hand each of its Messages to send,
// THEN Advance. It returns the first persistence failure WITHOUT sending that
// Ready's messages and without advancing; the caller must then treat the node as
// failed and never drive core again (INV-F1). This is the single implementation of
// the ordering, shared by the node's actor loop and the deterministic simulator.
func DrainReady(core *raft.Raft, st Storage, send func(raft.Message)) error {
	for core.HasReady() {
		rd := core.Ready()
		var hs *raftlog.HardState
		if rd.HardState != nil {
			hs = &raftlog.HardState{Term: rd.HardState.Term, Vote: rd.HardState.Vote, Commit: rd.HardState.Commit}
		}
		if hs != nil || len(rd.Entries) > 0 {
			if err := st.Save(hs, rd.Entries); err != nil {
				return err
			}
		}
		for _, m := range rd.Messages {
			send(m)
		}
		core.Advance()
	}
	return nil
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
	outboxes  map[NodeID]chan raft.Message

	mu     sync.Mutex
	status Status
	err    error // the fail-stop cause; nil if running or cleanly closed
}

type proposal struct {
	data   []byte
	result chan error
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
		outboxes:  map[NodeID]chan raft.Message{},
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
	n.wg.Add(2)
	go n.receiveLoop()
	go n.actorLoop()
	n.logf("event=raft_started node=%s peers=%d term=%d lastIndex=%d", cfg.ID, len(cfg.Peers), rc.Core.Term(), rc.Core.LastIndex())
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
// (fsync), then hand messages to the outboxes (DrainReady) — then applies committed
// entries and publishes a status snapshot. A persistence failure is returned before
// anything of that Ready is sent or applied.
func (n *Node) processReady() error {
	if err := DrainReady(n.core, n.log, n.enqueue); err != nil {
		return err
	}
	n.applyCommitted()
	n.snapshotStatus()
	return nil
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
	n.cancel()
}

// applyCommitted feeds committed-but-unapplied entries to the state machine in
// order, advancing appliedIndex only after each Apply succeeds.
func (n *Node) applyCommitted() {
	for _, e := range n.core.NextApply() {
		if n.sm != nil {
			if err := n.sm.Apply(e.Index, e.Data); err != nil {
				n.logf("event=raft_apply_failed node=%s index=%d err=%v", n.cfg.ID, e.Index, err)
				return // do not advance appliedIndex past a failed apply
			}
		}
		if err := n.core.AppliedTo(e.Index); err != nil {
			n.logf("event=raft_applied_to_failed node=%s index=%d err=%v", n.cfg.ID, e.Index, err)
			return
		}
	}
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
				continue // not a Raft message (e.g. a Probe); ignore
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
