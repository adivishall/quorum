// Package raftnode is the node driver for one Raft group (Phase 9, docs/RAFT.md
// §1, ADR-016). It is the impure layer around the pure internal/raft core: it owns
// the actor goroutine, the tick source, the durable log (internal/raftlog), the
// transport adapter, and the apply loop. The core owns none of these.
//
// A single goroutine (the actor loop) owns the core and the log, so the core needs
// no locks (ADR-002). External callers reach it only through channels (Propose) or
// a mutex-guarded status snapshot (Role/Term/LeaderID). The driver honours the
// Ready contract: it persists a Ready's HardState and Entries durably BEFORE
// sending its Messages, which is what makes durability-before-reply hold (INV-R6).
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
)

// DefaultTickInterval is the wall-clock duration of one logical tick
// (docs/DESIGN.md §8.3). The core only counts ticks; this is where they come from.
const DefaultTickInterval = 50 * time.Millisecond

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

	Rand *rand.Rand // optional; defaults to a seed derived from ID
	Logf func(string, ...any)
}

// raftlogOptions returns the durable-log options this config implies. The default
// (zero-value) config is durable (Sync: true); only an explicit DisableSync turns
// fsync off. Start uses this exact function, so a test that pins its result pins
// the real durability default.
func (c Config) raftlogOptions() raftlog.Options {
	return raftlog.Options{Sync: !c.DisableSync}
}

// NodeID re-exported for callers.
type NodeID = raft.NodeID

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

	recvCh    chan raft.Message
	proposeCh chan proposal

	mu     sync.Mutex
	status status
}

type proposal struct {
	data   []byte
	result chan error
}

type status struct {
	role   raft.Role
	term   uint64
	leader NodeID
	commit uint64
}

// Start recovers durable state, constructs the core, and launches the actor and
// receive goroutines. The caller owns the transport's lifecycle (Start does not
// close it); Close stops the driver and the durable log.
func Start(ctx context.Context, cfg Config) (*Node, error) {
	if cfg.TickInterval == 0 {
		cfg.TickInterval = DefaultTickInterval
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(seedFromID(cfg.ID)))
	}

	lg, rec, err := raftlog.Open(cfg.LogPath, cfg.raftlogOptions())
	if err != nil {
		return nil, err
	}
	// Rebuild the in-memory log from the durable entries and set the recovered
	// commit index.
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

	nctx, cancel := context.WithCancel(ctx)
	n := &Node{
		cfg: cfg, core: core, log: lg, tr: cfg.Transport, sm: cfg.StateMachine,
		ctx: nctx, cancel: cancel,
		recvCh:    make(chan raft.Message, 256),
		proposeCh: make(chan proposal),
	}
	n.snapshotStatus()

	n.wg.Add(2)
	go n.receiveLoop()
	go n.actorLoop()
	n.logf("event=raft_started node=%s peers=%d term=%d lastIndex=%d", cfg.ID, len(cfg.Peers), core.Term(), core.LastIndex())
	return n, nil
}

// Propose submits a command to be appended and replicated. It returns
// raft.ErrNotLeader if this node is not the leader. It reports acceptance into the
// log, not commitment (Phase 9 does not implement client commit-wait).
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
	}
}

// Role, Term, LeaderID, CommitIndex return a consistent status snapshot.
func (n *Node) Role() raft.Role  { n.mu.Lock(); defer n.mu.Unlock(); return n.status.role }
func (n *Node) Term() uint64     { n.mu.Lock(); defer n.mu.Unlock(); return n.status.term }
func (n *Node) LeaderID() NodeID { n.mu.Lock(); defer n.mu.Unlock(); return n.status.leader }
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.status.commit
}

// Close stops the driver: it cancels the actor and receive loops, waits for them,
// and closes the durable log. It is idempotent. It does not close the transport.
func (n *Node) Close() error {
	n.cancel()
	n.wg.Wait()
	return n.log.Close()
}

// actorLoop is the single goroutine that owns the core and the log.
func (n *Node) actorLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.core.Tick()
		case m := <-n.recvCh:
			_ = n.core.Step(m)
		case p := <-n.proposeCh:
			p.result <- n.core.Propose(p.data)
		}
		n.processReady()
	}
}

// processReady performs the core's pending effects in the required order: persist
// (fsync) HardState + Entries first, then send messages, then apply committed
// entries. Persist-before-send is what makes INV-R6 hold.
func (n *Node) processReady() {
	for n.core.HasReady() {
		rd := n.core.Ready()
		var hs *raftlog.HardState
		if rd.HardState != nil {
			hs = &raftlog.HardState{Term: rd.HardState.Term, Vote: rd.HardState.Vote, Commit: rd.HardState.Commit}
		}
		if hs != nil || len(rd.Entries) > 0 {
			if err := n.log.Save(hs, rd.Entries); err != nil {
				// A durable-write failure must not be treated as success. There is
				// no safe way to continue as leader, so stop the node (§31).
				n.logf("event=raft_persist_failed node=%s err=%v", n.cfg.ID, err)
				n.cancel()
				return
			}
		}
		for _, m := range rd.Messages {
			n.sendMessage(m)
		}
		n.core.Advance()
	}
	n.applyCommitted()
	n.snapshotStatus()
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
	n.status = status{role: n.core.Role(), term: n.core.Term(), leader: n.core.LeaderID(), commit: n.core.CommitIndex()}
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
