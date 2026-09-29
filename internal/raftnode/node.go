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
	"github.com/adivishall/quorum/internal/snapshot"
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
//
// Phase 15 (docs/MEMBERSHIP.md, docs/MULTI_RAFT.md): a node is a member of one
// group, Group. Its genesis configuration — the configuration below its log's
// first entry — is named once, at its first start, by exactly one of Peers,
// Bootstrap and Join, and recorded in the group identity file beside the log
// (identity.go); a restart may name the same genesis again or none at all, and
// is refused if it names another. From then on the group's configuration is
// replicated state: configuration entries in the log, carried by snapshots.
type Config struct {
	ID NodeID
	// Group is the Raft group this node is a member of. It is the identity its
	// snapshots and every frame it sends carry (envelope.go). Group 0 on a
	// transport of its own is the Phase 9–14 single-group deployment.
	Group replication.GroupID
	// Peers is the Phase 9 form of the genesis: every node a voter, no
	// addresses, including ID.
	Peers []NodeID
	// Bootstrap is the genesis configuration, with addresses (Phase 15).
	Bootstrap *replication.Configuration
	// Join starts a node that is not in the genesis: its genesis is the empty
	// configuration, and it learns its group's configuration from the leader
	// that adds it (docs/MEMBERSHIP.md §4). It never campaigns until a
	// configuration makes it a voter.
	Join bool

	Transport transport.Transport
	// Inbox, if non-nil, is where the node receives its group's frames, their
	// group envelope already removed: the multi-Raft host demultiplexes one
	// transport among its groups (internal/multiraft). Nil: the node reads the
	// transport itself and drops frames of any other group.
	Inbox   <-chan transport.Envelope
	LogPath string // durable raft log file for this group

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

	// SnapshotEvery makes the node snapshot its state machine every
	// SnapshotEvery applied entries and compact its log behind the snapshot
	// (Phase 14, docs/SNAPSHOTS.md §4); 0 never does. Independently of it, a
	// node whose StateMachine is a SnapshotStateMachine installs the snapshots
	// its leader sends, and restores the published one at startup.
	SnapshotEvery uint64
	// SnapshotRetain is how many entries below a new snapshot's index the
	// compaction keeps, so a follower slightly behind catches up by entries.
	SnapshotRetain uint64
	// Metrics, if set, receives this node's instrumentation (Phase 16,
	// docs/OBSERVABILITY.md); nil instruments nothing.
	Metrics *Metrics
}

// snapshotFiles are the node's snapshot files, beside its log.
func (c Config) snapshotFiles() snapshot.Files {
	return snapshot.Files{FS: c.FS, Base: c.LogPath}
}

// genesis returns the genesis configuration the config names — Peers, Bootstrap
// or the empty one of Join — and false when it names none (a restart that
// relies on the identity file). Naming more than one is an error, as is a Peers
// list that is empty-id, duplicated or without ID (raft's sentinel errors), or
// a Bootstrap without voters.
func (c Config) genesis() (replication.Configuration, bool, error) {
	named := 0
	for _, b := range []bool{len(c.Peers) > 0, c.Bootstrap != nil, c.Join} {
		if b {
			named++
		}
	}
	if named > 1 {
		return replication.Configuration{}, false, fmt.Errorf("%w: at most one of Peers, Bootstrap and Join names the genesis", ErrIdentity)
	}
	switch {
	case len(c.Peers) > 0:
		seen := map[NodeID]bool{}
		for _, p := range c.Peers {
			if p == "" {
				return replication.Configuration{}, false, raft.ErrEmptyPeer
			}
			if seen[p] {
				return replication.Configuration{}, false, raft.ErrDuplicatePeer
			}
			seen[p] = true
		}
		if !seen[c.ID] {
			return replication.Configuration{}, false, raft.ErrIDNotInPeers
		}
		conf := replication.VotersOf(c.Peers)
		return conf, true, conf.Validate()
	case c.Bootstrap != nil:
		if err := c.Bootstrap.Validate(); err != nil {
			return replication.Configuration{}, false, err
		}
		if len(c.Bootstrap.Voters) == 0 || c.Bootstrap.Joint() {
			return replication.Configuration{}, false, fmt.Errorf("%w: a genesis needs voters and cannot be joint", ErrIdentity)
		}
		return c.Bootstrap.Clone(), true, nil
	case c.Join:
		return replication.Configuration{}, true, nil
	}
	return replication.Configuration{}, false, nil
}

// identity reconciles the group identity file with the config (docs/MEMBERSHIP.md
// §2): an existing file must name this group and, if the config names a
// genesis, the same one; without a file there may be no durable state yet (a
// log or snapshot without an identity is refused), and the config must name the
// genesis, which is then recorded durably before anything else is written.
func (c Config) identity(files snapshot.Files) (Identity, error) {
	gen, named, err := c.genesis()
	if err != nil {
		return Identity{}, err
	}
	if err := removeIdentityOrphan(c.FS, c.LogPath); err != nil {
		return Identity{}, err
	}
	id, found, err := LoadIdentity(c.FS, c.LogPath)
	if err != nil {
		return Identity{}, err
	}
	if found {
		if id.Group != c.Group {
			return Identity{}, fmt.Errorf("%w: %s holds group %d, this node is configured for group %d", ErrIdentity, c.LogPath, id.Group, c.Group)
		}
		if named && !gen.Equal(id.Genesis) {
			return Identity{}, fmt.Errorf("%w: the configured genesis %s differs from the recorded %s", ErrIdentity, gen, id.Genesis)
		}
		return id, nil
	}
	for _, p := range []string{c.LogPath, files.Path()} {
		there, err := exists(c.FS, p)
		if err != nil {
			return Identity{}, err
		}
		if there {
			return Identity{}, fmt.Errorf("%w: %s exists and the group identity file does not", ErrIdentity, p)
		}
	}
	if !named {
		return Identity{}, fmt.Errorf("%w: a first start needs a genesis: Peers, Bootstrap or Join", ErrIdentity)
	}
	id = Identity{Group: c.Group, Genesis: gen}
	if err := writeIdentity(c.FS, c.LogPath, id); err != nil {
		return Identity{}, err
	}
	return id, nil
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

// Recovered is a core rebuilt from its durable log and snapshot, together with
// the open log.
type Recovered struct {
	Core  *raft.Raft
	Log   *raftlog.Log
	Mem   *replication.MemoryLog // the core's in-memory log
	State *raftlog.Recovered     // exactly what was read from the log (after any repair)
	// Snapshot is the published snapshot the state machine was restored from
	// (nil: none); Repaired says recovery completed an interrupted install.
	Snapshot *snapshot.Meta
	Repaired bool
	// Identity is the group identity the node recovered (or recorded, on a
	// first start).
	Identity Identity
	// Durable is the log and snapshots, for the Ready cycle.
	Durable *Durable
}

// Recover rebuilds a node from its durable state (docs/SNAPSHOTS.md §6): it
// reconciles the group identity file with cfg (recording it on a first start,
// docs/MEMBERSHIP.md §2), removes orphaned temporary files, loads and fully
// validates the published snapshot (if any) — refusing one of another group —,
// opens the durable log at cfg.LogPath (on cfg.FS), and
// reconciles the two — completing an install that crashed after publishing
// its snapshot, and refusing any contradiction. The in-memory log starts at
// the log's boundary; the commit index is the persisted (clamped) value, at
// least the snapshot's index; the state machine is restored from the snapshot
// and the applied index set to it; the core starts at the recovered term and
// vote, as a Follower, with the snapshot's configuration at its index (else the
// genesis) as its base configuration — the log's configuration entries override
// it (Raft §6). It is the exact startup path of Start, exported so the
// deterministic simulator restarts nodes through the same code. It uses ID,
// Group, Peers/Bootstrap/Join, LogPath, FS, DisableSync, Rand (required here),
// ElectionTicks, HeartbeatTicks, StateMachine, SnapshotEvery and
// SnapshotRetain.
func Recover(cfg Config) (*Recovered, error) {
	if cfg.Rand == nil {
		return nil, raft.ErrNoRand
	}
	files := cfg.snapshotFiles()
	ident, err := cfg.identity(files)
	if err != nil {
		return nil, err
	}
	if err := files.RemoveOrphans(); err != nil {
		return nil, err
	}
	meta, data, _, found, err := files.Load()
	if err != nil {
		return nil, fmt.Errorf("%w: the published snapshot: %w", ErrSnapshot, err)
	}
	if found && meta.Group != cfg.Group {
		return nil, fmt.Errorf("%w: the published snapshot is of group %d, this node's is %d", snapshot.ErrWrongGroup, meta.Group, cfg.Group)
	}
	ssm, _ := cfg.StateMachine.(SnapshotStateMachine)
	if found && ssm == nil {
		return nil, fmt.Errorf("%w: a snapshot is published and the state machine cannot restore it", ErrSnapshot)
	}
	lg, rec, err := raftlog.Open(cfg.LogPath, cfg.raftlogOptions())
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Recovered, error) {
		_ = lg.Close()
		return nil, err
	}
	repaired := false
	switch {
	case found:
		repair, err := reconcile(meta, rec)
		if err != nil {
			return fail(err)
		}
		if repair {
			// The log does not record the snapshot yet: write the boundary record
			// the interrupted install would have, then read the log back.
			if err := lg.Install(meta.Index, meta.Term); err != nil {
				return fail(fmt.Errorf("%w: completing the install of (%d,%d): %w", ErrSnapshot, meta.Index, meta.Term, err))
			}
			if err := lg.Close(); err != nil {
				return nil, err
			}
			if lg, rec, err = raftlog.Open(cfg.LogPath, cfg.raftlogOptions()); err != nil {
				return nil, err
			}
			if again, err := reconcile(meta, rec); err != nil || again {
				return fail(fmt.Errorf("%w: the repaired log still disagrees with the snapshot (%v)", ErrSnapshot, err))
			}
			repaired = true
		}
	case rec.Boundary.Index > 0:
		return fail(fmt.Errorf("%w: the log is compacted through %d and no snapshot is published", ErrSnapshot, rec.Boundary.Index))
	}
	mlog := replication.NewMemoryLog()
	if b := rec.Boundary; b.Index > 0 {
		if err := mlog.InstallSnapshot(b.Index, b.Term); err != nil {
			return fail(fmt.Errorf("raftnode: recovered boundary invalid: %w", err))
		}
	}
	if len(rec.Entries) > 0 {
		if err := mlog.Append(rec.Entries...); err != nil {
			return fail(fmt.Errorf("raftnode: recovered log is inconsistent: %w", err))
		}
	}
	commit := rec.HardState.Commit
	if found {
		commit = max(commit, meta.Index)
	}
	if commit > mlog.CommitIndex() {
		if err := mlog.Commit(commit); err != nil {
			return fail(fmt.Errorf("raftnode: recovered commit invalid: %w", err))
		}
	}
	var restored *snapshot.Meta
	if found {
		if err := ssm.RestoreSnapshot(meta.Index, data); err != nil {
			return fail(fmt.Errorf("%w: restoring the published snapshot: %w", ErrSnapshot, err))
		}
		if err := mlog.Apply(meta.Index); err != nil {
			return fail(fmt.Errorf("raftnode: applied index of the snapshot: %w", err))
		}
		restored = &meta
	}
	base, baseIndex := ident.Genesis, uint64(0)
	if found {
		base, baseIndex = meta.Conf, meta.Index
	}
	core, err := raft.New(raft.Config{
		ID: cfg.ID, Conf: &base, ConfIndex: baseIndex, Rand: cfg.Rand, Log: mlog,
		ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks,
		Term: rec.HardState.Term, Vote: rec.HardState.Vote,
	})
	if err != nil {
		return fail(err)
	}
	snaps := &Snapshots{Files: files, SM: ssm, Group: cfg.Group, Every: cfg.SnapshotEvery, Retain: cfg.SnapshotRetain}
	if found {
		snaps.meta = meta
	}
	return &Recovered{Core: core, Log: lg, Mem: mlog, State: rec, Snapshot: restored, Repaired: repaired, Identity: ident,
		Durable: &Durable{Log: lg, Snap: snaps}}, nil
}

// Node is a running Raft group driver.
type Node struct {
	cfg  Config
	core *raft.Raft
	log  *raftlog.Log
	dur  *Durable // the log and the snapshots (Phase 14; actor-owned)
	tr   transport.Transport
	sm   StateMachine

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{} // closed when the actor loop exits, for any reason

	recvCh    chan raft.Message
	chunkCh   chan inChunk // snapshot chunks from peers (Phase 14)
	proposeCh chan proposal
	writeCh   chan writeReq
	readCh    chan readReq
	confCh    chan confReq
	snapCh    chan chan error
	outboxes  map[NodeID]*outbox // actor-owned; one per peer it has sent to
	waiters   *Waiters           // requests waiting for an apply (actor-owned)
	reads     *Reads             // unconfirmed ReadIndex requests (actor-owned)
	changes   []*confWait        // membership changes awaiting completion (actor-owned)

	// Phase 15: the configuration last reported (actor-owned), for the
	// raft_conf and raft_removed events and for retiring outboxes.
	confSeen    replication.Configuration
	confSeenIdx uint64
	confLogged  bool
	removed     bool
	wasMember   bool // a member of its genesis, or of a configuration it held

	mu      sync.Mutex
	status  Status
	err     error           // the fail-stop cause; nil if running or cleanly closed
	sending map[NodeID]bool // peers a snapshot is being streamed to (Phase 14)

	app atomic.Pointer[AppHandler] // Phase 13: application messages (forwarding)

	// Phase 16 (docs/OBSERVABILITY.md). m's fields are nil when the node has
	// no Metrics; the rest is actor-owned.
	m              *nodeMetrics
	store          Storage // what the Ready cycle persists through (timed if m is enabled)
	inflight       map[uint64]inflightWrite
	lastCounters   raft.Counters
	lastLeader     NodeID
	lastLeaderTerm uint64

	// completed are this cycle's applied entries, their waiters completed
	// once the cycle's Status is published (actor-owned).
	completed []appliedEntry
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
// the same connections, framing, group envelope and fault injection as Raft
// traffic. An error means the message was not handed to the connection (e.g.
// the peer is not connected): nothing was sent.
func (n *Node) SendApp(ctx context.Context, peer NodeID, kind transport.MsgKind, payload []byte) error {
	return n.tr.Send(ctx, transport.NodeID(peer), kind, WrapGroup(n.cfg.Group, payload))
}

// Group returns the group this node is a member of.
func (n *Node) Group() replication.GroupID { return n.cfg.Group }

type proposal struct {
	data   []byte
	result chan error
}

// inChunk is one snapshot chunk a peer sent (transport kind InstallSnapshot).
type inChunk struct {
	from    NodeID
	payload []byte
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
	Group     replication.GroupID
	Role      raft.Role
	Term      uint64
	Leader    NodeID
	Commit    uint64
	LastIndex uint64
	Applied   uint64
	// Phase 14: the log's compaction boundary (entries at or below it are
	// discarded) and the published snapshot's index (0: none).
	Boundary uint64
	Snapshot uint64
	// Phase 15: the node's current configuration — the latest in its log,
	// committed or not — and the index of its entry (0: the base); whether a
	// change is under way; whether this node votes in it.
	Conf        replication.Configuration
	ConfIndex   uint64
	ConfPending bool
	Voter       bool
	// Removed: a committed configuration without this node was seen — it is
	// no longer a member of its group (docs/MEMBERSHIP.md §5).
	Removed bool
	// Phase 16 (docs/OBSERVABILITY.md): the core's role transitions since the
	// node started; the writes and reads waiting on this node; the durable
	// log's length; on a leader, each other member's match index.
	Counters      raft.Counters
	PendingWrites int
	PendingReads  int
	LogBytes      int64
	FollowerMatch map[NodeID]uint64
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
		cfg: cfg, core: rc.Core, log: rc.Log, dur: rc.Durable, tr: cfg.Transport, sm: cfg.StateMachine,
		ctx: nctx, cancel: cancel, done: make(chan struct{}),
		recvCh:    make(chan raft.Message, 256),
		chunkCh:   make(chan inChunk, 16),
		sending:   map[NodeID]bool{},
		proposeCh: make(chan proposal),
		writeCh:   make(chan writeReq),
		readCh:    make(chan readReq),
		confCh:    make(chan confReq),
		snapCh:    make(chan chan error),
		outboxes:  map[NodeID]*outbox{},
		waiters:   NewWaiters(),
		reads:     NewReads(),
		m:         cfg.Metrics.forGroup(cfg.Group),
		inflight:  map[uint64]inflightWrite{},
	}
	n.store = n.dur
	if n.m.enabled() {
		n.dur.Log = timedLog{LogStore: n.dur.Log, m: n.m}
		n.store = timedStorage{Durable: n.dur, m: n.m}
	}
	n.dur.Snap.Installed = n.waiters.Installed
	n.wasMember = rc.Identity.Genesis.IsMember(cfg.ID)
	n.noteConf()
	n.snapshotStatus()
	if rc.Snapshot != nil {
		n.logf("event=raft_snapshot_restored node=%s index=%d term=%d repaired=%v group=%d", cfg.ID, rc.Snapshot.Index, rc.Snapshot.Term, rc.Repaired, cfg.Group)
	}

	cfg.Metrics.add(n) // its gauges read the Status published above

	// Read what the start line reports while this goroutine still owns the
	// core: once the actor runs, only it may touch the core (it may be
	// stepping a message already).
	term, last := rc.Core.Term(), rc.Core.LastIndex()
	conf, _ := rc.Core.Conf()
	n.syncOutboxes()
	n.wg.Add(2)
	go n.receiveLoop()
	go n.actorLoop()
	n.logf("event=raft_started node=%s peers=%d term=%d lastIndex=%d group=%d conf=%q", cfg.ID, len(conf.Members()), term, last, cfg.Group, conf.String())
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

// Snapshot makes the node snapshot its state machine at its applied index now
// and compact its log behind the snapshot, as the SnapshotEvery trigger does
// (docs/SNAPSHOTS.md §5) — an operator's or a test's request (the admin API's
// "snapshot"). Nothing applied since the published snapshot: nothing to do.
// A durability failure stops the node, as a failed Save does.
func (n *Node) Snapshot(ctx context.Context) error {
	r := make(chan error, 1)
	select {
	case n.snapCh <- r:
	case <-n.ctx.Done():
		return raft.ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-r:
		return err
	case <-n.ctx.Done():
		return raft.ErrStopped
	}
}

// Status returns one consistent snapshot of the node's state. Use it (not the
// single-field accessors) whenever more than one field is needed together.
func (n *Node) Status() Status {
	n.mu.Lock()
	st := n.status
	n.mu.Unlock()
	st.Conf = st.Conf.Clone() // the caller's own copy
	if st.FollowerMatch != nil {
		fm := make(map[NodeID]uint64, len(st.FollowerMatch))
		for id, m := range st.FollowerMatch {
			fm[id] = m
		}
		st.FollowerMatch = fm
	}
	return st
}

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
		n.cfg.Metrics.remove(n)
	}()
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		var accepted *proposal   // a proposal the core appended, answered once durable
		var snapReply chan error // a snapshot request, answered once Status shows it
		var snapErr error
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.core.Tick()
		case m := <-n.recvCh:
			_ = n.core.Step(m)
		case c := <-n.chunkCh:
			n.receiveChunk(c)
		case p := <-n.proposeCh:
			if err := n.core.Propose(p.data); err != nil {
				p.result <- err
			} else {
				accepted = &p
			}
		case w := <-n.writeCh:
			if err := n.core.Propose(w.data); err != nil {
				n.m.refused.Inc()
				w.result <- writeAccepted{err: err}
			} else {
				// The entry is the log's tail, in the current term; it completes
				// when that index is applied — with this term, or as ErrLost.
				idx, term := n.core.LastIndex(), n.core.Term()
				n.m.accepted.Inc()
				n.trackAccepted(idx, term)
				w.result <- writeAccepted{index: idx, term: term, done: n.waiters.Add(idx, term, n.core.AppliedIndex())}
			}
		case r := <-n.readCh:
			rs, err := n.core.ReadIndex()
			if err != nil {
				r.result <- readAccepted{err: err}
			} else {
				r.result <- readAccepted{done: n.reads.Add(rs.ID, n.core.Term())}
			}
		case c := <-n.confCh:
			if err := n.core.ProposeConfChange(c.cc); err != nil {
				n.m.confChange(c.cc.Type, "refused")
				c.result <- confOutcome{err: err}
			} else {
				n.changes = append(n.changes, &confWait{since: n.core.LastIndex(), term: n.core.Term(), typ: c.cc.Type, result: c.result})
			}
		case r := <-n.snapCh:
			if n.dur.Snap.SM == nil {
				r <- fmt.Errorf("%w: this node's state machine cannot snapshot", ErrSnapshot)
				break
			}
			before := n.dur.Snap.Published().Index
			start := time.Now()
			err := n.dur.Snapshot(n.core, n.cfg.Hook)
			if err != nil && !errors.Is(err, snapshot.ErrTooLarge) && !errors.Is(err, raft.ErrConfUnknown) {
				r <- err
				n.fail(err) // a durability failure, exactly as in processReady
				return
			}
			snapReply, snapErr = r, err
			if after := n.dur.Snap.Published(); after.Index != before {
				n.m.snapRequest.Inc()
				n.m.snapCreate.Since(start)
				b, _ := n.core.Boundary()
				n.logf("event=raft_snapshot node=%s index=%d term=%d boundary=%d group=%d trigger=request", n.cfg.ID, after.Index, after.Term, b, n.cfg.Group)
			}
		}
		if err := n.processReady(); err != nil {
			if accepted != nil {
				accepted.result <- err
			}
			if snapReply != nil {
				snapReply <- err
			}
			n.fail(err)
			return
		}
		if accepted != nil {
			accepted.result <- nil
		}
		if snapReply != nil {
			snapReply <- snapErr
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
	defer n.dur.Snap.Unstage() // a staged snapshot lives for one cycle at most
	// Writes applied in this cycle complete when it ends — after the Status
	// that covers them is published (snapshotStatus, below), so no client that
	// has seen its write complete reads a Status that has not applied it. On
	// an early return the completions are delivered all the same: the entries
	// were applied.
	defer n.completeApplied()
	if err := DrainReadyAt(n.core, n.store, n.enqueue, n.confirmRead, n.cfg.Hook); err != nil {
		return err
	}
	n.trackCommitted()
	if err := ApplyCommitted(n.core, n.sm, n.cfg.Hook, n.applied); err != nil {
		if !errors.Is(err, ErrApply) {
			return err // a crash point fired
		}
		n.logf("event=raft_apply_failed node=%s group=%d err=%v", n.cfg.ID, n.cfg.Group, err)
	}
	n.trackApplied()
	// Phase 14: snapshot and compact when the trigger says so. A state too
	// large to snapshot is reported and skipped; any other failure is a
	// durability failure and stops the node, as a failed Save does.
	before := n.dur.Snap.Published().Index
	snapStart := time.Now()
	if err := n.dur.MaybeSnapshot(n.core, n.cfg.Hook); err != nil {
		if !errors.Is(err, snapshot.ErrTooLarge) && !errors.Is(err, raft.ErrConfUnknown) {
			return err
		}
		n.logf("event=raft_snapshot_skipped node=%s applied=%d group=%d err=%v", n.cfg.ID, n.core.AppliedIndex(), n.cfg.Group, err)
	}
	if after := n.dur.Snap.Published(); after.Index != before {
		n.m.snapPeriodic.Inc()
		n.m.snapCreate.Since(snapStart)
		b, _ := n.core.Boundary()
		n.logf("event=raft_snapshot node=%s index=%d term=%d boundary=%d group=%d trigger=periodic", n.cfg.ID, after.Index, after.Term, b, n.cfg.Group)
	}
	// A read registered in a term this node no longer leads will never be
	// confirmed (the core dropped it): tell its client to go elsewhere.
	n.reads.DropStale(n.core.Term(), n.core.Role() == raft.Leader)
	if n.noteConf() {
		n.syncOutboxes()
	}
	n.settleChanges()
	n.snapshotStatus()
	return nil
}

// confirmRead is DrainReadyAt's hand-off of a confirmed ReadIndex: the read now
// waits only for the state machine to reach its index.
func (n *Node) confirmRead(rs raft.ReadState) {
	n.reads.Confirmed(rs, n.waiters, n.core.AppliedIndex())
}

// applied is ApplyCommitted's hand-off after an entry is applied and recorded:
// the write that proposed it (or a read barrier at its index) completes, with
// the state machine's result, when the cycle ends (completeApplied).
func (n *Node) applied(e raft.Entry, result any) {
	n.completed = append(n.completed, appliedEntry{index: e.Index, term: e.Term, result: result})
}

// appliedEntry is an applied entry whose waiters complete at the end of the
// cycle (processReady).
type appliedEntry struct {
	index, term uint64
	result      any
}

// completeApplied completes the waiters of every entry applied this cycle.
func (n *Node) completeApplied() {
	for i, a := range n.completed {
		n.waiters.Applied(a.index, a.term, a.result)
		n.completed[i] = appliedEntry{}
	}
	n.completed = n.completed[:0]
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
	n.logf("event=raft_persist_failed node=%s group=%d err=%v", n.cfg.ID, n.cfg.Group, err)
	n.waiters.FailAll(err)
	n.reads.FailAll(err)
	n.cancel()
}

// outbox is one peer's queue of messages awaiting transmission and the stop
// signal of its sender goroutine.
type outbox struct {
	ch   chan raft.Message
	stop chan struct{}
}

// outboxFor returns peer's outbox, starting its sender on first use (Phase 15:
// the set of peers follows the configuration, so outboxes are made as the core
// first addresses a peer).
func (n *Node) outboxFor(peer NodeID) *outbox {
	if ob := n.outboxes[peer]; ob != nil {
		return ob
	}
	ob := &outbox{ch: make(chan raft.Message, OutboxSize), stop: make(chan struct{})}
	n.outboxes[peer] = ob
	n.wg.Add(1)
	go n.senderLoop(ob)
	return ob
}

// syncOutboxes gives every other member of the current configuration an outbox
// and stops those of nodes that are no longer members (their queued messages
// are dropped: a node outside the configuration has nothing to learn from
// this one, and one that returns is replicated to afresh).
func (n *Node) syncOutboxes() {
	conf, _ := n.core.Conf()
	for _, p := range conf.Members() {
		if p != n.cfg.ID {
			n.outboxFor(p)
		}
	}
	for p, ob := range n.outboxes {
		if !conf.IsMember(p) {
			close(ob.stop)
			delete(n.outboxes, p)
		}
	}
}

// enqueue hands a message (already persisted-for, by DrainReady's ordering) to its
// peer's outbox without ever blocking the actor. A full outbox drops the message;
// Raft's heartbeats retransmit whatever it carried.
func (n *Node) enqueue(m raft.Message) {
	if m.Type == raft.MsgSnapshot {
		n.startTransfer(m)
		return
	}
	if m.To == "" || m.To == n.cfg.ID {
		n.logf("event=raft_send_dropped node=%s to=%s type=%s reason=unknown_peer group=%d", n.cfg.ID, m.To, m.Type, n.cfg.Group)
		return
	}
	ob := n.outboxFor(m.To)
	select {
	case ob.ch <- m:
	default:
		n.m.outboxFull.Inc()
		n.logf("event=raft_send_dropped node=%s to=%s type=%s reason=outbox_full group=%d", n.cfg.ID, m.To, m.Type, n.cfg.Group)
	}
}

// senderLoop transmits one peer's messages in order. It is the only goroutine that
// can block on that peer's connection.
func (n *Node) senderLoop(ob *outbox) {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ob.stop:
			return
		case m := <-ob.ch:
			n.sendMessage(m)
		}
	}
}

// sendMessage marshals a core message and sends it over the transport, in its
// group's envelope. A peer that is not currently connected is expected (the
// dialer reconnects); the message is dropped and Raft retransmits on the next
// tick.
func (n *Node) sendMessage(m raft.Message) {
	kind, ok := kindForType(m.Type)
	if !ok {
		return
	}
	if err := n.tr.Send(n.ctx, transport.NodeID(m.To), kind, WrapGroup(n.cfg.Group, m.Marshal())); err != nil {
		// ErrPeerNotConnected / a write error: fine, Raft is retransmission-based.
		n.m.sendFailed.Inc()
		n.logf("event=raft_send_failed node=%s to=%s type=%s group=%d err=%v", n.cfg.ID, m.To, m.Type, n.cfg.Group, err)
	}
}

// receiveLoop decodes inbound transport frames into core messages and feeds them
// to the actor. The message's sender is the connection's handshake identity
// (env.Peer), never a payload field (INV-T4). Reading the transport itself (no
// Inbox), it removes each frame's group envelope and drops frames of other
// groups; from an Inbox, the host has done both.
func (n *Node) receiveLoop() {
	defer n.wg.Done()
	rc, unwrap := n.cfg.Inbox, false
	if rc == nil {
		rc, unwrap = n.tr.Receive(), true
	}
	for {
		select {
		case <-n.ctx.Done():
			return
		case env, ok := <-rc:
			if !ok {
				return // transport closed
			}
			if unwrap {
				g, payload, err := UnwrapGroup(env.Payload)
				if err != nil || g != n.cfg.Group {
					n.m.wrongGroup.Inc()
					n.logf("event=raft_frame_dropped node=%s group=%d from=%s kind=%d frame_group=%d err=%v", n.cfg.ID, n.cfg.Group, env.Peer, env.Kind, g, err)
					continue
				}
				env.Payload = payload
			}
			if env.Kind == transport.MsgInstallSnapshot {
				select {
				case n.chunkCh <- inChunk{from: NodeID(env.Peer), payload: env.Payload}:
				case <-n.ctx.Done():
					return
				}
				continue
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
				n.m.decode.Inc()
				n.logf("event=raft_decode_failed node=%s from=%s group=%d err=%v", n.cfg.ID, env.Peer, n.cfg.Group, err)
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
	b, _ := n.core.Boundary()
	st := Status{
		Group: n.cfg.Group,
		Role:  n.core.Role(), Term: n.core.Term(), Leader: n.core.LeaderID(),
		Commit: n.core.CommitIndex(), LastIndex: n.core.LastIndex(), Applied: n.core.AppliedIndex(),
		Boundary: b, Snapshot: n.dur.Snap.Published().Index,
		Conf: n.confSeen, ConfIndex: n.confSeenIdx, ConfPending: n.core.ConfPending(), Voter: n.core.IsVoter(),
		Removed:  n.removed,
		Counters: n.core.Counters(), PendingWrites: n.waiters.Len(), PendingReads: n.reads.Len(),
		LogBytes: n.log.Size(), FollowerMatch: n.core.Progress(),
	}
	n.observeStatus(st)
	n.mu.Lock()
	n.status = st
	n.mu.Unlock()
}

// startTransfer answers the core's offer of a snapshot to a follower (Phase
// 14, docs/SNAPSHOTS.md §8) by streaming the published snapshot — at or beyond
// the core's boundary, which a compaction never passes — in chunks on its own
// goroutine, so the peer's heartbeats keep flowing meanwhile (a chunk does not
// reset a follower's election timer; a heartbeat does). At most one transfer
// runs per peer; an offer while one runs is ignored. A failed send abandons
// the transfer, and the core offers again when its offer goes unanswered.
func (n *Node) startTransfer(m raft.Message) {
	meta, file, err := n.dur.Snap.SendFile()
	if err == nil && meta.Index < m.SnapshotIndex {
		err = fmt.Errorf("%w: published snapshot %d is below the log's boundary %d", ErrSnapshot, meta.Index, m.SnapshotIndex)
	}
	if err != nil {
		n.m.snapFailed.Inc()
		n.logf("event=raft_snapshot_send_failed node=%s to=%s group=%d err=%v", n.cfg.ID, m.To, n.cfg.Group, err)
		return
	}
	n.mu.Lock()
	busy := n.sending[m.To]
	n.sending[m.To] = true
	n.mu.Unlock()
	if busy {
		return
	}
	n.wg.Add(1)
	go n.transfer(m.To, m.Term, meta, file)
}

// transfer streams one snapshot to one peer.
func (n *Node) transfer(peer NodeID, term uint64, meta snapshot.Meta, file []byte) {
	defer n.wg.Done()
	defer func() {
		n.mu.Lock()
		delete(n.sending, peer)
		n.mu.Unlock()
	}()
	chunks := snapshot.Split(term, meta, file)
	for _, c := range chunks {
		if err := n.tr.Send(n.ctx, transport.NodeID(peer), transport.MsgInstallSnapshot, WrapGroup(n.cfg.Group, c.Marshal())); err != nil {
			n.m.snapFailed.Inc()
			n.logf("event=raft_snapshot_send_failed node=%s to=%s index=%d offset=%d group=%d err=%v", n.cfg.ID, peer, meta.Index, c.Offset, n.cfg.Group, err)
			return
		}
	}
	n.m.snapSent.Inc()
	n.logf("event=raft_snapshot_sent node=%s to=%s index=%d term=%d bytes=%d chunks=%d group=%d", n.cfg.ID, peer, meta.Index, meta.Term, len(file), len(chunks), n.cfg.Group)
}

// receiveChunk takes one chunk of a snapshot a peer is sending. A complete,
// valid snapshot is stepped into the core as its MsgSnapshot; the Ready cycle
// that follows installs it (or the core ignores it as already covered).
func (n *Node) receiveChunk(c inChunk) {
	m, err := n.dur.Snap.Receive(c.from, c.payload)
	if err != nil {
		n.logf("event=raft_snapshot_refused node=%s from=%s group=%d err=%v", n.cfg.ID, c.from, n.cfg.Group, err)
		return
	}
	if m == nil {
		return
	}
	m.To = n.cfg.ID
	n.logf("event=raft_snapshot_received node=%s from=%s index=%d term=%d group=%d", n.cfg.ID, c.from, m.SnapshotIndex, m.SnapshotTerm, n.cfg.Group)
	_ = n.core.Step(*m)
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
