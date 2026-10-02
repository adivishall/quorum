// Package multiraft is the node host of Phase 15 (docs/MULTI_RAFT.md): it runs
// the Raft groups one node process is a member of over ONE transport. Each
// group is an independent consensus domain — its own raftnode.Node (its own
// core, actor, durable log, snapshots, state machine, configuration and leader)
// under its own directory — and the host shares nothing mutable between them.
// What the host owns is only what is per-process:
//
//   - the group registry and lifecycle: recovering every group found on disk
//     at startup (a group that fails to recover is reported and left down; the
//     others start), creating a group explicitly (bootstrap or join), stopping
//     one;
//   - the demultiplexer: one goroutine reads the transport, removes each
//     frame's group envelope (raftnode.UnwrapGroup) and hands the frame to that
//     group's bounded inbox — or drops it (an unknown or stopped group, a
//     malformed envelope, a full inbox), never delivering it anywhere else;
//   - the transport's peer set: the union of the addresses the hosted groups'
//     configurations name, plus the static peers, kept in step with the
//     configurations (transport.PeerSet). The configurations are read from the
//     groups — replicated state — and never held authoritatively here.
//
// It never decides membership: a group's configuration changes only through
// its own log (raftnode.Node.ChangeMembership, docs/MEMBERSHIP.md).
package multiraft

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/transport"
	"github.com/adivishall/quorum/internal/vfs"
)

// GroupID and NodeID are re-exported for callers.
type (
	GroupID = replication.GroupID
	NodeID  = raftnode.NodeID
)

// DefaultInboxSize bounds each group's queue of inbound frames. A group whose
// actor falls behind loses frames beyond it — Raft retransmits — rather than
// stalling the demultiplexer, and with it every other group (INV-MB9).
const DefaultInboxSize = 1024

// Errors.
var (
	// ErrGroupExists: Create for a group that is already running.
	ErrGroupExists = errors.New("multiraft: the group is already hosted")
	// ErrNoGroup: the host runs no such group.
	ErrNoGroup = errors.New("multiraft: no such group on this node")
	// ErrClosed: the host is closed.
	ErrClosed = errors.New("multiraft: host closed")
	// ErrGroupBusy: the group is being started or stopped; retry once that
	// is done. Starts and stops of one group never overlap (audit H3).
	ErrGroupBusy = errors.New("multiraft: the group is starting or stopping")
)

// Config configures a Host.
type Config struct {
	ID NodeID
	// DataDir holds the groups: DataDir/groups/<group id>/raft.log and the
	// files beside it (docs/MULTI_RAFT.md §5).
	DataDir string
	// LogPathFor, if set, places group g's log elsewhere (the single-group
	// deployment keeps its Phase 9–14 path). Groups placed by it are not
	// discovered at startup: their owner creates them.
	LogPathFor func(GroupID) string
	// Transport is shared by every group. When it is a transport.PeerSet the
	// host keeps its peers in step with the groups' configurations.
	Transport transport.Transport
	// StaticPeers are transport peers the host never removes (the operator's
	// -peers). A joiner reaches its group's members through them.
	StaticPeers map[NodeID]string
	// NewStateMachine makes a group's state machine (the key-value store).
	NewStateMachine func(GroupID) raftnode.StateMachine
	// Node settings every group uses (raftnode.Config).
	TickInterval                  time.Duration
	DisableSync                   bool
	FS                            vfs.FS
	Hook                          raftnode.Hook
	SnapshotEvery, SnapshotRetain uint64
	ElectionTicks, HeartbeatTicks int
	InboxSize                     int
	Logf                          func(string, ...any)
	// OnGroup, if set, is told when a group starts (node non-nil) and when it
	// stops (node nil) — the client front's registry follows it.
	OnGroup func(g GroupID, node *raftnode.Node, sm raftnode.StateMachine)
	// Metrics, if set, receives the host's and every group's instrumentation
	// (Phase 16, docs/OBSERVABILITY.md).
	Metrics *metrics.Registry
}

// Group is one hosted group.
type Group struct {
	ID   GroupID
	Node *raftnode.Node
	SM   raftnode.StateMachine
}

type hosted struct {
	g     *Group
	inbox chan transport.Envelope
}

// Host runs a node's groups.
type Host struct {
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	groups map[GroupID]*hosted
	// busy holds a group while it is being started or stopped (audit H3).
	// A start reserves its group before it touches the group's files and
	// keeps the reservation until the group is registered and announced; a
	// stop takes the group out of groups into busy and keeps it there until
	// its node is closed. So a group never has two drivers on its log — two
	// starts cannot both pass the existence check, and a start cannot open
	// the log while a stop is still closing it — and OnGroup's attach and
	// detach of one group strictly alternate.
	busy        map[GroupID]string
	transitions sync.WaitGroup    // the starts and stops in flight; Close waits for them
	failed      map[GroupID]error // groups on disk that did not recover
	added       map[NodeID]string // transport peers the host added
	dropped     map[string]uint64 // frames dropped, by reason (observability)
	closed      bool

	nodeMetrics *raftnode.Metrics // shared by every group's node (nil: none)
	retired     *metrics.Counter
	failures    *metrics.Counter
}

// GroupDir is where group g lives under dataDir.
func GroupDir(dataDir string, g GroupID) string {
	return filepath.Join(dataDir, "groups", strconv.FormatUint(uint64(g), 10))
}

// LogPath is group g's Raft log under dataDir.
func LogPath(dataDir string, g GroupID) string {
	return filepath.Join(GroupDir(dataDir, g), "raft.log")
}

// Start recovers every group found under cfg.DataDir (docs/MULTI_RAFT.md §5):
// a directory groups/<id>/ with a group identity file. Each recovers on its
// own; one that fails is recorded (Failed) and left down, and the others
// start — a failure recovering one group neither blocks nor touches another.
// Groups are started in ascending id order, so startup is deterministic.
func Start(ctx context.Context, cfg Config) (*Host, error) {
	if cfg.ID == "" || cfg.DataDir == "" || cfg.Transport == nil || cfg.NewStateMachine == nil {
		return nil, fmt.Errorf("multiraft: ID, DataDir, Transport and NewStateMachine are required")
	}
	if cfg.TickInterval < 0 {
		return nil, fmt.Errorf("multiraft: tick interval %s is negative", cfg.TickInterval)
	}
	if cfg.InboxSize <= 0 {
		cfg.InboxSize = DefaultInboxSize
	}
	hctx, cancel := context.WithCancel(ctx)
	h := &Host{cfg: cfg, ctx: hctx, cancel: cancel, groups: map[GroupID]*hosted{}, busy: map[GroupID]string{}, failed: map[GroupID]error{},
		added: map[NodeID]string{}, dropped: map[string]uint64{}}
	h.instrument(cfg.Metrics)
	for id, addr := range cfg.StaticPeers {
		if ps, ok := cfg.Transport.(transport.PeerSet); ok && id != cfg.ID {
			if err := ps.AddPeer(transport.NodeID(id), addr); err != nil {
				cancel()
				return nil, err
			}
		}
	}
	var ids []GroupID
	if cfg.LogPathFor == nil {
		var err error
		if ids, err = h.onDisk(); err != nil {
			cancel()
			return nil, err
		}
	}
	for _, g := range ids {
		if _, err := h.start(g, raftnode.Config{}); err != nil {
			h.failed[g] = err
			h.failures.Inc()
			h.logf("event=group_failed node=%s group=%d err=%v", cfg.ID, g, err)
		}
	}
	h.wg.Add(2)
	go h.demux()
	go h.peerLoop()
	return h, nil
}

// mkdirDurable creates dir and any missing parents below DataDir, fsyncing
// each new directory's parent so the new entry survives a power loss: a group
// directory that vanished would make its node forget it ever held the group's
// durable state.
func (h *Host) mkdirDurable(dir string) error { return mkdirDurable(h.cfg.FS, dir) }

// Prepare records group g's first-start identity in cfg's data directory — a
// genesis member's (bootstrap non-nil) or a joiner's — creating its directory
// durably, without starting it: Start, run afterwards, finds the group and
// starts it from that identity. A node initializing its data directory
// prepares every group before any runs, so that no group ever ran in a
// directory whose initialization did not finish (cmd/dkvd, audit H1).
func Prepare(cfg Config, g GroupID, bootstrap *replication.Configuration) error {
	logPath := LogPath(cfg.DataDir, g)
	if cfg.LogPathFor != nil {
		logPath = cfg.LogPathFor(g)
	} else if err := mkdirDurable(cfg.FS, GroupDir(cfg.DataDir, g)); err != nil {
		return err
	}
	return raftnode.Prepare(raftnode.Config{ID: cfg.ID, Group: g, LogPath: logPath, FS: cfg.FS,
		Join: bootstrap == nil, Bootstrap: bootstrap})
}

func mkdirDurable(fsys vfs.FS, dir string) error {
	var missing []string
	for d := dir; ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		missing = append(missing, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
		if err := vfs.Or(fsys).SyncDir(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}

// onDisk lists the group ids with a directory under DataDir/groups.
func (h *Host) onDisk() ([]GroupID, error) {
	entries, err := os.ReadDir(filepath.Join(h.cfg.DataDir, "groups"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []GroupID
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		v, err := strconv.ParseUint(e.Name(), 10, 32)
		if err != nil || strconv.FormatUint(v, 10) != e.Name() {
			h.logf("event=group_dir_ignored node=%s dir=%q", h.cfg.ID, e.Name())
			continue
		}
		ids = append(ids, GroupID(v))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// Create starts group g on this node for the first time — as a member of its
// genesis (bootstrap non-nil) or as a joiner (bootstrap nil), which the
// group's leader then adds (docs/MEMBERSHIP.md §4). If the group's directory
// already holds it, its recorded identity must agree (raftnode refuses
// otherwise) and it simply starts. Creating a running group is ErrGroupExists.
func (h *Host) Create(g GroupID, bootstrap *replication.Configuration) (*Group, error) {
	nc := raftnode.Config{Join: bootstrap == nil, Bootstrap: bootstrap}
	return h.start(g, nc)
}

// Open starts group g again from its own files — its identity file names its
// genesis — after Stop, or after it failed to recover and was repaired. It is
// ErrGroupExists if the group runs, and refused if no identity file exists
// (a first start needs Create).
func (h *Host) Open(g GroupID) (*Group, error) { return h.start(g, raftnode.Config{}) }

// start runs group g's node with the genesis nc names (none: the identity
// file supplies it). The group is reserved for the whole start (busy), so
// nothing else starts or stops it meanwhile.
func (h *Host) start(g GroupID, nc raftnode.Config) (*Group, error) {
	if err := h.reserve(g, "starting"); err != nil {
		return nil, err
	}
	defer h.release(g)
	logPath := LogPath(h.cfg.DataDir, g)
	created := false
	if h.cfg.LogPathFor != nil {
		logPath = h.cfg.LogPathFor(g)
	} else {
		dir := GroupDir(h.cfg.DataDir, g)
		if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
			created = true
		}
		if err := h.mkdirDurable(dir); err != nil {
			return nil, err
		}
	}
	sm := h.cfg.NewStateMachine(g)
	inbox := make(chan transport.Envelope, h.cfg.InboxSize)
	node, err := raftnode.Start(h.ctx, h.nodeConfig(g, nc, logPath, sm, inbox))
	if err != nil {
		if created {
			// A start that failed before recording anything (an unknown
			// group, no genesis) leaves no directory behind to be found and
			// reported as a failed group at every later start. Remove only
			// succeeds on an empty directory: anything the start did write
			// stays.
			_ = os.Remove(GroupDir(h.cfg.DataDir, g))
		}
		return nil, err
	}
	grp := &Group{ID: g, Node: node, SM: sm}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = node.Close()
		return nil, ErrClosed
	}
	h.groups[g] = &hosted{g: grp, inbox: inbox}
	delete(h.failed, g)
	h.mu.Unlock()
	h.logf("event=group_started node=%s group=%d", h.cfg.ID, g)
	// Announced while the group is still reserved: a Stop cannot detach it
	// before this attach.
	if h.cfg.OnGroup != nil {
		h.cfg.OnGroup(g, node, sm)
	}
	h.syncPeers()
	return grp, nil
}

// nodeConfig completes group g's driver configuration from the host's. Every
// raftnode.Config field is set here or named in hostLeavesUnset with the
// reason (TestHostForwardsEveryNodeSetting), so a setting added to the driver
// cannot be silently dropped on its way from the process to its groups.
func (h *Host) nodeConfig(g GroupID, nc raftnode.Config, logPath string, sm raftnode.StateMachine, inbox chan transport.Envelope) raftnode.Config {
	nc.ID, nc.Group, nc.Transport, nc.Inbox = h.cfg.ID, g, h.cfg.Transport, inbox
	nc.LogPath, nc.StateMachine = logPath, sm
	nc.TickInterval, nc.DisableSync, nc.FS, nc.Hook = h.cfg.TickInterval, h.cfg.DisableSync, h.cfg.FS, h.cfg.Hook
	nc.SnapshotEvery, nc.SnapshotRetain = h.cfg.SnapshotEvery, h.cfg.SnapshotRetain
	nc.ElectionTicks, nc.HeartbeatTicks = h.cfg.ElectionTicks, h.cfg.HeartbeatTicks
	nc.Logf = h.cfg.Logf
	nc.Metrics = h.nodeMetrics
	return nc
}

// hostLeavesUnset are the raftnode.Config fields nodeConfig deliberately does
// not set, and why.
var hostLeavesUnset = map[string]string{
	"Peers":     "the legacy single-group genesis; a hosted group's genesis is Bootstrap or Join",
	"Bootstrap": "set by Create for a genesis member; Open reads the identity file",
	"Join":      "set by Create for a joiner",
	"Rand":      "raftnode seeds it from the node and the group, so groups on one node do not share a timeout sequence",
	// The core's AppendEntries budgets: its defaults fit every deployment
	// (the transport's frame, the decoder's count); tests set them directly.
	"MaxEntriesPerMsg": "the core's default budget",
	"MaxSizePerMsg":    "the core's default budget",
}

// reserve marks g busy, or reports why it cannot be: the host is closed, the
// group is running, or another start or stop of it is in flight.
func (h *Host) reserve(g GroupID, what string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case h.closed:
		return ErrClosed
	case h.busy[g] != "":
		return fmt.Errorf("%w: group %d is %s", ErrGroupBusy, g, h.busy[g])
	case h.groups[g] != nil:
		return fmt.Errorf("%w: group %d", ErrGroupExists, g)
	}
	h.busy[g] = what
	h.transitions.Add(1)
	return nil
}

// release ends g's transition.
func (h *Host) release(g GroupID) {
	h.mu.Lock()
	delete(h.busy, g)
	h.mu.Unlock()
	h.transitions.Done()
}

// Stop stops group g's node, keeping its files: a later Start of the host (or
// Create) recovers it. Frames for it are dropped meanwhile. The group stays
// reserved until its node is closed, so no start opens its log before then.
func (h *Host) Stop(g GroupID) error { return h.stop(g, nil) }

// stop is Stop, calling announce (if any) once the stop is certain — the group
// reserved for it — and before anything takes effect.
func (h *Host) stop(g GroupID, announce func()) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	if what := h.busy[g]; what != "" {
		h.mu.Unlock()
		return fmt.Errorf("%w: group %d is %s", ErrGroupBusy, g, what)
	}
	hg, ok := h.groups[g]
	if !ok {
		h.mu.Unlock()
		return fmt.Errorf("%w: group %d", ErrNoGroup, g)
	}
	delete(h.groups, g)
	h.busy[g] = "stopping"
	h.transitions.Add(1)
	h.mu.Unlock()
	defer h.release(g)
	if announce != nil {
		announce()
	}
	if h.cfg.OnGroup != nil {
		h.cfg.OnGroup(g, nil, nil)
	}
	err := hg.g.Node.Close()
	h.logf("event=group_stopped node=%s group=%d", h.cfg.ID, g)
	h.syncPeers()
	return err
}

// Group returns hosted group g, or nil.
func (h *Host) Group(g GroupID) *Group {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hg, ok := h.groups[g]; ok {
		return hg.g
	}
	return nil
}

// Groups returns the hosted group ids, ascending.
func (h *Host) Groups() []GroupID {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]GroupID, 0, len(h.groups))
	for g := range h.groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Failed returns the groups found on disk that did not recover, and why.
func (h *Host) Failed() map[GroupID]error {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[GroupID]error, len(h.failed))
	for g, err := range h.failed {
		out[g] = err
	}
	return out
}

// Dropped returns how many inbound frames were dropped, by reason.
func (h *Host) Dropped() map[string]uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]uint64, len(h.dropped))
	for k, v := range h.dropped {
		out[k] = v
	}
	return out
}

// Close stops every group and the host's goroutines. It does not close the
// transport (its owner does).
func (h *Host) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	groups := make([]*hosted, 0, len(h.groups))
	for _, hg := range h.groups {
		groups = append(groups, hg)
	}
	h.groups = map[GroupID]*hosted{}
	h.mu.Unlock()
	h.cancel()
	var first error
	for _, hg := range groups {
		if err := hg.g.Node.Close(); err != nil && first == nil {
			first = err
		}
	}
	// A start or stop in flight finishes on its own (a start that finds the
	// host closed closes the node it started); Close returns only once no
	// node it did not close can still be running.
	h.transitions.Wait()
	h.wg.Wait()
	return first
}

// demux delivers each inbound frame to the group its envelope names, or drops
// it (INV-MB6). It never blocks on a group: a full inbox drops the frame.
func (h *Host) demux() {
	defer h.wg.Done()
	rc := h.cfg.Transport.Receive()
	for {
		select {
		case <-h.ctx.Done():
			return
		case env, ok := <-rc:
			if !ok {
				return
			}
			g, payload, err := raftnode.UnwrapGroup(env.Payload)
			if err != nil {
				h.drop("malformed", env, 0, err)
				continue
			}
			h.mu.Lock()
			hg := h.groups[g]
			h.mu.Unlock()
			if hg == nil {
				h.drop("unknown_group", env, g, nil)
				continue
			}
			env.Payload = payload
			select {
			case hg.inbox <- env:
			default:
				h.drop("inbox_full", env, g, nil)
			}
		}
	}
}

func (h *Host) drop(reason string, env transport.Envelope, g GroupID, err error) {
	h.mu.Lock()
	h.dropped[reason]++
	n := h.dropped[reason]
	h.mu.Unlock()
	// Logged sparsely: a removed node's campaign can send a frame per tick.
	if n&(n-1) == 0 {
		h.logf("event=host_frame_dropped node=%s reason=%s from=%s kind=%d group=%d count=%d err=%v", h.cfg.ID, reason, env.Peer, env.Kind, g, n, err)
	}
}

// peerLoop keeps the transport's peers in step with the hosted groups'
// configurations, once per tick interval.
func (h *Host) peerLoop() {
	defer h.wg.Done()
	ivl := h.cfg.TickInterval
	if ivl <= 0 {
		ivl = raftnode.DefaultTickInterval
	}
	t := time.NewTicker(ivl)
	defer t.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-t.C:
			h.retireRemoved()
			h.syncPeers()
		}
	}
}

// retireRemoved stops every group whose node has seen a committed
// configuration without it (docs/MEMBERSHIP.md §5): it is no longer a member,
// and its files stay for inspection. A restart recovers it, and it is stopped
// again as soon as its recovered configuration says so.
func (h *Host) retireRemoved() {
	for _, g := range h.Groups() {
		grp := h.Group(g)
		if grp == nil || !grp.Node.Status().Removed {
			continue
		}
		// The decision is logged before it takes effect, so an observer that
		// sees the group gone also sees why — and only once the stop is
		// certain: a group busy starting refuses it (ErrGroupBusy), and the
		// next tick tries again, which logged and counted one retirement
		// again with every tick.
		_ = h.stop(g, func() {
			h.logf("event=group_retired node=%s group=%d reason=removed", h.cfg.ID, g)
			h.retired.Inc()
		})
	}
}

// syncPeers adds a transport peer for every member, with an address, of every
// hosted group's current configuration (committed or not: a leader replicates
// to a learner from the moment it appends it), and removes the peers it added
// that no group names any more. Static peers are never removed.
func (h *Host) syncPeers() {
	ps, ok := h.cfg.Transport.(transport.PeerSet)
	if !ok {
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	groups := make([]*hosted, 0, len(h.groups))
	for _, hg := range h.groups {
		groups = append(groups, hg)
	}
	h.mu.Unlock()
	want := map[NodeID]string{}
	for _, hg := range groups {
		conf := hg.g.Node.Status().Conf
		for _, list := range [][]replication.Member{conf.Voters, conf.Outgoing, conf.Learners} {
			for _, m := range list {
				if m.ID != h.cfg.ID && m.Addr != "" {
					want[m.ID] = m.Addr
				}
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, addr := range want {
		if _, static := h.cfg.StaticPeers[id]; static {
			continue
		}
		if h.added[id] == addr {
			continue
		}
		if old, ok := h.added[id]; ok && old != addr {
			_ = ps.RemovePeer(transport.NodeID(id))
		}
		if err := ps.AddPeer(transport.NodeID(id), addr); err != nil {
			h.logf("event=peer_add_failed node=%s peer=%s addr=%s err=%v", h.cfg.ID, id, addr, err)
			continue
		}
		h.added[id] = addr
	}
	for id := range h.added {
		if _, ok := want[id]; !ok {
			_ = ps.RemovePeer(transport.NodeID(id))
			delete(h.added, id)
		}
	}
}

func (h *Host) logf(format string, args ...any) {
	if h.cfg.Logf != nil {
		h.cfg.Logf(format, args...)
	}
}

// instrument registers the host's metrics and the driver's, which every group
// shares (Phase 16, docs/OBSERVABILITY.md).
func (h *Host) instrument(r *metrics.Registry) {
	if r == nil {
		return
	}
	h.nodeMetrics = raftnode.NewMetrics(r)
	h.retired = r.Counter("dkv_host_groups_retired_total", "Groups this host stopped because their node saw a committed configuration without it.")
	h.failures = r.Counter("dkv_host_group_failures_total", "Groups found on disk that failed to recover when the host started.")
	r.CollectGauge("dkv_host_groups", "Groups on this host: running, or failed (on disk, did not recover).", []string{"state"}, func(emit func(float64, ...string)) {
		h.mu.Lock()
		running, failed := len(h.groups), len(h.failed)
		h.mu.Unlock()
		emit(float64(running), "running")
		emit(float64(failed), "failed")
	})
	r.CollectCounter("dkv_host_frames_dropped_total", "Inbound frames the host did not deliver to any group, by reason: malformed (the group envelope did not decode), unknown_group (no such group runs here), inbox_full (the group's inbox was full). Raft retransmits.", []string{"reason"}, func(emit func(float64, ...string)) {
		dropped := h.Dropped()
		for _, reason := range []string{"malformed", "unknown_group", "inbox_full"} {
			emit(float64(dropped[reason]), reason)
		}
	})
}
