package raftnode

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/replication"
)

// Metrics is the Raft driver's instrumentation (Phase 16, docs/OBSERVABILITY.md).
// One Metrics serves every node of a process: each node takes its group's
// series at Start and leaves the scrape-time gauges when it stops. Every event
// is counted where it happens, on the node's own goroutines; the gauges read
// each node's Status, the snapshot its actor publishes after every cycle.
//
// A nil *Metrics (the default) instruments nothing.
type Metrics struct {
	campaigns, electionsWon, stepDowns, leaderChanges *metrics.CounterVec
	proposals                                         *metrics.CounterVec
	commit, apply, persist                            *metrics.HistogramVec
	persistEntries                                    *metrics.CounterVec
	snapCreated, snapInstalled, snapSent              *metrics.CounterVec
	snapCreate, snapInstall                           *metrics.HistogramVec
	dropped                                           *metrics.CounterVec
	confAdopted, confChanges                          *metrics.CounterVec

	mu    sync.Mutex
	nodes map[*Node]struct{}
}

// NewMetrics registers the driver's families in r. A nil registry gives a nil
// Metrics.
func NewMetrics(r *metrics.Registry) *Metrics {
	if r == nil {
		return nil
	}
	m := &Metrics{nodes: map[*Node]struct{}{}}
	m.campaigns = r.CounterVec("dkv_raft_campaigns_total", "Elections this node started (it became a candidate), counted by the Raft core at the transition.", "group")
	m.electionsWon = r.CounterVec("dkv_raft_elections_won_total", "Terms this node became leader of, counted by the Raft core at the transition.", "group")
	m.stepDowns = r.CounterVec("dkv_raft_leader_stepdowns_total", "Times this node stopped being leader (deposed by a higher term, or removed), counted by the Raft core.", "group")
	m.leaderChanges = r.CounterVec("dkv_raft_leader_changes_total", "Times this node observed a new leader: a (leader, term) it had not followed or been since its last observation, checked after every Ready cycle.", "group")
	m.proposals = r.CounterVec("dkv_raft_proposals_total", "Client writes offered to this node's Raft core: accepted (appended as leader) or refused (not leader).", "group", "result")
	m.commit = r.HistogramVec("dkv_raft_commit_seconds", "For each client write this node accepted as leader: from its append to this node seeing it committed, checked after every Ready cycle. Writes whose index was overwritten are not observed.", metrics.LatencyBuckets, "group")
	m.apply = r.HistogramVec("dkv_raft_apply_seconds", "For each client write this node accepted as leader and saw committed: from the commit to its application to the state machine.", metrics.LatencyBuckets, "group")
	m.persist = r.HistogramVec("dkv_raft_persist_seconds", "Duration of each durable Save of the Raft log (records written and fsynced) before the Ready's messages were sent.", metrics.LatencyBuckets, "group")
	m.persistEntries = r.CounterVec("dkv_raft_persisted_entries_total", "Log entries written by durable Saves.", "group")
	m.snapCreated = r.CounterVec("dkv_raft_snapshots_created_total", "Snapshots this node published: trigger is periodic (SnapshotEvery) or request (the admin API).", "group", "trigger")
	m.snapCreate = r.HistogramVec("dkv_raft_snapshot_create_seconds", "Duration of creating a snapshot: encoding, publishing it by rename and compacting the log behind it.", metrics.LatencyBuckets, "group")
	m.snapInstalled = r.CounterVec("dkv_raft_snapshots_installed_total", "Snapshots from a leader this node made durable and active.", "group")
	m.snapInstall = r.HistogramVec("dkv_raft_snapshot_install_seconds", "Duration of installing a received snapshot: term, publication, boundary record and state-machine restore.", metrics.LatencyBuckets, "group")
	m.snapSent = r.CounterVec("dkv_raft_snapshot_transfers_total", "Snapshot transfers this node streamed to a follower: result is sent or failed.", "group", "result")
	m.dropped = r.CounterVec("dkv_raft_messages_dropped_total", "Raft messages this node did not deliver: outbox_full (a peer's queue was full), send_failed (the transport refused it), decode (a frame did not decode), wrong_group (a frame of another group read off the transport). Raft retransmits.", "group", "reason")
	m.confAdopted = r.CounterVec("dkv_raft_configurations_total", "Configuration entries this node adopted after starting (a configuration takes effect when appended): kind is joint or stable.", "group", "kind")
	m.confChanges = r.CounterVec("dkv_raft_membership_changes_total", "Membership changes proposed through this node, by type and outcome: refused (not leader, or a change under way), completed, lost (its entry was overwritten).", "group", "type", "result")

	gauge := func(name, help string, pick func(Status) float64) {
		r.CollectGauge(name, help, []string{"group"}, func(emit func(float64, ...string)) {
			for _, st := range m.statuses() {
				emit(pick(st), groupLabel(st.Group))
			}
		})
	}
	gauge("dkv_raft_term", "The node's current term.", func(s Status) float64 { return float64(s.Term) })
	gauge("dkv_raft_commit_index", "The node's commit index.", func(s Status) float64 { return float64(s.Commit) })
	gauge("dkv_raft_applied_index", "The highest index the node's state machine has applied.", func(s Status) float64 { return float64(s.Applied) })
	gauge("dkv_raft_last_index", "The index of the last entry in the node's log.", func(s Status) float64 { return float64(s.LastIndex) })
	gauge("dkv_raft_snapshot_index", "The index of the node's published snapshot (0: none).", func(s Status) float64 { return float64(s.Snapshot) })
	gauge("dkv_raft_boundary_index", "The log's compaction boundary: entries at or below it are discarded.", func(s Status) float64 { return float64(s.Boundary) })
	gauge("dkv_raft_log_bytes", "The length of the node's durable Raft log file.", func(s Status) float64 { return float64(s.LogBytes) })
	gauge("dkv_raft_pending_writes", "Client writes waiting for their entry to be applied.", func(s Status) float64 { return float64(s.PendingWrites) })
	gauge("dkv_raft_pending_reads", "Linearizable reads waiting to be confirmed or applied.", func(s Status) float64 { return float64(s.PendingReads) })
	gauge("dkv_raft_has_leader", "1 if the node knows a leader of its current term, 0 otherwise.", func(s Status) float64 { return b2f(s.Leader != "") })
	gauge("dkv_raft_voters", "Voters of the node's current configuration (the incoming set while joint).", func(s Status) float64 { return float64(len(s.Conf.Voters)) })
	gauge("dkv_raft_learners", "Learners of the node's current configuration.", func(s Status) float64 { return float64(len(s.Conf.Learners)) })
	gauge("dkv_raft_configuration_joint", "1 while the node's current configuration is joint.", func(s Status) float64 { return b2f(s.Conf.Joint()) })
	gauge("dkv_raft_configuration_pending", "1 while a configuration change is uncommitted or joint.", func(s Status) float64 { return b2f(s.ConfPending) })
	gauge("dkv_raft_removed", "1 once the node has seen a committed configuration without itself.", func(s Status) float64 { return b2f(s.Removed) })
	r.CollectGauge("dkv_raft_role", "1 for the node's current role, 0 for the others.", []string{"group", "role"}, func(emit func(float64, ...string)) {
		for _, st := range m.statuses() {
			for _, role := range []raft.Role{raft.Follower, raft.Candidate, raft.Leader} {
				emit(b2f(st.Role == role), groupLabel(st.Group), strings.ToLower(role.String()))
			}
		}
	})
	r.CollectGauge("dkv_raft_follower_lag_entries", "On a leader, for each other member: the leader's last index minus the highest index the member is known to hold.", []string{"group", "peer"}, func(emit func(float64, ...string)) {
		for _, st := range m.statuses() {
			for peer, match := range st.FollowerMatch {
				lag := uint64(0)
				if st.LastIndex > match {
					lag = st.LastIndex - match
				}
				emit(float64(lag), groupLabel(st.Group), string(peer))
			}
		}
	})
	return m
}

func (m *Metrics) statuses() []Status {
	m.mu.Lock()
	nodes := make([]*Node, 0, len(m.nodes))
	for n := range m.nodes {
		nodes = append(nodes, n)
	}
	m.mu.Unlock()
	out := make([]Status, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.Status())
	}
	return out
}

func (m *Metrics) add(n *Node) {
	if m != nil {
		m.mu.Lock()
		m.nodes[n] = struct{}{}
		m.mu.Unlock()
	}
}

func (m *Metrics) remove(n *Node) {
	if m != nil {
		m.mu.Lock()
		delete(m.nodes, n)
		m.mu.Unlock()
	}
}

func groupLabel(g replication.GroupID) string { return strconv.FormatUint(uint64(g), 10) }

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// nodeMetrics are one group's series; every field is nil (and inert) when the
// node has no Metrics.
type nodeMetrics struct {
	campaigns, electionsWon, stepDowns, leaderChanges *metrics.Counter
	accepted, refused                                 *metrics.Counter
	commit, apply, persist                            *metrics.Histogram
	persistEntries                                    *metrics.Counter
	snapPeriodic, snapRequest, snapInstalled          *metrics.Counter
	snapCreate, snapInstall                           *metrics.Histogram
	snapSent, snapFailed                              *metrics.Counter
	outboxFull, sendFailed, decode, wrongGroup        *metrics.Counter
	confJoint, confStable                             *metrics.Counter
	confChanges                                       *metrics.CounterVec
	group                                             string
}

func (m *Metrics) forGroup(g replication.GroupID) *nodeMetrics {
	if m == nil {
		return &nodeMetrics{}
	}
	gl := groupLabel(g)
	return &nodeMetrics{
		group:     gl,
		campaigns: m.campaigns.With(gl), electionsWon: m.electionsWon.With(gl), stepDowns: m.stepDowns.With(gl),
		leaderChanges: m.leaderChanges.With(gl),
		accepted:      m.proposals.With(gl, "accepted"), refused: m.proposals.With(gl, "refused"),
		commit: m.commit.With(gl), apply: m.apply.With(gl), persist: m.persist.With(gl),
		persistEntries: m.persistEntries.With(gl),
		snapPeriodic:   m.snapCreated.With(gl, "periodic"), snapRequest: m.snapCreated.With(gl, "request"),
		snapInstalled: m.snapInstalled.With(gl),
		snapCreate:    m.snapCreate.With(gl), snapInstall: m.snapInstall.With(gl),
		snapSent: m.snapSent.With(gl, "sent"), snapFailed: m.snapSent.With(gl, "failed"),
		outboxFull: m.dropped.With(gl, "outbox_full"), sendFailed: m.dropped.With(gl, "send_failed"),
		decode: m.dropped.With(gl, "decode"), wrongGroup: m.dropped.With(gl, "wrong_group"),
		confJoint: m.confAdopted.With(gl, "joint"), confStable: m.confAdopted.With(gl, "stable"),
		confChanges: m.confChanges,
	}
}

func (nm *nodeMetrics) enabled() bool { return nm.commit != nil }

// confChange counts one membership change's outcome.
func (nm *nodeMetrics) confChange(t raft.ConfChangeType, result string) {
	if nm.confChanges != nil {
		nm.confChanges.With(nm.group, confChangeName(t), result).Inc()
	}
}

func confChangeName(t raft.ConfChangeType) string {
	switch t {
	case raft.AddLearner:
		return "add_learner"
	case raft.Promote:
		return "promote"
	case raft.RemoveVoter:
		return "remove_voter"
	case raft.RemoveLearner:
		return "remove_learner"
	}
	return "unknown"
}

// inflightWrite is a client write this node accepted as leader, followed to
// its commit and its application for the latency histograms.
type inflightWrite struct {
	term      uint64
	accepted  time.Time
	committed time.Time // zero until seen committed
}

// maxInflight bounds the table: past it, new writes are not followed (a
// leader accepting far more than it commits is visible in the pending-writes
// gauge; the table must not grow with it).
const maxInflight = 1 << 16

// trackAccepted starts following the write at (index, term).
func (n *Node) trackAccepted(index, term uint64) {
	if !n.m.enabled() || len(n.inflight) >= maxInflight {
		return
	}
	n.inflight[index] = inflightWrite{term: term, accepted: time.Now()}
}

// trackCommitted observes the commit latency of every followed write the
// commit index now covers. A write whose index holds another term's entry was
// lost: it is dropped unobserved.
func (n *Node) trackCommitted() {
	if len(n.inflight) == 0 {
		return
	}
	commit := n.core.CommitIndex()
	now := time.Now()
	for idx, w := range n.inflight {
		if idx > commit || !w.committed.IsZero() {
			continue
		}
		if t, err := n.core.TermAt(idx); err != nil || t != w.term {
			delete(n.inflight, idx)
			continue
		}
		w.committed = now
		n.inflight[idx] = w
		n.m.commit.ObserveDuration(now.Sub(w.accepted))
	}
}

// trackApplied observes the apply latency of every committed, followed write
// the state machine has now applied.
func (n *Node) trackApplied() {
	if len(n.inflight) == 0 {
		return
	}
	applied := n.core.AppliedIndex()
	now := time.Now()
	for idx, w := range n.inflight {
		if idx > applied || w.committed.IsZero() {
			continue
		}
		delete(n.inflight, idx)
		n.m.apply.ObserveDuration(now.Sub(w.committed))
	}
}

// observeStatus turns the change between two published statuses into counts:
// the core's role transitions (counted by the core, so none is missed) and a
// new leader observed.
func (n *Node) observeStatus(st Status) {
	d := st.Counters
	n.m.campaigns.Add(d.Campaigns - n.lastCounters.Campaigns)
	n.m.electionsWon.Add(d.ElectionsWon - n.lastCounters.ElectionsWon)
	n.m.stepDowns.Add(d.StepDowns - n.lastCounters.StepDowns)
	n.lastCounters = d
	if st.Leader != "" && (st.Leader != n.lastLeader || st.Term != n.lastLeaderTerm) {
		n.m.leaderChanges.Inc()
		n.lastLeader, n.lastLeaderTerm = st.Leader, st.Term
	}
}

// timedLog times the durable log's Saves.
type timedLog struct {
	LogStore
	m *nodeMetrics
}

func (l timedLog) Save(hs *raftlog.HardState, entries []raftlog.Entry) error {
	start := time.Now()
	err := l.LogStore.Save(hs, entries)
	if err == nil {
		l.m.persist.Since(start)
		l.m.persistEntries.Add(uint64(len(entries)))
	}
	return err
}

// timedStorage is the Ready cycle's storage with snapshot installs timed.
type timedStorage struct {
	*Durable
	m *nodeMetrics
}

func (s timedStorage) InstallSnapshot(meta raft.SnapshotMeta, hs *raftlog.HardState, at Hook) error {
	start := time.Now()
	err := s.Durable.InstallSnapshot(meta, hs, at)
	if err == nil {
		s.m.snapInstalled.Inc()
		s.m.snapInstall.Since(start)
	}
	return err
}
