// Command dkvd is a Quorum node process.
//
// It is a real OS process representing one node. It builds the internal TCP
// transport (internal/transport), listens and maintains connections to its
// peers. It runs until SIGINT or SIGTERM, then shuts down cleanly and exits 0.
//
// By default it runs the Phase 7 probe demo: it answers Probe messages and
// probes its peers. With -raft it runs one Raft group, group 0, over the
// transport (docs/RAFT.md); with -cluster it runs one Raft group per shard of
// the routing that names it (docs/MULTI_RAFT.md). Each group persists its log
// and snapshots under -data-dir and its state machine is the key-value store
// (internal/kv). With -client-listen it serves the client protocol
// (docs/API.md): PUT/GET/DELETE/REGISTER with request identity and
// deduplication, linearizable reads, one-hop forwarding to the group's leader.
// With -admin-listen it serves the admin protocol (membership changes,
// snapshots, status); with -metrics-listen, Prometheus metrics over HTTP at
// GET /metrics (docs/OBSERVABILITY.md). There is no HTTP client API.
//
//	dkvd -id node-1 -listen 127.0.0.1:7001 \
//	     -peers node-2=127.0.0.1:7002,node-3=127.0.0.1:7003 [-raft | -cluster] -data-dir DIR \
//	     [-client-listen ADDR] [-admin-listen ADDR] [-metrics-listen ADDR]
//
// Output is machine-readable "event=... key=value" lines on stdout, so a test or
// an operator can observe startup, connectivity, elections, replication, and
// shutdown.
//
// Exit codes: 0 after a clean shutdown (SIGINT/SIGTERM); 2 for a configuration or
// startup error; 1 if the Raft node fail-stops at runtime — its durable log could
// not persist state a reply depended on (INV-F1) — logged as event=raft_fatal. A
// node that cannot persist must not keep running as if it were a member.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/nodedir"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/routing"
	"github.com/adivishall/quorum/internal/transport"
	"github.com/adivishall/quorum/internal/vfs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// run is the testable entry point. It returns the process exit code: 0 on a
// clean shutdown, 2 on a configuration or startup error.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dkvd", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		id        = fs.String("id", "", "this node's id (required)")
		listen    = fs.String("listen", "", "listen address host:port (required)")
		peersArg  = fs.String("peers", "", "comma-separated peers as id=host:port")
		probeIvl  = fs.Duration("probe-interval", 100*time.Millisecond, "how often to probe each peer")
		raftMode  = fs.Bool("raft", false, "run a single Raft group over the transport (Phase 9) instead of the probe demo")
		dataDir   = fs.String("data-dir", "", "raft/cluster mode (required): the node's data directory — its Raft logs, snapshots and identity. Locked while the process runs; it belongs to one node of one cluster (docs/MULTI_RAFT.md §5)")
		initDir   = fs.Bool("init", false, "raft/cluster mode: initialize a new, empty -data-dir for this node (first start only; needs -cluster-id). A directory with no node is otherwise refused: a node whose state was lost must be replaced, not restarted empty")
		clusterID = fs.String("cluster-id", "", "raft/cluster mode: the cluster this node belongs to (letters, digits, '.', '_', '-'), recorded at -init; later starts may omit it or must repeat it")
		tickIvl   = fs.Duration("tick-interval", 50*time.Millisecond, "raft logical tick duration")
		crashAt   = fs.String("crash-at", "", "TEST SEAM (raft mode): kill this process with SIGKILL at a crash point, e.g. after-save:2 (the 2nd time it is reached), fsync:3 (before the 3rd fsync of any of the node's files — its Raft log and snapshot files), rename:1, syncdir:2, after-snapshot-publish:1 or before-reply:1 (before the 1st client response is written); see docs/CRASH_RECOVERY.md and docs/SNAPSHOTS.md")
		crashArm  = fs.Bool("crash-armed-by-signal", false, "TEST SEAM: count -crash-at occurrences only after this process receives SIGUSR1 (it logs event=crash_armed), so a test can crash at the Nth occurrence after a point of its choosing; driver and reply points only")
		clientAt  = fs.String("client-listen", "", "raft mode: serve the key-value client protocol (internal/kv, docs/API.md: PUT/GET/DELETE, REGISTER, request identity) on this host:port")
		sessMax   = fs.Int("session-max", kv.DefaultLimits.MaxSessions, "raft mode: the most client sessions the state machine keeps; the least recently used is evicted beyond it (docs/DEDUP.md). Part of the replicated state machine: every node of a group MUST use the same value")
		sessUnk   = fs.Int("session-max-unacked", kv.DefaultLimits.MaxUnacked, "raft mode: the most unacknowledged results one session may hold (docs/DEDUP.md). Every node of a group MUST use the same value")
		forward   = fs.Bool("client-forwarding", true, "raft mode: a node that is not the leader forwards a client request one hop to the leader; false is redirect-only (NOT_LEADER with a leader hint)")
		snapEv    = fs.Uint64("snapshot-every", 10000, "raft mode: snapshot the state machine every N applied entries and compact the Raft log behind the snapshot (docs/SNAPSHOTS.md); 0 never snapshots (the log then grows without bound). Each node decides on its own; values may differ")
		snapKeep  = fs.Uint64("snapshot-retain", 1000, "raft mode: entries kept in the Raft log below each new snapshot, so a follower slightly behind catches up by entries rather than a snapshot transfer")
		cluster   = fs.Bool("cluster", false, "run one Raft group per shard of the routing (Phase 15, docs/MULTI_RAFT.md): this node hosts the groups whose genesis replica group names it, under -data-dir/groups/, plus every group found there and every -join group; clients are routed key -> shard -> group")
		shards    = fs.Int("shards", 4, "cluster mode: the routing's shard count = the number of groups; identical on every node")
		rf        = fs.Int("rf", 3, "cluster mode: the routing's replication factor = each group's genesis size; identical on every node")
		nodesArg  = fs.String("nodes", "", "cluster mode: comma-separated node ids of the routing (the genesis cluster); default this node and its -peers. Identical on every node, including one that joins later")
		joinArg   = fs.String("join", "", "raft/cluster mode: comma-separated group ids this node hosts as a JOINER — it starts with no configuration and its group's leader adds it (admin add-learner); -raft takes only 0")
		metricsAt = fs.String("metrics-listen", "", "serve Prometheus metrics over HTTP (GET /metrics, docs/OBSERVABILITY.md) on this host:port — separate from the client and admin ports")
		adminAt   = fs.String("admin-listen", "", "raft/cluster mode: serve the admin protocol (docs/MULTI_RAFT.md §7: status, add-learner, promote, remove-voter, remove-learner, create-group, stop-group, snapshot) on this host:port — separate from the client port")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	crash, err := parseCrashAt(*crashAt)
	if err != nil {
		fmt.Fprintf(stderr, "dkvd: %v\n", err)
		return 2
	}
	if err := crash.validate(*crashArm, *clientAt != ""); err != nil {
		fmt.Fprintf(stderr, "dkvd: %v\n", err)
		return 2
	}

	peers, err := parsePeers(*peersArg)
	if err != nil {
		fmt.Fprintf(stderr, "dkvd: %v\n", err)
		return 2
	}

	// A non-positive interval would panic time.NewTicker in probeLoop, so it is
	// rejected here as invalid configuration rather than reaching the ticker.
	if *probeIvl <= 0 {
		fmt.Fprintf(stderr, "dkvd: -probe-interval must be positive, got %s\n", *probeIvl)
		return 2
	}
	// Likewise the Raft tick: a negative one would panic every group's actor,
	// and zero would disable the transport's idle timeout (120 ticks) while
	// the driver silently used its default (audit M5).
	if *tickIvl <= 0 {
		fmt.Fprintf(stderr, "dkvd: -tick-interval must be positive, got %s\n", *tickIvl)
		return 2
	}

	// readIdle: a connection that has delivered no frame for this long is treated
	// as dead and torn down, so the dialer reconnects. Without it a connection
	// that is established but silently delivers nothing — a peer that vanished
	// without sending a FIN, or one reached through a network element that accepts
	// bytes but never forwards them — parks the reader in Read forever; and because
	// a registered connection suppresses redialling and writes into such a socket
	// still succeed, the node believes it has a live peer it can never actually
	// reach (Phase 10, docs/FAULTS.md §13). It sits well above the Raft heartbeat
	// interval (HeartbeatTicks=2) and one election timeout (ElectionTicks in
	// [10,20) ticks), so a heartbeated link is never torn down; at ~120 ticks it is
	// 6–12× an election timeout, detecting a dead link within a couple of them. A
	// link that legitimately carries no traffic (two followers of the same leader)
	// is torn down and immediately redialled; that reconnect is harmless.
	readIdle := 120 * *tickIvl
	lg := &logger{w: stdout}
	var reg *metrics.Registry
	if *metricsAt != "" {
		reg = metrics.NewRegistry()
		metrics.RegisterProcess(reg)
		stopMetrics, err := serveMetrics(*metricsAt, reg, *id, lg)
		if err != nil {
			fmt.Fprintf(stderr, "dkvd: -metrics-listen: %v\n", err)
			return 2
		}
		defer stopMetrics()
	}
	tr, err := transport.NewTCPTransport(transport.Config{
		NodeID:          transport.NodeID(*id),
		ListenAddr:      *listen,
		Peers:           peers,
		Logf:            lg.logf,
		ReadIdleTimeout: readIdle,
		Metrics:         reg,
	})
	if err != nil {
		fmt.Fprintf(stderr, "dkvd: %v\n", err)
		return 2
	}
	lg.logf("event=ready node=%s addr=%s peers=%d", *id, tr.LocalAddr(), len(peers))

	if *raftMode && *cluster {
		fmt.Fprintln(stderr, "dkvd: -raft and -cluster are exclusive")
		_ = tr.Close()
		return 2
	}
	if (*clientAt != "" || *adminAt != "" || *joinArg != "" || *initDir || *clusterID != "") && !*raftMode && !*cluster {
		fmt.Fprintln(stderr, "dkvd: -client-listen, -admin-listen, -join, -init and -cluster-id require -raft or -cluster")
		_ = tr.Close()
		return 2
	}
	// A node's durable state is the whole of its Raft safety: it is never a
	// temporary directory that a restart would forget (audit H1).
	if (*raftMode || *cluster) && *dataDir == "" {
		fmt.Fprintln(stderr, "dkvd: -raft and -cluster require -data-dir")
		_ = tr.Close()
		return 2
	}
	join, err := parseGroups(*joinArg)
	if err != nil || (*raftMode && (len(join) > 1 || len(join) == 1 && join[0] != 0)) {
		fmt.Fprintf(stderr, "dkvd: bad -join %q (with -raft only group 0): %v\n", *joinArg, err)
		_ = tr.Close()
		return 2
	}
	var assign *multiraft.Assignment
	if *cluster {
		nodes := []routing.NodeID{routing.NodeID(*id)}
		for p := range peers {
			nodes = append(nodes, routing.NodeID(p))
		}
		if *nodesArg != "" {
			nodes = nil
			for _, n := range strings.Split(*nodesArg, ",") {
				nodes = append(nodes, routing.NodeID(strings.TrimSpace(n)))
			}
		}
		assign, err = multiraft.NewAssignment(routing.Config{ShardCount: *shards, ReplicationFactor: *rf, Nodes: nodes})
		if err != nil {
			fmt.Fprintf(stderr, "dkvd: the cluster's routing: %v\n", err)
			_ = tr.Close()
			return 2
		}
	}
	limits := kv.Limits{MaxSessions: *sessMax, MaxUnacked: *sessUnk}
	if limits.MaxSessions < 1 || limits.MaxUnacked < 1 {
		fmt.Fprintf(stderr, "dkvd: -session-max and -session-max-unacked must be at least 1, got %d and %d\n", limits.MaxSessions, limits.MaxUnacked)
		_ = tr.Close()
		return 2
	}
	if *raftMode || *cluster {
		r := raftRun{id: *id, listen: *listen, peers: peers, tr: tr, dataDir: *dataDir, init: *initDir, clusterID: *clusterID, tick: *tickIvl, lg: lg, stderr: stderr, clientAddr: *clientAt, crash: crash,
			limits: limits, redirectOnly: !*forward, snapshotEvery: *snapEv, snapshotRetain: *snapKeep,
			assign: assign, join: join, adminAddr: *adminAt, metrics: reg}
		if crash != nil {
			r.hook, r.fs = crash.install(ctx, lg, *id, *crashArm)
		}
		return runRaft(ctx, r)
	}

	n := &node{
		id:     transport.NodeID(*id),
		tr:     tr,
		lg:     lg,
		peers:  peers,
		acked:  make(map[transport.NodeID]bool),
		probed: 0,
	}
	if len(peers) == 0 {
		lg.logf("event=all_peers_acked node=%s", *id) // nothing to prove
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); n.serveReceive(ctx) }()
	go func() { defer wg.Done(); n.probeLoop(ctx, *probeIvl) }()

	<-ctx.Done() // SIGINT/SIGTERM (or test cancellation)
	lg.logf("event=shutdown_start node=%s", *id)
	_ = tr.Close()
	wg.Wait()
	n.mu.Lock()
	acked := len(n.acked)
	n.mu.Unlock()
	lg.logf("event=shutdown_done node=%s probes_sent=%d peers_acked=%d", *id, n.probesSent(), acked)
	return 0
}

// node holds the per-process probe state.
type node struct {
	id    transport.NodeID
	tr    *transport.TCPTransport
	lg    *logger
	peers map[transport.NodeID]string

	mu       sync.Mutex
	acked    map[transport.NodeID]bool
	allAcked bool
	probed   uint64
	requests uint64
}

func (n *node) probesSent() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.probed
}

// serveReceive answers Probes and records ProbeResponses until the transport's
// receive channel closes (on shutdown).
func (n *node) serveReceive(ctx context.Context) {
	for env := range n.tr.Receive() {
		switch env.Kind {
		case transport.MsgProbe:
			p, err := transport.ParseProbe(env.Payload)
			if err != nil {
				n.lg.logf("event=probe_decode_error peer=%s err=%v", env.Peer, err)
				continue
			}
			resp := transport.ProbeResponse{RequestID: p.RequestID, Token: p.Token}
			if err := n.tr.Send(ctx, env.Peer, transport.MsgProbeResponse, resp.Marshal()); err != nil {
				n.lg.logf("event=probe_reply_failed peer=%s err=%v", env.Peer, err)
			}
		case transport.MsgProbeResponse:
			if _, err := transport.ParseProbeResponse(env.Payload); err != nil {
				n.lg.logf("event=probe_response_decode_error peer=%s err=%v", env.Peer, err)
				continue
			}
			n.recordAck(env.Peer)
		default:
			// A reserved-but-unimplemented kind, or one no Phase 7 node sends.
			// Log and ignore; never crash on unexpected peer input.
			n.lg.logf("event=unexpected_kind peer=%s kind=%s", env.Peer, env.Kind)
		}
	}
}

func (n *node) recordAck(peer transport.NodeID) {
	n.mu.Lock()
	first := !n.acked[peer]
	n.acked[peer] = true
	done := !n.allAcked && len(n.acked) == len(n.peers) && len(n.peers) > 0
	if done {
		n.allAcked = true
	}
	n.mu.Unlock()
	if first {
		n.lg.logf("event=probe_ack node=%s peer=%s", n.id, peer)
	}
	if done {
		n.lg.logf("event=all_peers_acked node=%s", n.id)
	}
}

// probeLoop sends a Probe to every peer on a fixed interval. It is a transport
// liveness probe, not a Raft heartbeat.
func (n *node) probeLoop(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for peer := range n.peers {
				n.mu.Lock()
				n.requests++
				id := n.requests
				n.mu.Unlock()
				var tok [8]byte
				binary.BigEndian.PutUint64(tok[:], id)
				err := n.tr.Send(ctx, peer, transport.MsgProbe, transport.Probe{RequestID: id, Token: tok[:]}.Marshal())
				if err == nil {
					n.mu.Lock()
					n.probed++
					n.mu.Unlock()
				}
				// A peer not yet connected is expected early on; the dialer keeps
				// retrying, so the next tick will find it.
			}
		}
	}
}

// raftRun is everything runRaft needs. fs is the filesystem for the durable log:
// nil — always, in the real binary — is the OS; a test may substitute a fault
// injector to drive the fail-stop path end to end.
type raftRun struct {
	id      string
	listen  string // this node's transport address (a genesis member's address)
	peers   map[transport.NodeID]string
	tr      transport.Transport
	dataDir string
	// init and clusterID: -init and -cluster-id (internal/nodedir).
	init      bool
	clusterID string
	tick      time.Duration
	lg        *logger
	stderr    io.Writer
	fs        vfs.FS
	hook      raftnode.Hook // -crash-at driver point; nil in normal operation
	// clientAddr, if set, serves the key-value protocol (internal/kv) on that
	// address, for every group the node hosts (kv.Front).
	clientAddr   string
	crash        *crashPoint // -crash-at; nil in normal operation
	limits       kv.Limits   // the session table's bounds, identical on every node (zero: kv.DefaultLimits)
	redirectOnly bool        // -client-forwarding=false
	// Phase 14: -snapshot-every and -snapshot-retain.
	snapshotEvery, snapshotRetain uint64
	// Phase 15: -cluster mode's assignment (nil: -raft, one group 0 of this
	// node and its peers); -join's groups; -admin-listen.
	assign    *multiraft.Assignment
	join      []multiraft.GroupID
	adminAddr string
	// Phase 16: the process's metrics registry (nil: -metrics-listen unset).
	metrics *metrics.Registry
}

// groupWatch is what the event loop last reported for one group.
type groupWatch struct {
	leaderTerm, commit, followTerm uint64
	leader                         raftnode.NodeID
}

// runRaft runs this node's Raft groups (Phase 15, docs/MULTI_RAFT.md) over the
// already-built transport, through the node host (internal/multiraft): with
// -raft one group, 0, of this node and its peers — the Phase 9–14 single-group
// deployment, its log still at DATA/raft-ID.log; with -cluster one group per
// shard of the routing whose genesis names this node, under DATA/groups/ID/,
// plus every group found there and every -join group. It emits machine-readable
// events per group — raft_leader, raft_follower, raft_commit, each ending
// group=G — so a test or an operator can watch elections and replication over
// real TCP; each is derived from ONE consistent status snapshot, so a leader
// claim always pairs a role with the term it was actually held in. It returns 0
// on a clean shutdown, 2 on a startup error, and 1 (after event=raft_fatal) if
// the -raft group fail-stops at runtime. In -cluster mode a group that
// fail-stops is stopped alone (event=raft_fatal ... group=G) and the others go
// on: one group's failure must not take the others down (INV-MB9).
func runRaft(ctx context.Context, r raftRun) int {
	id, lg := r.id, r.lg
	dataDir := r.dataDir
	if dataDir == "" {
		fmt.Fprintln(r.stderr, "dkvd: no data directory")
		return 2
	}
	// The data directory is this process's (locked) and this node's (its
	// identity), and a directory with no node is initialized only on request
	// (-init): a node whose state was lost must not restart empty under its
	// old id (audit H1, internal/nodedir).
	nd, err := nodedir.Open(dataDir, nodedir.Options{Node: id, Cluster: r.clusterID, Init: r.init})
	if err != nil {
		fmt.Fprintf(r.stderr, "dkvd: -data-dir: %v\n", err)
		return 2
	}
	defer nd.Close()
	lg.logf("event=data_dir node=%s dir=%s cluster=%s state=%s", id, dataDir, nd.ID.Cluster, nd.State)
	limits := r.limits
	if limits == (kv.Limits{}) {
		limits = kv.DefaultLimits
	}
	var route func([]byte) replication.GroupID
	if r.assign != nil {
		route = r.assign.GroupOf
	}
	front := kv.NewFront(id, route)
	front.SetForwarding(!r.redirectOnly)
	km := kv.NewMetrics(r.metrics)
	front.SetMetrics(km)
	static := map[multiraft.NodeID]string{}
	for p, addr := range r.peers {
		static[multiraft.NodeID(p)] = addr
	}
	hc := multiraft.Config{
		ID: raftnode.NodeID(id), DataDir: dataDir, Transport: r.tr, StaticPeers: static,
		NewStateMachine: func(g multiraft.GroupID) raftnode.StateMachine {
			store := kv.NewStoreWithLimits(limits)
			km.Observe(store, g)
			return store
		},
		OnGroup: func(g multiraft.GroupID, node *raftnode.Node, sm raftnode.StateMachine) {
			if node == nil {
				front.Detach(g)
				return
			}
			front.Attach(g, node, sm.(*kv.Store))
		},
		TickInterval: r.tick, FS: r.fs, Hook: r.hook, // durable by default (DisableSync left false)
		SnapshotEvery: r.snapshotEvery, SnapshotRetain: r.snapshotRetain,
		Logf: lg.logf, Metrics: r.metrics,
	}
	if r.assign == nil {
		hc.LogPathFor = func(multiraft.GroupID) string { return filepath.Join(dataDir, "raft-"+id+".log") }
	}
	host, err := multiraft.Start(ctx, hc)
	if err != nil {
		fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
		return 2
	}
	// The genesis groups, then the joined ones. A group already running (found
	// on disk) is left as it is; a group that cannot start is fatal in -raft
	// mode and reported in -cluster mode.
	type creation struct {
		g    multiraft.GroupID
		boot *replication.Configuration
	}
	var creations []creation
	joining := map[multiraft.GroupID]bool{}
	for _, g := range r.join {
		joining[g] = true
	}
	if r.assign == nil {
		if !joining[0] {
			ids := []raftnode.NodeID{raftnode.NodeID(id)}
			for p := range r.peers {
				ids = append(ids, raftnode.NodeID(p))
			}
			conf := replication.VotersOf(ids)
			creations = append(creations, creation{0, &conf})
		}
	} else {
		addrs := map[multiraft.NodeID]string{multiraft.NodeID(id): r.listen}
		for p, addr := range r.peers {
			addrs[multiraft.NodeID(p)] = addr
		}
		for _, g := range r.assign.GenesisGroups(multiraft.NodeID(id)) {
			if joining[g] {
				continue
			}
			conf, err := r.assign.Genesis(g, addrs)
			if err != nil {
				fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
				_ = host.Close()
				return 2
			}
			creations = append(creations, creation{g, &conf})
		}
	}
	for _, g := range r.join {
		creations = append(creations, creation{g, nil})
	}
	// Genesis state is created only while the directory is being initialized.
	// In an initialized directory a genesis group with no state was lost, and
	// is reported rather than created empty: its node voted and acknowledged
	// as a member, and an empty replica under its id would do so again
	// without that state. In -raft mode group 0 is always opened through
	// Create (which checks the configured genesis against the recorded one),
	// so its identity file must already exist.
	initOK := true
	for _, c := range creations {
		if host.Group(c.g) != nil {
			continue
		}
		if !nd.Initializing() {
			if r.assign == nil {
				if _, found, err := raftnode.LoadIdentity(r.fs, hc.LogPathFor(c.g)); err != nil || !found {
					fmt.Fprintf(r.stderr, "dkvd: group %d has no state in the initialized data directory %s (%v): "+
						"its state was lost; replace this node through a membership change\n", c.g, dataDir, err)
					_ = host.Close()
					return 2
				}
			} else if c.boot != nil {
				lg.logf("event=group_failed node=%s group=%d err=%q", id, c.g,
					"genesis group with no state in an initialized data directory: its state was lost; replace this replica through a membership change")
				continue
			}
		}
		if _, err := host.Create(c.g, c.boot); err != nil {
			if r.assign == nil {
				fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
				_ = host.Close()
				return 2
			}
			initOK = false
			lg.logf("event=group_failed node=%s group=%d err=%v", id, c.g, err)
		}
	}
	if nd.Initializing() {
		if !initOK {
			lg.logf("event=init_incomplete node=%s dir=%s: a genesis group did not start; the next start resumes the initialization", id, dataDir)
		} else if err := nd.FinishInit(); err != nil {
			fmt.Fprintf(r.stderr, "dkvd: recording the initialization of %s: %v\n", dataDir, err)
			_ = host.Close()
			return 2
		} else {
			lg.logf("event=data_dir_initialized node=%s dir=%s cluster=%s", id, dataDir, nd.ID.Cluster)
		}
	}
	if r.assign == nil && host.Group(0) == nil {
		err := host.Failed()[0]
		fmt.Fprintf(r.stderr, "dkvd: group 0: %v\n", err)
		_ = host.Close()
		return 2
	}

	var clientLn net.Listener
	if r.clientAddr != "" {
		clientLn, err = net.Listen("tcp", r.clientAddr)
		if err != nil {
			fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
			_ = host.Close()
			return 2
		}
		if r.crash != nil && r.crash.reply != 0 {
			clientLn = &crashListener{Listener: clientLn, cp: r.crash}
		}
		go kv.Serve(ctx, clientLn, front, lg.logf)
		lg.logf("event=client_ready node=%s addr=%s forwarding=%t sessions=%d unacked=%d groups=%d", id, clientLn.Addr(), !r.redirectOnly, limits.MaxSessions, limits.MaxUnacked, len(host.Groups()))
	}
	var adminLn net.Listener
	if r.adminAddr != "" {
		adminLn, err = net.Listen("tcp", r.adminAddr)
		if err != nil {
			fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
			if clientLn != nil {
				_ = clientLn.Close()
			}
			_ = host.Close()
			return 2
		}
		go multiraft.ServeAdmin(ctx, adminLn, host, lg.logf)
		lg.logf("event=admin_ready node=%s addr=%s", id, adminLn.Addr())
	}

	fatal := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(20 * time.Millisecond)
		defer t.Stop()
		watch := map[multiraft.GroupID]*groupWatch{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			for _, g := range host.Groups() {
				grp := host.Group(g)
				if grp == nil {
					continue
				}
				select {
				case <-grp.Node.Done():
					if err := grp.Node.Err(); err != nil {
						lg.logf("event=raft_fatal node=%s err=%v group=%d", id, err, g)
						if r.assign == nil {
							close(fatal)
							return
						}
						_ = host.Stop(g)
					}
					continue
				default:
				}
				w := watch[g]
				if w == nil {
					w = &groupWatch{}
					watch[g] = w
				}
				st := grp.Node.Status() // one snapshot: role, term, leader and commit agree
				if st.Role == raft.Leader && st.Term > w.leaderTerm {
					lg.logf("event=raft_leader node=%s term=%d group=%d", id, st.Term, g)
					w.leaderTerm = st.Term
				}
				// A follower reports every (term, leader) it follows, so an observer
				// sees a node re-elected in a new term, not only a change of leader.
				if st.Role == raft.Follower && st.Leader != "" && (st.Leader != w.leader || st.Term != w.followTerm) {
					lg.logf("event=raft_follower node=%s term=%d leader=%s group=%d", id, st.Term, st.Leader, g)
					w.leader, w.followTerm = st.Leader, st.Term
				}
				if st.Commit != w.commit {
					lg.logf("event=raft_commit node=%s index=%d group=%d", id, st.Commit, g)
					w.commit = st.Commit
				}
			}
		}
	}()

	code := 0
	select {
	case <-ctx.Done():
	case <-fatal:
		code = 1
	}
	lg.logf("event=shutdown_start node=%s", id)
	if clientLn != nil {
		_ = clientLn.Close()
	}
	if adminLn != nil {
		_ = adminLn.Close()
	}
	var last raftnode.Status
	if grp := host.Group(0); grp != nil {
		last = grp.Node.Status()
	}
	_ = host.Close()
	_ = r.tr.Close()
	wg.Wait()
	lg.logf("event=shutdown_done node=%s role=%s term=%d commit=%d", id, last.Role, last.Term, last.Commit)
	return code
}

// crashPoint is a parsed -crash-at: a driver point (raftnode.Point), an I/O
// boundary of the node's files ("write", "fsync", "truncate", and since Phase
// 14 "rename" and "syncdir"; the Raft log and the snapshot files) or a Phase 12
// reply point of the client protocol ("before-reply", "after-reply"), and which
// occurrence fires. It is a test seam for the Phase 11 real-process crash tests
// (docs/CRASH_RECOVERY.md §8): at the point the process logs event=crash_point
// and kills itself with SIGKILL, so nothing after that boundary happens — the
// same points the deterministic simulator crashes at, on a real process, real
// files and a real restart. Unset in normal operation, it costs nothing.
type crashPoint struct {
	name   string
	driver raftnode.Point // zero for an I/O or reply point
	op     fault.Op       // zero for a driver or reply point
	reply  int            // replyBefore or replyAfter for a reply point, else 0
	nth    int

	die   func()      // log the point and SIGKILL this process
	armed atomic.Bool // occurrences count only once armed
	mu    sync.Mutex  // guards seen (client connections write concurrently)
	seen  int         // reply occurrences since arming
}

// Reply points are the client protocol's response boundary (Phase 12): the
// Nth response frame the node is about to write (before-reply), or has just
// handed to the kernel (after-reply). They complete the write-crash windows:
// a write can be committed and applied, and the process die before or after
// its client could learn so.
const (
	replyBefore = 1
	replyAfter  = 2
)

func parseCrashAt(spec string) (*crashPoint, error) {
	if spec == "" {
		return nil, nil
	}
	name, nth := spec, 1
	if i := strings.LastIndexByte(spec, ':'); i >= 0 {
		name = spec[:i]
		n, err := strconv.Atoi(spec[i+1:])
		if err != nil || n < 1 {
			return nil, fmt.Errorf("-crash-at %q: occurrence must be a positive integer", spec)
		}
		nth = n
	}
	cp := &crashPoint{name: name, nth: nth}
	if p, ok := raftnode.ParsePoint(name); ok {
		cp.driver = p
		return cp, nil
	}
	switch name {
	case "before-reply":
		cp.reply = replyBefore
	case "after-reply":
		cp.reply = replyAfter
	case "write":
		cp.op = fault.OpWrite
	case "fsync":
		cp.op = fault.OpSync
	case "truncate":
		cp.op = fault.OpTruncate
	case "rename":
		cp.op = fault.OpRename
	case "syncdir":
		cp.op = fault.OpSyncDir
	default:
		return nil, fmt.Errorf("-crash-at %q: unknown crash point", spec)
	}
	return cp, nil
}

// validate rejects combinations the seam cannot honour.
func (cp *crashPoint) validate(armedBySignal, clientListen bool) error {
	switch {
	case cp == nil && armedBySignal:
		return fmt.Errorf("-crash-armed-by-signal needs -crash-at")
	case cp == nil:
		return nil
	case armedBySignal && cp.op != 0:
		return fmt.Errorf("-crash-armed-by-signal: I/O points count from startup; arm a driver or reply point")
	case cp.reply != 0 && !clientListen:
		return fmt.Errorf("-crash-at %s needs -client-listen", cp.name)
	}
	return nil
}

// install returns the driver hook and the durable log's filesystem that make the
// process die at the point (a reply point is installed on the client listener
// instead; see crashListener). Driver points count occurrences from startup; I/O
// points count that operation on any of the node's files from startup too (the
// first fsync is the one Open issues on the recovered state; every Open also
// fsyncs the directory). With armedBySignal, driver
// and reply points count only occurrences after the process receives SIGUSR1:
// the test sends it, waits for event=crash_armed, then triggers exactly the
// operation it wants the process to die inside.
func (cp *crashPoint) install(ctx context.Context, lg *logger, id string, armedBySignal bool) (raftnode.Hook, vfs.FS) {
	cp.die = func() {
		lg.logf("event=crash_point node=%s point=%s n=%d", id, cp.name, cp.nth)
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {} // SIGKILL is not deliverable to ourselves any later than now
	}
	if armedBySignal {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGUSR1)
		go func() {
			select {
			case <-sig:
				cp.armed.Store(true)
				lg.logf("event=crash_armed node=%s point=%s n=%d", id, cp.name, cp.nth)
			case <-ctx.Done():
			}
		}()
	} else {
		cp.armed.Store(true)
	}
	if cp.driver != 0 {
		seen := 0 // the hook runs on the node's actor goroutine only
		return func(p raftnode.Point, _ uint64) error {
			if p == cp.driver && cp.armed.Load() {
				seen++
				if seen == cp.nth {
					cp.die()
				}
			}
			return nil
		}, nil
	}
	if cp.reply != 0 {
		return nil, nil
	}
	// The injecting filesystem (real OS underneath) records every operation on
	// the log for the process's lifetime — acceptable for a process that exists
	// to die at its Nth one, which is the only reason this flag is ever set.
	ifs := fault.NewInjectFS(nil)
	ifs.Arm(fault.Injection{Op: cp.op, Nth: cp.nth, At: cp.die})
	return nil, ifs
}

// replyHit counts one reply occurrence and reports whether it is the one.
func (cp *crashPoint) replyHit() bool {
	if !cp.armed.Load() {
		return false
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.seen++
	return cp.seen == cp.nth
}

// crashListener wraps the client listener so the process can die at a reply
// point: the kv protocol writes each response frame with exactly one Write.
type crashListener struct {
	net.Listener
	cp *crashPoint
}

func (l *crashListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &crashConn{Conn: c, cp: l.cp}, nil
}

type crashConn struct {
	net.Conn
	cp *crashPoint
}

func (c *crashConn) Write(b []byte) (int, error) {
	if c.cp.reply == replyBefore && c.cp.replyHit() {
		c.cp.die()
	}
	n, err := c.Conn.Write(b)
	if c.cp.reply == replyAfter && c.cp.replyHit() {
		c.cp.die()
	}
	return n, err
}

// parsePeers parses "id=host:port,id=host:port" into a map. It rejects empty
// entries, a missing '=', an empty id, and a duplicate id — never silently.
// parseGroups parses -join's comma-separated group ids.
func parseGroups(s string) ([]multiraft.GroupID, error) {
	if s == "" {
		return nil, nil
	}
	var out []multiraft.GroupID
	seen := map[multiraft.GroupID]bool{}
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(f), 10, 32)
		if err != nil {
			return nil, err
		}
		g := multiraft.GroupID(v)
		if seen[g] {
			return nil, fmt.Errorf("group %d twice", g)
		}
		seen[g] = true
		out = append(out, g)
	}
	return out, nil
}

func parsePeers(s string) (map[transport.NodeID]string, error) {
	peers := make(map[transport.NodeID]string)
	s = strings.TrimSpace(s)
	if s == "" {
		return peers, nil
	}
	for _, entry := range strings.Split(s, ",") {
		eq := strings.IndexByte(entry, '=')
		if eq < 0 {
			return nil, fmt.Errorf("malformed peer %q: want id=host:port", entry)
		}
		id := transport.NodeID(entry[:eq])
		addr := entry[eq+1:]
		if id == "" {
			return nil, fmt.Errorf("empty peer id in %q", entry)
		}
		if addr == "" {
			return nil, fmt.Errorf("empty address for peer %q", id)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("duplicate peer id %q", id)
		}
		peers[id] = addr
	}
	return peers, nil
}

// logger writes machine-readable event lines, safe for concurrent use.
type logger struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *logger) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.w, format+"\n", args...)
}

// serveMetrics serves reg at GET /metrics on addr (Phase 16,
// docs/OBSERVABILITY.md) and returns the function that stops it. The listener is
// bound before it returns, so a bad address is a startup error.
func serveMetrics(addr string, reg *metrics.Registry, id string, lg *logger) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(reg))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	lg.logf("event=metrics_ready node=%s addr=%s", id, ln.Addr())
	return func() { _ = srv.Close() }, nil
}
