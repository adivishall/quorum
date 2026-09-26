// Command dkvd is a Quorum node process (Phase 7).
//
// It is a real OS process representing one node. It builds the internal TCP
// transport (internal/transport), listens, maintains connections to its
// configured peers, answers Probe messages with ProbeResponse, and periodically
// probes its peers to prove bidirectional connectivity. It runs until SIGINT or
// SIGTERM, then shuts the transport down cleanly and exits 0.
//
// By default it runs the Phase 7 probe demo (no Raft, no storage, no client
// serving). With -raft it instead runs a single Phase 9 Raft group over the same
// transport (docs/RAFT.md): it elects a leader, appends the mandatory no-op,
// replicates, and persists its log under -data-dir. Its state machine is the
// Phase 12 key-value store (internal/kv), and with -client-listen it serves the
// minimal Phase 12 operation protocol — PUT/GET/DELETE, writes completed when
// committed and applied, reads through ReadIndex — which exists so real
// processes can be driven by history-recording test clients
// (docs/LINEARIZABILITY.md). It is not the client API: no request ids, no
// forwarding, no deduplication (Phase 13), no HTTP (Phase 15).
//
//	dkvd -id node-1 -listen 127.0.0.1:7001 \
//	     -peers node-2=127.0.0.1:7002,node-3=127.0.0.1:7003 [-raft -data-dir DIR] [-client-listen ADDR]
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
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
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
		id       = fs.String("id", "", "this node's id (required)")
		listen   = fs.String("listen", "", "listen address host:port (required)")
		peersArg = fs.String("peers", "", "comma-separated peers as id=host:port")
		probeIvl = fs.Duration("probe-interval", 100*time.Millisecond, "how often to probe each peer")
		raftMode = fs.Bool("raft", false, "run a single Raft group over the transport (Phase 9) instead of the probe demo")
		dataDir  = fs.String("data-dir", "", "directory for the durable Raft log (raft mode; a temp dir if empty)")
		tickIvl  = fs.Duration("tick-interval", 50*time.Millisecond, "raft logical tick duration")
		crashAt  = fs.String("crash-at", "", "TEST SEAM (raft mode): kill this process with SIGKILL at a crash point, e.g. after-save:2 (the 2nd time it is reached), fsync:3 (before the 3rd fsync of the durable log) or before-reply:1 (before the 1st client response is written); see docs/CRASH_RECOVERY.md")
		crashArm = fs.Bool("crash-armed-by-signal", false, "TEST SEAM: count -crash-at occurrences only after this process receives SIGUSR1 (it logs event=crash_armed), so a test can crash at the Nth occurrence after a point of its choosing; driver and reply points only")
		clientAt = fs.String("client-listen", "", "raft mode: serve the key-value client protocol (internal/kv, docs/API.md: PUT/GET/DELETE, REGISTER, request identity) on this host:port")
		sessMax  = fs.Int("session-max", kv.DefaultLimits.MaxSessions, "raft mode: the most client sessions the state machine keeps; the least recently used is evicted beyond it (docs/DEDUP.md). Part of the replicated state machine: every node of a group MUST use the same value")
		sessUnk  = fs.Int("session-max-unacked", kv.DefaultLimits.MaxUnacked, "raft mode: the most unacknowledged results one session may hold (docs/DEDUP.md). Every node of a group MUST use the same value")
		forward  = fs.Bool("client-forwarding", true, "raft mode: a node that is not the leader forwards a client request one hop to the leader; false is redirect-only (NOT_LEADER with a leader hint)")
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
	tr, err := transport.NewTCPTransport(transport.Config{
		NodeID:          transport.NodeID(*id),
		ListenAddr:      *listen,
		Peers:           peers,
		Logf:            lg.logf,
		ReadIdleTimeout: readIdle,
	})
	if err != nil {
		fmt.Fprintf(stderr, "dkvd: %v\n", err)
		return 2
	}
	lg.logf("event=ready node=%s addr=%s peers=%d", *id, tr.LocalAddr(), len(peers))

	if *clientAt != "" && !*raftMode {
		fmt.Fprintln(stderr, "dkvd: -client-listen requires -raft")
		_ = tr.Close()
		return 2
	}
	limits := kv.Limits{MaxSessions: *sessMax, MaxUnacked: *sessUnk}
	if limits.MaxSessions < 1 || limits.MaxUnacked < 1 {
		fmt.Fprintf(stderr, "dkvd: -session-max and -session-max-unacked must be at least 1, got %d and %d\n", limits.MaxSessions, limits.MaxUnacked)
		_ = tr.Close()
		return 2
	}
	if *raftMode {
		r := raftRun{id: *id, peers: peers, tr: tr, dataDir: *dataDir, tick: *tickIvl, lg: lg, stderr: stderr, clientAddr: *clientAt, crash: crash,
			limits: limits, redirectOnly: !*forward}
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
	peers   map[transport.NodeID]string
	tr      transport.Transport
	dataDir string
	tick    time.Duration
	lg      *logger
	stderr  io.Writer
	fs      vfs.FS
	hook    raftnode.Hook // -crash-at driver point; nil in normal operation
	// clientAddr, if set, serves the Phase 12 key-value protocol (internal/kv)
	// on that address: PUT/DELETE complete when committed and applied here,
	// GET goes through ReadIndex. The node's state machine is a kv.Store.
	clientAddr   string
	crash        *crashPoint // -crash-at; nil in normal operation
	limits       kv.Limits   // the session table's bounds, identical on every node (zero: kv.DefaultLimits)
	redirectOnly bool        // -client-forwarding=false
}

// runRaft runs a single Raft group (Phase 9) over the already-built transport
// instead of the probe demo. The group membership is this node plus its peers. It
// emits machine-readable events — raft_leader, raft_follower, raft_commit — so a
// test or an operator can watch an election and replication happen over real TCP.
// Each event is derived from ONE consistent status snapshot, so a leader claim
// always pairs a role with the term it was actually held in. It returns 0 on a
// clean shutdown, 2 on a startup error, and 1 (after event=raft_fatal) if the node
// fail-stops at runtime.
func runRaft(ctx context.Context, r raftRun) int {
	id, lg := r.id, r.lg
	group := []raftnode.NodeID{raftnode.NodeID(id)}
	for p := range r.peers {
		group = append(group, raftnode.NodeID(p))
	}
	dataDir := r.dataDir
	if dataDir == "" {
		d, err := os.MkdirTemp("", "dkvd-raft-")
		if err != nil {
			fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
			return 2
		}
		dataDir = d
	} else if err := os.MkdirAll(dataDir, 0o755); err != nil {
		fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
		return 2
	}

	limits := r.limits
	if limits == (kv.Limits{}) {
		limits = kv.DefaultLimits
	}
	store := kv.NewStoreWithLimits(limits)
	n, err := raftnode.Start(ctx, raftnode.Config{
		ID: raftnode.NodeID(id), Peers: group, Transport: r.tr,
		LogPath:      filepath.Join(dataDir, "raft-"+id+".log"),
		StateMachine: store,
		TickInterval: r.tick, Logf: lg.logf, FS: r.fs, // durable by default (DisableSync left false)
		Hook: r.hook, // nil unless -crash-at (a test seam)
	})
	if err != nil {
		fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
		return 2
	}
	var clientLn net.Listener
	if r.clientAddr != "" {
		clientLn, err = net.Listen("tcp", r.clientAddr)
		if err != nil {
			fmt.Fprintf(r.stderr, "dkvd: %v\n", err)
			_ = n.Close()
			return 2
		}
		if r.crash != nil && r.crash.reply != 0 {
			clientLn = &crashListener{Listener: clientLn, cp: r.crash}
		}
		srv := kv.NewServer(id, n, store)
		srv.SetForwarding(!r.redirectOnly)
		go kv.Serve(ctx, clientLn, srv, lg.logf)
		lg.logf("event=client_ready node=%s addr=%s forwarding=%t sessions=%d unacked=%d", id, clientLn.Addr(), !r.redirectOnly, limits.MaxSessions, limits.MaxUnacked)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(20 * time.Millisecond)
		defer t.Stop()
		var loggedLeaderTerm, lastCommit, lastFollowTerm uint64
		var lastLeader raftnode.NodeID
		for {
			select {
			case <-ctx.Done():
				return
			case <-n.Done():
				return
			case <-t.C:
				st := n.Status() // one snapshot: role, term, leader and commit agree
				if st.Role == raft.Leader && st.Term > loggedLeaderTerm {
					lg.logf("event=raft_leader node=%s term=%d", id, st.Term)
					loggedLeaderTerm = st.Term
				}
				// A follower reports every (term, leader) it follows, so an observer
				// sees a node re-elected in a new term, not only a change of leader.
				if st.Role == raft.Follower && st.Leader != "" && (st.Leader != lastLeader || st.Term != lastFollowTerm) {
					lg.logf("event=raft_follower node=%s term=%d leader=%s", id, st.Term, st.Leader)
					lastLeader, lastFollowTerm = st.Leader, st.Term
				}
				if st.Commit != lastCommit {
					lg.logf("event=raft_commit node=%s index=%d", id, st.Commit)
					lastCommit = st.Commit
				}
			}
		}
	}()

	code := 0
	select {
	case <-ctx.Done():
	case <-n.Done():
		if err := n.Err(); err != nil {
			lg.logf("event=raft_fatal node=%s err=%v", id, err)
			code = 1
		}
	}
	lg.logf("event=shutdown_start node=%s", id)
	if clientLn != nil {
		_ = clientLn.Close()
	}
	_ = n.Close()
	_ = r.tr.Close()
	wg.Wait()
	st := n.Status()
	lg.logf("event=shutdown_done node=%s role=%s term=%d commit=%d", id, st.Role, st.Term, st.Commit)
	return code
}

// crashPoint is a parsed -crash-at: a driver point (raftnode.Point), an I/O
// boundary of the durable log ("write", "fsync", "truncate") or a Phase 12
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
// points count that operation on the log file from startup too (the first fsync
// is the one Open issues on the recovered state). With armedBySignal, driver
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
