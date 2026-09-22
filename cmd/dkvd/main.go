// Command dkvd is a Quorum node process (Phase 7).
//
// It is a real OS process representing one node. It builds the internal TCP
// transport (internal/transport), listens, maintains connections to its
// configured peers, answers Probe messages with ProbeResponse, and periodically
// probes its peers to prove bidirectional connectivity. It runs until SIGINT or
// SIGTERM, then shuts the transport down cleanly and exits 0.
//
// It does NOT run Raft, replicate, host the storage engine, serve clients, or
// elect leaders — Phase 7 exists to prove the networking infrastructure
// (docs/TRANSPORT.md §11).
//
//	dkvd -id node-1 -listen 127.0.0.1:7001 \
//	     -peers node-2=127.0.0.1:7002,node-3=127.0.0.1:7003
//
// Output is machine-readable "event=... key=value" lines on stdout, so a test or
// an operator can observe startup, connectivity, probes, and shutdown.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/adivishall/quorum/internal/transport"
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
	)
	if err := fs.Parse(args); err != nil {
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

	lg := &logger{w: stdout}
	tr, err := transport.NewTCPTransport(transport.Config{
		NodeID:     transport.NodeID(*id),
		ListenAddr: *listen,
		Peers:      peers,
		Logf:       lg.logf,
	})
	if err != nil {
		fmt.Fprintf(stderr, "dkvd: %v\n", err)
		return 2
	}
	lg.logf("event=ready node=%s addr=%s peers=%d", *id, tr.LocalAddr(), len(peers))

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
