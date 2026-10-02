package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/record"
)

// Envelope is a received message, tagged with the peer that sent it. The peer is
// the handshake identity of the connection the frame arrived on, never a value
// from the payload (INV-T4, ADR-013).
type Envelope struct {
	Peer    NodeID
	Kind    MsgKind
	Payload []byte
}

// Transport is the interface later cluster/Raft code uses. It carries bytes
// tagged with a kind; it does not know what the bytes mean.
type Transport interface {
	// Send delivers a message to peer over its established connection. It fails
	// (ErrPeerNotConnected) if there is no live connection, and respects ctx
	// cancellation/deadline.
	Send(ctx context.Context, peer NodeID, kind MsgKind, payload []byte) error
	// Receive returns the channel of inbound messages. It is closed after Close
	// completes.
	Receive() <-chan Envelope
	// LocalID returns this node's id.
	LocalID() NodeID
	// Close shuts the transport down: stops accepting and dialing, closes every
	// connection, waits for all goroutines to exit, and closes Receive. It is
	// idempotent.
	Close() error
}

// PeerSet is a transport whose peers can change at runtime (Phase 15,
// docs/MULTI_RAFT.md §4). The transport never decides membership: its owner —
// the multi-Raft host, from the configurations of the groups it runs — adds
// the members it must reach and removes the ones it no longer shares a group
// with.
type PeerSet interface {
	// AddPeer admits peer at addr: its connections are accepted from now on
	// and, if this node has the smaller id, it is dialed (ADR-014). Adding a
	// known peer at the same address does nothing; at another address it is an
	// error (remove it first).
	AddPeer(id NodeID, addr string) error
	// RemovePeer forgets peer: its dial loop stops, its connection is closed and
	// its connections are refused from now on. An unknown peer is ignored.
	RemovePeer(id NodeID) error
}

const recvBuffer = 256

// TCPTransport is the framed-TCP implementation of Transport.
type TCPTransport struct {
	cfg  Config
	ln   net.Listener
	recv chan Envelope

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	conns  map[NodeID]*conn
	peers  map[NodeID]*peer // the peers it accepts and (smaller id) dials
	closed bool

	self      hello         // what this node announces in every handshake
	handshake chan struct{} // a slot per inbound connection in its handshake

	m *transportMetrics // Phase 16; its counters are nil (inert) without Config.Metrics
}

// peer is a known peer: its address and the cancellation of its dial loop.
type peer struct {
	addr string
	stop context.CancelFunc
}

var (
	_ Transport = (*TCPTransport)(nil)
	_ PeerSet   = (*TCPTransport)(nil)
)

// NewTCPTransport validates cfg, starts listening, and starts the accept loop
// and the dial loops for peers this node is responsible for dialing (the node
// with the smaller id dials — ADR-014). A ListenAddr with port 0 lets the OS
// choose; LocalAddr reports the chosen address.
func NewTCPTransport(cfg Config) (*TCPTransport, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, err
	}
	return newTCPTransport(cfg.withDefaults(), ln), nil
}

// newTCPTransport runs a transport on an open listener (tests substitute one
// that fails).
func newTCPTransport(cfg Config, ln net.Listener) *TCPTransport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &TCPTransport{
		cfg:    cfg,
		ln:     ln,
		recv:   make(chan Envelope, recvBuffer),
		ctx:    ctx,
		cancel: cancel,
		conns:  make(map[NodeID]*conn),
		peers:  make(map[NodeID]*peer),
		self:   hello{id: cfg.NodeID, cluster: cfg.ClusterID, digest: cfg.SettingsDigest},

		handshake: make(chan struct{}, cfg.MaxPendingHandshakes),
	}
	t.m = newTransportMetrics(cfg.Metrics, t)

	t.wg.Add(1)
	go t.acceptLoop()

	t.mu.Lock()
	for id, addr := range cfg.Peers {
		t.addPeerLocked(id, addr)
	}
	t.mu.Unlock()
	t.logf("event=node_started node=%s listen=%s peers=%d", cfg.NodeID, ln.Addr(), len(cfg.Peers))
	return t
}

// addPeerLocked records a peer and, if this node has the smaller id, starts
// its dial loop. t.mu is held.
func (t *TCPTransport) addPeerLocked(id NodeID, addr string) {
	ctx, stop := context.WithCancel(t.ctx)
	t.peers[id] = &peer{addr: addr, stop: stop}
	if t.cfg.NodeID < id { // smaller id dials
		t.wg.Add(1)
		go t.dialLoop(ctx, id, addr)
	}
}

// AddPeer admits a peer at runtime (PeerSet).
func (t *TCPTransport) AddPeer(id NodeID, addr string) error {
	switch {
	case id == "":
		return fmt.Errorf("%w: empty peer id", ErrInvalidConfig)
	case len(id) > MaxNodeIDLen:
		return fmt.Errorf("%w: peer id %q exceeds %d bytes", ErrInvalidConfig, id, MaxNodeIDLen)
	case id == t.cfg.NodeID:
		return fmt.Errorf("%w: peer id %q is this node's own id (self-dial)", ErrInvalidConfig, id)
	}
	if err := validateAddr(addr); err != nil {
		return fmt.Errorf("%w: peer %q addr %q: %v", ErrInvalidConfig, id, addr, err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	if p, ok := t.peers[id]; ok {
		if p.addr == addr {
			return nil
		}
		return fmt.Errorf("%w: peer %q is known at %s, not %s", ErrInvalidConfig, id, p.addr, addr)
	}
	t.addPeerLocked(id, addr)
	t.logf("event=peer_added node=%s peer=%s addr=%s", t.cfg.NodeID, id, addr)
	return nil
}

// RemovePeer forgets a peer at runtime (PeerSet).
func (t *TCPTransport) RemovePeer(id NodeID) error {
	t.mu.Lock()
	p, ok := t.peers[id]
	if !ok {
		t.mu.Unlock()
		return nil
	}
	delete(t.peers, id)
	p.stop()
	c := t.conns[id]
	t.mu.Unlock()
	if c != nil {
		c.close() // its serve deregisters it
	}
	t.logf("event=peer_removed node=%s peer=%s", t.cfg.NodeID, id)
	return nil
}

// Peers returns the peers the transport currently knows, with their addresses.
func (t *TCPTransport) Peers() map[NodeID]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[NodeID]string, len(t.peers))
	for id, p := range t.peers {
		out[id] = p.addr
	}
	return out
}

// isPeer reports whether id is a known peer.
func (t *TCPTransport) isPeer(id NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.peers[id]
	return ok
}

// LocalID returns this node's id.
func (t *TCPTransport) LocalID() NodeID { return t.cfg.NodeID }

// LocalAddr returns the actual listening address (useful when the config used
// port 0).
func (t *TCPTransport) LocalAddr() net.Addr { return t.ln.Addr() }

// Receive returns the inbound-message channel.
func (t *TCPTransport) Receive() <-chan Envelope { return t.recv }

func (t *TCPTransport) logf(format string, args ...any) {
	if t.cfg.Logf != nil {
		t.cfg.Logf(format, args...)
	}
}

// Send delivers a message to peer's live connection.
func (t *TCPTransport) Send(ctx context.Context, peer NodeID, kind MsgKind, payload []byte) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		t.m.closed.Inc()
		return ErrClosed
	}
	c := t.conns[peer]
	t.mu.Unlock()
	if len(payload) > MaxFrameSize {
		// The peer refuses a frame over MaxFrameSize and drops the connection:
		// it is refused here, before a byte is written (audit D2).
		t.m.writeFailed.Inc()
		return fmt.Errorf("%w: a %d-byte payload", ErrFrameTooLarge, len(payload))
	}
	if c == nil {
		t.m.notConnected.Inc()
		return ErrPeerNotConnected
	}
	if err := c.send(ctx, kind, payload); err != nil {
		t.m.writeFailed.Inc()
		return err
	}
	t.m.frameSent(kind, len(payload))
	return nil
}

// Close shuts the transport down. Idempotent (INV-T6).
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	conns := make([]*conn, 0, len(t.conns))
	for _, c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()

	t.cancel()       // stop dial loops and unblock recv sends
	_ = t.ln.Close() // unblock the accept loop
	for _, c := range conns {
		c.close() // unblock reader loops and blocked writes
	}
	t.wg.Wait() // every goroutine has exited
	close(t.recv)
	t.logf("event=shutdown_complete node=%s", t.cfg.NodeID)
	return nil
}

// acceptLoop accepts inbound connections until the transport is closed. An
// Accept error while it is open — descriptors exhausted (EMFILE), a transient
// network error — is logged and retried after a growing pause (audit D7/F10):
// returning would leave the listener open with nobody accepting, so peers
// would connect into the backlog and be silently black-holed. If the loop
// ever does give up, it closes the listener, so peers see refusals instead.
func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
	defer t.ln.Close()
	backoff := 5 * time.Millisecond
	for {
		nc, err := t.ln.Accept()
		if err != nil {
			if t.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return // shutdown
			}
			t.m.acceptFailed.Inc()
			t.logf("event=accept_failed node=%s err=%v retry_in=%s", t.cfg.NodeID, err, backoff)
			if sleepCtx(t.ctx, backoff) {
				return
			}
			backoff = min(2*backoff, time.Second)
			continue
		}
		backoff = 5 * time.Millisecond
		select {
		case t.handshake <- struct{}{}:
		default:
			// Too many connections still in their handshake: refuse this one
			// rather than let unauthenticated connections exhaust descriptors.
			t.m.handshakeRefused.Inc()
			_ = nc.Close()
			continue
		}
		t.wg.Add(1)
		go t.handleInbound(nc)
	}
}

// handleInbound reads the dialer's hello, validates it — not this node, a
// known peer that dials this node (the smaller id dials), this cluster, these
// settings — answers with a status and this node's own hello, and serves the
// connection if it accepted it.
func (t *TCPTransport) handleInbound(nc net.Conn) {
	defer t.wg.Done()
	released := false
	release := func() {
		if !released {
			released = true
			<-t.handshake
		}
	}
	defer release()

	_ = nc.SetDeadline(time.Now().Add(t.cfg.HandshakeTimeout))
	// Shutdown interrupts a handshake in flight instead of waiting out its
	// timeout.
	stop := context.AfterFunc(t.ctx, func() { _ = nc.Close() })
	defer stop()
	h, err := readHandshake(nc)
	if err != nil {
		t.logf("event=handshake_failed dir=inbound err=%v", mapTimeout(err))
		_ = nc.Close()
		return
	}
	status := statusAccepted
	switch {
	case h.id == t.cfg.NodeID:
		status = statusSelfConnection
	case !t.isPeer(h.id):
		status = statusUnknownPeer
	case h.id > t.cfg.NodeID:
		status = statusWrongDirection
	case h.cluster != t.self.cluster:
		status = statusWrongCluster
	case string(h.digest) != string(t.self.digest):
		status = statusWrongSettings
	}
	if err := writeReply(nc, status, t.self); err != nil || status != statusAccepted {
		if status != statusAccepted {
			err = statusError(status)
			t.m.handshakeRejected.Inc()
		}
		t.logf("event=handshake_failed dir=inbound peer=%q cluster=%q err=%v", h.id, h.cluster, mapTimeout(err)) // an unauthenticated claim: quoted
		_ = nc.Close()
		return
	}
	if !stop() {
		return // shut down: the connection is closed
	}
	_ = nc.SetDeadline(time.Time{}) // clear
	release()
	t.serve(h.id, nc, "inbound")
}

// dial handshakes an outbound connection to peer: it sends this node's hello
// and requires the accepter's answer to accept it and to come from peer, of
// this cluster, with these settings. Cancelling ctx (shutdown, or the peer's
// removal) interrupts it instead of waiting out the handshake timeout.
func (t *TCPTransport) dial(ctx context.Context, nc net.Conn, peer NodeID) error {
	_ = nc.SetDeadline(time.Now().Add(t.cfg.HandshakeTimeout))
	stop := context.AfterFunc(ctx, func() { _ = nc.Close() })
	defer stop()
	if err := writeHandshake(nc, t.self); err != nil {
		return err
	}
	status, h, err := readReply(nc)
	switch {
	case err != nil:
		return err
	case status != statusAccepted:
		t.m.handshakeRejected.Inc()
		return fmt.Errorf("refused by %q: %w", h.id, statusError(status))
	case h.id != peer:
		t.m.handshakeRejected.Inc()
		return fmt.Errorf("%w: dialed %s, reached %q", ErrWrongPeer, peer, h.id)
	case h.cluster != t.self.cluster:
		t.m.handshakeRejected.Inc()
		return fmt.Errorf("%w: %q belongs to %q", ErrClusterMismatch, h.id, h.cluster)
	case string(h.digest) != string(t.self.digest):
		t.m.handshakeRejected.Inc()
		return fmt.Errorf("%w: %q", ErrSettingsMismatch, h.id)
	}
	if !stop() {
		return ctx.Err() // cancelled: the connection is closed
	}
	_ = nc.SetDeadline(time.Time{})
	return nil
}

// dialLoop maintains a connection to one peer: dial, handshake, serve until the
// connection dies, then retry — bounded interval, cancelled on shutdown or
// when the peer is removed (ctx).
func (t *TCPTransport) dialLoop(ctx context.Context, peer NodeID, addr string) {
	defer t.wg.Done()
	d := net.Dialer{Timeout: t.cfg.DialTimeout}
	for {
		if ctx.Err() != nil {
			return
		}
		if t.hasConn(peer) {
			if sleepCtx(ctx, t.cfg.DialRetryInterval) {
				return
			}
			continue
		}
		nc, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			if ctx.Err() == nil {
				t.m.dialFailed.Inc()
			}
			if sleepCtx(ctx, t.cfg.DialRetryInterval) {
				return
			}
			continue
		}
		if err := t.dial(ctx, nc, peer); err != nil {
			t.m.dialFailed.Inc()
			t.logf("event=handshake_failed dir=outbound peer=%s err=%v", peer, mapTimeout(err))
			_ = nc.Close()
			if sleepCtx(ctx, t.cfg.DialRetryInterval) {
				return
			}
			continue
		}
		t.serve(peer, nc, "outbound") // blocks until the connection dies
		// A dead connection waits the same retry interval as a failed dial.
		// Redialling immediately would spin at CPU speed against a peer that
		// accepts and instantly closes — a crash-looping peer, or a partition
		// that resets connections — burning ports and flooding logs.
		if sleepCtx(ctx, t.cfg.DialRetryInterval) {
			return
		}
	}
}

// serve registers the connection (keep-existing) and runs its reader loop,
// deregistering when the loop ends. It blocks for the connection's lifetime.
func (t *TCPTransport) serve(peer NodeID, nc net.Conn, dir string) {
	c := newConn(peer, nc, t.cfg.WriteTimeout)

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		c.close()
		return
	}
	if _, exists := t.conns[peer]; exists {
		// keep-existing: a connection to this peer already exists.
		t.mu.Unlock()
		c.close()
		return
	}
	if _, known := t.peers[peer]; !known {
		// Removed while this connection was being set up.
		t.mu.Unlock()
		c.close()
		return
	}
	t.conns[peer] = c
	t.mu.Unlock()
	if dir == "inbound" {
		t.m.inbound.Inc()
	} else {
		t.m.outbound.Inc()
	}
	t.logf("event=peer_connected peer=%s dir=%s", peer, dir)

	t.readLoop(c)

	t.mu.Lock()
	if t.conns[peer] == c {
		delete(t.conns, peer)
	}
	t.mu.Unlock()
	c.close()
	t.logf("event=peer_disconnected peer=%s", peer)
}

// readLoop reads frames until the connection ends or errors, delivering each as
// an Envelope tagged with the connection's peer identity. The read idle timeout
// is re-armed before every read, so it measures silence, not frame size: a
// large frame arriving slowly over a busy link is not torn down while its
// bytes still flow.
func (t *TCPTransport) readLoop(c *conn) {
	hdr := make([]byte, record.HeaderSize)
	var r io.Reader = c.nc
	if t.cfg.ReadIdleTimeout > 0 {
		r = idleReader{c.nc, t.cfg.ReadIdleTimeout}
	}
	for {
		kind, payload, err := readFrame(r, hdr)
		if err != nil {
			return // io.EOF (clean) or a protocol error — either way the conn is done
		}
		t.m.frameReceived(kind, len(payload))
		select {
		case t.recv <- Envelope{Peer: c.peer, Kind: kind, Payload: payload}:
		case <-t.ctx.Done():
			return
		}
	}
}

// idleReader re-arms a connection's read deadline before every read.
type idleReader struct {
	nc   net.Conn
	idle time.Duration
}

func (r idleReader) Read(p []byte) (int, error) {
	_ = r.nc.SetReadDeadline(time.Now().Add(r.idle))
	return r.nc.Read(p)
}

// hasConn reports whether a live connection to peer is registered.
func (t *TCPTransport) hasConn(peer NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.conns[peer]
	return ok
}

// sleepCtx waits for d or until ctx ends; it returns true if ctx ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}

// mapTimeout renders a net timeout as ErrHandshakeTimeout for clearer logs.
func mapTimeout(err error) error {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return ErrHandshakeTimeout
	}
	return err
}
