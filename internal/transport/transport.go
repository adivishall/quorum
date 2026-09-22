package transport

import (
	"context"
	"errors"
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
	closed bool
}

var _ Transport = (*TCPTransport)(nil)

// NewTCPTransport validates cfg, starts listening, and starts the accept loop
// and the dial loops for peers this node is responsible for dialing (the node
// with the smaller id dials — ADR-014). A ListenAddr with port 0 lets the OS
// choose; LocalAddr reports the chosen address.
func NewTCPTransport(cfg Config) (*TCPTransport, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &TCPTransport{
		cfg:    cfg,
		ln:     ln,
		recv:   make(chan Envelope, recvBuffer),
		ctx:    ctx,
		cancel: cancel,
		conns:  make(map[NodeID]*conn),
	}

	t.wg.Add(1)
	go t.acceptLoop()

	for peer, addr := range cfg.Peers {
		if cfg.NodeID < peer { // smaller id dials
			t.wg.Add(1)
			go t.dialLoop(peer, addr)
		}
	}
	t.logf("event=node_started node=%s listen=%s peers=%d", cfg.NodeID, ln.Addr(), len(cfg.Peers))
	return t, nil
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
		return ErrClosed
	}
	c := t.conns[peer]
	t.mu.Unlock()
	if c == nil {
		return ErrPeerNotConnected
	}
	return c.send(ctx, kind, payload)
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

// acceptLoop accepts inbound connections until the listener is closed.
func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		nc, err := t.ln.Accept()
		if err != nil {
			return // listener closed on shutdown
		}
		t.wg.Add(1)
		go t.handleInbound(nc)
	}
}

// handleInbound reads the dialer's handshake, validates it, and serves the
// connection. The accepter sends no handshake back (the handshake is one-way).
func (t *TCPTransport) handleInbound(nc net.Conn) {
	defer t.wg.Done()

	_ = nc.SetReadDeadline(time.Now().Add(t.cfg.HandshakeTimeout))
	peer, err := readHandshake(nc)
	if err != nil {
		t.logf("event=handshake_failed dir=inbound err=%v", mapTimeout(err))
		_ = nc.Close()
		return
	}
	_ = nc.SetReadDeadline(time.Time{}) // clear

	if peer == t.cfg.NodeID {
		t.logf("event=handshake_failed dir=inbound err=%v", ErrSelfConnection)
		_ = nc.Close()
		return
	}
	if _, ok := t.cfg.Peers[peer]; !ok {
		t.logf("event=handshake_failed dir=inbound peer=%s err=%v", peer, ErrUnknownPeer)
		_ = nc.Close()
		return
	}
	t.serve(peer, nc, "inbound")
}

// dialLoop maintains a connection to one peer: dial, handshake, serve until the
// connection dies, then retry — bounded interval, cancelled on shutdown.
func (t *TCPTransport) dialLoop(peer NodeID, addr string) {
	defer t.wg.Done()
	d := net.Dialer{Timeout: t.cfg.DialTimeout}
	for {
		if t.ctx.Err() != nil {
			return
		}
		if t.hasConn(peer) {
			if t.sleep(t.cfg.DialRetryInterval) {
				return
			}
			continue
		}
		nc, err := d.DialContext(t.ctx, "tcp", addr)
		if err != nil {
			if t.sleep(t.cfg.DialRetryInterval) {
				return
			}
			continue
		}
		_ = nc.SetWriteDeadline(time.Now().Add(t.cfg.HandshakeTimeout))
		if err := writeHandshake(nc, t.cfg.NodeID); err != nil {
			t.logf("event=handshake_failed dir=outbound peer=%s err=%v", peer, mapTimeout(err))
			_ = nc.Close()
			if t.sleep(t.cfg.DialRetryInterval) {
				return
			}
			continue
		}
		_ = nc.SetWriteDeadline(time.Time{})
		t.serve(peer, nc, "outbound") // blocks until the connection dies
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
	t.conns[peer] = c
	t.mu.Unlock()
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
// an Envelope tagged with the connection's peer identity.
func (t *TCPTransport) readLoop(c *conn) {
	hdr := make([]byte, record.HeaderSize)
	for {
		if t.cfg.ReadIdleTimeout > 0 {
			_ = c.nc.SetReadDeadline(time.Now().Add(t.cfg.ReadIdleTimeout))
		}
		kind, payload, err := readFrame(c.nc, hdr)
		if err != nil {
			return // io.EOF (clean) or a protocol error — either way the conn is done
		}
		select {
		case t.recv <- Envelope{Peer: c.peer, Kind: kind, Payload: payload}:
		case <-t.ctx.Done():
			return
		}
	}
}

// hasConn reports whether a live connection to peer is registered.
func (t *TCPTransport) hasConn(peer NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.conns[peer]
	return ok
}

// sleep waits for d or until shutdown; it returns true if shutdown fired.
func (t *TCPTransport) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-t.ctx.Done():
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
