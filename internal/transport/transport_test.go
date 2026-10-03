package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTransport(t *testing.T, id NodeID, peers map[NodeID]string) *TCPTransport {
	t.Helper()
	tr, err := NewTCPTransport(Config{
		NodeID:            id,
		ListenAddr:        "127.0.0.1:0",
		Peers:             peers,
		HandshakeTimeout:  2 * time.Second,
		DialRetryInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewTCPTransport(%q): %v", id, err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func waitConnected(t *testing.T, tr *TCPTransport, peer NodeID, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if tr.hasConn(peer) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %s did not connect to %s within %s", tr.LocalID(), peer, d)
}

func recv(t *testing.T, tr *TCPTransport, d time.Duration) Envelope {
	t.Helper()
	select {
	case e := <-tr.Receive():
		return e
	case <-time.After(d):
		t.Fatalf("node %s: no message within %s", tr.LocalID(), d)
		return Envelope{}
	}
}

// connectedPair builds two connected nodes. The higher id ("b") is built first so
// its listen address is known to the dialer ("a", the lower id, which dials).
func connectedPair(t *testing.T) (a, b *TCPTransport) {
	t.Helper()
	b = newTransport(t, "b", map[NodeID]string{"a": "127.0.0.1:0"}) // b never dials a; addr is a placeholder
	a = newTransport(t, "a", map[NodeID]string{"b": b.LocalAddr().String()})
	waitConnected(t, a, "b", 3*time.Second)
	waitConnected(t, b, "a", 3*time.Second)
	return a, b
}

func TestTCPHandshakeAndProbe(t *testing.T) {
	a, b := connectedPair(t)
	ctx := context.Background()

	// a -> b: Probe
	if err := a.Send(ctx, "b", MsgProbe, Probe{RequestID: 1, Token: []byte("ping")}.Marshal()); err != nil {
		t.Fatalf("a.Send: %v", err)
	}
	env := recv(t, b, 2*time.Second)
	if env.Peer != "a" || env.Kind != MsgProbe {
		t.Fatalf("b received %+v, want from a kind Probe", env)
	}
	p, err := ParseProbe(env.Payload)
	if err != nil || p.RequestID != 1 || string(p.Token) != "ping" {
		t.Fatalf("b decoded probe %+v err=%v", p, err)
	}

	// b -> a: ProbeResponse on the same (bidirectional) connection
	if err := b.Send(ctx, "a", MsgProbeResponse, ProbeResponse{RequestID: 1, Token: p.Token}.Marshal()); err != nil {
		t.Fatalf("b.Send: %v", err)
	}
	env = recv(t, a, 2*time.Second)
	if env.Peer != "b" || env.Kind != MsgProbeResponse {
		t.Fatalf("a received %+v, want from b kind ProbeResponse", env)
	}
	resp, err := ParseProbeResponse(env.Payload)
	if err != nil || resp.RequestID != 1 || string(resp.Token) != "ping" {
		t.Fatalf("a decoded response %+v err=%v", resp, err)
	}
}

// TestReceivedEnvelopeCarriesConnectionPeerID and TestPayloadCannotSpoofSender:
// the envelope's Peer is the handshake identity of the connection, never a value
// from the payload. Here a's payload token spells "b" (a would-be spoof), yet the
// envelope is attributed to "a".
func TestPayloadCannotSpoofSender(t *testing.T) {
	a, b := connectedPair(t)
	if err := a.Send(context.Background(), "b", MsgProbe, Probe{RequestID: 9, Token: []byte("b")}.Marshal()); err != nil {
		t.Fatal(err)
	}
	env := recv(t, b, 2*time.Second)
	if env.Peer != "a" {
		t.Fatalf("envelope attributed to %q, want the connection identity \"a\"", env.Peer)
	}
}

func TestPerConnectionOrderPreserved(t *testing.T) {
	a, b := connectedPair(t)
	const n = 200
	for i := 0; i < n; i++ {
		if err := a.Send(context.Background(), "b", MsgProbe, Probe{RequestID: uint64(i)}.Marshal()); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		env := recv(t, b, 2*time.Second)
		p, err := ParseProbe(env.Payload)
		if err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
		if p.RequestID != uint64(i) {
			t.Fatalf("frame %d arrived with id %d — per-connection order not preserved", i, p.RequestID)
		}
	}
}

func TestConcurrentSendersDoNotInterleave(t *testing.T) {
	a, b := connectedPair(t)
	const senders, each = 8, 50
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				_ = a.Send(context.Background(), "b", MsgProbe, Probe{RequestID: uint64(s*1000 + i), Token: []byte("concurrent")}.Marshal())
			}
		}(s)
	}
	wg.Wait()
	// Every frame must decode cleanly — interleaved bytes would corrupt a frame.
	for i := 0; i < senders*each; i++ {
		env := recv(t, b, 3*time.Second)
		if _, err := ParseProbe(env.Payload); err != nil {
			t.Fatalf("frame %d did not decode (bytes interleaved?): %v", i, err)
		}
	}
}

func TestReconnectAfterConnectionDrop(t *testing.T) {
	a, b := connectedPair(t)
	// Probe once.
	if err := a.Send(context.Background(), "b", MsgProbe, Probe{RequestID: 1}.Marshal()); err != nil {
		t.Fatal(err)
	}
	recv(t, b, 2*time.Second)

	// Force-drop a's connection to b (as a network blip would). a is the dialer,
	// so its dial loop must re-establish the connection.
	a.mu.Lock()
	c := a.conns["b"]
	a.mu.Unlock()
	if c == nil {
		t.Fatal("no connection to drop")
	}
	c.close()

	// Both sides re-establish.
	waitConnected(t, a, "b", 3*time.Second)
	waitConnected(t, b, "a", 3*time.Second)

	// Probe again on the new connection.
	if err := a.Send(context.Background(), "b", MsgProbe, Probe{RequestID: 2}.Marshal()); err != nil {
		t.Fatalf("send after reconnect: %v", err)
	}
	env := recv(t, b, 2*time.Second)
	if p, _ := ParseProbe(env.Payload); p.RequestID != 2 {
		t.Fatalf("after reconnect got id %d, want 2", p.RequestID)
	}
}

// TestDialerBacksOffWhenPeerKeepsClosingConnections pins the dial loop's retry
// pacing at the connection level, not only the dial level: against a peer that
// accepts and instantly closes every connection (a crash-looping process, or a
// partition that resets connections), the dialer must wait its retry interval
// after the connection dies, not redial at CPU speed. Without that backoff the
// dialer reconnects roughly every 250µs (measured against a resetting proxy),
// burning an ephemeral port and two log lines per cycle.
//
// Since the handshake is answered (version 2), a peer that closes before
// answering fails the dial itself: only a peer that answers and then closes
// reaches the connection's own death. Both are checked; the first is the
// backoff after a connection dies.
func TestDialerBacksOffWhenPeerKeepsClosingConnections(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer bool
	}{
		{"after the handshake is answered", true},
		{"before the handshake is answered", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			var accepted, answered atomic.Int64
			go func() {
				for {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					accepted.Add(1)
					if tc.answer {
						if _, err := readHandshake(c); err == nil && writeReply(c, statusAccepted, hello{id: "b"}) == nil {
							answered.Add(1)
						}
					}
					_ = c.Close()
				}
			}()

			a, err := NewTCPTransport(Config{
				NodeID: "a", ListenAddr: "127.0.0.1:0",
				Peers:             map[NodeID]string{"b": ln.Addr().String()},
				DialRetryInterval: 100 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()

			time.Sleep(650 * time.Millisecond)
			got := accepted.Load()
			if got == 0 {
				t.Fatal("the dialer never attempted a connection; the test proved nothing")
			}
			if tc.answer && answered.Load() == 0 {
				t.Fatal("no handshake was answered: no connection was established to die")
			}
			// At one attempt per 100ms interval, 650ms allows ~7 attempts; 15
			// leaves slack for scheduling. A dialer without the backoff makes
			// hundreds.
			if got > 15 {
				t.Fatalf("%d connection attempts in 650ms with a 100ms retry interval: the dialer must back off after a connection dies, not spin", got)
			}
		})
	}
}

// TestReaderIdleTimeoutReconnectsASilentConnection pins the read-idle-timeout: a
// connection that is established but silently delivers nothing must be torn down
// so the dialer reconnects. This is the Phase 10 real-process flake's root cause
// — a proxied link (and, in production, a peer that vanished without closing the
// socket) can leave the dialer with a connection whose writes succeed into a dead
// socket while its reader blocks in Read forever; because a registered connection
// suppresses redialling, the node then believes it has a peer it can never reach.
// The black-hole peer accepts connections and holds them open, never sending a
// frame; with the idle timeout the dialer must tear each down and redial. Without
// it (ReadIdleTimeout == 0) the dialer registers the first connection and blocks
// forever: exactly one accept.
func TestReaderIdleTimeoutReconnectsASilentConnection(t *testing.T) {
	bh, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer bh.Close()
	var accepts atomic.Int64
	go func() {
		var held []net.Conn
		defer func() {
			for _, c := range held {
				_ = c.Close()
			}
		}()
		for {
			c, err := bh.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			// Complete the handshake as b, then fall silent: hold open, never
			// read, never send, never close — an ESTABLISHED connection that
			// delivers nothing.
			if _, err := readHandshake(c); err == nil {
				_ = writeReply(c, statusAccepted, hello{id: "b"})
			}
			held = append(held, c)
		}
	}()

	const idle = 200 * time.Millisecond
	a, err := NewTCPTransport(Config{
		NodeID: "a", ListenAddr: "127.0.0.1:0",
		Peers:             map[NodeID]string{"b": bh.Addr().String()},
		DialRetryInterval: 50 * time.Millisecond,
		ReadIdleTimeout:   idle,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	// Each silent connection is torn down after ~idle and redialled after the
	// retry interval, so several accepts occur within a few idle periods.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && accepts.Load() < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := accepts.Load(); got < 3 {
		t.Fatalf("black-hole peer saw %d connection(s) in 3s; with a %s read idle timeout a silent connection must be torn down and redialled, not held open forever", got, idle)
	}
}

func TestSendToUnconnectedPeerFails(t *testing.T) {
	a := newTransport(t, "a", map[NodeID]string{"z": "127.0.0.1:1"}) // z never comes up
	err := a.Send(context.Background(), "z", MsgProbe, Probe{}.Marshal())
	if !errors.Is(err, ErrPeerNotConnected) {
		t.Fatalf("send to unconnected peer: err = %v, want ErrPeerNotConnected", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	a, _ := connectedPair(t)
	if err := a.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestSendAfterCloseFails(t *testing.T) {
	a, _ := connectedPair(t)
	_ = a.Close()
	if err := a.Send(context.Background(), "b", MsgProbe, Probe{}.Marshal()); !errors.Is(err, ErrClosed) {
		t.Fatalf("send after close: err = %v, want ErrClosed", err)
	}
}

func TestNoGoroutineLeakAfterClose(t *testing.T) {
	base := runtime.NumGoroutine()
	// Build, connect, exchange, and close a pair entirely within this function.
	func() {
		b, err := NewTCPTransport(Config{NodeID: "b", ListenAddr: "127.0.0.1:0", Peers: map[NodeID]string{"a": "127.0.0.1:0"}, DialRetryInterval: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		a, err := NewTCPTransport(Config{NodeID: "a", ListenAddr: "127.0.0.1:0", Peers: map[NodeID]string{"b": b.LocalAddr().String()}, DialRetryInterval: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		waitConnected(t, a, "b", 3*time.Second)
		waitConnected(t, b, "a", 3*time.Second)
		_ = a.Send(context.Background(), "b", MsgProbe, Probe{RequestID: 1}.Marshal())
		recv(t, b, 2*time.Second)
		_ = a.Close()
		_ = b.Close()
	}()

	// Poll until goroutines return to baseline (allow a small slack for the
	// runtime's own transient goroutines).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= base+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines did not return to baseline: have %d, base %d", runtime.NumGoroutine(), base)
}

// --- adversarial handshakes over a raw dial ---

// rawExpectClosed dials a's listener, runs write (which may send a bad
// handshake), and asserts a closes the connection (a read returns EOF/err).
func rawExpectClosed(t *testing.T, addr string, write func(net.Conn)) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if write != nil {
		write(c)
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	// The accepter may answer a refused handshake with its reply before it
	// closes: read until the connection ends.
	buf := make([]byte, 16)
	for err == nil {
		_, err = c.Read(buf)
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		// A reset or EOF are both acceptable evidence the accepter closed it.
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatalf("accepter did not close the connection (read timed out): %v", err)
		}
	}
}

func TestSelfConnectionRejected(t *testing.T) {
	a := newTransport(t, "a", map[NodeID]string{"b": "127.0.0.1:1"})
	rawExpectClosed(t, a.LocalAddr().String(), func(c net.Conn) {
		_ = writeHandshake(c, hello{id: "a"}) // announce a's own id
	})
}

func TestUnknownPeerRejected(t *testing.T) {
	a := newTransport(t, "a", map[NodeID]string{"b": "127.0.0.1:1"})
	rawExpectClosed(t, a.LocalAddr().String(), func(c net.Conn) {
		_ = writeHandshake(c, hello{id: "stranger"}) // not a configured peer
	})
}

func TestHandshakeTimeoutClosesConnection(t *testing.T) {
	tr, err := NewTCPTransport(Config{
		NodeID:           "a",
		ListenAddr:       "127.0.0.1:0",
		Peers:            map[NodeID]string{"b": "127.0.0.1:1"},
		HandshakeTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	// Send nothing; the accepter must close the connection after the handshake
	// timeout rather than waiting forever.
	rawExpectClosed(t, tr.LocalAddr().String(), nil)
}

func TestBadMagicClosesConnection(t *testing.T) {
	a := newTransport(t, "a", map[NodeID]string{"b": "127.0.0.1:1"})
	rawExpectClosed(t, a.LocalAddr().String(), func(c net.Conn) {
		_, _ = c.Write([]byte("NOPEnot-a-handshake"))
	})
}

// --- Phase 15: a peer set that changes at runtime ---

// TestAddPeerConnectsAndRemovePeerDisconnects: a peer unknown at construction
// is refused; once both sides add each other the lower id dials and frames
// flow; once one side removes the other the connection is closed, its
// handshakes are refused again, and the dialer stops dialing it.
func TestAddPeerConnectsAndRemovePeerDisconnects(t *testing.T) {
	a := newTransport(t, "a", nil)
	c := newTransport(t, "c", nil)
	rawExpectClosed(t, a.LocalAddr().String(), func(nc net.Conn) { _ = writeHandshake(nc, hello{id: "c"}) })

	if err := c.AddPeer("a", a.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	if err := a.AddPeer("c", c.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	waitConnected(t, a, "c", 3*time.Second)
	waitConnected(t, c, "a", 3*time.Second)
	if err := a.Send(context.Background(), "c", MsgProbe, Probe{RequestID: 7}.Marshal()); err != nil {
		t.Fatal(err)
	}
	if env := recv(t, c, 2*time.Second); env.Peer != "a" || env.Kind != MsgProbe {
		t.Fatalf("c received %+v", env)
	}
	// Adding it again at the same address does nothing; at another is refused.
	if err := a.AddPeer("c", c.LocalAddr().String()); err != nil {
		t.Fatalf("re-adding a known peer: %v", err)
	}
	if err := a.AddPeer("c", "127.0.0.1:1"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("re-adding a known peer at another address: %v", err)
	}

	if err := c.RemovePeer("a"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for c.hasConn("a") || a.hasConn("c") {
		if time.Now().After(deadline) {
			t.Fatalf("still connected after the removal: a->c %v, c->a %v", a.hasConn("c"), c.hasConn("a"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	// a keeps dialing c, and c refuses every attempt: no connection comes back
	// over several retry intervals.
	time.Sleep(300 * time.Millisecond)
	if c.hasConn("a") || a.hasConn("c") {
		t.Fatal("a removed peer reconnected")
	}
	if _, ok := c.Peers()["a"]; ok {
		t.Fatal("Peers still lists the removed peer")
	}
	if err := c.RemovePeer("a"); err != nil {
		t.Fatalf("removing an unknown peer: %v", err)
	}
	// Re-admitting it restores the link.
	if err := c.AddPeer("a", a.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	waitConnected(t, a, "c", 3*time.Second)
}

// TestAddPeerRefusesInvalidPeers: the same rules as the construction-time peer
// set, and nothing after Close.
func TestAddPeerRefusesInvalidPeers(t *testing.T) {
	a := newTransport(t, "a", nil)
	for name, tc := range map[string]struct {
		id   NodeID
		addr string
	}{
		"empty id":   {"", "127.0.0.1:1"},
		"self":       {"a", "127.0.0.1:1"},
		"long id":    {NodeID(make([]byte, MaxNodeIDLen+1)), "127.0.0.1:1"},
		"bad addr":   {"b", "nowhere"},
		"empty addr": {"b", ""},
	} {
		if err := a.AddPeer(tc.id, tc.addr); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v", name, err)
		}
	}
	_ = a.Close()
	if err := a.AddPeer("b", "127.0.0.1:1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddPeer after Close: %v", err)
	}
}

// TestRemovedPeerDialLoopExits: removing a peer this node dials stops its dial
// loop — goroutines return to the baseline without closing the transport.
func TestRemovedPeerDialLoopExits(t *testing.T) {
	a := newTransport(t, "a", nil)
	base := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		id := NodeID("p" + string(rune('a'+i)))
		if err := a.AddPeer(id, "127.0.0.1:1"); err != nil { // nothing listens: the loop keeps retrying
			t.Fatal(err)
		}
	}
	if runtime.NumGoroutine() < base+20 {
		t.Fatalf("expected 20 dial loops: have %d goroutines, base %d", runtime.NumGoroutine(), base)
	}
	for i := 0; i < 20; i++ {
		_ = a.RemovePeer(NodeID("p" + string(rune('a'+i))))
	}
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > base+2 {
		if time.Now().After(deadline) {
			t.Fatalf("dial loops did not exit: have %d goroutines, base %d", runtime.NumGoroutine(), base)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
