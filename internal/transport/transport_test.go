package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
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
	buf := make([]byte, 16)
	if _, err := c.Read(buf); err == nil {
		t.Fatal("connection was not closed by the accepter")
	} else if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
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
		_ = writeHandshake(c, "a") // announce a's own id
	})
}

func TestUnknownPeerRejected(t *testing.T) {
	a := newTransport(t, "a", map[NodeID]string{"b": "127.0.0.1:1"})
	rawExpectClosed(t, a.LocalAddr().String(), func(c net.Conn) {
		_ = writeHandshake(c, "stranger") // not a configured peer
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
