package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/record"
)

// Phase 5 of the audit's roadmap: cluster isolation (H2), settings agreement
// (H5), a two-way handshake that checks who answered, connections that are
// never reused after a failed write (M2), an accept loop that survives errors
// (D7/F10), bounded pre-handshake connections, and a sender that never writes
// a frame its peer would refuse (D2).

type logSink struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logSink) logf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.WriteString(fmt.Sprintf(f, a...) + "\n")
}

func (l *logSink) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// pairWith builds a and b, each the other's peer, with the given cluster ids
// and digests.
func pairWith(t *testing.T, ca, cb string, da, db []byte) (a, b *TCPTransport, la, lb *logSink) {
	t.Helper()
	la, lb = &logSink{}, &logSink{}
	var err error
	b, err = NewTCPTransport(Config{NodeID: "b", ListenAddr: "127.0.0.1:0", ClusterID: cb, SettingsDigest: db,
		Peers: map[NodeID]string{"a": "127.0.0.1:1"}, DialRetryInterval: 20 * time.Millisecond, Logf: lb.logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	a, err = NewTCPTransport(Config{NodeID: "a", ListenAddr: "127.0.0.1:0", ClusterID: ca, SettingsDigest: da,
		Peers: map[NodeID]string{"b": b.LocalAddr().String()}, DialRetryInterval: 20 * time.Millisecond, Logf: la.logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a, b, la, lb
}

// neverConnect requires the pair to stay disconnected for a while, and both
// logs to name the reason.
func neverConnect(t *testing.T, a, b *TCPTransport, la, lb *logSink, reason string) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if a.hasConn("b") || b.hasConn("a") {
			t.Fatalf("the pair connected; they must refuse each other (%s)", reason)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := a.Send(context.Background(), "b", MsgProbe, Probe{RequestID: 1}.Marshal()); !errors.Is(err, ErrPeerNotConnected) {
		t.Fatalf("Send across a refused pair = %v, want ErrPeerNotConnected", err)
	}
	for name, l := range map[string]*logSink{"dialer": la, "accepter": lb} {
		if !strings.Contains(l.String(), reason) {
			t.Fatalf("the %s's log does not name %q:\n%s", name, reason, l.String())
		}
	}
}

// TestNodesOfAnotherClusterNeverConnect (audit H2): two nodes that name each
// other as peers — a wrong -peers entry, a recycled address — but belong to
// different clusters refuse each other, both ways, and no frame flows.
func TestNodesOfAnotherClusterNeverConnect(t *testing.T) {
	a, b, la, lb := pairWith(t, "prod", "staging", nil, nil)
	neverConnect(t, a, b, la, lb, "another cluster")
	// The same pair in one cluster connects.
	a2, _, _, _ := pairWith(t, "prod", "prod", []byte{1}, []byte{1})
	waitConnected(t, a2, "b", 3*time.Second)
}

// TestNodesWithOtherReplicaSettingsNeverConnect (audit H5): nodes whose
// replica settings digests differ — session limits or routing that would make
// their replicas decide entries differently — never exchange a frame.
func TestNodesWithOtherReplicaSettingsNeverConnect(t *testing.T) {
	a, b, la, lb := pairWith(t, "prod", "prod", []byte{1, 2}, []byte{1, 3})
	neverConnect(t, a, b, la, lb, "replica settings differ")
}

// TestDialerChecksTheAnswer: the handshake is two-way. a dials its peer "b"
// and the accepter accepts — but announces itself as another node, of another
// cluster, or with other settings: a stale address now serving another node,
// or an accepter whose own checks are absent. a drops the connection instead
// of taking the accepter's frames for b's, and names why.
func TestDialerChecksTheAnswer(t *testing.T) {
	for _, tc := range []struct {
		answer hello
		reason string
	}{
		{hello{id: "x", cluster: "prod", digest: []byte{1}}, "reached another node"},
		{hello{id: "b", cluster: "staging", digest: []byte{1}}, "another cluster"},
		{hello{id: "b", cluster: "prod", digest: []byte{2}}, "replica settings differ"},
		{hello{id: "b", cluster: "prod"}, "replica settings differ"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			addr, accepts := acceptingPeer(t, tc.answer)
			la := &logSink{}
			a, err := NewTCPTransport(Config{NodeID: "a", ListenAddr: "127.0.0.1:0", ClusterID: "prod", SettingsDigest: []byte{1},
				Peers: map[NodeID]string{"b": addr}, DialRetryInterval: 20 * time.Millisecond, Logf: la.logf})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = a.Close() })
			deadline := time.Now().Add(400 * time.Millisecond)
			for time.Now().Before(deadline) {
				if a.hasConn("b") {
					t.Fatalf("a registered the connection although the accepter announced %+v", tc.answer)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if accepts.Load() < 2 {
				t.Fatalf("a dialled %d times: it must drop the connection and retry", accepts.Load())
			}
			if !strings.Contains(la.String(), tc.reason) {
				t.Fatalf("a's log does not name %q:\n%s", tc.reason, la.String())
			}
		})
	}
}

// TestInboundFromTheWrongDirectionIsRefused (audit F16): the node with the
// smaller id dials; a connection that claims a LARGER known peer's id never
// comes from that peer, and is refused — so it cannot take the peer's slot.
func TestInboundFromTheWrongDirectionIsRefused(t *testing.T) {
	a := newTransport(t, "a", map[NodeID]string{"z": "127.0.0.1:1"})
	nc, err := net.DialTimeout("tcp", a.LocalAddr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeHandshake(nc, hello{id: "z"}); err != nil {
		t.Fatal(err)
	}
	status, h, err := readReply(nc)
	if err != nil || status != statusWrongDirection || h.id != "a" {
		t.Fatalf("reply to a wrong-direction dial: status %d from %q, %v; want wrong direction from a", status, h.id, err)
	}
	if a.hasConn("z") {
		t.Fatal("the wrong-direction connection was registered")
	}
}

// acceptingPeer listens and answers every dialer's handshake with "accepted"
// and the given hello, whatever the dialer announced; then it reads nothing at
// all, so the dialer's writes fill the socket buffers and block.
func acceptingPeer(t *testing.T, answer hello) (addr string, accepts *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepts = &atomic.Int64{}
	var held []net.Conn
	var mu sync.Mutex
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		mu.Unlock()
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetReadBuffer(4096)
			}
			if _, err := readHandshake(c); err == nil {
				_ = writeReply(c, statusAccepted, answer)
			}
			accepts.Add(1)
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	return ln.Addr().String(), accepts
}

// TestFailedWriteClosesTheConnection (audit M2): a frame whose write times out
// part-way has left part of itself on the wire; any frame after it would be
// read as garbage. The connection is closed at once — not kept for the next
// Send — and the dialer reconnects.
func TestFailedWriteClosesTheConnection(t *testing.T) {
	addr, accepts := acceptingPeer(t, hello{id: "b"})
	a, err := NewTCPTransport(Config{NodeID: "a", ListenAddr: "127.0.0.1:0", Peers: map[NodeID]string{"b": addr},
		DialRetryInterval: 20 * time.Millisecond, WriteTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	waitConnected(t, a, "b", 3*time.Second)
	big := make([]byte, MaxFrameSize-64) // far beyond any socket buffer
	if err := a.Send(context.Background(), "b", MsgAppendEntries, big); err == nil {
		t.Fatal("a 16 MiB frame to a peer that reads nothing was written in full: the premise failed")
	}
	// The reader loop deregisters the connection as soon as its closed socket
	// fails a read.
	deadline := time.Now().Add(2 * time.Second)
	for accepts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if accepts.Load() < 2 {
		t.Fatal("the connection a partial frame was left on was kept: the dialer never reconnected")
	}
}

// TestSendRefusesAFrameItsPeerWouldRefuse (audit D2): a payload over
// MaxFrameSize is refused before a byte is written, and the connection stays
// healthy.
func TestSendRefusesAFrameItsPeerWouldRefuse(t *testing.T) {
	a, b := connectedPair(t)
	if err := a.Send(context.Background(), "b", MsgAppendEntries, make([]byte, MaxFrameSize+1)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Send of an oversized frame = %v, want ErrFrameTooLarge", err)
	}
	if err := a.Send(context.Background(), "b", MsgProbe, Probe{RequestID: 9}.Marshal()); err != nil {
		t.Fatalf("Send after the refusal: %v", err)
	}
	if env := recv(t, b, 3*time.Second); env.Kind != MsgProbe {
		t.Fatalf("received %v after the refusal, want the probe", env.Kind)
	}
}

// flakyListener fails its first Accepts with a temporary error.
type flakyListener struct {
	net.Listener
	fails atomic.Int64
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if l.fails.Add(-1) >= 0 {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
	}
	return l.Listener.Accept()
}

// TestAcceptLoopSurvivesAcceptErrors (audit D7/F10): Accept errors — the
// process out of descriptors — are logged and retried; before, the loop
// returned for good, leaving the listener open with nobody accepting, so peers
// connected into the backlog and were silently black-holed.
func TestAcceptLoopSurvivesAcceptErrors(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fl := &flakyListener{Listener: ln}
	fl.fails.Store(3)
	log := &logSink{}
	cfg := Config{NodeID: "b", ListenAddr: ln.Addr().String(), Peers: map[NodeID]string{"a": "127.0.0.1:1"}, Logf: log.logf}
	b := newTCPTransport(cfg.withDefaults(), fl)
	t.Cleanup(func() { _ = b.Close() })
	a := newTransport(t, "a", map[NodeID]string{"b": ln.Addr().String()})
	waitConnected(t, a, "b", 5*time.Second)
	if n := strings.Count(log.String(), "event=accept_failed"); n != 3 {
		t.Fatalf("%d accept failures logged, want 3:\n%s", n, log.String())
	}
}

// TestPendingHandshakesAreBounded (audit F10): connections that never finish
// their handshake hold a slot each; beyond MaxPendingHandshakes a new one is
// refused at once instead of costing another descriptor for the handshake
// timeout. Once the slots time out, a real peer connects.
func TestPendingHandshakesAreBounded(t *testing.T) {
	b, err := NewTCPTransport(Config{NodeID: "b", ListenAddr: "127.0.0.1:0", Peers: map[NodeID]string{"a": "127.0.0.1:1"},
		MaxPendingHandshakes: 2, HandshakeTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	var silent []net.Conn
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", b.LocalAddr().String())
		if err != nil {
			t.Fatal(err)
		}
		silent = append(silent, c)
	}
	defer func() {
		for _, c := range silent {
			_ = c.Close()
		}
	}()
	waitUntil(t, 2*time.Second, func() bool { return len(b.handshake) == 2 })
	c, err := net.Dial("tcp", b.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("the third pending connection was served")
	}
	if waited := time.Since(start); waited > 300*time.Millisecond {
		t.Fatalf("the third pending connection was held %s; it must be refused at once", waited)
	}
	// After the silent ones time out, a real peer connects — and its
	// connection, past its handshake, holds no slot.
	a := newTransport(t, "a", map[NodeID]string{"b": b.LocalAddr().String()})
	waitConnected(t, a, "b", 5*time.Second)
	waitUntil(t, 2*time.Second, func() bool { return len(b.handshake) == 0 })
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("the condition never held")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestDialerDropsAPeerThatNeverAnswers: an accepter that takes the connection
// but never answers the handshake is dropped after the handshake timeout and
// redialled — never registered as connected.
func TestDialerDropsAPeerThatNeverAnswers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepts atomic.Int64
	go func() {
		var held []net.Conn
		defer func() {
			for _, c := range held {
				_ = c.Close()
			}
		}()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			held = append(held, c) // never answers
		}
	}()
	a, err := NewTCPTransport(Config{NodeID: "a", ListenAddr: "127.0.0.1:0", Peers: map[NodeID]string{"b": ln.Addr().String()},
		DialRetryInterval: 20 * time.Millisecond, HandshakeTimeout: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	deadline := time.Now().Add(3 * time.Second)
	for accepts.Load() < 3 && time.Now().Before(deadline) {
		if a.hasConn("b") {
			t.Fatal("a peer that never answered the handshake was registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if accepts.Load() < 3 {
		t.Fatalf("%d dials in 3s: the unanswered handshake was not timed out and retried", accepts.Load())
	}
}

// TestCloseInterruptsHandshakesInFlight: since the handshake is answered, a
// dialer waits for the reply; Close interrupts that wait, and an inbound
// handshake still being read, instead of waiting out the handshake timeout.
func TestCloseInterruptsHandshakesInFlight(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c // never answers
		}
	}()
	a, err := NewTCPTransport(Config{NodeID: "a", ListenAddr: "127.0.0.1:0", HandshakeTimeout: 30 * time.Second,
		Peers: map[NodeID]string{"b": ln.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	var server net.Conn
	select {
	case server = <-accepted:
		defer server.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("a never dialled b")
	}
	silent, err := net.Dial("tcp", a.LocalAddr().String()) // sends no handshake
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	waitUntil(t, 2*time.Second, func() bool { return len(a.handshake) == 1 })
	start := time.Now()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Close took %s: it waited out a handshake in flight", took)
	}
}

// TestSlowFrameIsNotCutByTheIdleTimeout: the read idle timeout measures
// silence, not frame size. A frame whose bytes arrive steadily but take longer
// than the timeout in all — a large AppendEntries or snapshot chunk over a
// busy link — is received; before, one deadline covered the whole frame and
// the connection was torn down mid-frame, every time.
func TestSlowFrameIsNotCutByTheIdleTimeout(t *testing.T) {
	const idle = 200 * time.Millisecond
	b, err := NewTCPTransport(Config{NodeID: "b", ListenAddr: "127.0.0.1:0", Peers: map[NodeID]string{"a": "127.0.0.1:1"}, ReadIdleTimeout: idle})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	nc, err := net.Dial("tcp", b.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err := writeHandshake(nc, hello{id: "a"}); err != nil {
		t.Fatal(err)
	}
	if status, _, err := readReply(nc); err != nil || status != statusAccepted {
		t.Fatalf("handshake: status %d, %v", status, err)
	}
	payload := make([]byte, 64<<10)
	for i := range payload {
		payload[i] = byte(i)
	}
	var frame strings.Builder
	if _, err := writeFrame(&frame, nil, MsgAppendEntries, payload); err != nil {
		t.Fatal(err)
	}
	wire := []byte(frame.String())
	const chunks = 8 // 8 × idle/2 = twice the idle timeout in all
	step := (len(wire) + chunks - 1) / chunks
	for off := 0; off < len(wire); off += step {
		if _, err := nc.Write(wire[off:min(off+step, len(wire))]); err != nil {
			t.Fatalf("the connection was torn down mid-frame: %v", err)
		}
		time.Sleep(idle / 2)
	}
	env := recv(t, b, 3*time.Second)
	if env.Kind != MsgAppendEntries || env.Peer != "a" || string(env.Payload) != string(payload) {
		t.Fatalf("received %v from %s (%d bytes), want the slow frame", env.Kind, env.Peer, len(env.Payload))
	}
}

// throttled reads at most 64 KiB per Read, pausing before each.
type throttled struct{ r net.Conn }

func (t throttled) Read(p []byte) (int, error) {
	time.Sleep(5 * time.Millisecond)
	return t.r.Read(p[:min(len(p), 64<<10)])
}

// TestCallerDeadlineDoesNotCutAStartedFrame: a caller's context bounds only
// the wait for the connection's writer. A frame already started is bounded by
// the write timeout alone: cutting it at the caller's deadline would leave part
// of it on the wire and — since a failed write closes the connection — tear
// down the connection every group of the node shares, for one request.
func TestCallerDeadlineDoesNotCutAStartedFrame(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan []byte, 1)
	held := make(chan net.Conn, 1) // closed when the test ends, not when the frame is read
	defer func() {
		select {
		case c := <-held:
			_ = c.Close()
		default:
		}
	}()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		held <- c
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetReadBuffer(256 << 10)
		}
		if _, err := readHandshake(c); err != nil {
			return
		}
		_ = writeReply(c, statusAccepted, hello{id: "b"})
		_, payload, err := readFrame(throttled{c}, make([]byte, record.HeaderSize))
		if err == nil {
			got <- payload
		}
		close(got)
	}()
	a, err := NewTCPTransport(Config{NodeID: "a", ListenAddr: "127.0.0.1:0", Peers: map[NodeID]string{"b": ln.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	waitConnected(t, a, "b", 3*time.Second)
	payload := make([]byte, 8<<20) // ~0.65 s to drain at the reader's pace
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := a.Send(ctx, "b", MsgAppendEntries, payload); err != nil {
		t.Fatalf("Send with a short caller deadline: %v; the started frame was cut", err)
	}
	if took := time.Since(start); took < 100*time.Millisecond {
		t.Fatalf("the premise failed: the frame was written in %s, within the caller's deadline", took)
	}
	select {
	case p := <-got:
		if string(p) != string(payload) {
			t.Fatalf("the peer read %d bytes, not the frame sent", len(p))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the peer never read the frame")
	}
	if !a.hasConn("b") {
		t.Fatal("the connection was torn down")
	}
}

// TestAnUnauthenticatedIDCannotForgeLogLines: whoever reaches the listen port
// announces an id of its choosing, and a refused handshake is logged with it.
// Logged unquoted, a newline in it forged a whole event line of dkvd's
// machine-readable output.
func TestAnUnauthenticatedIDCannotForgeLogLines(t *testing.T) {
	lb := &logSink{}
	b, err := NewTCPTransport(Config{NodeID: "b", ListenAddr: "127.0.0.1:0", ClusterID: "prod", SettingsDigest: []byte{1}, Logf: lb.logf})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	nc, err := net.Dial("tcp", b.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err := writeHandshake(nc, hello{id: "x\nevent=raft_leader node=b term=999 group=0", cluster: "whatever"}); err != nil {
		t.Fatal(err)
	}
	_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if status, _, err := readReply(nc); err != nil || status == statusAccepted {
		t.Fatalf("premise: the forged hello was not refused (status %d, %v)", status, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(lb.String(), "event=handshake_failed dir=inbound") {
		if time.Now().After(deadline) {
			t.Fatalf("premise: the refusal was not logged:\n%s", lb.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, line := range strings.Split(lb.String(), "\n") {
		if strings.HasPrefix(line, "event=raft_leader") {
			t.Fatalf("a forged log line from an unauthenticated hello: %q", line)
		}
	}
}
