package integration

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// tcpProxy is a test-owned TCP forwarder placed on one node-to-node link, so a
// test can partition real dkvd processes at the network level without root, OS
// firewall rules, or any change to dkvd. Every pair of nodes has exactly one TCP
// connection, dialed by the lower id (ADR-014); pointing the dialer's peer address
// at a proxy puts that whole link under the test's control.
//
// Cut closes every connection through the proxy (both sides see the connection
// die) and refuses new ones by closing them on accept, until Heal. That models a
// partition that resets connections; a partition that silently black-holes
// packets is not modelled (it would need kernel-level packet filtering).
type tcpProxy struct {
	target string
	ln     net.Listener

	mu     sync.Mutex
	cut    bool
	closed bool
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup
}

func startProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &tcpProxy{target: target, ln: ln, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.acceptLoop()
	t.Cleanup(p.Close)
	return p
}

func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) acceptLoop() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.cut || p.closed {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		p.conns[c] = struct{}{}
		p.wg.Add(1)
		p.mu.Unlock()
		go p.serve(c)
	}
}

func (p *tcpProxy) serve(c net.Conn) {
	defer p.wg.Done()
	up, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		p.forget(c)
		_ = c.Close()
		return
	}
	p.mu.Lock()
	if p.cut || p.closed {
		p.mu.Unlock()
		p.forget(c)
		_ = c.Close()
		_ = up.Close()
		return
	}
	p.conns[up] = struct{}{}
	p.mu.Unlock()

	var pipes sync.WaitGroup
	pipes.Add(2)
	go func() { defer pipes.Done(); _, _ = io.Copy(up, c); _ = up.Close(); _ = c.Close() }()
	go func() { defer pipes.Done(); _, _ = io.Copy(c, up); _ = c.Close(); _ = up.Close() }()
	pipes.Wait()
	p.forget(c)
	p.forget(up)
}

func (p *tcpProxy) forget(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

// Cut severs the link: every live connection is closed and new ones are refused.
func (p *tcpProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = true
	for c := range p.conns {
		_ = c.Close()
	}
}

// Heal lets new connections through again (the dialer's retry loop reconnects).
func (p *tcpProxy) Heal() {
	p.mu.Lock()
	p.cut = false
	p.mu.Unlock()
}

// Close stops the proxy and waits for its goroutines.
func (p *tcpProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	_ = p.ln.Close()
	p.wg.Wait()
}

// TestProxyForwardsCutsAndHeals pins the proxy's own behaviour, so the partition
// tests built on it prove what they claim: bytes flow both ways, Cut kills a live
// connection and refuses new ones, and Heal restores service.
func TestProxyForwardsCutsAndHeals(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	p := startProxy(t, echo.Addr().String())

	roundTrip := func(c net.Conn) error {
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := c.Write([]byte("ping")); err != nil {
			return err
		}
		buf := make([]byte, 4)
		_, err := io.ReadFull(c, buf)
		return err
	}

	c1, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(c1); err != nil {
		t.Fatalf("healthy proxy did not forward: %v", err)
	}
	p.Cut()
	if err := roundTrip(c1); err == nil {
		t.Fatal("a live connection survived Cut")
	}
	c2, err := net.Dial("tcp", p.Addr())
	if err == nil {
		if err := roundTrip(c2); err == nil {
			t.Fatal("a new connection got through a cut proxy")
		}
		_ = c2.Close()
	}
	p.Heal()
	c3, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	if err := roundTrip(c3); err != nil {
		t.Fatalf("healed proxy did not forward: %v", err)
	}
}
