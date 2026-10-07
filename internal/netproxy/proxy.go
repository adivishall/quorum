// Package netproxy is a TCP forwarder placed on one node-to-node link, so a
// test or an experiment can partition real dkvd processes at the network level
// without root, OS firewall rules, or any change to dkvd.
//
// Every pair of nodes has exactly one TCP connection, dialed by the node with
// the smaller id (ADR-014). Pointing the dialer's peer address at a proxy puts
// that whole link — both directions — under the caller's control.
//
// Cut closes every connection through the proxy (both sides see the connection
// die) and refuses new ones by closing them on accept, until Heal. That models a
// partition that resets connections. A partition that silently black-holes
// packets is not modelled (it would need kernel-level packet filtering), and
// neither is a one-way partition: the link is one bidirectional connection.
package netproxy

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// Proxy forwards connections accepted on its own loopback address to a target.
type Proxy struct {
	target string
	ln     net.Listener

	mu     sync.Mutex
	cut    bool
	closed bool
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup

	logMu sync.Mutex
	log   strings.Builder
}

// Start listens on a fresh loopback port and forwards every connection to
// target until Close.
func Start(target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("netproxy: listen: %w", err)
	}
	p := &Proxy{target: target, ln: ln, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.acceptLoop()
	return p, nil
}

// Addr is the address to dial instead of the target.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Target is the address the proxy forwards to.
func (p *Proxy) Target() string { return p.target }

// IsCut reports whether the link is cut.
func (p *Proxy) IsCut() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cut
}

// logf records a timestamped line of the proxy's own activity, so a failing
// run shows what the proxy actually did with each connection rather than
// leaving it to inference.
func (p *Proxy) logf(format string, args ...any) {
	p.logMu.Lock()
	fmt.Fprintf(&p.log, time.Now().Format("15:04:05.000000")+" "+format+"\n", args...)
	p.logMu.Unlock()
}

// Log returns the proxy's activity log.
func (p *Proxy) Log() string {
	p.logMu.Lock()
	defer p.logMu.Unlock()
	return p.log.String()
}

func (p *Proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.cut || p.closed {
			p.mu.Unlock()
			p.logf("accept client=%s refused (cut)", c.RemoteAddr())
			_ = c.Close()
			continue
		}
		p.conns[c] = struct{}{}
		p.wg.Add(1)
		p.mu.Unlock()
		p.logf("accept client=%s", c.RemoteAddr())
		go p.serve(c)
	}
}

func (p *Proxy) serve(c net.Conn) {
	defer p.wg.Done()
	start := time.Now()
	up, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		p.logf("updial client=%s err=%v took=%s", c.RemoteAddr(), err, time.Since(start))
		p.forget(c)
		_ = c.Close()
		return
	}
	p.logf("updial client=%s up_local=%s up_remote=%s took=%s",
		c.RemoteAddr(), up.LocalAddr(), up.RemoteAddr(), time.Since(start))
	p.mu.Lock()
	if p.cut || p.closed {
		p.mu.Unlock()
		p.logf("drop client=%s (cut after updial)", c.RemoteAddr())
		p.forget(c)
		_ = c.Close()
		_ = up.Close()
		return
	}
	p.conns[up] = struct{}{}
	p.mu.Unlock()

	var pipes sync.WaitGroup
	pipes.Add(2)
	go func() {
		defer pipes.Done()
		n, err := io.Copy(up, c)
		p.logf("pipe client->up done client=%s bytes=%d err=%v", c.RemoteAddr(), n, err)
		_ = up.Close()
		_ = c.Close()
	}()
	go func() {
		defer pipes.Done()
		n, err := io.Copy(c, up)
		p.logf("pipe up->client done client=%s bytes=%d err=%v", c.RemoteAddr(), n, err)
		_ = c.Close()
		_ = up.Close()
	}()
	pipes.Wait()
	p.forget(c)
	p.forget(up)
}

func (p *Proxy) forget(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

// Cut severs the link: every live connection is closed and new ones are refused.
func (p *Proxy) Cut() {
	p.logf("CUT")
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = true
	for c := range p.conns {
		_ = c.Close()
	}
}

// Heal lets new connections through again (the dialer's retry loop reconnects).
func (p *Proxy) Heal() {
	p.logf("HEAL")
	p.mu.Lock()
	p.cut = false
	p.mu.Unlock()
}

// Close stops the proxy and waits for its goroutines. It is idempotent.
func (p *Proxy) Close() {
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
