package netproxy

import (
	"io"
	"net"
	"testing"
	"time"
)

// echoServer accepts connections and echoes what it reads, until the test ends.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	return ln.Addr().String()
}

func roundTrip(c net.Conn) error {
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte("ping")); err != nil {
		return err
	}
	buf := make([]byte, 4)
	_, err := io.ReadFull(c, buf)
	return err
}

// TestProxyForwardsCutsAndHeals pins the proxy's own behaviour, so the
// partition tests and experiments built on it prove what they claim: bytes flow
// both ways, Cut kills a live connection and refuses new ones, and Heal
// restores service.
func TestProxyForwardsCutsAndHeals(t *testing.T) {
	p, err := Start(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c1, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	if err := roundTrip(c1); err != nil {
		t.Fatalf("healthy proxy did not forward: %v", err)
	}
	p.Cut()
	if !p.IsCut() {
		t.Fatal("IsCut is false after Cut")
	}
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
	if p.IsCut() {
		t.Fatal("IsCut is true after Heal")
	}
	c3, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	if err := roundTrip(c3); err != nil {
		t.Fatalf("healed proxy did not forward: %v", err)
	}
}

// TestCloseEndsEveryConnection: Close tears down live connections and the
// listener, waits for its goroutines, and may be called again. A test or an
// experiment that fails midway relies on it to leave nothing running.
func TestCloseEndsEveryConnection(t *testing.T) {
	p, err := Start(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Close()
	if err := roundTrip(c); err == nil {
		t.Fatal("a connection survived Close")
	}
	if c2, err := net.DialTimeout("tcp", p.Addr(), time.Second); err == nil {
		_ = c2.Close()
		t.Fatal("a closed proxy still accepts")
	}
	if p.Target() == "" || p.Log() == "" {
		t.Fatalf("target %q, log %q", p.Target(), p.Log())
	}
}
