package kv_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/kv/workload"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raftnode"
)

// serveAll exposes every node's Server on a TCP port and returns wire Clients
// to them, so the wire tier is exercised end to end against real nodes.
func serveAll(t *testing.T, c *cluster) (map[raftnode.NodeID]*kv.Client, []workload.Endpoint) {
	t.Helper()
	clients := map[raftnode.NodeID]*kv.Client{}
	var eps []workload.Endpoint
	for _, id := range c.ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv, _ := c.eps[id].current()
		go kv.Serve(c.ctx, ln, srv, nil)
		cl, err := kv.Dial(string(id), ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cl.Close(); ln.Close() })
		clients[id] = cl
		eps = append(eps, cl)
	}
	return clients, eps
}

// TestWireClientAgainstRealNodes: every status the protocol has, produced by real
// nodes: ok, notfound, notleader with a hint, invalid; values round-trip
// including the empty value; one connection serves many requests.
func TestWireClientAgainstRealNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, false)
	l := c.waitLeader(0, 10*time.Second)
	clients, _ := serveAll(t, c)
	lc := clients[l]
	if _, err := lc.Put(ctx, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, m, err := lc.Get(ctx, []byte("k"))
	if err != nil || string(v) != "v1" || m.Node != string(l) || m.Index == 0 || m.Term == 0 {
		t.Fatalf("get: %q %+v %v", v, m, err)
	}
	if _, err := lc.Put(ctx, []byte("k"), []byte{}); err != nil {
		t.Fatal(err)
	}
	if v, _, err := lc.Get(ctx, []byte("k")); err != nil || v == nil || len(v) != 0 {
		t.Fatalf("empty value must be present and empty: %q %v (nil=%v)", v, err, v == nil)
	}
	if _, err := lc.Delete(ctx, []byte("k")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := lc.Get(ctx, []byte("k")); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("get after delete: %v, want ErrNotFound", err)
	}
	if _, err := lc.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("second delete must succeed: %v", err)
	}
	if _, err := lc.Put(ctx, nil, []byte("v")); !errors.Is(err, kv.ErrInvalid) {
		t.Fatalf("empty key: %v, want ErrInvalid", err)
	}
	for _, id := range c.ids {
		if id == l {
			continue
		}
		var nl *kv.NotLeaderError
		_, err := clients[id].Put(ctx, []byte("k"), []byte("x"))
		if !errors.As(err, &nl) || nl.Leader != string(l) || nl.Node != string(id) {
			t.Fatalf("follower %s: %v, want not-leader with hint %s", id, err, l)
		}
		if _, _, err := clients[id].Get(ctx, []byte("k")); !errors.As(err, &nl) {
			t.Fatalf("follower read: %v", err)
		}
	}
}

// TestWireWorkloadIsLinearizable: the full concurrent workload through the wire
// protocol, one connection per client.
func TestWireWorkloadIsLinearizable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, false)
	c.waitLeader(0, 10*time.Second)
	// One Client per workload client: Clients are sequential-use only.
	var eps []workload.Endpoint
	for _, id := range c.ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv, _ := c.eps[id].current()
		go kv.Serve(ctx, ln, srv, nil)
		t.Cleanup(func() { ln.Close() })
		eps = append(eps, &dialing{node: string(id), addr: ln.Addr().String(), t: t})
	}
	rec := lincheck.NewRecorder()
	st := workload.Run(ctx, eps, workload.Options{Clients: 8, OpsPerClient: 30, Keys: 3, Timeout: 5 * time.Second, Seed: 3, GetPct: 40, DeletePct: 10}, rec)
	check(t, rec, st)
}

// dialing is an endpoint that gives each calling goroutine its own connection.
type dialing struct {
	node, addr string
	t          *testing.T
}

func (d *dialing) Name() string { return d.node }
func (d *dialing) client() (*kv.Client, error) {
	return kv.Dial(d.node, d.addr)
}
func (d *dialing) Put(ctx context.Context, key, value []byte) (kv.Meta, error) {
	c, err := d.client()
	if err != nil {
		return kv.Meta{Node: d.node}, err
	}
	defer c.Close()
	return c.Put(ctx, key, value)
}
func (d *dialing) Get(ctx context.Context, key []byte) ([]byte, kv.Meta, error) {
	c, err := d.client()
	if err != nil {
		return nil, kv.Meta{Node: d.node}, err
	}
	defer c.Close()
	return c.Get(ctx, key)
}
func (d *dialing) Delete(ctx context.Context, key []byte) (kv.Meta, error) {
	c, err := d.client()
	if err != nil {
		return kv.Meta{Node: d.node}, err
	}
	defer c.Close()
	return c.Delete(ctx, key)
}

// TestWireServerDisconnectsHostileClients: garbage, an oversized frame, and a
// frame of the wrong kind each close the connection; the server keeps serving
// other clients.
func TestWireServerDisconnectsHostileClients(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 1, false)
	l := c.waitLeader(0, 10*time.Second)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := c.eps[l].current()
	go kv.Serve(ctx, ln, srv, nil)
	defer ln.Close()
	hostile := [][]byte{
		[]byte("GET / HTTP/1.1\r\n\r\n"),
		func() []byte { // declares a 100 MiB frame
			b := make([]byte, 9)
			binary.LittleEndian.PutUint32(b[4:8], 100<<20)
			return b
		}(),
	}
	for i, payload := range hostile {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		conn.Write(payload)
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		var ne net.Error
		switch {
		case err == nil:
			t.Fatalf("hostile input %d: the server answered instead of closing", i)
		case errors.Is(err, io.EOF):
		case errors.As(err, &ne) && !ne.Timeout(): // connection reset: also closed
		default:
			t.Fatalf("hostile input %d: want the connection closed, got %v", i, err)
		}
		conn.Close()
	}
	// A well-behaved client is still served.
	cl, err := kv.Dial(string(l), ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := cl.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put after hostile clients: %v", err)
	}
}

// TestWireClientReportsUnknownWhenTheConnectionDies: a node that stops mid-request
// leaves the client with ErrUnknown, never a fabricated answer; reconnecting to a
// dead address is ErrUnavailable (definite: nothing was sent).
func TestWireClientReportsUnknownWhenTheConnectionDies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, false)
	l := c.waitLeader(0, 10*time.Second)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sctx, scancel := context.WithCancel(ctx)
	srv, _ := c.eps[l].current()
	go kv.Serve(sctx, ln, srv, nil)
	cl, err := kv.Dial(string(l), ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := cl.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	scancel() // the server goes away, closing every connection
	time.Sleep(50 * time.Millisecond)
	_, err = cl.Put(ctx, []byte("k"), []byte("w"))
	if !errors.Is(err, kv.ErrUnknown) && !errors.Is(err, kv.ErrUnavailable) {
		t.Fatalf("after the server died: %v, want ErrUnknown or ErrUnavailable", err)
	}
	if _, err := cl.Put(ctx, []byte("k"), []byte("w")); !errors.Is(err, kv.ErrUnavailable) {
		t.Fatalf("reconnect to a dead address: %v, want ErrUnavailable", err)
	}
}
