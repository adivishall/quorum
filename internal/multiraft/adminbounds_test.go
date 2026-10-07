package multiraft

import (
	"context"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// The admin port's bounds (audit M1, L): a capped number of connections, an
// accept loop that survives errors, a clamped request timeout, and log lines
// a client cannot forge.

type adminLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *adminLog) logf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.b, f+"\n", a...)
}

func (l *adminLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// serveAdminOn serves h's admin protocol on ln, logging to log, until the test
// ends.
func serveAdminOn(t *testing.T, ln net.Listener, h *Host, log *adminLog) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ServeAdmin(ctx, ln, h, log.logf)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return ln.Addr().String()
}

func adminHost(t *testing.T) *Host {
	t.Helper()
	c := newHostCluster(t, "a")
	c.startHost("a", false)
	return c.hosts["a"]
}

// TestAdminCapsItsConnections: beyond MaxAdminConns a new connection is
// closed at once; a slot freed serves the next.
func TestAdminCapsItsConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := serveAdminOn(t, ln, adminHost(t), &adminLog{})
	var held []net.Conn
	for i := 0; i < MaxAdminConns; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		held = append(held, c)
	}
	// Each held connection is served (a status answered on the last one
	// proves every slot was taken by a served connection, not still queued).
	if _, err := held[len(held)-1].Write([]byte(`{"op":"status"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	_ = held[len(held)-1].SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := held[len(held)-1].Read(make([]byte, 1)); err != nil {
		t.Fatalf("a connection within the cap was not served: %v", err)
	}
	over, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer over.Close()
	_ = over.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, err := over.Read(make([]byte, 1)); n != 0 || err == nil || isTimeoutErr(err) {
		t.Fatalf("a connection beyond the cap: read %d, %v; want it closed at once", n, err)
	}
	_ = held[0].Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := AdminCall(ctx, addr, AdminRequest{Op: "status"})
		cancel()
		if err == nil && resp.OK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no slot was freed by a closed connection: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func isTimeoutErr(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}

type flakyAdminListener struct {
	net.Listener
	fails atomic.Int64
}

func (l *flakyAdminListener) Accept() (net.Conn, error) {
	if l.fails.Add(-1) >= 0 {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
	}
	return l.Listener.Accept()
}

// TestAdminSurvivesAcceptErrors: an Accept error is logged and retried — the
// loop used to return for good, leaving the listener open and unanswered.
func TestAdminSurvivesAcceptErrors(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fl := &flakyAdminListener{Listener: ln}
	fl.fails.Store(3)
	log := &adminLog{}
	addr := serveAdminOn(t, fl, adminHost(t), log)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if resp, err := AdminCall(ctx, addr, AdminRequest{Op: "status"}); err != nil || !resp.OK {
		t.Fatalf("status after accept errors: %+v, %v", resp, err)
	}
	if n := strings.Count(log.String(), "event=admin_accept_failed"); n != 3 {
		t.Fatalf("%d accept failures logged, want 3:\n%s", n, log.String())
	}
}

// TestAdminTimeoutIsClamped: a request's timeout_ms is bounded; a huge value
// (which also overflowed into a negative duration) holds a connection no
// longer than MaxAdminTimeout.
func TestAdminTimeoutIsClamped(t *testing.T) {
	for ms, want := range map[int]time.Duration{
		0: DefaultAdminTimeout, -5: DefaultAdminTimeout, 1500: 1500 * time.Millisecond,
		int(MaxAdminTimeout / time.Millisecond):   MaxAdminTimeout,
		int(MaxAdminTimeout/time.Millisecond) + 1: MaxAdminTimeout,
		math.MaxInt: MaxAdminTimeout, math.MaxInt / int(time.Millisecond) * 10: MaxAdminTimeout,
	} {
		if got := adminTimeout(ms); got != want {
			t.Errorf("adminTimeout(%d) = %s, want %s", ms, got, want)
		}
	}
}

// TestAdminLogLinesCannotBeForged: the op and id in an admin log line are
// the client's; a newline in either stays inside the quoted field instead of
// starting a line of its own that tests and tools would parse as an event.
func TestAdminLogLinesCannotBeForged(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	log := &adminLog{}
	addr := serveAdminOn(t, ln, adminHost(t), log)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := AdminCall(ctx, addr, AdminRequest{Op: "promote\nevent=raft_leader node=x term=99", Group: 1, ID: "n9\nevent=forged"}); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(log.String(), "\n") {
		if strings.HasPrefix(line, "event=raft_leader") || strings.HasPrefix(line, "event=forged") {
			t.Fatalf("a client forged a log line: %q\nlog:\n%s", line, log.String())
		}
	}
	if !strings.Contains(log.String(), `op="promote\nevent=raft_leader node=x term=99"`) {
		t.Fatalf("the op was not logged quoted:\n%s", log.String())
	}
}

// TestAdminDropsAnIdleConnection: a connection that sends no request within
// the idle timeout is closed, freeing its slot — so idle connections cannot
// hold the port's few slots for ever.
func TestAdminDropsAnIdleConnection(t *testing.T) {
	prev := adminIdle
	t.Cleanup(func() { adminIdle = prev }) // after the server stops: cleanups run last-registered first
	adminIdle = 150 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := serveAdminOn(t, ln, adminHost(t), &adminLog{})
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := c.Read(make([]byte, 1)); n != 0 || err == nil || isTimeoutErr(err) {
		t.Fatalf("an idle admin connection: read %d, %v; want it closed", n, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the idle connection was closed after %s", took)
	}
}

// TestAMemberIDCannotForgeLogLines: a member's id and address enter the
// group's replicated configuration, and every node then logs them as fields
// of its own event lines — quoting the admin's line did not cover those. A
// member id or address with whitespace or a control character is refused at
// the admin port; before, add-learner accepted one and the leader forged an
// event line with every heartbeat to it.
func TestAMemberIDCannotForgeLogLines(t *testing.T) {
	c := newHostCluster(t, "a")
	h := c.startHost("a", false)
	c.create(1, "a")
	c.leader(1, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	forged := "x\nevent=raft_leader node=forged term=999 group=1"
	for _, req := range []AdminRequest{
		{Op: "add-learner", Group: 1, ID: forged, Addr: "127.0.0.1:1"},
		{Op: "add-learner", Group: 1, ID: "x", Addr: "127.0.0.1:1\nevent=raft_leader node=forged"},
		{Op: "create-group", Group: 2, Voters: []Member{{ID: forged, Addr: "127.0.0.1:1"}}},
	} {
		if resp := h.Admin(ctx, req); resp.OK || !strings.Contains(resp.Error, "whitespace or control characters") {
			t.Fatalf("%s with a forging member was not refused: %+v", req.Op, resp)
		}
	}
	if conf := h.Group(1).Node.Status().Conf; len(conf.Members()) != 1 {
		t.Fatalf("the refused member entered the configuration: %s", conf)
	}
	time.Sleep(100 * time.Millisecond)
	for _, line := range strings.Split(c.log("a"), "\n") {
		if strings.HasPrefix(line, "event=raft_leader node=forged") {
			t.Fatalf("a member id forged a log line: %q", line)
		}
	}
}
