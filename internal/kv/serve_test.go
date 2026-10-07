package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/record"
)

// The client port's bounds (audit M1): a capped number of connections, a
// frame that must complete once begun, a response that must be read, an
// accept loop that survives errors, and frame buffers that grow with the
// bytes that arrive.

// okDoer answers every request OK, concurrently.
type okDoer struct{ value []byte }

func (okDoer) Name() string { return "ok" }
func (d okDoer) Do(context.Context, Request) (Response, error) {
	return Response{Status: StatusOK, Node: "ok", Value: d.value}, nil
}

type syncLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *syncLog) logf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.b, f+"\n", a...)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// serveOn starts ServeWith on a fresh listener (or ln, if given) and returns
// its address.
func serveOn(t *testing.T, ln net.Listener, d Doer, cfg ServeConfig, logf func(string, ...any)) string {
	t.Helper()
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ServeWith(ctx, ln, d, logf, cfg)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return ln.Addr().String()
}

// roundTrip sends one GET on c and reads its response.
func roundTrip(c net.Conn) (Response, error) {
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeFrame(c, kindRequest, encodeRequest(Request{Op: ReqGet, Key: []byte("k")})); err != nil {
		return Response{}, err
	}
	payload, err := readFrame(c, kindResponse, maxResponseFrame)
	if err != nil {
		return Response{}, err
	}
	return decodeResponse(payload)
}

// closedSoon reports whether the server closes c within d, sending nothing.
func closedSoon(c net.Conn, d time.Duration) bool {
	_ = c.SetReadDeadline(time.Now().Add(d))
	n, err := c.Read(make([]byte, 1))
	return n == 0 && err != nil && !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err)
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// TestServeCapsItsConnections: beyond MaxConns a new connection is closed at
// once; one of the served connections closing frees a slot.
func TestServeCapsItsConnections(t *testing.T) {
	log := &syncLog{}
	addr := serveOn(t, nil, okDoer{}, ServeConfig{MaxConns: 2}, log.logf)
	var held []net.Conn
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if resp, err := roundTrip(c); err != nil || resp.Status != StatusOK {
			t.Fatalf("connection %d within the cap: %+v, %v", i, resp, err)
		}
		held = append(held, c)
	}
	over, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer over.Close()
	if !closedSoon(over, 2*time.Second) {
		t.Fatal("a connection beyond the cap was kept")
	}
	if !strings.Contains(log.String(), "event=kv_conn_refused") {
		t.Fatalf("the refusal was not logged:\n%s", log.String())
	}
	_ = held[0].Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := roundTrip(c)
		_ = c.Close()
		if err == nil && resp.Status == StatusOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no slot was freed by a closed connection: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestServeDropsAStalledFrameButKeepsAnIdleConnection: a client that begins a
// frame and stalls is disconnected after FrameTimeout; one that sends nothing
// at all is kept, and served when it finally sends a request.
func TestServeDropsAStalledFrameButKeepsAnIdleConnection(t *testing.T) {
	const timeout = 200 * time.Millisecond
	addr := serveOn(t, nil, okDoer{}, ServeConfig{FrameTimeout: timeout}, nil)
	stalled, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	frame, _ := record.Encode(nil, kindRequest, encodeRequest(Request{Op: ReqGet, Key: []byte("k")}))
	if _, err := stalled.Write(frame[:3]); err != nil {
		t.Fatal(err)
	}
	if !closedSoon(stalled, 10*timeout) {
		t.Fatal("a connection whose frame stalled was kept")
	}

	idle, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	time.Sleep(3 * timeout)
	if resp, err := roundTrip(idle); err != nil || resp.Status != StatusOK {
		t.Fatalf("an idle connection's first request after %s: %+v, %v; idle connections must not be timed out", 3*timeout, resp, err)
	}
}

// TestServeDropsAClientThatDoesNotRead: a client that sends requests but never
// reads its responses is disconnected after WriteTimeout, freeing its slot.
func TestServeDropsAClientThatDoesNotRead(t *testing.T) {
	addr := serveOn(t, nil, okDoer{value: bytes.Repeat([]byte{'v'}, 512<<10)},
		ServeConfig{MaxConns: 1, WriteTimeout: 200 * time.Millisecond}, nil)
	deaf, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer deaf.Close()
	if tc, ok := deaf.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096)
	}
	frame, _ := record.Encode(nil, kindRequest, encodeRequest(Request{Op: ReqGet, Key: []byte("k")}))
	for i := 0; i < 64; i++ { // 32 MiB of responses: far beyond any socket buffer
		if _, err := deaf.Write(frame); err != nil {
			break
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := roundTrip(c)
		_ = c.Close()
		if err == nil && resp.Status == StatusOK {
			return // the deaf client's slot was freed
		}
		if time.Now().After(deadline) {
			t.Fatal("a client that never reads still holds the only slot")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// failingListener fails its first Accepts with a temporary error.
type failingListener struct {
	net.Listener
	fails atomic.Int64
}

func (l *failingListener) Accept() (net.Conn, error) {
	if l.fails.Add(-1) >= 0 {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
	}
	return l.Listener.Accept()
}

// TestServeSurvivesAcceptErrors: an Accept error is logged and retried, not the
// end of the server (before, the loop returned while the listener stayed open:
// clients connected into its backlog and were never answered).
func TestServeSurvivesAcceptErrors(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fl := &failingListener{Listener: ln}
	fl.fails.Store(3)
	log := &syncLog{}
	addr := serveOn(t, fl, okDoer{}, ServeConfig{}, log.logf)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if resp, err := roundTrip(c); err != nil || resp.Status != StatusOK {
		t.Fatalf("after accept errors: %+v, %v", resp, err)
	}
	if n := strings.Count(log.String(), "event=kv_accept_failed"); n != 3 {
		t.Fatalf("%d accept failures logged, want 3:\n%s", n, log.String())
	}
}

// TestReadFrameGrowsWithTheBytesThatArrive: a header declaring the largest
// request frame, followed by a few bytes and the end of the stream, costs a
// read chunk — not the declared length, as when the buffer was sized from the
// header alone.
func TestReadFrameGrowsWithTheBytesThatArrive(t *testing.T) {
	hdr := make([]byte, record.HeaderSize)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(MaxRequestFrame))
	hdr[8] = byte(kindRequest)
	stream := append(hdr, bytes.Repeat([]byte{'x'}, 100)...)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const reads = 16
	for i := 0; i < reads; i++ {
		if _, err := readFrame(bytes.NewReader(stream), kindRequest, MaxRequestFrame); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("a truncated frame: %v, want io.ErrUnexpectedEOF", err)
		}
	}
	runtime.ReadMemStats(&after)
	if per := (after.TotalAlloc - before.TotalAlloc) / reads; per > 4*readChunk {
		t.Fatalf("a truncated frame declaring %d bytes allocated %d bytes; the buffer must grow with the bytes that arrive", MaxRequestFrame, per)
	}
	// A whole frame of the largest size still reads intact.
	payload := bytes.Repeat([]byte{'y'}, MaxRequestFrame-record.HeaderSize)
	whole, err := record.Encode(nil, kindRequest, payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readFrame(bytes.NewReader(whole), kindRequest, MaxRequestFrame)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("a whole large frame: %d bytes, %v", len(got), err)
	}
}
