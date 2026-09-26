package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/record"
)

// The Phase 12 client wire protocol: a request/response pair per operation over
// a plain TCP connection, each message one record in the shared checksummed
// framing (docs/DESIGN.md §2). It exists so real dkvd processes can be driven
// by test clients that record histories; it is not the Phase 15 HTTP API and
// carries no request ids (Phase 13). Requests on one connection are answered in
// order; a client wanting concurrency opens more connections.
//
//	request  (kind 1): op u8 (1 put, 2 get, 3 delete) | key(len+bytes) | value(len+bytes, put only)
//	response (kind 2): status u8 | term uvarint | index uvarint | payload(len+bytes)
//	  status: 0 ok (payload = value for get), 1 notfound, 2 notleader (payload =
//	  leader hint), 3 lost, 4 invalid (payload = message), 5 error (payload =
//	  message; the outcome is unknown to the client)
const (
	kindRequest  record.Kind = 1
	kindResponse record.Kind = 2

	wireGet = 2 // OpPut=1 and OpDelete=2 are Command ops; on the wire get is 2 and delete 3
	wirePut = 1
	wireDel = 3

	statusOK        = 0
	statusNotFound  = 1
	statusNotLeader = 2
	statusLost      = 3
	statusInvalid   = 4
	statusError     = 5

	// MaxRequestFrame bounds a request (a maximal key and value plus framing).
	MaxRequestFrame = MaxKeyLen + MaxValueLen + 64
	// serverRequestTimeout caps how long the server works on one request whose
	// client is still connected; a client normally gives up first.
	serverRequestTimeout = 60 * time.Second
)

// ErrProtocol means the peer sent something that is not this protocol.
var ErrProtocol = errors.New("kv: protocol error")

type request struct {
	op    byte
	key   []byte
	value []byte
}

type response struct {
	status  byte
	term    uint64
	index   uint64
	payload []byte
}

func (r request) encode() []byte {
	b := []byte{r.op}
	b = binary.AppendUvarint(b, uint64(len(r.key)))
	b = append(b, r.key...)
	if r.op == wirePut {
		b = binary.AppendUvarint(b, uint64(len(r.value)))
		b = append(b, r.value...)
	}
	return b
}

func decodeRequest(b []byte) (request, error) {
	if len(b) < 1 {
		return request{}, ErrProtocol
	}
	r := request{op: b[0]}
	if r.op != wirePut && r.op != wireGet && r.op != wireDel {
		return request{}, fmt.Errorf("%w: op %d", ErrProtocol, r.op)
	}
	i := 1
	key, n, err := readBytes(b[i:], MaxKeyLen)
	if err != nil {
		return request{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	r.key = key
	i += n
	if r.op == wirePut {
		v, n, err := readBytes(b[i:], MaxValueLen)
		if err != nil {
			return request{}, fmt.Errorf("%w: %v", ErrProtocol, err)
		}
		r.value = v
		i += n
	}
	if i != len(b) {
		return request{}, fmt.Errorf("%w: trailing bytes", ErrProtocol)
	}
	return r, nil
}

func (r response) encode() []byte {
	b := []byte{r.status}
	b = binary.AppendUvarint(b, r.term)
	b = binary.AppendUvarint(b, r.index)
	b = binary.AppendUvarint(b, uint64(len(r.payload)))
	return append(b, r.payload...)
}

func decodeResponse(b []byte) (response, error) {
	if len(b) < 1 {
		return response{}, ErrProtocol
	}
	r := response{status: b[0]}
	i := 1
	var n int
	if r.term, n = uvarint(b[i:]); n <= 0 {
		return response{}, ErrProtocol
	}
	i += n
	if r.index, n = uvarint(b[i:]); n <= 0 {
		return response{}, ErrProtocol
	}
	i += n
	p, n, err := readBytes(b[i:], MaxValueLen)
	if err != nil {
		return response{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	r.payload = p
	i += n
	if i != len(b) {
		return response{}, fmt.Errorf("%w: trailing bytes", ErrProtocol)
	}
	return r, nil
}

// writeFrame writes one framed message.
func writeFrame(w io.Writer, kind record.Kind, payload []byte) error {
	buf, err := record.Encode(nil, kind, payload)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// readFrame reads one framed message of the expected kind, strictly (a torn or
// damaged frame is a protocol error, never repaired — this is a socket).
func readFrame(r io.Reader, want record.Kind) ([]byte, error) {
	hdr := make([]byte, record.HeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(hdr[4:8])
	if length > uint32(MaxRequestFrame) {
		return nil, fmt.Errorf("%w: frame of %d bytes", ErrProtocol, length)
	}
	frame := make([]byte, record.HeaderSize+int(length))
	copy(frame, hdr)
	if _, err := io.ReadFull(r, frame[record.HeaderSize:]); err != nil {
		return nil, err
	}
	rd := record.NewReader(newBytesReader(frame), "wire", int64(len(frame)))
	kind, payload, err := rd.Next()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if kind != want {
		return nil, fmt.Errorf("%w: kind %d, want %d", ErrProtocol, kind, want)
	}
	return payload, nil
}

type bytesReader struct {
	b []byte
	i int
}

func newBytesReader(b []byte) *bytesReader { return &bytesReader{b: b} }

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

// Serve answers requests on ln with srv until ctx ends or ln is closed. Each
// connection is served by one goroutine, requests strictly in order; a request
// in progress is abandoned (its outcome unknown to the client) if the connection
// drops. logf may be nil.
func Serve(ctx context.Context, ln net.Listener, srv *Server, logf func(string, ...any)) {
	var wg sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			wg.Wait()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			serveConn(ctx, c, srv, logf)
		}()
	}
}

func serveConn(ctx context.Context, c net.Conn, srv *Server, logf func(string, ...any)) {
	defer c.Close()
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Close the connection when the server stops, so a blocked read returns.
	go func() {
		<-cctx.Done()
		_ = c.Close()
	}()
	for {
		payload, err := readFrame(c, kindRequest)
		if err != nil {
			if !errors.Is(err, io.EOF) && logf != nil && cctx.Err() == nil {
				logf("event=kv_conn_closed err=%v", err)
			}
			return
		}
		req, err := decodeRequest(payload)
		if err != nil {
			if logf != nil {
				logf("event=kv_bad_request err=%v", err)
			}
			return // a peer that does not speak the protocol is disconnected
		}
		rctx, rcancel := context.WithTimeout(cctx, serverRequestTimeout)
		resp := handle(rctx, srv, req)
		rcancel()
		if err := writeFrame(c, kindResponse, resp.encode()); err != nil {
			return
		}
	}
}

// handle performs one request and classifies the result.
func handle(ctx context.Context, srv *Server, req request) response {
	var (
		m   Meta
		err error
		val []byte
	)
	switch req.op {
	case wirePut:
		m, err = srv.Put(ctx, req.key, req.value)
	case wireDel:
		m, err = srv.Delete(ctx, req.key)
	case wireGet:
		val, m, err = srv.Get(ctx, req.key)
	}
	resp := response{term: m.Term, index: m.Index}
	var nl *NotLeaderError
	switch {
	case err == nil:
		resp.status, resp.payload = statusOK, val
	case errors.Is(err, ErrNotFound):
		resp.status = statusNotFound
	case errors.As(err, &nl):
		resp.status, resp.payload = statusNotLeader, []byte(nl.Leader)
	case errors.Is(err, ErrLost):
		resp.status = statusLost
	case errors.Is(err, ErrInvalid):
		resp.status, resp.payload = statusInvalid, []byte(err.Error())
	default:
		resp.status, resp.payload = statusError, []byte(err.Error())
	}
	return resp
}

// Client speaks the wire protocol to one node. It is safe for sequential use
// only (one request at a time); open one Client per concurrent client.
type Client struct {
	node string
	addr string
	mu   sync.Mutex
	conn net.Conn
}

// Dial connects to a node's client port. node is the id the history records.
func Dial(node, addr string) (*Client, error) {
	c := &Client{node: node, addr: addr}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	conn, err := net.DialTimeout("tcp", c.addr, 3*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// Name is the node id this client talks to.
func (c *Client) Name() string { return c.node }

// Close closes the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// Put, Get and Delete mirror Server's methods with the same error taxonomy. A
// broken connection (the node died, or the deadline passed mid-request) is
// ErrUnknown: the request may have been executed.
func (c *Client) Put(ctx context.Context, key, value []byte) (Meta, error) {
	r, err := c.do(ctx, request{op: wirePut, key: key, value: value})
	return c.meta(r), err
}

func (c *Client) Delete(ctx context.Context, key []byte) (Meta, error) {
	r, err := c.do(ctx, request{op: wireDel, key: key})
	return c.meta(r), err
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, Meta, error) {
	r, err := c.do(ctx, request{op: wireGet, key: key})
	if err != nil {
		return nil, c.meta(r), err
	}
	return r.payload, c.meta(r), nil
}

func (c *Client) meta(r response) Meta { return Meta{Node: c.node, Term: r.term, Index: r.index} }

// do sends one request and decodes the response, reconnecting first if the
// previous request broke the connection (a definite "connection refused" before
// any request was sent is ErrUnavailable — nothing was executed).
func (c *Client) do(ctx context.Context, req request) (response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return response{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(serverRequestTimeout)
	}
	_ = c.conn.SetDeadline(deadline)
	if err := writeFrame(c.conn, kindRequest, req.encode()); err != nil {
		// The request may or may not have left; treat it as unknown unless we
		// know nothing was written (a stale connection detected on the first
		// write cannot be told apart, so stay conservative).
		c.dropConn()
		return response{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	payload, err := readFrame(c.conn, kindResponse)
	if err != nil {
		c.dropConn()
		return response{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	r, err := decodeResponse(payload)
	if err != nil {
		c.dropConn()
		return response{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	switch r.status {
	case statusOK:
		return r, nil
	case statusNotFound:
		return r, ErrNotFound
	case statusNotLeader:
		return r, &NotLeaderError{Node: c.node, Leader: string(r.payload)}
	case statusLost:
		return r, ErrLost
	case statusInvalid:
		return r, fmt.Errorf("%w: %s", ErrInvalid, r.payload)
	default:
		return r, fmt.Errorf("%w: %s", ErrUnknown, r.payload)
	}
}

func (c *Client) dropConn() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// ErrUnavailable means the node could not be reached before any request was
// sent: a definite no-effect (the client may try another node).
var ErrUnavailable = errors.New("kv: node unavailable")
