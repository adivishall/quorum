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

// The client wire protocol, version 2 (Phase 13, docs/API.md): a
// request/response pair per operation over a plain TCP connection, each
// message one record in the shared checksummed framing (docs/DESIGN.md §2).
// Requests on one connection are answered in order, one at a time; a client
// wanting concurrency opens more connections. The same request and response
// encodings travel inside the internal forwarding messages (transport kinds
// Forward and ForwardResponse). Version 1 (Phase 12: no identity, six
// statuses) is retired — its record kinds (1, 2) are refused like any unknown
// kind. It is a framed binary protocol, not the Phase 15 HTTP API.
//
//	request  (kind 3): op u8 | clientID | requestID | ackedBelow | timeoutMillis | key(len+bytes) | value(len+bytes, PUT only)
//	response (kind 4): status u8 | flags u8 (bit 0 duplicate) | clientID | term | index |
//	                   node | via | leader | value | message   (each len+bytes)
//
// All integers are canonical uvarints; decoding is strict and total — a frame
// that is not exactly this is a protocol error and the connection is closed.
const (
	kindRequest  record.Kind = 3
	kindResponse record.Kind = 4

	maxNodeIDLen  = 255
	maxMessageLen = 1024

	// MaxRequestFrame bounds a request (a maximal key and value plus framing).
	MaxRequestFrame = MaxKeyLen + MaxValueLen + 128
	// maxResponseFrame bounds a response (a maximal value plus the strings).
	maxResponseFrame = MaxValueLen + 3*maxNodeIDLen + maxMessageLen + 128
)

// ErrProtocol means the peer sent something that is not this protocol.
var ErrProtocol = errors.New("kv: protocol error")

func encodeRequest(r Request) []byte {
	b := []byte{byte(r.Op)}
	b = binary.AppendUvarint(b, r.ClientID)
	b = binary.AppendUvarint(b, r.RequestID)
	b = binary.AppendUvarint(b, r.AckedBelow)
	b = binary.AppendUvarint(b, uint64(r.Timeout/time.Millisecond))
	b = binary.AppendUvarint(b, uint64(len(r.Key)))
	b = append(b, r.Key...)
	if r.Op == ReqPut {
		b = binary.AppendUvarint(b, uint64(len(r.Value)))
		b = append(b, r.Value...)
	}
	return b
}

// decodeRequest parses a request's framing only; whether its fields are in
// contract is Request.validate's job (so a well-framed but invalid request gets
// an INVALID_REQUEST answer rather than a closed connection).
func decodeRequest(b []byte) (Request, error) {
	if len(b) < 1 {
		return Request{}, ErrProtocol
	}
	r := Request{Op: ReqOp(b[0])}
	if r.Op < ReqPut || r.Op > ReqRegister {
		return Request{}, fmt.Errorf("%w: op %d", ErrProtocol, b[0])
	}
	d := decoder{b: b, i: 1}
	r.ClientID, r.RequestID, r.AckedBelow = d.uint(), d.uint(), d.uint()
	r.Timeout = time.Duration(d.uint()) * time.Millisecond
	r.Key = d.bytes(MaxKeyLen)
	if r.Op == ReqPut {
		r.Value = d.bytes(MaxValueLen)
	}
	if err := d.done(); err != nil {
		return Request{}, err
	}
	return r, nil
}

func encodeResponse(r Response) []byte {
	b := []byte{byte(r.Status), 0}
	if r.Duplicate {
		b[1] = 1
	}
	b = binary.AppendUvarint(b, r.ClientID)
	b = binary.AppendUvarint(b, r.Term)
	b = binary.AppendUvarint(b, r.Index)
	for _, s := range [][]byte{[]byte(truncate(r.Node, maxNodeIDLen)), []byte(truncate(r.Via, maxNodeIDLen)),
		[]byte(truncate(r.Leader, maxNodeIDLen)), r.Value, []byte(truncate(r.Message, maxMessageLen))} {
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func decodeResponse(b []byte) (Response, error) {
	if len(b) < 2 {
		return Response{}, ErrProtocol
	}
	r := Response{Status: Status(b[0])}
	if r.Status > maxStatus || b[1] > 1 {
		return Response{}, fmt.Errorf("%w: status %d flags %d", ErrProtocol, b[0], b[1])
	}
	r.Duplicate = b[1] == 1
	d := decoder{b: b, i: 2}
	r.ClientID, r.Term, r.Index = d.uint(), d.uint(), d.uint()
	r.Node = string(d.bytes(maxNodeIDLen))
	r.Via = string(d.bytes(maxNodeIDLen))
	r.Leader = string(d.bytes(maxNodeIDLen))
	if v := d.bytes(MaxValueLen); len(v) > 0 || r.Status == StatusOK {
		r.Value = v
	}
	r.Message = string(d.bytes(maxMessageLen))
	if err := d.done(); err != nil {
		return Response{}, err
	}
	return r, nil
}

// decoder reads canonical uvarints and bounded byte strings, remembering the
// first error.
type decoder struct {
	b   []byte
	i   int
	err error
}

func (d *decoder) uint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := uvarint(d.b[d.i:])
	if n <= 0 {
		d.err = fmt.Errorf("%w: bad integer", ErrProtocol)
		return 0
	}
	d.i += n
	return v
}

func (d *decoder) bytes(max int) []byte {
	if d.err != nil {
		return nil
	}
	v, n, err := readBytes(d.b[d.i:], max)
	if err != nil {
		d.err = fmt.Errorf("%w: %v", ErrProtocol, err)
		return nil
	}
	d.i += n
	return v
}

func (d *decoder) done() error {
	if d.err == nil && d.i != len(d.b) {
		d.err = fmt.Errorf("%w: trailing bytes", ErrProtocol)
	}
	return d.err
}

// Forwarding payloads (transport kinds Forward and ForwardResponse): a forward
// id the forwarder chose, and for a forward the time budget left, then the
// request or response in the client encoding.
func encodeForward(fid uint64, budget time.Duration, r Request) []byte {
	if budget < 0 {
		budget = 0
	}
	b := binary.AppendUvarint(nil, fid)
	b = binary.AppendUvarint(b, uint64(budget/time.Millisecond))
	return append(b, encodeRequest(r)...)
}

func decodeForward(b []byte) (uint64, time.Duration, Request, error) {
	d := decoder{b: b}
	fid, ms := d.uint(), d.uint()
	if d.err != nil {
		return 0, 0, Request{}, d.err
	}
	r, err := decodeRequest(b[d.i:])
	return fid, time.Duration(ms) * time.Millisecond, r, err
}

func encodeForwardResponse(fid uint64, r Response) []byte {
	return append(binary.AppendUvarint(nil, fid), encodeResponse(r)...)
}

func decodeForwardResponse(b []byte) (uint64, Response, error) {
	d := decoder{b: b}
	fid := d.uint()
	if d.err != nil {
		return 0, Response{}, d.err
	}
	r, err := decodeResponse(b[d.i:])
	return fid, r, err
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
func readFrame(r io.Reader, want record.Kind, max int) ([]byte, error) {
	hdr := make([]byte, record.HeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(hdr[4:8])
	if length > uint32(max) {
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
		payload, err := readFrame(c, kindRequest, MaxRequestFrame)
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
		resp, _ := srv.Do(cctx, req)
		if err := writeFrame(c, kindResponse, encodeResponse(resp)); err != nil {
			return
		}
	}
}

// Client speaks the wire protocol to one node. It is safe for sequential use
// only (one request at a time); open one Client per concurrent request stream.
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

// NewClient returns a client that connects lazily, on its first request.
func NewClient(node, addr string) *Client { return &Client{node: node, addr: addr} }

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

// Do sends one request and returns the response (Doer). A connection that
// cannot be established is ErrUnavailable (nothing was sent: definite); a
// connection that breaks — or a deadline that passes — after the request was
// written is ErrUnknown, and the connection is abandoned so a late response can
// never be read as the answer to a later request. The request's Timeout
// defaults to the context's remaining time.
func (c *Client) Do(ctx context.Context, req Request) (Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return Response{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(DefaultRequestTimeout)
	}
	if req.Timeout <= 0 {
		req.Timeout = time.Until(deadline)
	}
	_ = c.conn.SetDeadline(deadline)
	if err := writeFrame(c.conn, kindRequest, encodeRequest(req)); err != nil {
		// The request may or may not have left; a stale connection detected on
		// the first write cannot be told apart, so stay conservative.
		c.dropConn()
		return Response{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	payload, err := readFrame(c.conn, kindResponse, maxResponseFrame)
	if err != nil {
		c.dropConn()
		return Response{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	resp, err := decodeResponse(payload)
	if err != nil {
		c.dropConn()
		return Response{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	return resp, nil
}

// Put, Get and Delete are the anonymous convenience API (Phase 12 semantics:
// no deduplication), with the error taxonomy of api.go. A transport failure is
// ErrUnavailable (nothing sent) or ErrUnknown (sent, no answer).
func (c *Client) Put(ctx context.Context, key, value []byte) (Meta, error) {
	resp, err := c.Do(ctx, Request{Op: ReqPut, Key: key, Value: value})
	if err != nil {
		return Meta{Node: c.node}, err
	}
	return metaOf(resp), errorOf(resp)
}

func (c *Client) Delete(ctx context.Context, key []byte) (Meta, error) {
	resp, err := c.Do(ctx, Request{Op: ReqDelete, Key: key})
	if err != nil {
		return Meta{Node: c.node}, err
	}
	return metaOf(resp), errorOf(resp)
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, Meta, error) {
	resp, err := c.Do(ctx, Request{Op: ReqGet, Key: key})
	if err != nil {
		return nil, Meta{Node: c.node}, err
	}
	return resp.Value, metaOf(resp), errorOf(resp)
}

func (c *Client) dropConn() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}
