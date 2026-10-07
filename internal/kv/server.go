package kv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/transport"
)

// ErrLost is raftnode.ErrLost: the attempt's log entry was overwritten by a
// different one — a definite no-effect for that attempt.
var ErrLost = raftnode.ErrLost

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return fmt.Sprintf("kv: %s is not the leader (leader unknown)", e.Node)
	}
	return fmt.Sprintf("kv: %s is not the leader (leader %s)", e.Node, e.Leader)
}

// Is makes errors.Is(err, raft.ErrNotLeader) true.
func (e *NotLeaderError) Is(target error) bool { return target == raft.ErrNotLeader }

// DefaultRequestTimeout is how long a server works on a request whose client
// set no budget (Request.Timeout 0), and the most it will take any budget as.
const DefaultRequestTimeout = 10 * time.Second

// Server serves the client API on one node of a Raft group (docs/API.md,
// docs/CLIENT_SEMANTICS.md): writes and REGISTER through Node.Write (completed
// only when committed and applied here in the proposal's term, with the state
// machine's decision for that entry), reads through Node.ReadIndex then the
// local Store (docs/DESIGN.md §8.5). A node that is not the leader forwards the
// request ONE hop to the leader it believes in, over the internal transport,
// and relays the answer (§9 of the contract); a forwarded request is never
// forwarded again. The Store must be the node's state machine.
type Server struct {
	id    string
	node  *raftnode.Node
	store Machine

	mu        sync.Mutex
	nextFwd   uint64 // forward ids; random start per incarnation (see NewServer)
	pending   map[uint64]chan Response
	noForward bool     // redirect-only mode (SetForwarding(false))
	m         *Metrics // Phase 16; nil: not instrumented

	// Inbound forwards (audit M1): one slot per forward being served, and
	// the queue of forwards refused for lack of one, answered UNAVAILABLE by
	// a single goroutine that lives as long as the node.
	fwdSlots chan struct{}
	refusals chan refusal
	refuser  sync.Once
}

// MaxForwardsServed bounds the forwards a server serves at once. Each holds a
// goroutine for up to the request's timeout; beyond the bound a forward is
// answered UNAVAILABLE at once — definite, nothing executed — instead of
// costing another goroutine. Healthy operation stays far below it: a
// forward is a non-leader's client request, served within a commit.
const MaxForwardsServed = 256

// refusal is an inbound forward refused for lack of a slot.
type refusal struct {
	peer raftnode.NodeID
	fid  uint64
}

func (s *Server) setMetrics(m *Metrics) {
	s.mu.Lock()
	s.m = m
	s.mu.Unlock()
}

func (s *Server) metrics() *Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m
}

// SetForwarding turns forwarding on (the default) or off. Off, the server is in
// redirect-only mode: a non-leader answers NOT_LEADER with the leader it
// believes in, and the client goes there itself (docs/API.md §5).
func (s *Server) SetForwarding(on bool) {
	s.mu.Lock()
	s.noForward = !on
	s.mu.Unlock()
}

// NewServer wires a node and its state machine, and installs the node's
// application-message handler for forwarding.
func NewServer(id string, node *raftnode.Node, store Machine) *Server {
	var seed [8]byte
	_, _ = rand.Read(seed[:])
	s := &Server{id: id, node: node, store: store, pending: map[uint64]chan Response{},
		// A forwarder's ids start at a random point, so a response to a
		// previous incarnation's forward — the transport may deliver it over
		// the new connection — cannot be taken for one of this incarnation's.
		nextFwd:  binary.LittleEndian.Uint64(seed[:]) >> 1,
		fwdSlots: make(chan struct{}, MaxForwardsServed),
		refusals: make(chan refusal, MaxForwardsServed)}
	node.SetAppHandler(s.onApp)
	return s
}

// Name is the node id.
func (s *Server) Name() string { return s.id }

// Store returns the node's state machine.
func (s *Server) Store() Machine { return s.store }

// Node returns the node.
func (s *Server) Node() *raftnode.Node { return s.node }

// Do serves one client request (Doer). It never returns a transport error.
func (s *Server) Do(ctx context.Context, req Request) (Response, error) {
	return s.handle(ctx, req, ""), nil
}

// handle serves a request that arrived from a client (via "") or was forwarded
// by peer via. It tries to execute locally; a definite not-leader answer from
// the local node — nothing was proposed or registered — is then forwarded once,
// unless the request was itself forwarded.
func (s *Server) handle(ctx context.Context, req Request, via string) Response {
	if err := req.validate(); err != nil {
		return Response{Status: StatusInvalid, Node: s.id, Message: err.Error()}
	}
	timeout := req.Timeout
	if timeout <= 0 || timeout > DefaultRequestTimeout {
		timeout = DefaultRequestTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp := s.execute(ctx, req)
	s.mu.Lock()
	redirectOnly := s.noForward
	s.mu.Unlock()
	if resp.Status != StatusNotLeader || via != "" || redirectOnly {
		return resp
	}
	leader := resp.Leader
	if leader == "" || leader == s.id {
		return resp // no leader known (or an inconsistent hint): the client decides
	}
	return s.forward(ctx, leader, req)
}

// execute runs the request on this node.
func (s *Server) execute(ctx context.Context, req Request) Response {
	if req.Op == ReqGet {
		idx, err := s.node.ReadIndex(ctx)
		resp := Response{Node: s.id, Term: s.node.Term(), Index: idx}
		if err != nil {
			return s.failed(resp, err)
		}
		v, ok, err := s.store.Lookup(req.Key)
		if err != nil {
			return s.failed(resp, err)
		}
		if !ok {
			resp.Status = StatusNotFound
			return resp
		}
		resp.Value = v
		return resp
	}
	idx, term, result, err := s.node.Write(ctx, req.command().Encode())
	resp := Response{Node: s.id, Term: term, Index: idx}
	if err != nil {
		return s.failed(resp, err)
	}
	r, ok := result.(Result)
	if !ok {
		return Response{Status: StatusUnknown, Node: s.id, Message: "the state machine returned no result"}
	}
	resp.Index = r.Index
	switch r.Decision {
	case Registered:
		resp.ClientID = r.Index
	case Executed:
	case Duplicate:
		resp.Duplicate = true
	case Conflict:
		resp.Status, resp.Message = StatusConflict, fmt.Sprintf("request %d of client %d was already executed with a different command", req.RequestID, req.ClientID)
	case Stale:
		resp.Status, resp.Message = StatusStale, fmt.Sprintf("request %d is below client %d's acknowledged watermark", req.RequestID, req.ClientID)
	case Expired:
		resp.Status, resp.Message = StatusSessionExpired, fmt.Sprintf("session %d is unknown or expired", req.ClientID)
	case Limit:
		resp.Status, resp.Message = StatusSessionLimit, fmt.Sprintf("session %d holds its maximum of unacknowledged results", req.ClientID)
	default:
		resp.Status, resp.Message = StatusUnknown, "unexpected state-machine decision "+r.Decision.String()
	}
	return resp
}

// failed classifies a driver error into its status.
func (s *Server) failed(resp Response, err error) Response {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		resp.Status, resp.Leader = StatusNotLeader, string(s.node.LeaderID())
	case errors.Is(err, ErrLost):
		resp.Status = StatusLost
	case errors.Is(err, raft.ErrEntryTooLarge):
		// Refused by Propose before anything was appended: definite. The front
		// refuses such a request first; this is the layer below agreeing.
		resp.Status = StatusInvalid
	case errors.Is(err, raft.ErrBusy):
		// The leader is at its bound of uncommitted entries or pending reads
		// (audit M3): nothing was appended or registered. Definite.
		resp.Status = StatusUnavailable
	default:
		resp.Status = StatusUnknown
	}
	resp.Message = err.Error()
	return resp
}

// forward sends the request to leader and waits for its answer. Nothing sent
// (the leader is not connected) is UNAVAILABLE — definite; sent and unanswered
// before the deadline is UNKNOWN — the leader may have executed it. A forward
// is sent at most once.
func (s *Server) forward(ctx context.Context, leader string, req Request) Response {
	ch := make(chan Response, 1)
	s.mu.Lock()
	s.nextFwd++
	fid := s.nextFwd
	s.pending[fid] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, fid)
		s.mu.Unlock()
	}()
	budget := time.Until(deadlineOf(ctx))
	payload := encodeForward(fid, budget, req)
	m := s.metrics()
	if err := s.node.SendApp(ctx, raftnode.NodeID(leader), transport.MsgForward, payload); err != nil {
		st, result := StatusUnknown, "send_unknown"
		if errors.Is(err, transport.ErrPeerNotConnected) || errors.Is(err, transport.ErrClosed) {
			st, result = StatusUnavailable, "not_sent" // not handed to any connection: nothing was sent
		}
		m.forward(s.node.Group(), result)
		return Response{Status: st, Node: s.id, Leader: leader, Message: "forward to " + leader + ": " + err.Error()}
	}
	select {
	case resp := <-ch:
		m.forward(s.node.Group(), "answered")
		resp.Via = s.id
		return resp
	case <-ctx.Done():
		m.forward(s.node.Group(), "timeout")
		return Response{Status: StatusUnknown, Node: s.id, Leader: leader, Message: "forwarded to " + leader + "; no answer before the deadline"}
	}
}

func deadlineOf(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Now().Add(DefaultRequestTimeout)
}

// onApp is the node's application-message handler: an inbound forward is
// served in its own goroutine (never forwarded again), one of at most
// MaxForwardsServed, and answered to the peer that sent it — or refused
// UNAVAILABLE when every slot is taken; a forward response is delivered to the
// forward waiting for it, if any (a late or unknown one is dropped). It runs
// on the node's receive path and never blocks.
func (s *Server) onApp(peer raftnode.NodeID, kind transport.MsgKind, payload []byte) {
	switch kind {
	case transport.MsgForward:
		fid, budget, req, err := decodeForward(payload)
		if err != nil {
			return // not our protocol: drop (the forwarder times out: unknown)
		}
		select {
		case s.fwdSlots <- struct{}{}:
		default:
			s.refuse(peer, fid)
			return
		}
		go func() {
			defer func() { <-s.fwdSlots }()
			req.Timeout = budget
			resp := s.handle(context.Background(), req, string(peer))
			s.metrics().servedForward(s.node.Group(), req, resp)
			_ = s.node.SendApp(context.Background(), peer, transport.MsgForwardResponse, encodeForwardResponse(fid, resp))
		}()
	case transport.MsgForwardResponse:
		fid, resp, err := decodeForwardResponse(payload)
		if err != nil {
			return
		}
		s.mu.Lock()
		ch := s.pending[fid]
		s.mu.Unlock()
		if ch != nil {
			select {
			case ch <- resp:
			default:
			}
		}
	}
}

// refuse queues a forward refused for lack of a slot, to be answered
// UNAVAILABLE. A full queue drops it: its forwarder's deadline then answers its
// client UNKNOWN, as for any forward that goes unanswered.
func (s *Server) refuse(peer raftnode.NodeID, fid uint64) {
	s.refuser.Do(func() { go s.answerRefusals() })
	select {
	case s.refusals <- refusal{peer: peer, fid: fid}:
	default:
	}
}

// answerRefusals answers refused forwards until the node stops.
func (s *Server) answerRefusals() {
	for {
		select {
		case r := <-s.refusals:
			resp := Response{Status: StatusUnavailable, Node: s.id, Message: "too many forwarded requests in progress"}
			_ = s.node.SendApp(context.Background(), r.peer, transport.MsgForwardResponse, encodeForwardResponse(r.fid, resp))
		case <-s.node.Done():
			return
		}
	}
}

// --- the anonymous convenience API (Phase 12 clients) ---

// Put stores value under key as an anonymous request (no deduplication). It
// returns nil only once the write is committed by a quorum and applied.
func (s *Server) Put(ctx context.Context, key, value []byte) (Meta, error) {
	resp, _ := s.Do(ctx, Request{Op: ReqPut, Key: key, Value: value})
	return metaOf(resp), errorOf(resp)
}

// Delete removes key (idempotent) as an anonymous request.
func (s *Server) Delete(ctx context.Context, key []byte) (Meta, error) {
	resp, _ := s.Do(ctx, Request{Op: ReqDelete, Key: key})
	return metaOf(resp), errorOf(resp)
}

// Get returns the value under key after a confirmed ReadIndex has been applied
// on the leader. ErrNotFound means the key is absent.
func (s *Server) Get(ctx context.Context, key []byte) ([]byte, Meta, error) {
	resp, _ := s.Do(ctx, Request{Op: ReqGet, Key: key})
	return resp.Value, metaOf(resp), errorOf(resp)
}
