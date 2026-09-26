package kv

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// The Phase 13 client API (docs/API.md, docs/CLIENT_SEMANTICS.md): one Request,
// one Response, eleven statuses in three outcome classes. It is served by
// Server.Do in-process and by the framed-TCP protocol (wire.go) on a node's
// client port; the same types travel on the internal forwarding hop.

// ReqOp is a client operation.
type ReqOp uint8

const (
	ReqPut      ReqOp = 1
	ReqGet      ReqOp = 2
	ReqDelete   ReqOp = 3
	ReqRegister ReqOp = 4 // create a client session; the Response carries its ClientID
)

func (o ReqOp) String() string {
	switch o {
	case ReqPut:
		return "PUT"
	case ReqGet:
		return "GET"
	case ReqDelete:
		return "DELETE"
	case ReqRegister:
		return "REGISTER"
	}
	return fmt.Sprintf("op(%d)", o)
}

// Request is one client request. ClientID 0 is anonymous (no deduplication:
// Phase 12 semantics). An identified PUT/DELETE is request RequestID of session
// ClientID, and AckedBelow is the client's promise that it holds every response
// below it and will never send those ids again (docs/CLIENT_SEMANTICS.md §3).
// A GET may carry an identity; reads are never deduplicated.
type Request struct {
	Op                              ReqOp
	ClientID, RequestID, AckedBelow uint64
	Key, Value                      []byte
	// Timeout is the client's budget for this attempt (0: the server default).
	// The server stops working on it, and a forwarder stops waiting, when it
	// runs out.
	Timeout time.Duration
}

// Status is a response's outcome (docs/CLIENT_SEMANTICS.md §6).
type Status uint8

const (
	StatusOK             Status = 0  // definite, effect: executed now or earlier (Duplicate)
	StatusNotFound       Status = 1  // definite: a read found the key absent
	StatusNotLeader      Status = 2  // definite, no effect; Leader names the node this one believes in
	StatusUnavailable    Status = 3  // definite, no effect: nothing was sent onward
	StatusInvalid        Status = 4  // definite, no effect: malformed or out of contract
	StatusConflict       Status = 5  // definite, no effect: the RequestID is taken by a different command
	StatusStale          Status = 6  // definite, no effect: below the session's AckedBelow
	StatusSessionExpired Status = 7  // definite, no effect for THIS attempt: the session is unknown or evicted
	StatusSessionLimit   Status = 8  // definite, no effect: too many unacknowledged results in the session
	StatusLost           Status = 9  // definite, no effect for THIS attempt: its entry was overwritten
	StatusUnknown        Status = 10 // unknown: a deadline passed, a node stopped, a forward went unanswered
	maxStatus                   = StatusUnknown
)

var statusNames = map[Status]string{
	StatusOK: "OK", StatusNotFound: "NOT_FOUND", StatusNotLeader: "NOT_LEADER", StatusUnavailable: "UNAVAILABLE",
	StatusInvalid: "INVALID_REQUEST", StatusConflict: "REQUEST_CONFLICT", StatusStale: "REQUEST_STALE",
	StatusSessionExpired: "SESSION_EXPIRED", StatusSessionLimit: "SESSION_LIMIT", StatusLost: "LOST",
	StatusUnknown: "UNKNOWN_OUTCOME",
}

func (s Status) String() string {
	if n, ok := statusNames[s]; ok {
		return n
	}
	return fmt.Sprintf("status(%d)", s)
}

// Unknown reports whether the status leaves the request's effect unknown.
func (s Status) Unknown() bool { return s == StatusUnknown }

// Response is one response.
type Response struct {
	Status   Status
	Value    []byte // GET with OK: the value (may be empty: a present empty value)
	ClientID uint64 // REGISTER with OK: the new session's id
	// Index and Term: for a write, the log index and term of the execution the
	// response reports — for a Duplicate, the ORIGINAL execution; for a read,
	// the read index and the serving leader's term.
	Index, Term uint64
	Duplicate   bool   // OK for a request that had already executed: answered, not re-executed
	Node        string // the node that served the request
	Via         string // the node that forwarded it there, if any
	Leader      string // NOT_LEADER / UNAVAILABLE: the leader the answering node believes in, if any
	Message     string // detail for a human; never needed to interpret the status
}

// Doer is anything that answers Requests: a Server in-process, or a Client over
// the wire. err is a TRANSPORT failure only: ErrUnavailable when nothing was
// sent (definite, no effect), ErrUnknown when the request was sent and no
// response came (unknown). Every other outcome is a Response status.
type Doer interface {
	Name() string
	Do(ctx context.Context, req Request) (Response, error)
}

// Errors, for the anonymous convenience API (Put/Get/Delete) that Phase 12
// clients use. Each maps one status.
var (
	ErrNotFound       = errors.New("kv: key not found")
	ErrInvalid        = errors.New("kv: invalid request")
	ErrUnknown        = errors.New("kv: outcome unknown")
	ErrUnavailable    = errors.New("kv: node unavailable")
	ErrConflict       = errors.New("kv: request id already used for a different command")
	ErrStale          = errors.New("kv: request id below the session's acknowledged watermark")
	ErrSessionExpired = errors.New("kv: session unknown or expired")
	ErrSessionLimit   = errors.New("kv: too many unacknowledged requests in the session")
)

// validate checks a request against the protocol's rules (docs/API.md §3). A
// request that passes becomes a command that Decode accepts, so a validated
// write always applies.
func (r Request) validate() error {
	switch r.Op {
	case ReqRegister:
		if len(r.Key) != 0 || len(r.Value) != 0 || r.ClientID != 0 || r.RequestID != 0 || r.AckedBelow != 0 {
			return fmt.Errorf("%w: REGISTER carries no key, value or identity", ErrInvalid)
		}
		return nil
	case ReqPut, ReqGet, ReqDelete:
	default:
		return fmt.Errorf("%w: unknown operation %d", ErrInvalid, r.Op)
	}
	if len(r.Key) == 0 {
		return fmt.Errorf("%w: empty key", ErrInvalid)
	}
	if len(r.Key) > MaxKeyLen {
		return fmt.Errorf("%w: key of %d bytes exceeds %d", ErrInvalid, len(r.Key), MaxKeyLen)
	}
	if r.Op != ReqPut && len(r.Value) != 0 {
		return fmt.Errorf("%w: a %s carries no value", ErrInvalid, r.Op)
	}
	if len(r.Value) > MaxValueLen {
		return fmt.Errorf("%w: value of %d bytes exceeds %d", ErrInvalid, len(r.Value), MaxValueLen)
	}
	if r.ClientID == 0 {
		if r.RequestID != 0 || r.AckedBelow != 0 {
			return fmt.Errorf("%w: an anonymous request carries no request id or watermark", ErrInvalid)
		}
		return nil
	}
	if r.RequestID == 0 || r.AckedBelow == 0 || r.AckedBelow > r.RequestID {
		return fmt.Errorf("%w: an identified request needs request id >= 1 and 1 <= acked-below <= request id (got %d, %d)", ErrInvalid, r.RequestID, r.AckedBelow)
	}
	return nil
}

// command is the log entry a validated write request becomes.
func (r Request) command() Command {
	switch r.Op {
	case ReqRegister:
		return Command{Op: OpRegister}
	case ReqPut:
		v := r.Value
		if v == nil {
			v = []byte{}
		}
		return Command{Op: OpPut, Key: r.Key, Value: v, ClientID: r.ClientID, RequestID: r.RequestID, AckedBelow: r.AckedBelow}
	default:
		return Command{Op: OpDelete, Key: r.Key, ClientID: r.ClientID, RequestID: r.RequestID, AckedBelow: r.AckedBelow}
	}
}

// NotLeaderError is the definite rejection of a node that is not the leader,
// with the leader it believes in (empty if it knows none). errors.Is(err,
// raft.ErrNotLeader) holds for it.
type NotLeaderError struct {
	Node   string
	Leader string
}

// Meta is where and when a completed operation was served (the anonymous
// convenience API's view of a Response): the serving node, and the node that
// forwarded the request there, if any.
type Meta struct {
	Node  string
	Via   string
	Term  uint64
	Index uint64
}

// errorOf maps a response to the anonymous API's error taxonomy.
func errorOf(resp Response) error {
	switch resp.Status {
	case StatusOK:
		return nil
	case StatusNotFound:
		return ErrNotFound
	case StatusNotLeader:
		return &NotLeaderError{Node: resp.Node, Leader: resp.Leader}
	case StatusUnavailable:
		return fmt.Errorf("%w: %s", ErrUnavailable, resp.Message)
	case StatusInvalid:
		return fmt.Errorf("%w: %s", ErrInvalid, resp.Message)
	case StatusConflict:
		return ErrConflict
	case StatusStale:
		return ErrStale
	case StatusSessionExpired:
		return ErrSessionExpired
	case StatusSessionLimit:
		return ErrSessionLimit
	case StatusLost:
		return ErrLost
	default:
		return fmt.Errorf("%w: %s", ErrUnknown, resp.Message)
	}
}

func metaOf(resp Response) Meta {
	return Meta{Node: resp.Node, Via: resp.Via, Term: resp.Term, Index: resp.Index}
}
