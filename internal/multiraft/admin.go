package multiraft

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/replication"
)

// The admin protocol (Phase 15, docs/MULTI_RAFT.md §7): a deliberately small
// control interface, on its own listener and separate from the client
// protocol. Each request is one line of JSON, answered by one line of JSON, in
// order, on a plain TCP connection. It acts on THIS node's groups only, and
// never decides membership itself: a membership operation is submitted to the
// local node of the group, which must be the group's leader (otherwise the
// answer names the leader it believes in), and completes only when the
// group's log says so (raftnode.Node.ChangeMembership).
//
//	status                                   every hosted group's state; the groups that failed to recover
//	add-learner    group id addr             add a non-voting member at addr
//	promote        group id                  make a learner a voter (joint consensus)
//	remove-voter   group id                  remove a voter (joint consensus)
//	remove-learner group id                  remove a learner
//	create-group   group join                host the group as a joiner (its leader then adds this node)
//	create-group   group voters              host the group as a genesis member of voters
//	start-group    group                     start a stopped group again from its own files
//	stop-group     group                     stop hosting the group, keeping its files
//	snapshot       group                     snapshot the group's state machine now

// AdminRequest is one admin command.
type AdminRequest struct {
	Op      string   `json:"op"`
	Group   uint32   `json:"group"`
	ID      string   `json:"id,omitempty"`
	Addr    string   `json:"addr,omitempty"`
	Join    bool     `json:"join,omitempty"`
	Voters  []Member `json:"voters,omitempty"`
	Timeout int      `json:"timeout_ms,omitempty"` // default 10s, at most 60s
}

// Member is a member in an admin request or response.
type Member struct {
	ID   string `json:"id"`
	Addr string `json:"addr,omitempty"`
}

// AdminResponse is one answer.
type AdminResponse struct {
	OK     bool              `json:"ok"`
	Error  string            `json:"error,omitempty"`
	Leader string            `json:"leader,omitempty"` // not the leader: the one this node believes in
	Conf   *ConfStatus       `json:"conf,omitempty"`   // a membership change: the configuration reached
	Index  uint64            `json:"index,omitempty"`  // ... and its entry's index
	Groups []GroupStatus     `json:"groups,omitempty"`
	Failed map[string]string `json:"failed,omitempty"` // groups on disk that did not recover
}

// ConfStatus is a configuration.
type ConfStatus struct {
	Voters   []Member `json:"voters"`
	Outgoing []Member `json:"outgoing,omitempty"`
	Learners []Member `json:"learners,omitempty"`
}

// GroupStatus is one hosted group's state.
type GroupStatus struct {
	Group       uint32     `json:"group"`
	Role        string     `json:"role"`
	Term        uint64     `json:"term"`
	Leader      string     `json:"leader"`
	Commit      uint64     `json:"commit"`
	Applied     uint64     `json:"applied"`
	LastIndex   uint64     `json:"last_index"`
	Boundary    uint64     `json:"boundary"`
	Snapshot    uint64     `json:"snapshot"`
	Conf        ConfStatus `json:"conf"`
	ConfIndex   uint64     `json:"conf_index"`
	ConfPending bool       `json:"conf_pending"`
	Voter       bool       `json:"voter"`
	Removed     bool       `json:"removed"`
}

func confStatus(c replication.Configuration) ConfStatus {
	conv := func(ms []replication.Member) []Member {
		out := []Member{}
		for _, m := range ms {
			out = append(out, Member{ID: string(m.ID), Addr: m.Addr})
		}
		return out
	}
	cs := ConfStatus{Voters: conv(c.Voters)}
	if len(c.Outgoing) > 0 {
		cs.Outgoing = conv(c.Outgoing)
	}
	if len(c.Learners) > 0 {
		cs.Learners = conv(c.Learners)
	}
	return cs
}

// IDs returns the member ids of a list.
func IDs(ms []Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

// Bounds of the admin port (audit M1, L): what one connection may cost.
const (
	// MaxAdminLine bounds one admin request line.
	MaxAdminLine = 64 << 10
	// MaxAdminConns bounds the admin connections served at once; one beyond
	// it is closed as soon as it is accepted. The admin port serves an
	// operator's occasional command, one per connection.
	MaxAdminConns = 16
	// AdminIdleTimeout bounds the wait for a connection's next request line,
	// and AdminWriteTimeout the writing of an answer.
	AdminIdleTimeout  = 30 * time.Second
	AdminWriteTimeout = 10 * time.Second
	// DefaultAdminTimeout is a request's timeout when it names none, and
	// MaxAdminTimeout the most it may name.
	DefaultAdminTimeout = 10 * time.Second
	MaxAdminTimeout     = 60 * time.Second
)

// ServeAdmin answers admin requests on ln for host h until ctx ends or ln is
// closed. logf may be nil. An Accept error is logged and retried after a
// growing pause, never the end of the loop.
func ServeAdmin(ctx context.Context, ln net.Listener, h *Host, logf func(string, ...any)) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	slots := make(chan struct{}, MaxAdminConns)
	backoff := 5 * time.Millisecond
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			logf("event=admin_accept_failed node=%s err=%v retry_in=%s", h.cfg.ID, err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, time.Second)
			continue
		}
		backoff = 5 * time.Millisecond
		select {
		case slots <- struct{}{}:
		default:
			_ = c.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			serveAdminConn(ctx, c, h, logf)
		}()
	}
}

func serveAdminConn(ctx context.Context, c net.Conn, h *Host, logf func(string, ...any)) {
	defer c.Close()
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-cctx.Done()
		_ = c.Close()
	}()
	sc := bufio.NewScanner(c)
	sc.Buffer(make([]byte, 4096), MaxAdminLine)
	enc := json.NewEncoder(c)
	for {
		_ = c.SetReadDeadline(time.Now().Add(adminIdle))
		if !sc.Scan() {
			return
		}
		var req AdminRequest
		var resp AdminResponse
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			resp = AdminResponse{Error: "malformed request: " + err.Error()}
		} else {
			resp = h.Admin(cctx, req)
			if req.Op != "status" {
				// Quoted: the op and id are the client's, and a newline in
				// either would otherwise forge log lines.
				logf("event=admin node=%s op=%q group=%d id=%q ok=%v err=%q", h.cfg.ID, req.Op, req.Group, req.ID, resp.OK, resp.Error)
			}
		}
		_ = c.SetWriteDeadline(time.Now().Add(AdminWriteTimeout))
		if err := enc.Encode(resp); err != nil {
			return
		}
	}
}

// adminIdle is AdminIdleTimeout, a variable so tests can shorten it.
var adminIdle = AdminIdleTimeout

// adminTimeout is a request's timeout: its own, clamped to MaxAdminTimeout,
// or DefaultAdminTimeout.
func adminTimeout(ms int) time.Duration {
	switch {
	case ms <= 0:
		return DefaultAdminTimeout
	case ms >= int(MaxAdminTimeout/time.Millisecond): // also before ms×1e6 could overflow
		return MaxAdminTimeout
	}
	return time.Duration(ms) * time.Millisecond
}

// Admin executes one admin request against this host.
func (h *Host) Admin(ctx context.Context, req AdminRequest) AdminResponse {
	timeout := adminTimeout(req.Timeout)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	g := GroupID(req.Group)
	fail := func(err error) AdminResponse { return AdminResponse{Error: err.Error()} }
	switch req.Op {
	case "status":
		resp := AdminResponse{OK: true, Groups: []GroupStatus{}}
		for _, id := range h.Groups() {
			grp := h.Group(id)
			if grp == nil {
				continue
			}
			st := grp.Node.Status()
			resp.Groups = append(resp.Groups, GroupStatus{
				Group: uint32(id), Role: st.Role.String(), Term: st.Term, Leader: string(st.Leader),
				Commit: st.Commit, Applied: st.Applied, LastIndex: st.LastIndex, Boundary: st.Boundary, Snapshot: st.Snapshot,
				Conf: confStatus(st.Conf), ConfIndex: st.ConfIndex, ConfPending: st.ConfPending, Voter: st.Voter, Removed: st.Removed,
			})
		}
		if failed := h.Failed(); len(failed) > 0 {
			resp.Failed = map[string]string{}
			for id, err := range failed {
				resp.Failed[fmt.Sprint(id)] = err.Error()
			}
		}
		return resp
	case "add-learner", "promote", "remove-voter", "remove-learner":
		grp := h.Group(g)
		if grp == nil {
			return fail(fmt.Errorf("%w: group %d", ErrNoGroup, g))
		}
		if req.ID == "" {
			return fail(errors.New("an id is required"))
		}
		cc := raft.ConfChange{Member: raft.Member{ID: NodeID(req.ID), Addr: req.Addr}}
		switch req.Op {
		case "add-learner":
			cc.Type = raft.AddLearner
			if req.Addr == "" {
				return fail(errors.New("add-learner needs the member's address"))
			}
		case "promote":
			cc.Type = raft.Promote
		case "remove-voter":
			cc.Type = raft.RemoveVoter
		default:
			cc.Type = raft.RemoveLearner
		}
		conf, idx, err := grp.Node.ChangeMembership(ctx, cc)
		if err != nil {
			resp := fail(err)
			if errors.Is(err, raft.ErrNotLeader) {
				resp.Leader = string(grp.Node.LeaderID())
			}
			return resp
		}
		cs := confStatus(conf)
		return AdminResponse{OK: true, Conf: &cs, Index: idx}
	case "create-group":
		var boot *replication.Configuration
		switch {
		case req.Join && len(req.Voters) == 0:
		case !req.Join && len(req.Voters) > 0:
			var voters []replication.Member
			for _, m := range req.Voters {
				voters = append(voters, replication.Member{ID: NodeID(m.ID), Addr: m.Addr})
			}
			sort.Slice(voters, func(i, j int) bool { return voters[i].ID < voters[j].ID })
			c, err := replication.NewConfiguration(voters, nil)
			if err != nil {
				return fail(err)
			}
			boot = &c
		default:
			return fail(errors.New("create-group needs exactly one of join or voters"))
		}
		if _, err := h.Create(g, boot); err != nil {
			return fail(err)
		}
		return AdminResponse{OK: true}
	case "start-group":
		if _, err := h.Open(g); err != nil {
			return fail(err)
		}
		return AdminResponse{OK: true}
	case "stop-group":
		if err := h.Stop(g); err != nil {
			return fail(err)
		}
		return AdminResponse{OK: true}
	case "snapshot":
		grp := h.Group(g)
		if grp == nil {
			return fail(fmt.Errorf("%w: group %d", ErrNoGroup, g))
		}
		if err := grp.Node.Snapshot(ctx); err != nil {
			return fail(err)
		}
		return AdminResponse{OK: true, Index: grp.Node.Status().Snapshot}
	}
	return fail(fmt.Errorf("unknown op %q", req.Op))
}

// AdminCall sends one admin request to the admin listener at addr and returns
// its answer. err is a transport failure only.
func AdminCall(ctx context.Context, addr string, req AdminRequest) (AdminResponse, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return AdminResponse{}, err
	}
	defer c.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	b, err := json.Marshal(req)
	if err != nil {
		return AdminResponse{}, err
	}
	if _, err := c.Write(append(b, '\n')); err != nil {
		return AdminResponse{}, err
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return AdminResponse{}, err
	}
	var resp AdminResponse
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &resp); err != nil {
		return AdminResponse{}, err
	}
	return resp, nil
}
