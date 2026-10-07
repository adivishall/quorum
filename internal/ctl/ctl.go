// Package ctl is dkvctl, the operator's command line (docs/OPERATIONS.md): a
// client of dkvd's admin protocol and nothing else — every answer it prints
// is a node's own status, read now, or a verdict internal/health derives from
// such answers. It does not cache, guess or simulate: a node that does not
// answer is reported unreachable.
//
// Exit codes, so a script or a probe can branch on them:
//
//	0  ok: the command succeeded; health: healthy; ready: ready
//	1  degraded (health), or the operation itself failed
//	2  usage error
//	3  unavailable (health, leader: a group with no confirmed leader) or not ready
//	4  unreachable: no node answered
package ctl

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/adivishall/quorum/internal/health"
	"github.com/adivishall/quorum/internal/multiraft"
)

// Exit codes.
const (
	ExitOK          = 0
	ExitDegraded    = 1 // also: the requested operation failed
	ExitUsage       = 2
	ExitUnavailable = 3 // also: not ready
	ExitUnreachable = 4
)

const usage = `usage: dkvctl [flags] <command> [command flags]

flags:
  -nodes id=addr,...   the nodes' admin addresses (dkvd -admin-listen)
  -admin addr          one node's admin address (instead of -nodes)
  -json                machine-readable output
  -timeout d           per-node request timeout (default 2s)
  -max-apply-lag n     entries a node's applied index may trail its commit (default 64)
  -max-follower-lag n  entries a voter may trail its leader (default 1024)

commands:
  status               every group on every node: role, term, leader, indexes, snapshot, pending
  leader [-group g]    each group's leader and term, and how many voters confirm it
  groups               each group's configuration: voters, learners, a joint configuration
  members -group g     one group's configuration
  lag                  per group, on its leader: every member's match index and lag; every node's apply lag
  health               each group's health (healthy, degraded, unavailable) and every node's readiness
  ready [node]         one node's readiness (live is not ready; ready is not a healthy cluster)
  snapshot -group g [node]   make a node (default: the group's leader) snapshot the group

exit: 0 ok/healthy/ready, 1 degraded or the operation failed, 2 usage,
      3 unavailable or not ready, 4 unreachable
`

// node is one addressed node.
type node struct{ name, addr string }

// env is one invocation's settings.
type env struct {
	nodes   []node
	json    bool
	timeout time.Duration
	opts    health.Options
	stdout  io.Writer
	stderr  io.Writer
}

// Run runs dkvctl with args and returns its exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dkvctl", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	nodesF := fs.String("nodes", "", "")
	adminF := fs.String("admin", "", "")
	jsonF := fs.Bool("json", false, "")
	timeout := fs.Duration("timeout", 2*time.Second, "")
	applyLag := fs.Uint64("max-apply-lag", 0, "")
	followerLag := fs.Uint64("max-follower-lag", 0, "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(stderr, "dkvctl: %v\n%s", err, usage)
		return ExitUsage
	}
	e := &env{json: *jsonF, timeout: *timeout, stdout: stdout, stderr: stderr,
		opts: health.Options{MaxApplyLag: *applyLag, MaxFollowerLag: *followerLag}}
	switch {
	case *nodesF != "" && *adminF != "":
		fmt.Fprintf(stderr, "dkvctl: -nodes and -admin are alternatives\n")
		return ExitUsage
	case *adminF != "":
		e.nodes = []node{{name: *adminF, addr: *adminF}}
	case *nodesF != "":
		for _, part := range strings.Split(*nodesF, ",") {
			name, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
			if !ok || name == "" || addr == "" {
				fmt.Fprintf(stderr, "dkvctl: -nodes %q: want id=host:port,...\n", *nodesF)
				return ExitUsage
			}
			e.nodes = append(e.nodes, node{name, addr})
		}
	default:
		fmt.Fprintf(stderr, "dkvctl: name the nodes: -nodes id=addr,... or -admin addr\n%s", usage)
		return ExitUsage
	}
	if e.timeout <= 0 {
		fmt.Fprintf(stderr, "dkvctl: -timeout must be positive\n")
		return ExitUsage
	}
	if fs.NArg() == 0 {
		fmt.Fprint(stderr, usage)
		return ExitUsage
	}
	cmd, rest := fs.Arg(0), fs.Args()[1:]
	switch cmd {
	case "status":
		return e.status(ctx, rest)
	case "leader":
		return e.leader(ctx, rest)
	case "groups":
		return e.groups(ctx, rest, false)
	case "members":
		return e.groups(ctx, rest, true)
	case "lag":
		return e.lag(ctx, rest)
	case "health":
		return e.health(ctx, rest)
	case "ready":
		return e.ready(ctx, rest)
	case "snapshot":
		return e.snapshot(ctx, rest)
	case "help", "-h", "-help":
		fmt.Fprint(stdout, usage)
		return ExitOK
	}
	fmt.Fprintf(stderr, "dkvctl: unknown command %q\n%s", cmd, usage)
	return ExitUsage
}

// probe asks every node for its status, in parallel.
func (e *env) probe(ctx context.Context) []health.Probe {
	out := make([]health.Probe, len(e.nodes))
	var wg sync.WaitGroup
	for i, n := range e.nodes {
		wg.Add(1)
		go func(i int, n node) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, e.timeout)
			defer cancel()
			resp, err := multiraft.AdminCall(cctx, n.addr, multiraft.AdminRequest{Op: "status"})
			p := health.Probe{Node: n.name}
			if err != nil {
				p.Err = err.Error()
			} else {
				p.Status = &resp
				if resp.Node != "" && n.name == n.addr {
					p.Node = resp.Node // -admin: name the node by its own id
				}
			}
			out[i] = p
		}(i, n)
	}
	wg.Wait()
	return out
}

func answered(ps []health.Probe) bool {
	for _, p := range ps {
		if p.Status != nil {
			return true
		}
	}
	return false
}

// emit writes v as JSON.
func (e *env) emit(v any) {
	enc := json.NewEncoder(e.stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func (e *env) table() *tabwriter.Writer { return tabwriter.NewWriter(e.stdout, 0, 2, 2, ' ', 0) }

// unreachable reports that no node answered.
func (e *env) unreachable(ps []health.Probe) int {
	if e.json {
		e.emit(map[string]any{"verdict": health.Unreachable, "nodes": ps})
		return ExitUnreachable
	}
	for _, p := range ps {
		fmt.Fprintf(e.stderr, "dkvctl: %s: unreachable: %s\n", p.Node, p.Err)
	}
	return ExitUnreachable
}

func noFlags(name string, args []string, stderr io.Writer) bool {
	if len(args) > 0 {
		fmt.Fprintf(stderr, "dkvctl %s: unexpected arguments %q\n", name, args)
		return false
	}
	return true
}

func (e *env) status(ctx context.Context, args []string) int {
	if !noFlags("status", args, e.stderr) {
		return ExitUsage
	}
	ps := e.probe(ctx)
	if !answered(ps) {
		return e.unreachable(ps)
	}
	if e.json {
		type nodeStatus struct {
			Node   string                   `json:"node"`
			Error  string                   `json:"error,omitempty"`
			Status *multiraft.AdminResponse `json:"status,omitempty"`
		}
		var out []nodeStatus
		for _, p := range ps {
			out = append(out, nodeStatus{p.Node, p.Err, p.Status})
		}
		e.emit(out)
		return ExitOK
	}
	w := e.table()
	fmt.Fprintln(w, "NODE\tGROUP\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAST\tSNAPSHOT\tBOUNDARY\tVOTER\tPENDING W/R")
	for _, p := range ps {
		if p.Status == nil {
			fmt.Fprintf(w, "%s\t-\tunreachable\t\t\t\t\t\t\t\t\t\n", p.Node)
			continue
		}
		if len(p.Status.Groups) == 0 {
			fmt.Fprintf(w, "%s\t-\tno groups\t\t\t\t\t\t\t\t\t\n", p.Node)
		}
		for _, g := range p.Status.Groups {
			leader := g.Leader
			if leader == "" {
				leader = "-"
			}
			role := g.Role
			if g.Removed {
				role += " (removed)"
			}
			fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%v\t%d/%d\n", p.Node, g.Group, role, g.Term, leader,
				g.Commit, g.Applied, g.LastIndex, g.Snapshot, g.Boundary, g.Voter, g.PendingWrites, g.PendingReads)
		}
		for g, err := range p.Status.Failed {
			fmt.Fprintf(w, "%s\t%s\tFAILED: %s\t\t\t\t\t\t\t\t\t\n", p.Node, g, err)
		}
	}
	_ = w.Flush()
	return ExitOK
}

func (e *env) leader(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("leader", flag.ContinueOnError)
	fs.SetOutput(e.stderr)
	group := fs.Int("group", -1, "only this group")
	if err := fs.Parse(args); err != nil || fs.NArg() > 0 {
		return ExitUsage
	}
	ps := e.probe(ctx)
	if !answered(ps) {
		return e.unreachable(ps)
	}
	h := health.Cluster(ps, e.opts)
	type row struct {
		Group    uint32 `json:"group"`
		Leader   string `json:"leader"`
		Term     uint64 `json:"term"`
		Agreeing int    `json:"agreeing"`
		Quorum   int    `json:"quorum"`
		Verdict  string `json:"verdict"`
	}
	var rows []row
	code := ExitOK
	for _, g := range h.Groups {
		if *group >= 0 && g.Group != uint32(*group) {
			continue
		}
		rows = append(rows, row{g.Group, g.Leader, g.Term, g.Agreeing, g.Quorum, g.Verdict})
		if g.Verdict == health.Unavailable {
			code = ExitUnavailable
		}
	}
	if *group >= 0 && len(rows) == 0 {
		fmt.Fprintf(e.stderr, "dkvctl: no node reports group %d\n", *group)
		return ExitUnavailable
	}
	if e.json {
		e.emit(rows)
		return code
	}
	w := e.table()
	fmt.Fprintln(w, "GROUP\tLEADER\tTERM\tCONFIRMED BY\tVERDICT")
	for _, r := range rows {
		leader := r.Leader
		if r.Verdict == health.Unavailable {
			leader = "none confirmed"
		}
		fmt.Fprintf(w, "%d\t%s\t%d\t%d of quorum %d\t%s\n", r.Group, leader, r.Term, r.Agreeing, r.Quorum, r.Verdict)
	}
	_ = w.Flush()
	return code
}

// confOf returns each group's configuration as its leader reports it — or,
// with no leader, as the node with the highest configuration index does.
func confOf(ps []health.Probe) map[uint32]multiraft.GroupStatus {
	out := map[uint32]multiraft.GroupStatus{}
	for _, p := range ps {
		if p.Status == nil {
			continue
		}
		for _, g := range p.Status.Groups {
			cur, ok := out[g.Group]
			switch {
			case !ok:
				out[g.Group] = g
			case g.Role == "Leader" && (cur.Role != "Leader" || g.Term > cur.Term):
				out[g.Group] = g
			case cur.Role != "Leader" && g.ConfIndex > cur.ConfIndex:
				out[g.Group] = g
			}
		}
	}
	return out
}

func (e *env) groups(ctx context.Context, args []string, one bool) int {
	name := "groups"
	if one {
		name = "members"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(e.stderr)
	group := fs.Int("group", -1, "the group")
	if err := fs.Parse(args); err != nil || fs.NArg() > 0 || (one && *group < 0) {
		if one && *group < 0 {
			fmt.Fprintf(e.stderr, "dkvctl members: -group is required\n")
		}
		return ExitUsage
	}
	ps := e.probe(ctx)
	if !answered(ps) {
		return e.unreachable(ps)
	}
	confs := confOf(ps)
	var ids []uint32
	for g := range confs {
		if *group < 0 || g == uint32(*group) {
			ids = append(ids, g)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) == 0 && *group >= 0 {
		fmt.Fprintf(e.stderr, "dkvctl: no node reports group %d\n", *group)
		return ExitUnavailable
	}
	type row struct {
		Group     uint32               `json:"group"`
		From      string               `json:"reported_by_role"`
		Conf      multiraft.ConfStatus `json:"conf"`
		ConfIndex uint64               `json:"conf_index"`
		Pending   bool                 `json:"conf_pending"`
		Hosts     []string             `json:"hosted_on"`
	}
	var rows []row
	for _, g := range ids {
		c := confs[g]
		r := row{Group: g, From: c.Role, Conf: c.Conf, ConfIndex: c.ConfIndex, Pending: c.ConfPending}
		for _, p := range ps {
			if p.Status == nil {
				continue
			}
			for _, gs := range p.Status.Groups {
				if gs.Group == g {
					r.Hosts = append(r.Hosts, p.Node)
				}
			}
		}
		rows = append(rows, r)
	}
	if e.json {
		e.emit(rows)
		return ExitOK
	}
	w := e.table()
	fmt.Fprintln(w, "GROUP\tVOTERS\tLEARNERS\tJOINT (OUTGOING)\tCONF INDEX\tPENDING\tHOSTED ON")
	for _, r := range rows {
		dash := func(ms []multiraft.Member) string {
			if len(ms) == 0 {
				return "-"
			}
			return strings.Join(multiraft.IDs(ms), ",")
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%v\t%s\n", r.Group, dash(r.Conf.Voters), dash(r.Conf.Learners), dash(r.Conf.Outgoing),
			r.ConfIndex, r.Pending, strings.Join(r.Hosts, ","))
	}
	_ = w.Flush()
	return ExitOK
}

func (e *env) lag(ctx context.Context, args []string) int {
	if !noFlags("lag", args, e.stderr) {
		return ExitUsage
	}
	ps := e.probe(ctx)
	if !answered(ps) {
		return e.unreachable(ps)
	}
	h := health.Cluster(ps, e.opts)
	type applyLag struct {
		Node     string `json:"node"`
		Group    uint32 `json:"group"`
		Commit   uint64 `json:"commit"`
		Applied  uint64 `json:"applied"`
		ApplyLag uint64 `json:"apply_lag"`
	}
	var applies []applyLag
	for _, p := range ps {
		if p.Status == nil {
			continue
		}
		for _, g := range p.Status.Groups {
			var l uint64
			if g.Commit > g.Applied {
				l = g.Commit - g.Applied
			}
			applies = append(applies, applyLag{p.Node, g.Group, g.Commit, g.Applied, l})
		}
	}
	if e.json {
		e.emit(map[string]any{"groups": h.Groups, "apply": applies})
		return ExitOK
	}
	w := e.table()
	fmt.Fprintln(w, "GROUP\tLEADER\tTERM\tVOTER\tSTATE\tLAG (ENTRIES)")
	for _, g := range h.Groups {
		leader := g.Leader
		if leader == "" {
			leader = "-"
		}
		for _, v := range g.Voters {
			lag := fmt.Sprint(v.Lag)
			if v.State != "following" && v.State != "lagging" {
				lag = "-"
			}
			fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%s\t%s\n", g.Group, leader, g.Term, v.Node, v.State, lag)
		}
	}
	_ = w.Flush()
	fmt.Fprintln(e.stdout)
	w = e.table()
	fmt.Fprintln(w, "NODE\tGROUP\tCOMMIT\tAPPLIED\tAPPLY LAG")
	for _, a := range applies {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\n", a.Node, a.Group, a.Commit, a.Applied, a.ApplyLag)
	}
	_ = w.Flush()
	return ExitOK
}

func (e *env) health(ctx context.Context, args []string) int {
	if !noFlags("health", args, e.stderr) {
		return ExitUsage
	}
	ps := e.probe(ctx)
	h := health.Cluster(ps, e.opts)
	code := map[string]int{health.Healthy: ExitOK, health.Degraded: ExitDegraded, health.Unavailable: ExitUnavailable,
		health.Unreachable: ExitUnreachable}[h.Verdict]
	if e.json {
		e.emit(h)
		return code
	}
	fmt.Fprintf(e.stdout, "cluster: %s\n", h.Verdict)
	w := e.table()
	fmt.Fprintln(w, "GROUP\tVERDICT\tLEADER\tTERM\tCONFIRMED\tREASONS")
	for _, g := range h.Groups {
		leader := g.Leader
		if leader == "" {
			leader = "-"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d/%d\t%s\n", g.Group, g.Verdict, leader, g.Term, g.Agreeing, g.Quorum, strings.Join(g.Reasons, "; "))
	}
	_ = w.Flush()
	w = e.table()
	fmt.Fprintln(w, "NODE\tREADINESS\tREASONS")
	for _, n := range h.Nodes {
		fmt.Fprintf(w, "%s\t%s\t%s\n", n.Node, n.Verdict, strings.Join(n.Reasons, "; "))
	}
	_ = w.Flush()
	return code
}

func (e *env) ready(ctx context.Context, args []string) int {
	if len(args) > 1 {
		fmt.Fprintf(e.stderr, "dkvctl ready: at most one node\n")
		return ExitUsage
	}
	target := e.nodes
	if len(args) == 1 {
		target = nil
		for _, n := range e.nodes {
			if n.name == args[0] {
				target = []node{n}
			}
		}
		if target == nil {
			fmt.Fprintf(e.stderr, "dkvctl ready: no node %q in -nodes\n", args[0])
			return ExitUsage
		}
	} else if len(e.nodes) > 1 {
		fmt.Fprintf(e.stderr, "dkvctl ready: name the node (one of -nodes)\n")
		return ExitUsage
	}
	sub := &env{nodes: target, json: e.json, timeout: e.timeout, opts: e.opts, stdout: e.stdout, stderr: e.stderr}
	p := sub.probe(ctx)[0]
	r := health.Node(p, e.opts)
	code := map[string]int{health.Ready: ExitOK, health.NotReady: ExitUnavailable, health.Unreachable: ExitUnreachable}[r.Verdict]
	if e.json {
		e.emit(r)
		return code
	}
	fmt.Fprintf(e.stdout, "%s: %s\n", r.Node, r.Verdict)
	for _, why := range r.Reasons {
		fmt.Fprintf(e.stdout, "  %s\n", why)
	}
	return code
}

func (e *env) snapshot(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	fs.SetOutput(e.stderr)
	group := fs.Int("group", -1, "the group")
	if err := fs.Parse(args); err != nil || *group < 0 || fs.NArg() > 1 {
		fmt.Fprintf(e.stderr, "dkvctl snapshot: -group g [node]\n")
		return ExitUsage
	}
	var target *node
	if fs.NArg() == 1 {
		for i := range e.nodes {
			if e.nodes[i].name == fs.Arg(0) {
				target = &e.nodes[i]
			}
		}
		if target == nil {
			fmt.Fprintf(e.stderr, "dkvctl snapshot: no node %q in -nodes\n", fs.Arg(0))
			return ExitUsage
		}
	} else {
		ps := e.probe(ctx)
		if !answered(ps) {
			return e.unreachable(ps)
		}
		h := health.Cluster(ps, e.opts)
		for _, g := range h.Groups {
			if g.Group == uint32(*group) && g.Leader != "" {
				for i := range e.nodes {
					if e.nodes[i].name == g.Leader || ps[i].Status != nil && ps[i].Status.Node == g.Leader {
						target = &e.nodes[i]
					}
				}
			}
		}
		if target == nil {
			fmt.Fprintf(e.stderr, "dkvctl snapshot: no leader of group %d among the nodes; name one\n", *group)
			return ExitUnavailable
		}
	}
	cctx, cancel := context.WithTimeout(ctx, e.timeout+10*time.Second)
	defer cancel()
	resp, err := multiraft.AdminCall(cctx, target.addr, multiraft.AdminRequest{Op: "snapshot", Group: uint32(*group)})
	if err != nil {
		fmt.Fprintf(e.stderr, "dkvctl: %s: unreachable: %v\n", target.name, err)
		return ExitUnreachable
	}
	if e.json {
		e.emit(resp)
	}
	if !resp.OK {
		if !e.json {
			fmt.Fprintf(e.stderr, "dkvctl: %s refused: %s\n", target.name, resp.Error)
		}
		return ExitDegraded
	}
	if !e.json {
		fmt.Fprintf(e.stdout, "%s: group %d snapshot at index %d\n", target.name, *group, resp.Index)
	}
	return ExitOK
}
