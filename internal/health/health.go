// Package health turns the admin protocol's status answers into health and
// readiness verdicts (docs/OPERATIONS.md). It is pure: it reads status
// responses already obtained — or the failure to obtain one — and decides, so
// every verdict is a function of what the nodes said and can be tested on
// synthesized states.
//
// There are two views, and they answer different questions:
//
//   - Node readiness (Node): can this one node serve traffic, judging by its
//     own state? Live — its admin port answered — is not ready, and ready is
//     not a healthy cluster.
//   - Group health (Cluster): can each Raft group make progress, judging by
//     what a majority of its voters agree on? It needs several nodes' answers.
//
// What neither can see: an isolated node that still believes it leads (Raft
// without CheckQuorum keeps that belief until it hears a higher term) looks
// ready to itself; only the cluster view, where the majority follows another
// leader in a higher term, exposes it. Both views are snapshots: a verdict says
// what the answers showed when they were taken.
package health

import (
	"fmt"
	"sort"

	"github.com/adivishall/quorum/internal/multiraft"
)

// Verdicts.
const (
	Unreachable = "unreachable" // the node did not answer
	Ready       = "ready"       // node: may serve traffic
	NotReady    = "not-ready"   // node: answered, but must not be sent traffic
	Healthy     = "healthy"     // group or cluster: a confirmed quorum, every voter following and caught up
	Degraded    = "degraded"    // a confirmed quorum, but some voter unreachable, behind or following another leader
	Unavailable = "unavailable" // no quorum confirms a leader: the group cannot be shown to make progress
)

// Options bound what counts as caught up.
type Options struct {
	// MaxApplyLag is how far a node's applied index may trail its commit
	// index (0: 64 entries).
	MaxApplyLag uint64
	// MaxFollowerLag is how far, on the leader, a voter's match index may
	// trail the leader's last index (0: 1024 entries).
	MaxFollowerLag uint64
}

func (o Options) applyLag() uint64 {
	if o.MaxApplyLag == 0 {
		return 64
	}
	return o.MaxApplyLag
}

func (o Options) followerLag() uint64 {
	if o.MaxFollowerLag == 0 {
		return 1024
	}
	return o.MaxFollowerLag
}

// Probe is one node's answer to a status request, or why there was none.
type Probe struct {
	Node   string                   `json:"node"` // the name the caller knows the node by
	Status *multiraft.AdminResponse `json:"-"`
	Err    string                   `json:"error,omitempty"`
}

// GroupReadiness is one hosted group's part in a node's readiness.
type GroupReadiness struct {
	Group    uint32 `json:"group"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	Leader   string `json:"leader"`
	ApplyLag uint64 `json:"apply_lag"`
	Ready    bool   `json:"ready"`
	Reason   string `json:"reason,omitempty"`
}

// NodeReadiness is a node's readiness verdict.
type NodeReadiness struct {
	Node    string           `json:"node"`
	Verdict string           `json:"verdict"`
	Live    bool             `json:"live"`
	Reasons []string         `json:"reasons,omitempty"`
	Groups  []GroupReadiness `json:"groups,omitempty"`
}

// Node decides one node's readiness from its own status:
//
//	unreachable  no answer
//	not-ready    it answered, but hosts no group; a group failed to recover; or
//	             in some group it belongs to it knows no leader, or its applied
//	             index trails its commit index by more than MaxApplyLag
//	ready        otherwise
//
// A group the node was removed from does not count: it is being retired and
// the node is no longer its member. A joiner not yet added to its group's
// configuration (no voters known) is not ready for that group.
func Node(p Probe, o Options) NodeReadiness {
	r := NodeReadiness{Node: p.Node}
	if p.Status == nil {
		r.Verdict = Unreachable
		if p.Err != "" {
			r.Reasons = []string{p.Err}
		}
		return r
	}
	st := p.Status
	r.Live = true
	if !st.OK {
		r.Verdict, r.Reasons = NotReady, []string{"status refused: " + st.Error}
		return r
	}
	var failed []string
	for g, err := range st.Failed {
		failed = append(failed, fmt.Sprintf("group %s failed to recover: %s", g, err))
	}
	sort.Strings(failed)
	r.Reasons = append(r.Reasons, failed...)
	member := 0
	for _, gs := range st.Groups {
		gr := GroupReadiness{Group: gs.Group, Role: gs.Role, Term: gs.Term, Leader: gs.Leader, Ready: true}
		if gs.Commit > gs.Applied {
			gr.ApplyLag = gs.Commit - gs.Applied
		}
		switch {
		case gs.Removed:
			gr.Ready, gr.Reason = true, "removed from the group (being retired)"
		case len(gs.Conf.Voters) == 0:
			gr.Ready, gr.Reason = false, "not yet a member: no configuration known"
		case gs.Leader == "":
			gr.Ready, gr.Reason = false, "no leader known"
		case gr.ApplyLag > o.applyLag():
			gr.Ready, gr.Reason = false, fmt.Sprintf("applied %d trails commit %d by more than %d", gs.Applied, gs.Commit, o.applyLag())
		}
		if !gs.Removed {
			member++
		}
		if !gr.Ready {
			r.Reasons = append(r.Reasons, fmt.Sprintf("group %d: %s", gs.Group, gr.Reason))
		}
		r.Groups = append(r.Groups, gr)
	}
	if member == 0 && len(st.Failed) == 0 {
		r.Reasons = append(r.Reasons, "hosts no group")
	}
	if len(r.Reasons) == 0 {
		r.Verdict = Ready
	} else {
		r.Verdict = NotReady
	}
	return r
}

// Voter is one voter's part in its group's health.
type Voter struct {
	Node   string `json:"node"`
	State  string `json:"state"` // following, leading, unreachable, stale, lagging, not-hosting
	Term   uint64 `json:"term,omitempty"`
	Leader string `json:"leader,omitempty"`
	Lag    uint64 `json:"lag,omitempty"` // on the leader: last index minus this voter's match
}

// GroupHealth is one group's verdict.
type GroupHealth struct {
	Group    uint32   `json:"group"`
	Verdict  string   `json:"verdict"`
	Leader   string   `json:"leader,omitempty"`
	Term     uint64   `json:"term,omitempty"`
	Quorum   int      `json:"quorum"`   // voters needed
	Agreeing int      `json:"agreeing"` // voters confirmed following the leader in its term (the leader included)
	Voters   []Voter  `json:"voters"`
	Pending  bool     `json:"conf_pending,omitempty"`
	Reasons  []string `json:"reasons,omitempty"`
}

// ClusterHealth is the verdict over every group the probed nodes report.
type ClusterHealth struct {
	Verdict string          `json:"verdict"`
	Nodes   []NodeReadiness `json:"nodes"`
	Groups  []GroupHealth   `json:"groups"`
}

// Cluster decides each group's health from every probed node's status. For a
// group, the leader is the node reporting itself leader in the highest term,
// and its voters those of that leader's configuration (with no leader, the
// union of the voters any node reports — both halves of a joint configuration
// count). Then:
//
//	healthy      the leader's term is followed by a majority of the voters
//	             (the leader counts itself), and every voter answered, follows
//	             the leader in its term, and trails it by at most MaxFollowerLag
//	degraded     a majority confirms the leader, but some voter is unreachable,
//	             follows another leader or term (stale), or lags
//	unavailable  no majority confirms a leader
//
// A voter the caller did not name at all counts as unreachable: health is
// never claimed for nodes nobody asked. The cluster's verdict is its worst
// group's — unavailable before degraded before healthy — and a probed node
// that is unreachable makes it at least degraded.
func Cluster(probes []Probe, o Options) ClusterHealth {
	out := ClusterHealth{Verdict: Healthy}
	type view struct {
		node string
		gs   multiraft.GroupStatus
	}
	byGroup := map[uint32][]view{}
	answered := map[string]bool{}
	for _, p := range probes {
		nr := Node(p, o)
		out.Nodes = append(out.Nodes, nr)
		if p.Status == nil || !p.Status.OK {
			continue
		}
		name := p.Node
		if p.Status.Node != "" {
			name = p.Status.Node
		}
		answered[name] = true
		for _, gs := range p.Status.Groups {
			byGroup[gs.Group] = append(byGroup[gs.Group], view{name, gs})
		}
	}
	var groups []uint32
	for g := range byGroup {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	worse := func(v string) {
		rank := map[string]int{Healthy: 0, Degraded: 1, Unavailable: 2}
		if rank[v] > rank[out.Verdict] {
			out.Verdict = v
		}
	}
	for _, g := range groups {
		vs := byGroup[g]
		var leader *view
		for i := range vs {
			if vs[i].gs.Role == "Leader" && !vs[i].gs.Removed && (leader == nil || vs[i].gs.Term > leader.gs.Term) {
				leader = &vs[i]
			}
		}
		voterSet := map[string]bool{}
		addVoters := func(cs multiraft.ConfStatus) {
			for _, m := range cs.Voters {
				voterSet[m.ID] = true
			}
			for _, m := range cs.Outgoing {
				voterSet[m.ID] = true
			}
		}
		if leader != nil {
			addVoters(leader.gs.Conf)
		} else {
			for _, v := range vs {
				addVoters(v.gs.Conf)
			}
		}
		var voters []string
		for id := range voterSet {
			voters = append(voters, id)
		}
		sort.Strings(voters)
		gh := GroupHealth{Group: g, Quorum: len(voters)/2 + 1}
		byNode := map[string]multiraft.GroupStatus{}
		for _, v := range vs {
			byNode[v.node] = v.gs
		}
		if leader != nil {
			gh.Leader, gh.Term, gh.Pending = leader.node, leader.gs.Term, leader.gs.ConfPending
		}
		allWell := true
		for _, id := range voters {
			vt := Voter{Node: id}
			gs, hosted := byNode[id]
			switch {
			case !answered[id]:
				vt.State = Unreachable
			case !hosted:
				vt.State = "not-hosting"
			default:
				vt.Term, vt.Leader = gs.Term, gs.Leader
				switch {
				case leader == nil:
					vt.State = "stale"
				case id == leader.node:
					vt.State = "leading"
				case gs.Leader == leader.node && gs.Term == leader.gs.Term:
					vt.State = "following"
				default:
					vt.State = "stale"
				}
			}
			if leader != nil && (vt.State == "leading" || vt.State == "following") {
				gh.Agreeing++
				if id != leader.node {
					if m, ok := leader.gs.FollowerMatch[id]; ok && leader.gs.LastIndex > m {
						vt.Lag = leader.gs.LastIndex - m
					}
					if vt.Lag > o.followerLag() {
						vt.State = "lagging"
					}
				}
			}
			if vt.State != "leading" && vt.State != "following" {
				allWell = false
				gh.Reasons = append(gh.Reasons, fmt.Sprintf("%s is %s", id, vt.State))
			}
			gh.Voters = append(gh.Voters, vt)
		}
		switch {
		case leader == nil:
			gh.Verdict = Unavailable
			gh.Reasons = append([]string{"no node reports leading"}, gh.Reasons...)
		case gh.Agreeing < gh.Quorum:
			gh.Verdict = Unavailable
			gh.Reasons = append([]string{fmt.Sprintf("%d of %d voters confirm %s in term %d; a quorum is %d", gh.Agreeing, len(voters), leader.node, leader.gs.Term, gh.Quorum)}, gh.Reasons...)
		case !allWell:
			gh.Verdict = Degraded
		default:
			gh.Verdict = Healthy
		}
		worse(gh.Verdict)
		out.Groups = append(out.Groups, gh)
	}
	anyAnswered := false
	for _, n := range out.Nodes {
		if n.Live {
			anyAnswered = true
		} else {
			worse(Degraded)
		}
	}
	if !anyAnswered {
		out.Verdict = Unreachable
	} else if len(out.Groups) == 0 {
		worse(Unavailable)
	}
	return out
}
