// Package lab runs reproducible experiments on real Quorum clusters (#3,
// docs/CLUSTER_BENCHMARKS.md): it launches real dkvd processes, drives them
// with a load.Config workload, acts on them while the load runs — kills a
// leader, restarts nodes, changes membership, takes snapshots — and records
// what the clients saw, what the nodes' metrics say, and how long each action
// took to settle.
//
// Nothing is simulated: every node is a dkvd process with its own data
// directory, talking TCP on loopback. Times are real; the workload's random
// choices are seeded. With ClusterConfig.Links every node-to-node link runs
// through a proxy (internal/netproxy), so a run can partition the cluster;
// Chaos (chaos.go) runs a seeded fault schedule against it under a recorded,
// linearizability-checked workload.
package lab

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/netproxy"
	"github.com/adivishall/quorum/internal/nodedir"
	"github.com/adivishall/quorum/internal/routing"
)

// ClusterConfig describes a cluster to launch.
type ClusterConfig struct {
	Bin      string `json:"-"`    // the dkvd binary
	Mode     string `json:"mode"` // "raft" (one group) or "cluster" (one group per shard)
	Nodes    int    `json:"nodes"`
	Spares   int    `json:"spares"` // extra nodes, not in the genesis, started only by a scenario
	Shards   int    `json:"shards"` // cluster mode
	RF       int    `json:"rf"`     // cluster mode
	DataRoot string `json:"-"`
	// Tick is dkvd's -tick-interval; SnapshotEvery its -snapshot-every (0
	// keeps dkvd's default, 10,000 entries). dkvd always fsyncs its log.
	Tick          time.Duration `json:"tick"`
	SnapshotEvery uint64        `json:"snapshot_every"`
	PortBase      int           `json:"-"`
	Extra         []string      `json:"extra,omitempty"`
	// StateMachine is dkvd -state-machine for every node: "" (dkvd's default,
	// memory) or "lsm" (S2, docs/STORAGE_INTEGRATION.md §8).
	StateMachine string `json:"state_machine,omitempty"`
	// Links routes every link between genesis nodes through a proxy
	// (internal/netproxy) on the dialing side, so the run can partition the
	// cluster (Cut, Isolate, Heal, HealAll). Off by default: the proxy adds a
	// loopback hop, and the baseline measurements were taken without one. A
	// spare's links are direct — its address enters the group configuration,
	// which every dialer shares — so partitions cover the genesis nodes only.
	Links bool `json:"links,omitempty"`
	// Start starts a node's process, a restart's included; nil is
	// (*exec.Cmd).Start. A test passes its own launcher, so the lab's
	// processes are race-scanned and die with the test like every other
	// process it starts.
	Start func(*exec.Cmd) error `json:"-"`
}

// Node is one dkvd process slot.
type Node struct {
	ID          string
	Addr        string // transport
	ClientAddr  string
	AdminAddr   string
	MetricsAddr string
	Dir         string
	Spare       bool
	// join are the groups a spare was started to join: every later start of
	// it passes them again, as dkvd requires of a joiner (its data directory
	// records it joined, not that it was a genesis member).
	join []multiraft.GroupID

	mu   sync.Mutex
	cmd  *exec.Cmd
	out  *bytes.Buffer
	done chan struct{}
}

// Alive reports whether the node's process is running.
func (n *Node) Alive() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil {
		return false
	}
	select {
	case <-n.done:
		return false
	default:
		return true
	}
}

// Output returns what the node's processes wrote so far.
func (n *Node) Output() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.out == nil {
		return ""
	}
	return n.out.String()
}

// Cluster is a running set of dkvd processes.
type Cluster struct {
	cfg   ClusterConfig
	nodes []*Node // genesis first, then spares
	byID  map[string]*Node
	route *multiraft.Assignment
	// links holds one proxy per genesis pair, keyed (dialer, accepter): the
	// transport's smaller id dials (ADR-014), so the dialer's peer address
	// for the accepter is the proxy, and the accepter's for the dialer is a
	// real address it never dials.
	links map[[2]string]*netproxy.Proxy
}

// Build compiles dkvd from the repository at repo into dir and returns its
// path.
func Build(repo, dir string) (string, error) {
	bin := filepath.Join(dir, "dkvd")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/dkvd")
	cmd.Dir = repo
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("lab: building dkvd: %v\n%s", err, out)
	}
	return bin, nil
}

// freePorts returns n ports from base upward that can be bound now. The range
// sits below every common ephemeral range, so the kernel does not hand them to
// outgoing connections between this check and dkvd binding them.
func freePorts(base, n int) ([]int, error) {
	var out []int
	for p := base; p < base+4000 && len(out) < n; p++ {
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(p))
		if err != nil {
			continue
		}
		_ = ln.Close()
		out = append(out, p)
	}
	if len(out) < n {
		return nil, fmt.Errorf("lab: %d free ports from %d, need %d", len(out), base, n)
	}
	return out, nil
}

// Start launches the genesis nodes and waits until every group has a leader.
func Start(ctx context.Context, cfg ClusterConfig) (*Cluster, error) {
	if cfg.Nodes < 1 {
		return nil, errors.New("lab: at least one node")
	}
	if cfg.Mode == "" {
		cfg.Mode = "raft"
	}
	if cfg.Mode != "raft" && cfg.Mode != "cluster" {
		return nil, fmt.Errorf("lab: unknown mode %q", cfg.Mode)
	}
	if cfg.Tick == 0 {
		cfg.Tick = 50 * time.Millisecond
	}
	if cfg.PortBase == 0 {
		cfg.PortBase = 31000
	}
	if cfg.Mode == "cluster" {
		if cfg.Shards <= 0 {
			cfg.Shards = 4
		}
		if cfg.RF <= 0 || cfg.RF > cfg.Nodes {
			cfg.RF = min(3, cfg.Nodes)
		}
	}
	total := cfg.Nodes + cfg.Spares
	ports, err := freePorts(cfg.PortBase, 4*total)
	if err != nil {
		return nil, err
	}
	c := &Cluster{cfg: cfg, byID: map[string]*Node{}}
	var genesis []routing.NodeID
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("n%d", i+1)
		n := &Node{ID: id, Addr: addr(ports[4*i]), ClientAddr: addr(ports[4*i+1]), AdminAddr: addr(ports[4*i+2]),
			MetricsAddr: addr(ports[4*i+3]), Dir: filepath.Join(cfg.DataRoot, id), Spare: i >= cfg.Nodes}
		c.nodes = append(c.nodes, n)
		c.byID[id] = n
		if !n.Spare {
			genesis = append(genesis, routing.NodeID(id))
		}
	}
	if cfg.Mode == "cluster" {
		if c.route, err = multiraft.NewAssignment(routing.Config{ShardCount: cfg.Shards, ReplicationFactor: cfg.RF, Nodes: genesis}); err != nil {
			return nil, err
		}
	}
	if cfg.Links {
		c.links = map[[2]string]*netproxy.Proxy{}
		for _, a := range c.nodes {
			for _, b := range c.nodes {
				if a.Spare || b.Spare || a.ID >= b.ID {
					continue
				}
				p, err := netproxy.Start(b.Addr)
				if err != nil {
					c.Close()
					return nil, err
				}
				c.links[[2]string{a.ID, b.ID}] = p
			}
		}
	}
	for _, n := range c.nodes {
		if !n.Spare {
			if err := c.launch(n); err != nil {
				c.Close()
				return nil, err
			}
		}
	}
	for _, g := range c.Groups() {
		if _, _, err := c.WaitLeader(ctx, g, "", 30*time.Second); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func addr(p int) string { return "127.0.0.1:" + strconv.Itoa(p) }

// Groups are the cluster's Raft groups.
func (c *Cluster) Groups() []multiraft.GroupID {
	if c.route == nil {
		return []multiraft.GroupID{0}
	}
	return c.route.Groups()
}

// Route is the cluster's key → group routing (nil in raft mode).
func (c *Cluster) Route() *multiraft.Assignment { return c.route }

// Node returns the node named id.
func (c *Cluster) Node(id string) *Node { return c.byID[id] }

// Nodes returns every node slot, genesis first.
func (c *Cluster) Nodes() []*Node { return append([]*Node(nil), c.nodes...) }

// Endpoints are the running genesis nodes' client ports, for a load run.
func (c *Cluster) Endpoints() []load.Endpoint {
	var out []load.Endpoint
	for _, n := range c.nodes {
		if !n.Spare {
			out = append(out, load.Endpoint{Name: n.ID, Addr: n.ClientAddr})
		}
	}
	return out
}

// launch starts n's process with the cluster's flags (and, for a spare it
// started, -join).
func (c *Cluster) launch(n *Node) error {
	join := n.join
	var peers []string
	for _, o := range c.nodes {
		if o != n && !o.Spare {
			peers = append(peers, o.ID+"="+c.peerAddr(n.ID, o))
		}
	}
	args := []string{"-id", n.ID, "-listen", n.Addr, "-data-dir", n.Dir, "-tick-interval", c.cfg.Tick.String(),
		"-client-listen", n.ClientAddr, "-admin-listen", n.AdminAddr, "-metrics-listen", n.MetricsAddr}
	if len(peers) > 0 {
		args = append(args, "-peers", strings.Join(peers, ","))
	}
	if c.cfg.Mode == "raft" {
		args = append(args, "-raft")
	} else {
		var ids []string
		for _, o := range c.nodes {
			if !o.Spare {
				ids = append(ids, o.ID)
			}
		}
		args = append(args, "-cluster", "-shards", strconv.Itoa(c.cfg.Shards), "-rf", strconv.Itoa(c.cfg.RF), "-nodes", strings.Join(ids, ","))
	}
	if c.cfg.SnapshotEvery > 0 {
		args = append(args, "-snapshot-every", strconv.FormatUint(c.cfg.SnapshotEvery, 10))
	}
	if len(join) > 0 {
		var gs []string
		for _, g := range join {
			gs = append(gs, strconv.FormatUint(uint64(g), 10))
		}
		args = append(args, "-join", strings.Join(gs, ","))
	}
	// A node's first start initializes its data directory (internal/nodedir):
	// -init with the cluster's id, once; a restart passes neither.
	if _, err := os.Stat(filepath.Join(n.Dir, nodedir.IdentityFile)); errors.Is(err, fs.ErrNotExist) {
		args = append(args, "-init", "-cluster-id", "lab")
	}
	if c.cfg.StateMachine != "" {
		args = append(args, "-state-machine", c.cfg.StateMachine)
	}
	args = append(args, c.cfg.Extra...)
	cmd := exec.Command(c.cfg.Bin, args...)
	n.mu.Lock()
	if n.out == nil {
		n.out = &bytes.Buffer{}
	}
	w := &lockedWriter{mu: &n.mu, b: n.out}
	cmd.Stdout, cmd.Stderr = w, w
	start := c.cfg.Start
	if start == nil {
		start = (*exec.Cmd).Start
	}
	if err := start(cmd); err != nil {
		n.mu.Unlock()
		return err
	}
	done := make(chan struct{})
	n.cmd, n.done = cmd, done
	n.mu.Unlock()
	go func() { _ = cmd.Wait(); close(done) }()
	// Ready once the admin port answers.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := multiraft.AdminCall(ctx, n.AdminAddr, multiraft.AdminRequest{Op: "status"})
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-done:
			return fmt.Errorf("lab: %s exited during startup: %s", n.ID, lastLines(n.Output(), 3))
		case <-time.After(20 * time.Millisecond):
		}
	}
	return fmt.Errorf("lab: %s's admin port never answered", n.ID)
}

// lockedWriter appends to a buffer under the node's lock.
type lockedWriter struct {
	mu *sync.Mutex
	b  *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

// Kill SIGKILLs a node and waits for it to die: no shutdown path runs.
func (c *Cluster) Kill(id string) error { return c.signal(id, syscall.SIGKILL) }

// Stop SIGTERMs a node and waits for its clean shutdown.
func (c *Cluster) Stop(id string) error { return c.signal(id, syscall.SIGTERM) }

func (c *Cluster) signal(id string, sig syscall.Signal) error {
	n := c.byID[id]
	n.mu.Lock()
	cmd, done := n.cmd, n.done
	n.mu.Unlock()
	if cmd == nil {
		return fmt.Errorf("lab: %s is not running", id)
	}
	if err := cmd.Process.Signal(sig); err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("lab: %s did not exit after %v", id, sig)
	}
}

// Restart starts a stopped node again on its data directory.
func (c *Cluster) Restart(id string) error {
	n := c.byID[id]
	if n.Alive() {
		return fmt.Errorf("lab: %s is running", id)
	}
	return c.launch(n)
}

// StartSpare starts a spare as a joiner of groups (it holds no configuration
// until a membership change adds it).
func (c *Cluster) StartSpare(id string, groups []multiraft.GroupID) error {
	n := c.byID[id]
	if !n.Spare {
		return fmt.Errorf("lab: %s is not a spare", id)
	}
	n.join = append([]multiraft.GroupID(nil), groups...)
	return c.launch(n)
}

// lastLines returns the last n lines of s, joined by " | ": what a startup
// error needs to show, where the whole output belongs in the node's log.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

// Close kills every process, then closes every link proxy. It is safe to
// call more than once, and on a cluster Start failed to finish.
func (c *Cluster) Close() {
	for _, n := range c.nodes {
		if n.Alive() {
			_ = c.Kill(n.ID)
		}
	}
	for _, p := range c.links {
		p.Close()
	}
}

// peerAddr is the address node from dials to reach o: the link's proxy when
// from is the dialing side of a proxied link, o's own address otherwise.
func (c *Cluster) peerAddr(from string, o *Node) string {
	if p, ok := c.links[[2]string{from, o.ID}]; ok {
		return p.Addr()
	}
	return o.Addr
}

// link returns the proxy on the link between a and b, in either order.
func (c *Cluster) link(a, b string) (*netproxy.Proxy, error) {
	if c.links == nil {
		return nil, errors.New("lab: the cluster was started without Links: it cannot be partitioned")
	}
	if b < a {
		a, b = b, a
	}
	p, ok := c.links[[2]string{a, b}]
	if !ok {
		return nil, fmt.Errorf("lab: no proxied link between %s and %s (partitions cover genesis nodes only)", a, b)
	}
	return p, nil
}

// Cut severs the link between a and b: both sides see their connection die,
// and new connections are refused until Heal. The nodes keep running and
// keep serving their clients — only the node-to-node link is cut.
func (c *Cluster) Cut(a, b string) error {
	p, err := c.link(a, b)
	if err != nil {
		return err
	}
	p.Cut()
	return nil
}

// Heal restores the link between a and b; the dialer reconnects on its next
// retry (the transport's DialRetryInterval).
func (c *Cluster) Heal(a, b string) error {
	p, err := c.link(a, b)
	if err != nil {
		return err
	}
	p.Heal()
	return nil
}

// Isolate cuts every link between id and the other genesis nodes.
func (c *Cluster) Isolate(id string) error {
	if c.links == nil {
		return errors.New("lab: the cluster was started without Links: it cannot be partitioned")
	}
	if n, ok := c.byID[id]; !ok || n.Spare {
		return fmt.Errorf("lab: %s is not a genesis node", id)
	}
	for k, p := range c.links {
		if k[0] == id || k[1] == id {
			p.Cut()
		}
	}
	return nil
}

// HealAll restores every link.
func (c *Cluster) HealAll() {
	for _, p := range c.links {
		p.Heal()
	}
}

// Cuts lists the links that are cut now, each as (smaller id, larger id),
// sorted.
func (c *Cluster) Cuts() [][2]string {
	var out [][2]string
	for k, p := range c.links {
		if p.IsCut() {
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}

// Pause freezes node id's process (SIGSTOP): it holds its connections and its
// state but answers nothing, as a node in a long GC pause or a stalled VM
// would. Resume (SIGCONT) lets it continue.
func (c *Cluster) Pause(id string) error { return c.send(id, syscall.SIGSTOP) }

// Resume continues a paused node (SIGCONT).
func (c *Cluster) Resume(id string) error { return c.send(id, syscall.SIGCONT) }

// send delivers sig to node id's process without waiting for it to exit.
func (c *Cluster) send(id string, sig syscall.Signal) error {
	n, ok := c.byID[id]
	if !ok {
		return fmt.Errorf("lab: no node %s", id)
	}
	n.mu.Lock()
	cmd := n.cmd
	n.mu.Unlock()
	if cmd == nil || !n.Alive() {
		return fmt.Errorf("lab: %s is not running", id)
	}
	return cmd.Process.Signal(sig)
}

// Admin sends one admin request to node id.
func (c *Cluster) Admin(ctx context.Context, id string, req multiraft.AdminRequest) (multiraft.AdminResponse, error) {
	return multiraft.AdminCall(ctx, c.byID[id].AdminAddr, req)
}

// Leader returns the node that leads group g in the highest term any running
// node reports a leader for, and that term.
func (c *Cluster) Leader(ctx context.Context, g multiraft.GroupID) (string, uint64, error) {
	var leader string
	var term uint64
	for _, n := range c.nodes {
		if !n.Alive() {
			continue
		}
		actx, cancel := context.WithTimeout(ctx, time.Second)
		resp, err := c.Admin(actx, n.ID, multiraft.AdminRequest{Op: "status"})
		cancel()
		if err != nil {
			continue
		}
		for _, gs := range resp.Groups {
			if multiraft.GroupID(gs.Group) == g && strings.EqualFold(gs.Role, "leader") && gs.Term >= term {
				leader, term = n.ID, gs.Term
			}
		}
	}
	if leader == "" {
		return "", 0, fmt.Errorf("lab: no leader of group %d", g)
	}
	return leader, term, nil
}

// WaitLeader polls every 10 ms until some running node other than exclude
// leads group g in a term above after (0: any), and returns it with the time
// it took.
func (c *Cluster) WaitLeader(ctx context.Context, g multiraft.GroupID, exclude string, timeout time.Duration) (string, time.Duration, error) {
	return c.waitLeaderAfter(ctx, g, exclude, 0, timeout)
}

func (c *Cluster) waitLeaderAfter(ctx context.Context, g multiraft.GroupID, exclude string, after uint64, timeout time.Duration) (string, time.Duration, error) {
	start := time.Now()
	for time.Since(start) < timeout {
		if id, term, err := c.Leader(ctx, g); err == nil && id != exclude && term > after {
			return id, time.Since(start), nil
		}
		select {
		case <-ctx.Done():
			return "", 0, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return "", 0, fmt.Errorf("lab: no new leader of group %d within %s", g, timeout)
}

// Scrape reads node id's /metrics.
func (c *Cluster) Scrape(id string) (metrics.Samples, error) {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + c.byID[id].MetricsAddr + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lab: %s /metrics: %s", id, resp.Status)
	}
	return metrics.Parse(bufio.NewReader(resp.Body))
}

// ScrapeAll reads every running node's /metrics.
func (c *Cluster) ScrapeAll() map[string]metrics.Samples {
	out := map[string]metrics.Samples{}
	for _, n := range c.nodes {
		if n.Alive() {
			if ss, err := c.Scrape(n.ID); err == nil {
				out[n.ID] = ss
			}
		}
	}
	return out
}

// running returns the running nodes' ids, sorted.
func (c *Cluster) running() []string {
	var out []string
	for _, n := range c.nodes {
		if n.Alive() {
			out = append(out, n.ID)
		}
	}
	sort.Strings(out)
	return out
}
