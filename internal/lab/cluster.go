// Package lab runs reproducible experiments on real Quorum clusters (#3,
// docs/CLUSTER_BENCHMARKS.md): it launches real dkvd processes, drives them
// with a load.Config workload, acts on them while the load runs — kills a
// leader, restarts nodes, changes membership, takes snapshots — and records
// what the clients saw, what the nodes' metrics say, and how long each action
// took to settle.
//
// Nothing is simulated: every node is a dkvd process with its own data
// directory, talking TCP on loopback. Times are real; the workload's random
// choices are seeded.
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
	for _, n := range c.nodes {
		if !n.Spare {
			if err := c.launch(n, nil); err != nil {
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

// launch starts n's process with the cluster's flags (join, for a spare).
func (c *Cluster) launch(n *Node, join []multiraft.GroupID) error {
	var peers []string
	for _, o := range c.nodes {
		if o != n && !o.Spare {
			peers = append(peers, o.ID+"="+o.Addr)
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
	args = append(args, c.cfg.Extra...)
	cmd := exec.Command(c.cfg.Bin, args...)
	n.mu.Lock()
	if n.out == nil {
		n.out = &bytes.Buffer{}
	}
	w := &lockedWriter{mu: &n.mu, b: n.out}
	cmd.Stdout, cmd.Stderr = w, w
	if err := cmd.Start(); err != nil {
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
			return fmt.Errorf("lab: %s exited during startup:\n%s", n.ID, n.Output())
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
	return c.launch(n, nil)
}

// StartSpare starts a spare as a joiner of groups (it holds no configuration
// until a membership change adds it).
func (c *Cluster) StartSpare(id string, groups []multiraft.GroupID) error {
	n := c.byID[id]
	if !n.Spare {
		return fmt.Errorf("lab: %s is not a spare", id)
	}
	return c.launch(n, groups)
}

// Close kills every process.
func (c *Cluster) Close() {
	for _, n := range c.nodes {
		if n.Alive() {
			_ = c.Kill(n.ID)
		}
	}
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
