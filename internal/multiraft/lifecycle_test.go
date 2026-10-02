package multiraft

import (
	"errors"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
)

// The group lifecycle (audit H3): starts and stops of one group never overlap,
// so a group never has two drivers on its log, and OnGroup's attach and detach
// of a group strictly alternate.

// gate blocks a callback at an exact point: the FIRST call closes entered and
// returns only once open is closed; every later call passes straight through
// (so a lifecycle that wrongly runs a second start or stop beside the held one
// completes it, and the test fails on its result rather than hanging).
type gate struct {
	used    atomic.Bool
	entered chan struct{}
	open    chan struct{}
	opened  sync.Once
}

// newGate makes a gate that is opened, at the latest, when the test ends —
// before the host is closed (cleanups run last-registered first), so a test
// that fails while a transition is held fails at once instead of waiting in
// Close for the held transition.
func newGate(t *testing.T) *gate {
	g := &gate{entered: make(chan struct{}), open: make(chan struct{})}
	t.Cleanup(g.release)
	return g
}

func (g *gate) release() { g.opened.Do(func() { close(g.open) }) }

func (g *gate) block() {
	if g.used.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.open
	}
}

func lifecycleHost(t *testing.T, c *hostCluster, id NodeID, newSM func(GroupID) raftnode.StateMachine, onGroup func(GroupID, *raftnode.Node, raftnode.StateMachine)) *Host {
	t.Helper()
	h, err := Start(c.ctx, Config{
		ID: id, DataDir: c.dirs[id], Transport: c.trs[id],
		NewStateMachine: newSM, OnGroup: onGroup,
		TickInterval: 10 * time.Millisecond, DisableSync: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	c.hosts[id] = h
	return h
}

// TestAStartingGroupCannotBeStartedOrStoppedAgain: while one start of a group
// is in flight — reserved, its files possibly being opened — a second Create
// or Open of it is refused, not run beside it (before the reservation, both
// passed the existence check and the second's node replaced the first's in
// the registry, leaving it running unreachable on the same log), and so is a
// Stop. Once the first start is done the group is simply running.
func TestAStartingGroupCannotBeStartedOrStoppedAgain(t *testing.T) {
	c := newHostCluster(t, "n1")
	g := newGate(t)
	var mu sync.Mutex
	made := 0
	h := lifecycleHost(t, c, "n1", func(GroupID) raftnode.StateMachine {
		mu.Lock()
		made++
		mu.Unlock()
		g.block() // the first start waits here, its group reserved
		return &recSM{}
	}, nil)
	boot := c.genesis("n1")
	first := make(chan error, 1)
	go func() {
		_, err := h.Create(7, boot)
		first <- err
	}()
	<-g.entered
	if _, err := h.Create(7, boot); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("a second Create while the first is in flight: %v, want ErrGroupBusy", err)
	}
	if _, err := h.Open(7); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("an Open while a Create is in flight: %v, want ErrGroupBusy", err)
	}
	if err := h.Stop(7); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("a Stop while a Create is in flight: %v, want ErrGroupBusy", err)
	}
	g.release()
	if err := <-first; err != nil {
		t.Fatalf("the first Create: %v", err)
	}
	mu.Lock()
	if made != 1 {
		t.Fatalf("%d state machines were made for one group: a second start ran", made)
	}
	mu.Unlock()
	if _, err := h.Create(7, boot); !errors.Is(err, ErrGroupExists) {
		t.Fatalf("Create of a running group: %v, want ErrGroupExists", err)
	}
}

// TestAStoppingGroupCannotBeStartedUntilItsNodeIsClosed: a Stop takes the
// group out of the registry before its node is closed; before the reservation
// an Open in that window recovered the log — truncating a torn tail — under
// the still-running old node. Now it is refused until the old node is closed,
// and then it recovers the group's state.
func TestAStoppingGroupCannotBeStartedUntilItsNodeIsClosed(t *testing.T) {
	c := newHostCluster(t, "n1")
	g := newGate(t)
	var stopping sync.Mutex
	block := false
	h := lifecycleHost(t, c, "n1", func(GroupID) raftnode.StateMachine { return &recSM{} },
		func(_ GroupID, node *raftnode.Node, _ raftnode.StateMachine) {
			stopping.Lock()
			b := block
			stopping.Unlock()
			if node == nil && b {
				g.block() // the stop waits here: detached, its node not yet closed
			}
		})
	grp, err := h.Create(3, c.genesis("n1"))
	if err != nil {
		t.Fatal(err)
	}
	waitLeads(t, grp.Node)
	term := grp.Node.Status().Term
	stopping.Lock()
	block = true
	stopping.Unlock()
	stopped := make(chan error, 1)
	go func() { stopped <- h.Stop(3) }()
	<-g.entered
	select {
	case <-grp.Node.Done():
		t.Fatal("the premise failed: the old node closed before the stop was held")
	default:
	}
	if _, err := h.Open(3); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("an Open while the old node is still running: %v, want ErrGroupBusy", err)
	}
	if _, err := h.Create(3, c.genesis("n1")); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("a Create while the old node is still running: %v, want ErrGroupBusy", err)
	}
	g.release()
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	grp2, err := h.Open(3)
	if err != nil {
		t.Fatalf("Open after the stop: %v", err)
	}
	if st := grp2.Node.Status(); st.Term < term {
		t.Fatalf("the reopened group recovered term %d, below the %d it reached", st.Term, term)
	}
}

func waitLeads(t *testing.T, n *raftnode.Node) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().Commit > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the single-member group did not commit its no-op")
}

// TestConcurrentLifecycleOperations: many goroutines create, open and stop two
// groups at random (run under -race). For any interleaving: each group's
// attaches and detaches strictly alternate — never two attached nodes, the
// sign of two drivers on one log — and once the host is closed, every node
// that was ever attached has stopped: none was orphaned running.
func TestConcurrentLifecycleOperations(t *testing.T) {
	c := newHostCluster(t, "n1")
	var mu sync.Mutex
	attached := map[GroupID]*raftnode.Node{}
	var everyNode []*raftnode.Node
	var violations []string
	h := lifecycleHost(t, c, "n1", func(GroupID) raftnode.StateMachine { return &recSM{} },
		func(g GroupID, node *raftnode.Node, _ raftnode.StateMachine) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case node != nil && attached[g] != nil:
				violations = append(violations, "group attached twice: two nodes running it")
			case node == nil && attached[g] == nil:
				violations = append(violations, "group detached while not attached")
			}
			attached[g] = node
			if node != nil {
				everyNode = append(everyNode, node)
			}
		})
	boot := c.genesis("n1")
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 300; i++ {
				g := GroupID(1 + rng.Intn(2))
				var err error
				switch rng.Intn(3) {
				case 0:
					_, err = h.Create(g, boot)
				case 1:
					_, err = h.Open(g)
				default:
					err = h.Stop(g)
				}
				if err != nil && !errors.Is(err, ErrGroupBusy) && !errors.Is(err, ErrGroupExists) &&
					!errors.Is(err, ErrNoGroup) && !errors.Is(err, raftnode.ErrIdentity) {
					mu.Lock()
					violations = append(violations, "unexpected error: "+err.Error())
					mu.Unlock()
				}
			}
		}(int64(w))
	}
	wg.Wait()
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(violations) > 0 {
		t.Fatalf("%d violations, first: %s", len(violations), violations[0])
	}
	t.Logf("%d nodes attached over the run", len(everyNode))
	if len(everyNode) < 10 {
		t.Fatalf("only %d starts over the run: the interleavings were not exercised", len(everyNode))
	}
	for i, n := range everyNode {
		select {
		case <-n.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("node %d of %d still runs after Close: orphaned", i+1, len(everyNode))
		}
	}
}

// TestAFailedFirstStartLeavesNoDirectory: an Open of a group this node never
// had fails, and leaves no empty group directory behind to be reported as a
// failed group at every later start.
func TestAFailedFirstStartLeavesNoDirectory(t *testing.T) {
	c := newHostCluster(t, "n1")
	h := lifecycleHost(t, c, "n1", func(GroupID) raftnode.StateMachine { return &recSM{} }, nil)
	if _, err := h.Open(12345); err == nil {
		t.Fatal("Open of a group with no state succeeded")
	}
	if _, err := os.Stat(GroupDir(c.dirs["n1"], 12345)); !os.IsNotExist(err) {
		t.Fatalf("the failed start left its directory: %v", err)
	}
	if _, err := h.Create(12345, &replication.Configuration{}); err == nil {
		t.Fatal("Create with an empty genesis succeeded")
	}
}
