package fault

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/transport"
)

// recTransport is an inner transport that records what actually reached it.
type recTransport struct {
	id   transport.NodeID
	mu   sync.Mutex
	sent []sentMsg
	recv chan transport.Envelope
}

type sentMsg struct {
	peer    transport.NodeID
	kind    transport.MsgKind
	payload string
}

func newRec(id string) *recTransport {
	return &recTransport{id: transport.NodeID(id), recv: make(chan transport.Envelope)}
}

func (r *recTransport) Send(_ context.Context, peer transport.NodeID, kind transport.MsgKind, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, sentMsg{peer, kind, string(payload)})
	return nil
}
func (r *recTransport) Receive() <-chan transport.Envelope { return r.recv }
func (r *recTransport) LocalID() transport.NodeID          { return r.id }
func (r *recTransport) Close() error                       { return nil }
func (r *recTransport) got() []sentMsg {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sentMsg(nil), r.sent...)
}

func send(t *testing.T, tr transport.Transport, peer string, kind transport.MsgKind, p string) {
	t.Helper()
	if err := tr.Send(context.Background(), transport.NodeID(peer), kind, []byte(p)); err != nil {
		t.Fatalf("send: %v", err)
	}
}

// TestDropRuleByKindAndCount proves a Drop rule loses exactly the next Count
// messages of the selected kind on the selected link, and nothing else.
func TestDropRuleByKindAndCount(t *testing.T) {
	net := NewNetwork()
	inner := newRec("a")
	tr := net.Wrap(inner)
	net.AddRule(Rule{From: "a", To: "b", Kinds: []transport.MsgKind{transport.MsgAppendEntries}, Action: Drop, Count: 2})

	send(t, tr, "b", transport.MsgAppendEntries, "ae1")  // dropped
	send(t, tr, "b", transport.MsgRequestVote, "rv1")    // other kind: passes
	send(t, tr, "c", transport.MsgAppendEntries, "ae-c") // other link: passes
	send(t, tr, "b", transport.MsgAppendEntries, "ae2")  // dropped (count 2)
	send(t, tr, "b", transport.MsgAppendEntries, "ae3")  // rule spent: passes

	var got []string
	for _, m := range inner.got() {
		got = append(got, m.payload)
	}
	want := []string{"rv1", "ae-c", "ae3"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("delivered %v, want %v", got, want)
	}
	if s := net.Stats(); s.Dropped != 2 || s.Passed != 3 {
		t.Fatalf("stats %+v, want 2 dropped 3 passed", s)
	}
}

// TestPartitionIsDirectionalAndHeals proves a one-way block loses only that
// direction, isolation loses everything touching the node, and HealAll restores
// delivery.
func TestPartitionIsDirectionalAndHeals(t *testing.T) {
	net := NewNetwork()
	a, b := newRec("a"), newRec("b")
	ta, tb := net.Wrap(a), net.Wrap(b)

	net.Block("a", "b")
	send(t, ta, "b", transport.MsgProbe, "a->b") // lost
	send(t, tb, "a", transport.MsgProbe, "b->a") // other direction delivered
	if len(a.got()) != 0 || len(b.got()) != 1 {
		t.Fatalf("one-way block: a sent %v, b sent %v", a.got(), b.got())
	}

	net.HealAll()
	net.Isolate("b", []string{"a", "b", "c"})
	send(t, tb, "a", transport.MsgProbe, "lost1")
	send(t, tb, "c", transport.MsgProbe, "lost2")
	send(t, ta, "c", transport.MsgProbe, "a->c")
	if len(b.got()) != 1 || len(a.got()) != 1 {
		t.Fatalf("isolate b: a sent %v, b sent %v", a.got(), b.got())
	}

	net.HealAll()
	send(t, tb, "a", transport.MsgProbe, "healed")
	if got := b.got(); len(got) != 2 || got[1].payload != "healed" {
		t.Fatalf("after heal b sent %v", got)
	}
}

// TestDuplicateSendsExtraCopies proves a Duplicate rule delivers the message plus
// Copies identical copies.
func TestDuplicateSendsExtraCopies(t *testing.T) {
	net := NewNetwork()
	inner := newRec("a")
	tr := net.Wrap(inner)
	net.AddRule(Rule{Action: Duplicate, Copies: 2})
	send(t, tr, "b", transport.MsgRequestVoteResponse, "vote")
	if got := inner.got(); len(got) != 3 || got[0] != got[1] || got[1] != got[2] {
		t.Fatalf("delivered %v, want 3 identical copies", got)
	}
}

// TestDuplicationComposesWithTerminalRules proves the two-stage rule model: a
// global Duplicate rule and a Hold rule for one kind together hold every copy of
// that kind, while other kinds are still duplicated and delivered.
func TestDuplicationComposesWithTerminalRules(t *testing.T) {
	net := NewNetwork()
	inner := newRec("a")
	tr := net.Wrap(inner)
	net.AddRule(Rule{Action: Duplicate, Copies: 1})
	net.AddRule(Rule{Kinds: []transport.MsgKind{transport.MsgAppendEntries}, Action: Hold})
	send(t, tr, "b", transport.MsgAppendEntries, "ae")
	send(t, tr, "b", transport.MsgRequestVote, "rv")
	if got := inner.got(); len(got) != 2 || got[0].payload != "rv" || got[1].payload != "rv" {
		t.Fatalf("delivered %v, want both copies of rv and no ae", got)
	}
	if s := net.Stats(); s.Held != 2 || s.Duplicated != 2 {
		t.Fatalf("stats %+v, want 2 held (both ae copies) and 2 duplicates", s)
	}
	net.ClearRules()
	if n := net.Release(false); n != 2 {
		t.Fatalf("released %d, want the 2 held copies", n)
	}
}

// TestHoldAndReleaseReordered proves held messages are not delivered until
// Release, and a reversed release delivers them in the opposite order.
func TestHoldAndReleaseReordered(t *testing.T) {
	net := NewNetwork()
	inner := newRec("a")
	tr := net.Wrap(inner)
	id := net.AddRule(Rule{Action: Hold})
	for _, p := range []string{"1", "2", "3"} {
		send(t, tr, "b", transport.MsgAppendEntries, p)
	}
	if len(inner.got()) != 0 {
		t.Fatalf("held messages leaked: %v", inner.got())
	}
	net.RemoveRule(id)
	if n := net.Release(true); n != 3 {
		t.Fatalf("released %d, want 3", n)
	}
	got := inner.got()
	if len(got) != 3 || got[0].payload != "3" || got[2].payload != "1" {
		t.Fatalf("reversed release delivered %v, want 3,2,1", got)
	}
}

// TestBlockStallsSendUntilRemovedOrCancelled proves a Block rule makes Send block
// (a wedged peer) until the rule is removed — then the message is delivered — and
// that a blocked Send honours context cancellation.
func TestBlockStallsSendUntilRemovedOrCancelled(t *testing.T) {
	net := NewNetwork()
	inner := newRec("a")
	tr := net.Wrap(inner)
	id := net.AddRule(Rule{To: "b", Action: Block})

	done := make(chan error, 1)
	go func() { done <- tr.Send(context.Background(), "b", transport.MsgAppendEntries, []byte("stuck")) }()
	select {
	case err := <-done:
		t.Fatalf("Send returned (%v) while blocked", err)
	case <-time.After(100 * time.Millisecond):
	}
	send(t, tr, "c", transport.MsgAppendEntries, "other-peer") // other links are not blocked
	net.RemoveRule(id)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unblocked Send: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Send did not unblock after the rule was removed")
	}
	if got := inner.got(); len(got) != 2 || got[1].payload != "stuck" {
		t.Fatalf("delivered %v, want other-peer then stuck", got)
	}

	net.AddRule(Rule{To: "b", Action: Block})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- tr.Send(ctx, "b", transport.MsgAppendEntries, []byte("x")) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled blocked Send: %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked Send ignored cancellation")
	}
}
