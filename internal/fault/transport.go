package fault

import (
	"context"
	"sync"

	"github.com/adivishall/quorum/internal/transport"
)

// Action is what a matching Rule does to an outbound message.
type Action uint8

const (
	// Drop loses the message: Send reports success, as a network that accepted
	// the bytes and then lost them would.
	Drop Action = iota + 1
	// Duplicate sends the message and then Copies extra copies of it.
	Duplicate
	// Hold queues the message instead of sending it, until Release — a delay
	// whose length and release order the test controls (Release can reverse the
	// order, which is a reordering).
	Hold
	// Block makes Send itself block — a stuck TCP write to a peer that stopped
	// reading — until the rule is removed or the sender's context ends. It models
	// a slow or wedged peer, not a lost message.
	Block
)

func (a Action) String() string {
	switch a {
	case Drop:
		return "drop"
	case Duplicate:
		return "duplicate"
	case Hold:
		return "hold"
	case Block:
		return "block"
	default:
		return "action(?)"
	}
}

// Rule selects outbound messages by link and kind and applies an Action to them.
//
// Rules compose in two stages. First every live Duplicate rule that matches adds
// its copies, so a message becomes 1 + (sum of Copies) identical messages. Then
// each of those copies independently meets the partition and the terminal rules
// (Drop, Hold, Block), where the first matching live rule wins. So "duplicate
// everything" and "hold AppendEntries" together hold every copy of every
// AppendEntries.
type Rule struct {
	// From and To select the link; empty matches any node.
	From, To string
	// Kinds selects message kinds; empty matches every kind.
	Kinds []transport.MsgKind
	// Action is applied to each matching message.
	Action Action
	// Copies is the number of extra copies a Duplicate rule sends (>= 1).
	Copies int
	// Count limits the rule to the next Count matching messages, after which it
	// is spent — "drop the next 3 AppendEntries". 0 means unlimited. A Block rule
	// ignores Count: it blocks until removed.
	Count int
}

func (r *Rule) matches(from, to string, kind transport.MsgKind) bool {
	if (r.From != "" && r.From != from) || (r.To != "" && r.To != to) {
		return false
	}
	if len(r.Kinds) == 0 {
		return true
	}
	for _, k := range r.Kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// RuleID names an added rule so it can be removed.
type RuleID int

// NetStats counts what a Network did to the messages that crossed it.
type NetStats struct {
	Passed     int // copies forwarded to the inner transport
	Dropped    int // copies lost to a Drop rule or a blocked link
	Duplicated int // extra copies created by Duplicate rules
	Held       int // copies queued by a Hold rule
	Released   int // held copies later sent by Release
	Blocked    int // times a copy's Send blocked on a Block rule
}

// Network is an in-process fault controller shared by a set of real transports:
// wrap every node's transport with the same Network, and its partitions and
// rules then apply to the traffic between them. Faults are applied on the send
// side — a message on a blocked link, or matched by a Drop rule, never reaches
// the inner transport — so the real transports, framing and connections below
// are exercised unchanged for every message that does get through.
//
// Rule decisions are deterministic functions of the order in which messages are
// sent (first matching rule wins; Count consumes in send order), but in a
// multi-goroutine system that order is itself scheduled by the runtime, so a run
// is not replayable the way the single-goroutine simulator is. Tests built on it
// assert properties that must hold under ANY interleaving (docs/FAULTS.md).
type Network struct {
	mu      sync.Mutex
	links   Links
	rules   []ruleEntry
	nextID  RuleID
	held    []heldMsg
	stats   NetStats
	changed chan struct{} // closed and replaced on every rule/link change
}

type ruleEntry struct {
	id      RuleID
	rule    Rule
	applied int
}

type heldMsg struct {
	tr      transport.Transport
	peer    transport.NodeID
	kind    transport.MsgKind
	payload []byte
}

// NewNetwork returns a Network with no partitions and no rules.
func NewNetwork() *Network { return &Network{changed: make(chan struct{})} }

// notify wakes every sender blocked on a Block rule. Called with n.mu held.
func (n *Network) notify() {
	close(n.changed)
	n.changed = make(chan struct{})
}

// AddRule installs r after every existing rule and returns its id.
func (n *Network) AddRule(r Rule) RuleID {
	n.mu.Lock()
	defer n.mu.Unlock()
	if r.Action == Duplicate && r.Copies < 1 {
		r.Copies = 1
	}
	n.nextID++
	n.rules = append(n.rules, ruleEntry{id: n.nextID, rule: r})
	n.notify()
	return n.nextID
}

// RemoveRule removes a rule (a no-op if it is already gone), waking any sender it
// was blocking.
func (n *Network) RemoveRule(id RuleID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, e := range n.rules {
		if e.id == id {
			n.rules = append(n.rules[:i], n.rules[i+1:]...)
			break
		}
	}
	n.notify()
}

// ClearRules removes every rule.
func (n *Network) ClearRules() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rules = nil
	n.notify()
}

// Block cuts the one direction from -> to.
func (n *Network) Block(from, to string) { n.withLinks(func(l *Links) { l.Block(from, to) }) }

// Cut blocks both directions between a and b.
func (n *Network) Cut(a, b string) { n.withLinks(func(l *Links) { l.Cut(a, b) }) }

// Isolate partitions node from every other member.
func (n *Network) Isolate(node string, members []string) {
	n.withLinks(func(l *Links) { l.Isolate(node, members) })
}

// Split partitions group a from group b.
func (n *Network) Split(a, b []string) { n.withLinks(func(l *Links) { l.Split(a, b) }) }

// HealAll removes every partition (rules are untouched).
func (n *Network) HealAll() { n.withLinks(func(l *Links) { l.HealAll() }) }

func (n *Network) withLinks(f func(*Links)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	f(&n.links)
	n.notify()
}

// Release sends every held message through the transport that held it, in the
// order they were held — or reversed, which delivers a delayed batch out of
// order — and returns how many it sent. A message whose link is partitioned at
// release time is dropped. Send errors are ignored: a released message may be
// lost like any other.
func (n *Network) Release(reverse bool) int {
	n.mu.Lock()
	batch := n.held
	n.held = nil
	n.mu.Unlock()
	if reverse {
		for i, j := 0, len(batch)-1; i < j; i, j = i+1, j-1 {
			batch[i], batch[j] = batch[j], batch[i]
		}
	}
	sent := 0
	for _, h := range batch {
		n.mu.Lock()
		cut := n.links.Blocked(string(h.tr.LocalID()), string(h.peer))
		if cut {
			n.stats.Dropped++
		} else {
			n.stats.Released++
		}
		n.mu.Unlock()
		if cut {
			continue
		}
		_ = h.tr.Send(context.Background(), h.peer, h.kind, h.payload)
		sent++
	}
	return sent
}

// Stats returns a snapshot of the counters.
func (n *Network) Stats() NetStats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// Wrap returns tr decorated with this network's faults. Receive, LocalID and
// Close pass straight through to tr.
func (n *Network) Wrap(tr transport.Transport) transport.Transport {
	return &faultTransport{inner: tr, net: n}
}

// live reports whether a rule still applies (a Count-limited rule is spent after
// Count applications).
func (e *ruleEntry) live() bool { return e.rule.Count == 0 || e.applied < e.rule.Count }

// copiesFor applies the duplication stage: every live matching Duplicate rule
// contributes its copies. Called without n.mu.
func (n *Network) copiesFor(from, to string, kind transport.MsgKind) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	extra := 0
	for i := range n.rules {
		e := &n.rules[i]
		if e.rule.Action == Duplicate && e.rule.matches(from, to, kind) && e.live() {
			e.applied++
			extra += e.rule.Copies
		}
	}
	n.stats.Duplicated += extra
	return extra
}

type decision uint8

const (
	pass decision = iota
	lose
	hold
	block
)

// decide applies the partition and then the first matching live terminal rule to
// one copy of a message. For a block it also returns the channel to wait on.
// Called without n.mu.
func (n *Network) decide(from, to string, kind transport.MsgKind, payload []byte, tr transport.Transport, peer transport.NodeID) (decision, <-chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.links.Blocked(from, to) {
		n.stats.Dropped++
		return lose, nil
	}
	for i := range n.rules {
		e := &n.rules[i]
		if e.rule.Action == Duplicate || !e.rule.matches(from, to, kind) {
			continue
		}
		if e.rule.Action == Block {
			n.stats.Blocked++
			return block, n.changed
		}
		if !e.live() {
			continue
		}
		e.applied++
		switch e.rule.Action {
		case Drop:
			n.stats.Dropped++
			return lose, nil
		case Hold:
			n.stats.Held++
			n.held = append(n.held, heldMsg{tr: tr, peer: peer, kind: kind, payload: append([]byte(nil), payload...)})
			return hold, nil
		}
	}
	n.stats.Passed++
	return pass, nil
}

// faultTransport is a transport.Transport whose Send consults the Network.
type faultTransport struct {
	inner transport.Transport
	net   *Network
}

var _ transport.Transport = (*faultTransport)(nil)

// Send duplicates the message as the Duplicate rules say, then routes each copy
// through the partition and terminal rules. It returns the first copy's result;
// a lost or held copy reports success, as a real network would.
func (t *faultTransport) Send(ctx context.Context, peer transport.NodeID, kind transport.MsgKind, payload []byte) error {
	from := string(t.inner.LocalID())
	extra := t.net.copiesFor(from, string(peer), kind)
	first := t.sendOne(ctx, from, peer, kind, payload)
	for i := 0; i < extra; i++ {
		_ = t.sendOne(ctx, from, peer, kind, payload)
	}
	return first
}

func (t *faultTransport) sendOne(ctx context.Context, from string, peer transport.NodeID, kind transport.MsgKind, payload []byte) error {
	for {
		d, wait := t.net.decide(from, string(peer), kind, payload, t.inner, peer)
		switch d {
		case lose, hold:
			return nil
		case block:
			select {
			case <-wait: // a rule or link changed: decide again
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
			return t.inner.Send(ctx, peer, kind, payload)
		}
	}
}

func (t *faultTransport) Receive() <-chan transport.Envelope { return t.inner.Receive() }
func (t *faultTransport) LocalID() transport.NodeID          { return t.inner.LocalID() }
func (t *faultTransport) Close() error                       { return t.inner.Close() }
