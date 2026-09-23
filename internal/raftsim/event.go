package raftsim

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// Kind is an event type in a simulation script.
type Kind uint8

const (
	// Tick advances one node's logical clock by one tick.
	Tick Kind = iota + 1
	// Deliver hands one in-flight message to its recipient.
	Deliver
	// Drop removes one in-flight message without delivering it.
	Drop
	// Duplicate adds an identical copy of one in-flight message (sent last, so it
	// is also reordered behind everything already in flight).
	Duplicate
	// Delay makes one in-flight message undeliverable for N steps.
	Delay
	// Block cuts one direction From -> To (an asymmetric partition).
	Block
	// Unblock restores one direction From -> To.
	Unblock
	// Cut blocks both directions between From and To.
	Cut
	// Isolate cuts Node off from every other node.
	Isolate
	// HealAll removes every partition.
	HealAll
	// Crash kills Node's process. With Power, it also models a power loss on the
	// node's disk, keeping at most N bytes of each file's un-synced tail.
	Crash
	// Restart recovers a down node from its disk through the real startup path.
	Restart
	// Pause freezes a node (no ticks, no deliveries to it): a GC pause or SIGSTOP.
	Pause
	// Resume unfreezes a paused node.
	Resume
	// FailPersist arms a one-shot fault on Node's disk: its next Op fails (for a
	// short write, after N bytes).
	FailPersist
	// Disarm removes every unfired persistence fault on every node.
	Disarm
	// Release makes every delayed message deliverable now.
	Release
	// Propose submits command Data to Node.
	Propose
	// CheckConverged asserts post-fault convergence (INV-F3).
	CheckConverged
)

var kindNames = map[Kind]string{
	Tick: "tick", Deliver: "deliver", Drop: "drop", Duplicate: "dup", Delay: "delay",
	Block: "block", Unblock: "unblock", Cut: "cut", Isolate: "isolate", HealAll: "healall",
	Crash: "crash", Restart: "restart", Pause: "pause", Resume: "resume",
	FailPersist: "failpersist", Disarm: "disarm", Release: "release", Propose: "propose",
	CheckConverged: "check-converged",
}

func (k Kind) String() string {
	if s, ok := kindNames[k]; ok {
		return s
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// PersistOp is the disk operation a FailPersist event makes fail.
type PersistOp uint8

const (
	// FailWrite fails the next write outright (nothing reaches the file).
	FailWrite PersistOp = iota + 1
	// ShortWrite writes only N bytes of the next write, then fails: a torn record.
	ShortWrite
	// FailSync fails the next fsync (the written bytes stay cached, not durable).
	FailSync
)

var persistNames = map[PersistOp]string{FailWrite: "write", ShortWrite: "short", FailSync: "sync"}

func (o PersistOp) String() string { return persistNames[o] }

// Event is one step of a simulation script. Which fields matter depends on Kind:
//
//	Tick, Isolate, Restart, Pause, Resume     Node
//	Deliver, Drop, Duplicate                  From, To, Pos
//	Delay                                     From, To, Pos, N (steps)
//	Block, Unblock, Cut                       From, To
//	Crash                                     Node, Power, N (torn bytes kept)
//	FailPersist                               Node, Op, N (short-write length)
//	Propose                                   Node, Data
//
// A message is addressed by its link and its position among the messages
// currently in flight on that link, oldest first (Pos 0 = the oldest). Addressing
// by link rather than by a global sequence number keeps a script meaningful when a
// minimizer deletes earlier events: the event still names "the k-th message
// in flight from a to b", and if no such message exists the event is skipped.
type Event struct {
	Kind  Kind
	Node  NodeID
	From  NodeID
	To    NodeID
	Pos   int
	N     int
	Power bool
	Op    PersistOp
	Data  string
}

// String renders an event in the script syntax ParseEvent reads back.
func (e Event) String() string {
	switch e.Kind {
	case Tick, Isolate, Restart, Pause, Resume:
		return fmt.Sprintf("%s %s", e.Kind, e.Node)
	case Deliver, Drop, Duplicate:
		return fmt.Sprintf("%s %s %s %d", e.Kind, e.From, e.To, e.Pos)
	case Delay:
		return fmt.Sprintf("%s %s %s %d %d", e.Kind, e.From, e.To, e.Pos, e.N)
	case Block, Unblock, Cut:
		return fmt.Sprintf("%s %s %s", e.Kind, e.From, e.To)
	case Crash:
		if e.Power {
			return fmt.Sprintf("crash %s power %d", e.Node, e.N)
		}
		return fmt.Sprintf("crash %s process", e.Node)
	case FailPersist:
		if e.Op == ShortWrite {
			return fmt.Sprintf("failpersist %s short %d", e.Node, e.N)
		}
		return fmt.Sprintf("failpersist %s %s", e.Node, e.Op)
	case Propose:
		return fmt.Sprintf("propose %s %s", e.Node, e.Data)
	default:
		return e.Kind.String()
	}
}

// ParseEvent parses one line of the script syntax produced by Event.String.
func ParseEvent(line string) (Event, error) {
	f := strings.Fields(line)
	if len(f) == 0 {
		return Event{}, fmt.Errorf("raftsim: empty event")
	}
	var k Kind
	for kk, name := range kindNames {
		if name == f[0] {
			k = kk
		}
	}
	if k == 0 {
		return Event{}, fmt.Errorf("raftsim: unknown event %q", f[0])
	}
	e := Event{Kind: k}
	need := func(n int) error {
		if len(f) != n {
			return fmt.Errorf("raftsim: %q wants %d fields, has %d", line, n, len(f))
		}
		return nil
	}
	atoi := func(s string) (int, error) {
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("raftsim: bad number %q in %q", s, line)
		}
		return v, nil
	}
	var err error
	switch k {
	case Tick, Isolate, Restart, Pause, Resume:
		if err = need(2); err == nil {
			e.Node = NodeID(f[1])
		}
	case Deliver, Drop, Duplicate:
		if err = need(4); err == nil {
			e.From, e.To = NodeID(f[1]), NodeID(f[2])
			e.Pos, err = atoi(f[3])
		}
	case Delay:
		if err = need(5); err == nil {
			e.From, e.To = NodeID(f[1]), NodeID(f[2])
			if e.Pos, err = atoi(f[3]); err == nil {
				e.N, err = atoi(f[4])
			}
		}
	case Block, Unblock, Cut:
		if err = need(3); err == nil {
			e.From, e.To = NodeID(f[1]), NodeID(f[2])
		}
	case Crash:
		switch {
		case len(f) == 3 && f[2] == "process":
			e.Node = NodeID(f[1])
		case len(f) == 4 && f[2] == "power":
			e.Node, e.Power = NodeID(f[1]), true
			e.N, err = atoi(f[3])
		default:
			err = fmt.Errorf("raftsim: bad crash %q", line)
		}
	case FailPersist:
		switch {
		case len(f) == 3 && f[2] == "write":
			e.Node, e.Op = NodeID(f[1]), FailWrite
		case len(f) == 3 && f[2] == "sync":
			e.Node, e.Op = NodeID(f[1]), FailSync
		case len(f) == 4 && f[2] == "short":
			e.Node, e.Op = NodeID(f[1]), ShortWrite
			e.N, err = atoi(f[3])
		default:
			err = fmt.Errorf("raftsim: bad failpersist %q", line)
		}
	case Propose:
		if err = need(3); err == nil {
			e.Node, e.Data = NodeID(f[1]), f[2]
		}
	default:
		err = need(1)
	}
	if err != nil {
		return Event{}, err
	}
	return e, nil
}

// FormatScript renders a script, one event per line.
func FormatScript(events []Event) string {
	var b strings.Builder
	for _, e := range events {
		b.WriteString(e.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// ParseScript parses a script; blank lines and lines starting with '#' are ignored.
func ParseScript(text string) ([]Event, error) {
	var out []Event
	sc := bufio.NewScanner(strings.NewReader(text))
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		e, err := ParseEvent(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}
