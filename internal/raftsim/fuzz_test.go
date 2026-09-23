package raftsim

import (
	"testing"
)

// FuzzFaultSchedule lets the Go fuzzer search for a fault schedule that breaks an
// invariant. Each 3-byte group of the input becomes one event that applies to the
// cluster's current state (see fuzzEvent); after at most maxFuzzEvents events the
// cluster is stabilized and must converge (INV-F3). Every continuous invariant runs
// after every event, and a violation fails with the exact script to replay.
//
//	go test ./internal/raftsim -run '^$' -fuzz FuzzFaultSchedule -fuzztime 60s
func FuzzFaultSchedule(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1})
	f.Add([]byte{9, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 11, 2, 0, 1, 0, 0, 0, 1, 0})
	f.Add([]byte{14, 1, 2, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 0, 11, 1, 0, 1, 0, 0, 1, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := New(Config{Nodes: 3, Seed: int64(len(data))})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i+2 < len(data) && i/3 < maxFuzzEvents && c.Violation() == nil; i += 3 {
			c.Apply(fuzzEvent(c, data[i], data[i+1], data[i+2]))
		}
		if c.Violation() == nil {
			c.Stabilize(400)
		}
		if v := c.Violation(); v != nil {
			t.Fatalf("%v\n--- script ---\n%s", v, FormatScript(c.Script()))
		}
	})
}

const maxFuzzEvents = 400

// fuzzEvent maps three bytes to an event that applies to the current state (so
// the fuzzer spends its effort on meaningful schedules): a message event picks an
// in-flight message, a node event picks a node.
func fuzzEvent(c *Cluster, k, a, b byte) Event {
	id := c.ids[int(a)%len(c.ids)]
	other := c.ids[int(b)%len(c.ids)]
	msg := func(kind Kind) Event {
		if len(c.flights) == 0 {
			return Event{Kind: Tick, Node: id}
		}
		f := c.flights[int(b)%len(c.flights)]
		pos := 0
		for _, g := range c.flights {
			if g == f {
				break
			}
			if g.msg.From == f.msg.From && g.msg.To == f.msg.To {
				pos++
			}
		}
		return Event{Kind: kind, From: f.msg.From, To: f.msg.To, Pos: pos, N: 1 + int(a)%50}
	}
	switch k % 16 {
	case 0, 1, 2:
		return Event{Kind: Tick, Node: id}
	case 3, 4, 5:
		return msg(Deliver)
	case 6:
		return msg(Drop)
	case 7:
		return msg(Duplicate)
	case 8:
		return msg(Delay)
	case 9:
		return Event{Kind: Propose, Node: id, Data: "f" + string(rune('a'+len(c.script)%26)) + itoa(len(c.script))}
	case 10:
		return Event{Kind: Block, From: id, To: other}
	case 11:
		return Event{Kind: HealAll}
	case 12:
		return Event{Kind: Crash, Node: id, Power: b%2 == 1, N: int(b) % 64}
	case 13:
		return Event{Kind: Restart, Node: id}
	case 14:
		return Event{Kind: FailPersist, Node: id, Op: PersistOp(1 + int(b)%3), N: 1 + int(b)%20}
	default:
		if c.Paused(id) {
			return Event{Kind: Resume, Node: id}
		}
		return Event{Kind: Pause, Node: id}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var d []byte
	for ; n > 0; n /= 10 {
		d = append([]byte{byte('0' + n%10)}, d...)
	}
	return string(d)
}
