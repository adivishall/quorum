package lincheck

import "fmt"

// State is the sequential reference model's state for ONE key: a register that
// is either absent or holds a value. It is the Phase 1 storage contract
// (docs/INVARIANTS.md INV-A2, INV-A4, INV-A5) restated as a pure function:
//
//   - Put(v)  → present with value v; an empty value is a present key.
//   - Get     → the value if present, else NotFound.
//   - Delete  → absent; deleting an absent key succeeds (idempotent) and never
//     reports whether the key existed.
//
// Keys are opaque bytes compared bytewise; values are opaque bytes. There is no
// other operation and no state beyond this — which is exactly what makes the
// checker's per-key memoization effective.
type State struct {
	Present bool
	Value   string
}

// Initial is the state of a key nothing has written.
var Initial = State{}

// Step applies op to the state and reports whether the op's observed outcome and
// output are consistent with the model: the new state, and ok. For an op whose
// outcome is OK/NotFound the observation must match; an Incomplete op (no
// observation) is consistent by construction; a Rejected op never reaches Step.
func Step(s State, op Op) (State, bool) {
	switch op.Kind {
	case Put:
		if op.Outcome != OK && op.Outcome != Incomplete {
			return s, false
		}
		return State{Present: true, Value: string(op.Value)}, true
	case Delete:
		if op.Outcome != OK && op.Outcome != Incomplete {
			return s, false
		}
		return State{}, true
	case Get:
		switch op.Outcome {
		case Incomplete:
			return s, true
		case OK:
			return s, s.Present && s.Value == string(op.Output)
		case NotFound:
			return s, !s.Present
		}
	}
	return s, false
}

// Expected describes what the model would have returned for a Get in state s,
// for failure reports.
func Expected(s State) string {
	if !s.Present {
		return "notfound"
	}
	return fmt.Sprintf("%q", s.Value)
}

// Observed describes what a Get actually returned, for failure reports.
func Observed(op Op) string {
	switch op.Outcome {
	case OK:
		return fmt.Sprintf("%q", op.Output)
	case NotFound:
		return "notfound"
	}
	return op.Outcome.String()
}

// Sequential runs ops in the given order from Initial and returns the first op
// whose observation the model contradicts (index into ops, or -1). It is the
// reference-model differential check for histories that are known to be
// sequential.
func Sequential(ops []Op) (State, int) {
	s := Initial
	for i, op := range ops {
		if !op.Effective() {
			continue
		}
		var ok bool
		if s, ok = Step(s, op); !ok {
			return s, i
		}
	}
	return s, -1
}
