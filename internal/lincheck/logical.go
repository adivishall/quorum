package lincheck

import (
	"fmt"
	"math"
	"sort"
)

// Logical returns the history of LOGICAL operations (Phase 13,
// docs/LINEARIZABILITY.md §15): every group of identified writes — PUTs and
// DELETEs with the same non-zero (ClientID, RequestID) — becomes one operation,
// because under the request-identity contract (docs/CLIENT_SEMANTICS.md §4) they
// are sends of one logical request that changes state at most once. Reads are
// never merged (a read is not deduplicated: each one really executes), and
// anonymous ops are left alone.
//
// A group's command is the command of its OK ops — there may be only one:
// two different commands both acknowledged under one identity means the server
// accepted a conflicting reuse, which is reported as an error, never checked
// around. With no OK op, it is the command of its unanswered ops, which must
// likewise agree (otherwise which of them might have taken effect is ambiguous,
// and the history is refused as unsupported rather than guessed at). Ops of the
// group carrying any other command are excluded: they were refused, or — when
// the group's command was acknowledged — cannot have taken effect, since the
// acknowledged one owns the identity.
//
// The merged operation took effect at most once, after the FIRST send of its
// command and before the FIRST acknowledgement of it: it is invoked at the
// earliest invocation among the sends of its command, completes at the earliest
// OK among them, and is OK if any send was acknowledged, Incomplete (optional)
// if any was unanswered, and Rejected (excluded) if every send was refused.
// Every send's attempts are kept on it as metadata.
func (h History) Logical() (History, error) {
	type ident struct{ c, r uint64 }
	groups := map[ident][]Op{}
	var out History
	for _, op := range h.Ops {
		if op.ClientID == 0 || op.Kind == Get {
			out.Ops = append(out.Ops, op)
			continue
		}
		k := ident{op.ClientID, op.RequestID}
		groups[k] = append(groups[k], op)
	}
	idents := make([]ident, 0, len(groups))
	for k := range groups {
		idents = append(idents, k)
	}
	sort.Slice(idents, func(i, j int) bool {
		if idents[i].c != idents[j].c {
			return idents[i].c < idents[j].c
		}
		return idents[i].r < idents[j].r
	})
	for _, k := range idents {
		ops := groups[k]
		if len(ops) == 1 {
			out.Ops = append(out.Ops, ops[0])
			continue
		}
		merged, err := mergeGroup(ops)
		if err != nil {
			return History{}, &IdentityError{ClientID: k.c, RequestID: k.r, Ops: ops, Reason: err.Error()}
		}
		out.Ops = append(out.Ops, merged)
	}
	sort.Slice(out.Ops, func(i, j int) bool { return out.Ops[i].ID < out.Ops[j].ID })
	return out, nil
}

// IdentityError is a history that breaks the request-identity contract; Ops
// are the sends of the offending request.
type IdentityError struct {
	ClientID, RequestID uint64
	Ops                 []Op
	Reason              string
}

func (e *IdentityError) Error() string {
	return fmt.Sprintf("request (cid=%d, rid=%d): %s", e.ClientID, e.RequestID, e.Reason)
}

// command is what makes two sends the same request.
type command struct {
	kind  Kind
	key   string
	value string
}

func commandOf(op Op) command { return command{op.Kind, op.Key, string(op.Value)} }

func mergeGroup(ops []Op) (Op, error) {
	var owner *command
	for _, op := range ops {
		if op.Outcome != OK {
			continue
		}
		c := commandOf(op)
		if owner != nil && *owner != c {
			return Op{}, fmt.Errorf("two different commands were both acknowledged (ops %d and another): a conflicting reuse was accepted", op.ID)
		}
		owner = &c
	}
	if owner == nil {
		for _, op := range ops {
			if op.Outcome != Incomplete {
				continue
			}
			c := commandOf(op)
			if owner != nil && *owner != c {
				return Op{}, fmt.Errorf("unanswered sends carry different commands (op %d): which one may have taken effect is ambiguous — unsupported", op.ID)
			}
			owner = &c
		}
	}
	if owner == nil {
		c := commandOf(ops[0]) // every send was refused: any member stands for the group
		owner = &c
	}
	var m Op
	m.ID = math.MaxInt
	m.Invoke, m.Complete = math.MaxInt64, math.MaxInt64
	anyOK, anyIncomplete := false, false
	var firstRejectedComplete int64 = math.MaxInt64
	for _, op := range ops {
		if commandOf(op) != *owner {
			continue
		}
		if op.ID < m.ID {
			m.ID, m.Client = op.ID, op.Client
		}
		m.Kind, m.Key, m.Value = op.Kind, op.Key, op.Value
		m.ClientID, m.RequestID = op.ClientID, op.RequestID
		if op.Invoke < m.Invoke {
			m.Invoke = op.Invoke
		}
		m.Attempts = append(m.Attempts, op.Attempts...)
		switch op.Outcome {
		case OK:
			anyOK = true
			if op.Complete < m.Complete {
				m.Complete = op.Complete
				m.Node, m.Term, m.Index = op.Node, op.Term, op.Index
			}
		case Incomplete:
			anyIncomplete = true
		default:
			if op.Complete < firstRejectedComplete {
				firstRejectedComplete = op.Complete
			}
		}
	}
	switch {
	case anyOK:
		m.Outcome = OK
	case anyIncomplete:
		m.Outcome, m.Complete = Incomplete, 0
	default:
		// Every send was refused. A member's completion follows the earliest
		// invocation, so the interval stays well formed (and is never used:
		// refused operations are excluded from the search).
		m.Outcome, m.Complete = Rejected, firstRejectedComplete
	}
	return m, nil
}
