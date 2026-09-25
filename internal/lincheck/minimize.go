package lincheck

// Minimize shrinks a non-linearizable key history to a smaller one that fails
// for the SAME reason, by greedy chunk deletion (delta debugging), so a failure
// report shows a handful of ops rather than a whole workload.
//
// Deleting operations from a record can invent violations that never happened
// (drop a Delete and a later NotFound becomes "wrong"), so a deletion is kept
// only when the remainder is anchored to the original failure: it must still be
// non-linearizable; every value a remaining Get returned must still be written
// by some remaining Put (of any outcome — a rejected write that was observed is
// part of the story); the culprit — the op the deepest search point could not
// explain in the full history — must remain; and removing that culprit alone
// must make the remainder linearizable, so every other remaining observation is
// explained and the culprit is the one thing that is not. The result is
// 1-minimal under that rule. Rejected and incomplete ops are candidates for
// deletion like any other, and are kept only if the anchor needs them.
func Minimize(ops []Op, maxStates int) []Op {
	failing := func(s []Op) bool {
		kr := checkKey(s, maxStates)
		return !kr.ok && !kr.budget
	}
	if len(ops) == 0 || !failing(ops) {
		return ops
	}
	culprit := 0
	if kr := checkKey(ops, maxStates); kr.stuck != nil && len(kr.stuck.culprits) > 0 {
		culprit = kr.stuck.culprits[0]
	}
	keep := func(s []Op) bool {
		if len(s) == 0 || !producersPresent(s) || !failing(s) {
			return false
		}
		if culprit == 0 {
			return true
		}
		rest := make([]Op, 0, len(s))
		found := false
		for _, op := range s {
			if op.ID == culprit {
				found = true
				continue
			}
			rest = append(rest, op)
		}
		return found && !failing(rest)
	}
	cur := append([]Op(nil), ops...)
	if !keep(cur) {
		return ops // the anchor does not hold on the full history; report it whole
	}
	for chunk := len(cur) / 2; chunk >= 1; {
		removed := false
		for start := 0; start < len(cur); {
			end := start + chunk
			if end > len(cur) {
				end = len(cur)
			}
			cand := append(append([]Op(nil), cur[:start]...), cur[end:]...)
			if keep(cand) {
				cur, removed = cand, true
				continue
			}
			start = end
		}
		if !removed {
			chunk /= 2
		}
	}
	return cur
}

// producersPresent reports whether every successful Get's output is written by a
// Put in ops — of any outcome, since an observed value from a rejected or
// unanswered write is exactly the kind of evidence a counterexample must keep.
func producersPresent(ops []Op) bool {
	written := map[string]bool{}
	for _, op := range ops {
		if op.Kind == Put {
			written[string(op.Value)] = true
		}
	}
	for _, op := range ops {
		if op.Kind == Get && op.Outcome == OK && !written[string(op.Output)] {
			return false
		}
	}
	return true
}
