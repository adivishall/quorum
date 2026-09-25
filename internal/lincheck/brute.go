package lincheck

import "sort"

// BruteForce decides linearizability of one key's ops by exhaustive enumeration:
// every subset of the optional (Incomplete) writes, and every ordering of the
// chosen ops that respects real-time precedence, run through the model. It is
// the oracle the search-based checker is tested against, and is only usable for
// small histories (it is factorial in the op count).
func BruteForce(all []Op) bool {
	ops := effective(all)
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Invoke < ops[j].Invoke })
	var optional, required []int
	for i, op := range ops {
		if op.Optional() {
			optional = append(optional, i)
		} else {
			required = append(required, i)
		}
	}
	// Every subset of the optional ops.
	for mask := 0; mask < 1<<uint(len(optional)); mask++ {
		chosen := append([]int(nil), required...)
		for j, i := range optional {
			if mask&(1<<uint(j)) != 0 {
				chosen = append(chosen, i)
			}
		}
		used := make([]bool, len(ops))
		if permute(ops, chosen, used, Initial, 0) {
			return true
		}
	}
	return false
}

// permute tries every next op among chosen that no unused chosen op precedes.
func permute(ops []Op, chosen []int, used []bool, st State, placed int) bool {
	if placed == len(chosen) {
		return true
	}
	for _, i := range chosen {
		if used[i] {
			continue
		}
		blocked := false
		for _, j := range chosen {
			if !used[j] && j != i && ops[j].Complete != 0 && ops[j].Complete < ops[i].Invoke {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}
		st2, ok := Step(st, ops[i])
		if !ok {
			continue
		}
		used[i] = true
		if permute(ops, chosen, used, st2, placed+1) {
			return true
		}
		used[i] = false
	}
	return false
}
