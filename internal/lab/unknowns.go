package lab

import (
	"strings"

	"github.com/adivishall/quorum/internal/load"
)

// Causes of an unknown outcome, read from its attempt trace (load.OpTrace).
const (
	// CauseRefusedUntilGiveUp: an attempt went unanswered — the request may
	// have taken effect — and every attempt after it was a definite refusal
	// that sent nothing to a leader (the node was down, knew no leader, or
	// was not the leader), until the attempts ran out. The client gave up
	// before any retry reached a leader that could have settled it.
	CauseRefusedUntilGiveUp = "unanswered-then-refused"
	// CauseUnansweredAtGiveUp: the last attempt itself went unanswered —
	// the budget ran out while the request was still in doubt at a node.
	CauseUnansweredAtGiveUp = "unanswered-at-give-up"
	// CauseCancelled: the operation's own context ended (the run stopped).
	CauseCancelled = "cancelled"
	// CauseOther: none of the above.
	CauseOther = "other"
)

// unanswered reports whether an attempt left its request in doubt: the
// connection broke or timed out after the request was sent, or the node
// answered UNKNOWN_OUTCOME.
func unanswered(a load.AttemptTrace) bool {
	if a.Err != "" {
		return !strings.Contains(a.Err, "node unavailable") // ErrUnavailable: nothing was sent
	}
	return a.Status == "UNKNOWN_OUTCOME"
}

// refusal reports whether an attempt was definitely refused without reaching
// a leader that could settle the request: nothing was sent (the node could not
// be reached), or the node knew no leader, was not one, or saw the attempt's
// entry overwritten.
func refusal(a load.AttemptTrace) bool {
	if a.Err != "" {
		return strings.Contains(a.Err, "node unavailable")
	}
	switch a.Status {
	case "UNAVAILABLE", "NOT_LEADER", "LOST", "SESSION_LIMIT":
		return true
	}
	return false
}

// ClassifyUnknown names why an operation ended unknown.
func ClassifyUnknown(t load.OpTrace) string {
	if strings.Contains(t.Err, "context canceled") || strings.Contains(t.Err, "context deadline exceeded") {
		return CauseCancelled
	}
	last := -1
	for i, a := range t.Attempts {
		if unanswered(a) {
			last = i
		}
	}
	switch {
	case last < 0:
		return CauseOther
	case last == len(t.Attempts)-1:
		return CauseUnansweredAtGiveUp
	}
	for _, a := range t.Attempts[last+1:] {
		if !refusal(a) {
			return CauseOther
		}
	}
	return CauseRefusedUntilGiveUp
}
