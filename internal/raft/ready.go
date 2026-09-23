package raft

// Ready is the batch of effects the driver must perform after feeding input to
// the core. The driver MUST act in field order: persist HardState (if set) and
// Entries durably FIRST, then send Messages. That ordering is what makes "durable
// before the dependent reply" (INV-R6) hold by construction (ADR-016). The apply
// path is separate (NextApply/AppliedTo), performed after persisting.
//
// Ready and Advance are a pair: the driver calls Ready, performs its effects, then
// calls Advance, with no intervening Tick/Propose/Step. Advance drains the effects
// the Ready reported.
type Ready struct {
	// HardState is non-nil when currentTerm/votedFor changed, or when the commit
	// index advanced (persisted as an optimization). Persist it before Messages.
	HardState *HardState
	// Entries are the log entries written since the last Advance, in index order.
	// Persist them before Messages. On the durable log an append reconstructs a
	// suffix replacement (ADR-016), so these are always the current tail.
	Entries []Entry
	// Messages are the RPCs to send AFTER HardState and Entries are durable.
	Messages []Message
}

// empty reports whether a Ready carries nothing to do.
func (rd Ready) empty() bool {
	return rd.HardState == nil && len(rd.Entries) == 0 && len(rd.Messages) == 0
}

// HasReady reports whether there are pending effects to collect with Ready. It
// does not report apply readiness — the driver checks NextApply separately.
func (r *Raft) HasReady() bool {
	return r.hsDirty ||
		r.log.CommitIndex() != r.lastPersistedCommit ||
		r.unstable != 0 ||
		len(r.msgs) != 0
}

// Ready returns the pending effects without draining them (call Advance after
// acting on them). It does not mutate Raft state.
func (r *Raft) Ready() Ready {
	var rd Ready
	if r.hsDirty || r.log.CommitIndex() != r.lastPersistedCommit {
		rd.HardState = &HardState{
			Term:   r.currentTerm,
			Vote:   r.votedFor,
			Commit: r.log.CommitIndex(),
		}
	}
	if r.unstable != 0 {
		// Slice cannot error: unstable is always in [1, lastIndex].
		es, _ := r.log.Slice(r.unstable, r.log.LastIndex()+1)
		rd.Entries = es
	}
	rd.Messages = r.msgs
	return rd
}

// Advance drains the effects reported by the preceding Ready: it clears the
// outbound messages, marks the HardState clean, and records the log tail as
// persisted. Call it only after the Ready's effects are durably done and its
// messages sent.
func (r *Raft) Advance() {
	r.msgs = nil
	r.hsDirty = false
	r.lastPersistedCommit = r.log.CommitIndex()
	r.unstable = 0
}
