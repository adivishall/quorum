#!/usr/bin/env bash
#
# mutation.sh — Raft mutation testing: the Phase 9 protocol rules (docs/RAFT.md
# §12a), the Phase 10 failure-handling rules and fault-model fidelity
# (docs/FAULTS.md §12), the Phase 11 crash-recovery rules
# (docs/CRASH_RECOVERY.md §10), and the Phase 12 client-visible consistency
# rules — ReadIndex, write completion, the client protocol and policy, the
# state machine, and the linearizability checker itself
# (docs/LINEARIZABILITY.md §11).
#
# For each mutant it applies a real source edit that violates a specific Raft rule,
# runs the test(s) that should catch that violation, and requires them to FAIL (the
# mutant is "killed"). A mutant that survives — the killer tests still pass with the
# rule broken — is a hole in the suite and fails this runner. Every edit is reverted
# with `git checkout`, so the working tree is unchanged when this exits. This is not
# a permanent runtime flag or a source-string inspection: it exercises altered
# behaviour and proves the existing correctness suite detects it.
#
# Usage: scripts/mutation.sh   (from anywhere; requires a clean git working tree)

set -uo pipefail
cd "$(dirname "$0")/.."

# Each mutant edits a tracked source file and reverts it with `git checkout`. That
# is only safe if the file has no uncommitted changes to lose, so each mutant
# checks its own target file is clean before editing (below). Untracked files (e.g.
# an as-yet-uncommitted copy of this script) do not affect revert safety.

TOUCHED=()
revert_all() {
  local f
  for f in "${TOUCHED[@]:-}"; do
    [ -n "$f" ] && git checkout -- "$f" 2>/dev/null
  done
}
trap revert_all EXIT

FAIL=0
KILLED=0
TOTAL=0

# mutant NAME FILE SEARCH REPLACE PKGS TESTREGEX   (PKGS may list several packages)
mutant() {
  local name="$1" file="$2" search="$3" replace="$4" pkg="$5" tests="$6"
  TOTAL=$((TOTAL + 1))
  TOUCHED+=("$file")

  if ! git diff --quiet -- "$file"; then
    echo "✗ $name: $file has uncommitted changes; refusing to mutate (revert safety)."
    FAIL=$((FAIL + 1))
    return
  fi

  S="$search" R="$replace" perl -0pi -e 's/\Q$ENV{S}\E/$ENV{R}/' "$file"
  if git diff --quiet -- "$file"; then
    echo "✗ $name: PATTERN DID NOT MATCH in $file — cannot mutate (source changed?)."
    FAIL=$((FAIL + 1))
    git checkout -- "$file" 2>/dev/null
    return
  fi

  # shellcheck disable=SC2086 # $pkg is a deliberate word-split package list
  if go test $pkg -run "$tests" -count=1 -timeout 180s >/tmp/mutation.$$.log 2>&1; then
    echo "✗ $name: SURVIVED — killer tests [$tests] still PASSED with the rule broken."
    FAIL=$((FAIL + 1))
  else
    echo "✓ $name: killed by [$tests]."
    KILLED=$((KILLED + 1))
  fi
  git checkout -- "$file" 2>/dev/null
}

echo "== Raft mutation testing =="

# 1. Remove the current-term commit restriction (§5.4.2 / figure 8). The mandatory
#    no-op keeps this unreachable through the message flow, so it is killed by the
#    direct commit-rule test rather than by TestFigure8 (see docs/RAFT.md).
mutant "current-term-commit-rule" internal/raft/raft.go \
  'if t == r.currentTerm {' 'if t == r.currentTerm || true {' \
  ./internal/raft 'TestCommitRuleRequiresCurrentTerm'

# 2. Remove the mandatory election no-op.
mutant "mandatory-election-no-op" internal/raft/raft.go \
  'r.appendEntry(nil)' '_ = r.id' \
  ./internal/raft 'TestNoOpAppendedOnElection'

# 3. Allow a second vote to a different candidate in the same term.
mutant "one-vote-per-term" internal/raft/raft.go \
  'if (r.votedFor == "" || r.votedFor == m.From) && r.candidateUpToDate(' \
  'if (true || r.votedFor == m.From) && r.candidateUpToDate(' \
  ./internal/raft 'TestVoteGrantedOncePerTerm|TestVoteAgainstReferenceModel'

# 4. Permit overwriting a committed suffix (the Phase 8 log guard).
mutant "no-overwrite-committed" internal/replication/log.go \
  'if f <= l.commit {' 'if false && f <= l.commit {' \
  ./internal/replication 'TestCannotReplaceCommittedEntry'

# 5. Disable term-based conflict backtracking (fall back to naive per-index).
mutant "conflict-term-backtracking" internal/raft/raft.go \
  'back := r.backupNextIndex(m.ConflictTerm, m.ConflictIndex)' \
  'back := r.nextIndex[peer] - 1' \
  ./internal/raft 'TestConflictBackupByTerm'

# 6. Let stale (lower-term) messages mutate state instead of being rejected.
mutant "stale-message-inert" internal/raft/raft.go \
  'if m.Term < r.currentTerm {' 'if false && m.Term < r.currentTerm {' \
  ./internal/raft 'TestStaleAppendResponseIgnored|TestStaleMessageIsInert'

# 7. Fail to step down on a higher-term message.
mutant "higher-term-step-down" internal/raft/raft.go \
  'if m.Term > r.currentTerm {' 'if false && m.Term > r.currentTerm {' \
  ./internal/raft 'TestHigherTermForcesStepDown|TestLeaderCompleteness'

# 8. Violate persist-before-dependent-reply (skip the durable Save entirely).
mutant "persist-before-reply" internal/raftnode/crashpoint.go \
  'if hs != nil || len(rd.Entries) > 0 {' \
  'if false && (hs != nil || len(rd.Entries) > 0) {' \
  ./internal/raftnode 'TestPersistBeforeReplyOnDriverPath'

echo "== Phase 10: failure handling =="

# 9. Save returns before its fsync (the reply would depend on cached bytes only).
mutant "fsync-before-reply" internal/raftlog/raftlog.go \
  '	if l.sync {
		if err := l.f.Sync(); err != nil {' \
  '	if false && l.sync {
		if err := l.f.Sync(); err != nil {' \
  "./internal/raftlog ./internal/raftnode ./internal/raftsim" \
  'TestSaveIsDurableWhenItReturns|TestReplyOnlyAfterFsync|TestAckedEntrySurvivesPowerLoss'

# 10. Open does not make the state it recovered durable (a node could acknowledge
#     entries it recovered from un-fsynced cache — the bug the simulator found).
mutant "fsync-recovered-state-at-open" internal/raftlog/raftlog.go \
  '	// Make the recovered state (and any tail repair) durable before it is used.
	if err := f.Sync(); err != nil {' \
  '	// Make the recovered state (and any tail repair) durable before it is used.
	if err := error(nil); err != nil {' \
  "./internal/raftlog ./internal/raftsim" \
  'TestOpenMakesRecoveredStateDurable|TestNoAckOfUnsyncedEntriesAfterFailedFsync'

# 11. A failed write/fsync does not poison the log (later writes land behind a torn record).
mutant "durability-failure-latches" internal/raftlog/raftlog.go \
  '	if l.failed != nil {
		return l.failed
	}' \
  '	if false && l.failed != nil {
		return l.failed
	}' \
  ./internal/raftlog 'TestFailedWriteLatchesAndLogStaysRecoverable|TestFailedSyncLatches'

# 12. The driver ignores a persistence failure and sends the Ready's messages anyway.
mutant "fail-stop-on-persist-failure" internal/raftnode/crashpoint.go \
  '			if err := st.Save(hs, rd.Entries); err != nil {
				return err
			}' \
  '			if err := st.Save(hs, rd.Entries); err != nil {
				_ = err
			}' \
  "./internal/raftnode ./internal/raftsim" \
  'TestPersistFailureIsFailStop|TestVoteNotSentWhenItCannotBePersisted'

# 13. The actor sends synchronously, so one wedged peer stalls the whole node.
mutant "actor-never-blocks-on-a-peer" internal/raftnode/node.go \
  '	select {
	case ch <- m:
	default:' \
  '	n.sendMessage(m)
	return
	select {
	case ch <- m:
	default:' \
  ./internal/raftnode 'TestWedgedPeerDoesNotStallTheLeader'

# 14. Propose ignores the caller's deadline while the node is busy persisting.
mutant "propose-honours-deadline" internal/raftnode/node.go \
  '	case <-n.ctx.Done():
		return raft.ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}' \
  '	case <-n.ctx.Done():
		return raft.ErrStopped
	}
}' \
  ./internal/raftnode 'TestProposeHonoursContextDuringSlowFsync'

# 15. dkvd keeps running after its Raft node fail-stops.
mutant "dkvd-exits-on-fail-stop" cmd/dkvd/main.go \
  '	case <-ctx.Done():
	case <-n.Done():
		if err := n.Err(); err != nil {' \
  '	case <-ctx.Done():
	case <-make(chan struct{}):
		if err := n.Err(); err != nil {' \
  ./cmd/dkvd 'TestRaftModeExitsNonZeroWhenTheLogFails'

# 16. Duplicated vote responses from one voter are counted as separate votes.
mutant "duplicate-votes-idempotent" internal/raft/raft.go \
  'r.votesGranted[m.From] = true' \
  'r.votesGranted[m.From+NodeID(rune(48+len(r.votesGranted)))] = true' \
  ./internal/raftsim 'TestDuplicatedVoteDoesNotCountTwice'

# 17. A reordered, older AppendEntries success moves a follower's progress backward.
mutant "stale-success-ignored" internal/raft/raft.go \
  'if m.MatchIndex > r.matchIndex[peer] {' 'if true {' \
  ./internal/raftsim 'TestStaleSuccessDoesNotRegressReplication'

echo "== Phase 10: fault-model fidelity (the harness must do what it claims) =="

# 18. The simulator delivers across a partition.
mutant "sim-partition-enforced" internal/raftsim/cluster.go \
  '	if c.links.Blocked(string(m.From), string(m.To)) {
		c.stats.DroppedPartition++' \
  '	if false && c.links.Blocked(string(m.From), string(m.To)) {
		c.stats.DroppedPartition++' \
  ./internal/raftsim 'TestLeaderIsolatedFromMajority'

# 19. The simulator delivers a delayed message early.
mutant "sim-delay-enforced" internal/raftsim/chaos.go \
  '			if f.holdUntil <= c.step && !c.Paused(f.msg.To) {
				next = f' \
  '			if !c.Paused(f.msg.To) {
				next = f' \
  ./internal/raftsim 'TestDelayedOldTermAppendIsInert'

# 20. A simulated restart ignores the node's disk.
mutant "sim-restart-from-disk" internal/raftsim/cluster.go \
  '		ID: n.id, Peers: c.ids, LogPath: logPath, FS: n.inj,' \
  '		ID: n.id, Peers: c.ids, LogPath: logPath, FS: fault.NewMemFS(),' \
  ./internal/raftsim 'TestFollowerCrashAndCatchUp'

# 21. The power-loss model keeps bytes that were never fsynced.
mutant "memfs-power-loss-drops-unsynced" internal/fault/memfs.go \
  '		kept := append([]byte(nil), n.durable()...)' \
  '		kept := append([]byte(nil), n.data...)' \
  ./internal/fault 'TestPowerLossKeepsOnlySyncedBytes'

# 22. The transport decorator lets a dropped message through.
mutant "network-drop-rule" internal/fault/transport.go \
  '		case Drop:
			n.stats.Dropped++
			return lose, nil' \
  '		case Drop:
			n.stats.Dropped++
			return pass, nil' \
  ./internal/fault 'TestDropRuleByKindAndCount'

# 23. Remove the dialer's back-off after a connection dies (redial immediately),
#     so a peer that accepts-and-resets becomes a reconnect storm (Phase 10, bug 6).
mutant "dialer-backs-off-after-dead-conn" internal/transport/transport.go \
  '		t.serve(peer, nc, "outbound") // blocks until the connection dies
		// A dead connection waits the same retry interval as a failed dial.
		// Redialling immediately would spin at CPU speed against a peer that
		// accepts and instantly closes — a crash-looping peer, or a partition
		// that resets connections — burning ports and flooding logs.
		if t.sleep(t.cfg.DialRetryInterval) {
			return
		}' \
  '		t.serve(peer, nc, "outbound") // blocks until the connection dies' \
  ./internal/transport 'TestDialerBacksOffWhenPeerKeepsClosingConnections'

# 24. Ignore the read idle deadline, so a silent (established-but-dead) connection
#     blocks the reader forever and the peer is never reconnected (Phase 10, bug 6).
mutant "read-idle-timeout-detects-dead-conn" internal/transport/transport.go \
  '		if t.cfg.ReadIdleTimeout > 0 {
			_ = c.nc.SetReadDeadline(time.Now().Add(t.cfg.ReadIdleTimeout))
		}' \
  '		if false {
			_ = c.nc.SetReadDeadline(time.Time{})
		}' \
  ./internal/transport 'TestReaderIdleTimeoutReconnectsASilentConnection'

# --- Phase 11: crash-recovery rules (docs/CRASH_RECOVERY.md §10) ---

# 25. Write a Save's entries before its term change (the pre-Phase-11 order): a
#     crash between the two records leaves entries of a term above the durable
#     currentTerm, and recovery refuses the log — the node can never restart.
mutant "term-durable-before-entries-of-that-term" internal/raftlog/raftlog.go \
  '	if len(entries) > 0 && (hs.Term != prev.Term || hs.Vote != prev.Vote) {' \
  '	if false && len(entries) > 0 && (hs.Term != prev.Term || hs.Vote != prev.Vote) {' \
  './internal/raftlog ./internal/raftsim' 'TestTermChangeIsDurableBeforeEntriesOfThatTerm|TestPrePhase11OrderLeftAnUnrecoverableLog|TestSingleNodeCrashInsideItsElectionSave'

# 26. Let the leading HardState record carry the NEW commit: a crash before the
#     replacing entries leaves the old, conflicting entries under a commit that
#     covers them.
mutant "commit-not-durable-before-its-entries" internal/raftlog/raftlog.go \
  '		lead = &HardState{Term: hs.Term, Vote: hs.Vote, Commit: prev.Commit}' \
  '		lead = &HardState{Term: hs.Term, Vote: hs.Vote, Commit: hs.Commit}' \
  ./internal/raftlog 'TestCommitNeverCoversEntriesTheSaveHadNotWritten'

# 27. Trust a persisted commit beyond the entries actually recovered.
mutant "recovered-commit-clamped-to-log" internal/raftlog/raftlog.go \
  '	if rec.HardState.Commit > uint64(len(rec.Entries)) {' \
  '	if false && rec.HardState.Commit > uint64(len(rec.Entries)) {' \
  ./internal/raftlog 'TestCommitBeyondRecoveredLogIsClamped'

# 28. Do not truncate a torn tail on recovery, so the next append lands behind
#     garbage and the log becomes unopenable.
mutant "torn-tail-truncated-on-recovery" internal/raftlog/raftlog.go \
  '	if next < info.Size() {' \
  '	if false && next < info.Size() {' \
  ./internal/raftlog 'TestTornTailIsTruncated'

# 29. Treat every framing failure as a torn tail — silently skipping mid-log
#     corruption instead of refusing to open.
mutant "mid-log-corruption-is-fatal" internal/raftlog/raftlog.go \
  '			if errors.Is(err, record.ErrTornTail) {
				break // a crash mid-append; the last good offset is the append point
			}' \
  '			if true {
				break // a crash mid-append; the last good offset is the append point
			}' \
  ./internal/raftlog 'TestMidCorruptionIsFatal'

# 30. Accept a recovered currentTerm below the term of an entry in the log
#     (recovery inventing coherence instead of refusing an incoherent log).
mutant "recover-refuses-term-below-log" internal/raft/config.go \
  '		if c.Term < lt {
			return nil, ErrTermRegression
		}' \
  '		if false && c.Term < lt {
			return nil, ErrTermRegression
		}' \
  ./internal/raftnode 'TestRecoverRefusesATermBelowItsLog'

# 31. Restore volatile leader state on restart: a recovered node believes it
#     still leads its recovered term.
mutant "restart-as-follower-never-leader" internal/raft/raft.go \
  '		role:           Follower,' \
  '		role:           Leader,' \
  ./internal/raftsim 'TestCrashMatrix|TestCrashDuringSuffixReplacement'

# 32. Record an entry as applied BEFORE the state machine applies it, so a crash
#     between the two loses the application.
mutant "applied-recorded-only-after-apply" internal/raftnode/crashpoint.go \
  '		if sm != nil {
			if err := sm.Apply(e.Index, e.Data); err != nil {
				return fmt.Errorf("%w: index %d: %w", ErrApply, e.Index, err)
			}
		}
		if err := at.hit(AfterApply, e.Index); err != nil {
			return err
		}
		if err := core.AppliedTo(e.Index); err != nil {
			return fmt.Errorf("%w: AppliedTo(%d): %w", ErrApply, e.Index, err)
		}' \
  '		if err := core.AppliedTo(e.Index); err != nil {
			return fmt.Errorf("%w: AppliedTo(%d): %w", ErrApply, e.Index, err)
		}
		if sm != nil {
			if err := sm.Apply(e.Index, e.Data); err != nil {
				return fmt.Errorf("%w: index %d: %w", ErrApply, e.Index, err)
			}
		}
		if err := at.hit(AfterApply, e.Index); err != nil {
			return err
		}' \
  ./internal/raftnode 'TestCrashPointAbortStopsExactlyThere|TestApplyFailureIsWrappedAndLeavesTheRestPending'

# 33. Forget the durable vote on restart: a node that voted in its current term
#     could vote again, for someone else.
mutant "recover-restores-vote" internal/raftnode/node.go \
  '		Term: rec.HardState.Term, Vote: rec.HardState.Vote,' \
  '		Term: rec.HardState.Term, Vote: "",' \
  ./internal/raftsim 'TestVoterCrashAroundPersistingItsVote|TestCrashMatrix'

echo "== Phase 12: client-visible consistency =="

# 34. A follower registers a ReadIndex (and broadcasts as if it led).
mutant "readindex-requires-leader" internal/raft/raft.go \
  'func (r *Raft) ReadIndex() (ReadState, error) {
	if r.role != Leader {' \
  'func (r *Raft) ReadIndex() (ReadState, error) {
	if false && r.role != Leader {' \
  "./internal/raft ./internal/raftsim" 'TestReadIndexRequiresLeader|TestKVHistoryOfAScriptIsReplayable'

# 35. A new leader's read index is its commit index alone, which can lag entries
#     its predecessor committed: a read served below the new leader's no-op.
mutant "readindex-at-least-the-noop" internal/raft/raft.go \
  '	if r.termStart > rs.Index {' \
  '	if false && r.termStart > rs.Index {' \
  "./internal/raft ./internal/raftsim" 'TestNewLeaderReadIndexIsAtLeastItsNoop|TestKVNewLeaderReadWaitsForItsNoop'

# 36. Acknowledgements of a heartbeat sent BEFORE the read confirm it (a stale
#     ReadIndex response accepted): a deposed leader serves its old state.
mutant "readindex-ignores-acks-sent-before-the-read" internal/raft/raft.go \
  'seq: r.hbSeq + 1})' \
  'seq: r.hbSeq})' \
  "./internal/raft ./internal/raftsim" 'TestAcksFromBeforeTheReadDoNotConfirmIt|TestKVStaleLeaderReadIsNeverServed'

# 37. A ReadIndex confirmed without a quorum (the leader alone suffices).
mutant "readindex-needs-a-quorum" internal/raft/raft.go \
  '		if acks < quorum(len(r.peers)) {' \
  '		if acks < 1 {' \
  "./internal/raft ./internal/raftsim" 'TestReadIndexIsConfirmedByAQuorumRound|TestIsolatedLeaderNeverConfirmsARead|TestKVStaleLeaderReadIsNeverServed|TestKVMinorityLeaderWithAFollowerNeverServesARead'

# 38. Unconfirmed reads survive the leader stepping down in the core.
mutant "unconfirmed-reads-die-with-leadership" internal/raft/raft.go \
  '		r.pending = nil // unconfirmed reads die with the leadership' \
  '		_ = r.pending // mutant: kept across the step-down' \
  ./internal/raft 'TestPendingReadsAreDroppedOnStepDown'

# 39. The driver ignores a leadership change: a read registered in a term the
#     node no longer leads is never failed (its client learns nothing).
mutant "driver-fails-reads-on-leadership-change" internal/raftnode/waiters.go \
  '		if !leader || p.term != term {' \
  '		if false {' \
  "./internal/raftnode ./internal/raftsim" 'TestReadsConfirmAndDropStale|TestKVStaleLeaderReadIsNeverServed'

# 40. A PUT completes when appended, before a quorum commits it.
mutant "write-completes-only-when-committed-and-applied" internal/raftnode/waiters.go \
  '	if term == 0 && index <= applied {' \
  '	if index <= applied || term != 0 {' \
  "./internal/raftnode ./internal/raftsim" 'TestWaitersCompleteWritesOnlyInTheirTerm|TestKVWriteIsNotAcknowledgedBeforeCommit'

# 41. A write whose index was taken by a DIFFERENT committed entry reports success.
mutant "lost-write-is-not-success" internal/raftnode/waiters.go \
  '		case wt.term == 0 || wt.term == term:' \
  '		case true:' \
  "./internal/raftnode ./internal/raftsim" 'TestWaitersCompleteWritesOnlyInTheirTerm|TestKVWriteIsNotAcknowledgedBeforeCommit'

# 42. A confirmed read is served before the state machine reaches its read index
#     (the client sees state older than the index that was confirmed).
mutant "read-waits-for-apply-to-reach-the-read-index" internal/raftnode/waiters.go \
  '	if rs.Index <= applied {' \
  '	if true {' \
  "./internal/raftnode ./internal/raftsim" 'TestReadsConfirmAndDropStale|TestKVNewLeaderReadWaitsForItsNoop'

# 43. Server.Get bypasses ReadIndex: a local read on whatever node was asked.
mutant "server-get-goes-through-readindex" internal/kv/server.go \
  '	idx, err := s.node.ReadIndex(ctx)' \
  '	idx, err := s.node.CommitIndex(), error(nil)' \
  "./internal/kv ./tests/integration" 'TestFollowerLocalReadIsCaughtAsNonLinearizable|TestRealStaleLeaderNeverServesARead'

# 44. dkvd serves clients from a store that is not the node's state machine:
#     committed state vanishes from the client-visible view.
mutant "dkvd-serves-the-replicated-state-machine" cmd/dkvd/main.go \
  'kv.NewServer(id, n, store)' \
  'kv.NewServer(id, n, kv.NewStore())' \
  ./tests/integration 'TestRealSequentialBaselineMatchesTheModel'

# 45. DELETE does not remove the key from the state machine.
mutant "store-delete-removes-the-key" internal/kv/store.go \
  '		delete(s.m, string(c.Key))' \
  '		_ = c.Key' \
  "./internal/kv ./internal/raftsim" 'TestStoreMatchesTheStorageContract|TestStoreMatchesTheReferenceModel|TestKVSeededHistoriesAreLinearizable'

# 46. PUT of an empty value is treated as absence (the Phase 1 contract: an
#     empty value is a present key).
mutant "store-empty-value-is-present" internal/kv/store.go \
  '		s.m[string(c.Key)] = append([]byte{}, c.Value...)' \
  '		if len(c.Value) == 0 {
			delete(s.m, string(c.Key))
		} else {
			s.m[string(c.Key)] = append([]byte{}, c.Value...)
		}' \
  ./internal/kv 'TestStoreMatchesTheStorageContract|TestStoreMatchesTheReferenceModel|TestWireClientAgainstRealNodes'

# 47. The client keeps a connection whose request timed out: the late response
#     is read as the answer to the NEXT request.
mutant "wire-client-abandons-a-timed-out-connection" internal/kv/wire.go \
  '	payload, err := readFrame(c.conn, kindResponse)
	if err != nil {
		c.dropConn()' \
  '	payload, err := readFrame(c.conn, kindResponse)
	if err != nil {' \
  ./internal/kv 'TestWireClientNeverMatchesALateResponseToANewRequest'

# 48. The test client retries a write whose outcome is unknown (a hidden retry
#     that could apply it twice under one recorded operation).
mutant "workload-never-retries-an-unknown-write" internal/kv/workload/workload.go \
  '			if kind != lincheck.Get {' \
  '			if false && kind != lincheck.Get {' \
  ./internal/kv/workload 'TestClientPolicy'

echo "== Phase 12: the linearizability checker itself =="

# 49. The checker ignores real-time order (any op may be linearized next).
mutant "checker-respects-real-time" internal/lincheck/check.go \
  '		if lin.has(i) || s.inv[i] >= minRes {' \
  '		if lin.has(i) {' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestCheckerAgreesWithIndependentOracle'

# 50. The checker discards unanswered writes as if they never happened.
mutant "checker-keeps-unanswered-writes-optional" internal/lincheck/check.go \
  '		if !op.Effective() {
			kr.excluded++' \
  '		if !op.Effective() || op.Optional() {
			kr.excluded++' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestCheckerAgreesWithIndependentOracle'

# 51. The checker forces definite rejections into the linearization.
mutant "checker-excludes-definite-rejections" internal/lincheck/history.go \
  'func (o Op) Effective() bool { return o.Outcome != Rejected }' \
  'func (o Op) Effective() bool { return true }' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestCheckerAgreesWithIndependentOracle'

# 52. The checker memoizes on the linearized set alone, forgetting the register
#     state (unsound pruning: linearizable histories rejected).
mutant "checker-memo-includes-the-register-state" internal/lincheck/check.go \
  '	key := lin.key(st)' \
  '	key := lin.key(Initial)' \
  ./internal/lincheck 'TestCheckerAgreesWithIndependentOracle'

# 53. The model reads an absent key as a present empty value.
mutant "model-absent-is-not-empty" internal/lincheck/model.go \
  '		return s, s.Present && s.Value == string(op.Output)' \
  '		return s, s.Value == string(op.Output)' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestCheckerAgreesWithIndependentOracle'

echo "== Phase 12: the same rules, killed by client-visible histories ALONE =="
# The mutants above list unit killers next to history-level ones, so a kill
# does not show that the history-level test has teeth by itself. These do: each
# is killed with only a recorded-history test (simulated or real processes).

# 54. No-quorum ReadIndex, on five real processes: a minority leader that still
#     has a follower acknowledging it serves a stale read.
mutant "readindex-needs-a-quorum (real processes)" internal/raft/raft.go \
  '		if acks < quorum(len(r.peers)) {' \
  '		if acks < 1 {' \
  ./tests/integration 'TestRealMinorityLeaderWithAFollowerNeverServesARead'

# 55. The same, in the simulator's scripted minority-leader attack.
mutant "readindex-needs-a-quorum (simulated history)" internal/raft/raft.go \
  '		if acks < quorum(len(r.peers)) {' \
  '		if acks < 1 {' \
  ./internal/raftsim 'TestKVMinorityLeaderWithAFollowerNeverServesARead'

# 56. Pre-read acknowledgements confirm the read: the simulated stale leader
#     serves its old value once the delayed acks arrive.
mutant "readindex-ignores-acks-sent-before-the-read (simulated history)" internal/raft/raft.go \
  'seq: r.hbSeq + 1})' \
  'seq: r.hbSeq})' \
  ./internal/raftsim 'TestKVStaleLeaderReadIsNeverServed'

# 57. No no-op rule: the new leader serves below its predecessor's last commit.
mutant "readindex-at-least-the-noop (simulated history)" internal/raft/raft.go \
  '	if r.termStart > rs.Index {' \
  '	if false && r.termStart > rs.Index {' \
  ./internal/raftsim 'TestKVNewLeaderReadWaitsForItsNoop'

# 58. Writes acknowledged at append: caught by the SEEDED workloads alone.
mutant "write-completes-only-when-committed-and-applied (seeded histories)" internal/raftnode/waiters.go \
  '	if term == 0 && index <= applied {' \
  '	if index <= applied || term != 0 {' \
  ./internal/raftsim 'TestKVSeededHistoriesAreLinearizable'

# 59. Server.Get bypasses ReadIndex, on real processes.
mutant "server-get-goes-through-readindex (real processes)" internal/kv/server.go \
  '	idx, err := s.node.ReadIndex(ctx)' \
  '	idx, err := s.node.CommitIndex(), error(nil)' \
  ./tests/integration 'TestRealStaleLeaderNeverServesARead'

echo "== $KILLED/$TOTAL mutants killed =="
rm -f /tmp/mutation.$$.log
if [ "$FAIL" -ne 0 ]; then
  echo "MUTATION TESTING FAILED: $FAIL mutant(s) survived or could not be applied." >&2
  exit 1
fi
echo "MUTATION TESTING PASSED: every mutant was killed."
