#!/usr/bin/env bash
#
# mutation.sh — Raft mutation testing: the Phase 9 protocol rules (docs/RAFT.md
# §12a), the Phase 10 failure-handling rules and fault-model fidelity
# (docs/FAULTS.md §12), the Phase 11 crash-recovery rules
# (docs/CRASH_RECOVERY.md §10), the Phase 12 client-visible consistency
# rules — ReadIndex, write completion, the client protocol and policy, the
# state machine, and the linearizability checker itself
# (docs/LINEARIZABILITY.md §11) — the Phase 13 request-identity rules:
# deduplication, conflicts, the watermark, bounds and eviction, forwarding, the
# session client, the checker over logical operations and the session model
# (docs/DEDUP.md §9) — and the Phase 14 snapshot and compaction rules: the
# orderings of creation, publication, compaction and installation, the format's
# validation, recovery's reconciliation, the protocol, and the session table's
# place in the snapshot (docs/SNAPSHOTS.md §18).
#
# For each mutant it applies a real source edit that violates a specific Raft rule,
# runs the test(s) that should catch that violation, and requires them to FAIL (the
# mutant is "killed"). A mutant that survives — the killer tests still pass with the
# rule broken — is a hole in the suite and fails this runner. Every edit is reverted
# with `git checkout`, so the working tree is unchanged when this exits. This is not
# a permanent runtime flag or a source-string inspection: it exercises altered
# behaviour and proves the existing correctness suite detects it.
#
# Usage: scripts/mutation.sh          (from anywhere; requires a clean git working tree)
#        DRY=1 scripts/mutation.sh    (apply and revert every mutant without running
#                                      tests: checks every pattern still matches)
#
# A mutant whose edit does not compile is NOT killed: the runner reports it as a
# failure of the runner, never as a kill.

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

  if [ -n "${DRY:-}" ]; then
    echo "· $name: pattern applies"
    git checkout -- "$file" 2>/dev/null
    return
  fi

  # shellcheck disable=SC2086 # $pkg is a deliberate word-split package list
  if go test $pkg -run "$tests" -count=1 -timeout 300s >/tmp/mutation.$$.log 2>&1; then
    echo "✗ $name: SURVIVED — killer tests [$tests] still PASSED with the rule broken."
    FAIL=$((FAIL + 1))
  elif grep -qE '\[build failed\]|\[setup failed\]' /tmp/mutation.$$.log; then
    echo "✗ $name: the mutant does not compile — that is not a kill."
    grep -m 3 -E '\.go:[0-9]+' /tmp/mutation.$$.log
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
  'back := max(r.backupNextIndex(m.ConflictTerm, m.ConflictIndex), r.matchIndex[peer]+1)' \
  'back := max(r.nextIndex[peer]-1, r.matchIndex[peer]+1)' \
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
#     (Phase 14: the rule lives in progress(), shared with snapshot responses.)
mutant "stale-success-ignored" internal/raft/raft.go \
  '	if match <= r.matchIndex[peer] {
		return
	}' \
  '	if false && match <= r.matchIndex[peer] {
		return
	}' \
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
  '	rec.HardState.Commit = min(rec.HardState.Commit, rec.LastIndex())' \
  '	_ = rec.LastIndex()' \
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
  '		role:               Follower,' \
  '		role:               Leader,' \
  ./internal/raftsim 'TestCrashMatrix|TestCrashDuringSuffixReplacement'

# 32. Record an entry as applied BEFORE the state machine applies it, so a crash
#     between the two loses the application.
mutant "applied-recorded-only-after-apply" internal/raftnode/crashpoint.go \
  '		var result any
		if sm != nil {
			var err error
			if rsm != nil {
				result, err = rsm.ApplyResult(e.Index, e.Data)
			} else {
				err = sm.Apply(e.Index, e.Data)
			}
			if err != nil {
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
		var result any
		if sm != nil {
			var err error
			if rsm != nil {
				result, err = rsm.ApplyResult(e.Index, e.Data)
			} else {
				err = sm.Apply(e.Index, e.Data)
			}
			if err != nil {
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

# 41. A write whose index was taken by a DIFFERENT committed entry reports success
#     (and receives that other entry's result).
mutant "lost-write-is-not-success" internal/raftnode/waiters.go \
  '		case wt.term == term:' \
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
  '	payload, err := readFrame(c.conn, kindResponse, maxResponseFrame)
	if err != nil {
		c.dropConn()' \
  '	payload, err := readFrame(c.conn, kindResponse, maxResponseFrame)
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

# 60. The key-value codecs accept a length or term written in more bytes than it
#     needs (found by fuzzing in the Phase 12 gate): one command, two encodings.
mutant "codecs-accept-only-canonical-varints" internal/kv/command.go \
  '	if n > 0 && n != uvarintLen(v) {' \
  '	if false && n > 0 && n != uvarintLen(v) {' \
  ./internal/kv 'TestDecodeRejectsMalformedCommands|TestWireCodecsRoundTrip|FuzzDecodeIsTotal|FuzzDecodeRequestIsTotal|FuzzDecodeResponseIsTotal'

echo "== Phase 13: request identity, deduplication, forwarding and the session client =="

# 61. The dedup lookup is skipped: a retry of an executed request executes again.
mutant "a-retry-is-answered-from-its-original" internal/kv/store.go \
  '	if rec, ok := ss.results[c.RequestID]; ok {' \
  '	if rec, ok := ss.results[c.RequestID]; ok && false {' \
  "./internal/kv ./internal/raftsim" 'TestStoreAgreesWithTheSessionModel|TestUnknownWriteRetriedAfterLeaderCrashIsOneRequest|TestKVSimCommittedRequestRetriedAfterLeaderCrash'

# 62. A reused request id is matched without its fingerprint: a DIFFERENT
#     command under an executed id is answered as its duplicate.
mutant "conflicting-reuse-is-detected-by-fingerprint" internal/kv/store.go \
  '		if rec.fp == fp {' \
  '		if true {' \
  ./internal/kv 'TestStoreAgreesWithTheSessionModel|TestConflictingReuseAndIdentityScope'

# 63. A conflicting reuse is refused but TAKES EFFECT.
mutant "a-refused-conflict-has-no-effect" internal/kv/store.go \
  '		return Result{Decision: Conflict}' \
  '		s.write(c)
		return Result{Decision: Conflict}' \
  ./internal/kv 'TestStoreAgreesWithTheSessionModel|TestConflictingReuseAndIdentityScope'

# 64. The fingerprint omits the value: PUT(k, x) and PUT(k, y) under one id are
#     "the same request".
mutant "the-fingerprint-covers-the-whole-command" internal/kv/command.go \
  '	return sha256.Sum256(Command{Op: c.Op, Key: c.Key, Value: c.Value}.Encode())' \
  '	return sha256.Sum256(Command{Op: c.Op, Key: c.Key}.Encode())' \
  ./internal/kv 'TestConflictingReuseAndIdentityScope'

# 65. An unknown (evicted) session is silently re-created: its retries execute
#     again, their results forgotten with the eviction.
mutant "an-evicted-session-is-never-revived" internal/kv/store.go \
  '	if ss == nil {
		return Result{Decision: Expired}
	}' \
  '	if ss == nil {
		ss = &session{ackedBelow: 1, results: map[uint64]execution{}}
		s.sessions[c.ClientID] = ss
	}' \
  "./internal/kv ./internal/raftsim" 'TestStoreAgreesWithTheSessionModel|TestEvictedSessionIsRefusedNotReexecuted|TestKVSeededHistoriesAreLinearizable/kv-sessions-evict'

# 66. The watermark forgets the result AT it, not only below: a request still in
#     flight loses its result.
mutant "the-watermark-forgets-only-acknowledged-results" internal/kv/store.go \
  '			if rid < w {' \
  '			if rid <= w {' \
  "./internal/kv ./internal/raftsim" 'TestStoreAgreesWithTheSessionModel|TestReplayRebuildsTheSessionTable|TestKVSeededHistoriesAreLinearizable/kv-sessions'

# 67. A request below the watermark is executed instead of refused STALE.
mutant "a-request-below-the-watermark-is-stale" internal/kv/store.go \
  '	if c.RequestID < ss.ackedBelow {' \
  '	if false && c.RequestID < ss.ackedBelow {' \
  ./internal/kv 'TestStoreAgreesWithTheSessionModel|TestConflictingReuseAndIdentityScope'

# 68. The unacknowledged-result bound is not enforced (unbounded memory).
mutant "a-session-holds-at-most-maxunacked-results" internal/kv/store.go \
  '	if len(ss.results) >= s.limits.MaxUnacked {' \
  '	if false && len(ss.results) >= s.limits.MaxUnacked {' \
  ./internal/kv 'TestStoreAgreesWithTheSessionModel|TestSessionLimitRefusesRatherThanForgets'

# 69. LRU evicts the MOST recently used session — the one just registered.
mutant "eviction-takes-the-least-recently-used" internal/kv/store.go \
  '				if lru == 0 || ss.last < s.sessions[lru].last {' \
  '				if lru == 0 || ss.last > s.sessions[lru].last {' \
  "./internal/kv ./internal/raftsim" 'TestStoreAgreesWithTheSessionModel|TestEvictedSessionIsRefusedNotReexecuted|TestKVSeededHistoriesAreLinearizable/kv-sessions-evict'

# 70. A forwarded request is forwarded again (forwarding can loop).
mutant "a-forwarded-request-is-never-forwarded-again" internal/kv/server.go \
  '	if resp.Status != StatusNotLeader || via != "" || redirectOnly {' \
  '	if resp.Status != StatusNotLeader || redirectOnly {' \
  ./internal/kv 'TestForwardedRequestIsNeverForwardedAgain'

# 71. A forward that went unanswered is reported OK (the forwarder answers
#     before, or without, the leader's answer).
mutant "an-unanswered-forward-is-unknown" internal/kv/server.go \
  '		return Response{Status: StatusUnknown, Node: s.id, Leader: leader, Message: "forwarded to " + leader + "; no answer before the deadline"}' \
  '		return Response{Status: StatusOK, Node: s.id, Leader: leader, Message: "forwarded to " + leader + "; no answer before the deadline"}' \
  ./internal/kv 'TestForwardedRequestWhoseAnswerIsLostIsRetriedSafely'

# 72. The forwarder strips the request's identity: forwarded retries and
#     duplicates are anonymous, never deduplicated.
mutant "the-forwarder-keeps-the-identity" internal/kv/server.go \
  '	payload := encodeForward(fid, budget, req)' \
  '	payload := encodeForward(fid, budget, Request{Op: req.Op, Key: req.Key, Value: req.Value, Timeout: req.Timeout})' \
  ./internal/kv 'TestConcurrentDuplicatesAtTwoNodes|TestForwardedRequestWhoseAnswerIsLostIsRetriedSafely'

# 73. The session client sends every attempt after the first under a NEW
#     request id (so a retry of an unknown outcome is a new request).
mutant "a-retry-keeps-its-request-id" internal/kv/session.go \
  'RequestID: rid, AckedBelow' \
  'RequestID: rid + uint64(out.Attempts) - 1, AckedBelow' \
  ./internal/kv 'TestUnknownWriteRetriedAfterLeaderCrashIsOneRequest|TestForwardedRequestWhoseAnswerIsLostIsRetriedSafely'

# 74. The session client reports a request whose attempts went unanswered as
#     KNOWN (a definite no-effect) when its attempts run out.
mutant "an-unanswered-request-is-not-known" internal/kv/session.go \
  '	out.Known = !unknown
	if unknown {' \
  '	out.Known = true
	if unknown {' \
  ./internal/kv 'TestDuplicateSentBeforeTheOriginalCommits'

# 75. The session watermark ignores requests in flight: a concurrent request's
#     AckedBelow passes an unanswered one, whose result the server forgets.
mutant "the-watermark-waits-for-requests-in-flight" internal/kv/session.go \
  '		if rid < w {
			w = rid' \
  '		if false && rid < w {
			w = rid' \
  ./internal/kv 'TestConcurrentRequestsFromOneSession'

# 76. dkvd ignores -session-max / -session-max-unacked.
mutant "dkvd-applies-the-configured-session-limits" cmd/dkvd/main.go \
  '	store := kv.NewStoreWithLimits(limits)' \
  '	store := kv.NewStore()' \
  ./tests/integration 'TestRealSessionContractSurvivesFullClusterRestart'

# 87. Request validation drops the watermark bound: a request with AckedBelow
#     above its RequestID is proposed, and every replica refuses the entry at
#     apply as malformed.
mutant "requests-are-validated-before-they-are-proposed" internal/kv/api.go \
  '	if r.RequestID == 0 || r.AckedBelow == 0 || r.AckedBelow > r.RequestID {' \
  '	if r.RequestID == 0 || r.AckedBelow == 0 {' \
  ./internal/kv 'TestRequestValidationRejectsEveryOutOfContractField|TestValidatedRequestsAlwaysApply'

# 88. A duplicate is answered with its OWN entry's index instead of the
#     original execution's (a result that belongs to a different entry).
mutant "a-duplicate-reports-the-original-execution" internal/kv/store.go \
  '			return Result{Decision: Duplicate, Index: rec.index}' \
  '			return Result{Decision: Duplicate, Index: index}' \
  "./internal/kv ./tests/integration" 'TestStoreAgreesWithTheSessionModel|TestConcurrentDuplicatesAtTwoNodes|TestRealConcurrentDuplicatesThroughEveryNode'

# 89. A restart marks the recovered commit applied instead of replaying it into
#     the fresh state machine: the session table (with the rest of the state)
#     is lost, and a retry after the restart is refused or re-executed.
mutant "a-restart-rebuilds-the-session-table-by-replay" internal/raftnode/node.go \
  '			return fail(fmt.Errorf("raftnode: recovered commit invalid: %w", err))
		}
	}' \
  '			return fail(fmt.Errorf("raftnode: recovered commit invalid: %w", err))
		}
	}
	_ = mlog.Apply(commit)' \
  "./internal/kv ./internal/raftsim ./tests/integration" 'TestRetryAfterEveryNodeRestarts|TestKVSimSessionsSurviveARestartOfEveryNode|TestRealSessionContractSurvivesFullClusterRestart'

# 90. A duration past time.Duration is decoded (it overflows, and the frame
#     re-encodes to different bytes — the canonicality bug fuzzing found).
mutant "durations-that-overflow-are-protocol-errors" internal/kv/wire.go \
  '	if d.err == nil && ms > maxMillis {' \
  '	if false && d.err == nil && ms > maxMillis {' \
  ./internal/kv 'TestDurationsThatOverflowAreProtocolErrors|FuzzDecodeRequestIsTotal'

# 91. raftnode.Start reads the core after the actor goroutine owns it (the data
#     race the Phase 13 gate's race suite found). Killed only under the race
#     detector, so "-race" rides in the package list.
mutant "start-reads-the-core-before-the-actor-owns-it" internal/raftnode/node.go \
  '	term, last := rc.Core.Term(), rc.Core.LastIndex()
	n.wg.Add(2)
	go n.receiveLoop()
	go n.actorLoop()' \
  '	n.wg.Add(2)
	go n.receiveLoop()
	go n.actorLoop()
	term, last := rc.Core.Term(), rc.Core.LastIndex()' \
  "-race ./internal/raftnode" 'TestStartDoesNotTouchTheCoreOnceTheActorOwnsIt'

# 92. The session client backs off a fixed interval (no doubling): it spends
#     its attempts during an ordinary election and gives up on a request that
#     would complete moments later (what the Phase 13 gate found).
mutant "the-session-back-off-doubles" internal/kv/session.go \
  '		d *= 2' \
  '		d *= 1' \
  ./internal/kv 'TestSessionRidesOutAnElectionLongerThanItsAttemptsTimesBackoff|TestBackoffDoublesUpToItsCap'

echo "== Phase 13: the checker over logical operations, and the reference model =="

# 77. Request identity is not scoped by client: the same RequestID from two
#     clients is merged into one request.
mutant "checker-identity-includes-the-client" internal/lincheck/logical.go \
  '		k := ident{op.ClientID, op.RequestID}' \
  '		k := ident{1, op.RequestID}' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestCheckerAgreesWithOracleOnRequestIdentity'

# 78. Two different commands acknowledged under one identity are checked around
#     instead of reported (an accepted conflicting reuse passes).
mutant "checker-reports-an-accepted-conflict" internal/lincheck/logical.go \
  '		if owner != nil && *owner != c {
			return Op{}, fmt.Errorf("two different commands were both acknowledged' \
  '		if false && owner != nil && *owner != c {
			return Op{}, fmt.Errorf("two different commands were both acknowledged' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestLogicalRefusesWhatTheContractForbidsOrLeavesOpen'

# 79. A merged request is invoked at its LAST send, not its first (its effect
#     from an early send becomes impossible).
mutant "checker-invokes-a-request-at-its-first-send" internal/lincheck/logical.go \
  '		if op.Invoke < m.Invoke {' \
  '		if op.Invoke > m.Invoke || m.Invoke == math.MaxInt64 {' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestCheckerAgreesWithOracleOnRequestIdentity'

# 80. A merged request completes at its LAST acknowledgement, not its first
#     (reads between the two may miss an acknowledged write).
mutant "checker-completes-a-request-at-its-first-ok" internal/lincheck/logical.go \
  '			if op.Complete < m.Complete {' \
  '			if op.Complete > m.Complete || m.Complete == math.MaxInt64 {' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestCheckerAgreesWithOracleOnRequestIdentity'

# 81. Sends carrying another command are merged in (a refused conflicting reuse
#     becomes part of the request).
mutant "checker-excludes-sends-of-another-command" internal/lincheck/logical.go \
  '		if commandOf(op) != *owner {
			continue' \
  '		if false && commandOf(op) != *owner {
			continue' \
  ./internal/lincheck 'TestKnownGoodAndKnownBadCorpus|TestCheckerAgreesWithOracleOnRequestIdentity'

# 82. The reference session model deduplicates nothing (the store/model
#     differential must cut both ways).
mutant "model-answers-a-retry-from-its-original" internal/lincheck/session.go \
  '		if r.rid == c.RequestID {' \
  '		if false && r.rid == c.RequestID {' \
  "./internal/lincheck ./internal/kv" 'TestSessionModelFollowsTheContract|TestStoreAgreesWithTheSessionModel'

echo "== Phase 13: the same rules, killed by client-visible histories of real processes ALONE =="

# 83. No dedup lookup, on real processes: in a crash window where the original
#     committed, the history — A read, then B, then A again after the retry —
#     is rejected by the checker, before any explicit assertion runs.
mutant "a-retry-is-answered-from-its-original (real processes)" internal/kv/store.go \
  '	if rec, ok := ss.results[c.RequestID]; ok {' \
  '	if rec, ok := ss.results[c.RequestID]; ok && false {' \
  ./tests/integration 'TestRealSessionRetryAcrossCrashWindows'

# 84. The forwarder strips identity, on real processes: copies of one request
#     sent through every node execute more than once.
mutant "the-forwarder-keeps-the-identity (real processes)" internal/kv/server.go \
  '	payload := encodeForward(fid, budget, req)' \
  '	payload := encodeForward(fid, budget, Request{Op: req.Op, Key: req.Key, Value: req.Value, Timeout: req.Timeout})' \
  ./tests/integration 'TestRealConcurrentDuplicatesThroughEveryNode'

# 85. A new request id for a retry after a TRANSPORT failure (the connection
#     died mid-request). In-process servers never fail at the transport, so no
#     in-process test reaches this branch; real processes do: the retry after
#     a forwarder died before relaying executes again.
mutant "a-retry-after-a-dead-connection-keeps-its-request-id (real processes)" internal/kv/session.go \
  '			unknown = true // sent, no answer: retry the same request' \
  '			unknown = true // sent, no answer: retry the same request
			rid = s.Reserve()' \
  ./tests/integration 'TestRealForwarderDiesBeforeRelaying'

# 86. An evicted session revived, on real processes, through a full restart.
mutant "an-evicted-session-is-never-revived (real processes)" internal/kv/store.go \
  '	if ss == nil {
		return Result{Decision: Expired}
	}' \
  '	if ss == nil {
		ss = &session{ackedBelow: 1, results: map[uint64]execution{}}
		s.sessions[c.ClientID] = ss
	}' \
  ./tests/integration 'TestRealSessionContractSurvivesFullClusterRestart'

echo "== Phase 14: snapshots and log compaction (docs/SNAPSHOTS.md §18) =="

# 93. Compact before the snapshot is durable: the log is rewritten without the
#     prefix before the snapshot covering it is published, so a crash between
#     leaves the only record of the prefix nowhere.
mutant "the-snapshot-is-durable-before-the-log-is-compacted" internal/raftnode/snapshot.go \
  '	if err := at.hit(BeforeSnapshotPublish, idx); err != nil {
		return err
	}
	if err := s.Files.Publish(file); err != nil {' \
  '	if err := at.hit(BeforeSnapshotPublish, idx); err != nil {
		return err
	}
	s.meta = meta
	if err := d.compact(core, at); err != nil {
		return err
	}
	if err := s.Files.Publish(file); err != nil {' \
  "./internal/raftnode ./internal/raftsim" 'TestSnapshotCrashPointsRecover|TestSnapshotCrashMatrix'

# 94. Compact beyond the applied index, in memory.
mutant "compaction-never-passes-the-applied-index" internal/replication/log.go \
  '	case index > l.applied:
		return ErrCompactBeyondApplied' \
  '	case false && index > l.applied:
		return ErrCompactBeyondApplied' \
  ./internal/replication 'TestCompactDiscardsOnlyTheAppliedPrefix|TestAgainstReferenceModel'

# 95. Compact beyond the durable commit, on disk: an uncommitted entry — one a
#     new leader may replace — is discarded into a snapshot.
mutant "durable-compaction-never-passes-the-commit" internal/raftlog/raftlog.go \
  '	case index > rec.HardState.Commit:' \
  '	case false && index > rec.HardState.Commit:' \
  ./internal/raftlog 'TestCompactAndInstallRefuseImpossibleBoundaries'

# 96. A restart resumes the in-memory log at the wrong boundary.
mutant "recovery-resumes-at-the-log-boundary" internal/raftnode/node.go \
  '		if err := mlog.InstallSnapshot(b.Index, b.Term); err != nil {' \
  '		if err := mlog.InstallSnapshot(b.Index+1, b.Term); err != nil {' \
  ./internal/raftnode 'TestRecoverFromSnapshotAndSuffix|TestLaggingFollowerCatchesUpBySnapshot'

# 97. A restart recovers the boundary with the wrong term.
mutant "recovery-keeps-the-boundary-term" internal/raftnode/node.go \
  '		if err := mlog.InstallSnapshot(b.Index, b.Term); err != nil {' \
  '		if err := mlog.InstallSnapshot(b.Index, b.Term+1); err != nil {' \
  ./internal/raftsim 'TestSimRestartFromSnapshotAfterPowerLoss|TestSnapshotCrashMatrix'

# 98. A corrupt published snapshot is silently ignored instead of refused.
mutant "a-corrupt-snapshot-is-refused-not-ignored" internal/snapshot/files.go \
  '		return Meta{}, nil, nil, true, fmt.Errorf("%s: %w", f.Path(), err)' \
  '		return Meta{}, nil, nil, false, nil' \
  "./internal/snapshot ./internal/raftsim" 'TestLoadRefusesACorruptPublishedSnapshot|TestSimCorruptPublishedSnapshotRefusesToStart'

# 99. The state's SHA-256 is not checked (the records' CRCs alone pass).
mutant "the-snapshot-checksum-is-verified" internal/snapshot/snapshot.go \
  '	if sha256.Sum256(data) != sum {' \
  '	if false && sha256.Sum256(data) != sum {' \
  ./internal/snapshot 'TestCorpus'

# 100. Bytes after the footer are accepted (what FuzzDecode found).
mutant "nothing-may-follow-the-footer" internal/snapshot/snapshot.go \
  '	if rd.NextOffset() != int64(len(b)) {' \
  '	if false && rd.NextOffset() != int64(len(b)) {' \
  ./internal/snapshot 'TestCorpus'

# 101. The snapshot drops the session table: after a restore, a retry of an
#      executed request is no longer recognized.
mutant "the-snapshot-carries-the-session-table" internal/kv/snapshot.go \
  '	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {' \
  '	ids = nil
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {' \
  "./internal/kv ./internal/raftsim" 'TestSnapshotPlusSuffixEqualsFullReplay|TestSimDedupSurvivesSnapshotCompactionAndRestart'

# 102. A follower refuses a snapshot its commit already covers instead of
#      answering it covered (the leader then never ends its offer).
mutant "a-follower-answers-a-covered-snapshot" internal/raft/raft.go \
  '	if m.SnapshotIndex <= commit {' \
  '	if false && m.SnapshotIndex <= commit {' \
  ./internal/raft 'TestFollowerIgnoresASnapshotItAlreadyCovers'

# 103. A stale snapshot resets the log: an install at or below the commit
#      index is accepted, and entries not yet applied are skipped.
mutant "a-stale-snapshot-never-resets-the-log" internal/replication/log.go \
  '	if index <= l.commit {
		return ErrStaleSnapshot' \
  '	if false && index <= l.commit {
		return ErrStaleSnapshot' \
  ./internal/replication 'TestInstallSnapshotKeepsOnlyAMatchingSuffix|TestAgainstReferenceModel'

# 104. The leader ignores that the entries a follower needs are compacted: it
#      never offers the snapshot, and the follower never catches up.
mutant "a-leader-offers-a-snapshot-when-entries-are-compacted" internal/raft/raft.go \
  '	if base, baseTerm := r.log.Boundary(); next <= base {' \
  '	if base, baseTerm := r.log.Boundary(); false && next <= base {' \
  "./internal/raft ./internal/raftsim" 'TestLaggingFollowerCatchesUpBySnapshot|TestSimLaggingFollowerInstallsASnapshot'

# 105. A transfer is accepted before it is complete.
mutant "only-a-complete-transfer-is-accepted" internal/snapshot/transfer.go \
  '	if t.received < t.total {
		return nil, nil
	}' \
  '	if false && t.received < t.total {
		return nil, nil
	}' \
  "./internal/snapshot ./internal/raftsim" 'TestReceiverAcceptsOnlyAnInOrderCompleteSnapshot|TestSimLaggingFollowerInstallsASnapshot'

# 106. Chunks are accepted out of order (a duplicate or skipped chunk is written).
mutant "chunks-are-accepted-in-order-only" internal/snapshot/transfer.go \
  '		t.total != c.Total || t.received != c.Offset {' \
  '		t.total != c.Total {' \
  "./internal/snapshot ./internal/raftsim" 'TestReceiverAcceptsOnlyAnInOrderCompleteSnapshot|TestSimDuplicatedAndReorderedTransfers'

# 107. An install discards a valid suffix, in memory.
mutant "an-install-keeps-a-matching-suffix" internal/replication/log.go \
  '	if t, err := l.Term(index); err == nil && t == term && index <= l.LastIndex() {' \
  '	if t, err := l.Term(index); false && err == nil && t == term && index <= l.LastIndex() {' \
  "./internal/replication ./internal/raft" 'TestInstallSnapshotKeepsOnlyAMatchingSuffix|TestFollowerInstallKeepsAMatchingSuffix'

# 108. Replaying a boundary record discards a valid suffix, on disk.
mutant "replay-keeps-a-matching-suffix" internal/raftlog/raftlog.go \
  '		if rec.Entries[b.Index-cur.Index-1].Term == b.Term {' \
  '		if false && rec.Entries[b.Index-cur.Index-1].Term == b.Term {' \
  ./internal/raftlog 'TestInstallAppliesTheInstallRule|TestRandomLogHistoriesMatchTheModel'

# 109. A restart forgets its snapshot: the state machine is validated against
#      it but not restored, while the applied index says it was.
mutant "restart-restores-the-published-snapshot" internal/raftnode/node.go \
  '		if err := ssm.RestoreSnapshot(meta.Index, data); err != nil {' \
  '		if err := ssm.ValidateSnapshot(meta.Index, data); err != nil {' \
  "./internal/raftnode ./internal/raftsim" 'TestRecoverFromSnapshotAndSuffix|TestSimRestartFromSnapshotAfterPowerLoss'

# 110. A restart with a compacted log and no snapshot starts anyway — from an
#      empty state machine that believes it applied the prefix.
mutant "recovery-refuses-a-compacted-log-without-a-snapshot" internal/raftnode/node.go \
  '	case rec.Boundary.Index > 0:
		return fail(' \
  '	case false && rec.Boundary.Index > 0:
		return fail(' \
  ./internal/raftnode 'TestRecoverRefusesAContradictedSnapshot'

# 111. Startup deletes the published snapshot along with the temporaries.
mutant "startup-keeps-the-published-snapshot" internal/snapshot/files.go \
  '	for _, p := range []string{f.TmpPath(), f.RecvPath()} {' \
  '	for _, p := range []string{f.TmpPath(), f.RecvPath(), f.Path()} {' \
  ./internal/raftnode 'TestRecoverFromSnapshotAndSuffix'

# 112. An install publishes the snapshot before the new term is durable: a crash
#      between leaves a snapshot whose term exceeds the durable currentTerm.
mutant "install-persists-the-term-before-publishing" internal/raftnode/snapshot.go \
  '	if hs != nil {
		if cur := d.Log.HardState(); hs.Term != cur.Term || hs.Vote != cur.Vote {
			if err := d.Log.Save(&raftlog.HardState{Term: hs.Term, Vote: hs.Vote, Commit: cur.Commit}, nil); err != nil {
				return err
			}
		}
	}
	if err := at.hit(BeforeInstallPublish, meta.Index); err != nil {
		return err
	}
	if err := s.Files.PublishReceived(); err != nil {
		return err
	}' \
  '	if err := at.hit(BeforeInstallPublish, meta.Index); err != nil {
		return err
	}
	if err := s.Files.PublishReceived(); err != nil {
		return err
	}
	if hs != nil {
		if cur := d.Log.HardState(); hs.Term != cur.Term || hs.Vote != cur.Vote {
			if err := d.Log.Save(&raftlog.HardState{Term: hs.Term, Vote: hs.Vote, Commit: cur.Commit}, nil); err != nil {
				return err
			}
		}
	}' \
  ./internal/raftnode 'TestInstallOrderIsTermPublishBoundary'

# 113. An install records the boundary before publishing the snapshot: a crash
#      between leaves a log compacted past its only snapshot.
mutant "install-publishes-before-recording-the-boundary" internal/raftnode/snapshot.go \
  '	if err := s.Files.PublishReceived(); err != nil {
		return err
	}
	s.meta, s.file = st.Meta, nil
	if err := at.hit(AfterInstallPublish, meta.Index); err != nil {
		return err
	}
	if err := d.Log.Install(meta.Index, meta.Term); err != nil {
		return err
	}' \
  '	if err := d.Log.Install(meta.Index, meta.Term); err != nil {
		return err
	}
	if err := s.Files.PublishReceived(); err != nil {
		return err
	}
	s.meta, s.file = st.Meta, nil
	if err := at.hit(AfterInstallPublish, meta.Index); err != nil {
		return err
	}' \
  "./internal/raftnode ./internal/raftsim" 'TestInstallOrderIsTermPublishBoundary|TestSnapshotCrashMatrix'

# 114. Open fsyncs the directory only when it creates the log (what the crash
#      test found): after a compaction's rename survived a process crash, an
#      acknowledged append is lost with the name on a power loss.
mutant "open-makes-the-log-name-durable" internal/raftlog/raftlog.go \
  '	if err := fsys.SyncDir(filepath.Dir(path)); err != nil {
		_ = f.Close()
		return nil, nil, err
	}' \
  '	if _, statErr := fsys.Stat(path + ".never"); statErr == nil {
		_ = f.Close()
		return nil, nil, statErr
	}' \
  ./internal/raftlog 'TestCompactIsAtomicUnderEveryCrash'

# 115. A Ready's installed snapshot is not made durable and active before its
#      response leaves.
mutant "an-installed-snapshot-is-made-durable-before-the-response" internal/raftnode/crashpoint.go \
  '		if rd.Snapshot != nil {
			in, ok := st.(Installer)' \
  '		if false && rd.Snapshot != nil {
			in, ok := st.(Installer)' \
  "./internal/raftnode ./internal/raftsim" 'TestLaggingFollowerCatchesUpBySnapshot|TestSimLaggingFollowerInstallsASnapshot'

# 116. A transfer's state is not validated before the core sees it.
mutant "a-transfer-is-validated-before-the-core-sees-it" internal/raftnode/snapshot.go \
  '	if err := s.SM.ValidateSnapshot(got.Meta.Index, got.Data); err != nil {' \
  '	if err := error(nil); err != nil {' \
  ./internal/raftnode 'TestReceiveRefusesWhatCannotBeInstalled'

# 117. Another group's snapshot is accepted.
mutant "a-transfer-must-be-this-groups" internal/raftnode/snapshot.go \
  '	if !snapshot.SameGroup(got.Meta.Members, s.Members) {' \
  '	if false && !snapshot.SameGroup(got.Meta.Members, s.Members) {' \
  ./internal/raftnode 'TestReceiveRefusesWhatCannotBeInstalled'

# 118. A write whose index an install replaced is reported as applied — with no
#      result: its outcome is unknown, not success.
mutant "an-install-leaves-a-write-unknown" internal/raftnode/waiters.go \
  '				wt.ch <- Outcome{Index: idx, Err: ErrSuperseded}' \
  '				wt.ch <- Outcome{Index: idx}' \
  ./internal/raftnode 'TestWaitersSettleOnInstall'

# 119. Recovery does not complete an install that crashed after publishing.
mutant "recovery-completes-an-interrupted-install" internal/raftnode/node.go \
  '		if repair {' \
  '		if false && repair {' \
  "./internal/raftnode ./internal/raftsim" 'TestInstallCrashPointsRecover|TestSnapshotCrashMatrix'

# 122. A stale rejection backs nextIndex up to or below matchIndex: with the
#      prefix compacted the peer is offered a snapshot its commit covers and is
#      stranded (what the 200-seed gate found, kv-snapshots-partitions seed 100).
mutant "a-stale-rejection-never-backs-up-below-the-match" internal/raft/raft.go \
  '	back := max(r.backupNextIndex(m.ConflictTerm, m.ConflictIndex), r.matchIndex[peer]+1)' \
  '	back := r.backupNextIndex(m.ConflictTerm, m.ConflictIndex)' \
  "./internal/raft ./internal/raftsim" 'TestStaleRejectionNeverBacksUpBelowTheMatch|TestSnapshotRegressionSeeds'

# 123. The simulator duplicates a snapshot chunk without its payload (the
#      harness bug seed 153 found): a duplicated transfer must still be chunks.
mutant "sim-duplicates-a-chunk-with-its-payload" internal/raftsim/cluster.go \
  '		c.flights = append(c.flights, &flight{seq: c.seq, msg: f.msg, chunk: f.chunk})' \
  '		c.flights = append(c.flights, &flight{seq: c.seq, msg: f.msg})' \
  ./internal/raftsim 'TestSimDuplicatedAndReorderedTransfers|TestSnapshotRegressionSeeds'

# 124. The recorder's History shares the attempts of ops in flight (the
#      latent race the Phase 14 leader-partition fix exposed).
mutant "a-history-is-a-snapshot" internal/lincheck/history.go \
  '		cp.Attempts = append([]Attempt(nil), op.Attempts...)' \
  '		cp.Attempts = op.Attempts' \
  ./internal/lincheck 'TestHistoryIsASnapshotWhileClientsRecord'

echo "== Phase 14: the same rules, killed by real processes ALONE =="

# 120. The session table left out of the snapshot, on real processes: the
#      retry of a request whose entry was compacted away on every node, after
#      every process restarted, is executed again instead of answered.
mutant "the-snapshot-carries-the-session-table (real processes)" internal/kv/snapshot.go \
  '	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {' \
  '	ids = nil
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {' \
  ./tests/integration 'TestRealRetryAfterSnapshotIsADuplicate'

# 121. Recovery does not complete an interrupted install, on real processes.
mutant "recovery-completes-an-interrupted-install (real processes)" internal/raftnode/node.go \
  '		if repair {' \
  '		if false && repair {' \
  ./tests/integration 'TestRealFollowerCrashesDuringSnapshotInstallation'

echo "== $KILLED/$TOTAL mutants killed =="
rm -f /tmp/mutation.$$.log
if [ "$FAIL" -ne 0 ]; then
  echo "MUTATION TESTING FAILED: $FAIL mutant(s) survived or could not be applied." >&2
  exit 1
fi
echo "MUTATION TESTING PASSED: every mutant was killed."
