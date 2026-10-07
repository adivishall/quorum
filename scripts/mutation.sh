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
# place in the snapshot (docs/SNAPSHOTS.md §18) — and the Phase 15 membership
# and multi-Raft rules: joint-consensus quorums, who may campaign, vote and
# lead, the configuration's history across snapshots and recovery, the group
# identity, and group routing (docs/MEMBERSHIP.md §9, docs/MULTI_RAFT.md §8).
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
#        ONLY=regex scripts/mutation.sh  (only the mutants whose names match)
#
# A mutant whose edit does not compile is NOT killed: the runner reports it as a
# failure of the runner, never as a kill.

set -uo pipefail
cd "$(dirname "$0")/.."

# Each mutant edits a tracked source file and reverts it with `git checkout`. That
# is only safe if the file has no uncommitted changes to lose, so each mutant
# checks its own target file is clean before editing (below). Untracked files (e.g.
# an as-yet-uncommitted copy of this script) do not affect revert safety.

LOG="${TMPDIR:-/tmp}/mutation.$$.log"
# CURRENT is the file a mutant has edited and not yet reverted: the only file
# the exit trap may check out. (A list of every file any mutant touched made
# the trap check out, at a normal exit, files reverted long before — losing
# whatever a developer had edited in them during the run: a review of the
# runner.)
CURRENT=""
revert_current() {
  if [ -n "$CURRENT" ]; then
    git checkout -- "$CURRENT" 2>/dev/null
    CURRENT=""
  fi
}
trap revert_current EXIT

FAIL=0
KILLED=0
TOTAL=0

# mutant NAME FILE SEARCH REPLACE PKGS TESTREGEX   (PKGS may list several packages)
mutant() {
  local name="$1" file="$2" search="$3" replace="$4" pkg="$5" tests="$6"
  if [ -n "${ONLY:-}" ] && ! [[ "$name" =~ $ONLY ]]; then
    return
  fi
  TOTAL=$((TOTAL + 1))

  # Revert safety rests on git: the target must be tracked (git diff is blind
  # to an untracked file, so a mutation of one went unnoticed and was never
  # reverted — audit) and clean (a checkout must lose nothing).
  if ! git ls-files --error-unmatch -- "$file" >/dev/null 2>&1; then
    echo "✗ $name: $file is not tracked by git; refusing to mutate (revert safety)."
    FAIL=$((FAIL + 1))
    return
  fi
  if ! git diff --quiet -- "$file"; then
    echo "✗ $name: $file has uncommitted changes; refusing to mutate (revert safety)."
    FAIL=$((FAIL + 1))
    return
  fi
  local sites
  sites=$(S="$search" perl -0ne '$n = () = /\Q$ENV{S}\E/g; print $n' "$file")
  if [ "${sites:-0}" -gt 1 ]; then
    echo "⚠ $name: the pattern matches $sites sites in $file; only the first is mutated."
  fi
  # Only a file this mutant is about to edit — clean, so a checkout loses
  # nothing — is the exit trap's to revert, until it is reverted. (Recording
  # it before the check above made the trap check out a refused, dirty file
  # at exit, discarding its uncommitted changes: found in Phase 15.)
  CURRENT="$file"

  S="$search" R="$replace" perl -0pi -e 's/\Q$ENV{S}\E/$ENV{R}/' "$file"
  if git diff --quiet -- "$file"; then
    echo "✗ $name: PATTERN DID NOT MATCH in $file — cannot mutate (source changed?)."
    FAIL=$((FAIL + 1))
    revert_current
    return
  fi

  if [ -n "${DRY:-}" ]; then
    echo "· $name: pattern applies"
    revert_current
    return
  fi

  # shellcheck disable=SC2086 # $pkg is a deliberate word-split package list
  if go test $pkg -run "$tests" -count=1 -timeout 300s >"$LOG" 2>&1; then
    echo "✗ $name: SURVIVED — killer tests [$tests] still PASSED with the rule broken."
    FAIL=$((FAIL + 1))
  elif grep -qE '\[build failed\]|\[setup failed\]|go build dkvd: |lab: building dkvd: ' "$LOG"; then
    # A test that builds dkvd itself (tests/integration, internal/lab) fails
    # its test, not its package, when the mutant breaks dkvd's build.
    echo "✗ $name: the mutant does not compile — that is not a kill."
    grep -m 3 -E '\.go:[0-9]+' "$LOG"
    FAIL=$((FAIL + 1))
  elif ! grep -qE -- '--- FAIL: |^panic: |^fatal error: ' "$LOG"; then
    # Attribution (audit): a kill is a failing test — a test's FAIL, or the
    # binary dying in one (a panic, a timeout). A run that failed without
    # either failed somewhere else.
    echo "✗ $name: the run failed without a failing test — not attributable to the mutant."
    tail -n 5 "$LOG"
    FAIL=$((FAIL + 1))
  else
    revert_current
    # Real-process killers run on real timing; with CONFIRM=1, every killer.
    # A kill counts only if the same tests pass on the clean tree, so a
    # flaky failure can never pass for one (audit).
    if [ -n "${CONFIRM:-}" ] || [[ "$pkg" == *tests/integration* ]]; then
      # shellcheck disable=SC2086
      if ! go test $pkg -run "$tests" -count=1 -timeout 300s >"$LOG.clean" 2>&1; then
        echo "✗ $name: [$tests] fail on the clean tree too — a flaky or broken test is not a kill."
        tail -n 5 "$LOG.clean"
        FAIL=$((FAIL + 1))
        return
      fi
      echo "✓ $name: killed by [$tests] (they pass on the clean tree)."
    else
      echo "✓ $name: killed by [$tests]."
    fi
    KILLED=$((KILLED + 1))
  fi
  revert_current
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
#    (Phase 15: any node grants — one vote per term — and only voters' votes count.)
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
	case ob.ch <- m:
	default:' \
  '	n.sendMessage(m)
	return
	select {
	case ob.ch <- m:
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
#     (Phase 15: the host's supervisor closes fatal when group 0 fails.)
mutant "dkvd-exits-on-fail-stop" cmd/dkvd/main.go \
  '	case <-ctx.Done():
	case <-fatal:
		code = 1' \
  '	case <-ctx.Done():
	case <-make(chan struct{}):
		code = 1' \
  ./cmd/dkvd 'TestRaftModeExitsNonZeroWhenTheLogFails'

# 16. Duplicated vote responses from one voter are counted as separate votes.
#     (Phase 15: votes are a set over the configuration's voters, so the
#     Phase 10 form — a fresh map key per copy — counts nothing at all; a
#     repeated grant now credits a voter that has not voted.)
mutant "duplicate-votes-idempotent" internal/raft/raft.go \
  '		r.votesGranted[m.From] = true
		r.maybeBecomeLeader()' \
  '		if r.votesGranted[m.From] {
			for _, id := range r.conf.VoterIDs() {
				if !r.votesGranted[id] {
					r.votesGranted[id] = true
					break
				}
			}
		}
		r.votesGranted[m.From] = true
		r.maybeBecomeLeader()' \
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
  '		ID: n.id, Peers: peers, Join: peers == nil, LogPath: logPath, FS: n.inj,' \
  '		ID: n.id, Peers: peers, Join: peers == nil, LogPath: logPath, FS: fault.NewMemFS(),' \
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
		if sleepCtx(ctx, t.cfg.DialRetryInterval) {
			return
		}' \
  '		t.serve(peer, nc, "outbound") // blocks until the connection dies' \
  ./internal/transport 'TestDialerBacksOffWhenPeerKeepsClosingConnections'

# 24. Ignore the read idle deadline, so a silent (established-but-dead) connection
#     blocks the reader forever and the peer is never reconnected (Phase 10, bug 6).
mutant "read-idle-timeout-detects-dead-conn" internal/transport/transport.go \
  '	if t.cfg.ReadIdleTimeout > 0 {
		r = idleReader{c.nc, t.cfg.ReadIdleTimeout}
	}' \
  '	if false {
		r = idleReader{c.nc, t.cfg.ReadIdleTimeout}
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
			return none, ErrTermRegression
		}' \
  '		if false && c.Term < lt {
			return none, ErrTermRegression
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
			cmd := e.Data
			if e.Type != replication.EntryNormal {
				cmd = nil
			}
			switch {
			case csm != nil:
				result, err = csm.ApplyEntry(e.Index, e.Term, cmd)
			case rsm != nil:
				result, err = rsm.ApplyResult(e.Index, cmd)
			default:
				err = sm.Apply(e.Index, cmd)
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
			cmd := e.Data
			if e.Type != replication.EntryNormal {
				cmd = nil
			}
			switch {
			case csm != nil:
				result, err = csm.ApplyEntry(e.Index, e.Term, cmd)
			case rsm != nil:
				result, err = rsm.ApplyResult(e.Index, cmd)
			default:
				err = sm.Apply(e.Index, cmd)
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
  'seq: r.hbSeq})' \
  'seq: r.hbSeq - 1})' \
  "./internal/raft ./internal/raftsim" 'TestAcksFromBeforeTheReadDoNotConfirmIt|TestKVStaleLeaderReadIsNeverServed'

# 37. A ReadIndex confirmed without a quorum (the leader alone suffices).
mutant "readindex-needs-a-quorum" internal/raft/raft.go \
  '		if !r.hasQuorum(func(id NodeID) bool { return id == r.id || r.ackSeq[id] >= p.seq }) {' \
  '		if false && !r.hasQuorum(func(id NodeID) bool { return id == r.id || r.ackSeq[id] >= p.seq }) {' \
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
#     (Phase 15: every group's server is attached to the front with its store.)
mutant "dkvd-serves-the-replicated-state-machine" cmd/dkvd/main.go \
  'front.Attach(g, node, sm.(kv.Machine))' \
  'front.Attach(g, node, kv.NewStore())' \
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
  '		if !r.hasQuorum(func(id NodeID) bool { return id == r.id || r.ackSeq[id] >= p.seq }) {' \
  '		if false && !r.hasQuorum(func(id NodeID) bool { return id == r.id || r.ackSeq[id] >= p.seq }) {' \
  ./tests/integration 'TestRealMinorityLeaderWithAFollowerNeverServesARead'

# 55. The same, in the simulator's scripted minority-leader attack.
mutant "readindex-needs-a-quorum (simulated history)" internal/raft/raft.go \
  '		if !r.hasQuorum(func(id NodeID) bool { return id == r.id || r.ackSeq[id] >= p.seq }) {' \
  '		if false && !r.hasQuorum(func(id NodeID) bool { return id == r.id || r.ackSeq[id] >= p.seq }) {' \
  ./internal/raftsim 'TestKVMinorityLeaderWithAFollowerNeverServesARead'

# 56. Pre-read acknowledgements confirm the read: the simulated stale leader
#     serves its old value once the delayed acks arrive.
mutant "readindex-ignores-acks-sent-before-the-read (simulated history)" internal/raft/raft.go \
  'seq: r.hbSeq})' \
  'seq: r.hbSeq - 1})' \
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
  '		return Result{Decision: Conflict}, ef' \
  '		return Result{Decision: Executed, Index: index}, ef' \
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
		return Result{Decision: Expired}, effect{}
	}' \
  '	if ss == nil {
		ss = &session{ackedBelow: 1, results: map[uint64]execution{}}
		t.sessions[c.ClientID] = ss
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
  '	if len(ss.results) >= t.limits.MaxUnacked {' \
  '	if false && len(ss.results) >= t.limits.MaxUnacked {' \
  ./internal/kv 'TestStoreAgreesWithTheSessionModel|TestSessionLimitRefusesRatherThanForgets'

# 69. LRU evicts the MOST recently used session — the one just registered.
mutant "eviction-takes-the-least-recently-used" internal/kv/store.go \
  '				if lru == 0 || ss.last < t.sessions[lru].last {' \
  '				if lru == 0 || ss.last > t.sessions[lru].last {' \
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
#     (Phase 15: the host builds every group's state machine.)
mutant "dkvd-applies-the-configured-session-limits" cmd/dkvd/main.go \
  '				m = kv.NewStoreWithLimits(limits)' \
  '				m = kv.NewStore()' \
  ./tests/integration 'TestRealSessionContractSurvivesFullClusterRestart'

# 87. Request validation drops the watermark bound: a request with AckedBelow
#     above its RequestID is proposed, and every replica refuses the entry at
#     apply as malformed.
mutant "requests-are-validated-before-they-are-proposed" internal/kv/api.go \
  '	} else if r.RequestID == 0 || r.AckedBelow == 0 || r.AckedBelow > r.RequestID {' \
  '	} else if r.RequestID == 0 || r.AckedBelow == 0 {' \
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
	conf, _ := rc.Core.Conf()
	n.syncOutboxes()
	n.wg.Add(2)
	go n.receiveLoop()
	go n.actorLoop()' \
  '	n.syncOutboxes()
	n.wg.Add(2)
	go n.receiveLoop()
	go n.actorLoop()
	term, last := rc.Core.Term(), rc.Core.LastIndex()
	conf, _ := rc.Core.Conf()' \
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
		return Result{Decision: Expired}, effect{}
	}' \
  '	if ss == nil {
		ss = &session{ackedBelow: 1, results: map[uint64]execution{}}
		t.sessions[c.ClientID] = ss
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
  '		if err := ssm.RestoreSnapshot(meta.Index, meta.Term, data); err != nil {' \
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

# 117. Another group's snapshot is accepted (Phase 15: the identity is the
#      group id; the same members in another group are another group).
mutant "a-transfer-must-be-this-groups" internal/raftnode/snapshot.go \
  '	if got.Meta.Group != s.Group {' \
  '	if false && got.Meta.Group != s.Group {' \
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

# 125. A forward sent twice (docs/CLIENT_SEMANTICS.md §9): an anonymous write
#      executes twice — the premise the Phase 12 fault test now checks in
#      every node's log (it had duplicated forwards itself; SNAPSHOTS.md §17).
mutant "a-forward-is-sent-once" internal/kv/server.go \
  '	if err := s.node.SendApp(ctx, raftnode.NodeID(leader), transport.MsgForward, payload); err != nil {' \
  '	_ = s.node.SendApp(ctx, raftnode.NodeID(leader), transport.MsgForward, payload)
	if err := s.node.SendApp(ctx, raftnode.NodeID(leader), transport.MsgForward, payload); err != nil {' \
  ./internal/kv 'TestLinearizableUnderMessageFaults'

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

echo "== Phase 15: membership and multi-Raft =="

# 126. A joint configuration commits with the OLD majority alone.
mutant "a-joint-quorum-needs-the-new-majority" internal/raft/membership.go \
  '	if !majority(conf.VoterIDs(), has) {' \
  '	if false && !majority(conf.VoterIDs(), has) {' \
  ./internal/raft 'TestQuorumRules|TestPromoteNeedsTheNewMajority'

# 127. A joint configuration commits with the NEW majority alone.
mutant "a-joint-quorum-needs-the-old-majority" internal/raft/membership.go \
  '	if conf.Joint() && !majority(conf.OutgoingIDs(), has) {' \
  '	if false && conf.Joint() && !majority(conf.OutgoingIDs(), has) {' \
  ./internal/raft 'TestQuorumRules|TestRemoveVoterNeedsTheOldMajority'

# 128. A learner campaigns (a new member votes for itself too early).
mutant "a-learner-never-campaigns" internal/raft/membership.go \
  '	if r.conf.IsVoter(r.id) {
		return nil, true
	}' \
  '	if r.conf.IsMember(r.id) {
		return nil, true
	}' \
  ./internal/raft 'TestLearnerReplicatesButNeverCampaignsOrCounts'

# 129. A promotion skips the joint configuration: the final one at once.
mutant "a-promotion-goes-through-joint-consensus" internal/raft/membership.go \
  '		next.Outgoing = cur.Clone().Voters
		return next, next.Validate()
	case RemoveVoter:' \
  '		return next, next.Validate()
	case RemoveVoter:' \
  ./internal/raft 'TestConfChangeTransitions|TestPromoteNeedsTheNewMajority'

# 130. A removal skips the joint configuration.
mutant "a-removal-goes-through-joint-consensus" internal/raft/membership.go \
  '		next, err := replication.NewConfiguration(without(cur.Voters, id), cur.Learners)
		if err != nil {
			return Configuration{}, err
		}
		next.Outgoing = cur.Clone().Voters' \
  '		next, err := replication.NewConfiguration(without(cur.Voters, id), cur.Learners)
		if err != nil {
			return Configuration{}, err
		}' \
  ./internal/raft 'TestConfChangeTransitions|TestRemoveVoterNeedsTheOldMajority'

# 131. An install keeps the node's old configuration instead of the
#      snapshot's (a snapshot loses the membership it carries).
mutant "an-install-adopts-the-snapshots-configuration" internal/raft/raft.go \
  '	r.baseConf, r.baseConfIndex = m.Conf.Clone(), m.SnapshotIndex' \
  '	r.baseConfIndex = m.SnapshotIndex' \
  ./internal/raft 'TestSnapshotInstallAdoptsTheConfiguration|TestConfAtAnswersOnlyWithEvidence'

# 132. Recovery starts from the genesis, not the published snapshot's
#      configuration (the recovered membership differs from the snapshot).
mutant "recovery-starts-from-the-snapshots-configuration" internal/raftnode/node.go \
  '		base, baseIndex = meta.Conf, meta.Index' \
  '		baseIndex = meta.Index' \
  ./internal/raftnode 'TestMembershipSurvivesRestarts'

# 133. A snapshot taken during a joint configuration records it as stable.
mutant "a-snapshot-carries-its-configuration" internal/raftnode/snapshot.go \
  '	meta := snapshot.Meta{Group: s.Group, Conf: conf, Index: idx, Term: term}' \
  '	meta := snapshot.Meta{Group: s.Group, Conf: replication.Configuration{Voters: conf.Voters, Learners: conf.Learners}, Index: idx, Term: term}' \
  ./internal/raftsim 'TestSimSnapshotDuringJointConfiguration'

# 134. ConfAt answers for a joiner that knows no configuration (a guess).
mutant "confat-knows-nothing-for-a-joiner" internal/raft/membership.go \
  '	if r.baseConf.Empty() {
		return Configuration{}, ErrConfUnknown
	}' \
  '	if false {
		return Configuration{}, ErrConfUnknown
	}' \
  ./internal/raft 'TestJoinerKnowsNoConfigurationUntilItLearnsOne|TestConfAtAnswersOnlyWithEvidence'

# 135. ConfAt answers below its evidence: the base configuration passed off
#      as the configuration before a change the log holds.
mutant "confat-answers-only-with-evidence" internal/raft/membership.go \
  '		if e.Type == replication.EntryConfig {
			return Configuration{}, ErrConfUnknown
		}' \
  '		if false && e.Type == replication.EntryConfig {
			return Configuration{}, ErrConfUnknown
		}' \
  ./internal/raft 'TestConfAtAnswersOnlyWithEvidence|TestBaseConfigurationHoldsOnlyAtItsIndex'

# 136. A voterless base configuration is accepted.
mutant "a-base-configuration-has-voters" internal/raft/raft.go \
  '	if len(base.Voters) == 0 && (!base.Empty() || cfg.ConfIndex > 0) {' \
  '	if false {' \
  ./internal/raft 'TestConfAtAnswersOnlyWithEvidence'

# 137. A stranger with a stale log is heard: a removed node's inflated terms
#      depose the group's leader (a stale member regains authority).
mutant "a-stale-stranger-is-refused" internal/raft/raft.go \
  '		if m.Type != MsgVoteRequest || !r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {' \
  '		if m.Type != MsgVoteRequest {' \
  ./internal/raft 'TestRemovedNodeCannotDeposeOrLead|TestNonMemberVoteRequestIsHeardOnlyWithAnUpToDateLog'

# 138. An up-to-date stranger is refused: a joiner never votes for the
#      promotion that needs it (the vote deadlock).
mutant "an-up-to-date-stranger-is-heard" internal/raft/raft.go \
  '		if m.Type != MsgVoteRequest || !r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {' \
  '		if true {' \
  ./internal/raft 'TestJoinerWithNoConfigurationVotesForItsPromotion|TestNonMemberVoteRequestIsHeardOnlyWithAnUpToDateLog'

# 139. Only a node that believes it is a voter grants a vote: a learner that
#      missed its promotion never elects anyone (the vote deadlock).
mutant "any-node-may-grant-a-vote" internal/raft/raft.go \
  '	if (r.votedFor == "" || r.votedFor == m.From) && r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {' \
  '	if r.conf.IsVoter(r.id) && (r.votedFor == "" || r.votedFor == m.From) && r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {' \
  ./internal/raft 'TestPromotedLearnerThatMissedItsPromotionStillElects|TestJoinerWithNoConfigurationVotesForItsPromotion'

# 140. A leader that lost its leadership during its own removal can never
#      finish it (the removal deadlock).
mutant "a-removed-leader-can-finish-its-removal" internal/raft/membership.go \
  '	p, err := r.ConfAt(r.confIndex - 1)' \
  '	return nil, false
	p, err := r.ConfAt(r.confIndex - 1)' \
  ./internal/raft 'TestRemovedLeaderThatLostItsLeadershipFinishesItsRemoval'

# 141. The final entry waits for an acknowledgement that never comes when the
#      leader alone is its quorum.
mutant "the-final-entry-commits-at-once" internal/raft/membership.go \
  '		r.maybeCommit()
		return
	}
	if !r.conf.IsVoter(r.id) {' \
  '		return
	}
	if !r.conf.IsVoter(r.id) {' \
  ./internal/raft 'TestFinalEntryCommitsAtOnceWhenTheLeaderAloneIsItsQuorum'

# 142. A removed leader keeps leading after its removal commits.
mutant "a-removed-leader-steps-down" internal/raft/membership.go \
  '	if !r.conf.IsVoter(r.id) {
		r.becomeFollower(r.currentTerm, "")' \
  '	if false {
		r.becomeFollower(r.currentTerm, "")' \
  ./internal/raft 'TestLeaderRemovedStepsDownOnceTheFinalEntryCommits'

# 143. The peers a leader replicates to do not follow the configuration: a
#      removed member keeps being sent to.
mutant "the-peers-follow-the-configuration" internal/raft/membership.go \
  '	r.peers = r.conf.Members()' \
  '	r.peers = append(r.peers, r.conf.Members()...)' \
  ./internal/raftsim 'TestSimRemoveAFollower'

# 144. A configuration entry without voters is accepted.
mutant "a-configuration-entry-needs-voters" internal/replication/config.go \
  '	if len(c.Voters) == 0 {
		return Configuration{}, fmt.Errorf("%w: a configuration entry without voters", ErrInvalidConfiguration)' \
  '	if false {
		return Configuration{}, fmt.Errorf("%w: a configuration entry without voters", ErrInvalidConfiguration)' \
  "./internal/replication ./internal/raftlog" 'TestConfigurationEntriesNeedVoters|TestVoterlessConfigurationEntryIsCorruption'

# 145. The stable configuration may be represented as a joint one.
mutant "a-joint-configuration-is-canonical" internal/replication/config.go \
  '		if same {' \
  '		if false && same {' \
  ./internal/replication 'TestJointConfigurationIsCanonical'

# 146. The identity file's group is not checked.
mutant "the-identity-names-the-group" internal/raftnode/node.go \
  '		if id.Group != c.Group {' \
  '		if false {' \
  ./internal/raftnode 'TestIdentityFileRules'

# 147. A restart may name another genesis.
mutant "the-identity-names-the-genesis" internal/raftnode/node.go \
  '		if named && !gen.Equal(id.Genesis) {' \
  '		if false {' \
  ./internal/raftnode 'TestIdentityFileRules'

# 148. Durable state without an identity file is accepted as a fresh start.
mutant "durable-state-needs-an-identity" internal/raftnode/node.go \
  '		if there {' \
  '		if there && false {' \
  ./internal/raftnode 'TestIdentityFileRules'

# 149. An overwritten change is reported as a success.
mutant "a-lost-change-is-reported-lost" internal/raftnode/membership.go \
  '			ours = err == nil && t == w.term' \
  '			ours = err == nil && (t == w.term || true)' \
  ./internal/raftnode 'TestChangeMembershipReportsALostChange'

# 150. A joiner behind its own addition is reported removed.
mutant "only-a-former-member-is-removed" internal/raftnode/membership.go \
  '	if !n.removed && n.wasMember && !conf.Empty()' \
  '	if !n.removed && !conf.Empty()' \
  ./internal/raftnode 'TestJoinerInstallingASnapshotThatPredatesItIsNotRemoved'

# 151. A configuration entry reaches the state machine as its bytes.
mutant "configuration-entries-reach-the-state-machine-empty" internal/raftnode/crashpoint.go \
  '			if e.Type != replication.EntryNormal {' \
  '			if false && e.Type != replication.EntryNormal {' \
  ./internal/raftnode 'TestConfigurationEntriesReachTheStateMachineEmpty'

# 152. A node reading its transport steps another group's frames.
mutant "a-node-drops-other-groups-frames" internal/raftnode/node.go \
  '				if err != nil || g != n.cfg.Group {' \
  '				if err != nil {' \
  ./internal/raftnode 'TestNodeDropsFramesOfOtherGroups'

# 153. Every group's messages go out labelled group 0: two groups' traffic
#      mixed as one.
mutant "a-frame-carries-its-group" internal/raftnode/node.go \
  'WrapGroup(n.cfg.Group, m.Marshal())' \
  'WrapGroup(0, m.Marshal())' \
  ./internal/multiraft 'TestTwoGroupsOnThreeNodesAreIndependent'

# 154. The host delivers a frame to another group than its envelope names.
mutant "the-host-delivers-to-the-named-group" internal/multiraft/host.go \
  '			hg := h.groups[g]' \
  '			hg := h.groups[g^1]' \
  ./internal/multiraft 'TestFramesReachExactlyTheirGroup'

# 155. Every group shares group 0's log.
mutant "each-group-has-its-own-log" internal/multiraft/host.go \
  '	logPath := LogPath(h.cfg.DataDir, g)' \
  '	logPath := LogPath(h.cfg.DataDir, 0)' \
  ./internal/multiraft 'TestTwoGroupsOnThreeNodesAreIndependent'

# 156. A request naming another group than its key's executes there.
mutant "a-misrouted-request-is-refused" internal/kv/front.go \
  '		if g := f.route(req.Key); g != req.Group {' \
  '		if g := f.route(req.Key); false && g != req.Group {' \
  ./internal/kv 'TestFrontRefusesAMisroutedRequest'

echo "== Phase 15: the same rules, killed by real processes ALONE =="

# 157. A removed node's inflated terms depose the group's leader, on real
#      processes (a stale member regains authority).
mutant "a-stale-stranger-is-refused (real processes)" internal/raft/raft.go \
  '		if m.Type != MsgVoteRequest || !r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {' \
  '		if m.Type != MsgVoteRequest {' \
  ./tests/integration 'TestRealRemoveAPartitionedMember'

# 158. A joiner behind its own addition is retired by its host, on real
#      processes: it never catches up.
mutant "only-a-former-member-is-removed (real processes)" internal/raftnode/membership.go \
  '	if !n.removed && n.wasMember && !conf.Empty()' \
  '	if !n.removed && !conf.Empty()' \
  ./tests/integration 'TestRealNewMemberCatchesUpBySnapshotAndTheClusterRestarts'

echo "== Phase 15: found by the 200-seed gate =="

# 159. The leader appends the final configuration before the joint entry is
#      committed — on any commit while joint — and so switches to the final
#      configuration's quorum before the joint one was ever satisfied. Killed by
#      the core test the gate showed was missing, and by the simulator's INV-MB3
#      precondition on the regression seeds.
mutant "the-final-entry-waits-for-the-joint-commit" internal/raft/membership.go \
  '	if r.role != Leader || r.confIndex > r.log.CommitIndex() {' \
  '	if r.role != Leader || (r.confIndex > r.log.CommitIndex() && !r.conf.Joint()) {' \
  './internal/raft ./internal/raftsim' 'TestFinalEntryWaitsForTheJointCommit|TestMembershipRegressionSeeds'

echo "== Phase 16: observability (docs/OBSERVABILITY.md) =="

# 160. A series is keyed by its label values joined with a separator: a value
#      holding the separator makes two label sets one series (found by
#      FuzzParse).
mutant "series-keys-are-unambiguous" internal/metrics/metrics.go \
  '	return b.String()' \
  '	return strings.Join(values, "\xff")' \
  ./internal/metrics 'TestLabelValuesAreNeverAmbiguous'

# 161. The front answers a request without counting it.
mutant "the-front-counts-every-answer" internal/kv/front.go \
  '	m.request(req, resp, start, f.Server(req.Group) != nil)' \
  '	_ = start' \
  ./internal/kv 'TestKVMetricsMatchTheResponses'

# 162. The state machine's decisions are not observed (the metric would have
#      to fall back on kv.Store.Stats, which a restore resets).
mutant "every-apply-decision-is-observed" internal/kv/store.go \
  '	if t.observe != nil {
		t.observe(d)
	}' \
  '	_ = t.observe' \
  ./internal/kv 'TestKVMetricsMatchTheResponses|TestDecisionCountsSurviveARestore'

# 163. The core's cumulative role transitions are added whole at every
#      published status instead of by difference: each election counted many
#      times.
mutant "role-transitions-are-counted-by-difference" internal/raftnode/metrics.go \
  '	n.m.electionsWon.Add(d.ElectionsWon - n.lastCounters.ElectionsWon)' \
  '	n.m.electionsWon.Add(d.ElectionsWon)' \
  ./internal/raftnode 'TestMetricsMatchWhatTheClusterDid'

# 164. The durable log forgets the bytes it appends: its size stays at what
#      Open found.
mutant "the-log-size-counts-every-record" internal/raftlog/raftlog.go \
  '	l.size += int64(n)' \
  '	l.size += int64(n) * 0' \
  ./internal/raftlog 'TestSizeIsTheFileLength'

# 165. A write completes inside the apply loop again, before the cycle's
#      Status covers it (what the observability branch's race gate found).
mutant "a-write-completes-after-its-status" internal/raftnode/node.go \
  '	n.completed = append(n.completed, appliedEntry{index: e.Index, term: e.Term, result: result})' \
  '	n.waiters.Applied(e.Index, e.Term, result)' \
  ./internal/raftnode 'TestWriteCompletionFollowsItsStatus'

echo "== #2: the load generator (docs/LOAD_TESTING.md) =="

# 166. Open loop measures an operation from when a client takes it, not from
#      when it was due: a stall hides the queue behind it (coordinated
#      omission).
mutant "open-loop-latency-counts-from-the-due-time" internal/load/load.go \
  '				for t := range tasks {
					class := c.do(runCtx, t)' \
  '				for t := range tasks {
					t.intended = time.Now()
					class := c.do(runCtx, t)' \
  ./internal/load 'TestOpenLoopCountsAStallAsLatency'

# 167. Throughput counts the operations due in the window, however late they
#      completed: an overloaded open-loop run reports the offered rate.
mutant "throughput-counts-completions-in-the-window" internal/load/load.go \
  '	res.OKPerSec = float64(r.inWindow.Load()) / cfg.Duration.Seconds()' \
  '	res.OKPerSec = float64(res.Classes[ClassOK]+res.Classes[ClassNotFound]) / cfg.Duration.Seconds()' \
  ./internal/load 'TestOverloadReportsAchievedThroughput'

echo "== #3: the cluster lab (docs/CLUSTER_BENCHMARKS.md) =="

# 168. The median of a configuration's runs is read from unsorted values: the
#      report's medians are whichever run happened to finish in the middle.
mutant "lab-median-of-sorted-runs" internal/lab/experiment.go \
  '	sort.Float64s(v)
	n := len(v)' \
  '	sort.Float64s(v[:0])
	n := len(v)' \
  ./internal/lab 'TestSummarize'

# 169. A restarted node's counters are subtracted from its previous
#      process's: its CPU, persists and elections come out short or negative.
mutant "lab-restarted-counters-from-zero" internal/lab/experiment.go \
  '		if restarted || before == nil {' \
  '		if before == nil {' \
  ./internal/lab 'TestUsageFromScrapes'

# 170. The snapshot scenario takes no snapshot (the bug the first suite run
#      had): it measures the steady load under another name.
mutant "lab-snapshot-scenario-snapshots" internal/lab/scenarios.go \
  '		e.Cluster.SnapshotEvery = 100' \
  '		e.Cluster.SnapshotEvery = 0' \
  ./internal/lab 'TestScenariosAndSuite'

# 171. The commit index is treated as a counter: a restarted node's commit
#      advance becomes its whole commit index.
mutant "lab-commit-advance-is-an-index" internal/lab/experiment.go \
  '	u.CommitAdvance = after.Sum("dkv_raft_commit_index") - before.Sum("dkv_raft_commit_index")' \
  '	u.CommitAdvance = delta("dkv_raft_commit_index")' \
  ./internal/lab 'TestUsageFromScrapes'

# 172. Replication traffic counts every frame kind: votes and responses
#      inflate the leader's AppendEntries per entry.
mutant "lab-append-frames-by-kind" internal/lab/experiment.go \
  '	u.AppendFramesSent = delta("dkv_transport_frames_sent_total", "kind", "append_entries")' \
  '	u.AppendFramesSent = delta("dkv_transport_frames_sent_total")' \
  ./internal/lab 'TestUsageFromScrapes'

# --- The entry-size limit (C1, docs/RAFT.md §16): one bound, enforced at every
#     boundary an entry crosses. Each mutant lets an entry one byte (or 1 MiB)
#     over the limit through exactly one boundary; the test named for that
#     boundary must catch it.

# 173. The front admits a write whose ENCODED command exceeds the entry limit
#      (raw key and value within their limits): the C1 bug's entry point.
mutant "entry-limit-at-the-front" internal/kv/api.go \
  '		if n := r.command().EncodedLen(); n > MaxCommandLen {' \
  '		if n := r.command().EncodedLen(); n > 2*MaxCommandLen {' \
  ./internal/kv '^TestEncodedEntryLimitDecidesWriteAdmission$'

# 174. Command.Validate (and so Decode) accepts an over-limit command.
mutant "entry-limit-in-the-command" internal/kv/command.go \
  '	if n := c.EncodedLen(); n > MaxCommandLen {' \
  '	if n := c.EncodedLen(); n > 2*MaxCommandLen {' \
  ./internal/kv '^TestEncodedEntryLimitDecidesWriteAdmission$'

# 175. Propose accepts an entry over the limit (a leader appends it; a
#      follower answers not-leader instead of a definite refusal).
mutant "entry-limit-at-propose" internal/raft/raft.go \
  '	if len(data) > MaxEntryDataLen {' \
  '	if len(data) > 2*MaxEntryDataLen {' \
  ./internal/raft '^TestProposeOverTheEntryLimitIsRefused$'

# 176. Step accepts an AppendEntries carrying an over-limit entry: its term is
#      adopted and the follower answers it.
mutant "entry-limit-in-step" internal/raft/raft.go \
  '			if len(e.Data) > MaxEntryDataLen {' \
  '			if len(e.Data) > 2*MaxEntryDataLen {' \
  ./internal/raft '^TestStepRefusesAnOversizedAppendEntries$'

# 177. The in-memory log holds an over-limit entry.
mutant "entry-limit-in-the-log" internal/replication/log.go \
  '		if len(e.Data) > MaxEntryDataLen {' \
  '		if len(e.Data) > 2*MaxEntryDataLen {' \
  ./internal/replication '^TestEntrySizeLimit$'

# 178. The durable log persists an entry its own replay refuses to read (the
#      node that wrote it could never restart).
mutant "entry-limit-at-save" internal/raftlog/raftlog.go \
  '		if len(e.Data) > MaxEntryDataLen {' \
  '		if len(e.Data) > 2*MaxEntryDataLen {' \
  ./internal/raftlog '^TestSaveRefusesAnEntryOverTheLimit$'

# 179. A proposal refused as too large is answered UNKNOWN: the client cannot
#      tell a definite refusal from a write that may have happened.
mutant "entry-too-large-is-invalid" internal/kv/server.go \
  '	case errors.Is(err, raft.ErrEntryTooLarge):' \
  '	case false && errors.Is(err, raft.ErrEntryTooLarge):' \
  ./internal/kv '^TestEntryTooLargeFromBelowIsInvalid$'

# --- The data directory (audit H1, M5; internal/nodedir, docs/MULTI_RAFT.md §5):
#     a node's durable state is its own, locked, and never silently empty.

# 180. -raft/-cluster run without a data directory (the old temporary default).
#      Since nodedir also refuses an empty path, the start still fails; the
#      kill is that the operator is no longer told which flag is missing.
mutant "data-dir-required" cmd/dkvd/main.go \
  '	if (*raftMode || *cluster) && *dataDir == "" {' \
  '	if false && (*raftMode || *cluster) && *dataDir == "" {' \
  ./cmd/dkvd '^TestRunRequiresADataDirAndAPositiveTick$'

# 181. A zero tick is accepted (the idle timeout disabled, the driver's
#      default silently used).
mutant "tick-must-be-positive" cmd/dkvd/main.go \
  '	if *tickIvl <= 0 {' \
  '	if *tickIvl < 0 {' \
  ./cmd/dkvd '^TestRunRequiresADataDirAndAPositiveTick$'

# 182. A negative tick reaches the actor's time.NewTicker.
mutant "raftnode-negative-tick" internal/raftnode/node.go \
  '	if cfg.TickInterval < 0 {' \
  '	if false && cfg.TickInterval < 0 {' \
  ./internal/raftnode '^TestStartRefusesANegativeTick$'

# 183. An empty data directory starts as a new node without -init: a wiped
#      member restarts empty under its old id.
mutant "data-dir-needs-init" internal/nodedir/nodedir.go \
  '	case !legacy && !opts.Init:' \
  '	case false:' \
  ./internal/nodedir '^TestFreshDirectoryNeedsInit$'

# 184. A data directory recorded for another node is accepted.
mutant "data-dir-belongs-to-its-node" internal/nodedir/nodedir.go \
  '		case id.Node != opts.Node:' \
  '		case false:' \
  ./internal/nodedir '^TestIdentityMismatchIsRefused$'

# 185. -init re-initializes an initialized directory (so it could live in a
#      unit file, and a wiped directory would be re-initialized silently).
mutant "data-dir-init-once" internal/nodedir/nodedir.go \
  '		case id.Initialized && opts.Init:' \
  '		case false:' \
  ./internal/nodedir '^TestInitializationLifecycle$'

# 186. Two processes share a data directory (a shared lock excludes nothing).
mutant "data-dir-exclusive-lock" internal/nodedir/lock_unix.go \
  'syscall.LOCK_EX|syscall.LOCK_NB' \
  'syscall.LOCK_SH|syscall.LOCK_NB' \
  ./internal/nodedir '^TestDataDirectoryIsLocked$'

# 187. A group's state recorded for another node is run by this one.
mutant "group-identity-names-its-node" internal/raftnode/node.go \
  '		if id.Node != "" && id.Node != c.ID {' \
  '		if false {' \
  ./internal/raftnode '^TestIdentityFileNamesItsNode$'

# 188. An initialized directory whose group state is gone creates it empty
#      (-raft mode; mutant 266 is -cluster mode's).
mutant "genesis-only-while-initializing" cmd/dkvd/main.go \
  'hc.LogPathFor(c.g)); err != nil || !found {' \
  'hc.LogPathFor(c.g)); (err != nil || !found) && false {' \
  ./cmd/dkvd '^TestDataDirectoryRules$'

# --- The group lifecycle (audit H3, docs/MULTI_RAFT.md §3): starts and stops of
#     one group never overlap.

# 189. A start does not see another start of its group in flight: both pass
#      the existence check and two drivers open one log.
mutant "lifecycle-one-start-at-a-time" internal/multiraft/host.go \
  '	case h.busy[g] != "":' \
  '	case false:' \
  ./internal/multiraft '^TestAStartingGroupCannotBeStartedOrStoppedAgain$'

# 190. A stop releases its group before its node is closed: an Open recovers
#      the log under the still-running old node.
mutant "lifecycle-stop-holds-the-group" internal/multiraft/host.go \
  '	h.busy[g] = "stopping"' \
  '	_ = "stopping"' \
  ./internal/multiraft '^TestAStoppingGroupCannotBeStartedUntilItsNodeIsClosed$'

# 191. A failed first start leaves its empty directory, reported as a failed
#      group at every later start.
mutant "failed-start-leaves-no-directory" internal/multiraft/host.go \
  '			// stays.
			_ = os.Remove(GroupDir(h.cfg.DataDir, g))' \
  '			// stays.
			_ = GroupDir(h.cfg.DataDir, g)' \
  ./internal/multiraft '^TestAFailedFirstStartLeavesNoDirectory$'

# --- Replica settings (audit H5): what the replicated state machine's definition
#     depends on is pinned, and a node setting cannot be dropped on its way to
#     the groups.

# 192. A start with other replica settings than the directory recorded runs.
mutant "replica-settings-pinned" internal/nodedir/nodedir.go \
  '		case opts.Settings != id.Settings:' \
  '		case false:' \
  ./internal/nodedir '^TestSettingsArePinned$'

# 193. dkvd does not hand its settings to the data directory.
mutant "dkvd-pins-its-settings" cmd/dkvd/main.go \
  'Init: init, Settings: settings})' \
  'Init: init, Settings: ""})' \
  ./cmd/dkvd '^TestReplicaSettingsArePinned$'

# 194. Routing flags outside -cluster mode are silently ignored again.
mutant "routing-flags-cluster-only" cmd/dkvd/main.go \
  '			if explicit[name] {' \
  '			if false {' \
  ./cmd/dkvd '^TestRoutingFlagsBelongToClusterMode$'

# 195. A node setting is dropped on its way to the hosted groups.
mutant "host-forwards-every-setting" internal/multiraft/host.go \
  '	nc.Metrics = h.nodeMetrics' \
  '	_ = h.nodeMetrics' \
  ./internal/multiraft '^TestHostForwardsEveryNodeSetting$'

# 196. Every group of a node draws the node's election timeout sequence.
mutant "groups-draw-their-own-seeds" internal/raftnode/node.go \
  '	if g != 0 {' \
  '	if false {' \
  ./internal/raftnode '^TestGroupsDrawDifferentElectionSeeds$'

# 197. The accepter refuses a dialer of another cluster (audit H2).
mutant "accepter-checks-cluster" internal/transport/transport.go \
  '	case h.cluster != t.self.cluster:
		status = statusWrongCluster' \
  '	case false:
		status = statusWrongCluster' \
  ./internal/transport '^(TestNodesOfAnotherClusterNeverConnect|TestTheAccepterRefusesAnotherClusterOrSettings)$'

# 198. The accepter refuses a dialer with other replica settings (audit H5).
mutant "accepter-checks-settings" internal/transport/transport.go \
  '	case string(h.digest) != string(t.self.digest):
		status = statusWrongSettings' \
  '	case false:
		status = statusWrongSettings' \
  ./internal/transport '^(TestNodesWithOtherReplicaSettingsNeverConnect|TestTheAccepterRefusesAnotherClusterOrSettings)$'

# 199. The dialer refuses an answer from another node than the one it dialed.
mutant "dialer-checks-who-answered" internal/transport/transport.go \
  '	case h.id != peer:' \
  '	case false:' \
  ./internal/transport '^TestDialerChecksTheAnswer$'

# 200. The dialer refuses an accepter of another cluster.
mutant "dialer-checks-cluster" internal/transport/transport.go \
  '	case h.cluster != t.self.cluster:
		t.m.handshakeRejected.Inc()' \
  '	case false:
		t.m.handshakeRejected.Inc()' \
  ./internal/transport '^TestDialerChecksTheAnswer$'

# 201. The dialer refuses an accepter with other replica settings.
mutant "dialer-checks-settings" internal/transport/transport.go \
  '	case string(h.digest) != string(t.self.digest):
		t.m.handshakeRejected.Inc()' \
  '	case false:
		t.m.handshakeRejected.Inc()' \
  ./internal/transport '^TestDialerChecksTheAnswer$'

# 202. A known peer with the larger id never dials; its claim is refused.
mutant "accepter-checks-direction" internal/transport/transport.go \
  '	case h.id > t.cfg.NodeID:' \
  '	case false:' \
  ./internal/transport '^TestInboundFromTheWrongDirectionIsRefused$'

# 203. A failed write closes the connection (audit M2).
mutant "failed-write-closes-the-connection" internal/transport/conn.go \
  '	if err != nil {
		c.close()
	}
	return err' \
  '	return err' \
  ./internal/transport '^TestFailedWriteClosesTheConnection$'

# 204. Send refuses a frame its peer would refuse (audit D2).
mutant "send-refuses-an-oversized-frame" internal/transport/transport.go \
  '	if len(payload) > MaxFrameSize {' \
  '	if false {' \
  ./internal/transport '^TestSendRefusesAFrameItsPeerWouldRefuse$'

# 205. An Accept error is retried, not the end of the accept loop (audit D7).
mutant "accept-loop-retries" internal/transport/transport.go \
  '			if t.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {' \
  '			if true {' \
  ./internal/transport '^TestAcceptLoopSurvivesAcceptErrors$'

# 206. Connections in their handshake are bounded.
mutant "pending-handshakes-bounded" internal/transport/transport.go \
  '		handshake: make(chan struct{}, cfg.MaxPendingHandshakes),' \
  '		handshake: make(chan struct{}, 1<<16),' \
  ./internal/transport '^TestPendingHandshakesAreBounded$'

# 207. A connection past its handshake gives its slot back.
mutant "handshake-slot-released" internal/transport/transport.go \
  '	release()
	t.serve(h.id, nc, "inbound")' \
  '	t.serve(h.id, nc, "inbound")' \
  ./internal/transport '^TestPendingHandshakesAreBounded$'

# 208. The dialer's handshake is bounded by the handshake timeout.
mutant "dialer-handshake-timeout" internal/transport/transport.go \
  '	_ = nc.SetDeadline(time.Now().Add(t.cfg.HandshakeTimeout))
	stop := context.AfterFunc(ctx' \
  '	stop := context.AfterFunc(ctx' \
  ./internal/transport '^TestDialerDropsAPeerThatNeverAnswers$'

# 209. Shutdown interrupts an outbound handshake waiting for its answer.
mutant "close-interrupts-dial" internal/transport/transport.go \
  '	stop := context.AfterFunc(ctx, func() { _ = nc.Close() })' \
  '	stop := func() bool { return true }' \
  ./internal/transport '^TestCloseInterruptsHandshakesInFlight$'

# 210. Shutdown interrupts an inbound handshake being read.
mutant "close-interrupts-inbound-handshake" internal/transport/transport.go \
  '	stop := context.AfterFunc(t.ctx, func() { _ = nc.Close() })' \
  '	stop := func() bool { return true }' \
  ./internal/transport '^TestCloseInterruptsHandshakesInFlight$'

# 211. The idle timeout measures silence, not a frame's size.
mutant "idle-timeout-per-read" internal/transport/transport.go \
  '		r = idleReader{c.nc, t.cfg.ReadIdleTimeout}
	}
	for {' \
  '	}
	for {
		if t.cfg.ReadIdleTimeout > 0 {
			_ = c.nc.SetReadDeadline(time.Now().Add(t.cfg.ReadIdleTimeout))
		}' \
  ./internal/transport '^TestSlowFrameIsNotCutByTheIdleTimeout$'

# 212. A caller's deadline does not cut a started frame.
mutant "caller-deadline-spares-a-started-frame" internal/transport/conn.go \
  '		_ = c.nc.SetWriteDeadline(time.Now().Add(c.writeTimeout))' \
  '		dl := time.Now().Add(c.writeTimeout)
		if d, ok := ctx.Deadline(); ok && d.Before(dl) {
			dl = d
		}
		_ = c.nc.SetWriteDeadline(dl)' \
  ./internal/transport '^TestCallerDeadlineDoesNotCutAStartedFrame$'

# 213. dkvd gives its transport the data directory's cluster id and settings.
mutant "dkvd-transport-carries-identity" cmd/dkvd/main.go \
  '		tcfg.ClusterID, tcfg.SettingsDigest = nd.ID.Cluster, settingsDigest(nd.ID.Settings)' \
  '		_ = settingsDigest' \
  ./tests/integration '^TestRealImpostorsNeverJoinTheGroup$'

# 214. A leader's uncommitted entries are bounded (audit M3).
mutant "uncommitted-entries-bounded" internal/raft/raft.go \
  '	if len(r.uncommitted) >= r.maxUncommittedEntries ||' \
  '	if false && len(r.uncommitted) >= r.maxUncommittedEntries ||' \
  ./internal/raft '^TestIsolatedLeaderRefusesProposalsBeyondItsBound$'

# 215. ... and their bytes.
mutant "uncommitted-bytes-bounded" internal/raft/raft.go \
  '		r.uncommittedBytes > 0 && r.uncommittedBytes+len(data) > r.maxUncommittedBytes {' \
  '		false {' \
  ./internal/raft '^TestUncommittedBytesBound$'

# 216. A proposal on a tail holding no data is always admitted.
mutant "dataless-tail-admits" internal/raft/raft.go \
  '		r.uncommittedBytes > 0 && r.uncommittedBytes+len(data) > r.maxUncommittedBytes {' \
  '		r.uncommittedBytes+len(data) > r.maxUncommittedBytes {' \
  ./internal/raft '^TestUncommittedBytesBound$'

# 217. A new leader's inherited tail counts toward its bound.
mutant "inherited-tail-counts" internal/raft/raft.go \
  '		for _, e := range tail {
			r.trackUncommitted(len(e.Data))
		}' \
  '		_ = tail' \
  ./internal/raft '^TestInheritedTailCountsTowardTheBound$'

# 218. Committing shrinks the leader's tracked tail.
mutant "commit-shrinks-the-tail" internal/raft/raft.go \
  '		if r.role == Leader {
			n := min(int(idx-old), len(r.uncommitted))' \
  '		if false {
			n := min(int(idx-old), len(r.uncommitted))' \
  ./internal/raft '^TestIsolatedLeaderRefusesProposalsBeyondItsBound$'

# 219. Reads awaiting confirmation are bounded.
mutant "pending-reads-bounded" internal/raft/raft.go \
  '	if len(r.pending) >= r.maxPendingReads {' \
  '	if false {' \
  ./internal/raft '^TestIsolatedLeaderRefusesReadsBeyondItsBound$'

# 220. An apply failure stops the node (audit M4).
mutant "apply-failure-fail-stops" internal/raftnode/node.go \
  '		return err // ErrApply, or a crash point fired' \
  '		if !errors.Is(err, ErrApply) {
			return err
		}' \
  ./internal/raftnode '^TestApplyFailureStopsTheNode$'

# 221. A write whose client gave up is forgotten.
mutant "abandoned-write-forgotten" internal/raftnode/node.go \
  '		n.abandon(abandoned{index: acc.index, ch: acc.done})' \
  '		_ = abandoned{}' \
  ./internal/raftnode '^TestAbandonedRequestsLeaveNothingBehind$'

# 222. A read whose client gave up is forgotten.
mutant "abandoned-read-forgotten" internal/raftnode/node.go \
  '		n.abandon(abandoned{readID: acc.id, ch: acc.done})' \
  '		_ = abandoned{}' \
  ./internal/raftnode '^TestAbandonedRequestsLeaveNothingBehind$'

# 223. Cancel removes the waiter.
mutant "waiters-cancel" internal/raftnode/waiters.go \
  '		if wt.ch == ch {' \
  '		if wt.ch == nil {' \
  ./internal/raftnode '^(TestAbandonedRequestsLeaveNothingBehind|TestForgetReleasesWhatTheActorHolds)$'

# 224. A write abandoned before its acceptance was read is forgotten too.
mutant "abandoned-before-acceptance" internal/raftnode/node.go \
  '			if acc.err == nil {
				n.waiters.Cancel(acc.index, acc.done)
			}' \
  '			_ = acc' \
  ./internal/raftnode '^TestForgetReleasesWhatTheActorHolds$'

# 225. Cancel removes only the abandoned waiter, not another at its index.
mutant "cancel-only-the-abandoned" internal/raftnode/waiters.go \
  '	if len(ws) == 0 {
		delete(w.byIndex, index)' \
  '	if true {
		w.n -= len(ws)
		delete(w.byIndex, index)' \
  ./internal/raftnode '^TestForgetReleasesWhatTheActorHolds$'

# 226. ErrBusy is a definite refusal: UNAVAILABLE, never UNKNOWN.
mutant "busy-is-unavailable" internal/kv/server.go \
  '	case errors.Is(err, raft.ErrBusy):' \
  '	case false:' \
  ./internal/kv '^TestBusyFromBelowIsUnavailable$'

# 227. The client port caps its connections (audit M1).
mutant "kv-conns-capped" internal/kv/wire.go \
  '	slots := make(chan struct{}, cfg.MaxConns)' \
  '	slots := make(chan struct{}, 1<<20)' \
  ./internal/kv '^TestServeCapsItsConnections$'

# 228. A request frame, once begun, must complete within FrameTimeout.
mutant "kv-frame-deadline" internal/kv/wire.go \
  '	_ = c.SetReadDeadline(time.Now().Add(timeout))
	return readFrame' \
  '	return readFrame' \
  ./internal/kv '^TestServeDropsAStalledFrameButKeepsAnIdleConnection$'

# 229. ... and an idle connection is never timed out.
mutant "kv-idle-not-timed-out" internal/kv/wire.go \
  '	_ = c.SetReadDeadline(time.Time{})
	var first [1]byte' \
  '	_ = c.SetReadDeadline(time.Now().Add(timeout))
	var first [1]byte' \
  ./internal/kv '^TestServeDropsAStalledFrameButKeepsAnIdleConnection$'

# 230. A response must be written within WriteTimeout.
mutant "kv-write-deadline" internal/kv/wire.go \
  '		_ = c.SetWriteDeadline(time.Now().Add(cfg.WriteTimeout))' \
  '		_ = cfg.WriteTimeout' \
  ./internal/kv '^TestServeDropsAClientThatDoesNotRead$'

# 231. An Accept error does not end the client port.
mutant "kv-accept-retries" internal/kv/wire.go \
  '			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			logf("event=kv_accept_failed' \
  '			if true {
				return
			}
			logf("event=kv_accept_failed' \
  ./internal/kv '^TestServeSurvivesAcceptErrors$'

# 232. A frame's buffer grows with the bytes that arrive.
mutant "kv-frame-grows" internal/kv/wire.go \
  '	frame := make([]byte, record.HeaderSize, record.HeaderSize+min(int(length), readChunk))' \
  '	frame := make([]byte, record.HeaderSize, record.HeaderSize+int(length))' \
  ./internal/kv '^TestReadFrameGrowsWithTheBytesThatArrive$'

# 233. Inbound forwards are bounded; beyond, UNAVAILABLE.
mutant "kv-forwards-bounded" internal/kv/server.go \
  '		select {
		case s.fwdSlots <- struct{}{}:
		default:
			s.refuse(peer, fid)
			return
		}' \
  '		s.fwdSlots <- struct{}{}' \
  ./internal/kv '^TestForwardsBeyondTheBoundAreRefusedUnavailable$'

# 234. A request naming an unhosted group is labelled "other" (audit M7).
mutant "kv-unhosted-group-label" internal/kv/front.go \
  '	m.request(req, resp, start, f.Server(req.Group) != nil)' \
  '	m.request(req, resp, start, true)' \
  ./internal/kv '^TestClientsCannotCreateMetricSeries$'

# 235. The admin port caps its connections.
mutant "admin-conns-capped" internal/multiraft/admin.go \
  '	slots := make(chan struct{}, MaxAdminConns)' \
  '	slots := make(chan struct{}, 1<<20)' \
  ./internal/multiraft '^TestAdminCapsItsConnections$'

# 236. An Accept error does not end the admin port.
mutant "admin-accept-retries" internal/multiraft/admin.go \
  '			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {' \
  '			if true {' \
  ./internal/multiraft '^TestAdminSurvivesAcceptErrors$'

# 237. An idle admin connection is closed.
mutant "admin-idle-deadline" internal/multiraft/admin.go \
  '		_ = c.SetReadDeadline(time.Now().Add(adminIdle))' \
  '		_ = adminIdle' \
  ./internal/multiraft '^TestAdminDropsAnIdleConnection$'

# 238. An admin request's timeout is clamped.
mutant "admin-timeout-clamped" internal/multiraft/admin.go \
  '	case ms >= int(MaxAdminTimeout/time.Millisecond): // also before ms×1e6 could overflow' \
  '	case false:' \
  ./internal/multiraft '^TestAdminTimeoutIsClamped$'

# 239. Admin log fields from the client are quoted.
mutant "admin-log-quoted" internal/multiraft/admin.go \
  '				logf("event=admin node=%s op=%q group=%d id=%q ok=%v err=%q", h.cfg.ID, req.Op, req.Group, req.ID, resp.OK, resp.Error)' \
  '				logf("event=admin node=%s op=%s group=%d id=%s ok=%v err=%q", h.cfg.ID, req.Op, req.Group, req.ID, resp.OK, resp.Error)' \
  ./internal/multiraft '^TestAdminLogLinesCannotBeForged$'

# 240. dkvd keeps the admin port on loopback unless allowed (audit H6).
mutant "dkvd-admin-loopback" cmd/dkvd/main.go \
  '	if *adminAt != "" && !*adminAny && !isLoopback(*adminAt) {' \
  '	if false && !*adminAny {' \
  ./cmd/dkvd '^TestAdminListensOnLoopbackUnlessAllowed$'

# 241. A snapshot's state buffer is sized by the bytes that exist.
mutant "snapshot-decode-allocation" internal/snapshot/snapshot.go \
  '	data := make([]byte, 0, min(dataLen, uint64(len(b))))' \
  '	data := make([]byte, 0, dataLen)' \
  ./internal/snapshot '^TestDecodeAllocatesNoMoreThanItsInput$'

# 242. The metrics server bounds its connections.
mutant "metrics-server-idle" cmd/dkvd/main.go \
  '		IdleTimeout:       60 * time.Second,' \
  '		IdleTimeout:       0,' \
  ./cmd/dkvd '^TestMetricsServerBoundsItsConnections$'

# 243. An AppendEntries carries at most MaxEntriesPerMsg entries (audit H4).
mutant "append-entry-budget" internal/raft/raft.go \
  '	hi := min(last+1, next+uint64(r.maxEntriesPerMsg))' \
  '	hi := last + 1' \
  ./internal/raft '^TestEntryBudgetBindsABacklogOfSmallEntries$'

# 244. ... and at most MaxSizePerMsg bytes of entries.
mutant "append-byte-budget" internal/raft/raft.go \
  '	entries, _ := r.log.SliceBounded(next, hi, r.maxSizePerMsg)' \
  '	entries, _ := r.log.Slice(next, hi)' \
  ./internal/raft '^TestLaggingFollowerCatchesUpInBudgetedBatches$'

# 245. An acknowledged batch of a cut backlog sends the next at once.
mutant "cut-backlog-streams" internal/raft/raft.go \
  '	if r.role == Leader && r.cut[peer] && match < r.log.LastIndex() {' \
  '	if false {' \
  ./internal/raft '^TestLaggingFollowerCatchesUpInBudgetedBatches$'

# 246. ... which needs the cut recorded.
mutant "cut-recorded" internal/raft/raft.go \
  '	r.cut[peer] = next+uint64(len(entries)) <= last' \
  '	r.cut[peer] = false' \
  ./internal/raft '^TestLaggingFollowerCatchesUpInBudgetedBatches$'

# 247. Reads registered in one cycle share one round.
mutant "reads-share-a-round" internal/raft/raft.go \
  '	if !r.roundUnsent {' \
  '	if true {' \
  ./internal/raft '^TestReadsInOneCycleShareOneRound$'

# 248. A read never joins a round already sent.
mutant "read-never-joins-a-sent-round" internal/raft/ready.go \
  '	r.roundUnsent = false // its messages are sent' \
  '	_ = r.roundUnsent' \
  ./internal/raft '^TestAReadNeverJoinsARoundAlreadySent$'

# 249. A read round is entry-less heartbeats, not the unacknowledged tail.
mutant "read-round-entry-less" internal/raft/raft.go \
  '				r.sendHeartbeat(p)' \
  '				r.sendAppend(p)' \
  ./internal/raft '^TestReadsInOneCycleShareOneRound$'

# 250. A broadcast is a round a read can join.
mutant "broadcast-is-a-round" internal/raft/raft.go \
  'func (r *Raft) broadcastAppend(retransmit bool) {
	r.hbSeq++
	r.roundUnsent = true' \
  'func (r *Raft) broadcastAppend(retransmit bool) {
	r.hbSeq++' \
  ./internal/raft '^TestReadsInOneCycleShareOneRound$'

# 251. The driver takes the reads waiting together, so they share a round.
mutant "driver-batches-reads" internal/raftnode/node.go \
  '			for i := 1; i < maxReadsPerCycle; i++ {' \
  '			for i := 1; i < 1; i++ {' \
  ./internal/raftnode '^TestConcurrentReadsShareRounds$'

# 252. One vote per term: a voter refuses a second candidate in its term — the
#      check that once skipped itself on every run (audit H.1).
mutant "restarted-voter-refuses-same-term" internal/raft/raft.go \
  '	if (r.votedFor == "" || r.votedFor == m.From) && r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {' \
  '	if r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {' \
  ./internal/raftsim '^TestVoterCrashAroundPersistingItsVote$'

# 253–255. The second and third sites of two durability rules (the patterns of
#      fsync-before-reply and durability-failure-latches match several sites
#      and mutate only the first, Save's — audit): Install fsyncs its boundary
#      before reporting it, and Install and Compact refuse to run on a failed
#      log.
mutant "install-fsyncs-its-boundary" internal/raftlog/raftlog.go \
  '	if l.sync {
		if err := l.f.Sync(); err != nil {
			return l.fail(err)
		}
	}
	l.boundary = Boundary{index, term}' \
  '	if false && l.sync {
		if err := l.f.Sync(); err != nil {
			return l.fail(err)
		}
	}
	l.boundary = Boundary{index, term}' \
  ./internal/raftlog '^(TestInstallSurvivesEveryCrash|TestInstallIsDurableWhenItReturns)$'

mutant "install-refuses-a-failed-log" internal/raftlog/raftlog.go \
  'func (l *Log) Install(index, term uint64) error {
	if l.failed != nil {' \
  'func (l *Log) Install(index, term uint64) error {
	if false && l.failed != nil {' \
  ./internal/raftlog '^TestAFailedLogRefusesInstallAndCompact$'

mutant "compact-refuses-a-failed-log" internal/raftlog/raftlog.go \
  'func (l *Log) Compact(index, term uint64) error {
	if l.failed != nil {' \
  'func (l *Log) Compact(index, term uint64) error {
	if false && l.failed != nil {' \
  ./internal/raftlog '^TestAFailedLogRefusesInstallAndCompact$'

# 256. Install never deletes a manifest CURRENT may name (audit M11).
mutant "install-keeps-a-manifest-current-may-name" internal/storage/manifest/manifest.go \
  '		if !errors.Is(err, ErrFailed) {
			_ = FS.Remove(path) // CURRENT was never touched
		}' \
  '		_ = FS.Remove(path)' \
  ./internal/storage/manifest '^TestInstallUnderEveryFault$'

# 257. A failed manifest Writer refuses every later edit.
mutant "manifest-writer-latches" internal/storage/manifest/manifest.go \
  '	if w.failed != nil {
		return w.failed
	}' \
  '	if false {
		return w.failed
	}' \
  ./internal/storage/manifest '^TestAppendUnderEveryFault$'

# 258. A compaction's output survives an ambiguous manifest edit.
mutant "ambiguous-compaction-keeps-its-output" internal/storage/lsmcompact.go \
  '		if produced && !errors.Is(err, manifest.ErrFailed) {' \
  '		if produced && !errors.Is(err, nil) {' \
  ./internal/storage '^TestAnAmbiguousCompactionEditKeepsItsOutput$'

# 259. A latched compaction failure stops the compactor.
mutant "latched-compactor-stops" internal/storage/lsmcompact.go \
  '			if s.CompactionError() != nil {' \
  '			if false {' \
  ./internal/storage '^TestALatchedCompactionErrorStopsTheCompactor$'

# 260. A flush makes the WAL durable before its manifest edit (audit D10).
mutant "flush-syncs-the-wal-first" internal/storage/lsmstore.go \
  '		if err := s.w.Sync(); err != nil {
			_ = r.Close()' \
  '		if err := error(nil); err != nil {
			_ = r.Close()' \
  ./internal/storage '^TestAFlushMakesTheWALDurableBeforeItsEdit$'

# 261. A failed WAL append latches.
mutant "wal-append-failure-latches" internal/storage/wal/wal.go \
  '		w.syncErr = fmt.Errorf("wal: appending to %s: %w", segmentName(w.seg), err)
		return w.syncErr' \
  '		return fmt.Errorf("wal: appending to %s: %w", segmentName(w.seg), err)' \
  ./internal/storage/wal '^TestATornAppendLatches$'

# 262. Close after a failed flush reports it and does not flush again.
mutant "wal-close-reports-the-latch" internal/storage/wal/wal.go \
  '	syncErr := w.syncErr' \
  '	var syncErr error' \
  ./internal/storage/wal '^TestCloseAfterAFailedFlushReportsItAndDoesNotFlushAgain$'

# 263. A flush during replay at open, before the store has a WAL, does not
# sync a WAL that does not exist.
mutant "replay-flush-has-no-wal-to-sync" internal/storage/lsmstore.go \
  '	if s.w != nil && s.opts.WAL.SyncMode != wal.SyncOff {
		if err := s.w.Sync(); err != nil {' \
  '	if s.opts.WAL.SyncMode != wal.SyncOff {
		if err := s.w.Sync(); err != nil {' \
  ./internal/storage '^TestAReplayThatFlushesOpens$'

# 264. Close after a failed WRITE still flushes the records acknowledged before it.
mutant "wal-close-flushes-after-a-failed-write" internal/storage/wal/wal.go \
  '	if !w.flushFailed && w.opts.SyncMode != SyncOff {' \
  '	if w.syncErr == nil && w.opts.SyncMode != SyncOff {' \
  ./internal/storage/wal '^TestCloseAfterAFailedWriteStillFlushes$'

# 265. A failed flush is remembered as one, so Close does not flush again.
mutant "wal-failed-flush-is-remembered" internal/storage/wal/wal.go \
  '		w.flushFailed = true
' \
  '' \
  ./internal/storage/wal '^TestCloseAfterAFailedFlushReportsItAndDoesNotFlushAgain$'

# 266. In -cluster mode, a -join group whose state was lost from an
# initialized directory is reported, not created again empty (audit H1).
mutant "lost-join-group-is-not-recreated" cmd/dkvd/main.go \
  '		} else {
			what := "genesis group"' \
  '		} else if c.boot != nil {
			what := "genesis group"' \
  ./cmd/dkvd '^TestALostJoinGroupIsReportedNotRecreated$'

# 267. A leader's configuration change re-checks its pending reads: when it
# alone becomes the quorum, no reply would ever confirm them.
mutant "conf-change-confirms-pending-reads" internal/raft/membership.go \
  '		// ever come to confirm it otherwise.
		r.confirmReads()' \
  '		// ever come to confirm it otherwise.' \
  ./internal/raft '^TestReadsPendingWhenTheLeaderBecomesItsOwnQuorumAreConfirmed$'

# 268. A leader stepped down by its own removal's commit continues no backlog.
mutant "stepped-down-leader-continues-nothing" internal/raft/raft.go \
  '	if r.role == Leader && r.cut[peer] && match < r.log.LastIndex() {' \
  '	if r.cut[peer] && match < r.log.LastIndex() {' \
  ./internal/raft '^TestARemovedLeaderSendsNothingOnceItStepsDown$'

# 269. A snapshot offer ends a cut backlog: its acknowledgement sends the next
# batch once.
mutant "snapshot-offer-ends-the-cut" internal/raft/raft.go \
  '		delete(r.cut, peer)
		if r.snapPending[peer] == 0 {' \
  '		if r.snapPending[peer] == 0 {' \
  ./internal/raft '^TestASnapshotInstallSendsTheNextBatchOnce$'

# 270. An apply failure publishes the Status covering the entries applied
# before it, whose writes complete.
mutant "apply-failure-publishes-status" internal/raftnode/node.go \
  '		}
		n.snapshotStatus()
		return err // ErrApply, or a crash point fired' \
  '		}
		return err // ErrApply, or a crash point fired' \
  ./internal/raftnode '^TestApplyFailureStopsTheNode$'

# 271. A proposal sends a peer with a cut batch in flight a heartbeat, not
# the batch again (review of PR #10).
mutant "proposal-does-not-resend-the-batch-in-flight" internal/raft/raft.go \
  '		if r.cut[p] && !retransmit {' \
  '		if false {' \
  ./internal/raft '^TestABatchInFlightIsNotResentWithEveryProposal$'

# 272. ... but the heartbeat tick does, so a lost batch is not stranded.
mutant "heartbeat-tick-resends-the-batch" internal/raft/raft.go \
  '			r.heartbeatElapsed = 0
			r.broadcastAppend(true)' \
  '			r.heartbeatElapsed = 0
			r.broadcastAppend(false)' \
  ./internal/raft '^TestABatchInFlightIsNotResentWithEveryProposal$'

# 273. An acknowledgement never moves nextIndex back.
mutant "ack-never-lowers-next-index" internal/raft/raft.go \
  '	r.nextIndex[peer] = max(r.nextIndex[peer], match+1)' \
  '	r.nextIndex[peer] = match + 1' \
  ./internal/raft '^TestAReadDoesNotMoveNextIndexBack$'

# 274. Abandon notices are never dropped: a burst of clients giving up at
# once leaves nothing behind (review of PR #10).
mutant "abandon-notices-are-never-dropped" internal/raftnode/node.go \
  '	n.abandonList = append(n.abandonList, a)' \
  '	if len(n.abandonList) < 4 {
		n.abandonList = append(n.abandonList, a)
	}' \
  ./internal/raftnode '^TestABurstOfAbandonedRequestsLeavesNothingBehind$'

# 275. A periodic snapshot's failure publishes Status before the cycle's
# writes complete.
mutant "snapshot-failure-publishes-status" internal/raftnode/node.go \
  '			n.snapshotStatus() // the cycle'"'"'s applied writes complete (deferred): Status first
' \
  '' \
  ./internal/raftnode '^TestASnapshotFailurePublishesStatusFirst$'

# 276. The admin refuses a member id or address that could forge log lines.
mutant "admin-checks-member-ids" internal/multiraft/admin.go \
  '		if err := checkMember(req.ID, req.Addr); err != nil {' \
  '		if err := error(nil); err != nil {' \
  ./internal/multiraft '^TestAMemberIDCannotForgeLogLines$'

# 277. ... the genesis voters of create-group too.
mutant "admin-checks-genesis-voters" internal/multiraft/admin.go \
  '				if err := checkMember(m.ID, m.Addr); err != nil {' \
  '				if err := error(nil); err != nil {' \
  ./internal/multiraft '^TestAMemberIDCannotForgeLogLines$'

# 278. A refused handshake logs the claimed (unauthenticated) id quoted.
mutant "handshake-failure-quotes-the-claimed-id" internal/transport/transport.go \
  'peer=%q cluster=%q err=%v", h.id' \
  'peer=%s cluster=%q err=%v", h.id' \
  ./internal/transport '^TestAnUnauthenticatedIDCannotForgeLogLines$'

# 279. A retirement is announced only once its stop is certain.
mutant "retirement-announced-once" internal/multiraft/host.go \
  '	if what := h.busy[g]; what != "" {
		h.mu.Unlock()
		return fmt.Errorf("%w: group %d is %s", ErrGroupBusy, g, what)
	}
	hg, ok := h.groups[g]' \
  '	if what := h.busy[g]; what != "" {
		h.mu.Unlock()
		if announce != nil {
			announce()
		}
		return fmt.Errorf("%w: group %d is %s", ErrGroupBusy, g, what)
	}
	hg, ok := h.groups[g]' \
  ./internal/multiraft '^TestARetirementIsLoggedOnce$'

# 280. An initialization that cannot record a group runs nothing: it exits
# before any group starts (review of PR #10, audit H1).
mutant "unfinished-init-runs-no-group" cmd/dkvd/main.go \
  'the next start resumes the initialization)\n", c.g, dataDir, err)
				return 2' \
  'the next start resumes the initialization)\n", c.g, dataDir, err)
				continue' \
  ./cmd/dkvd '^TestAnUnfinishedInitRunsNoGroup$'

# 281. A node that would host no group is refused from its flags, before the
# data directory records anything.
mutant "flag-only-refusal-records-nothing" cmd/dkvd/main.go \
  '		if *initDir && assign != nil && len(join) == 0 && len(assign.GenesisGroups(multiraft.NodeID(*id))) == 0 {' \
  '		if false {' \
  ./cmd/dkvd '^TestAnInitRefusedByItsFlagsRecordsNothing$'

# 282. An unfinished initialization with no group state takes new settings.
mutant "unfinished-init-without-groups-repins" internal/nodedir/nodedir.go \
  '		if !legacy {
			id.Settings = opts.Settings' \
  '		if legacy && false {
			id.Settings = opts.Settings' \
  ./internal/nodedir '^TestAnUnfinishedInitWithNoGroupStateTakesNewFlags$'

# 283. A legacy log is matched by its whole name.
mutant "legacy-log-whole-name" internal/nodedir/nodedir.go \
  '			if name != own && !strings.HasPrefix(name, own+".") {' \
  '			if !strings.HasPrefix(name, own) {' \
  ./internal/nodedir '^TestALegacyLogIsMatchedByItsWholeName$'

# 284. Every return of run closes the transport.
mutant "run-closes-its-transport" cmd/dkvd/main.go \
  '	defer func() { _ = tr.Close() }() // on every return; Close is idempotent
' \
  '' \
  ./cmd/dkvd '^TestAStartupErrorClosesTheTransport$'

# 285. The metrics port caps its connections.
mutant "metrics-port-caps-connections" cmd/dkvd/main.go \
  '	ln = capListener(ln, maxMetricsConns)' \
  '' \
  ./cmd/dkvd '^TestMetricsPortCapsItsConnections$'

# 286. Open sweeps orphans only against the durable fresh manifest
# (review of PR #10, audit M11).
mutant "open-sweeps-after-the-fresh-manifest" internal/storage/lsmstore.go \
  '	if err := s.installManifest(state); err != nil {
		s.closeManifest()' \
  '	if err := s.sweepOrphans(files); err != nil {
		return nil, err
	}
	if err := s.installManifest(state); err != nil {
		s.closeManifest()' \
  ./internal/storage '^TestOpenSweepsOnlyAgainstADurableManifest$'

# 287. A failed open closes the manifest it installed.
mutant "failed-open-closes-its-manifest" internal/storage/lsmstore.go \
  '	if err := s.installManifest(state); err != nil {
		s.closeManifest()
		s.closeAllReaders()' \
  '	if err := s.installManifest(state); err != nil {
		s.closeAllReaders()' \
  ./internal/storage '^TestAFailedOpenClosesItsManifest$'

# 288. An explicit compaction honours the latch.
mutant "explicit-compaction-honours-the-latch" internal/storage/lsmcompact.go \
  '	if err := s.CompactionError(); err != nil {
		return false, classify("compact"' \
  '	if err := s.CompactionError(); err != nil && false {
		return false, classify("compact"' \
  ./internal/storage '^TestAnExplicitCompactionAfterAFailureRunsNothing$'

# 289. SyncOff never fsyncs the WAL, a flush included.
mutant "syncoff-flush-does-not-fsync" internal/storage/lsmstore.go \
  '	if s.w != nil && s.opts.WAL.SyncMode != wal.SyncOff {' \
  '	if s.w != nil {' \
  ./internal/storage '^TestSyncOffNeverFsyncsTheWAL$'

# 290. The options bound a put to what one WAL record holds.
mutant "options-bound-a-put-by-the-wal-record" internal/storage/store.go \
  '	if int64(o.MaxKeySize)+int64(o.MaxValueSize) > maxKeyValueBytes {' \
  '	if false {' \
  ./internal/storage '^TestAPutAlwaysFitsAWALRecord$'

# 291. An oversized record is refused before anything is written, unlatched.
mutant "oversized-record-latches-nothing" internal/storage/wal/wal.go \
  '	if len(payload) > record.MaxRecordSize {' \
  '	if false {' \
  ./internal/storage/wal '^TestAnOversizedRecordLatchesNothing$'

# 292. A failed write does not stop the batch syncer.
mutant "failed-write-keeps-the-batch-flush" internal/storage/wal/wal.go \
  '				if !w.closed && !w.flushFailed {' \
  '				if !w.closed && w.syncErr == nil {' \
  ./internal/storage/wal '^TestAFailedWriteDoesNotStopTheBatchFlush$'

# 293. Sync after a failed write flushes.
mutant "sync-after-a-failed-write-flushes" internal/storage/wal/wal.go \
  '	if w.flushFailed {
		return w.syncErr
	}
	if err := w.syncLocked(); err != nil {' \
  '	if w.syncErr != nil {
		return w.syncErr
	}
	if err := w.syncLocked(); err != nil {' \
  ./internal/storage/wal '^TestSyncAfterAFailedWriteFlushes$'

# 294. The actor takes the reads waiting together, a cycle's worth at once:
# taking a few per cycle costs a round each few (review of PR #10; the
# killer holds the actor while the reads queue, so the count is exact).
mutant "reads-taken-a-cycle-at-once" internal/raftnode/node.go \
  '			for i := 1; i < maxReadsPerCycle; i++ {' \
  '			for i := 1; i < 8; i++ {' \
  ./internal/raftnode '^TestConcurrentReadsShareRounds$'

# 295. A child's race report fails its integration test.
mutant "race-report-fails-the-test" tests/integration/launch_test.go \
  '				t.Errorf("%s reported a data race:\n%s", filepath.Base(cmd.Path), report)' \
  '				_ = report' \
  ./tests/integration '^TestARaceReportFailsItsTest$'

# 296. The host refuses a negative tick at Start.
mutant "host-refuses-a-negative-tick" internal/multiraft/host.go \
  '	if cfg.TickInterval < 0 {
		return nil, fmt.Errorf("multiraft: tick interval %s is negative", cfg.TickInterval)' \
  '	if false {
		return nil, fmt.Errorf("multiraft: tick interval %s is negative", cfg.TickInterval)' \
  ./internal/multiraft '^TestHostRefusesANegativeTick$'

# 297. The lab starts every process through the launcher it is given, so an
# integration test's lab processes are race-scanned and die with the test.
mutant "lab-starts-through-its-launcher" internal/lab/cluster.go \
  '	if err := start(cmd); err != nil {' \
  '	if err := cmd.Start(); err != nil {' \
  ./tests/integration '^TestALabProcessDiesWithItsTest$'

echo "== the chaos-and-operability milestone (docs/CHAOS.md, docs/OPERATIONS.md) =="

# 298. Cut leaves live connections up: a "partition" that only refuses new
#      connections, while the existing link keeps flowing.
mutant "proxy-cut-closes-live-connections" internal/netproxy/proxy.go \
  '	p.cut = true
	for c := range p.conns {
		_ = c.Close()
	}' \
  '	p.cut = true' \
  ./internal/netproxy '^TestProxyForwardsCutsAndHeals$'

# 299. The lab puts the proxy on the wrong side of a link: the dialer reaches
#      its peer directly, and cutting the link cuts nothing.
mutant "lab-proxy-on-the-dialing-side" internal/lab/cluster.go \
  '	if p, ok := c.links[[2]string{from, o.ID}]; ok {' \
  '	if p, ok := c.links[[2]string{o.ID, from}]; ok {' \
  ./internal/lab '^TestLinksFollowTheDialer$'

# 300. The next fault is scheduled from the previous one's start, not its
#      recovery: impairments overlap, and two at once can take a quorum.
mutant "chaos-one-impairment-at-a-time" internal/lab/chaos.go \
  '		t += f.Hold + between(cc.Every)' \
  '		t += between(cc.Every)' \
  ./internal/lab '^TestPlanChaosKeepsOneImpairmentAtATime$'

# 301. A chaos run whose schedule failed (a restart that did not happen)
#      passes on its history and convergence alone.
mutant "chaos-error-fails-the-run" internal/lab/chaos.go \
  '	return r.Errors() == 0 && r.Check.Linearizable' \
  '	return r.Check.Linearizable' \
  ./internal/lab '^TestARunWithAnErrorFails$'

# 302. A node that knows no leader is ready.
mutant "readiness-needs-a-leader" internal/health/health.go \
  '		case gs.Leader == "":' \
  '		case false:' \
  ./internal/health '^(TestLeaderLost|TestNodeReadinessReasons)$'

# 303. A leader is accepted without a quorum confirming it: a cut-off leader
#      counts as a working group.
mutant "health-needs-a-confirming-quorum" internal/health/health.go \
  '		case gh.Agreeing < gh.Quorum:' \
  '		case gh.Agreeing < 1:' \
  ./internal/health '^TestMajorityDown$'

# 304. dkvctl health exits 0 on a degraded cluster: a probe sees nothing wrong.
mutant "dkvctl-degraded-is-not-ok" internal/ctl/ctl.go \
  '	code := map[string]int{health.Healthy: ExitOK, health.Degraded: ExitDegraded, health.Unavailable: ExitUnavailable,' \
  '	code := map[string]int{health.Healthy: ExitOK, health.Degraded: ExitOK, health.Unavailable: ExitUnavailable,' \
  ./internal/ctl '^TestUnhealthyStatesExitNonZero$'

# 305. An unknown outcome is recorded as a refusal: the checker is told a
#      write that may have taken effect did not.
mutant "history-keeps-unknown-as-incomplete" internal/load/load.go \
  '			h.End(hid, lincheck.Incomplete, nil, "", 0, 0)' \
  '			h.End(hid, lincheck.Rejected, nil, "", 0, 0)' \
  ./internal/load '^TestTracesKeepEveryUnknownOperation$'

# 306. Every recorded write carries the same value: the checker cannot tell
#      which write a read saw.
mutant "history-values-are-distinct" internal/load/load.go \
  '	if c.cfg.History == nil {
		return c.value
	}' \
  '	if true {
		return c.value
	}' \
  ./internal/load '^TestHistoryRecordsEveryOperation$'

# 307. An UNAVAILABLE answer is not counted a refusal: the rolling restart's
#      unknown outcomes are misclassified.
mutant "unknown-cause-counts-unavailable" internal/lab/unknowns.go \
  '	case "UNAVAILABLE", "NOT_LEADER", "LOST", "SESSION_LIMIT":' \
  '	case "NOT_LEADER", "LOST", "SESSION_LIMIT":' \
  ./internal/lab '^TestClassifyUnknown$'

# 308. Status omits the leader's follower match: lag cannot be read.
mutant "status-reports-follower-match" internal/multiraft/admin.go \
  '			if len(st.FollowerMatch) > 0 {' \
  '			if false {' \
  ./internal/multiraft '^TestAdminStatusNamesItsNodeAndTheLeadersFollowers$'

# 309. A leader deposed with a write in flight answers UNKNOWN, not LOST: a
#      definite no-effect answer, which the client may retry safely, is lost.
mutant "deposed-leader-answers-lost" internal/kv/server.go \
  '	case errors.Is(err, ErrLost):
		resp.Status = StatusLost' \
  '	case errors.Is(err, ErrLost):
		resp.Status = StatusUnknown' \
  ./tests/integration '^TestRealDeposedLeaderAnswersLostThenDies$'

# 310. S1: an apply batch's mutations and its index written as two records — a
#      crash between them recovers the data without its index (R1).
mutant "apply-batch-is-one-record" internal/storage/wal/wal.go \
  '	return w.append(KindApplyBatch, a.AppendTo(nil))' \
  '	if len(a.Ops) > 0 {
		if err := w.append(KindWriteBatch, a.Ops.AppendTo(nil)); err != nil {
			return err
		}
	}
	return w.append(KindApplyBatch, ApplyBatch{Applied: a.Applied}.AppendTo(nil))' \
  ./internal/storage '^(TestApplyCrashMatrix|TestTheLegacyPairSplitsAndApplyDoesNot)$'

# 311. S1: the index written before, and apart from, the mutations — a crash
#      between them recovers an index without its data (R1).
mutant "apply-index-never-without-data" internal/storage/wal/wal.go \
  '	return w.append(KindApplyBatch, a.AppendTo(nil))' \
  '	if err := w.append(KindApplyBatch, ApplyBatch{Applied: a.Applied}.AppendTo(nil)); err != nil {
		return err
	}
	if len(a.Ops) == 0 {
		return nil
	}
	return w.append(KindWriteBatch, a.Ops.AppendTo(nil))' \
  ./internal/storage '^TestApplyCrashMatrix$'

# 312. S1: the index published before the append — a failed Apply leaves an index
#      nothing in the log records.
mutant "apply-index-published-after-the-append" internal/storage/apply.go \
  '		if err := s.w.AppendApply(ab); err != nil {' \
  '		s.applied = applied
		if err := s.w.AppendApply(ab); err != nil {' \
  ./internal/storage '^TestAFailedApplyPublishesNothing$'

# 313. S1: the index published before the mutations — a reader sees an index whose
#      data it cannot yet read.
mutant "apply-data-visible-before-index" internal/storage/apply.go \
  '		s.mu.RLock()
		mem := s.cur.mem
		s.mu.RUnlock()
		for _, o := range ops {' \
  '		s.mu.Lock()
		s.applied = applied
		s.mu.Unlock()
		s.mu.RLock()
		mem := s.cur.mem
		s.mu.RUnlock()
		for _, o := range ops {' \
  ./internal/storage '^TestPublicationOrder$'

# 314. S1: replay does not restore an apply batch's index — the recovered index lags
#      the recovered data.
mutant "replay-restores-the-apply-index" internal/storage/lsmstore.go \
  '			s.applied = AppliedIndex{Index: ab.Applied.Index, Term: ab.Applied.Term}
			s.appliedSeq = s.seq' \
  '			s.appliedSeq = s.seq' \
  ./internal/storage '^(TestApplyBatchesAndRecovery|TestApplyCrashMatrix)$'

# 315. S1: replay restores the index before the batch's mutations — the index
#      covers a sequence its data has not reached.
mutant "replay-index-after-its-mutations" internal/storage/lsmstore.go \
  '			if err := replay(ab.Ops); err != nil {
				return err
			}
			s.applied = AppliedIndex{Index: ab.Applied.Index, Term: ab.Applied.Term}
			s.appliedSeq = s.seq' \
  '			s.applied = AppliedIndex{Index: ab.Applied.Index, Term: ab.Applied.Term}
			s.appliedSeq = s.seq
			if err := replay(ab.Ops); err != nil {
				return err
			}' \
  ./internal/storage '^(TestApplyBatchesAndRecovery|TestApplyCrashMatrix)$'

# 316. S1: replay skips an empty apply batch — a no-op entry's index is lost.
mutant "replay-keeps-an-empty-batch-index" internal/storage/lsmstore.go \
  '		Apply: func(ab wal.ApplyBatch) error {
			if err := replay(ab.Ops); err != nil {' \
  '		Apply: func(ab wal.ApplyBatch) error {
			if len(ab.Ops) == 0 {
				return nil
			}
			if err := replay(ab.Ops); err != nil {' \
  ./internal/storage '^(TestAnEmptyBatchIsRecoveredAsTheIndex|TestApplyCrashMatrix)$'

# 317. S1: Apply accepts an index that does not advance.
mutant "apply-index-must-advance" internal/storage/apply.go \
  '		if applied.Index == 0 || applied.Term == 0 || !ab.Advances(cur) {' \
  '		if applied.Index == 0 || applied.Term == 0 {' \
  ./internal/storage '^TestTheAppliedIndexMustAdvance$'

# 318. S1: the term is not part of advancing — an index in an older term is accepted,
#      live and at replay.
mutant "apply-term-must-not-fall" internal/storage/wal/batch.go \
  '	return a.Applied.Index > prev.Index && a.Applied.Term >= prev.Term' \
  '	return a.Applied.Index > prev.Index' \
  "./internal/storage ./internal/storage/wal" '^(TestTheAppliedIndexMustAdvance|TestRecoverReplaysApplyBatchesAsUnits)$'

# 319. S1: replay accepts an apply record that does not advance — a log the writer
#      never produces is replayed as though it had.
mutant "replay-refuses-a-non-advancing-batch" internal/storage/wal/recover.go \
  '		if !ab.Advances(rec.AppliedIndex) {' \
  '		if false {' \
  "./internal/storage ./internal/storage/wal" '^(TestRecoverReplaysApplyBatchesAsUnits|TestApplyReplayRefusesAnIndexThatDoesNotAdvance)$'

# 320. S1: the applied index takes a sequence number — the live numbering drifts
#      from what replay reproduces.
mutant "apply-index-takes-no-sequence" internal/storage/apply.go \
  '		s.mu.Lock()
		s.applied = applied
		s.appliedSeq = s.seq' \
  '		s.seq++
		s.mu.Lock()
		s.applied = applied
		s.appliedSeq = s.seq' \
  ./internal/storage '^TestApplyBatchesAndRecovery$'

# 321. S1: replay numbers an apply batch differently from Apply — the recovered
#      sequence is not the one assigned.
mutant "replay-numbers-apply-batches-as-apply-did" internal/storage/lsmstore.go \
  '			s.applied = AppliedIndex{Index: ab.Applied.Index, Term: ab.Applied.Term}
			s.appliedSeq = s.seq' \
  '			s.seq++
			s.applied = AppliedIndex{Index: ab.Applied.Index, Term: ab.Applied.Term}
			s.appliedSeq = s.seq' \
  ./internal/storage '^(TestApplyBatchesAndRecovery|TestApplyCrashMatrix)$'

# 322. S1 gap (a): a new WAL directory's parent is not fsynced — a power loss takes
#      the directory and every synced segment in it.
mutant "wal-directory-is-durable" internal/storage/wal/wal.go \
  '		if err := fsys.SyncDir(filepath.Dir(filepath.Clean(dir))); err != nil {' \
  '		if _, err := filepath.Dir(filepath.Clean(dir)), error(nil); err != nil {' \
  "./internal/storage ./internal/storage/wal" '^(TestANewWALDirectoryIsDurable|TestApplyCrashMatrix)$'

# 323. S1 gap (b): recovery does not fsync the newest segment — what it replays, and
#      what a replay flush records, can be lost to a power loss after open.
mutant "recovery-syncs-the-newest-segment" internal/storage/wal/recover.go \
  '			if err := syncSegment(fsys, segmentPath(dir, seg)); err != nil {' \
  '			if err := error(nil); err != nil {' \
  "./internal/storage ./internal/storage/wal" '^(TestRecoverSyncsTheNewestSegment|TestApplyCrashMatrix)$'

# 324. S1: the decoder accepts an apply record of an unknown version.
mutant "apply-record-version-checked" internal/storage/wal/batch.go \
  '	if v := payload[0]; v != applyBatchVersion {' \
  '	if v := payload[0]; v == 0 && v != applyBatchVersion {' \
  ./internal/storage/wal '^TestDecodeApplyBatchRefusesMalformedPayloads$'

# 325. S1: the decoder accepts an apply record at index or term zero, which no
#      applied entry has.
mutant "apply-record-names-an-entry" internal/storage/wal/batch.go \
  '	if applied.Index == 0 || applied.Term == 0 {' \
  '	if false {' \
  ./internal/storage/wal '^TestDecodeApplyBatchRefusesMalformedPayloads$'

# 326. S1: WALStore replays around an apply batch — its mutations are silently lost.
mutant "walstore-refuses-apply-batches" internal/storage/walstore.go \
  '			return fmt.Errorf("walstore: holds no apply batches; this log was written by another engine: %w", ErrCorrupt)' \
  '			return nil' \
  ./internal/storage '^TestWALStoreRefusesApplyBatches$'

# 327. S1: MemFS keeps files under a directory a power loss should take — the model
#      would hide gap (a).
mutant "memfs-power-loss-takes-undurable-directories" internal/fault/memfs.go \
  '		if !m.undurable(name) {' \
  '		if true {' \
  ./internal/fault '^TestAMadeDirectoryIsDurableOnlyOnceItsParentIsSynced$'

# 328. S1: the WAL decoders accept a count or length written in more bytes than
#      it needs — one batch, two encodings (found by fuzzing apply batches).
mutant "wal-varints-are-canonical" internal/storage/wal/batch.go \
  '	if n > 0 && n != len(binary.AppendUvarint(nil, v)) {' \
  '	if false {' \
  ./internal/storage/wal '^(TestDecodeBatchRefusesOverlongVarints|TestDecodeApplyBatchRefusesMalformedPayloads)$'

# 329. S2: the cycle's batch leaves out the session records a decision changed — a
#      restart forgets the sessions, and a retry executes again.
mutant "lsm-batch-carries-session-records" internal/kv/lsm.go \
  '		muts = append(muts, storage.Mutation{Kind: storage.MutationPut, Key: sessionKey(id), Value: encodeSession(ss)})' \
  '		_ = ss' \
  ./internal/kv '^TestLSMMachineMatchesTheStoreOnAScript$'

# 330. S2: the applied index advances without the cycle's writes.
mutant "lsm-batch-carries-user-writes" internal/kv/lsm.go \
  '	if err := m.db.Apply(context.Background(), muts, m.last); err != nil {' \
  '	if err := m.db.Apply(context.Background(), nil, m.last); err != nil {' \
  ./internal/kv '^TestLSMMachineMatchesTheStoreOnAScript$'

# 331. S2: an entry the engine already holds is applied again at replay — a REGISTER
#      resets its session, a request becomes a duplicate of itself.
mutant "lsm-skips-what-its-engine-holds" internal/kv/lsm.go \
  '	if index <= m.engine.Index {' \
  '	if false {' \
  ./internal/kv '^TestLSMMachineSkipsWhatItsEngineHolds$'

# 332. S2: a staged write is invisible until the cycle is recorded — a read released
#      by its barrier mid-cycle misses the entry it waited for.
mutant "lsm-staged-writes-visible" internal/kv/lsm.go \
  '	if ov, ok := m.overlay[string(key)]; ok {' \
  '	if ov, ok := m.overlay[string(key)]; ok && false {' \
  ./internal/kv '^TestLSMMachineStagedWritesAreVisibleBeforeTheyAreRecorded$'

# 333. S2: a snapshot below the engine's state replaces it with the older state.
mutant "lsm-restore-below-engine-is-noop" internal/kv/lsm.go \
  '	if m.engine.Index >= index {' \
  '	if false {' \
  ./internal/kv '^TestLSMMachineARestoreBelowItsEngineChangesNothing$'

# 334. S2: a restore leaves the session table out of the engine.
mutant "lsm-restore-carries-session-records" internal/kv/lsm.go \
  '		muts = append(muts, storage.Mutation{Kind: storage.MutationPut, Key: sessionKey(id), Value: encodeSession(st.sessions[id])})' \
  '		_ = id' \
  ./internal/kv '^TestLSMMachineRestoresASnapshot$'

# 335. S2: the machine's record of what its engine holds does not follow the cycle —
#      its snapshot names a stale index.
mutant "lsm-engine-index-follows-the-cycle" internal/kv/lsm.go \
  '	m.engine = m.last
	m.resetCycle()' \
  '	m.resetCycle()' \
  ./internal/kv '^TestLSMMachineMatchesTheStoreOnAScript$'

# 336. S2: a session a decision touched is not rewritten.
mutant "lsm-touched-session-recorded" internal/kv/lsm.go \
  '		m.touched[ef.touched] = true' \
  '		_ = ef.touched' \
  ./internal/kv '^TestLSMMachineMatchesTheStoreOnAScript$'

# 337. S2: a failed cycle completes its waiters — a client is acknowledged for a
#      state the engine did not record.
mutant "cycle-failure-completes-no-waiter" internal/raftnode/node.go \
  '			n.completed = n.completed[:0]
		}
		n.snapshotStatus()' \
  '			_ = n.completed
		}
		n.snapshotStatus()' \
  ./internal/kv '^TestLSMEngineFailureFailsStopsTheNode$'

# 338. S2: the cycle is never recorded — nothing a durable machine staged reaches its
#      engine.
mutant "cycle-is-recorded" internal/raftnode/crashpoint.go \
  '		if err := csm.EndCycle(); err != nil {' \
  '		if err := error(nil); err != nil {' \
  ./internal/kv '^TestLSMEngineFailureFailsStopsTheNode$'

# 339. S2: a stopped group's machine is never closed — its engine stays open.
mutant "host-closes-the-machine" internal/multiraft/host.go \
  '	if cerr := closeMachine(hg.g.SM); cerr != nil && err == nil {' \
  '	if cerr := error(nil); cerr != nil && err == nil {' \
  ./internal/multiraft '^TestStopAndCloseCloseTheMachine$'

# 340. S2: a directory that holds an engine starts as memory — the engine goes stale
#      behind the memory machine's rebuild.
mutant "dkvd-refuses-memory-over-an-engine" cmd/dkvd/main.go \
  '					return nil, fmt.Errorf("%s holds an LSM state machine; start this node with -state-machine lsm", dir)' \
  '					_ = dir' \
  ./tests/integration '^TestRealStateMachineKindIsAnOperatorsChoice$'

# 341. S2: a user key is stored under the session tag — a user key can be a session
#      record.
mutant "user-keys-are-tagged" internal/kv/namespace.go \
  '	return append(append(make([]byte, 0, 1+len(key)), tagUser), key...)' \
  '	return append(append(make([]byte, 0, 1+len(key)), tagSession), key...)' \
  ./internal/kv '^TestUserKeysNeverCollideWithSessionRecords$'

# 342. S2: the session records are not loaded at open — a restart forgets every
#      session.
mutant "session-records-load-at-open" internal/kv/lsm.go \
  '		id, ok := parseSessionKey([]byte(k))
		if !ok {' \
  '		id, ok := parseSessionKey([]byte(k))
		if !ok || true {' \
  ./internal/kv '^TestLSMMachineMatchesTheStoreOnAScript$'

# 343. S2: a group whose state machine cannot be made leaves its directory behind,
#      to be reported as a failed group at every later start.
mutant "failed-machine-leaves-no-directory" internal/multiraft/host.go \
  '	if err != nil {
		if created {
			_ = os.Remove(GroupDir(h.cfg.DataDir, g))
		}
		return nil, fmt.Errorf("multiraft: group %d: its state machine: %w", g, err)' \
  '	if err != nil {
		return nil, fmt.Errorf("multiraft: group %d: its state machine: %w", g, err)' \
  ./internal/multiraft '^TestAFailedMachineLeavesNoDirectory$'

echo "== $KILLED/$TOTAL mutants killed =="
rm -f "$LOG" "$LOG.clean"
if [ "$TOTAL" -eq 0 ]; then
  echo "MUTATION TESTING FAILED: no mutant matched ONLY=${ONLY:-}; nothing was tested." >&2
  exit 1
fi
if [ "$FAIL" -ne 0 ]; then
  echo "MUTATION TESTING FAILED: $FAIL mutant(s) survived or could not be applied." >&2
  exit 1
fi
if [ -n "${DRY:-}" ]; then
  echo "DRY RUN PASSED: every pattern applies ($TOTAL mutants); nothing was tested."
  exit 0
fi
echo "MUTATION TESTING PASSED: every mutant was killed."
