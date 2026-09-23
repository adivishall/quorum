#!/usr/bin/env bash
#
# mutation.sh — Raft mutation testing: the Phase 9 protocol rules (docs/RAFT.md
# §12a) and the Phase 10 failure-handling rules and fault-model fidelity
# (docs/FAULTS.md §9).
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
mutant "persist-before-reply" internal/raftnode/node.go \
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
mutant "fail-stop-on-persist-failure" internal/raftnode/node.go \
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

echo "== $KILLED/$TOTAL mutants killed =="
rm -f /tmp/mutation.$$.log
if [ "$FAIL" -ne 0 ]; then
  echo "MUTATION TESTING FAILED: $FAIL mutant(s) survived or could not be applied." >&2
  exit 1
fi
echo "MUTATION TESTING PASSED: every mutant was killed."
