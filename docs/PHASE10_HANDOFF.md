# Phase 10 handoff (session checkpoint)

Checkpoint written when the session was stopped mid-validation. Phase 10 is **implemented and
committed locally but NOT complete**: one real-process test is flaky, nothing is pushed, and CI
has never run on the Phase 10 commits. Phase 11 has not been started.

```yaml
phase: 10
status: implementation-complete, validation-incomplete
branch: main
head: a552dfa9ecdabf6c5271f3b4a09bbb11212d4249   # before this handoff commit
phase9_base: c4899c7d09dbcc901581e2d4ba12db9dfcb9f5b8
origin_main: c4899c7d09dbcc901581e2d4ba12db9dfcb9f5b8   # nothing pushed
ahead_of_origin: 12            # 13 after this handoff commit
working_tree: clean            # before this file was added
uncommitted_changes: none
internal_raft_changed_since_phase9: false   # `git diff c4899c7 -- internal/raft` is empty
flaky_test: tests/integration TestRealRestartWhileIsolated   # root cause NOT established
ci_run_on_phase10: never
phase11_started: false
```

## Commits made (oldest first, all local)

| SHA | Purpose |
|---|---|
| be7b1cd | docs(faults): Phase 10 fault model + ADR-017 (spec; INV-F PLANNED) |
| 363271d | feat(fault): `internal/vfs` seam; `internal/fault` MemFS, InjectFS, Links, Network |
| 20482c2 | fix(raftlog): durability-failure latch; fsync recovered state at Open; I/O via vfs |
| 123cc5a | fix(raftnode): fail-stop, per-peer outboxes, consistent Status, durable Propose + ctx; shared DrainReady/Recover |
| 186eda9 | fix(dkvd): events from one status snapshot; exit 1 on fail-stop |
| cde1aca | feat(raftsim): deterministic simulator + scripted scenarios A–I |
| 76a1023 | test(raftsim): seeded schedules, replay, minimization, platform hash, fuzz |
| 4d72fcb | test(integration): real-process fault tests + TCP proxy; dkvd follower (term,leader) events |
| 52c0442 | test(mutation): 14 Phase 10 mutants; `make faults`, `make fuzz` |
| 5ba5f93 | docs: finalize Phase 10 (FAULTS, INVARIANTS, FAILURE_MODEL §7, stale-doc fixes) |
| 368a250 | ci: faults, mutation, fuzz jobs |
| a552dfa | test(raftsim): fault-coverage guard checked across the seed set, not per run |

## Components completed

- `internal/vfs` (filesystem seam), `internal/fault` (MemFS crash model, InjectFS I/O faults incl.
  stalls, Links, transport `Network` decorator), `internal/raftsim` (simulator, events, trace,
  continuous INV-R1..R10 + INV-F2/F4 checks, chaos profiles, Stabilize/INV-F3, Replay, Minimize,
  fuzz).
- Fixes found by Phase 10: persistence-failure latch/fail-stop (raftlog + raftnode), fsync of the
  recovered log at `raftlog.Open` (simulator-found: ack of un-fsynced cached entries), per-peer
  outboxes (wedged peer stalled the leader), `Propose` honours caller ctx and answers after
  persistence, dkvd torn-status events / zombie on failure / follower term events.
- Docs: `docs/FAULTS.md` (verified reference), INV-F1..F5 in `docs/INVARIANTS.md`, FAILURE_MODEL §7
  matrix, ROADMAP Phase 10 ☑ (with qualifiers), stale Phase 6–9 doc statements corrected.

## Verified so far (and where)

| Check | Result | Tree it ran on |
|---|---|---|
| gofmt, go vet | clean | a552dfa |
| per-commit `go build && go vet && go test -count=1 ./...` (clean worktree, 12 commits) | 11 PASS; 368a250 FAIL = the flaky test below (368a250 changes only CI YAML) | each commit |
| `make check` (race, all packages) | PASS | code-identical to a552dfa except the a552dfa test guard and transport comments |
| `make mutation` | 22/22 killed | 52c0442 (no mutated file changed since) |
| `make faults FAULT_SEEDS=2000` (10,000 schedules) | PASS, 0 invariant violations | a552dfa |
| `make fuzz FUZZTIME=10s` | 17/17 targets PASS | a552dfa |
| real-process fault tests, 5× under `-race` | PASS | 4d72fcb |
| `TestRealRestartWhileIsolated` ×15 (no race) | 14 PASS / **1 FAIL** | a552dfa |
| GitHub Actions | **not run** (nothing pushed) | — |

## The flaky test — evidence, not a diagnosis

**Test:** `tests/integration/raft_fault_test.go: TestRealRestartWhileIsolated`.
**Rate observed:** 1/15 isolated runs; 1 more failure inside the per-commit loop (at 368a250).
**Failure:** `raft_fault_test.go:528: no confirmed leader among [n3 n2] above term 1 within 20s` —
i.e. at the FIRST re-election (after n1 is isolated+killed and n3, the old leader, is killed and
restarted), not after the partition heals.

**What the failing run's output shows:**
- n2 campaigns for 20 s; every `VoteRequest` to n1 fails (`peer not connected` — expected, n1 is
  dead and isolated).
- n2 logs `peer_disconnected peer=n3` (old n3 killed) and then `peer_connected peer=n3
  dir=outbound` — n2 believes it has a live connection to n3 through the (n2,n3) proxy.
- The restarted n3 logs only `node_started`, `ready`, `raft_started term=1 lastIndex=1`,
  `raft_commit index=1`: **no `peer_connected`, no `handshake_failed`, no `raft_follower`**.
  So n2's handshake never reached (or was never registered by) the new n3 process, while n2's
  side of the proxied connection stayed open.

**Not yet established:** why. Unconfirmed hypotheses to test, in order:
1. The proxy (`tests/integration/tcpproxy_test.go`) pairs n2's new client connection with an
   upstream dial made in the window between the old n3 dying and the new n3 listening, leaving
   the client leg open while the upstream leg is dead or never delivers the handshake. The
   transport has no read idle timeout by default (`DefaultReadIdleTimeout = 0`), so n2 would not
   notice a half-dead proxied connection.
2. n3's transport received the connection but closed it silently via the keep-existing rule in
   `TCPTransport.serve` (no log line on that path).
3. Something else in the restart window (port rebind timing on macOS).
Before interruption I was also considering a *different* issue — `waitLeader` followed by
`waitFollows` can pin a leader that a later disruption replaces — but this failure is NOT that;
treat it as an unconfirmed secondary robustness concern.

**Consequences to correct once resolved:** commit 4d72fcb's message says the real-process suite
was "stable across 5 consecutive -race runs" — true when written, but the suite is now known to
be flaky. The same restart-through-proxy pattern is used by `TestRealLeaderCrashAndReelection`,
`TestRealFollowerCrashAndCatchUp` and `TestRealRepeatedCrashRestart`; they may share the cause.

## Remaining before Phase 10 can be declared complete

1. Root-cause the flake (instrument: log proxy upstream dial results/closures; log the
   keep-existing close in `serve`; re-run the single test with `-count=30`). Fix the root cause
   (test harness or transport), not the symptom. Then re-run it ≥30× plain and ≥10× `-race`.
2. Re-run at the final HEAD: `make check`, `make integration`, `make mutation`,
   `make faults FAULT_SEEDS=200`, `make fuzz FUZZTIME=10s`.
3. Push to `origin/main`; confirm all five CI jobs (check, integration, faults, mutation, fuzz)
   are green on the pushed HEAD. **Risk:** `TestTraceIsPlatformIndependent` pins a trace hash
   recorded on darwin/arm64; if linux/amd64 differs, investigate the nondeterminism — do not just
   re-record the hash.
4. Verify tree clean and local main == origin/main; decide whether to keep or delete this file.
5. Write the final Phase 10 report (18-point format requested), including the flake and its fix.

## Next commands (next session)

```bash
cd "/Users/adivishal/Projects/DISTRIBUTED KV"
git status && git log --oneline c4899c7..HEAD          # expect 13 local commits, clean tree
go test -count=30 -v -run 'TestRealRestartWhileIsolated$' ./tests/integration > /tmp/iso.log 2>&1; grep -cE '^--- FAIL' /tmp/iso.log
go test -count=10 -run 'TestReal|TestProxy' ./tests/integration   # do the sibling restart tests flake too?
# after fixing the root cause:
make check && make integration && make mutation && make faults FAULT_SEEDS=200 && make fuzz FUZZTIME=10s
git push origin main && gh run list --branch main --limit 1
gh run view <run-id> --json conclusion,jobs --jq '.conclusion, (.jobs[] | "\(.name): \(.conclusion)")'
```

## Risks and open questions

- 13 commits exist only in this local clone until pushed.
- The flake's root cause may be in the test proxy (test-only) or in the transport's handling of a
  half-open connection (production behaviour: no read idle timeout); which one decides whether a
  production fix is needed.
- CI runtimes of the new mutation and fuzz jobs are unmeasured.
- The platform-independence of the simulator trace is asserted by a test that has only ever run on
  darwin/arm64.
