# DECISIONS

Architecture decision records. Each entry states the decision, the alternatives that were
genuinely considered, why the alternative lost, and what the decision costs us. A decision
with no cost listed has not been thought about hard enough.

---

## ADR-001 — Multi-Raft (one Raft group per shard), not a single global log

**Decision.** Partition the key space into a fixed number of shards; each shard is an
independent Raft group with its own log, leader, and storage.

**Alternatives.** (a) One global Raft log for the whole cluster. (b) Leaderless quorum
replication (Dynamo-style) with read repair.

**Why not (a).** Every write in the cluster would serialize behind one leader, making the
sharding layer decorative — you could not demonstrate independent shard failure or independent
elections, which is half the point of the project. (b) gives availability but only eventual
consistency, and the project's central claim is linearizability.

**Cost.** No cross-shard atomicity, ever. N heartbeat streams instead of 1. Rebalancing (which
we are not implementing) becomes a per-shard data migration problem.

---

## ADR-002 — Raft core is a pure, deterministic state machine with no I/O and no clock

**Decision.** `internal/raft` exposes `Step`/`Tick`/`Propose`/`Ready`/`Advance`. It does not
touch disk, sockets, `time.Now()`, or global randomness. Time enters as logical ticks;
randomness is an injected `rand.Source`.

**Alternatives.** The "natural" design: a `RaftNode` goroutine with timers, a network client,
and a storage handle wired directly in.

**Why not.** Consensus bugs are timing bugs. A Raft implementation with embedded goroutines and
timers can only be tested by running it and hoping the scheduler reproduces the interleaving
that breaks it. A pure core means a whole 5-node cluster with a partitioned, message-dropping,
message-duplicating network runs in one goroutine, deterministically, replayable from a seed —
so a failure found in CI is reproducible on a laptop with one integer. This is the design
decision the entire test strategy rests on.

**Cost.** More ceremony: the driver must correctly persist everything in a `Ready` before
sending its messages, and `Advance` must not be called early. Getting that contract wrong
reintroduces exactly the durability bugs we are trying to avoid, so the contract is asserted
in tests rather than assumed.

---

## ADR-003 — Custom framed TCP RPC internally; HTTP/JSON for clients; no gRPC

**Decision.** Hand-written binary codec and framing over TCP for node-to-node traffic. HTTP/JSON
for the client API.

**Alternatives.** gRPC + protobuf for everything.

**Why not gRPC.** The fault-injection framework (Phase 10) needs to drop, delay, duplicate, and
reorder **individual logical messages**. Under gRPC, framing and connection management are
opaque, and fault injection degrades to "kill the connection" — which is a much weaker test
and would make the chaos suite partly theatre. Owning the transport is what makes the fault
matrix real. Secondary: no protoc in CI, and fuzzing our own codec tests our code rather than
protobuf's.

**Cost.** We write and test marshalling by hand, including the boring parts (varints, bounds
checks, malformed-input handling). We do not get gRPC's streaming, deadlines, or interceptors
for free. `transport.Transport` is an interface so a gRPC backend remains possible.

---

## ADR-004 — ReadIndex for linearizable reads; leader leases rejected

**Decision.** A linearizable read confirms leadership with a quorum heartbeat round before
serving (`docs/DESIGN.md` §8.5).

**Alternatives.** (a) Route reads through the Raft log as no-op entries. (b) Leader leases:
serve reads locally for a lease period, assuming bounded clock drift.

**Why not (a).** Correct but forces a disk write and a full replication round per read.
**Why not (b).** Leases trade a clock assumption for latency. `docs/FAILURE_MODEL.md` §3 commits
to making no clock assumption for safety, and taking it back for a performance win would make
the headline consistency claim conditional on unverifiable hardware behavior.

**Cost.** Every linearizable read pays one network round trip to a quorum. Read throughput is
bounded by the leader. `stale` mode exists as an explicit, clearly-labeled escape hatch.

---

## ADR-005 — Static cluster membership in v1

**Decision.** Node set and shard count are fixed at bootstrap. No runtime join/leave, no shard
rebalancing.

**Alternatives.** Raft joint consensus (§6 of the paper) plus a data-migration protocol.

**Why not.** It is a large, genuinely hard subsystem — configuration changes that overlap with
elections are one of the most bug-prone parts of any Raft implementation — and it is orthogonal
to everything the demo needs to show (sharding, replication, election, recovery). Building it
badly would be worse than not building it.

**Cost.** Cannot grow or shrink a running cluster. Node *failure and recovery* works fine (the
node rejoins with the same ID); node *replacement* requires a restart of the cluster config.
This is stated in `docs/LIMITATIONS.md` rather than glossed.

---

## ADR-006 — Two logs per shard (Raft log + engine WAL), accepting 2x write amplification

**Decision.** The Raft log and the storage engine's WAL are separate files, both fsynced.

**Alternatives.** Use the Raft log *as* the engine's WAL — the state machine replays from the
Raft log after the last flushed applied index, and the engine keeps no WAL of its own.

**Why not (yet).** The unified design is what production systems do and it halves write
amplification, but it couples the storage engine to the consensus layer: the engine can no
longer be used, tested, or benchmarked standalone, and Phases 1–5 depend on exactly that
independence. Keeping them separate means a storage bug is provably a storage bug.

**Cost.** Every write is durably written twice. This will show up in the Phase 5 vs Phase 19
benchmark comparison and we will report it as a real number rather than hide it. The engine
already records `appliedIndex`, so the unification is a future change, not a rewrite.

---

## ADR-007 — Size-tiered compaction before leveled

**Decision.** v1 uses size-tiered compaction (merge all of L0 when it reaches 4 files; 10x size
ratio between levels).

**Alternatives.** Leveled compaction (LevelDB/RocksDB style) from the start.

**Why not.** Leveled compaction has better read and space amplification and worse write
amplification, and choosing it on the basis of a blog post rather than a measurement is exactly
the kind of cargo-culting this project is supposed to avoid. Size-tiered is simpler, easier to
prove correct (especially tombstone handling), and gives us a baseline to measure against.

**Cost.** Worse read amplification — a point lookup may touch more files. Bloom filters mitigate
it. If Phase 5 benchmarks show it dominating, the decision gets revisited with data.

---

## ADR-008 — Single-key API only; no transactions, no range scans in v1

**Decision.** The public API is `PUT`/`GET`/`DELETE` on one key.

**Why.** This is not a limitation working around laziness; it is what makes the consistency
claim provable. Linearizability composes across single-object operations (Herlihy & Wing's
locality property), so per-key linearizability yields a linearizable system. Add multi-key
atomicity and that argument collapses and a much harder one is required.

**Cost.** Not a general-purpose database. Range scans exist internally (compaction needs them)
but are not exposed.
