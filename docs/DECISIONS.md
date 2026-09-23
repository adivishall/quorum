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

---

## ADR-009 — The Bloom hash is FNV-1a + a MurmurHash3 finalizer, versioned by the SSTable magic

**Decision.** One 64-bit hash per user key: FNV-1a 64 followed by MurmurHash3's `fmix64`. Its two
32-bit halves feed the Kirsch–Mitzenmacher construction `g_i = h1 + i*h2 (mod m)` that
`docs/DESIGN.md` §5 specifies. The serialized filter carries **no version field**; its identity is
versioned by the SSTable footer magic (`DKVSST01`), so changing the hash is a format change that
must bump that magic.

**Alternatives.** (a) FNV-1a alone. (b) `hash/maphash`. (c) xxhash or SipHash as a dependency.
(d) A version byte inside the filter encoding.

**Why not (a).** This construction reads the hash's *high* half as `h2`, and FNV-1a is a
multiply-xor chain whose high bits barely mix. `h2` would be near-constant across keys and the `k`
probes would collapse toward a single bit — a filter that technically has no false negatives and
filters almost nothing. The finalizer costs three shifts and two multiplies and makes the halves
independent; the measured false-positive rate is 0.8220% against a theoretical 0.8194%.

**Why not (b).** `hash/maphash` is seeded randomly per process. The filter is persisted inside an
SSTable, so a per-process seed would make a filter written by one process return **false** for keys
another process knows are present. That is a false negative by construction, and false negatives are
the one thing a Bloom filter may never produce.

**Why not (c).** It would be the project's first third-party dependency, for a non-cryptographic
hash whose measured behaviour here already matches theory. A Bloom filter is a performance
structure, not a security primitive: an adversary who can choose keys can inflate the
false-positive rate, which costs block reads and cannot cause a wrong answer.

**Why not (d).** `docs/DESIGN.md` §5 specifies the encoding as `k u8 | m u32 | bits`, and the format
is already versioned one level up. Adding a field the spec does not have, to version something the
file's magic already versions, is improvising on a specified format.

**Cost.** Changing the hash later invalidates every existing SSTable and requires a magic bump —
there is no per-filter migration path. That is the same cost every other on-disk format in this
project carries, and it is the reason format versioning exists.

---

## ADR-010 — One MANIFEST record is one complete version edit

**Decision.** A MANIFEST holds records of a single kind, `VersionEdit`, each carrying a whole atomic
change to the file set. `docs/DESIGN.md` §6's operation numbers (`AddFile`, `DeleteFile`,
`SetNextFileNum`, `SetLastSequence`, `SetLogNumber`, `SetApplied`) become **field tags inside** that
record's payload rather than record kinds of their own.

**Alternatives.** (a) §6 read literally: one record per operation, with a compaction appending a
"record group" of `AddFile` + `DeleteFile`. (b) Explicit group begin/end marker records.

**Why not (a).** It is not atomic, and the failure is severe. The framing in `docs/DESIGN.md` §2
has no grouping primitive, so a crash between a compaction's `AddFile` and its `DeleteFile` records
would leave a manifest in which the output **and all of its inputs** are simultaneously live. That
is a state no version of the database was ever in, every file in it is individually valid, and
`Apply` has no way to recognise it as wrong — so the engine would come up cleanly holding every
superseded version and every dropped tombstone the inputs contained. Deleted keys would resurrect,
silently, and the MANIFEST would have failed at the one job it exists to do.

**Why not (b).** Markers make the *reader* responsible for noticing an unterminated group, which is
more machinery and more ways to get it wrong than simply making the unit of atomicity the unit of
checksumming. The WAL already faced this exact problem and solved it this exact way: a write batch
is one record, so either the whole batch replays or none of it does (`docs/WAL.md` §2).

**Cost.** A version edit is bounded by `record.MaxRecordSize` (64 MiB), so a single edit cannot name
an unbounded number of files. At ~60 bytes per `AddFile` that is roughly a million files in one
edit, which is far beyond anything this engine produces. The deviation from a literal reading of §6
is also a documentation cost: §6's table now describes tag numbers rather than record kinds, and
this ADR is the record of why.

## ADR-011 — Benchmark methodology: exact percentiles, storage-level write amplification, one flush counter

**Decision.** Phase 5's harness (`internal/bench`, `cmd/dkvbench`) makes four choices worth
recording, and adds exactly one instrumentation hook to the engine.

1. **Latencies are stored, not bucketed.** Every operation's duration is kept and the samples are
   sorted once at the end; percentiles use the **nearest-rank** method (the p-th percentile is the
   sample at 1-based rank `ceil(p/100 × n)`). At the operation counts these benchmarks run (millions
   at most), that is a few MB and one sort per benchmark.

2. **Write amplification is a storage-engine metric with an explicit formula.** WA = physical bytes
   the engine wrote / logical bytes the client stored, where physical = WAL segment bytes + flushed
   SSTable bytes + cumulative compaction-output bytes (`docs/BENCHMARKS.md` §3.8). It is stated to be
   *not* a filesystem-level physical-write count.

3. **One flush counter was added to the engine.** `FlushStats` (flushes, bytes, entries) is the one
   quantity Phase 5 needed that the engine did not already expose: compaction already counted its
   input and output bytes, but the flush half of the engine's writes was uncounted, and write
   amplification needs both. It is recorded only on the flush success path.

4. **Every measured benchmark repeats and reports variance;** no run modifies the engine's semantics.

**Alternatives.** (a) An HdrHistogram-style bucketed latency recorder. (b) A vaguer single
"write amplification" number, or a filesystem-level `iostat`-style physical count. (c) Reconstructing
flush bytes after the fact from live SSTable sizes instead of a counter. (d) A general metrics
framework.

**Why not (a).** Bucketing introduces bucket-boundary error that then has to be reasoned about and
documented; storing samples removes it entirely, and the memory cost is affordable here. If a future
phase benchmarks billions of operations, a histogram becomes the right trade — that is a later
decision, not this one.

**Why not (b).** A bare "write amplification: 3.3×" invites the reader to assume a
filesystem-level measurement the project cannot make (it has no OS-level write accounting). Naming
the three components and the formula makes the number checkable and its scope honest.

**Why not (c).** Compaction consumes flushed files, so live SSTable sizes undercount what flushing
actually wrote. A cumulative counter is the only way to attribute physical bytes correctly, and it
is four lines.

**Why not (d).** Observability is Phase 16. A metrics framework now would be scope creep; the flush
counter is purpose-built for the one measurement that needed it and nothing more.

**Cost.** One `atomic` triple on the engine's struct and three `Add` calls on the flush success
path — no behaviour change, and the counters are tested (`TestFlushStatsAccountForEveryFlushedTable`,
`TestFlushStatsCountFlushOutputSeparatelyFromCompaction`). The methodology choices are a documentation
cost: `docs/BENCHMARKS.md` §D exists so a reader can see exactly how each number was produced.

---

## ADR-012 — Routing is two consistent-hash rings: a fixed shard ring for `key → shard`, and a node ring for `shard → replica group`

**Context.** `docs/ARCHITECTURE.md` §4 draws one arrow, `key ─sha256▶ token ─▶ hash ring ─▶ shard id ─▶ replica group {n1,n2,n3}`, and states two facts that pull in opposite directions: the shard count is **fixed** at bootstrap (16 by default, ADR-005), and INV-C3 requires that *a membership change of one node moves only ≈ 1/N of the keys*. If routing were a single ring of shards, a node join or leave would move **zero** keys — because the shard a key lands in has nothing to do with which nodes exist — and INV-C3 would be untestable. If routing were `hash(key) % nodes`, a node change would move almost **all** keys. Neither is what the architecture wants. The redistribution property INV-C3 asks for lives in the `shard → node` step, not the `key → shard` step, and Phase 6 has to make that explicit rather than leave it implied by a one-line diagram.

**Decision.** `internal/routing` builds **two** deterministic consistent-hash rings from one immutable `Config` (`{ShardCount, ReplicationFactor, Nodes}`):

1. **Shard ring — `key → ShardID`.** The `ShardCount` shards are each placed on a 64-bit ring at `VNodesPerShard` virtual-node tokens. `Route(key)` hashes the key to a token and returns the shard owning the first ring position clockwise. This ring depends only on `ShardCount`, so it is **stable across every node membership change** — which is exactly what a fixed shard count means. It is a real ring, not `token % ShardCount`, so `key → shard` would redistribute by ≈ 1/ShardCount *if* the shard count ever changed (it does not in v1); using a ring here honours the architecture diagram and keeps the shard placement uniform via virtual nodes.

2. **Node ring — `shard → replica group`.** The `Nodes` are each placed on the ring at `VNodesPerNode` virtual-node tokens. Each shard has an anchor token; walking the node ring clockwise from it collects the first `ReplicationFactor` **distinct** node IDs — an ordered replica group whose head is the shard's **primary**. A one-node membership change reinserts one node's virtual tokens and therefore reassigns only the ≈ 1/(N+1) of shards adjacent to them; every other shard keeps its primary. This is where INV-C3's redistribution actually happens, and where `hash % N` would fail.

The replica group is **declarative metadata** — an ordered list of node IDs per shard, computed and inspectable now. Phase 6 does not replicate anything, run a Raft group, or move data; ADR-001/ADR-005 still own that. `internal/routing` imports nothing from `storage`, `raft`, `transport`, `cluster`, no networking, no clock, and no global randomness: a route is a pure function of `(key, Config)` and is bit-identical on every machine.

**The deterministic choices, settled here and specified precisely in `docs/ROUTING.md`:**

- **Token.** `token = binary.BigEndian.Uint64(sha256(bytes)[:8])` — the first 8 bytes of the SHA-256 digest, big-endian. Big-endian matches the internal-key sequence encoding (`docs/DESIGN.md` §1); "first 8 bytes" is the simplest fully-specified selection and SHA-256's uniformity makes any 8 bytes uniform. Keys are hashed as **opaque bytes** — no normalisation, casing, or trimming (INV-A5). Virtual-node tokens use the same rule over a structured label.
- **Ordering / lookup.** Ring positions are sorted ascending by `(token, ownerID, vnodeIndex)` once at construction; lookup is binary search for the first position `≥ token`, wrapping to index 0. Construction never depends on Go map iteration order.
- **Boundary rule.** A position `P` owns the half-open arc `(predecessor, P]`: a key whose token equals a position exactly is owned by that position (successor is inclusive).
- **Token collisions.** Two virtual nodes hashing to the same 64-bit token is a legal, deterministic case, resolved by the `(token, ownerID, vnodeIndex)` tie-break — **not** an error. It is distinct from a duplicate node **identity**, which *is* an error.
- **Membership change (Phase 6 sense).** Because membership is static at runtime (ADR-005), a "membership change" is the construction of a **new** immutable `Config` with a node added or removed and a comparison of the two routers — never a mutation of a live router. INV-C3 is tested exactly this way.

**Alternatives.** (a) A single shard ring only, no node ring — routing returns only a shard. (b) `hash(key) % ShardCount` for key→shard and `shard % len(nodes)` for shard→node. (c) Physical nodes on the ring (one point per node), no virtual nodes. (d) Rendezvous (HRW) hashing instead of a ring.

**Why not.** (a) leaves INV-C2's "every shard has exactly one replica group" and INV-C3's node-membership redistribution with nothing to test — the phase's exit criteria would be unmeetable. (b) is the anti-pattern the roadmap names: `shard % len(nodes)` reshuffles ≈ (N-1)/N of shards on any node change, the exact failure consistent hashing exists to prevent. (c) gives high load variance — a single node can own an arbitrarily large arc, and one node's departure dumps its whole arc on its lone successor; virtual nodes cut the variance to ≈ 1/√V and spread a departing node's load across many successors. (d) HRW is a fine alternative with even smoother distribution, but the architecture diagram commits to a *ring* with ordered token positions, and a ring gives the ordered structure the visualization renders directly; HRW has no positions to draw.

**Cost.** Two rings and a virtual-node multiplier: memory is `O((ShardCount·VNodesPerShard) + (Nodes·VNodesPerNode))` ring points, all built once and immutable. `VNodesPerShard`/`VNodesPerNode` are fixed constants (128 each) rather than tuned per deployment — a documented default, not a measured optimum, and revisited only if a distribution measurement ever justifies it. With only 16 shards the shards-moved count on a node change is quantised to whole shards, so INV-C3's movement is asserted as a principled band around `ShardCount/(N+1)` plus an exact "a key moves iff its shard's primary moved" attribution, not as a single fixed percentage (`docs/ROUTING.md` §7). The replica-group metadata invites the reader to think replication exists; it does not, and `docs/LIMITATIONS.md` and `docs/ROUTING.md` §9 say so in as many words.

---

## ADR-013 — The internal transport frames with the §2 record format (checksummed), not the §9 sketch

**Context.** Phase 0 left two different node-to-node frame layouts in the design. `docs/DESIGN.md` §2 is the checksummed record framing shared by the WAL, Raft log and MANIFEST: `crc32c(length‖kind‖payload)[4] · length[4] · kind[1] · payload`, little-endian. `docs/DESIGN.md` §9's wire-protocol sketch is a *different*, checksumless header: `length[4] · msgType[2] · flags[2] · requestID[8] · payload`. They cannot both be the frame. Phase 7 has to pick one and say why, because the choice determines what a malformed peer can do and what one framing implementation the project fuzzes.

**Decision.** The internal transport frames every message with the **§2 record format**, reusing `internal/record` (`record.Encode`, `record.MaxRecordSize`, `record.HeaderSize`, CRC-32C). The 1-byte `kind` **is** the message type (`Probe`, `ProbeResponse`, and the reserved Raft/`Forward` kinds). `docs/DESIGN.md` §9's `msgType`/`flags`/`requestID` header is retired for the frame: a message type wider than 255 is not needed (there are a handful of RPCs), and `requestID` and request/response correlation are **message-level** concerns that live inside the payload codec (`Probe`/`ProbeResponse` each carry a `uint64` request id), not in every frame's header. "Is this a response" is expressed by a distinct response *kind*, not a flag bit. `docs/DESIGN.md` §9 is updated to describe what was built, with a note pointing here. `docs/FAILURE_MODEL.md` §2 already commits to this shape — "we rely on TCP checksums plus our own framing; a frame that fails to parse closes the connection" — so §2 framing is the reading that makes that sentence true.

A transport frame carries no sender identity. **The peer a message came from is the identity established by the handshake on that connection, never a field the payload claims** (a payload-claimed sender would let any peer impersonate any other over an unauthenticated link). The handshake is a fixed preamble sent once per connection before any frame: `"DKV1"[4] · version[uint32 LE] · idLen[uint16 LE] · nodeID[idLen]`. It has no checksum of its own — the magic and the bounded, range-checked lengths are the validation, and a corrupted id is opaque bytes either way; a completed handshake is confirmed by the first valid frame, which *is* checksummed. Bounds: `MaxFrameSize = 16 MiB` (a hostile peer's per-frame allocation is bounded well below the 64 MiB on-disk record max, and Phase 14 will chunk snapshots rather than send one giant frame); `MaxNodeIDLen = 256` bytes; handshake timeout and dial timeout default to a few seconds. Exact values and the full grammar are in `docs/TRANSPORT.md`.

**A network transport is not a WAL.** The WAL's §2 reader treats a torn record at EOF as a recoverable crash-mid-append and truncates. The transport reader does the **opposite**: any short read, unexpected EOF, oversized length, CRC mismatch, or unknown kind is a hard protocol error that closes the connection. There is no torn-tail repair on a socket — a truncated TCP frame is a failed message, not durable state to salvage. Reusing the *format* and the CRC is DRY; reusing the *policy* would be a bug, so the transport has its own strict reader.

**Alternatives.** (a) Implement §9 literally (checksumless header with `msgType`/`flags`/`requestID`). (b) A third, transport-only framing. (c) Put the sender node id in every frame.

**Why not.** (a) drops the checksum `docs/FAILURE_MODEL.md` §2 relies on and forks a second framing implementation to test and fuzz, exactly what `internal/record`'s package doc warns against. (b) is gratuitous — the project already has one framing format and the task's instruction is to reuse it. (c) duplicates the handshake identity in every frame and, worse, invites trusting it over the connection identity; the rule "identity comes from the handshake" is simpler and is the only one that is safe on an unauthenticated link.

**Cost.** `docs/DESIGN.md` §9's sketch no longer matches the field layout, so it is rewritten (and this ADR records why). Request/response correlation is now per-message rather than a universal frame field, so every RPC that needs it carries its own request id — a few bytes, and only where it is actually used. The 16 MiB frame cap is a policy that Phase 14 (snapshots) may have to revisit with chunking; it is documented as a Phase 7 bound, not a permanent one.

---

## ADR-014 — One bidirectional connection per peer pair; the lower node id dials, and reconnect is the dialer's bounded retry

**Context.** A static cluster of *n* nodes (ADR-005) has `n(n-1)/2` peer pairs. If every node dials every peer, each pair opens two TCP connections and the two race; the transport then has to pick one and tear the other down, and a dial-vs-accept race can leave both sides briefly disagreeing about which connection is live. Phase 7 needs a deterministic answer to "which connection is *the* connection for this pair," to duplicate connections, to self-connection, and to reconnect — without building dynamic membership or election machinery.

**Decision.** For each peer pair, the node with the **lexicographically smaller node id dials**; the larger id only accepts. A pair therefore has exactly one initiator and, in the normal case, exactly one TCP connection, used **bidirectionally** — once the handshake completes, both ends run a reader loop and share a mutex-guarded writer, so either side sends frames on it. Duplicates are still possible under a race or a stale half-open socket, so registration is **keep-existing**: if a connection to a peer is already live, a newly-completed one for the same peer is closed. A handshake whose node id equals our own is rejected (`ErrSelfConnection`); a handshake from an id that is not a configured peer is rejected. **Reconnect is the dialer's job**: the smaller-id side runs a dial loop per higher-id peer that retries at a fixed bounded interval (no exponential backoff — there is nothing to be gentle to in a small static cluster) until connected, and stops when the transport's context is cancelled. The larger-id side never dials; it waits to be redialed. A dropped connection is detected by a failed read or write, which tears the connection down on both ends; the dialer's loop then re-establishes it.

**Alternatives.** (a) Every node dials every peer, resolve races by a tiebreak per connection. (b) One connection per message (dial, send, close). (c) Symmetric dial with a negotiated "keep the connection whose initiator has the smaller id" rule in the handshake.

**Why not.** (a) and (c) both need race resolution that (a)'s double-dialing makes routine and (c) pushes into the handshake; the "smaller id dials" rule removes the race at the source with no negotiation. (b) reopens a TCP connection and repeats the handshake for every RPC — pointless overhead for Raft's steady stream of heartbeats and AppendEntries, and it makes per-connection frame ordering (which later phases rely on) meaningless. The keep-existing dedupe is retained as a belt-and-braces for the residual race (a peer restart that redials before the old socket's death is noticed).

**Cost.** Connectivity for a pair waits for the smaller-id node to be up and dialing: if the larger-id node starts first, it sits idle until the smaller one dials it. For a static cluster this is fine — the cluster is "up" once all nodes are running — but it means a node cannot force a connection to a lower-id peer, which a future dynamic-membership phase might want to change. Reconnect being a fixed-interval retry means a flapping peer is redialed at a steady cadence rather than backing off; acceptable at cluster scale, and revisited only if a measurement shows the redials matter.

---

## ADR-015 — Phase 8 is a *local* replicated-log model with a 1-based Raft-shaped log; it adds no distributed guarantee

**Context.** Phase 9 implements Raft. Raft's correctness is argued over a **local log** with an exact contract — contiguous indexes, a follower rewriting a conflicting suffix, a `commitIndex` that advances and a state machine that applies committed entries in order — and Phase 6 already produces the `shard → ordered replica group` metadata that says *who* a shard's replicas are. The roadmap puts a replication phase (8) between networking (7) and Raft (9) precisely so the local primitive Raft stands on exists, is deterministic, and has its edge cases executable *before* consensus is written on top of it. The risk this phase has to avoid is scope: it would be easy to start deciding *when* an entry commits, which is Raft, and to attach a distributed-consistency claim the code cannot support.

**Decision.** `internal/replication` defines two things and nothing above them:

1. **A `ReplicaGroup`** — an immutable, validated, replication-layer handle on one shard's ordered replica set, *consumed from* `internal/routing` (`ReplicaGroupsFromRouter`) rather than recomputed. It carries the shard id, the ordered replica node IDs (head = primary, matching the routing contract), and the replication factor (= replica count). It reuses `routing.NodeID`/`routing.ShardID` so the same identity types flow through the system. Membership stays static (ADR-005): no join/leave/promote/rebalance. Validation **refuses** an empty group, an empty or duplicate replica id, RF < 1, and RF ≠ replica count — it never deduplicates or reorders, because a duplicate is a caller bug and the order *is* the routing contract.

2. **A local `Log`** — a small explicit interface (`Append`, `TruncateAndAppend`, `Term`, `At`, `Slice`, `FirstIndex`, `LastIndex`, `Commit`/`CommitIndex`, `Apply`/`AppliedIndex`/`Unapplied`) with an in-memory reference implementation, `MemoryLog`. Indexes are **1-based**, contiguous, with 0 as the empty sentinel — the Raft convention (`docs/DESIGN.md` §8.4) so Phase 9 inherits rather than translates it. Terms are non-decreasing. Ranges are half-open `[lo,hi)`. Suffix replacement retains a prefix, drops the existing suffix, appends a contiguous batch, cannot start past the end (no gap), and **cannot replace a committed entry**. `commitIndex`/`appliedIndex` are monotonic watermarks with `applied ≤ commit ≤ lastIndex` enforced; application is a watermark so an index is applied at most once. Following ADR-002, `MemoryLog` holds no locks, starts no goroutine, and reads no clock or randomness — it is a pure object a single goroutine drives, exactly like the coming `raft.Raft`. Entry `Data` is copied in and out (INV-A1 discipline).

Phase 8 also names the **state-machine seam** (`StateMachine.Apply(index, command)`) without building the driver, and wires nothing to the LSM engine. The new invariants are the **INV-P** series, distinct from every existing namespace. `docs/REPLICATION.md` specifies all of the above.

The decision the interface encodes, repeated because it is the boundary: **Phase 8 records that an index *is* committed; it does not decide that a distributed group is *allowed* to commit it.** No file in the repository gains a consensus, replication, linearizability, or fault-tolerance claim.

**Alternatives.** (a) Skip Phase 8 and build the log inside `internal/raft` in Phase 9. (b) Make the log persistent now (share the WAL's record framing). (c) 0-based indexing. (d) A larger "replication engine" interface that already models terms/leaders so Phase 9 has less to write. (e) Re-derive replica groups in the replication layer instead of consuming routing's.

**Why not.** (a) is how Raft implementations end up untested at the log level: the log's edge cases (gap rejection, committed-suffix protection, copy-safety, commit/apply monotonicity) get debugged tangled up with elections. Isolating them is the point of a separate phase, and it is cheap. (b) makes the bugs durable before the interface is known to be right — the same reasoning as deferring snapshots (Phase 14) until consensus is correct; the in-memory model is the deterministic thing to drive first, and persistence is a later, additive change. (c) fights the whole Raft literature and `docs/DESIGN.md` §8, buying nothing. (d) is scope creep in the exact direction this phase must not go — terms-as-elections, leader state, and quorums are Raft, and putting them in the log's interface would either be dead code or a consensus claim Phase 8 cannot back. (e) duplicates ADR-012's hashing in a second package; consuming the public metadata keeps one source of truth and honours the layer map (`docs/ARCHITECTURE.md` §3).

**Cost.** The Phase 8 log is in-memory, so nothing here survives a restart — a limitation stated in `docs/LIMITATIONS.md`, removed when the persistent replicated log lands. `MemoryLog` being lock-free means it is single-goroutine-only, an assumption Phase 9's node driver must honour (it will, per ADR-002/ARCHITECTURE §5b). Enforcing non-decreasing terms and committed-suffix protection locally is slightly more validation than a "dumb" append-only buffer would need, but it turns a class of Phase 9 bug into a loud local error instead of silent divergence, which is the trade this project makes everywhere. Reusing `routing.NodeID`/`routing.ShardID` couples `internal/replication` to `internal/routing`'s public types; that is the intended dependency direction (routing → replica-group metadata → replication) and is far cheaper than a parallel identity type that would have to be converted at every boundary.
