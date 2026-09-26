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

**Decision.** For each peer pair, the node with the **lexicographically smaller node id dials**; the larger id only accepts. A pair therefore has exactly one initiator and, in the normal case, exactly one TCP connection, used **bidirectionally** — once the handshake completes, both ends run a reader loop and share a mutex-guarded writer, so either side sends frames on it. Duplicates are still possible under a race or a stale half-open socket, so registration is **keep-existing**: if a connection to a peer is already live, a newly-completed one for the same peer is closed. A handshake whose node id equals our own is rejected (`ErrSelfConnection`); a handshake from an id that is not a configured peer is rejected. **Reconnect is the dialer's job**: the smaller-id side runs a dial loop per higher-id peer that retries at a fixed bounded interval (no exponential backoff — there is nothing to be gentle to in a small static cluster) until connected, and stops when the transport's context is cancelled. The larger-id side never dials; it waits to be redialed. A cleanly dropped connection is detected by a failed read or write, which tears it down on both ends; the dialer's loop then re-establishes it.

**Amended in Phase 10 — a silent connection is not detected by read/write failure.** The clause above assumed every dead connection surfaces as a read or write error. It does not. A connection that is established but delivers nothing — a peer that vanished without sending a FIN (host crash, network black-hole), or, in the real-process fault tests, a proxied link that accepts bytes but never forwards them — leaves the reader blocked in `Read` indefinitely, while writes into the dead socket's buffer still succeed. Because a registered connection also suppresses the dial loop (`hasConn`), the node then believes it holds a live peer it can never actually reach, and never reconnects. Two defences were added (`docs/FAULTS.md` §13): the dialer waits its retry interval after a connection *dies*, not only after a failed dial, so a peer that accepts-and-resets cannot become a reconnect storm; and, in the Raft deployment, `cmd/dkvd` sets `transport.Config.ReadIdleTimeout` (~120 ticks — well above the heartbeat interval and one election timeout, so a heartbeated link is never torn down) so a connection that delivers no frame for that long is torn down and redialled. TCP keepalive was considered and rejected: a connection stuck in a listener's accept backlog is kernel-established, so keepalive probes are answered and the connection is never detected as dead — only an application-level idle deadline catches it.

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

---

## ADR-016 — Raft is a pure `Ready`/`Advance` core driving the Phase 8 log; codecs live in `internal/raft`; the durable log is an append-only record stream reconstructed with per-record truncation

**Context.** Phase 9 implements Raft (§5.1–5.4 of Ongaro & Ousterhout), which `docs/DESIGN.md` §8 already specifies for this repository: a pure deterministic core (ADR-002), a durable log + `HardState` (§8.1), the tick/election model (§8.2–8.3), log matching with a conflict hint (§8.4), and the §5.4.2 commit rule. Several concrete design points are left open by §8 and must be settled before code exists, because getting them wrong is how a Raft implementation ends up either untestable or subtly unsafe: how the pure core hands work to an impure driver, where the message codec lives given the layer rules, how a *suffix-replacing* Raft log is made durable on an *append-only* record stream, and what crash policy that log uses.

**Decision.**

1. **The core is a pure `Ready`/`Advance` state machine that drives the Phase 8 `replication.Log`.** `internal/raft.Raft` exposes `Tick()`, `Propose(data)`, `Step(msg)` as the only inputs, and `Ready()`/`Advance()` plus `NextApply()`/`AppliedTo(index)` as the only outputs — no sockets, goroutines, clock, filesystem, or global randomness (election-timeout jitter comes from an injected `rand.Source`). A `Ready` carries, in the order the driver must act on them, the `HardState` to persist (when `currentTerm`/`votedFor` changed), the unstable log `Entries` to persist, and the `Messages` to send **after** those persist — which is what makes "durable before the dependent reply" (INV-R6) hold by construction rather than by remembering to fsync. Apply is deliberately *not* in `Ready`: the driver pulls `NextApply()` (the log's committed-but-unapplied entries) after persisting, applies them to the state machine, and only then calls `AppliedTo`, so an apply failure cannot advance `appliedIndex`. The core's log **is** a `replication.Log` (an in-memory `MemoryLog` in practice), so Raft *drives the Phase 8 primitive* directly: `TruncateAndAppend` for AppendEntries, `Append` for proposals, `Commit` for the commit rule, `Unapplied`/`Apply` for the apply path. This is the etcd/raft `Ready` contract, trimmed to what this project needs; it is chosen over a "step returns messages" model because only an explicit persist-then-send batch expresses the ordering safety Raft requires.

2. **The Raft message codec lives in `internal/raft` (pure bytes) and the driver maps message types to the transport's reserved kinds.** `internal/raft` defines `Message`/`MessageType` and hand-written, bounded `Marshal`/`Unmarshal` (no gob/JSON/protobuf, matching ADR-003), fuzzed in that package. It does **not** import `internal/transport`: the driver (`internal/raftnode`) maps `MessageType` ↔ the already-reserved `transport.MsgRequestVote`/`MsgRequestVoteResponse`/`MsgAppendEntries`/`MsgAppendEntriesResponse` kinds (ADR-013) and moves the bytes. This keeps the transport a generic byte carrier that "does not know what a term means" (the §25 rule) and keeps the Raft core free of TCP, while still reusing the one framing the project fuzzes — no second wire protocol is invented. The snapshot/forward kinds stay reserved and inactive (Phases 14/13).

3. **The durable Raft log is an append-only record stream, and a conflicting-suffix replacement is reconstructed by replaying each `Entry` record as a truncate-and-set.** The file (`internal/raftlog`) reuses the §2 record framing (`internal/record`): a stream of `Entry` records and `HardState` records, kinds in the Raft-log namespace. Raft *overwrites* a conflicting suffix, but the file is never rewritten in place: a replacement simply **appends** the new `Entry` records, and recovery replays records in file order, applying each `Entry` at index `i` as "set index `i` = this entry and drop anything above `i`" — exactly the Phase 8 `TruncateAndAppend` semantics per record. This reconstructs the final in-memory log from an append-only file, so a truncation costs no rewrite and the "last record wins" rule §8.1 states falls out naturally (it is also how the last `HardState` wins). `commitIndex` is persisted as a `HardState` field but is an optimization only: recovery never trusts it beyond `min(persisted, lastIndex)` and correctness does not depend on it (§8.1).

4. **The Raft log's crash policy is not the WAL's.** A torn final record (crash mid-append) truncates to the last good offset and the recovered state is the prefix before it — safe, because the reply that depended on the torn record was never sent (persist precedes reply). Any of: a checksum mismatch with bytes following, a zero-filled header with data after it, an unknown record kind, a malformed payload, or a non-monotonic index progression that a crash cannot explain, is fatal and refuses to open — never silently skipped (the same discipline as INV-S8/INV-M2, and `internal/record` already distinguishes `ErrTornTail` from `ErrCorrupt`).

5. **Membership is fixed (ADR-005); quorum is `⌊n/2⌋+1` of the configured group, and a one-node group commits on its own.** The core computes majority over the immutable peer set. A single-node group reaches quorum with only its own `matchIndex`, so it elects itself, appends the mandatory no-op, and commits without waiting for a peer — tested explicitly. The core operates on **one** group (Multi-Raft: the eventual driver instantiates one per shard, §27); Phase 9 ships a single-group driver.

**Alternatives.** (a) A "`Step` returns `[]Message`" core with persistence as a side call. (b) Put the Raft codec in `internal/transport` beside `Probe` (ADR-013's pattern). (c) A rewrite-in-place or truncating durable log. (d) Persist `commitIndex` as required state. (e) Bake the group size / quorum into the transport or routing layer.

**Why not.** (a) loses the one property that matters — it cannot express "these messages may only leave after this fsync" without the driver re-deriving the dependency, which is precisely the durability bug ADR-002 warns the driver contract must not reintroduce. (b) would make `internal/transport` import `internal/replication` (for `Entry`) and understand terms and log indexes, breaking the §25 boundary that keeps fault injection (Phase 10) able to treat messages as opaque; the Raft codec is not a framing, so it does not belong with the framing. (c) an in-place truncating log is more code and more crash windows (a partially-truncated file is a new failure mode) for no benefit, since append-plus-replay reconstructs the same state and the log is bounded until snapshots (Phase 14) add compaction. (d) trusting a persisted `commitIndex` for correctness would make a stale or torn commit record able to *lower* safety; §8.1 already says it is recoverable, so it stays an optimization. (e) quorum is a Raft-group property; putting it in transport or routing would couple layers that ADR-012/ADR-013 keep separate.

**Cost.** The `Ready`/`Advance` contract is ceremony the driver must get exactly right (persist the whole `Ready` before sending any of its messages, `Advance` only after) — so that contract is asserted in tests, not assumed, exactly as ADR-002 anticipated. The append-only durable log grows on every truncation as well as every append, so a pathological election-churn workload writes more than the live log holds; this is bounded and irrelevant until Phase 14 adds snapshotting/compaction, and is recorded in `docs/LIMITATIONS.md`. Splitting the codec (in `raft`) from the kind mapping (in `raftnode`) means one extra tiny mapping layer; it is the price of keeping the transport ignorant of Raft, which is a property Phase 10 depends on. The durable log is one file per group with `f.Sync()` durability — process-kill safe, not power-loss proven, the same honest bound the WAL carries (`docs/FAILURE_MODEL.md`).

---

## ADR-017 — Fault injection lives at the real boundaries: a filesystem seam, transport and disk decorators, and a deterministic simulator that drives the real core, log and driver ordering

**Context.** Phase 10 must show how the Raft system behaves under drop, delay, duplication, reordering, partitions, crashes, restarts and persistence failures — reproducibly, from a seed. Two tempting designs would make that evidence worthless. One is to put fault hooks inside `internal/raft`, which would break ADR-002 (the core is pure) and test a core that production never runs. The other is a "fault framework" of booleans around test doubles: a fake log, a fake transport, a hand-copied driver loop. A simulator that re-implements the driver's persist-then-send ordering proves the copy, not the code; a mocked log that "fails" proves nothing about what the real log leaves on disk.

**Decision.**

1. **The core stays pure and unaware of faults.** `internal/raft` gains no code for Phase 10. Every fault is injected outside it.
2. **The durable log does all file I/O through a two-interface seam, `internal/vfs`** (`FS`, `File` — the subset of `package os` it uses; `*os.File` satisfies `File` as-is). A nil FS is the real OS, which is what production passes. This is the one seam through which persistence faults and crash behaviour are injected, underneath the unchanged `raftlog` code.
3. **`internal/fault` holds Raft-agnostic decorators and models**: `MemFS` (a crash-consistent in-memory filesystem — a process crash keeps written bytes, a modeled power loss keeps only fsynced ones), `InjectFS` (armed, one-shot write/fsync failures, torn writes and stalls over any FS, with an op log), `Links` (the partition model), and `Network` (a `transport.Transport` decorator with partitions and drop/duplicate/hold/block rules). It imports neither `raft` nor `raftlog`.
4. **The driver's two protocol-critical steps are single functions shared with the simulator**: `raftnode.DrainReady` (persist the Ready, then send, then advance; stop at the first persistence failure) and `raftnode.Recover` (open the durable log and rebuild the core). The node's actor loop calls them; so does the simulator.
5. **`internal/raftsim` is a deterministic, single-goroutine cluster simulator** built from those real parts. Only the clock (explicit ticks), the network (an in-memory queue) and the disk's crash behaviour are simulated. Every scheduling decision is an event in a script; a run is a pure function of (config, script), a seeded chaos run of (profile, seed); every run is traced, hashed, replayable and minimizable, and the INV-R and INV-F checks run after every event.
6. **Persistence failure is fail-stop.** A failed durable-log write or fsync latches in `raftlog` (no further writes) and stops the driver (no message of that Ready, no further event); `dkvd` exits non-zero. Recovery is a restart, which truncates any torn tail and makes the recovered state durable before use.

**Alternatives.** (a) Fault hooks in the Raft core. (b) A fake log and fake transport for fault tests. (c) Fault injection only against real processes (kill, iptables). (d) A simulator with its own copy of the driver loop. (e) Making persistence failures retryable.

**Why not.** (a) breaks ADR-002 and tests something production never runs. (b) proves the fakes; the real torn-tail truncation, record framing, and recovery would never meet a fault. (c) is not reproducible from a seed, needs root for real network faults, and cannot express "fail exactly this fsync"; real-process tests are kept as a third tier for what only they can show (real TCP, real SIGKILL, real processes), not as the only tier. (d) would let the simulator and the driver drift apart, so a driver ordering bug would pass every simulated schedule. (e) is unsafe: after a failed or short write the file may end in a partial record, so a retried append would turn a recoverable torn tail into mid-log corruption; after a failed fsync a later "successful" fsync says nothing about the earlier data.

**Cost.** Two new packages and one tiny one; two exported driver functions whose contracts must now stay stable for the simulator; an `FS` field on `raftlog.Options` and `raftnode.Config` that production leaves nil. The simulator's disk model is a model — it assumes fsync is honest and models torn data as a prefix — so it can prove the code's write/fsync *ordering*, not hardware durability, and it says so. A fail-stop node needs an operator to restart it; that is the intended trade, stated in `docs/FAILURE_MODEL.md` §4 ("a node that cannot persist must not vote and must not acknowledge").

---

## ADR-018 — Crash windows are named points on the driver's cycle and the log's record boundaries; the same points are crashed exhaustively in the simulator, in-process, and on real processes that kill themselves there

**Context.** Phase 10 crashed nodes between events. A process dies between two record writes, after an fsync but before the reply, after one peer got a message but not the next, after `Apply` but before `AppliedTo`. Phase 11 must show what each such window leaves on disk and what recovery makes of it, for every window — reproducibly, without timing guesses, and without putting crash machinery into the pure core (ADR-002) or duplicating the driver's ordering in a test harness (ADR-017).

**Decision.**

1. **Crash points are a small enumeration on the driver, `raftnode.Point`**: before/after the Save, after each message hand-off, before/after Advance, before/after Apply and after AppliedTo. The driver's two protocol-critical loops are single functions with an optional `Hook` at those points — `DrainReadyAt` and `ApplyCommitted` — used verbatim by the actor, the simulator and `dkvd`. A nil hook is production and costs a nil check. Returning an error from the hook aborts the cycle at that boundary, which is what a crash is from the node's point of view; the driver then stops exactly as it does for a persistence failure.
2. **I/O boundaries inside a Save and inside recovery are crash points at the `vfs` seam**, not in the driver: `fault.Injection.At` is an observation point — "before the Nth write / fsync / truncate of this file" — that runs a callback and lets the operation proceed. In the simulator the callback marks the disk's process crashed (every later operation on its handles fails, the kernel keeps every accepted byte); in `dkvd` it kills the process. The log's own code is unchanged.
3. **A Save's records are ordered so that a crash between any two leaves a log recovery accepts** (`raftlog.SavePlan`, one pure function the log writes by and the simulator's crash model reads): a changed term or vote first, carrying the previously durable commit; the entries; the new commit last. This replaces the entries-then-HardState order, under which a crash between the entry record and the HardState record of a single-node election left entries above the durable term and the core refused the log — the node could never restart. The matrix found it on its first run.
4. **The simulator enumerates and crashes at every point** (`raftsim.RunCrashMatrix`): one run with point recording collects every `(node, point, occurrence)` a scenario reaches; then one run per point and crash mode with `crashat` armed first, an immediate restart through `raftnode.Recover`, the rest of the scenario, and convergence. Every restart is checked against the shadow's record of every completed Save (INV-F2) and the Phase 11 checks (INV-CR1..3). The report is machine-readable and each cell names the event that reproduces it. `crashat` is also a chaos event (`crashpoints` profile) and a fuzz event.
5. **Real processes crash at the same points via `dkvd -crash-at=POINT[:N]`**: a test seam in the binary that installs the hook (driver points) or the observation point (I/O points) and, when reached, logs `event=crash_point` and SIGKILLs the process. Unset, it installs nothing.
6. **Application is documented as it is**: `appliedIndex` is volatile, a restart replays the whole committed prefix, so application is at-least-once across restarts and exactly-once within an incarnation (INV-CR4). No deduplication is added; that is Phase 13's contract, not the driver's.

**Alternatives.** (a) Sleep-and-kill real-process tests ("kill 100 ms after the proposal"). (b) Crash hooks inside `internal/raft`. (c) A separate crash harness that re-implements the driver loop with crash points. (d) Repair a term regression on recovery (raise `currentTerm` to the last entry's term) instead of changing the write order. (e) Persist `appliedIndex` so a restart does not re-apply.

**Why not.** (a) does not name a window — it hits whichever one the scheduler picks, and passes or fails for reasons a rerun cannot reproduce. (b) breaks ADR-002 and tests a core production never runs. (c) would drift from the real driver, so a driver ordering bug could pass every crashed schedule — the same reason ADR-017 shares `DrainReady`. (d) is a silent repair: recovery would invent a term (and forget whatever vote the interrupted Save was casting) rather than the log being coherent by construction; the write-order fix costs at most one extra ~15-byte record per Save and makes every boundary safe. (e) would change the node's durable format and semantics for a property the state machine will carry itself (the engine records its own applied index), and would mask the at-least-once contract instead of stating it.

**Cost.** A `Hook` field on `raftnode.Config`, two exported loop functions whose points must stay stable, an `At` field on `fault.Injection`, a flag on `dkvd` that is documented as a test seam, and one more HardState record on the rare Save that changes both term and commit. The simulator's model of a Save's records is derived from `SavePlan`, so a change to the order must change both — by construction, not by remembering. The matrix's scenario is bounded: it proves every point *it reaches*; the `crashpoints` profile and the fuzzer reach others. Real power loss stays untested.

---

## ADR-019 — Client-visible linearizability: ReadIndex in the pure core, completion at commit-and-apply in the proposal's term, explicit outcome classes, and a validated checker over histories from every tier

**Context.** Phases 9–11 made Raft safe; none of that says what a *client* sees. Raft safety does not make reads linearizable (a leader's local read is Raft-safe and stale), a write acknowledged at append is lost on a leader change, and a client that times out does not know whether its write happened. Phase 12 must define the client-visible contract, implement the parts the contract needs, and check the histories real clients observe — without trusting the checker blindly, without hiding retries, without clock assumptions, and without starting Phase 13 (request ids, forwarding, deduplication) or Phase 15 (the HTTP API).

**Decision.**

1. **The object is the whole key-value map with single-key PUT/GET/DELETE**, whose sequential specification is the Phase 1 register contract (`lincheck.Step`). Linearizability of the map is checked per key, which is exact by Herlihy & Wing's locality theorem because no operation touches two keys; a multi-key operation would invalidate this and needs a different checker.
2. **Reads use ReadIndex, implemented in the pure core** (`Raft.ReadIndex`, `Ready.ReadStates`): only a leader registers a read; the read index is `max(commitIndex, own no-op index)`; confirmation needs a quorum of AppendEntries responses to a request sent **after** registration, identified by a heartbeat sequence (`Message.Seq`) every response echoes; any role change drops unconfirmed reads. No leases — no clock assumption for safety.
3. **A write completes to the client only when its entry is committed and applied on the serving node, and the entry applied at its index carries the proposal's term** (`raftnode.Waiters`). A different term at that index is `ErrLost` — a definite no-effect. A deadline, a stop or a dead connection is unknown. The same `Waiters`/`Reads` code runs in the node and in the simulator.
4. **Every client-visible outcome is one of three classes and the history says which**: definite with effect (must linearize), definite without effect (excluded), unknown (a write optional, a read unconstrained). The test client never retries an unknown write; every attempt is recorded.
5. **A minimal operation protocol, not an API**: `kv.Serve`/`kv.Client` over framed TCP on `dkvd -client-listen`, one outstanding request per connection, a failed connection is never reused. It exists so real processes can be driven by history-recording clients.
6. **The checker is independent of the system and is itself tested first**: `internal/lincheck` imports nothing from Raft or KV; it is cross-validated against an oracle that shares none of its code (whole-history, definitional, backward real-time rule), against brute force, on a file corpus of known-good and known-bad histories, by fuzzing, and by mutants of itself. A search budget turns an intractable history into UNCHECKED, never a verdict.
7. **Histories come from three tiers, with faults at synchronization points**: real `dkvd` processes (with a signal-armed crash seam that kills the leader at an exact point of one write's life, including before and after the reply), the real driver in-process, and the deterministic simulator (client events in the script; INV-X5..X8 checked at each completion independently of the checker; replay and minimization). No fault is injected after a guessed sleep.

**Alternatives.** (a) Leader leases for reads. (b) Reads through the log (a no-op per read). (c) ReadIndex in the driver, outside the core. (d) Completing writes at commit on the leader without waiting for apply, or at append. (e) Retrying unknown writes in the test client. (f) Using a third-party checker (Porcupine/Knossos). (g) Building the Phase 15 HTTP API now to have something to test.

**Why not.** (a) needs bounded clock drift, which `docs/FAILURE_MODEL.md` refuses to assume. (b) is correct but costs a log entry and an fsync per read, and hides the question ReadIndex answers. (c) would put protocol state (which acknowledgement confirms which read) outside the core the simulator drives, and it could not be checked deterministically. (d) at append is simply wrong (a leader change loses it); at commit-without-apply the serving node's own later reads would have to wait anyway, and apply-in-term is the point that distinguishes "committed" from "lost" without extra state. (e) would apply a write twice under one recorded operation and make the history lie; retries belong to Phase 13's dedup. (f) would add a dependency for something small enough to own and, more importantly, to validate line by line against our exact outcome semantics. (g) is Phase 15 scope; a protocol that exists only to be tested is honest about that.

**Cost.** One uvarint per AppendEntries message and response (`Seq`); a quorum round trip per read; an isolated leader's pending reads and write waiters accumulate until it learns a higher term (no check-quorum); the `-client-listen` port and the crash seam's arming signal and reply points are flags production leaves unset. The guarantee is stated for recorded finite histories plus an argument with named assumptions — not a proof over all executions — and for honest histories only until Phase 13 (a hidden retry of an unknown write is outside it). The checker is exponential in mutually concurrent writes per key (UNCHECKED beyond ~16); the system's histories stay far below that.

## ADR-020 — Request identity is a cluster-assigned session plus a client RequestID; deduplication is decided at apply from a bounded session table inside the replicated state machine; non-leaders forward one hop

**Context.** Phase 12 left one hole in the client contract: a client that does not know whether its write took effect cannot ask again without risking a second execution, so the test client never retried an unknown write and a hidden retry was outside the guarantee (`TestRealIncompleteWriteThenRetry`). Phase 13 must let a client retry safely — across timeouts, crashes, restarts, leader changes, reconnects and forwarding — with statuses that say exactly what is known, with bounded memory, without inferring identity from connections, without keeping correctness-critical state outside Raft, and without starting Phase 14 (snapshots) or Phase 15 (the HTTP API).

**Decision.**

1. **Identity = (ClientID, RequestID).** A ClientID is a session the cluster assigns: the log index of the committed `REGISTER` entry that created it — unique for the life of the log without any randomness, never reused, never tied to a connection. A RequestID is chosen by the client, unique within its session. ClientID 0 keeps Phase 12's anonymous semantics. With every request the client sends **AckedBelow**, the lowest id it has no response for — its promise never to send the ids below it again.
2. **The decision is made at apply**, in log order, identically on every replica, by the replicated state machine (`kv.Store`): registered / executed / duplicate (with the original index) / conflict (same id, different SHA-256 fingerprint of the canonical command) / stale (below the watermark) / expired (unknown or evicted session) / limit. The table records (fingerprint, index) per executed request — a write's response is fully determined by it — and is rebuilt by log replay; there is no other persistence.
3. **Bounds are part of the state machine**: at most MaxSessions sessions (LRU by log index, never a clock) and MaxUnacked results per session; results below the watermark are forgotten. Eviction is never silent — an evicted session's requests are `SESSION_EXPIRED`, never executed as new; a request past the result bound is `SESSION_LIMIT`, never evicting a result a retry might need.
4. **Eleven statuses in three classes** — definite with effect, definite without effect (some only "for this attempt"), unknown — so the client always knows which it has; the client library retries only the unknown and the refusals that leave the request's fate open, and always under the same identity; reads are never deduplicated.
5. **Forwarding is one hop over the internal transport** (kinds 32/33): a forwarded request is never forwarded again, a forward is never resent, not-sent is `UNAVAILABLE`, sent-and-unanswered is `UNKNOWN_OUTCOME`; redirect-only is a flag. `raftnode` gains a generic application-message seam (`SetAppHandler`/`SendApp`) so the driver stays ignorant of client semantics.
6. **Verification treats retries as metadata**: a request's sends are one logical operation for the checker (invoked at the first send of its command, completed at its first acknowledgement), validated against an independent reading of the contract; the simulator checks every replica's every apply-time decision against an independent session model (INV-X11); real-process tests replay the processes' durable logs against the same model.

**Alternatives.** (a) A dedup table in the leader's memory, keyed by client connection or id. (b) Deduplicating at propose time. (c) Persisting the table separately from the log. (d) Client-generated random ClientIDs (UUIDs). (e) Caching full response bytes. (f) Time-based session expiry (TTL). (g) Evicting the oldest result when a session is full (bounded, "at-least-once after eviction"). (h) Redirect-only, no forwarding. (i) Multi-hop forwarding or forwarder-side retries. (j) Deduplicating reads.

**Why not.** (a) is volatile, per node and racy: a retry after a crash or at a new leader executes again, and two concurrent copies both miss. (b) cannot see a duplicate proposed before its original commits, nor a predecessor's unapplied tail; only apply sees one total order. (c) adds a second source of truth that must be kept atomic with the log. (d) collides with small probability and gives nothing to evict by in log order; a log index is unique by construction. (e) is unnecessary: (index, decision) determines the response. (f) needs clocks that agree across replicas, which the failure model refuses; a replicated decision cannot depend on local time. (g) turns an evicted result into a silent second execution — the thing this phase exists to prevent. (h) is kept as a mode, but forwarding lets any node serve and puts the unknown-outcome handling where the client's identity makes it safe. (i) can loop or duplicate work outside the log; one hop plus client retries is enough and provably terminates. (j) a read has no effect; recording it would cost memory for nothing.

**Cost.** One REGISTER entry per session; ~165 ns and a 32-byte fingerprint per identified write at apply (SHA-256 over the command: linear in the value); ≈107 B per remembered result, 13.4 MiB for a full table at the defaults; an O(MaxSessions) scan per evicting REGISTER (≈10 µs) and an O(MaxUnacked) scan per watermark raise (≈1 µs); one forwarding hop ≈26 µs on loopback (`docs/DEDUP.md` §8). The limits are configuration every replica must share, and nothing checks that they do. Sessions are not authenticated. The table is rebuilt by full replay, so Phase 14's snapshots must include it. The guarantee is at most once per identity — exactly once if it executes — not exactly-once delivery, and nothing for anonymous writes beyond Phase 12.
