# ARCHITECTURE

Status: **specification.** Every claim here is a *design intent* for the finished system,
not a description of what exists. As of Phase 12 these parts are real: the storage engine (WAL,
memtable, SSTables, Bloom filters, compaction, MANIFEST — Phases 1–5), routing as a library
(Phase 6), node processes and the TCP transport (Phase 7), the local replicated-log model (Phase
8), a single Raft group with a durable log and node driver (Phase 9), fault injection (Phase 10),
crash-window recovery (Phase 11), and — Phase 12 — a replicated key-value state machine
(`internal/kv`, in memory), linearizable reads through ReadIndex, write completion at
commit-and-apply, and a minimal test-facing operation protocol on each node's `-client-listen`
port. The client API, request forwarding and deduplication, one Raft group per hosted shard, the
LSM engine as the state machine, snapshots and the dashboard are still design.
`docs/LIMITATIONS.md` and the per-phase reports record what is actually true of the code at any
point in time.

---

## 1. What this system is

Quorum is a distributed, replicated, sharded key-value store. It supports exactly three
logical operations on opaque byte keys and values:

```
PUT(key, value)
GET(key) -> value | NotFound
DELETE(key)
```

There are no transactions, no range scans exposed to clients (v1), no secondary indexes,
and no multi-key atomicity. That restriction is deliberate and it is what makes the
consistency claim in `docs/CONSISTENCY.md` defensible rather than aspirational.

A client talks to *any* node. That node routes the request to the shard that owns the key,
and to the Raft leader of that shard's replica group. The client never needs to know which
physical node stores a key.

---

## 2. Layer map

```
                      ┌─────────────────────────┐
  client ────────────▶│  HTTP/JSON API  (:8080) │   pkg/client, internal/api
                      └───────────┬─────────────┘
                                  │
                      ┌───────────▼─────────────┐
                      │   Router                │   internal/routing
                      │   key → shard → leader  │
                      └───────────┬─────────────┘
                                  │  local call, or internal RPC to peer
                      ┌───────────▼─────────────┐
                      │   ShardServer           │   internal/cluster
                      │   one per hosted shard  │
                      └───────────┬─────────────┘
                                  │  Propose(cmd)
                      ┌───────────▼─────────────┐
                      │   Raft group (shard N)  │   internal/raft
                      │   deterministic core    │
                      └───────────┬─────────────┘
                                  │  committed entries, in log order
                      ┌───────────▼─────────────┐
                      │   State machine         │   internal/storage
                      │   LSM engine            │
                      └───────────┬─────────────┘
                                  │
                   WAL ──▶ MemTable ──flush──▶ SSTables ──compact──▶ SSTables
```

Node-to-node traffic (Raft RPCs, request forwarding) uses our own framed TCP protocol
(`internal/transport`, port 7001+). Client traffic uses HTTP/JSON (port 8080+).
The two are separated so that fault injection on the internal network cannot be confused
with client-side failures.

As of Phase 7 the internal transport is **implemented** (`internal/transport`, `cmd/dkvd`,
`docs/TRANSPORT.md`): real node processes, a checksummed framed-TCP protocol, a version
handshake, one bidirectional connection per peer pair, and `Probe`/`ProbeResponse` liveness.
It carries bytes tagged with a message kind and knows nothing of what they mean. Since Phase 9
it carries Raft traffic (`RequestVote`/`AppendEntries`, codec in `internal/raft`, ADR-016); it
still forwards no client requests and hosts no storage engine. Phase 10 injects network faults
*around* it — a `transport.Transport` decorator in-process, TCP proxies between real processes —
without changing it (ADR-017).

---

## 3. Components and ownership boundaries

| Package | Owns | Must not know about |
|---|---|---|
| `internal/storage` | on-disk format, WAL, memtable, SSTables, compaction, local reads/writes | Raft, shards, the network |
| `internal/raft` | terms, elections, log replication, commit index | disk I/O, sockets, wall-clock time, the KV format |
| `internal/transport` | framing, dialing, timeouts, retries | the meaning of any message |
| `internal/routing` | hash ring, key→shard, shard→replica set | storage layout, Raft internals |
| `internal/cluster` | node lifecycle, shard hosting, wiring raft↔storage↔transport | wire encoding details |
| `internal/api` | HTTP surface, validation, error mapping | consensus, storage |
| `internal/raftlog` | the durable Raft log: record framing, crash policy, recovery, the failure latch | the Raft algorithm, sockets |
| `internal/raftnode` | the driver: actor loop, ticks, persist-then-send ordering, per-peer outboxes, apply | the algorithm's rules (it drives the core) |
| `internal/vfs` | the filesystem seam the durable log does I/O through | everything else |
| `internal/fault` | drop/delay/duplicate/partition injection (a transport decorator), a crash-consistent disk model, I/O fault injection | Raft (it is a set of decorators and models) |
| `internal/raftsim` | the deterministic fault-injection simulator (tests only) | wall-clock time, goroutines, real I/O |
| `internal/metrics` | counters, histograms | business logic |

The rule that matters most: **`internal/raft` performs no I/O and reads no clock.**
See §5.

---

## 4. Shard model (multi-Raft)

The key space is divided into a **fixed** number of shards (default 16), decided at cluster
bootstrap and never changed at runtime in v1.

```
key ──sha256──▶ 64-bit token ──▶ hash ring ──▶ shard id ──▶ replica group {n1,n2,n3}
```

As of Phase 6 the routing half of this pipeline is **implemented** in `internal/routing` and
specified in `docs/ROUTING.md`: `key → shard` is a consistent-hash ring over the fixed shard
set, and `shard → replica group` is a second consistent-hash ring over the node set, producing
**declarative** ownership metadata. ADR-012 records why routing is two rings rather than one, and
why the redistribution guarantee (INV-C3) lives in the node ring.

As of Phase 8, `internal/replication` (`docs/REPLICATION.md`, ADR-015) turns that declarative
`shard → replica group` metadata into a validated, immutable `ReplicaGroup`, and defines the
**local replicated-log model** — a `Log` interface with an in-memory `MemoryLog` — that Phase 9's
Raft will drive: contiguous 1-based indexes, deterministic suffix replacement that cannot
overwrite a committed entry, and monotonic commit/apply bookkeeping. This is a **local** primitive
only. The `replica group → Raft group` step — actually replicating across nodes, deciding when an
entry commits, electing a leader, and serving requests — is Phase 9+ and does not exist yet;
Phase 8 adds no distributed or consistency guarantee.

As of Phase 9, `internal/raft`, `internal/raftlog`, and `internal/raftnode` (`docs/RAFT.md`,
ADR-016) implement **Raft**: the pure deterministic core (§5a below is now real, not aspirational)
drives the Phase 8 log, a durable log + HardState makes its state crash-safe, and a node driver
runs a real group over the transport (electing a leader, replicating, committing). This is the
consensus core for one group; the multi-Raft node that instantiates one group per shard and serves
clients is a later phase. Raft's safety properties are verified in deterministic simulation and by
a real 3-process smoke test; end-to-end linearizability and request serving are Phases 12–13 and
are **not** claimed yet.

As of Phase 10 (`docs/FAULTS.md`, ADR-017) that group's behaviour under failure is tested at
three levels: a deterministic simulator (`internal/raftsim`) drives the real core, the real durable
log on a crash-modeling disk, and the driver's own persist-then-send and recovery functions
through seeded, replayable fault schedules with every safety invariant checked after every event;
the real driver runs under injected disk and network faults; and real processes are killed,
frozen and partitioned. `internal/raft` itself gained no fault code — the core stays pure.

As of Phase 12 (`docs/LINEARIZABILITY.md`, ADR-019) the group serves **client operations** end to
end: `internal/kv` is its state machine (`kv.Store`, the Phase 1 register semantics in memory,
rebuilt by replay on restart) and its server (`kv.Server`: `PUT`/`DELETE` through
`raftnode.Node.Write`, which completes only when the entry is committed and applied on this node in
the proposal's term; `GET` through `raftnode.Node.ReadIndex`, which completes only when a quorum has
confirmed this leader after the read was registered and the store has applied through the read
index). The pure core gained ReadIndex (a heartbeat sequence echoed by every AppendEntries
response) and stays pure. The histories clients observe — from real processes, the in-process
driver and the simulator — are checked by `internal/lincheck`. The protocol that carries them
(`kv.Serve`/`kv.Client`, framed TCP on `-client-listen`) is a test boundary, not the client API:
no HTTP, no request ids, no forwarding, no deduplication.

As of Phase 11 (`docs/CRASH_RECOVERY.md`, ADR-018) the node's **crash windows** are characterised
and proven: the driver's persist → send → advance → apply cycle exposes named crash points
(`raftnode.Point`), the durable log's record boundaries are crash points at the `vfs` seam, and the
same points are exercised three ways — exhaustively in the simulator (a crash at every point a
scenario reaches, in every crash mode, each followed by a real recovery and checked against an
independent record of what was persisted), in-process on the real driver, and on real `dkvd`
processes that SIGKILL themselves at the point. What a crash leaves and what recovery makes of it
is stated per boundary; the one window that bricked a node (a Save's record order) was found and
fixed. The core is still untouched.

Each shard is an **independent Raft group** with its own log, its own leader, and its own
storage directory. A 3-node cluster with 16 shards runs 16 Raft groups; every node is a
member of every shard's group when RF == cluster size, and of a subset otherwise.

This is the "multi-Raft" design (TiKV/CockroachDB style) rather than one global Raft log.
Rationale: a single global log makes every write serialize behind one leader, which makes
the sharding phase decorative. Independent groups mean shards genuinely fail, elect, and
recover independently — which is what the demo is supposed to show.

Cost of this choice, stated up front:
- No cross-shard atomicity. A write touching two keys in two shards is two independent
  operations. We do not offer, and will not claim, multi-key transactions.
- N Raft groups means N heartbeat streams. At 16 shards × 3 nodes this is fine; it does not
  scale to thousands of shards without batching heartbeats, which we are not doing.

---

## 5. Concurrency model

Three distinct disciplines, chosen per layer:

**(a) Raft core — deterministic state machine, zero concurrency.**
`raft.Raft` is a pure object. It has no goroutines, no locks, no timers, no sockets.
The only entry points are:

```go
Step(m Message) error   // handle an inbound RPC
Tick()                  // advance logical time by one tick
Propose(data []byte)    // append a client command
Ready() Ready           // messages to send, entries to persist, entries to apply
Advance(Ready)          // acknowledge that the Ready was durably handled
```

Randomness (election timeout jitter) comes from an injected `rand.Source`. Given the same
seed and the same sequence of `Step`/`Tick` calls, the node's behavior is bit-for-bit
reproducible. This is the single most important design decision in the project: it turns
consensus testing from "run it and hope" into deterministic, replayable unit tests, and it
lets a whole cluster be simulated in one goroutine with a controlled network.

**(b) Node driver — one goroutine per Raft group.**
A `RaftNode` goroutine owns one `raft.Raft` and serializes: inbound RPCs, ticks from a real
`time.Ticker`, and client proposals — all via channels. Because only this goroutine touches
the core, the core needs no mutexes.

**(c) Storage — reader/writer with immutable snapshots.**
Writes are serialized by a single write lock (one writer at a time, which the WAL requires
anyway). Reads take a reference-counted, immutable *version* (memtable + sstable set) and
proceed lock-free against it. Compaction installs a new version atomically; old versions are
dropped when their last reader releases them.

Everything is validated under `go test -race`.

---

## 6. Persistence layout

```
<data-dir>/
  node.json                 # node id, addr, bootstrap config (written once)
  shard-0000/
    raft/
      hardstate             # term, votedFor, commitIndex — fsynced before any reply
      log/000001.log        # segmented raft log, CRC'd records
      snapshot/000042.snap
    kv/
      MANIFEST-000001       # append-only set-of-sstables edit log
      CURRENT               # points at the live MANIFEST (atomic rename)
      wal/000007.log        # engine WAL
      000013.sst
      000014.sst
  shard-0001/
    ...
```

Two logs exist per shard (Raft log and engine WAL). That is a real 2x write amplification
and we are not going to hide it — see `docs/DESIGN.md` §"Write amplification".

---

## 7. Process and deployment topology

One OS process per node (`cmd/dkvd`). Ports per node:

| Port | Protocol | Purpose |
|---|---|---|
| 7001+ | framed TCP | internal: Raft RPC, request forwarding, control |
| 8080+ | HTTP/JSON | client API, `/health`, `/cluster`, `/metrics` |

`docker compose up` starts 3 (or 5) such processes in separate containers plus the
dashboard. The demo kills *containers/processes*, not simulated in-memory nodes.

The in-process deterministic simulator (§5a) exists for tests only and is never presented
as the multi-node demo.

---

## 8. Request lifecycle (write)

1. Client `PUT /kv/user:123` hits node A.
2. Router: `shard = ring.Lookup("user:123")` → shard 5. Replica group = {A, B, C}.
3. Node A knows (from its own Raft group for shard 5, or from a cached hint) that B is the
   current leader. If A is not the leader it forwards over internal RPC, or returns a
   redirect — decided in Phase 13, documented in `docs/API.md`.
4. Leader B: `Propose(encode(Put, key, value, clientID, seqNo))`.
5. Raft appends to B's log, persists (fsync), replicates via AppendEntries.
6. When a majority has acknowledged the entry *and* the entry is from B's current term,
   `commitIndex` advances.
7. Committed entries are handed to the state machine in log order. The engine writes the
   WAL record, updates the memtable, and records the applied Raft index.
8. Only after apply does B answer the client `200 OK`.

Step 8 is what lets us talk about linearizability. Answering at step 6 would be faster and
would be a lie about read-your-writes.

**Phase 12 — what is implemented of this path.** Steps 4–8 are real for one group, with the
in-memory `kv.Store` in place of the engine (step 7) and the `-client-listen` protocol in place of
HTTP (steps 1–3: a non-leader answers "not leader" with a hint and the client redirects; there is
no forwarding and no `clientID/seqNo` — Phase 13). Step 8 is precise: the reply is sent only after
the entry at the proposal's index is applied **with the proposal's term**; a different entry
applied there means the write was lost (a definite no-effect); a deadline or a dead node means the
outcome is unknown (`docs/LINEARIZABILITY.md` §3–§4). Reads do not enter the log: the leader
registers a ReadIndex, confirms leadership with a quorum round begun after the read, and answers
once applied through the read index (§5 there).

---

## 9. What is explicitly out of scope for v1

- Dynamic cluster membership (adding/removing nodes at runtime) and shard rebalancing.
  The hash ring *algorithm* handles membership change; the *cluster* does not migrate data.
- Cross-shard transactions.
- Authentication, authorization, TLS.
- Range scans / iterators over the client API.
- Geo-distribution, WAN tuning, lease-based reads.

These are listed here so that the absence of them is a documented decision rather than a
discovered hole.
