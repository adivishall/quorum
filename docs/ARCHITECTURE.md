# ARCHITECTURE

Status: **specification.** Every claim here is a *design intent* for the finished system,
not a description of what exists. As of Phase 3 the storage layer — WAL, memtable, SSTables,
recovery — is real (`docs/WAL.md`, `docs/LSM.md`); everything about sharding, replication,
consensus and networking is still design. `docs/LIMITATIONS.md` and the per-phase reports
record what is actually true of the code at any point in time.

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

---

## 3. Components and ownership boundaries

| Package | Owns | Must not know about |
|---|---|---|
| `internal/storage` | on-disk format, WAL, memtable, SSTables, compaction, local reads/writes | Raft, shards, the network |
| `internal/raft` | terms, elections, log replication, commit index | disk I/O, sockets, wall-clock time, the KV format |
| `internal/transport` | framing, dialing, timeouts, retries, fault hooks | the meaning of any message |
| `internal/routing` | hash ring, key→shard, shard→replica set | storage layout, Raft internals |
| `internal/cluster` | node lifecycle, shard hosting, wiring raft↔storage↔transport | wire encoding details |
| `internal/api` | HTTP surface, validation, error mapping | consensus, storage |
| `internal/fault` | drop/delay/duplicate/partition injection | everything else (it is a decorator) |
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
