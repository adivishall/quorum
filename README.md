# dkv — a distributed key-value store

A sharded, Raft-replicated key-value store with an LSM-tree storage engine, written from
scratch in Go. No Raft library, no embedded database, no consensus service. The storage
engine and the consensus implementation are the project.

> **Status: Phase 0 of 25 — specification complete, implementation not started.**
> This README will grow as phases land. Right now it would be dishonest to show benchmark
> numbers, a feature list, or a demo, because none of them exist yet. See
> [docs/ROADMAP.md](docs/ROADMAP.md) for what is done and what is not.

---

## What it is meant to do

Expose three operations on opaque keys — `PUT`, `GET`, `DELETE` — and handle the distributed
parts invisibly: which shard owns a key, which replica leads that shard, how the write is
replicated, when it is committed, and where the bytes physically land.

```
client ─▶ HTTP API ─▶ router ─▶ shard leader ─▶ Raft ─▶ LSM engine
                                                          │
                                       WAL ─▶ MemTable ─▶ SSTable ─▶ compaction
```

## The one thing this project refuses to do

Claim a guarantee it has not verified. Specifically:

- The consistency model is **linearizable single-key operations**, with the conditions and
  the retry caveat spelled out in [docs/CONSISTENCY.md](docs/CONSISTENCY.md). Not "strong
  consistency", which is a marketing phrase, not a model.
- Every guarantee maps to a numbered invariant in [docs/INVARIANTS.md](docs/INVARIANTS.md),
  and every invariant names the test that checks it. An invariant with no passing test is
  marked `PLANNED` and may not be cited as a guarantee anywhere else.
- The things it cannot do are enumerated in [docs/LIMITATIONS.md](docs/LIMITATIONS.md) —
  including the ones that are inherent (no Byzantine tolerance, no liveness under full
  asynchrony) and the ones that are a choice (no dynamic membership, no transactions).

## Documentation

| Document | What it covers |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Layers, ownership boundaries, shard model, concurrency model, request lifecycle |
| [DESIGN.md](docs/DESIGN.md) | On-disk formats, record framing, SSTable layout, Raft state transitions, wire protocol, recovery sequence |
| [CONSISTENCY.md](docs/CONSISTENCY.md) | The consistency model, its conditions, what is explicitly not claimed, how it gets verified |
| [FAILURE_MODEL.md](docs/FAILURE_MODEL.md) | Crash/network/clock/disk assumptions and the fault-injection matrix |
| [INVARIANTS.md](docs/INVARIANTS.md) | Numbered, falsifiable invariants bound to tests |
| [DECISIONS.md](docs/DECISIONS.md) | ADRs — what was chosen, what was rejected, what it costs |
| [LIMITATIONS.md](docs/LIMITATIONS.md) | What it does not do |
| [ROADMAP.md](docs/ROADMAP.md) | 25 phases, exit criteria, and why the order is what it is |

## Development

Requires Go 1.27+.

```bash
make check     # gofmt + go vet + go test -race  — the gate every phase must pass
make build
make test
```

## License

Not yet chosen.
