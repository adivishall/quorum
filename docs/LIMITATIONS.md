# LIMITATIONS

The things this system does not do, cannot do, or has not proven. Kept current: an item may be
removed only when a test exists showing it is no longer true.

**Status: Phase 2.** A single-node key-value store with a durable write-ahead log exists.
Nothing else does.

### True right now, and temporary

| Limitation | Removed in |
|---|---|
| **The entire live data set is held in memory.** The WAL makes writes durable; it does not make them fit on disk. A dataset larger than RAM will not fit. | Phase 3 (memtable + SSTables) |
| **The entire log is replayed on every open.** Startup time is proportional to total bytes ever written, not to the live data set. | Phase 4 (truncation) |
| **The WAL grows without bound.** Nothing reclaims segments. | Phase 4 |
| **Power-loss durability is claimed for `sync` mode but untested, and untested for every other mode.** The crash tests destroy a real process with SIGKILL, which only proves data reached the kernel. | not testable here — see `docs/WAL.md` §9 |
| Segments missing from the *start* of the WAL sequence are undetectable (gaps in the middle are refused) | Phase 4 (MANIFEST log number) |
| A length field corrupted within the 64 MiB range can cause a torn-tail/corruption misclassification in the newest segment | inherent to this framing; `docs/WAL.md` §8 |
| `sync` mode serialises writers behind the device flush (~3.9 ms/append measured on an M4) | Phase 5, if group commit is measured to be worth it |
| No SSTables, no compaction, no Bloom filters | Phases 3–4 |
| No networking, no cluster, no replication, no consensus | Phases 7–9 |
| Memory is bounded only by the host: there is no eviction and no flush-to-disk, so a large dataset will OOM | Phase 3 (memtable flush) |
| `dkv put` cannot carry a maximum-size (1 MiB) value, because `ARG_MAX` is 1 MiB on macOS and the kernel rejects the exec. `dkv shell` can. This is an OS limit, not a dkv limit. | not applicable — use `dkv shell`, or the HTTP API from Phase 15 |
| The CLI is still in-memory-only: it does not yet open a data directory, so `dkv` remains ephemeral even though the storage layer is not | Phase 15 (CLI wiring) |
| The interactive shell cannot express keys containing whitespace, because it splits on whitespace. The one-shot form and the Go API can. | Phase 15 (HTTP API) |

### The list below is what will still be true when v1 is complete.

---

## Not implemented (by decision)

| Limitation | Why | Reference |
|---|---|---|
| No dynamic cluster membership; no adding/removing nodes at runtime | Joint consensus + data migration is a large subsystem orthogonal to the demo | ADR-005 |
| No shard rebalancing | Same | ADR-005 |
| No cross-shard transactions, no multi-key atomicity | Deliberate: it is what makes the linearizability argument hold | ADR-008 |
| No range scans / iterators in the client API | Not needed; would complicate the consistency story | ADR-008 |
| No authentication, authorization, or TLS | Out of scope; the project is about storage and consensus | — |
| No compression, no block cache, no prefix compression | Deferred until a benchmark justifies them | DESIGN §11 |
| No leveled compaction | Size-tiered first, with measurements | ADR-007 |
| No leader leases | Would require a clock-drift assumption | ADR-004 |

## Inherent / not claimable

| Limitation | Explanation |
|---|---|
| Not Byzantine fault tolerant | Raft is not a BFT protocol. A malicious or memory-corrupted node can break the cluster. |
| Liveness requires partial synchrony | FLP: no consensus protocol makes progress in a fully asynchronous network with failures. Safety holds regardless; liveness does not. |
| Losing a majority of a shard's replicas permanently loses that shard's data | No replication factor survives permanent loss of a quorum. |
| `fsync` honesty is assumed | Consumer SSDs with volatile write caches can acknowledge before durability. Undetectable from userspace. |
| Power-loss durability is untested | We test SIGKILL (process death). We cannot test power loss on a laptop, so `wal.sync=batch` is documented as surviving process kill only. |
| Corrupted SSTables are detected, not repaired | Repair = wipe the replica and re-sync from the leader. Manual in v1. |
| Dedup table is bounded | A retry arriving after its session is evicted degrades to at-least-once. The bound will be stated with a number once implemented. |
| Single-machine Docker demo is not a durability demo | All containers share one disk. It demonstrates topology, routing, election, and recovery — not independent hardware failure. |

## Scale limits (to be measured, not guessed)

These will be filled in with real numbers from Phases 5 and 19. Until then they are blank
rather than estimated:

- Maximum practical shard count per node: *unmeasured*
- Write throughput ceiling: *unmeasured*
- Read throughput ceiling: *unmeasured*
- Leader election time: *unmeasured*
- Maximum value size: 1 MiB (enforced limit, `docs/DESIGN.md` §1)
- Maximum key size: 4 KiB (enforced limit)

## Not production-ready

Stated plainly so it is never implied otherwise: this system has not run in production, has no
operational tooling, no backup/restore, no upgrade path, no security model, and no track record.
It is an engineering exercise built to be correct and explainable, not to be deployed.
