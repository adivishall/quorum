# LIMITATIONS

The things this system does not do, cannot do, or has not proven. Kept current: an item may be
removed only when a test exists showing it is no longer true.

**Status: Phase 0.** No code exists yet, so at this moment the honest summary is: *everything*
is a limitation. The list below is the set of limitations that will still be true when v1 is
complete.

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
