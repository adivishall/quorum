// Package kv is the minimal client-visible operation boundary Phase 12 needs to
// observe real PUT/GET/DELETE histories on a Raft group (docs/LINEARIZABILITY.md,
// ADR-019). It is deliberately not the Phase 13/15 client API: no HTTP, no
// request ids, no deduplication, no forwarding. What it holds:
//
//   - Command: the log-entry encoding of a write (PUT or DELETE). A GET never
//     enters the log; reads are served through ReadIndex (docs/DESIGN.md §8.5).
//   - Store: the deterministic in-memory key-value state machine behind the
//     raftnode seam, with exactly the Phase 1 storage semantics (an empty value
//     is a present key; DELETE is idempotent; keys are opaque bytes).
//   - Server: PUT/GET/DELETE on top of a raftnode.Node — a write completes only
//     when its entry has been committed and applied on the serving node in the
//     term it was proposed in; a read completes only after a ReadIndex confirmed
//     by a quorum and applied locally.
//   - A small framed-TCP request/response protocol and Client, so real dkvd
//     processes can be driven by test clients that record histories.
//
// The Store's semantics are pinned against internal/storage's MemStore and
// against internal/lincheck's reference model, so the three can never drift.
package kv
