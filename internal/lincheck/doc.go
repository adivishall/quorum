// Package lincheck records client-visible operation histories and decides
// whether a finite history is linearizable (Phase 12, docs/LINEARIZABILITY.md,
// ADR-019).
//
// A history is a set of operations, each with an invocation position and — if it
// ever got a response — a completion position, drawn from one totally ordered
// event sequence (a logical counter, never a wall clock). A history is
// linearizable (Herlihy & Wing, 1990) iff there is a sequential order of its
// operations that (1) is legal for the sequential specification — here a
// single-key register with PUT/GET/DELETE, the exact semantics of the Phase 1
// storage contract — (2) produces the outputs the clients actually observed, and
// (3) respects real time: if A completed before B was invoked, A precedes B.
// Operations that overlap in time may be ordered either way.
//
// Linearizability is local (Herlihy & Wing's locality theorem): a history is
// linearizable iff its projection onto every key is, so the checker works one key
// at a time. Within a key it searches for a linearization with a depth-first
// walk over "which operation is linearized next", memoizing failed (linearized
// set, register state) pairs — the Wing–Gong / Lowe construction as used by
// Porcupine. The search is exponential in the worst case; the register's state
// space is tiny (absent, or one value), which is what makes the memoization
// effective, and every run has a state budget after which it reports the
// history as unchecked rather than pretending.
//
// Incomplete operations are treated by an explicit rule, never guessed at
// (docs/LINEARIZABILITY.md §4): an operation with a definite rejection
// (Outcome Rejected — not leader, proposal lost, invalid input, node unreachable
// before the request was accepted) had no effect and is excluded; an operation
// with no response (Outcome Incomplete — timeout, node crashed, ambiguous
// error) may have taken effect at any time after its invocation or never, so a
// write is optional in the linearization and a read constrains nothing.
//
// What the checker proves is exactly this: the finite history it was given is
// or is not linearizable. It proves nothing about histories it did not see.
// It is independent of the distributed implementation — it imports nothing from
// the Raft, node or KV packages — so it can also be tested against known good and
// known bad histories and against a brute-force oracle.
package lincheck
