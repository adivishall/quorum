# API — the client protocol (wire protocol v2, Phase 13)

Status: **Phase 13.** The protocol `dkvd -client-listen` serves and `internal/kv` implements
(`wire.go`, `api.go`, `server.go`, `session.go`). It is a framed binary protocol over TCP — not the
Phase 15 HTTP API, which does not exist yet. `docs/CLIENT_SEMANTICS.md` is the contract it carries
(what every field and status *means*); `docs/DEDUP.md` is how the server keeps it.

---

## 1. Connection

- A plain TCP connection to a node's client port. No handshake, no authentication, no TLS.
- Each message is **one record** in the shared checksummed framing (`docs/DESIGN.md` §2):
  `crc32c(length ‖ kind ‖ payload) u32 | length u32 | kind u8 | payload`, little-endian. A frame
  that fails its checksum, is truncated, exceeds its size bound or has the wrong kind is a
  **protocol error: the server closes the connection** (a socket is never repaired).
- Requests on one connection are answered **in order, one at a time**. A client wanting
  concurrency opens more connections. A client that times out must **abandon the connection**: a
  late response on it would otherwise be read as the answer to its next request
  (`kv.Client` does this; mutant 47).
- A connection carries no identity. Sessions (§3) are named in every request, so a client may use
  any number of connections, to any nodes, over the life of one session.

Record kinds: **3 = request, 4 = response.** Kinds 1 and 2 were version 1 (Phase 12: no identity,
six statuses); version 1 is retired and its kinds are refused like any unknown kind.

## 2. Messages

All integers are **canonical** unsigned LEB128 varints (a value written in more bytes than it needs
is a protocol error). Byte strings are a varint length followed by the bytes.

**Request** (kind 3):

| Field | Type | Meaning |
|---|---|---|
| op | u8 | 1 PUT · 2 GET · 3 DELETE · 4 REGISTER (anything else: protocol error) |
| clientID | varint | the session (0: anonymous) |
| requestID | varint | the request within the session (0 when anonymous) |
| ackedBelow | varint | the client's watermark (0 when anonymous) |
| timeoutMillis | varint | the client's budget for this attempt; 0 = the server default; more than fits a `time.Duration` (≈292 years): protocol error |
| key | bytes | ≤ 4 KiB (longer: protocol error) |
| value | bytes | **PUT only** — absent for every other op; ≤ 1 MiB |

**Response** (kind 4):

| Field | Type | Meaning |
|---|---|---|
| status | u8 | §5 (above 10: protocol error) |
| flags | u8 | bit 0: **duplicate** — answered from an earlier execution (other bits: protocol error) |
| clientID | varint | REGISTER with OK: the new session's id |
| term | varint | the term of the execution reported (a read: the serving leader's term) |
| index | varint | the log index of the execution reported — **for a duplicate, the original's**; a read: its read index |
| node | bytes | the node that served the request (≤ 255) |
| via | bytes | the node that forwarded it, if any (≤ 255) |
| leader | bytes | NOT_LEADER: the leader this node believes in, if any (≤ 255) |
| value | bytes | GET with OK: the value (may be empty — an empty value is present) |
| message | bytes | human-readable detail (≤ 1024), never needed to act |

Decoding is strict and total: any frame that is not exactly one of these is a protocol error, and
every decoder is fuzzed for totality (`FuzzDecodeRequestIsTotal`, `FuzzDecodeResponseIsTotal`,
`FuzzDecodeForwardIsTotal`, `FuzzDecodeIsTotal` for log commands).

## 3. Operations

| Op | Needs a session | Proposed to Raft | Answer |
|---|---|---|---|
| `REGISTER` | no (carries no key, value or ids) | yes | OK with `clientID` = the index of the REGISTER entry |
| `PUT key value` | optional — identified writes are deduplicated | yes | OK (possibly duplicate) or a refusal (§5) |
| `DELETE key` | optional — the same | yes | OK (deleting an absent key is OK) |
| `GET key` | no; ids are validated, then **ignored** (reads are never deduplicated) | no — ReadIndex (`docs/LINEARIZABILITY.md` §5) | OK with the value, or NOT_FOUND |

A write is answered only after its entry is committed **and applied** on the serving node in the
term it was proposed in (`docs/LINEARIZABILITY.md` §3); the answer is the state machine's decision
for that entry (`docs/DEDUP.md` §3).

## 4. Validation (INVALID_REQUEST)

A well-framed request whose fields break the contract is answered `INVALID_REQUEST` — nothing is
proposed. The rules (`Request.validate`; each pinned by
`TestRequestValidationRejectsEveryOutOfContractField`, mutant 87):

- op ∈ {PUT, GET, DELETE, REGISTER};
- REGISTER carries no key, value, clientID, requestID or ackedBelow;
- PUT/GET/DELETE: a non-empty key of at most 4 KiB; a value only on PUT, at most 1 MiB;
- anonymous (clientID 0): requestID = ackedBelow = 0;
- identified: requestID ≥ 1 and 1 ≤ ackedBelow ≤ requestID.

Anything that validates becomes a log command every replica can apply
(`TestValidatedRequestsAlwaysApply`) — no client can get an entry proposed that replicas refuse.
Identifiers are any 64-bit values; one that is not a canonical varint of at most 64 bits (an
overlong encoding, more than 64 bits, truncated) is a protocol error before validation
(`TestMalformedIdentifiersAreProtocolErrors`). There is no client-supplied hash to check — the
state machine fingerprints the command itself. What validation cannot know is decided at apply,
deterministically: a clientID that names no
session is `SESSION_EXPIRED`; a reused requestID is a duplicate or `REQUEST_CONFLICT`; one below the
session's watermark is `REQUEST_STALE` (CLIENT_SEMANTICS §4). The timeout is clamped to the server
maximum (10 s; 0 means that maximum).

## 5. Statuses

| Code | Status | Class |
|---|---|---|
| 0 | `OK` | definite, effect (now, or earlier when `duplicate`) |
| 1 | `NOT_FOUND` | definite (a read found nothing) |
| 2 | `NOT_LEADER` | definite, no effect; `leader` names a hint, if any |
| 3 | `UNAVAILABLE` | definite, no effect: nothing was sent onward |
| 4 | `INVALID_REQUEST` | definite, no effect |
| 5 | `REQUEST_CONFLICT` | definite, no effect: the requestID is taken by a different command |
| 6 | `REQUEST_STALE` | definite, no effect: below the session's watermark |
| 7 | `SESSION_EXPIRED` | definite, no effect **for this attempt**: the session is unknown or evicted |
| 8 | `SESSION_LIMIT` | definite, no effect: the session holds its maximum of unacknowledged results |
| 9 | `LOST` | definite, no effect **for this attempt**: its entry was overwritten |
| 10 | `UNKNOWN_OUTCOME` | unknown: it may or may not have taken effect |

What a client does with each is CLIENT_SEMANTICS §6. A transport failure before the request was
written is `UNAVAILABLE`; after it was written, `UNKNOWN_OUTCOME`.

## 6. Forwarding and redirection

A node that is not the leader **forwards** the request one hop to the leader it believes in, over
the internal transport, and relays the answer with `via` set to itself:

- transport kinds **32 `Forward`** — `forwardID varint | budgetMillis varint | request` — and
  **33 `ForwardResponse`** — `forwardID varint | response` — in the client encodings of §2
  (`docs/TRANSPORT.md` §5);
- a forwarded request is **never forwarded again**: a non-leader receiving one answers
  `NOT_LEADER` (no loops, whatever the nodes believe; mutant 70);
- a forward is **sent at most once**; its answer is waited for within the client's budget:
  - not sent (the leader is not connected) → `UNAVAILABLE`;
  - sent, not answered in time → `UNKNOWN_OUTCOME` (the leader may have executed it; mutant 71);
  - no leader known → `NOT_LEADER` with no hint;
- forward ids start at a random point per process, so a response to a previous incarnation's
  forward is never matched to a new one; a response nobody waits for is dropped;
- the request travels with its identity intact, so a forwarded retry is deduplicated like any
  other (mutant 72).

`dkvd -client-forwarding=false` is **redirect-only** mode: a non-leader answers `NOT_LEADER` naming
the leader, and the client goes there itself.

## 7. The client library (`internal/kv`)

`kv.Session` implements the contract's client half:

```go
s, err := kv.Register(ctx, endpoints, kv.SessionOptions{AttemptTimeout: 2*time.Second, MaxAttempts: 8, Backoff: 20*time.Millisecond})
out := s.Put(ctx, key, value, nil)   // one logical request; retried under its identity
// out.Err == nil: OK (out.Response.Duplicate tells whether this attempt found an earlier execution)
// out.Known == false: the request may have taken effect — report it as unknown
s2 := kv.ResumeSession(endpoints, opts, s.ID(), s.Next())   // after a client restart that persisted both
```

Per attempt: a `NOT_LEADER` hint is followed; `UNAVAILABLE` tries the next node after a back-off;
`UNKNOWN_OUTCOME`, `LOST` and a transport failure after sending are **retried with the same
requestID**; `SESSION_LIMIT` backs off and retries; `SESSION_EXPIRED`, `REQUEST_CONFLICT`,
`REQUEST_STALE` and `INVALID_REQUEST` stop. When the attempts run out after an unanswered one,
`Known` is false (mutant 74). A session is safe for concurrent use: each request's `ackedBelow` is
the lowest id still in flight (mutant 75). `kv.Client` is the one-connection transport; `Put/Get/
Delete` on `kv.Server` and `kv.Client` remain as the anonymous Phase 12 calls.

## 8. Compatibility

Version 2 replaced version 1 in place (Phase 13); nothing outside this repository spoke version 1.
There is no version negotiation: a version-1 frame (kinds 1, 2) is a protocol error. The Phase 15
HTTP API will be a separate listener.
