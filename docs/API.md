# API — the client protocol (wire protocol v3, Phase 15)

Status: **Phase 15.** The protocol `dkvd -client-listen` serves and `internal/kv` implements
(`wire.go`, `api.go`, `front.go`, `server.go`, `session.go`, `sharded.go`). It is a framed binary
protocol over TCP; there is no HTTP API. `docs/CLIENT_SEMANTICS.md` is the contract it carries (what
every field and status *means*); `docs/DEDUP.md` is how the server keeps it; `docs/MULTI_RAFT.md` §6
is how a request finds its group. Version 3 is version 2 (Phase 13) plus the request's **group**.

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
- **What a connection may cost is bounded** (`kv.ServeConfig`, audit M1): a node serves at most
  1024 client connections at once — one beyond is closed as soon as it is accepted; a request
  frame must arrive whole within 10 s of its first byte; a response must be taken within 10 s.
  Either deadline missed closes the connection. An **idle** connection is never timed out: closing
  one could race a request its client is sending, which the client would then have to report as
  an unknown outcome. So 1024 idle connections hold every slot, and new clients are refused until
  one closes: the cap bounds the cost of connections, not who holds them (there is no
  authentication to tell clients apart, `docs/LIMITATIONS.md`). A frame's buffer grows with the bytes that arrive, never to the length a
  header merely declares. Tests: `TestServeCapsItsConnections`,
  `TestServeDropsAStalledFrameButKeepsAnIdleConnection`, `TestServeDropsAClientThatDoesNotRead`,
  `TestServeSurvivesAcceptErrors`, `TestReadFrameGrowsWithTheBytesThatArrive`.

Record kinds: **5 = request, 4 = response.** Kinds 1 and 2 were version 1 (Phase 12: no identity,
six statuses) and kind 3 was version 2's request (Phase 13: no group); both versions are retired and
their request kinds are refused like any unknown kind — the connection is closed unanswered and
nothing reaches a server (`TestRetiredRequestKindsAreRefused`). The response is version 2's,
unchanged.

## 2. Messages

All integers are **canonical** unsigned LEB128 varints (a value written in more bytes than it needs
is a protocol error). Byte strings are a varint length followed by the bytes.

**Request** (kind 5):

| Field | Type | Meaning |
|---|---|---|
| op | u8 | 1 PUT · 2 GET · 3 DELETE · 4 REGISTER (anything else: protocol error) |
| group | varint | the Raft group the request is for — for a keyed request, the key's (§4); above 32 bits: protocol error (`TestRequestGroupRoundTrips`) |
| clientID | varint | the session — **of that group** (0: anonymous) |
| requestID | varint | the request within the session (0 when anonymous) |
| ackedBelow | varint | the client's watermark (0 when anonymous) |
| timeoutMillis | varint | the client's budget for this attempt; 0 = the server default; more than fits a `time.Duration` (≈292 years): protocol error |
| key | bytes | ≤ 4 KiB (longer: protocol error) |
| value | bytes | **PUT only** — absent for every other op; ≤ 1 MiB, and the write's whole entry ≤ 1 MiB (§3) |

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
| `REGISTER` | no (carries no key, value or ids; names its group) | yes, to that group | OK with `clientID` = the index of the REGISTER entry in that group's log |
| `PUT key value` | optional — identified writes are deduplicated | yes | OK (possibly duplicate) or a refusal (§5) |
| `DELETE key` | optional — the same | yes | OK (deleting an absent key is OK) |
| `GET key` | no; ids are validated, then **ignored** (reads are never deduplicated) | no — ReadIndex (`docs/LINEARIZABILITY.md` §5) | OK with the value, or NOT_FOUND |

A write is answered only after its entry is committed **and applied** on the serving node in the
term it was proposed in (`docs/LINEARIZABILITY.md` §3); the answer is the state machine's decision
for that entry (`docs/DEDUP.md` §3).

Every operation is for one key — hence one shard, hence one group — or registers a session in one
group. There is no multi-key or cross-group operation; a session exists in exactly one group, and
the same `clientID` in two groups names two unrelated sessions (CLIENT_SEMANTICS §3).

## 4. Validation (INVALID_REQUEST)

A well-framed request whose fields break the contract is answered `INVALID_REQUEST` — nothing is
proposed. The rules (`Request.validate`; each pinned by
`TestRequestValidationRejectsEveryOutOfContractField`, mutant 87):

- op ∈ {PUT, GET, DELETE, REGISTER};
- PUT/GET/DELETE: the group the request names is the group of its key under the cluster's routing
  (`kv.Front`; a client whose routing differs from the cluster's gets `INVALID_REQUEST` naming both,
  and nothing executes anywhere — `TestFrontRefusesAMisroutedRequest`, mutant 156);
- REGISTER carries no key, value, clientID, requestID or ackedBelow;
- PUT/GET/DELETE: a non-empty key of at most 4 KiB; a value only on PUT, at most 1 MiB;
- anonymous (clientID 0): requestID = ackedBelow = 0;
- identified: requestID ≥ 1 and 1 ≤ ackedBelow ≤ requestID;
- PUT/DELETE: the **log entry the write becomes** — its encoded command — is at most
  `kv.MaxCommandLen` = 1 MiB (1,048,576 bytes), the system's one entry-size limit
  (`raft.MaxEntryDataLen`, `docs/RAFT.md` §16). The encoding adds to the key and value an op
  byte, the uvarint lengths of key and value and, for an identified write, its three identity
  varints (1–10 bytes each), so **the largest value depends on the key and identity**: an
  anonymous PUT of a 1-byte key carries up to 1,048,570 bytes of value, the longest key with the
  largest identities leaves room for 1,044,444. A value of the full 1 MiB is therefore always
  `INVALID_REQUEST`. The refusal is definite and comes before anything is proposed or forwarded
  (`TestEncodedEntryLimitDecidesWriteAdmission`, `TestEntryLimitEndToEnd`, `TestRealEntryLimit`;
  mutants 173–179). Until this rule (audit C1), such a write was proposed, persisted by the
  leader as an entry every follower refused, and left that leader unable to restart.

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
| 2 | `NOT_LEADER` | definite, no effect; `leader` names a hint, if any — none when this node hosts no replica of the request's group |
| 3 | `UNAVAILABLE` | definite, no effect: nothing was sent onward, or the leader refused it at its bound of uncommitted entries or pending reads (it is probably cut off from its quorum; `docs/RAFT.md` §17) |
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
  (`docs/TRANSPORT.md` §5), inside the group envelope of the request's group, so a forward goes
  only to that group's leader and is served only by that group (`docs/MULTI_RAFT.md` §4);
- a forwarded request is **never forwarded again**: a non-leader receiving one answers
  `NOT_LEADER` (no loops, whatever the nodes believe; mutant 70);
- a forward is **sent at most once**; its answer is waited for within the client's budget:
  - not sent (the leader is not connected) → `UNAVAILABLE`;
  - refused by the leader, which serves at most 256 forwards at once (audit M1) → `UNAVAILABLE`,
    answered at once (`TestForwardsBeyondTheBoundAreRefusedUnavailable`);
  - sent, not answered in time → `UNKNOWN_OUTCOME` (the leader may have executed it; mutant 71);
  - no leader known → `NOT_LEADER` with no hint;
- forward ids start at a random point per process, so a response to a previous incarnation's
  forward is never matched to a new one; a response nobody waits for is dropped;
- the request travels with its identity intact, so a forwarded retry is deduplicated like any
  other (mutant 72).

A node that hosts no replica of the request's group answers `NOT_LEADER` with no hint: it knows
no member of that group to forward to, and nothing was proposed.

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

c := kv.NewSharded(endpoints, opts, route)   // a multi-group cluster: route is the cluster's key → group
out = c.Put(ctx, key, value, nil)            // registers the key's group's session on first use
```

`SessionOptions.Group` is the group a `Session` lives in (0, the single-group deployment's, by
default); every request it sends names that group. `kv.Sharded` holds one `Session` per group and
sends each keyed request through the session of the key's group
(`TestShardedClientRoutesEachKeyToItsGroup`).

Per attempt: a `NOT_LEADER` hint is followed; `UNAVAILABLE` tries the next node after a back-off
(`Backoff`, doubling for each consecutive refusal that names no usable leader, capped at 1 s, reset
when a leader is named — so a count of attempts spans an election under load);
`UNKNOWN_OUTCOME`, `LOST` and a transport failure after sending are **retried with the same
requestID**; `SESSION_LIMIT` backs off and retries; `SESSION_EXPIRED`, `REQUEST_CONFLICT`,
`REQUEST_STALE` and `INVALID_REQUEST` stop. When the attempts run out after an unanswered one,
`Known` is false (mutant 74). A session is safe for concurrent use: each request's `ackedBelow` is
the lowest id still in flight (mutant 75). `kv.Client` is the one-connection transport; `Put/Get/
Delete` on `kv.Server` and `kv.Client` remain as the anonymous Phase 12 calls.

## 8. Compatibility

Version 2 replaced version 1 in place (Phase 13), and version 3 replaced version 2 (Phase 15);
nothing outside this repository spoke either. There is no version negotiation: a version-1 or
version-2 request (kinds 1, 3) is a protocol error. The single-group deployment (`dkvd -raft`) is
group 0, so a version-3 client of it names group 0. There is no HTTP API; if one is built it will be
a separate listener.
