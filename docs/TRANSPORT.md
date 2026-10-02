# TRANSPORT — internal node-to-node networking (Phase 7)

Status: **implemented and verified (Phase 7).** This document specifies the internal transport
precisely enough to reimplement or fuzz, and binds each guarantee to a test in
`internal/transport` and `tests/integration`. It is the reference for `internal/transport`,
`cmd/dkvd`, ADR-013, ADR-014, and invariants INV-T1..T7 (`docs/INVARIANTS.md`).

The transport moves opaque messages between nodes over framed TCP. It does **not** know Raft,
shards, storage, or key/value semantics — it carries bytes tagged with a message kind and
delivers them tagged with the peer that sent them. What Phase 7 proves, and deliberately does
not, is §11.

---

## 1. Layers

```
cmd/dkvd (a node process)
   │  Send(ctx, peer, kind, payload) / <-Receive()
   ▼
internal/transport.TCPTransport
   │  accept loop · per-peer dial loop · connection registry
   ▼
per connection:  handshake  ·  reader loop  ·  mutex-guarded writer
   ▼
framed TCP (record framing, §2 format, checksummed)
```

The transport is unauthenticated plaintext TCP (§10). It bounds every allocation and rejects
malformed input, but it makes **no** cryptographic identity claim — the handshake identifies a
peer at the protocol level only.

---

## 2. Framing

Every message is one record in the shared §2 framing (`docs/DESIGN.md` §2, `internal/record`),
little-endian:

```
offset  size  field
0       4     crc32c(length ‖ kind ‖ payload)   (CRC-32C / Castagnoli)
4       4     length   (payload byte count)
8       1     kind     (message type, §5)
9       N     payload  (message codec bytes, §6)
```

Frames are written with `record.Encode`, so the transport and the on-disk logs share one
checksum implementation. The write path writes the **whole** frame even when the underlying
writer accepts fewer bytes than requested per call (an `io.Writer`, `net.Conn` included, may do
a short write): it loops until the frame is fully written under the connection's writer lock,
propagates the first write error, and refuses to spin on a writer that makes no progress
(returning `io.ErrShortWrite`). A partial frame is never left on the wire. Reading is the
transport's **own strict reader** — it does not reuse the WAL's torn-tail policy (ADR-013):

| Reader situation | Result |
|---|---|
| clean EOF at a frame boundary (0 bytes of a new header) | connection closed normally |
| short read of the 9-byte header, or of the declared payload | `ErrTruncatedFrame` → close connection |
| `length > MaxFrameSize` (16 MiB) | `ErrFrameTooLarge` → close connection, **before** allocating |
| CRC mismatch | `ErrBadChecksum` → close connection |
| `kind` not a known message type | `ErrUnknownKind` → close connection |

**A network transport is not a WAL.** A truncated TCP frame is a failed message and a failed
connection, never a repairable log tail. There is no resynchronisation and no truncate-and-
continue; a parse failure closes the connection (matching `docs/FAILURE_MODEL.md` §2).

`MaxFrameSize = 16 MiB` bounds a hostile peer's per-frame allocation. The declared `length` is
range-checked against it **before** any buffer is sized, so a peer cannot induce a large
allocation with a lie. (Snapshot streaming, Phase 14, chunks rather than raises this: a snapshot
travels in chunks of at most 1 MiB — `docs/SNAPSHOTS.md` §9.)

## 3. Handshake

Exactly once per connection, before any frame, the **dialer sends its hello** and the
**accepter answers** with a status and its own hello (version 2; audit H2, H5):

```
dialer -> accepter                       accepter -> dialer
offset  size  field                      offset  size  field
0       4     magic "DKV1"               0       4     magic "DKV1"
4       4     version u32 LE (2)         4       4     version u32 LE (2)
                                         8       1     status
8       2     idLen u16 LE               9       2     idLen u16 LE
10      idLen node id                    11      idLen node id
.       2     clusterLen u16 LE          .       2     clusterLen u16 LE
.       len   cluster id                 .       len   cluster id
.       1     digestLen u8               .       1     digestLen u8
.       len   settings digest            .       len   settings digest
```

Each side announces **who it is** (its node id), **which cluster it belongs to** (the cluster id
its data directory records, `docs/MULTI_RAFT.md` §5) and **the digest of its replica settings**
(`cmd/dkvd`: the SHA-256 of the settings its data directory pins — the session limits, and in
`-cluster` mode the routing — which every replica of a group must share). A connection is used
only if both sides agree on both:

- A **node of another cluster** — a wrong `-peers` entry, a recycled address, a copied unit file —
  is never heard as a peer (audit H2). Before version 2 any process that announced a configured
  peer's id was that peer: its votes and acknowledgements counted as the real node's.
- **Nodes whose replica settings differ** never exchange a frame (audit H5): their replicas would
  decide the same entries differently and diverge.
- The **dialer checks who answered**: an accepter announcing another id than the peer dialed (a
  stale address now serving another node) is dropped (`ErrWrongPeer`), as is one of another
  cluster or other settings — the check is made on both sides, so neither relies on the other's.

Accepter statuses: `0` accepted, `1` unknown peer, `2` another cluster, `3` other settings, `4`
self-connection, `5` wrong direction (a known peer with the LARGER id dialed; the smaller id
dials, §4 — such a claim never comes from that peer, so it may not take its slot). Any status but
`0` closes the connection on both sides and is logged on both (`event=handshake_failed`); the
dialer retries at its interval, so a node whose cluster or settings are fixed connects again.
Cluster ids and digests are opaque here: empty values are equal only to empty values (the
in-process library tests set neither).

- **No checksum.** The magic gates a wrong service, the version gates a wrong protocol, and the
  bounded lengths gate a malformed preamble. Every subsequent frame is checksummed.
- Bounds: `MaxNodeIDLen = 256`, `MaxClusterIDLen = 256`, `MaxDigestLen = 64` bytes; every length
  is checked before anything is allocated. `idLen == 0` is rejected (`ErrEmptyNodeID`).
- **Handshake timeout:** each side's whole handshake must complete within `HandshakeTimeout`
  (default 5 s) or the connection is closed (`ErrHandshakeTimeout`): an accepter that never
  answers is dropped and redialled, never registered.
- **Bounded pending handshakes:** at most `MaxPendingHandshakes` (default 64) inbound
  connections may be in their handshake at once; beyond it a new one is closed at once
  (`dkv_transport_handshakes_refused_total{reason="busy"}`), so unauthenticated connections cannot
  each hold a descriptor and a goroutine for the timeout without bound.
- **Rejections** (all close the connection): wrong magic → `ErrBadMagic`; unknown version →
  `ErrVersionMismatch`; a length out of range → `ErrHandshakeTooLarge`; short read →
  `ErrTruncatedHandshake`; unknown reply status → `ErrMalformedHandshake`. A peer must not
  exchange application messages until its handshake succeeds (INV-T3).
- **Compatibility:** version 1 (a one-way hello with no cluster or settings) is refused
  (`ErrVersionMismatch`): every node of a cluster must run a version-2 binary; a rolling upgrade
  from a version-1 binary is not supported.

This is **identity and configuration agreement, not authentication** (§10): a process that
knows the cluster id and the settings can still claim any configured id.

## 4. Connection model (ADR-014)

For each peer pair the **lexicographically smaller node id dials**; the larger only accepts.
A pair therefore has one initiator and, normally, one TCP connection, used **bidirectionally**:
after the handshake both ends run a reader loop and share a mutex-guarded writer.

- **Registry, keep-existing.** At most one live connection per peer. If one already exists when
  a new one completes its handshake, the newcomer is closed. This absorbs the residual race
  (a peer restart that redials before the old socket's death is noticed).
- **Reconnect is the dialer's job.** The smaller-id side runs a per-peer dial loop that retries
  at a fixed bounded interval (`DialRetryInterval`, default 500 ms — no exponential backoff)
  until connected, and stops on transport shutdown. The larger-id side waits to be redialed.
  The interval applies after a *dead connection* exactly as after a failed dial (Phase 10):
  without it, a peer that accepts and instantly closes — a crash-looping process, or a
  partition that resets connections — turns the dial loop into a reconnect storm (measured at
  roughly one attempt per 250 µs, burning an ephemeral port and two log lines each).
  `TestDialerBacksOffWhenPeerKeepsClosingConnections` pins the pacing.
- **Concurrent sends** on one connection are serialised by a per-connection writer mutex, so two
  goroutines' frames never interleave their bytes. A blocked write is bounded by the write
  deadline and unblocked by shutdown.
- **Peer identity** on a received message is the handshake identity of the connection it arrived
  on — never a field in the payload (INV-T4, ADR-013).

## 5. Message kinds

The 1-byte frame `kind` is the message type. Phase 7 implemented the probe kinds; Phase 9 activated
the four Raft kinds; Phase 13 activated the forwarding kinds; Phase 14 activated the snapshot kinds:

| kind | name | status |
|---|---|---|
| 1 | `Probe` | implemented (§7) |
| 2 | `ProbeResponse` | implemented (§7) |
| 16 | `RequestVote` | implemented (Phase 9) — codec in `internal/raft`, carried by `internal/raftnode` |
| 17 | `RequestVoteResponse` | implemented (Phase 9) |
| 18 | `AppendEntries` | implemented (Phase 9) |
| 19 | `AppendEntriesResponse` | implemented (Phase 9) |
| 20 | `InstallSnapshot` | implemented (Phase 14) — a snapshot chunk; codec in `internal/snapshot` (`Chunk`), streamed and reassembled by `internal/raftnode` |
| 21 | `InstallSnapshotResponse` | implemented (Phase 14) — the core's `MsgSnapshotResponse`, codec in `internal/raft` |
| 32 | `Forward` | implemented (Phase 13) — codec in `internal/kv`, sent with `raftnode.SendApp` |
| 33 | `ForwardResponse` | implemented (Phase 13) |

The Raft kinds carry a hand-written bounded codec that lives in `internal/raft` (not here — the
transport stays ignorant of what a term means, ADR-013/ADR-016); `internal/raftnode` maps message
types to these kinds. The forwarding kinds carry a client request or response wrapped with a
forward id (`docs/API.md` §6); `raftnode` hands every non-Raft kind it receives to the
application's handler (`SetAppHandler`) and sends them for it (`SendApp`), so the transport and
the Raft driver stay ignorant of client semantics. The snapshot kinds (Phase 14) are not a request
and a response: kind 20 carries one chunk of the published snapshot file (`term, index, snapTerm,
total, offset, data`, strictly decoded); the core's `MsgSnapshot` is realized as a transfer of
chunks and a completed, validated transfer becomes a `MsgSnapshot` on the receiving side; kind 21 is
the core's `MsgSnapshotResponse`. A frame with a reserved-but-unimplemented kind is accepted
at the frame layer and ignored by a node that has no handler for it; an entirely unknown kind is
`ErrUnknownKind` at the frame layer.

## 6. Codec

Hand-written, deterministic, bounded, reflection-free (ADR-003). Primitives:

- fixed `uint64` — 8 bytes little-endian;
- `uvarint` lengths — `binary.Uvarint`, and on decode a length is checked against the bytes
  remaining in the frame *and* against a maximum before any slice is allocated;
- length-prefixed bytes — `uvarint(len) ‖ bytes`.

Every decoder is total on hostile input: it never trusts a declared length, never allocates on
an unchecked size, consumes exactly the frame payload (trailing bytes are `ErrTrailingBytes`),
and returns an error rather than panicking. Round-trip and malformed-input are covered by unit
tests and fuzz targets (§ tests below).

## 7. Probe / ProbeResponse

The Phase 7 application message, deliberately trivial — a transport/liveness probe, **not** a
Raft heartbeat.

```
Probe          = requestID uint64 ‖ token(len-prefixed bytes)
ProbeResponse  = requestID uint64 ‖ token(len-prefixed bytes)   // echoes the request's token
```

A node receiving a `Probe` replies with a `ProbeResponse` carrying the same `requestID` and
echoing the `token`, sent back on the same connection. A completed round trip proves the whole
stack: TCP connection, handshake, framing, checksum, decode, dispatch, response encode, and
routing back to the right peer. `token` (bounded, default probes send 8 random bytes) makes the
round trip prove payload integrity beyond the frame CRC. The responder's identity is the
connection, not the payload.

## 8. Timeouts and deadlines

All bounded; none is an arbitrary huge value. Defaults, all configurable:

| deadline | default | on expiry |
|---|---|---|
| dial timeout | 3 s | dial fails, dial loop retries |
| handshake timeout | 5 s | connection closed |
| write deadline (per frame) | 5 s | write fails → connection closed at once (audit M2): a frame cut off part-way has left part of itself on the wire, so nothing more may be written into that stream; its reader loop deregisters it and the dialer reconnects |
| read idle deadline | 0 in the library (disabled); `cmd/dkvd` sets ~120 ticks | a connection that delivers no frame for this long is treated as dead and torn down, so the dialer reconnects |

A caller's context bounds only the wait for a connection's writer: once a frame has started,
only the write deadline bounds it, so one group's cancelled request never cuts a frame short on
the connection every group of the node shares. `Send` refuses a payload over `MaxFrameSize` before
writing anything (`ErrFrameTooLarge`), since the peer would refuse it and drop the connection.
An `Accept` error (descriptors exhausted) is logged (`event=accept_failed`, counted in
`dkv_transport_accept_failures_total`) and retried after a pause growing to 1 s; before, the accept
loop returned for good while the listener stayed open, so peers connected into its backlog and
were silently black-holed.

Correctness does not depend on any of these firing (safety is not timing-dependent,
`docs/FAILURE_MODEL.md` §2); they exist so nothing blocks forever. Shutdown unblocks a blocked
read or write by closing the connection, independent of any deadline.

## 9. Shutdown and ordering

**Shutdown** (`Transport.Close`, or the process on SIGINT/SIGTERM) is deterministic and
idempotent: it cancels the transport context, stops the accept loop and every dial loop, closes
every connection (which unblocks blocked reads and writes), waits for all reader/writer/dial
goroutines to exit, and closes the receive channel. Calling `Close` twice is safe. The
in-process transport test asserts no goroutine leak across construct/exchange/close.

**Ordering guarantees**, stated exactly so later phases do not over-read them:

- **Per-connection frame order: preserved.** TCP delivers a connection's bytes in order and the
  reader parses frames sequentially, so frames on one connection arrive in send order (INV-T5).
- **Global order across peers: not guaranteed.** Two messages to two different peers, or across
  a reconnect, have no ordering relationship.
- **Delivery: not guaranteed.** The transport does not retry or buffer across a broken
  connection; a `Send` on a dead connection fails visibly. At-least-once/at-most-once and
  idempotence are application concerns for Phase 9+ (`docs/CONSISTENCY.md` C4).
- **Duplicates:** the transport does not itself duplicate a delivered frame, but a reconnect can
  cause an application to resend; dedup is a later phase's job.

## 10. Security boundary

The transport is **unauthenticated plaintext TCP**. No TLS, no authentication. The handshake
node id is a protocol-level label, not a cryptographic identity — a peer on the network could
present any id, and the cluster id and settings digest (§3) are not secrets: they stop a
misconfigured or misaddressed node of another cluster, never a malicious one. What is enforced
regardless: bounded frame size, bounded handshake, bounded node
id, bounded allocations, and rejection (never a panic) on any malformed input. Authentication
and transport encryption are out of scope for v1 (`docs/LIMITATIONS.md`).

## 11. What this transport proves — and does not

**Proves (Phase 7):** real OS-process nodes; a real internal TCP transport with a checksummed
framing, a version handshake, hand-written bounded codecs, one bidirectional long-lived connection
per peer pair, automatic dialer-side reconnect, concurrent-safe sends, per-connection frame
ordering, and deterministic clean shutdown — demonstrated end to end by three real `cmd/dkvd`
processes exchanging `Probe`/`ProbeResponse` over localhost TCP and all exiting cleanly.

**What now rides on it (Phase 9):** the transport carries real **Raft** traffic — `dkvd -raft` runs
elections, `RequestVote`/`AppendEntries`, and commit over it (`internal/raftnode`, `docs/RAFT.md`),
proven by a real 3-process election and a SIGKILL recovery test. The transport itself is unchanged
and still does not know what a term or a log index means; it only moves the bytes.

**Fault injection (Phase 10).** The transport is unchanged. Message-level faults are injected
*around* it: `fault.Network` decorates any `Transport` with partitions and drop/duplicate/hold/
block rules for in-process tests, and the real-process tests partition `dkvd` processes with
test-owned TCP proxies on the node-to-node links (`docs/FAULTS.md`). Since Phase 10 the Raft driver
sends from one goroutine per peer (bounded outboxes), so a peer whose writes block delays only its
own messages (INV-F5); per-connection frame order is unaffected.

**Built on top of the transport since:** Raft (Phase 9), client request forwarding (Phase 13,
kinds 32/33), snapshot installation (Phase 14, kinds 20/21), and many Raft groups per node with a
peer set that changes at runtime (Phase 15, below). **Still not built:** an HTTP API and a
dashboard.

**Phase 15.** Every payload a group sends travels inside a group envelope, `uvarint(group) ‖
payload`, added by the group's driver and removed by the node host; the transport carries it as
opaque bytes, frame kinds unchanged, one connection per peer pair for every group the two nodes
share (`docs/MULTI_RAFT.md` §4). `transport.PeerSet` (`AddPeer`, `RemovePeer`) lets the host keep
the peer set equal to the members of every hosted group's configuration plus the static peers:
adding a peer starts dialing it (the lower id dials, as before), removing one closes its
connection (`TestAddPeerConnectsAndRemovePeerDisconnects`). `Probe` is a liveness probe, not a Raft heartbeat.

## 12. Invariants

| ID | Statement | Tests |
|---|---|---|
| INV-T1 | Frame parsing is bounded and explicit: a declared length over `MaxFrameSize` is rejected before allocation, and a frame is read with `io.ReadFull`-discipline regardless of TCP fragmentation. | `TestFrameTooLargeIsRejectedBeforeAlloc`, `TestFrameReassembledFromFragments`, `TestConcatenatedFramesDecodeIndividually`, `FuzzFrameDecode` |
| INV-T2 | Malformed transport input is rejected as a protocol error and the connection is closed — never repaired, resynchronised, or interpreted as valid data (a socket is not a WAL). | `TestTruncatedFrameIsError`, `TestBadChecksumIsError`, `TestUnknownKindIsError`, `TestHandshakeBadMagicRejected`, `TestHandshakeVersionMismatchRejected`, `TestProbeMalformedRejected`, `FuzzFrameDecode`, `FuzzHandshakeDecode`, `FuzzProbeDecode` |
| INV-T3 | A successful handshake precedes any application message; a connection that fails the handshake exchanges no frames. Both sides must agree on the cluster id and the settings digest, and the dialer must have reached the peer it dialed. | `TestSelfConnectionRejected`, `TestUnknownPeerRejected`, `TestHandshakeTimeoutClosesConnection`, `TestBadMagicClosesConnection`, `TestHandshakeTruncatedRejected`, `TestNodesOfAnotherClusterNeverConnect`, `TestNodesWithOtherReplicaSettingsNeverConnect`, `TestDialerChecksTheAnswer`, `TestInboundFromTheWrongDirectionIsRefused`, `TestDialerDropsAPeerThatNeverAnswers`, and the real-process `TestRealImpostorsNeverJoinTheGroup` |
| INV-T4 | Each received message is attributed to the peer identity established by the handshake on its connection, never to a value carried in the payload. | `TestTCPHandshakeAndProbe`, `TestPayloadCannotSpoofSender` |
| INV-T5 | Frames on a single connection are delivered in send order, and a frame is written in full (even across short writes) so its bytes never interleave or truncate. | `TestPerConnectionOrderPreserved`, `TestConcurrentSendersDoNotInterleave`, `TestFrameSurvivesPartialWrites`, `TestWriteErrorAfterPartialWriteIsReturned`, `TestZeroProgressWriterDoesNotLoopForever` |
| INV-T6 | Node shutdown terminates all transport resources: accept loop, dial loops, reader and writer paths, and connections; repeated shutdown is safe; no goroutine leak. | `TestCloseIsIdempotent`, `TestNoGoroutineLeakAfterClose`, `TestSendAfterCloseFails`, and the three-process `TestThreeNodeClusterProbesAndShutsDownCleanly` |
| INV-T7 | A connection whose write failed or timed out is never written to again: it is closed at once and redialled. A frame the peer would refuse is never written. Accept errors never stop the accept loop, and connections in their handshake are bounded. | `TestFailedWriteClosesTheConnection`, `TestSendRefusesAFrameItsPeerWouldRefuse`, `TestAcceptLoopSurvivesAcceptErrors`, `TestPendingHandshakesAreBounded` |

Mutants 197–213 (`scripts/mutation.sh`) break each rule of INV-T3's checks and INV-T7 in turn —
each side's cluster, settings, peer and direction checks, closing after a failed write, the frame
size check, the accept retry, the handshake bound and its release, the handshake deadlines and
their interruption by shutdown, the per-read idle timeout, and `dkvd` passing its identity to the
transport — and each is killed by the test named for it.

INV-C4 (Phase 6) is **VERIFIED** since Phase 15, when routing was integrated into request serving
(`docs/INVARIANTS.md`, `docs/MULTI_RAFT.md` §6). Phase 7 added no Raft invariant; INV-R1..R10 were established in Phase 9 (`docs/RAFT.md`) and re-verified
under injected faults in Phase 10 (`docs/FAULTS.md`).
