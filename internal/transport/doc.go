// Package transport is Quorum's internal node-to-node networking (Phase 7,
// docs/TRANSPORT.md, ADR-013, ADR-014).
//
// It moves opaque messages between nodes over framed TCP. It carries bytes
// tagged with a message kind and delivers them tagged with the peer that sent
// them; it does not know Raft, shards, storage, or key/value semantics. Later
// phases build replication, consensus and request forwarding on top of it
// without changing this layer.
//
// # Wire format
//
// Every message is one record in the shared §2 framing (internal/record),
// little-endian, checksummed with CRC-32C:
//
//	crc32c(length‖kind‖payload)[4] · length[4] · kind[1] · payload[N]
//
// The 1-byte kind is the message type. Unlike the WAL, the reader never repairs
// a torn frame — a truncated socket frame, an oversized length, a bad checksum,
// or an unknown kind is a hard protocol error that closes the connection
// (docs/TRANSPORT.md §2). A connection opens with a one-way handshake —
// "DKV1"‖version‖len-prefixed nodeID — so a wrong service or version fails
// immediately, and the peer a message came from is the handshake identity of its
// connection, never a value the payload claims.
//
// # Connection model
//
// One bidirectional connection per peer pair. The lexicographically smaller node
// id dials; the larger accepts (ADR-014). Reconnect is the dialer's bounded
// fixed-interval retry. Concurrent sends on one connection are serialised so
// their bytes never interleave, and per-connection frame order is preserved.
//
// # Scope
//
// Probe/ProbeResponse (liveness) are implemented here (Phase 7). Since Phase 9
// the RequestVote and AppendEntries kinds (and their responses) carry Raft
// traffic for internal/raftnode; their codec lives in internal/raft, and this
// package moves those payloads as opaque bytes. InstallSnapshot and Forward
// remain reserved identifiers with no codec. The transport itself implements no
// replication, Raft, election, forwarding, or consistency; docs/TRANSPORT.md §11
// is the explicit boundary.
package transport
