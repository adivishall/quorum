package raft

import (
	"math/rand"
	"sort"

	"github.com/adivishall/quorum/internal/replication"
)

// Default tick counts (docs/DESIGN.md §8.3). One tick is 50 ms in the driver, but
// the core only counts ticks and never knows the wall-clock duration.
const (
	DefaultElectionTicks  = 10
	DefaultHeartbeatTicks = 2
)

// Default bounds on a leader's outstanding work (audit M3). A leader that
// cannot reach a quorum — partitioned, its followers down — never commits, and
// without CheckQuorum it does not step down; every request it accepts until
// then stays: an uncommitted, persisted entry resent on every broadcast and a
// waiting client, or a read awaiting confirmation. These bound them; beyond a
// bound the leader refuses (ErrBusy, definite) instead of accepting work it
// can only hold. Healthy operation stays far below them: the uncommitted tail
// is about the writes in flight, and a read is confirmed within a heartbeat.
const (
	DefaultMaxUncommittedEntries = 1024
	DefaultMaxUncommittedBytes   = 64 << 20
	DefaultMaxPendingReads       = 1024
)

// Default budgets of one AppendEntries (audit H4): a leader sends a follower's
// backlog in messages of at most this many entries and bytes of entry data —
// one entry more than the byte budget when that entry alone exceeds it — so a
// message always fits what its receiver accepts (MaxEntriesPerMessage entries,
// the transport's frame), however far behind the follower is.
const (
	DefaultMaxEntriesPerMsg = 4096
	DefaultMaxSizePerMsg    = 1 << 20
	// MaxSizePerMsgLimit bounds MaxSizePerMsg: with one entry of
	// MaxEntryDataLen beyond it, a message stays under 10 MiB of entry data.
	MaxSizePerMsgLimit = 8 << 20
)

// Config constructs a Raft core. The membership it starts from is Conf — the
// configuration at log index ConfIndex (a snapshot's, at the snapshot's index;
// or the group's genesis, at 0) — or, when Conf is nil, the fixed voter set
// Peers (Phase 9's form: every node a voter, no addresses). The latest
// configuration entry in the log overrides either (Raft §6,
// docs/MEMBERSHIP.md §2).
type Config struct {
	// ID is this node's id. With Peers it must appear in Peers; with Conf it need
	// not appear at all — a joiner has an empty configuration, a removed node one
	// without itself — and then never campaigns or votes.
	ID NodeID
	// Peers is the bootstrap voter set, including ID, when Conf is nil. Order
	// does not matter — New sorts it — but it must be non-empty with no empty or
	// duplicate id.
	Peers []NodeID
	// Conf, if non-nil, is the base configuration (Phase 15). It may be empty: a
	// joiner that knows nothing of its group's configuration yet.
	Conf *replication.Configuration
	// ConfIndex is the log index at which Conf holds: the published snapshot's
	// index on recovery (the log may keep entries below it, Phase 14's retain),
	// 0 for the genesis. It must not exceed the log's last index; below the
	// log's boundary it is taken as the boundary. A configuration entry at or
	// below it in the log must agree with Conf (ErrConfMismatch otherwise).
	ConfIndex uint64

	// ElectionTicks and HeartbeatTicks default to 10 and 2. ElectionTicks must be
	// strictly greater than HeartbeatTicks.
	ElectionTicks  int
	HeartbeatTicks int
	// SnapshotRetryTicks is how long a leader waits, in ticks, for a follower to
	// answer a snapshot before offering it again (Phase 14, docs/SNAPSHOTS.md
	// §8). Zero means 4 × ElectionTicks.
	SnapshotRetryTicks int

	// Rand supplies election-timeout jitter; it is injected so elections are
	// deterministic under a seed (ADR-002). Required.
	Rand *rand.Rand

	// Log is the replicated log the core drives (Phase 8). Usually a fresh
	// replication.NewMemoryLog(); on restart, one preloaded with recovered
	// entries and its commit index set. Required.
	Log replication.Log

	// Term and Vote are the recovered durable HardState (zero for a fresh node).
	Term uint64
	Vote NodeID

	// MaxUncommittedEntries and MaxUncommittedBytes bound a leader's
	// uncommitted log tail, its election no-op included; at either, Propose
	// refuses with ErrBusy. A proposal is admitted whenever the tail holds no
	// data, so no entry within MaxEntryDataLen is refused forever.
	// MaxPendingReads bounds the reads awaiting confirmation; at it, ReadIndex
	// refuses with ErrBusy. Zero means the default; negative is invalid.
	MaxUncommittedEntries int
	MaxUncommittedBytes   int
	MaxPendingReads       int

	// MaxEntriesPerMsg and MaxSizePerMsg are the budgets of one AppendEntries
	// (at most MaxEntriesPerMessage and MaxSizePerMsgLimit). Zero means the
	// default; negative or over the limit is invalid.
	MaxEntriesPerMsg int
	MaxSizePerMsg    int
}

func (c *Config) withDefaults() {
	if c.ElectionTicks == 0 {
		c.ElectionTicks = DefaultElectionTicks
	}
	if c.SnapshotRetryTicks == 0 {
		c.SnapshotRetryTicks = 4 * c.ElectionTicks
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = DefaultHeartbeatTicks
	}
	if c.MaxUncommittedEntries == 0 {
		c.MaxUncommittedEntries = DefaultMaxUncommittedEntries
	}
	if c.MaxUncommittedBytes == 0 {
		c.MaxUncommittedBytes = DefaultMaxUncommittedBytes
	}
	if c.MaxPendingReads == 0 {
		c.MaxPendingReads = DefaultMaxPendingReads
	}
	if c.MaxEntriesPerMsg == 0 {
		c.MaxEntriesPerMsg = DefaultMaxEntriesPerMsg
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = DefaultMaxSizePerMsg
	}
}

// validate checks the config and returns the base configuration: Conf,
// validated, or the voters Peers names, sorted (deterministic iteration) and
// dedup-verified. It never repairs: a duplicate or empty id is an error.
func (c *Config) validate() (replication.Configuration, error) {
	var none replication.Configuration
	if c.ID == "" {
		return none, ErrNoID
	}
	if len(c.ID) > replication.MaxMemberLen {
		return none, ErrNoID
	}
	if c.Rand == nil {
		return none, ErrNoRand
	}
	if c.Log == nil {
		return none, ErrNoLog
	}
	if c.ElectionTicks <= 0 || c.HeartbeatTicks <= 0 || c.ElectionTicks <= c.HeartbeatTicks {
		return none, ErrInvalidTicks
	}
	if c.MaxUncommittedEntries < 0 || c.MaxUncommittedBytes < 0 || c.MaxPendingReads < 0 ||
		c.MaxEntriesPerMsg < 0 || c.MaxEntriesPerMsg > MaxEntriesPerMessage ||
		c.MaxSizePerMsg < 0 || c.MaxSizePerMsg > MaxSizePerMsgLimit {
		return none, ErrInvalidBounds
	}
	var base replication.Configuration
	if c.Conf != nil {
		if err := c.Conf.Validate(); err != nil {
			return none, err
		}
		base = c.Conf.Clone()
	} else {
		if len(c.Peers) == 0 {
			return none, ErrIDNotInPeers
		}
		peers := make([]NodeID, len(c.Peers))
		copy(peers, c.Peers)
		sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
		seenSelf := false
		for i, p := range peers {
			if p == "" {
				return none, ErrEmptyPeer
			}
			if i > 0 && peers[i] == peers[i-1] {
				return none, ErrDuplicatePeer
			}
			if p == c.ID {
				seenSelf = true
			}
		}
		if !seenSelf {
			return none, ErrIDNotInPeers
		}
		base = replication.VotersOf(peers)
		if err := base.Validate(); err != nil {
			return none, err
		}
	}
	// currentTerm can never be below a term already in the log.
	if last := c.Log.LastIndex(); last > 0 {
		lt, err := c.Log.Term(last)
		if err != nil {
			return none, err
		}
		if c.Term < lt {
			return none, ErrTermRegression
		}
	}
	return base, nil
}
