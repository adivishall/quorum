package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// GroupID identifies one Raft group: one log, one snapshot, one state machine,
// one membership, one leader (Phase 15, docs/MEMBERSHIP.md §1). It is a distinct
// type from ShardID: in Phase 15 shard s is served by group GroupID(s), which the
// cluster configuration records explicitly and nothing assumes permanent.
type GroupID uint32

// EntryType is the kind of a log entry (Phase 15). Every entry before Phase 15
// was a state-machine command; a configuration entry carries the group's
// membership instead (docs/MEMBERSHIP.md §2) and is never handed to the state
// machine.
type EntryType uint8

const (
	// EntryNormal is a state-machine command (or the leader's empty no-op).
	EntryNormal EntryType = 0
	// EntryConfig carries an encoded Configuration: the group's membership from
	// this index on, whether or not the entry is committed yet (Raft §6).
	EntryConfig EntryType = 1
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryConfig:
		return "config"
	}
	return fmt.Sprintf("entrytype(%d)", t)
}

// Bounds of a configuration. A group has at most MaxMembers members in all its
// lists together; an id or an address is at most MaxMemberLen bytes.
const (
	MaxMembers   = 64
	MaxMemberLen = 256
	// MaxEncodedConfiguration bounds EncodeConfiguration's output for a valid
	// configuration, for a container that length-prefixes one: a version and
	// three counts, then at most 2*MaxMembers list entries (a member is listed
	// twice when it is a voter and an outgoing voter), each an id and an address
	// with their uvarint lengths.
	MaxEncodedConfiguration = 4*binaryMaxUvarint + 2*MaxMembers*(2*MaxMemberLen+2*binaryMaxUvarint)
	binaryMaxUvarint        = 10
)

// Member is one node of a group: its id and the address other members reach it
// at (empty in the simulator and in in-process tests, where the transport already
// knows every peer).
type Member struct {
	ID   NodeID
	Addr string
}

// Configuration is a group's membership as replicated state (docs/MEMBERSHIP.md
// §2). Voters are the current voting members — C_new while a joint transition is
// in progress; Outgoing is C_old, non-empty exactly while the configuration is
// joint; Learners are replicated to but never vote, never count, never campaign.
// Each list is sorted by id. The zero Configuration is the empty one: a node that
// joins a group starts with it and learns the membership from its leader.
type Configuration struct {
	Voters   []Member
	Outgoing []Member
	Learners []Member
}

// ErrInvalidConfiguration is a configuration no sequence of membership changes
// produces: a duplicate or empty id, a learner that is also a voter, an id or
// address over the bound, too many members, or a malformed encoding.
var ErrInvalidConfiguration = errors.New("replication: invalid configuration")

// Joint reports whether the configuration is a joint one (C_old,new).
func (c Configuration) Joint() bool { return len(c.Outgoing) > 0 }

// Empty reports whether the configuration has no member at all (a joiner's).
func (c Configuration) Empty() bool {
	return len(c.Voters) == 0 && len(c.Outgoing) == 0 && len(c.Learners) == 0
}

// IsVoter reports whether id votes in the current or, in a joint configuration,
// the outgoing voter set.
func (c Configuration) IsVoter(id NodeID) bool {
	return containsMember(c.Voters, id) || containsMember(c.Outgoing, id)
}

// IsLearner reports whether id is a learner.
func (c Configuration) IsLearner(id NodeID) bool { return containsMember(c.Learners, id) }

// IsMember reports whether id is a voter (of either set) or a learner: a node the
// group replicates to and accepts responses from.
func (c Configuration) IsMember(id NodeID) bool { return c.IsVoter(id) || c.IsLearner(id) }

// Members returns every member id once, sorted: voters of both sets and learners.
func (c Configuration) Members() []NodeID {
	seen := map[NodeID]bool{}
	var out []NodeID
	for _, list := range [][]Member{c.Voters, c.Outgoing, c.Learners} {
		for _, m := range list {
			if !seen[m.ID] {
				seen[m.ID] = true
				out = append(out, m.ID)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Addr returns the address recorded for id in any list, if any.
func (c Configuration) Addr(id NodeID) (string, bool) {
	for _, list := range [][]Member{c.Voters, c.Outgoing, c.Learners} {
		for _, m := range list {
			if m.ID == id {
				return m.Addr, true
			}
		}
	}
	return "", false
}

// VoterIDs and OutgoingIDs are the ids of the two voter sets, sorted.
func (c Configuration) VoterIDs() []NodeID    { return memberIDs(c.Voters) }
func (c Configuration) OutgoingIDs() []NodeID { return memberIDs(c.Outgoing) }

// Equal reports whether two configurations have the same members in the same
// roles with the same addresses.
func (c Configuration) Equal(o Configuration) bool {
	return sameMembers(c.Voters, o.Voters) && sameMembers(c.Outgoing, o.Outgoing) && sameMembers(c.Learners, o.Learners)
}

// Clone returns a deep copy.
func (c Configuration) Clone() Configuration {
	return Configuration{Voters: cloneMembers(c.Voters), Outgoing: cloneMembers(c.Outgoing), Learners: cloneMembers(c.Learners)}
}

// String renders a configuration for logs and traces: "voters=[a b] outgoing=[a b c] learners=[d]".
func (c Configuration) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "voters=%s", idsString(c.Voters))
	if c.Joint() {
		fmt.Fprintf(&b, " outgoing=%s", idsString(c.Outgoing))
	}
	if len(c.Learners) > 0 {
		fmt.Fprintf(&b, " learners=%s", idsString(c.Learners))
	}
	return b.String()
}

// Validate checks the structural rules: every list sorted by id with no duplicate
// or empty id, no id in both Voters (or Outgoing) and Learners, bounded ids,
// addresses and member count. It never repairs.
func (c Configuration) Validate() error {
	for name, list := range map[string][]Member{"voters": c.Voters, "outgoing": c.Outgoing, "learners": c.Learners} {
		for i, m := range list {
			if m.ID == "" {
				return fmt.Errorf("%w: empty id among the %s", ErrInvalidConfiguration, name)
			}
			if len(m.ID) > MaxMemberLen || len(m.Addr) > MaxMemberLen {
				return fmt.Errorf("%w: %s %q: id or address over %d bytes", ErrInvalidConfiguration, name, m.ID, MaxMemberLen)
			}
			if i > 0 && list[i-1].ID >= m.ID {
				return fmt.Errorf("%w: %s not strictly ascending at %q", ErrInvalidConfiguration, name, m.ID)
			}
		}
	}
	for _, m := range c.Learners {
		if containsMember(c.Voters, m.ID) || containsMember(c.Outgoing, m.ID) {
			return fmt.Errorf("%w: %q is both a learner and a voter", ErrInvalidConfiguration, m.ID)
		}
	}
	if n := len(c.Members()); n > MaxMembers {
		return fmt.Errorf("%w: %d members, at most %d", ErrInvalidConfiguration, n, MaxMembers)
	}
	return nil
}

// configVersion is the encoding version of a configuration entry.
const configVersion = 1

// EncodeConfiguration renders a configuration canonically (docs/MEMBERSHIP.md
// §2): version, then Voters, Outgoing and Learners each as a count followed by
// (id, addr) pairs — canonical uvarints, length-prefixed bytes. The same
// configuration always encodes to the same bytes; it assumes a valid one.
func EncodeConfiguration(c Configuration) []byte {
	b := binary.AppendUvarint(nil, configVersion)
	for _, list := range [][]Member{c.Voters, c.Outgoing, c.Learners} {
		b = binary.AppendUvarint(b, uint64(len(list)))
		for _, m := range list {
			b = binary.AppendUvarint(b, uint64(len(m.ID)))
			b = append(b, m.ID...)
			b = binary.AppendUvarint(b, uint64(len(m.Addr)))
			b = append(b, m.Addr...)
		}
	}
	return b
}

// DecodeConfiguration parses an encoded configuration strictly: canonical
// integers, bounded lengths, no trailing bytes, and the structural rules of
// Validate. It accepts exactly what EncodeConfiguration produces from a valid
// configuration.
func DecodeConfiguration(b []byte) (Configuration, error) {
	d := confDecoder{b: b}
	if v := d.uint(); d.err == nil && v != configVersion {
		return Configuration{}, fmt.Errorf("%w: encoding version %d (this build reads %d)", ErrInvalidConfiguration, v, configVersion)
	}
	var c Configuration
	for _, list := range []*[]Member{&c.Voters, &c.Outgoing, &c.Learners} {
		n := d.uint()
		if d.err == nil && n > MaxMembers {
			return Configuration{}, fmt.Errorf("%w: %d members in one list", ErrInvalidConfiguration, n)
		}
		for i := uint64(0); i < n && d.err == nil; i++ {
			id := d.bytes(MaxMemberLen)
			addr := d.bytes(MaxMemberLen)
			if d.err == nil {
				*list = append(*list, Member{ID: NodeID(id), Addr: string(addr)})
			}
		}
	}
	if d.err != nil {
		return Configuration{}, d.err
	}
	if d.i != len(b) {
		return Configuration{}, fmt.Errorf("%w: %d trailing bytes", ErrInvalidConfiguration, len(b)-d.i)
	}
	if err := c.Validate(); err != nil {
		return Configuration{}, err
	}
	return c, nil
}

// NewConfiguration builds a validated stable configuration from voter and
// learner members (any order; sorted here). Duplicates are refused, not merged.
func NewConfiguration(voters, learners []Member) (Configuration, error) {
	c := Configuration{Voters: sortMembers(voters), Learners: sortMembers(learners)}
	if err := c.Validate(); err != nil {
		return Configuration{}, err
	}
	return c, nil
}

// VotersOf is the stable configuration whose voters are ids, with no addresses —
// what a fixed peer list (Phase 9's Config.Peers) means.
func VotersOf(ids []NodeID) Configuration {
	ms := make([]Member, 0, len(ids))
	for _, id := range ids {
		ms = append(ms, Member{ID: id})
	}
	return Configuration{Voters: sortMembers(ms)}
}

// --- helpers ---

func containsMember(list []Member, id NodeID) bool {
	for _, m := range list {
		if m.ID == id {
			return true
		}
	}
	return false
}

func memberIDs(list []Member) []NodeID {
	out := make([]NodeID, 0, len(list))
	for _, m := range list {
		out = append(out, m.ID)
	}
	return out
}

func sameMembers(a, b []Member) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func cloneMembers(list []Member) []Member {
	if list == nil {
		return nil
	}
	return append([]Member(nil), list...)
}

func sortMembers(list []Member) []Member {
	out := cloneMembers(list)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func idsString(list []Member) string {
	ss := make([]string, len(list))
	for i, m := range list {
		ss[i] = string(m.ID)
	}
	return "[" + strings.Join(ss, " ") + "]"
}

// confDecoder reads canonical uvarints and bounded byte strings, remembering the
// first error (the pattern of internal/kv and internal/snapshot).
type confDecoder struct {
	b   []byte
	i   int
	err error
}

func (d *confDecoder) uint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b[d.i:])
	if n <= 0 || n != uvarintLen(v) {
		d.err = fmt.Errorf("%w: bad or non-canonical integer", ErrInvalidConfiguration)
		return 0
	}
	d.i += n
	return v
}

func (d *confDecoder) bytes(max int) []byte {
	n := d.uint()
	if d.err != nil {
		return nil
	}
	if n > uint64(max) || n > uint64(len(d.b)-d.i) {
		d.err = fmt.Errorf("%w: byte string of %d", ErrInvalidConfiguration, n)
		return nil
	}
	out := append([]byte(nil), d.b[d.i:d.i+int(n)]...)
	d.i += int(n)
	return out
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
