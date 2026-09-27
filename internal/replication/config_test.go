package replication

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func mem(ids ...string) []Member {
	out := make([]Member, 0, len(ids))
	for _, id := range ids {
		out = append(out, Member{ID: NodeID(id), Addr: "127.0.0.1:" + id})
	}
	return out
}

// TestConfigurationRoundTripIsCanonical: every shape of configuration — empty,
// single voter, stable with learners, joint — encodes to the same bytes every
// time, decodes back equal, and the decoded value re-encodes byte-identically.
func TestConfigurationRoundTripIsCanonical(t *testing.T) {
	joint := Configuration{Voters: mem("a", "b", "d"), Outgoing: mem("a", "b", "c"), Learners: mem("e")}
	for name, c := range map[string]Configuration{
		"empty":     {},
		"solo":      VotersOf([]NodeID{"a"}),
		"stable":    {Voters: mem("a", "b", "c"), Learners: mem("d")},
		"joint":     joint,
		"no-addrs":  VotersOf([]NodeID{"c", "a", "b"}),
		"long-addr": {Voters: []Member{{ID: "a", Addr: strings.Repeat("x", MaxMemberLen)}}},
	} {
		if err := c.Validate(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		b1, b2 := EncodeConfiguration(c), EncodeConfiguration(c)
		if !bytes.Equal(b1, b2) {
			t.Fatalf("%s: two encodings differ", name)
		}
		got, err := DecodeConfiguration(b1)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !got.Equal(c) {
			t.Fatalf("%s: decoded %s, want %s", name, got, c)
		}
		if !bytes.Equal(EncodeConfiguration(got), b1) {
			t.Fatalf("%s: re-encoding differs", name)
		}
	}
	if got := VotersOf([]NodeID{"c", "a", "b"}).VoterIDs(); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("VotersOf did not sort: %v", got)
	}
}

// TestConfigurationValidationRefuses: every structural rule, each refused and
// never repaired.
func TestConfigurationValidationRefuses(t *testing.T) {
	for name, c := range map[string]Configuration{
		"empty id":                    {Voters: []Member{{ID: ""}}},
		"unsorted voters":             {Voters: mem("b", "a")},
		"duplicate voter":             {Voters: mem("a", "a")},
		"unsorted learners":           {Voters: mem("a"), Learners: mem("c", "b")},
		"learner is a voter":          {Voters: mem("a", "b"), Learners: mem("b")},
		"learner is an outgoing":      {Voters: mem("a"), Outgoing: mem("a", "b"), Learners: mem("b")},
		"id too long":                 {Voters: []Member{{ID: NodeID(strings.Repeat("x", MaxMemberLen+1))}}},
		"addr too long":               {Voters: []Member{{ID: "a", Addr: strings.Repeat("x", MaxMemberLen+1)}}},
		"too many members":            {Voters: manyMembers(MaxMembers + 1)},
		"too many across the lists":   {Voters: manyMembers(MaxMembers), Learners: []Member{{ID: "zz"}}},
		"duplicate outgoing":          {Voters: mem("a"), Outgoing: mem("a", "a", "b")},
		"learner duplicate":           {Voters: mem("a"), Learners: mem("b", "b")},
		"empty id among the learners": {Voters: mem("a"), Learners: []Member{{ID: ""}}},
	} {
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: %v", name, err)
		}
		if _, err := DecodeConfiguration(EncodeConfiguration(c)); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: decode accepted it: %v", name, err)
		}
	}
	if _, err := NewConfiguration(mem("a", "b"), mem("b")); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("NewConfiguration accepted a learner that is a voter: %v", err)
	}
	c, err := NewConfiguration(mem("b", "a"), nil)
	if err != nil || c.VoterIDs()[0] != "a" {
		t.Fatalf("NewConfiguration: %v %v", c, err)
	}
}

func manyMembers(n int) []Member {
	out := make([]Member, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, Member{ID: NodeID(string(rune('a'+i/26)) + string(rune('a'+i%26)))})
	}
	return out
}

// TestDecodeConfigurationRefusesMalformedBytes: truncations, a future version,
// a non-canonical integer and trailing bytes are all ErrInvalidConfiguration.
func TestDecodeConfigurationRefusesMalformedBytes(t *testing.T) {
	good := EncodeConfiguration(Configuration{Voters: mem("a", "b"), Learners: mem("c")})
	for n := 0; n < len(good); n++ {
		if _, err := DecodeConfiguration(good[:n]); err == nil {
			t.Fatalf("a configuration truncated to %d of %d bytes decoded", n, len(good))
		}
	}
	for name, b := range map[string][]byte{
		"empty":                 {},
		"future version":        {2, 0, 0, 0},
		"trailing byte":         append(append([]byte(nil), good...), 0),
		"non-canonical version": {0x81, 0x00, 0, 0, 0},
		"count over the bound":  {1, 0xff, 0x7f, 0, 0},
	} {
		if _, err := DecodeConfiguration(b); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestConfigurationRoles: voter, outgoing voter, learner and non-member are told
// apart; Members lists each id once, sorted; Addr finds an address in any list.
func TestConfigurationRoles(t *testing.T) {
	c := Configuration{Voters: mem("a", "b", "d"), Outgoing: mem("a", "b", "c"), Learners: mem("e")}
	if !c.Joint() || c.Empty() {
		t.Fatal("a joint configuration")
	}
	for id, want := range map[NodeID][3]bool{ // voter, learner, member
		"a": {true, false, true}, "c": {true, false, true}, "d": {true, false, true},
		"e": {false, true, true}, "z": {false, false, false},
	} {
		if c.IsVoter(id) != want[0] || c.IsLearner(id) != want[1] || c.IsMember(id) != want[2] {
			t.Errorf("%s: voter=%v learner=%v member=%v", id, c.IsVoter(id), c.IsLearner(id), c.IsMember(id))
		}
	}
	if got := c.Members(); len(got) != 5 || got[0] != "a" || got[4] != "e" {
		t.Fatalf("Members: %v", got)
	}
	if addr, ok := c.Addr("c"); !ok || addr != "127.0.0.1:c" {
		t.Fatalf("Addr(c): %q %v", addr, ok)
	}
	if _, ok := c.Addr("z"); ok {
		t.Fatal("Addr found a non-member")
	}
	if !(Configuration{}).Empty() {
		t.Fatal("the zero configuration is empty")
	}
	if s := c.String(); !strings.Contains(s, "outgoing=[a b c]") || !strings.Contains(s, "learners=[e]") {
		t.Fatalf("String: %s", s)
	}
	cl := c.Clone()
	cl.Voters[0].Addr = "changed"
	if c.Voters[0].Addr == "changed" {
		t.Fatal("Clone shares memory")
	}
}

// FuzzDecodeConfiguration: the decoder is total, and whatever it accepts
// re-encodes to the same bytes.
func FuzzDecodeConfiguration(f *testing.F) {
	f.Add(EncodeConfiguration(Configuration{Voters: mem("a", "b", "c")}))
	f.Add(EncodeConfiguration(Configuration{Voters: mem("a", "b", "d"), Outgoing: mem("a", "b", "c"), Learners: mem("e")}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := DecodeConfiguration(b)
		if err != nil {
			return
		}
		if !bytes.Equal(EncodeConfiguration(c), b) {
			t.Fatal("accepted a non-canonical configuration")
		}
	})
}
