package routing

import (
	"errors"
	"testing"
)

// TestInvalidConfigsAreRefused covers matrix E: every invalid configuration must
// fail explicitly with the right sentinel, wrapped in a *ConfigError, and nothing
// is silently repaired.
func TestInvalidConfigsAreRefused(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want error
	}{
		{"zero shards", Config{ShardCount: 0, ReplicationFactor: 1, Nodes: []NodeID{"n0"}}, ErrInvalidShardCount},
		{"negative shards", Config{ShardCount: -1, ReplicationFactor: 1, Nodes: []NodeID{"n0"}}, ErrInvalidShardCount},
		{"too many shards", Config{ShardCount: MaxShardCount + 1, ReplicationFactor: 1, Nodes: []NodeID{"n0"}}, ErrInvalidShardCount},
		{"no nodes", Config{ShardCount: 16, ReplicationFactor: 1, Nodes: nil}, ErrNoNodes},
		{"empty nodes slice", Config{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{}}, ErrNoNodes},
		{"rf zero", Config{ShardCount: 16, ReplicationFactor: 0, Nodes: []NodeID{"n0"}}, ErrInvalidReplicationFactor},
		{"rf negative", Config{ShardCount: 16, ReplicationFactor: -3, Nodes: []NodeID{"n0"}}, ErrInvalidReplicationFactor},
		{"rf exceeds nodes", Config{ShardCount: 16, ReplicationFactor: 4, Nodes: []NodeID{"n0", "n1", "n2"}}, ErrInvalidReplicationFactor},
		{"empty node id", Config{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{"n0", ""}}, ErrEmptyNodeID},
		{"duplicate node id", Config{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n0"}}, ErrDuplicateNode},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := NewRouter(c.cfg)
			if r != nil {
				t.Fatalf("got a Router for invalid config %+v", c.cfg)
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want errors.Is(_, %v)", err, c.want)
			}
			var ce *ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("err %v is not a *ConfigError; callers cannot inspect the field", err)
			}
			if ce.Field == "" {
				t.Errorf("ConfigError.Field is empty for %+v", c.cfg)
			}
		})
	}
}

// TestValidConfigsAreAccepted is the mirror: the boundary-valid configurations
// must build.
func TestValidConfigsAreAccepted(t *testing.T) {
	cases := []Config{
		{ShardCount: 1, ReplicationFactor: 1, Nodes: []NodeID{"n0"}},
		{ShardCount: MaxShardCount, ReplicationFactor: 1, Nodes: []NodeID{"n0"}},
		{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}},
		{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{"only"}},
	}
	for _, cfg := range cases {
		if _, err := NewRouter(cfg); err != nil {
			t.Errorf("NewRouter(%+v) = %v, want ok", cfg, err)
		}
	}
}

// TestDefaultConfig documents the v1 defaults.
func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if c.ShardCount != 16 || c.ReplicationFactor != 3 {
		t.Fatalf("DefaultConfig = %+v, want 16 shards / rf 3", c)
	}
	if len(c.Nodes) != 0 {
		t.Fatalf("DefaultConfig should carry no nodes, got %v", c.Nodes)
	}
}

// TestCanonicalIsDeterministicAndOrderIndependent: the same membership in any
// order serializes to identical, sorted, versioned bytes.
func TestCanonicalIsDeterministicAndOrderIndependent(t *testing.T) {
	a := Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n2", "n0", "n1"}}
	b := Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}}
	ba, err := a.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	bb, err := b.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if string(ba) != string(bb) {
		t.Fatalf("canonical differs by input order:\n a=%s\n b=%s", ba, bb)
	}
	want := `{"version":1,"shard_count":16,"replication_factor":3,"nodes":["n0","n1","n2"]}`
	if string(ba) != want {
		t.Fatalf("canonical = %s\nwant       %s", ba, want)
	}
}

// TestParseConfigRoundTrip: Canonical then ParseConfig reconstructs an
// equivalent, buildable Config.
func TestParseConfigRoundTrip(t *testing.T) {
	orig := Config{ShardCount: 32, ReplicationFactor: 2, Nodes: []NodeID{"b", "a", "c"}}
	data, err := orig.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRouter(got); err != nil {
		t.Fatalf("router from parsed config: %v", err)
	}
	// Canonical of the parsed config equals the original bytes.
	re, err := got.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if string(re) != string(data) {
		t.Fatalf("re-canonical differs:\n got %s\n want %s", re, data)
	}
}

// TestParseConfigRejectsBadInput: unknown version and malformed bytes are refused.
func TestParseConfigRejectsBadInput(t *testing.T) {
	if _, err := ParseConfig([]byte(`{"version":999,"shard_count":16,"replication_factor":1,"nodes":["n0"]}`)); !errors.Is(err, ErrConfigVersion) {
		t.Errorf("bad version: err = %v, want ErrConfigVersion", err)
	}
	if _, err := ParseConfig([]byte(`not json`)); !errors.Is(err, ErrMalformedConfig) {
		t.Errorf("garbage: err = %v, want ErrMalformedConfig", err)
	}
	if _, err := ParseConfig([]byte(`{"version":1,"shard_count":16,"replication_factor":1,"nodes":["n0"],"extra":true}`)); !errors.Is(err, ErrMalformedConfig) {
		t.Errorf("unknown field: err = %v, want ErrMalformedConfig", err)
	}
}
