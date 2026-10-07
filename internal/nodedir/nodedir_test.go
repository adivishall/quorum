package nodedir

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/record"
)

func open(t *testing.T, dir string, opts Options) *Dir {
	t.Helper()
	d, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open(%s, %+v): %v", dir, opts, err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestFreshDirectoryNeedsInit: a directory with no node — new, or one whose
// state was lost — is refused unless the operator initializes it (-init), and
// initializing needs a cluster id. A missing directory is created, mode 0700.
func TestFreshDirectoryNeedsInit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "data")
	if _, err := Open(dir, Options{Node: "n1", Cluster: "c1"}); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("a missing directory without -init: %v, want ErrUninitialized", err)
	}
	if _, err := os.Stat(filepath.Dir(dir)); !os.IsNotExist(err) {
		t.Fatalf("a refused Open created the directory: %v", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{Node: "n1", Cluster: "c1"}); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("an empty directory without -init: %v, want ErrUninitialized", err)
	}
	if err := os.RemoveAll(filepath.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{Node: "n1"}); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("a fresh directory without -init or a cluster id: %v, want ErrUninitialized", err)
	}
	if _, err := Open(dir, Options{Node: "n1", Init: true}); !errors.Is(err, ErrClusterID) {
		t.Fatalf("-init without a cluster id: %v, want ErrClusterID", err)
	}
	d := open(t, dir, Options{Node: "n1", Cluster: "c1", Init: true})
	if d.State != Initializing || d.ID != (Identity{Node: "n1", Cluster: "c1"}) {
		t.Fatalf("a new directory: state %s, identity %+v", d.State, d.ID)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("the data directory: %v, mode %v; want 0700", err, fi.Mode())
	}
}

// TestInitializationLifecycle: initialization finishes once; an interrupted
// one resumes on the next start with or without -init; a finished one runs,
// and refuses -init.
func TestInitializationLifecycle(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{Node: "n1", Cluster: "c1", Init: true})
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Close() // dies before FinishInit

	d = open(t, dir, Options{Node: "n1"})
	if d.State != Initializing || d.ID.Cluster != "c1" {
		t.Fatalf("an interrupted initialization resumes: state %s, identity %+v", d.State, d.ID)
	}
	if err := d.FinishInit(); err != nil {
		t.Fatal(err)
	}
	_ = d.Close()
	id, found, err := Load(dir)
	if err != nil || !found || !id.Initialized {
		t.Fatalf("after FinishInit: %+v %v %v", id, found, err)
	}

	d = open(t, dir, Options{Node: "n1", Cluster: "c1"})
	if d.State != Running {
		t.Fatalf("a restart: state %s, want running", d.State)
	}
	_ = d.Close()
	if _, err := Open(dir, Options{Node: "n1", Cluster: "c1", Init: true}); !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("-init on an initialized directory: %v, want ErrAlreadyInitialized", err)
	}
}

// TestIdentityMismatchIsRefused: a directory is one node's, of one cluster.
func TestIdentityMismatchIsRefused(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir, Options{Node: "n1", Cluster: "c1", Init: true})
	if err := d.FinishInit(); err != nil {
		t.Fatal(err)
	}
	_ = d.Close()
	for name, opts := range map[string]Options{
		"another node":           {Node: "n2", Cluster: "c1"},
		"another node, no id":    {Node: "n2"},
		"another cluster":        {Node: "n1", Cluster: "c2"},
		"another node, and init": {Node: "n2", Cluster: "c1", Init: true},
	} {
		if _, err := Open(dir, opts); !errors.Is(err, ErrIdentity) {
			t.Errorf("%s: %v, want ErrIdentity", name, err)
		}
	}
}

// TestDataDirectoryIsLocked: while one Open holds the directory, a second —
// in this process as in another — is refused; Close releases it.
func TestDataDirectoryIsLocked(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir, Options{Node: "n1", Cluster: "c1", Init: true})
	if _, err := Open(dir, Options{Node: "n1", Cluster: "c1"}); !errors.Is(err, ErrLocked) {
		t.Fatalf("a second Open while the first holds the directory: %v, want ErrLocked", err)
	}
	_ = d.Close()
	d = open(t, dir, Options{Node: "n1", Cluster: "c1"})
	_ = d.Close()
}

// TestLegacyDirectoryIsAdopted: a directory written before node identities —
// Raft state, no identity file — is adopted (with a cluster id) as this node's,
// unless it holds another node's log; -init refuses it (it is not empty), and
// so do files that are not a node's.
func TestLegacyDirectoryIsAdopted(t *testing.T) {
	write := func(t *testing.T, dir, name string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for name, file := range map[string]string{"-raft": "raft-n1.log", "-raft snapshot": "raft-n1.log.snap", "-cluster": "groups/0/raft.log"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			write(t, dir, file)
			if _, err := Open(dir, Options{Node: "n1", Cluster: "c1", Init: true}); !errors.Is(err, ErrAlreadyInitialized) {
				t.Fatalf("-init on legacy state: %v", err)
			}
			if _, err := Open(dir, Options{Node: "n1"}); !errors.Is(err, ErrClusterID) {
				t.Fatalf("adopting without a cluster id: %v", err)
			}
			d := open(t, dir, Options{Node: "n1", Cluster: "c1"})
			if d.State != Adopted || !d.ID.Initialized {
				t.Fatalf("state %s, identity %+v", d.State, d.ID)
			}
		})
	}
	dir := t.TempDir()
	write(t, dir, "raft-n2.log")
	if _, err := Open(dir, Options{Node: "n1", Cluster: "c1"}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("another node's log: %v, want ErrIdentity", err)
	}
	dir = t.TempDir()
	write(t, dir, "notes.txt")
	for _, init := range []bool{false, true} {
		if _, err := Open(dir, Options{Node: "n1", Cluster: "c1", Init: init}); !errors.Is(err, ErrNotEmpty) {
			t.Fatalf("a directory of other files (init %v): %v, want ErrNotEmpty", init, err)
		}
	}
}

// TestIdentityCodec: the encoding round-trips; every truncation, every bit
// flip, a future version, unknown flags and trailing bytes are refused.
func TestIdentityCodec(t *testing.T) {
	id := Identity{Node: "node-1", Cluster: "prod.eu-1", Initialized: true}
	good, err := Encode(id)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Decode(good); err != nil || got != id {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	for n := 0; n < len(good); n++ {
		if _, err := Decode(good[:n]); !errors.Is(err, ErrIdentity) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
	for i := range good {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), good...)
			bad[i] ^= 1 << bit
			if got, err := Decode(bad); err == nil && got == id {
				t.Fatalf("bit %d of byte %d went unnoticed", bit, i)
			}
		}
	}
	future, _ := record.Encode(nil, identityKind, append([]byte(identityMagic), 3, 0, 1, 'n', 1, 'c'))
	flags, _ := record.Encode(nil, identityKind, append([]byte(identityMagic), 1, 2, 1, 'n', 1, 'c'))
	trailing, _ := record.Encode(nil, identityKind, append([]byte(identityMagic), 1, 0, 1, 'n', 1, 'c', 0))
	for name, b := range map[string][]byte{"future version": future, "unknown flag": flags, "trailing byte": trailing} {
		if _, err := Decode(b); !errors.Is(err, ErrIdentity) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for _, bad := range []Identity{{Cluster: "c"}, {Node: "n"}, {Node: "n", Cluster: "has space"}} {
		if _, err := Encode(bad); err == nil {
			t.Errorf("Encode(%+v) accepted", bad)
		}
	}
}

// FuzzDecode: the decoder is total, and what it accepts re-encodes to the
// same bytes.
func FuzzDecode(f *testing.F) {
	good, _ := Encode(Identity{Node: "n1", Cluster: "c1", Initialized: true})
	f.Add(good)
	v2, _ := Encode(Identity{Node: "n1", Cluster: "c1", Initialized: true, Settings: "mode=raft"})
	f.Add(v2)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		id, err := Decode(b)
		if err != nil {
			return
		}
		again, err := Encode(id)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("accepted a non-canonical identity: %v", err)
		}
	})
}

// TestSettingsArePinned (audit H5): a directory records its node's replica
// settings when it is initialized, and a start with other settings is refused —
// they are part of the replicated state machine's definition, and a node
// cannot change them by restarting. A version-1 identity, from before
// settings were recorded, records the next start's.
func TestSettingsArePinned(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir, Options{Node: "n1", Cluster: "c1", Init: true, Settings: "mode=raft session-max=8"})
	if err := d.FinishInit(); err != nil {
		t.Fatal(err)
	}
	_ = d.Close()
	d = open(t, dir, Options{Node: "n1", Settings: "mode=raft session-max=8"})
	_ = d.Close()
	for _, other := range []string{"mode=raft session-max=9", "mode=cluster session-max=8", ""} {
		if _, err := Open(dir, Options{Node: "n1", Settings: other}); !errors.Is(err, ErrSettings) {
			t.Fatalf("settings %q over %q: %v, want ErrSettings", other, "mode=raft session-max=8", err)
		}
	}
	// A version-1 identity has none: the next start's are recorded.
	v1 := t.TempDir()
	if err := write(v1, Identity{Node: "n1", Cluster: "c1", Initialized: true}); err != nil {
		t.Fatal(err)
	}
	d = open(t, v1, Options{Node: "n1", Settings: "mode=raft session-max=8"})
	_ = d.Close()
	if id, _, err := Load(v1); err != nil || id.Settings != "mode=raft session-max=8" || !id.Initialized {
		t.Fatalf("the upgraded identity: %+v %v", id, err)
	}
	if _, err := Open(v1, Options{Node: "n1", Settings: "mode=raft session-max=9"}); !errors.Is(err, ErrSettings) {
		t.Fatalf("after the upgrade other settings: %v, want ErrSettings", err)
	}
}

// TestIdentityCodecV2: the version-2 encoding (with settings) round-trips, and
// every truncation and bit flip is refused or changes nothing; empty settings
// in a version-2 record are refused (they are what version 1 means).
func TestIdentityCodecV2(t *testing.T) {
	id := Identity{Node: "n1", Cluster: "c1", Initialized: true, Settings: "mode=cluster shards=4"}
	good, err := Encode(id)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Decode(good); err != nil || got != id {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	for n := 0; n < len(good); n++ {
		if _, err := Decode(good[:n]); !errors.Is(err, ErrIdentity) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
	for i := range good {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), good...)
			bad[i] ^= 1 << bit
			if got, err := Decode(bad); err == nil && got == id {
				t.Fatalf("bit %d of byte %d went unnoticed", bit, i)
			}
		}
	}
	empty, _ := record.Encode(nil, identityKind, append([]byte(identityMagic), 2, 0, 1, 'n', 1, 'c', 0))
	if _, err := Decode(empty); !errors.Is(err, ErrIdentity) {
		t.Fatalf("a version-2 record with empty settings: %v", err)
	}
}

// TestAnUnfinishedInitWithNoGroupStateTakesNewFlags: an initialization that
// stopped before any group state was recorded pinned nothing a group follows,
// so a retry with corrected settings or cluster id replaces them. Once group
// state exists, they stay pinned, finished or not. Before, the first attempt's
// values were pinned forever, and only removing node.identity by hand let the
// operator correct them.
func TestAnUnfinishedInitWithNoGroupStateTakesNewFlags(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir, Options{Node: "n1", Cluster: "c1", Init: true, Settings: "nodes=a"})
	_ = d.Close() // stopped before FinishInit, before any group
	d = open(t, dir, Options{Node: "n1", Cluster: "c2", Settings: "nodes=a,n1"})
	if d.State != Initializing || d.ID.Cluster != "c2" || d.ID.Settings != "nodes=a,n1" {
		t.Fatalf("the retry: state %s, identity %+v", d.State, d.ID)
	}
	_ = d.Close()
	if id, _, err := Load(dir); err != nil || id.Cluster != "c2" || id.Settings != "nodes=a,n1" {
		t.Fatalf("the recorded identity after the retry: %+v %v", id, err)
	}
	// A group's state now exists: the values it was recorded under stay.
	if err := os.MkdirAll(filepath.Join(dir, "groups", "0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{Node: "n1", Settings: "nodes=b"}); !errors.Is(err, ErrSettings) {
		t.Fatalf("other settings once a group exists: %v, want ErrSettings", err)
	}
	if _, err := Open(dir, Options{Node: "n1", Cluster: "c3", Settings: "nodes=a,n1"}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("another cluster once a group exists: %v, want ErrIdentity", err)
	}
}

// TestALegacyLogIsMatchedByItsWholeName: a node's legacy log is
// raft-<id>.log (and raft-<id>.log.*), and an id may contain ".log". The
// owner was cut at the first ".log": node "app.log1" was refused its own log,
// and node "app" adopted app.log1's.
func TestALegacyLogIsMatchedByItsWholeName(t *testing.T) {
	for _, tc := range []struct {
		node string
		ok   bool
	}{{"app.log1", true}, {"app", false}} {
		dir := t.TempDir()
		for _, f := range []string{"raft-app.log1.log", "raft-app.log1.log.group"} {
			if err := os.WriteFile(filepath.Join(dir, f), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		d, err := Open(dir, Options{Node: tc.node, Cluster: "c1", Settings: "s"})
		if tc.ok {
			if err != nil || d.State != Adopted {
				t.Fatalf("node %s on its own log: %v", tc.node, err)
			}
			_ = d.Close()
		} else if !errors.Is(err, ErrIdentity) {
			if err == nil {
				_ = d.Close()
			}
			t.Fatalf("node %s on node app.log1's log: %v, want ErrIdentity", tc.node, err)
		}
	}
}
