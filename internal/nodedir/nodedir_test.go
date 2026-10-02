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
	future, _ := record.Encode(nil, identityKind, append([]byte(identityMagic), 2, 0, 1, 'n', 1, 'c'))
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
