package storage

import (
	"path/filepath"
	"sync"
	"testing"
)

// TestANewEngineDirectoryIsSyncedIntoItsParent: a store that creates its
// directory makes the directory's own entry durable by syncing the parent —
// without it a power loss could take the whole engine, every table and
// MANIFEST record it fsynced, and with it the durable applied index R2
// compacts against. Reopening an existing directory syncs nothing new. Found by
// the review of the S3 fixes.
func TestANewEngineDirectoryIsSyncedIntoItsParent(t *testing.T) {
	var (
		mu     sync.Mutex
		synced []string
	)
	prev := syncDir
	syncDir = func(dir string) error {
		mu.Lock()
		synced = append(synced, dir)
		mu.Unlock()
		return prev(dir)
	}
	t.Cleanup(func() { syncDir = prev })

	parent := t.TempDir()
	dir := filepath.Join(parent, "engine")
	opts := DefaultOptions()
	s, err := OpenLSMStore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]string(nil), synced...)
	synced = nil
	mu.Unlock()
	if len(got) != 1 || got[0] != parent {
		t.Fatalf("creating %s synced %v, want exactly its parent %s", dir, got, parent)
	}

	s, err = OpenLSMStore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(synced) != 0 {
		t.Fatalf("reopening an existing directory synced %v, want nothing", synced)
	}
}
