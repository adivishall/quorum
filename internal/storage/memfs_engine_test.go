package storage_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/storage"
)

// S4 (docs/STORAGE_INTEGRATION.md §10.11): Options.FS carries every file the
// engine has — tables, the directory, the MANIFEST and the WAL — so the crash
// model of internal/fault covers all of them, not only the WAL.

// memEngineOpts returns options that put the whole engine on mem.
func memEngineOpts(mem *fault.MemFS, memTableSize int64) storage.Options {
	o := lsmOpts(memTableSize)
	o.FS = mem
	return o
}

// TestTheEngineRunsOnTheModelFilesystem: an engine opened on a MemFS flushes,
// compacts, reopens and recovers through it alone — the real directory of the
// same name is never created — and a power loss right after a flush keeps
// everything that flush made durable.
func TestTheEngineRunsOnTheModelFilesystem(t *testing.T) {
	mem := fault.NewMemFS()
	parent := t.TempDir()
	dir := filepath.Join(parent, "engine") // never created on the real disk
	durableStoreRoot(t, mem, parent)

	opts := memEngineOpts(mem, 1<<10)
	opts.DisableAutoCompaction = true
	s := openLSM(t, dir, opts)
	ctx := context.Background()
	want := map[string]string{}
	for i := 0; i < 400; i++ {
		k, v := fmt.Sprintf("k%04d", i%150), fmt.Sprintf("v%d", i)
		if err := s.Put(ctx, []byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	if err := s.Delete(ctx, []byte("k0007")); err != nil {
		t.Fatal(err)
	}
	delete(want, "k0007")
	mustFlush(t, s)
	if n, err := s.CompactAll(); err != nil || n == 0 {
		t.Fatalf("CompactAll: %d runs, %v", n, err)
	}
	if len(s.SSTables()) == 0 {
		t.Fatal("premise: the engine holds tables")
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the engine touched the real disk at %s: %v", dir, err)
	}

	// Everything was flushed; a power loss keeps exactly what the flush and the
	// compaction made durable.
	mem.CrashPowerLoss(0)
	_ = s.Close()
	s = openLSM(t, dir, opts)
	defer s.Close()
	got, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("recovered %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if string(got[k]) != v {
			t.Fatalf("key %s: %q, want %q", k, got[k], v)
		}
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the reopened engine touched the real disk at %s: %v", dir, err)
	}
}

// TestANewEngineDirectoryOutlivesAPowerLoss: on the model, a new engine
// directory survives a power loss only because the engine syncs its parent —
// the protection TestANewEngineDirectoryIsSyncedIntoItsParent observes, now
// with its consequence: without the sync the whole engine, and the durable
// applied index in its MANIFEST, would be gone.
func TestANewEngineDirectoryOutlivesAPowerLoss(t *testing.T) {
	mem := fault.NewMemFS()
	parent := t.TempDir()
	durableStoreRoot(t, mem, parent)
	dir := filepath.Join(parent, "engine")
	s := openLSM(t, dir, memEngineOpts(mem, 1<<20))
	mustApply(t, s, applyStep{nil, ai(5, 1)})
	mustFlush(t, s) // the applied index is durable in the MANIFEST
	mem.CrashPowerLoss(0)
	_ = s.Close()
	s = openLSM(t, dir, memEngineOpts(mem, 1<<20))
	defer s.Close()
	if got := s.DurableApplied(); got != ai(5, 1) {
		t.Fatalf("after a power loss the engine recovered applied %+v, want {5 1}: its directory was not durable", got)
	}
}
