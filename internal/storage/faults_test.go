package storage_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/manifest"
	"github.com/adivishall/quorum/internal/vfs"
)

// The engine's publication protocol under manifest faults (audit M11, D10).

// injectManifest routes the manifest's writes through an injector from now
// until the test ends. A store must be opened after it, so that its manifest
// writer is opened through the injector too.
func injectManifest(t *testing.T) *fault.InjectFS {
	t.Helper()
	inj := fault.NewInjectFS(nil)
	prev := manifest.FS
	manifest.FS = inj
	t.Cleanup(func() { manifest.FS = prev })
	return inj
}

// TestAnAmbiguousCompactionEditKeepsItsOutput: a compaction whose manifest
// append fails at the fsync has written its edit — the edit naming the output
// may be durable, and the next open reads it. The output must survive: the
// compaction used to delete it, and the next open then refused a store whose
// live file was missing.
func TestAnAmbiguousCompactionEditKeepsItsOutput(t *testing.T) {
	dir := t.TempDir()
	inj := injectManifest(t)
	s := openLSM(t, dir, manualCompactOpts(storage.DefaultMemTableSize, 4))
	for i := 0; i < 4; i++ {
		flushWith(t, s, fmt.Sprintf("k%d", i), "v")
	}
	inj.Arm(fault.Injection{Op: fault.OpSync})
	if _, err := s.Compact(); !errors.Is(err, storage.ErrIO) {
		t.Fatalf("a compaction whose manifest fsync failed: %v, want storage.ErrIO", err)
	}
	_ = s.Close()
	manifest.FS = vfs.OS{}
	s2, err := storage.OpenLSMStore(dir, manualCompactOpts(storage.DefaultMemTableSize, 4))
	if err != nil {
		t.Fatalf("reopen after an ambiguous compaction edit: %v", err)
	}
	defer func() { _ = s2.Close() }()
	for i := 0; i < 4; i++ {
		assertValue(t, s2, fmt.Sprintf("k%d", i), "v")
	}
}

// TestAFlushMakesTheWALDurableBeforeItsEdit: a flush's manifest edit
// declares everything through the store's sequence persisted in a table; at
// the moment it is written, the WAL holds that sequence durably. In batch mode
// a flush used to record the edit with WAL records still unsynced, and a power
// loss then left the manifest ahead of the durable WAL.
func TestAFlushMakesTheWALDurableBeforeItsEdit(t *testing.T) {
	dir := t.TempDir()
	inj := injectManifest(t)
	opts := lsmOpts(storage.DefaultMemTableSize)
	opts.WAL.SyncInterval = time.Hour // only an explicit sync flushes the WAL
	opts.WAL.SyncBytes = 1 << 40
	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()
	mustPut(t, s, "k", "v")
	if s.WALStats().UnsyncedBytes == 0 {
		t.Fatal("premise: the put's WAL record is already synced")
	}
	unsyncedAtEdit := int64(-1)
	inj.Arm(fault.Injection{Op: fault.OpWrite, At: func() { unsyncedAtEdit = s.WALStats().UnsyncedBytes }})
	mustFlush(t, s)
	if unsyncedAtEdit != 0 {
		t.Fatalf("the flush's manifest edit was written with %d WAL bytes unsynced", unsyncedAtEdit)
	}
}
