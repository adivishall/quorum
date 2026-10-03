package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/manifest"
	"github.com/adivishall/quorum/internal/storage/wal"
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

// TestAReplayThatFlushesOpens: an open whose WAL replay outgrows the memtable
// flushes during replay, before the store has a WAL of its own. Syncing the
// WAL before the flush's edit must not assume one exists: it did, and every
// such open panicked.
func TestAReplayThatFlushesOpens(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	for i := 0; i < 64; i++ {
		mustPut(t, s, fmt.Sprintf("key%03d", i), fmt.Sprintf("value%03d", i))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLSM(t, dir, lsmOpts(256))
	defer func() { _ = s.Close() }()
	if s.Recovery().FlushesOnReplay == 0 {
		t.Fatal("premise: the replay did not flush")
	}
	for i := 0; i < 64; i++ {
		got, err := s.Get(context.Background(), []byte(fmt.Sprintf("key%03d", i)))
		if err != nil || string(got) != fmt.Sprintf("value%03d", i) {
			t.Fatalf("key%03d after a replay that flushed: %q, %v", i, got, err)
		}
	}
}

// TestOpenSweepsOnlyAgainstADurableManifest (audit M11): a compaction whose
// manifest fsync failed may have left its edit — inputs deleted, output added
// — in the page cache only. The next open reads that edit; it must not delete
// the inputs before the state naming the output is durable. It swept them
// first: when the fresh manifest then failed to install, a power loss left the
// old manifest naming deleted tables, and the store never opened again.
func TestOpenSweepsOnlyAgainstADurableManifest(t *testing.T) {
	dir := t.TempDir()
	inj := injectManifest(t)
	opts := manualCompactOpts(storage.DefaultMemTableSize, 4)
	s := openLSM(t, dir, opts)
	for i := 0; i < 4; i++ {
		flushWith(t, s, fmt.Sprintf("k%d", i), "v")
	}
	mpath := filepath.Join(dir, manifest.Name(s.ManifestNumber()))
	st, err := os.Stat(mpath)
	if err != nil {
		t.Fatal(err)
	}
	durable := st.Size() // every edit so far was fsynced
	inj.Arm(fault.Injection{Op: fault.OpSync})
	if _, err := s.Compact(); !errors.Is(err, storage.ErrIO) {
		t.Fatalf("premise: the compaction whose manifest fsync failed: %v", err)
	}
	_ = s.Close()
	// Same boot: the open reads the unsynced edit; the fresh manifest fails.
	inj.Arm(fault.Injection{Op: fault.OpWrite, Err: syscall.ENOSPC})
	if _, err := storage.OpenLSMStore(dir, opts); err == nil {
		t.Fatal("premise: the open with a failing manifest install succeeded")
	}
	// Power loss: the never-fsynced edit is gone.
	if err := os.Truncate(mpath, durable); err != nil {
		t.Fatal(err)
	}
	manifest.FS = vfs.OS{}
	s = openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()
	for i := 0; i < 4; i++ {
		if got, err := s.Get(context.Background(), []byte(fmt.Sprintf("k%d", i))); err != nil || string(got) != "v" {
			t.Fatalf("k%d after the power loss: %q, %v", i, got, err)
		}
	}
}

// TestAFailedOpenClosesItsManifest: an open that fails after installing its
// fresh manifest — here removing an obsolete one — closes it. The error path
// closed the tables but not the manifest, leaking its descriptor.
func TestAFailedOpenClosesItsManifest(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("counts descriptors through /proc/self/fd")
	}
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	mustPut(t, s, "k", "v")
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	inj := injectManifest(t)
	inj.Arm(fault.Injection{Op: fault.OpRemove})
	if _, err := storage.OpenLSMStore(dir, lsmOpts(storage.DefaultMemTableSize)); err == nil {
		t.Fatal("premise: the open with a failing RemoveObsolete succeeded")
	}
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && strings.HasPrefix(target, dir) {
			t.Fatalf("the failed open left %s open", target)
		}
	}
}

// TestAnExplicitCompactionAfterAFailureRunsNothing (audit M11): after a
// compaction failure no compaction runs again in the process — an explicit
// Compact included. Each one merged again after a failed manifest edit and,
// the edit being refused, kept its output on disk: an orphan per call.
func TestAnExplicitCompactionAfterAFailureRunsNothing(t *testing.T) {
	dir := t.TempDir()
	inj := injectManifest(t)
	s := openLSM(t, dir, manualCompactOpts(storage.DefaultMemTableSize, 4))
	defer func() { _ = s.Close() }()
	for i := 0; i < 4; i++ {
		flushWith(t, s, fmt.Sprintf("k%d", i), "v")
	}
	inj.Arm(fault.Injection{Op: fault.OpSync})
	if _, err := s.Compact(); !errors.Is(err, storage.ErrIO) {
		t.Fatalf("premise: %v", err)
	}
	before := len(sstFiles(t, dir))
	for i := 0; i < 3; i++ {
		if ran, err := s.Compact(); ran || err == nil {
			t.Fatalf("Compact #%d after the failure: ran %v, err %v; want refused", i+1, ran, err)
		}
	}
	if after := len(sstFiles(t, dir)); after != before {
		t.Fatalf("explicit compactions after the failure left %d more tables on disk", after-before)
	}
}

// TestSyncOffNeverFsyncsTheWAL: a flush syncs the WAL before its manifest
// edit — except in SyncOff, whose definition is that the store never fsyncs
// the log (a test mode that must not look durable).
func TestSyncOffNeverFsyncsTheWAL(t *testing.T) {
	opts := lsmOpts(storage.DefaultMemTableSize)
	opts.WAL.SyncMode = wal.SyncOff
	s := openLSM(t, t.TempDir(), opts)
	defer func() { _ = s.Close() }()
	mustPut(t, s, "k", "v")
	mustFlush(t, s)
	if n := s.WALStats().Syncs; n != 0 {
		t.Fatalf("a SyncOff store fsynced its WAL %d times", n)
	}
}

// TestAPutAlwaysFitsAWALRecord: the options are refused when the largest key
// and value would not fit one WAL record. A store allowed them: a put of a
// value the log could not frame was refused by the WAL, which latched.
func TestAPutAlwaysFitsAWALRecord(t *testing.T) {
	opts := lsmOpts(storage.DefaultMemTableSize)
	opts.MaxValueSize = 100 << 20
	if _, err := storage.OpenLSMStore(t.TempDir(), opts); !errors.Is(err, storage.ErrInvalidOptions) {
		t.Fatalf("a 100 MiB MaxValueSize: %v, want ErrInvalidOptions", err)
	}
	opts.MaxValueSize = 32 << 20
	s := openLSM(t, t.TempDir(), opts)
	_ = s.Close()
}
