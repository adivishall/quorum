package raftlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/vfs"
)

func openTmp(t *testing.T) (string, *Log, *Recovered) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raft.log")
	l, rec, err := Open(path, Options{Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return path, l, rec
}

func reopen(t *testing.T, path string) (*Log, *Recovered) {
	t.Helper()
	l, rec, err := Open(path, Options{Sync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return l, rec
}

// TestEmptyLogRecovers proves a fresh log recovers to empty state.
func TestEmptyLogRecovers(t *testing.T) {
	_, l, rec := openTmp(t)
	defer l.Close()
	if len(rec.Entries) != 0 || rec.HardState.Term != 0 || rec.HardState.Vote != "" {
		t.Fatalf("fresh log recovered %+v, want empty", rec)
	}
}

// TestEntriesAndHardStateRoundTrip proves entries and hardstate survive a reopen.
func TestEntriesAndHardStateRoundTrip(t *testing.T) {
	path, l, _ := openTmp(t)
	entries := []Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 1, Data: nil}, {Index: 3, Term: 2, Data: []byte("c")}}
	if err := l.Save(&HardState{Term: 2, Vote: "n1", Commit: 2}, entries); err != nil {
		t.Fatal(err)
	}
	l.Close()

	l2, rec := reopen(t, path)
	defer l2.Close()
	if len(rec.Entries) != 3 {
		t.Fatalf("recovered %d entries, want 3", len(rec.Entries))
	}
	for i, want := range entries {
		if rec.Entries[i].Index != want.Index || rec.Entries[i].Term != want.Term || string(rec.Entries[i].Data) != string(want.Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, rec.Entries[i], want)
		}
	}
	if rec.HardState.Term != 2 || rec.HardState.Vote != "n1" || rec.HardState.Commit != 2 {
		t.Fatalf("hardstate = %+v, want {2 n1 2}", rec.HardState)
	}
}

// TestSuffixReplacementReconstructs proves an appended suffix replacement is
// reconstructed on replay: the later Entry at an existing index wins and drops
// everything above it (the append-only truncation trick, ADR-016).
func TestSuffixReplacementReconstructs(t *testing.T) {
	path, l, _ := openTmp(t)
	// Append 1,2,3 at term 1, then replace from index 2 with a term-2 suffix.
	if err := l.Save(nil, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Save(nil, []Entry{{Index: 2, Term: 2, Data: []byte("new2")}, {Index: 3, Term: 2}, {Index: 4, Term: 2}}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	_, rec := reopen(t, path)
	if len(rec.Entries) != 4 {
		t.Fatalf("recovered %d entries, want 4", len(rec.Entries))
	}
	if rec.Entries[0].Term != 1 || rec.Entries[1].Term != 2 || string(rec.Entries[1].Data) != "new2" {
		t.Fatalf("reconstruction wrong: %+v", rec.Entries)
	}
	// A shorter replacement also truncates correctly.
	l2, _ := reopen(t, path)
	if err := l2.Save(nil, []Entry{{Index: 2, Term: 3}}); err != nil {
		t.Fatal(err)
	}
	l2.Close()
	_, rec = reopen(t, path)
	if len(rec.Entries) != 2 || rec.Entries[1].Term != 3 {
		t.Fatalf("short replacement gave %+v, want 2 entries ending term 3", rec.Entries)
	}
}

// TestTornTailIsTruncated proves a partial final record (crash mid-append) is
// truncated on recovery and the earlier records survive.
func TestTornTailIsTruncated(t *testing.T) {
	path, l, _ := openTmp(t)
	if err := l.Save(&HardState{Term: 1, Commit: 2}, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	// Append a few garbage bytes: a header started but never completed.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03, 0x04}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, rec := reopen(t, path)
	defer l2.Close()
	if len(rec.Entries) != 2 || rec.HardState.Term != 1 {
		t.Fatalf("torn tail lost good records: %+v", rec)
	}
	// After truncation the file can be appended to again and re-read cleanly.
	if err := l2.Save(nil, []Entry{{Index: 3, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	l2.Close()
	_, rec = reopen(t, path)
	if len(rec.Entries) != 3 {
		t.Fatalf("append after torn-tail recovery failed: %d entries", len(rec.Entries))
	}
}

// TestMidCorruptionIsFatal proves damage before the final record refuses to open,
// rather than silently skipping a committed record.
func TestMidCorruptionIsFatal(t *testing.T) {
	path, l, _ := openTmp(t)
	if err := l.Save(nil, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	// Flip a byte inside the first record's payload; a later record follows it, so
	// this is mid-log corruption, not a torn tail.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[record.HeaderSize] ^= 0xff // first payload byte
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(path, Options{Sync: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mid corruption: err = %v, want ErrCorrupt", err)
	}
}

// TestUnknownRecordKindIsFatal proves a record with an undefined kind is refused.
func TestUnknownRecordKindIsFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := record.NewWriter(f)
	if _, err := w.Append(record.Kind(99), []byte("x")); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	if _, _, err := Open(path, Options{Sync: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unknown kind: err = %v, want ErrCorrupt", err)
	}
}

// TestGapInIndexesIsFatal proves an impossible index progression is refused.
func TestGapInIndexesIsFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := record.NewWriter(f)
	// index 1 then index 3 (skips 2) — a hole a correct log never writes.
	if _, err := w.Append(kindEntry, encodeEntry(Entry{Index: 1, Term: 1})); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(kindEntry, encodeEntry(Entry{Index: 3, Term: 1})); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	if _, _, err := Open(path, Options{Sync: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("index gap: err = %v, want ErrCorrupt", err)
	}
}

// TestInspectReadsWithoutTruncating proves Inspect returns the recovered state
// read-only: it reports the same entries/HardState as Open, and it does not modify
// the file even when a torn tail is present (so it is safe to call on a live or
// just-killed log).
func TestInspectReadsWithoutTruncating(t *testing.T) {
	path, l, _ := openTmp(t)
	if err := l.Save(&HardState{Term: 4, Vote: "n2", Commit: 2}, []Entry{{Index: 1, Term: 3}, {Index: 2, Term: 4}}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	// Append a torn (incomplete) record, as a crash mid-append would leave.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0xaa, 0xbb, 0xcc})
	f.Close()

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := Inspect(path)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if rec.HardState.Term != 4 || rec.HardState.Vote != "n2" || rec.HardState.Commit != 2 || len(rec.Entries) != 2 {
		t.Fatalf("Inspect recovered %+v, want term4/n2/commit2/2 entries", rec)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("Inspect modified the file: size %d -> %d", before.Size(), after.Size())
	}
}

// --- Phase 10: the durability boundary under injected faults (INV-F1, INV-R6) ---

const memPath = "/node/raft.log"

func openMem(t *testing.T, fsys vfs.FS) (*Log, *Recovered) {
	t.Helper()
	l, rec, err := Open(memPath, Options{Sync: true, FS: fsys})
	if err != nil {
		t.Fatalf("Open on %T: %v", fsys, err)
	}
	return l, rec
}

// TestSaveIsDurableWhenItReturns pins the fsync inside Save: once Save returns,
// every byte it wrote survives a modeled power loss. Without the fsync the bytes
// would only be cached, and a power loss would erase the HardState a vote reply
// depended on. With Sync off (the unsafe test-only mode) the same Save is NOT
// durable, which is exactly what DisableSync trades away.
func TestSaveIsDurableWhenItReturns(t *testing.T) {
	mem := fault.NewMemFS()
	l, _ := openMem(t, mem)
	if err := l.Save(&HardState{Term: 3, Vote: "n2"}, []Entry{{Index: 1, Term: 3, Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if !mem.FullySynced(memPath) {
		t.Fatal("Save returned with bytes not yet fsynced — a reply sent now could outlive a power loss's memory of it")
	}
	mem.CrashPowerLoss(0)
	rec, err := InspectFS(mem, memPath)
	if err != nil || rec.HardState.Term != 3 || rec.HardState.Vote != "n2" || len(rec.Entries) != 1 {
		t.Fatalf("after power loss: %+v %v, want term 3 vote n2 and 1 entry", rec, err)
	}

	unsafe := fault.NewMemFS()
	lu, _, err := Open(memPath, Options{Sync: false, FS: unsafe})
	if err != nil {
		t.Fatal(err)
	}
	if err := lu.Save(&HardState{Term: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if unsafe.FullySynced(memPath) {
		t.Fatal("Sync:false still fsynced; the option is not doing what it says")
	}
}

// TestFailedWriteLatchesAndLogStaysRecoverable proves the failure policy for a
// torn (short) write: Save fails, every later Save is refused WITHOUT touching the
// file — appending behind a partial record would make the log unopenable — and a
// fresh Open truncates the torn record and recovers everything before it.
func TestFailedWriteLatchesAndLogStaysRecoverable(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	l, _ := openMem(t, inj)
	if err := l.Save(&HardState{Term: 1, Vote: "n1"}, []Entry{{Index: 1, Term: 1}}); err != nil {
		t.Fatal(err)
	}

	// The second record write of the next Save is torn after 5 bytes.
	inj.Arm(fault.Injection{Op: fault.OpWrite, Nth: 2, Short: 5})
	err := l.Save(&HardState{Term: 2, Vote: "n3"}, []Entry{{Index: 2, Term: 2}})
	if !errors.Is(err, ErrFailed) || !errors.Is(err, fault.ErrInjected) {
		t.Fatalf("torn Save: err = %v, want ErrFailed wrapping the injected error", err)
	}
	opsAtFailure := len(inj.Ops())

	// Every later Save is refused, and nothing more reaches the file.
	for i := 0; i < 3; i++ {
		if err := l.Save(&HardState{Term: 9}, []Entry{{Index: 3, Term: 9}}); !errors.Is(err, ErrFailed) {
			t.Fatalf("Save after a failure: err = %v, want ErrFailed", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close of a failed log: %v", err)
	}
	for _, op := range inj.Ops()[opsAtFailure:] {
		if op.Op == fault.OpWrite || op.Op == fault.OpSync || op.Op == fault.OpTruncate {
			t.Fatalf("the failed log still performed %s after the failure", op.Op)
		}
	}

	// A process crash keeps the cached bytes, torn record included; reopening
	// repairs the tail and recovers the state before the failed Save's HardState.
	mem.CrashProcess()
	l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatalf("reopen after a torn write: %v (the log must stay recoverable)", err)
	}
	defer l2.Close()
	if rec.HardState.Term != 1 || rec.HardState.Vote != "n1" {
		t.Fatalf("recovered HardState %+v, want the last complete one {1 n1}", rec.HardState)
	}
	// Entry 2 was the Save's first record and completed before the torn write; a
	// completed-but-unacknowledged record may survive (INV-F2 allows a prefix of
	// an interrupted Save), and it must be well-formed if it does.
	if n := len(rec.Entries); n < 1 || n > 2 || rec.Entries[0].Term != 1 {
		t.Fatalf("recovered entries %+v, want entry 1 and at most the completed entry 2", rec.Entries)
	}
	if !mem.FullySynced(memPath) {
		t.Fatal("the torn-tail repair was not fsynced before the log was reused")
	}
}

// TestFailedSyncLatches proves the failure policy for an fsync failure: the Save
// fails, later Saves are refused without writing, Close does not fsync again, and
// the written-but-unsynced bytes are exactly what a power loss may erase.
func TestFailedSyncLatches(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	l, _ := openMem(t, inj)
	if err := l.Save(&HardState{Term: 1, Vote: "n1"}, nil); err != nil {
		t.Fatal(err)
	}
	inj.Arm(fault.Injection{Op: fault.OpSync})
	if err := l.Save(&HardState{Term: 2, Vote: "n2"}, nil); !errors.Is(err, ErrFailed) {
		t.Fatalf("Save with failing fsync: err = %v, want ErrFailed", err)
	}
	opsAtFailure := len(inj.Ops())
	if err := l.Save(&HardState{Term: 3}, nil); !errors.Is(err, ErrFailed) {
		t.Fatalf("Save after fsync failure: err = %v, want ErrFailed", err)
	}
	_ = l.Close()
	for _, op := range inj.Ops()[opsAtFailure:] {
		if op.Op == fault.OpWrite || op.Op == fault.OpSync {
			t.Fatalf("performed %s after an fsync failure", op.Op)
		}
	}

	// Power loss: only the last fsynced HardState survives.
	mem.CrashPowerLoss(0)
	rec, err := InspectFS(mem, memPath)
	if err != nil || rec.HardState.Term != 1 || rec.HardState.Vote != "n1" {
		t.Fatalf("after power loss: %+v %v, want the synced {1 n1}", rec, err)
	}
}

// TestOpenMakesRecoveredStateDurable pins the fix for a bug the Phase 10 simulator
// found. A Save writes a vote and its fsync fails; the process stops; the page
// cache still holds the vote, so the restarted process recovers it and would act
// on it — re-granting that vote — although a power loss could still erase it,
// after which the node could vote for someone else in the same term. Open must
// therefore make whatever it recovered durable before returning.
func TestOpenMakesRecoveredStateDurable(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	l, _ := openMem(t, inj)
	if err := l.Save(&HardState{Term: 1}, nil); err != nil {
		t.Fatal(err)
	}
	inj.Arm(fault.Injection{Op: fault.OpSync})
	if err := l.Save(&HardState{Term: 2, Vote: "c"}, nil); err == nil {
		t.Fatal("expected the fsync failure")
	}
	mem.CrashProcess() // the node fail-stops; the cache keeps the written vote

	l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if rec.HardState.Term != 2 || rec.HardState.Vote != "c" {
		t.Fatalf("recovered %+v, want the cached vote {2 c}", rec.HardState)
	}
	if !mem.FullySynced(memPath) {
		t.Fatal("Open returned state recovered from un-fsynced bytes without making it durable")
	}
	mem.CrashPowerLoss(0)
	if rec, err := InspectFS(mem, memPath); err != nil || rec.HardState.Vote != "c" {
		t.Fatalf("after power loss: %+v %v — the recovered vote was lost", rec, err)
	}
}

// TestNewLogCreationIsDurable proves Open makes a new log file's directory entry
// durable (SyncDir), so a power loss right after the first Save keeps the file.
func TestNewLogCreationIsDurable(t *testing.T) {
	mem := fault.NewMemFS()
	l, _ := openMem(t, mem)
	if err := l.Save(&HardState{Term: 1}, nil); err != nil {
		t.Fatal(err)
	}
	mem.CrashPowerLoss(0)
	if _, err := mem.Stat(memPath); err != nil {
		t.Fatalf("log file lost to a power loss after Open+Save: %v", err)
	}
}

// FuzzDecodeEntry and FuzzDecodeHardState prove the payload decoders never panic.
func FuzzDecodeEntry(f *testing.F) {
	f.Add(encodeEntry(Entry{Index: 1, Term: 1, Data: []byte("x")}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = decodeEntry(data) })
}

func FuzzDecodeHardState(f *testing.F) {
	f.Add(encodeHardState(HardState{Term: 1, Vote: "n1", Commit: 1}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = decodeHardState(data) })
}
