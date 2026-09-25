package raftlog

import (
	"errors"
	"fmt"
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

	// The next Save changes the term, so its records are the HardState then the
	// entry (SavePlan); the entry record — the second write — is torn after 5 bytes.
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
	// repairs the tail: the torn entry record is dropped whole, and the state is
	// the completed prefix of the interrupted Save (INV-F2) — the new term and
	// vote (their record completed before the torn write) over the old entries.
	mem.CrashProcess()
	l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatalf("reopen after a torn write: %v (the log must stay recoverable)", err)
	}
	defer l2.Close()
	if rec.HardState.Term != 2 || rec.HardState.Vote != "n3" || rec.HardState.Commit != 0 {
		t.Fatalf("recovered HardState %+v, want the completed leading record {2 n3 0}", rec.HardState)
	}
	if n := len(rec.Entries); n != 1 || rec.Entries[0].Term != 1 {
		t.Fatalf("recovered entries %+v, want exactly entry 1 (the torn entry 2 dropped whole)", rec.Entries)
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

// --- Phase 11: crash windows inside one Save (docs/CRASH_RECOVERY.md §5) ---

// crashBeforeWrite returns a MemFS+InjectFS pair whose owning process dies just
// before the nth write of the log after arming (fault.Injection.At): the file
// then holds exactly the records written before that boundary.
func crashBeforeWrite(mem *fault.MemFS, inj *fault.InjectFS, nth int) {
	inj.Arm(fault.Injection{Op: fault.OpWrite, Path: memPath, Nth: nth, At: mem.CrashProcess})
}

// TestTermChangeIsDurableBeforeEntriesOfThatTerm pins the record order of a Save
// that carries both a term change and entries of the new term — a single-node
// election, whose one Save holds the term, the self-vote and the no-op. Whichever
// record boundary the process dies at, the reopened log must never hold an entry
// whose term exceeds the recovered currentTerm: that log would be refused by the
// core (ErrTermRegression) and the node could never restart. Found by the Phase
// 11 crash matrix; with the old entries-first order, dying between the entry
// record and the HardState record bricked the node.
func TestTermChangeIsDurableBeforeEntriesOfThatTerm(t *testing.T) {
	for nth := 1; nth <= 3; nth++ {
		t.Run(fmt.Sprintf("crash before write %d", nth), func(t *testing.T) {
			mem := fault.NewMemFS()
			inj := fault.NewInjectFS(mem)
			l, _ := openMem(t, inj)
			crashBeforeWrite(mem, inj, nth)
			err := l.Save(&HardState{Term: 1, Vote: "n1", Commit: 1}, []Entry{{Index: 1, Term: 1}})
			if !errors.Is(err, fault.ErrCrashed) {
				t.Fatalf("Save = %v, want the crash at write %d", err, nth)
			}
			l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
			if err != nil {
				t.Fatalf("reopen after a crash before write %d: %v (a log the node wrote must reopen)", nth, err)
			}
			defer l2.Close()
			if n := len(rec.Entries); n > 0 && rec.Entries[n-1].Term > rec.HardState.Term {
				t.Fatalf("crash before write %d recovered an entry of term %d under currentTerm %d: the core refuses this log",
					nth, rec.Entries[n-1].Term, rec.HardState.Term)
			}
			// What each boundary leaves: nothing; the term and vote (commit still 0);
			// the term, vote and entry (commit still 0); the crash never reaches a
			// fourth record, so the final commit only lands with a complete Save.
			switch nth {
			case 1:
				if rec.HardState.Term != 0 || len(rec.Entries) != 0 {
					t.Fatalf("before write 1: recovered %+v, want nothing", rec)
				}
			case 2:
				if rec.HardState.Term != 1 || rec.HardState.Vote != "n1" || rec.HardState.Commit != 0 || len(rec.Entries) != 0 {
					t.Fatalf("before write 2: recovered %+v, want term 1 vote n1 commit 0 and no entry", rec)
				}
			case 3:
				if rec.HardState.Term != 1 || rec.HardState.Commit != 0 || len(rec.Entries) != 1 {
					t.Fatalf("before write 3: recovered %+v, want term 1, commit 0, the entry", rec)
				}
			}
		})
	}
}

// TestCommitNeverCoversEntriesTheSaveHadNotWritten pins the other half of the
// order: on a suffix replacement whose Save also raises the commit index, the
// HardState written BEFORE the replacing entries carries the OLD commit. If it
// carried the new one, a crash before the entries would leave the old,
// conflicting entries on disk under a commit index that covers them — and the
// restarted node would apply entries the cluster never committed.
func TestCommitNeverCoversEntriesTheSaveHadNotWritten(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	l, _ := openMem(t, inj)
	// Term 1: entries 1..3, only index 1 committed.
	if err := l.Save(&HardState{Term: 1, Vote: "n1", Commit: 1}, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Data: []byte("old2")}, {Index: 3, Term: 1, Data: []byte("old3")}}); err != nil {
		t.Fatal(err)
	}
	// A new leader in term 2 replaces 2..3 and its AppendEntries lets us commit 3:
	// one Save with a term change, replacing entries, and a higher commit. Die
	// before the first replacing entry record (write 2: the leading HardState is
	// write 1).
	crashBeforeWrite(mem, inj, 2)
	err := l.Save(&HardState{Term: 2, Vote: "", Commit: 3}, []Entry{{Index: 2, Term: 2, Data: []byte("new2")}, {Index: 3, Term: 2, Data: []byte("new3")}})
	if !errors.Is(err, fault.ErrCrashed) {
		t.Fatalf("Save = %v, want the crash", err)
	}
	_, rec, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatal(err)
	}
	if rec.HardState.Term != 2 {
		t.Fatalf("recovered term %d, want 2 (the term change precedes the entries)", rec.HardState.Term)
	}
	if rec.HardState.Commit != 1 {
		t.Fatalf("recovered commit %d over the old entries %q; want the previous commit 1 — a commit must never be durable before the entries it covers",
			rec.HardState.Commit, [][]byte{rec.Entries[1].Data, rec.Entries[2].Data})
	}
	if string(rec.Entries[1].Data) != "old2" {
		t.Fatalf("recovered entry 2 = %q, want the still-present old entry", rec.Entries[1].Data)
	}
}

// TestSavePlanIsTheRecordOrder pins SavePlan itself, which both the log and the
// simulator's crash model rely on: no HardState → no records around the entries;
// no entries → one trailing record; an unchanged term and vote → one trailing
// record after the entries; a changed term or vote → a leading record with the
// previous commit, plus a trailing one only if the commit changed too.
func TestSavePlanIsTheRecordOrder(t *testing.T) {
	prev := HardState{Term: 3, Vote: "a", Commit: 5}
	entries := []Entry{{Index: 6, Term: 4}}
	type want struct{ lead, trail *HardState }
	cases := []struct {
		name    string
		hs      *HardState
		entries []Entry
		want    want
	}{
		{"no hardstate", nil, entries, want{}},
		{"hardstate only", &HardState{Term: 4, Vote: "b", Commit: 5}, nil, want{nil, &HardState{Term: 4, Vote: "b", Commit: 5}}},
		{"commit only, with entries", &HardState{Term: 3, Vote: "a", Commit: 6}, entries, want{nil, &HardState{Term: 3, Vote: "a", Commit: 6}}},
		{"term change, same commit", &HardState{Term: 4, Vote: "b", Commit: 5}, entries, want{&HardState{Term: 4, Vote: "b", Commit: 5}, nil}},
		{"term change and commit", &HardState{Term: 4, Vote: "b", Commit: 6}, entries, want{&HardState{Term: 4, Vote: "b", Commit: 5}, &HardState{Term: 4, Vote: "b", Commit: 6}}},
		{"vote change only", &HardState{Term: 3, Vote: "c", Commit: 5}, entries, want{&HardState{Term: 3, Vote: "c", Commit: 5}, nil}},
	}
	same := func(a, b *HardState) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }
	for _, c := range cases {
		lead, trail := SavePlan(prev, c.hs, c.entries)
		if !same(lead, c.want.lead) || !same(trail, c.want.trail) {
			t.Errorf("%s: SavePlan = lead %+v trail %+v, want lead %+v trail %+v", c.name, lead, trail, c.want.lead, c.want.trail)
		}
	}
}

// TestReplayIsIdempotentAcrossReopens: reopening a log that needs no repair
// changes nothing on disk, and repeated reopens after a repair recover the same
// state every time — recovery never rewrites history.
func TestReplayIsIdempotentAcrossReopens(t *testing.T) {
	mem := fault.NewMemFS()
	l, _ := openMem(t, mem)
	if err := l.Save(&HardState{Term: 2, Vote: "n2", Commit: 2}, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	// Leave a torn record behind, as a crash mid-append would.
	f, _ := mem.OpenFile(memPath, os.O_WRONLY|os.O_APPEND, 0o644)
	_, _ = f.Write([]byte{7, 7, 7, 7, 7})
	_ = f.Close()
	var first []byte
	for i := 0; i < 4; i++ {
		l, rec, err := Open(memPath, Options{Sync: true, FS: mem})
		if err != nil {
			t.Fatalf("reopen %d: %v", i, err)
		}
		_ = l.Close()
		if rec.HardState.Term != 2 || rec.HardState.Commit != 2 || len(rec.Entries) != 2 {
			t.Fatalf("reopen %d recovered %+v", i, rec)
		}
		b, _ := mem.Cached(memPath)
		if first == nil {
			first = b // the first reopen repaired the tail
		} else if string(b) != string(first) {
			t.Fatalf("reopen %d changed the file: recovery must not rewrite a log that needs no repair", i)
		}
	}
	if !mem.FullySynced(memPath) {
		t.Fatal("the repaired log was left un-fsynced")
	}
}

// TestCorruptedFinalRecordIsTreatedAsTorn pins the documented policy for damage
// in the LAST record: with no bytes following it, a checksum failure is
// indistinguishable from an interrupted append, so it is truncated and the log
// opens with everything before it; the same damage with a record after it is
// mid-log corruption and refuses to open (TestMidCorruptionIsFatal).
func TestCorruptedFinalRecordIsTreatedAsTorn(t *testing.T) {
	path, l, _ := openTmp(t)
	if err := l.Save(&HardState{Term: 1, Commit: 1}, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Data: []byte("last")}}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff // inside the final record's payload
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	l2, rec, err := Open(path, Options{Sync: true})
	if err != nil {
		t.Fatalf("a corrupted FINAL record must be treated as a torn tail, got %v", err)
	}
	defer l2.Close()
	// The final record was the trailing HardState carrying the commit (written
	// after the entries): it is dropped whole, never partially applied, so the
	// recovered state is the leading term record over both entries, commit 0.
	if len(rec.Entries) != 2 || rec.HardState.Term != 1 || rec.HardState.Commit != 0 {
		t.Fatalf("recovered %+v, want both entries, term 1 and commit 0 (the damaged final record dropped whole)", rec)
	}
	if info, _ := os.Stat(path); info.Size() >= int64(len(data)) {
		t.Fatal("the damaged final record was not truncated away")
	}
}

// TestCommitBeyondRecoveredLogIsClamped: a HardState whose commit exceeds the
// entries actually recovered (possible only through damage or an interrupted
// Save; never through the record order) is clamped, never trusted (docs/DESIGN.md
// §8.1) — and a repeated HardState simply lets the last one win.
func TestCommitBeyondRecoveredLogIsClamped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := record.NewWriter(f)
	mustAppend := func(k record.Kind, p []byte) {
		t.Helper()
		if _, err := w.Append(k, p); err != nil {
			t.Fatal(err)
		}
	}
	mustAppend(kindHardState, encodeHardState(HardState{Term: 1, Vote: "x", Commit: 0}))
	mustAppend(kindEntry, encodeEntry(Entry{Index: 1, Term: 1}))
	mustAppend(kindEntry, encodeEntry(Entry{Index: 2, Term: 1}))
	mustAppend(kindHardState, encodeHardState(HardState{Term: 1, Vote: "x", Commit: 9})) // beyond the log
	mustAppend(kindHardState, encodeHardState(HardState{Term: 2, Vote: "y", Commit: 7})) // repeated: last wins
	f.Sync()
	f.Close()
	rec, err := Inspect(path)
	if err != nil {
		t.Fatal(err)
	}
	if rec.HardState.Term != 2 || rec.HardState.Vote != "y" {
		t.Fatalf("recovered HardState %+v, want the last one (term 2, vote y)", rec.HardState)
	}
	if rec.HardState.Commit != 2 {
		t.Fatalf("recovered commit %d, want it clamped to the 2 entries recovered", rec.HardState.Commit)
	}
}

// TestPartialRecordsFromInterruptedSaves generates torn artifacts by actually
// interrupting persistence — a short write cutting an Entry record and a
// HardState record at every byte offset — and reopens each: a partial record is
// dropped whole, everything before it survives, the repaired file is fsynced,
// and Inspect (read-only) agrees with Open about the recovered state.
func TestPartialRecordsFromInterruptedSaves(t *testing.T) {
	entryLen := len(encodeEntry(Entry{Index: 2, Term: 1, Data: []byte("payload")})) + record.HeaderSize
	hsLen := len(encodeHardState(HardState{Term: 1, Vote: "n1", Commit: 2})) + record.HeaderSize
	type cut struct {
		name  string
		nth   int // which write of the Save is torn
		short int
	}
	var cuts []cut
	for b := 0; b < entryLen; b++ {
		cuts = append(cuts, cut{fmt.Sprintf("entry@%d", b), 1, b})
	}
	for b := 0; b < hsLen; b++ {
		cuts = append(cuts, cut{fmt.Sprintf("hardstate@%d", b), 2, b})
	}
	for _, c := range cuts {
		mem := fault.NewMemFS()
		inj := fault.NewInjectFS(mem)
		l, _ := openMem(t, inj)
		if err := l.Save(&HardState{Term: 1, Vote: "n1", Commit: 1}, []Entry{{Index: 1, Term: 1}}); err != nil {
			t.Fatal(err)
		}
		// Same term and vote: the Save is the entry record then the HardState record.
		inj.Arm(fault.Injection{Op: fault.OpWrite, Path: memPath, Nth: c.nth, Short: c.short})
		if err := l.Save(&HardState{Term: 1, Vote: "n1", Commit: 2}, []Entry{{Index: 2, Term: 1, Data: []byte("payload")}}); err == nil {
			t.Fatalf("%s: the torn write did not fail the Save", c.name)
		}
		_ = l.Close()
		mem.CrashProcess()
		ins, err := InspectFS(mem, memPath)
		if err != nil {
			t.Fatalf("%s: Inspect: %v", c.name, err)
		}
		l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
		if err != nil {
			t.Fatalf("%s: reopen: %v", c.name, err)
		}
		_ = l2.Close()
		wantEntries, wantCommit := 1, uint64(1)
		if c.nth == 2 {
			wantEntries = 2 // the entry record completed; only the HardState is torn
		}
		if len(rec.Entries) != wantEntries || rec.HardState.Commit != wantCommit || rec.HardState.Term != 1 {
			t.Fatalf("%s: recovered %d entries commit %d term %d, want %d entries commit %d term 1", c.name, len(rec.Entries), rec.HardState.Commit, rec.HardState.Term, wantEntries, wantCommit)
		}
		if len(ins.Entries) != len(rec.Entries) || ins.HardState != rec.HardState {
			t.Fatalf("%s: Inspect recovered %+v but Open recovered %+v", c.name, ins, rec)
		}
		if !mem.FullySynced(memPath) {
			t.Fatalf("%s: the repaired log was not fsynced", c.name)
		}
	}
}

// TestCrashBeforeDirectorySyncLosesOnlyAFreshLog: the one crash window of a
// brand-new log — dying before the directory fsync that makes its creation
// durable (modelled as that fsync never completing: Open fails, nothing is
// returned to the node). A power loss then removes the file; the node boots
// fresh again, which is safe because nothing had ever been saved into it (Open
// syncs the directory before returning, so no Save can precede that fsync).
func TestCrashBeforeDirectorySyncLosesOnlyAFreshLog(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	inj.Arm(fault.Injection{Op: fault.OpSyncDir})
	if _, _, err := Open(memPath, Options{Sync: true, FS: inj}); !errors.Is(err, fault.ErrInjected) {
		t.Fatalf("Open = %v, want the failed directory sync", err)
	}
	mem.CrashPowerLoss(0)
	if _, err := mem.Stat(memPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the un-synced new file survived a power loss: %v", err)
	}
	l, rec, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if len(rec.Entries) != 0 || rec.HardState.Term != 0 {
		t.Fatalf("a fresh node recovered %+v", rec)
	}
}

// oldOrderSave is the pre-Phase-11 Save, kept ONLY as a test oracle: the entries,
// then the HardState, then the fsync. The production Save writes SavePlan's order.
func oldOrderSave(l *Log, hs *HardState, entries []Entry) error {
	for _, e := range entries {
		if _, err := l.w.Append(kindEntry, encodeEntry(e)); err != nil {
			return err
		}
	}
	if hs != nil {
		if _, err := l.w.Append(kindHardState, encodeHardState(*hs)); err != nil {
			return err
		}
	}
	return l.f.Sync()
}

// TestPrePhase11OrderLeftAnUnrecoverableLog is the permanent record of the bug the
// crash matrix found, reproduced deliberately: the old entries-first order, the
// process dying between the entry record and the HardState record of the one
// Save a single-node election is (term, self-vote, no-op). It proves three things
// in order — the old order really left the incoherent log; recovery does NOT
// repair it (the incoherence is visible, and the core refuses exactly this
// artifact: internal/raftnode's TestRecoverRefusesATermBelowItsLog); and the
// production order, dying at the same write, leaves a coherent log. The third
// part fails if the old order is ever restored (mutant
// term-durable-before-entries-of-that-term); the second fails if recovery ever
// starts inventing a term or dropping the entry to hide the damage.
func TestPrePhase11OrderLeftAnUnrecoverableLog(t *testing.T) {
	hs := &HardState{Term: 1, Vote: "n1", Commit: 1}
	entries := []Entry{{Index: 1, Term: 1}}

	// 1. The old order, dying before its second write — the HardState record.
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	l, _ := openMem(t, inj)
	crashBeforeWrite(mem, inj, 2)
	if err := oldOrderSave(l, hs, entries); !errors.Is(err, fault.ErrCrashed) {
		t.Fatalf("old-order Save = %v, want the crash before the HardState record", err)
	}
	// 2. What it left: an entry of term 1 under currentTerm 0. Recovery opens it
	// (it is not damage the log can judge) and reports it unchanged.
	l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatalf("Open of the old-order artifact: %v (recovery must report it, not refuse or repair it)", err)
	}
	_ = l2.Close()
	if len(rec.Entries) != 1 || rec.Entries[0].Term != 1 || rec.HardState.Term != 0 || rec.HardState.Vote != "" {
		t.Fatalf("old-order artifact recovered as %+v, want exactly the entry of term 1 under term 0 and no vote (no silent repair)", rec)
	}
	if last := rec.Entries[len(rec.Entries)-1].Term; last <= rec.HardState.Term {
		t.Fatalf("the old order did not reproduce the incoherence (entry term %d, currentTerm %d)", last, rec.HardState.Term)
	}

	// 3. The production order, dying at the very same write: coherent — the term
	// and vote landed, the entry did not, and nothing claims the entry's term.
	mem2 := fault.NewMemFS()
	inj2 := fault.NewInjectFS(mem2)
	l3, _ := openMem(t, inj2)
	crashBeforeWrite(mem2, inj2, 2)
	if err := l3.Save(hs, entries); !errors.Is(err, fault.ErrCrashed) {
		t.Fatalf("Save = %v, want the crash before write 2", err)
	}
	l4, rec2, err := Open(memPath, Options{Sync: true, FS: mem2})
	if err != nil {
		t.Fatalf("Open after the crash: %v", err)
	}
	_ = l4.Close()
	if n := len(rec2.Entries); n > 0 && rec2.Entries[n-1].Term > rec2.HardState.Term {
		t.Fatalf("the production order recovered an entry of term %d under currentTerm %d — the old bug is back",
			rec2.Entries[n-1].Term, rec2.HardState.Term)
	}
	if rec2.HardState.Term != 1 || rec2.HardState.Vote != "n1" || rec2.HardState.Commit != 0 || len(rec2.Entries) != 0 {
		t.Fatalf("recovered %+v, want term 1, vote n1, commit 0 and no entry", rec2)
	}
}
