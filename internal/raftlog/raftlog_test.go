package raftlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/record"
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
