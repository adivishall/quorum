package raftlog

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
)

// TestSaveAndReplayAtTheEntryLimit: an entry of exactly MaxEntryDataLen bytes
// is persisted and replays intact — the bound Save writes by is the bound
// replay reads by (C1, docs/RAFT.md §16).
func TestSaveAndReplayAtTheEntryLimit(t *testing.T) {
	path, l, _ := openTmp(t)
	data := bytes.Repeat([]byte{'v'}, MaxEntryDataLen)
	if err := l.Save(&HardState{Term: 1, Vote: "n1", Commit: 1}, []Entry{{Index: 1, Term: 1, Data: data}}); err != nil {
		t.Fatalf("Save(entry of %d bytes) = %v, want nil", len(data), err)
	}
	l.Close()
	l2, rec := reopen(t, path)
	defer l2.Close()
	if len(rec.Entries) != 1 || !bytes.Equal(rec.Entries[0].Data, data) {
		t.Fatalf("replayed %d entries; want the one %d-byte entry", len(rec.Entries), len(data))
	}
}

// TestSaveRefusesAnEntryOverTheLimit: a Save carrying an entry one byte over
// the limit writes nothing — not even the HardState or the entries before it in
// the same Save — fails the Log, and leaves a file that reopens to exactly the
// state before that Save. Before this rule the leader persisted such an entry
// and could then never restart ("corrupt log: length … out of range").
func TestSaveRefusesAnEntryOverTheLimit(t *testing.T) {
	path, l, _ := openTmp(t)
	if err := l.Save(&HardState{Term: 1, Vote: "n1", Commit: 1}, []Entry{{Index: 1, Term: 1, Data: []byte("ok")}}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	err = l.Save(&HardState{Term: 2, Vote: "n2", Commit: 1}, []Entry{
		{Index: 2, Term: 2, Data: []byte("fits")},
		{Index: 3, Term: 2, Data: bytes.Repeat([]byte{'v'}, MaxEntryDataLen+1)},
	})
	if !errors.Is(err, replication.ErrEntryTooLarge) || !errors.Is(err, ErrFailed) {
		t.Fatalf("Save(oversized entry) = %v, want ErrEntryTooLarge and ErrFailed", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("the refused Save wrote %d bytes to the log", len(after)-len(before))
	}
	// The failure latches (INV-F1): no later Save writes after the refused one.
	if err := l.Save(&HardState{Term: 2, Vote: "n2", Commit: 1}, []Entry{{Index: 2, Term: 2, Data: []byte("x")}}); !errors.Is(err, ErrFailed) {
		t.Fatalf("Save after the refusal = %v, want ErrFailed", err)
	}
	l.Close()
	l2, rec := reopen(t, path)
	defer l2.Close()
	if len(rec.Entries) != 1 || string(rec.Entries[0].Data) != "ok" || rec.HardState.Term != 1 || rec.HardState.Vote != "n1" {
		t.Fatalf("reopened to %+v; want the state before the refused Save", rec)
	}
}
