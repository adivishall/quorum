package raftlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/replication"
)

// Phase 15: configuration entries in the durable log (docs/MEMBERSHIP.md §2).
// A configuration entry is a typed record; a normal entry keeps its Phase 9
// encoding; recovery refuses an undecodable configuration and an unknown type.

func confEntry(index, term uint64, ids ...replication.NodeID) Entry {
	return Entry{Index: index, Term: term, Type: replication.EntryConfig, Data: replication.EncodeConfiguration(replication.VotersOf(ids))}
}

// TestConfigurationEntriesSurviveReopenAndCompaction: typed entries round-trip
// with their type, through a reopen and through a compaction's rewrite, and a
// normal entry is still written as the Phase 9 record kind.
func TestConfigurationEntriesSurviveReopenAndCompaction(t *testing.T) {
	path, l, _ := openTmp(t)
	entries := []Entry{
		{Index: 1, Term: 1, Data: nil},
		confEntry(2, 1, "n1", "n2", "n3"),
		{Index: 3, Term: 1, Data: []byte("x")},
		confEntry(4, 1, "n1", "n2", "n3", "n4"),
		{Index: 5, Term: 1, Data: []byte("y")},
	}
	if err := l.Save(&HardState{Term: 1, Vote: "n1", Commit: 5}, entries); err != nil {
		t.Fatal(err)
	}
	l.Close()
	l2, rec := reopen(t, path)
	check := func(rec *Recovered, from int) {
		t.Helper()
		for i, want := range entries[from:] {
			got := rec.Entries[i]
			if got.Index != want.Index || got.Type != want.Type || string(got.Data) != string(want.Data) {
				t.Fatalf("entry %d recovered as %+v, want %+v", want.Index, got, want)
			}
		}
	}
	check(rec, 0)
	if _, err := replication.DecodeConfiguration(rec.Entries[3].Data); err != nil {
		t.Fatalf("the recovered configuration entry does not decode: %v", err)
	}
	// The compaction rewrite keeps the types of the entries it carries over.
	if err := l2.Compact(1, 1); err != nil {
		t.Fatal(err)
	}
	l2.Close()
	l3, rec := reopen(t, path)
	defer l3.Close()
	if rec.Boundary.Index != 1 || len(rec.Entries) != 4 {
		t.Fatalf("after compaction: boundary %d, %d entries", rec.Boundary.Index, len(rec.Entries))
	}
	check(rec, 1)
	// A normal entry is still the Phase 9 record; a configuration entry is the
	// typed record.
	if entryKind(entries[0]) != kindEntry || entryKind(entries[1]) != kindEntryTyped {
		t.Fatal("record kinds")
	}
}

// TestUndecodableConfigurationEntryIsCorruption: a typed record whose
// configuration does not decode, a typed record of an unknown type, and a
// typed record claiming to be a normal entry all refuse to open.
func TestUndecodableConfigurationEntryIsCorruption(t *testing.T) {
	for name, payload := range map[string][]byte{
		"undecodable configuration": append([]byte{byte(replication.EntryConfig)}, encodeEntry(Entry{Index: 1, Term: 1, Data: []byte{0xff, 0xff}})...),
		"unknown type":              append([]byte{9}, encodeEntry(Entry{Index: 1, Term: 1, Data: []byte("x")})...),
		"normal as typed":           append([]byte{byte(replication.EntryNormal)}, encodeEntry(Entry{Index: 1, Term: 1, Data: []byte("x")})...),
		"empty":                     {},
	} {
		path := filepath.Join(t.TempDir(), "raft.log")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		w := record.NewWriter(f)
		if _, err := w.Append(kindEntryTyped, payload); err != nil {
			t.Fatal(err)
		}
		// Bytes after it, so it is never a torn tail.
		if _, err := w.Append(kindHardState, encodeHardState(HardState{Term: 1})); err != nil {
			t.Fatal(err)
		}
		f.Sync()
		f.Close()
		if _, _, err := Open(path, Options{Sync: true}); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
}

// FuzzDecodeTypedEntry: the typed-entry decoder is total.
func FuzzDecodeTypedEntry(f *testing.F) {
	f.Add(encodeTypedEntry(confEntry(1, 1, "a", "b")))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = decodeTypedEntry(data) })
}
