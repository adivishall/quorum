package raftlog

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSizeIsTheFileLength (Phase 16, docs/OBSERVABILITY.md): Size is the log
// file's length on disk after Open, after every Save, after an Install's
// boundary record, after a compaction's rewrite, and after reopening — never an
// estimate.
func TestSizeIsTheFileLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")
	onDisk := func() int64 {
		t.Helper()
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return info.Size()
	}
	check := func(l *Log, when string) {
		t.Helper()
		if got, want := l.Size(), onDisk(); got != want {
			t.Fatalf("%s: Size %d, the file holds %d bytes", when, got, want)
		}
	}
	l, _, err := Open(path, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	check(l, "a new log")
	if l.Size() != 0 {
		t.Fatalf("a new log is %d bytes", l.Size())
	}
	mustSave(t, l, &HardState{Term: 1, Vote: "a"})
	check(l, "a HardState")
	mustSave(t, l, &HardState{Term: 1, Vote: "a", Commit: 20}, ents(1, 20, 1)...)
	check(l, "twenty entries")
	grown := l.Size()
	if err := l.Compact(15, 1); err != nil {
		t.Fatal(err)
	}
	check(l, "after compaction")
	if l.Size() >= grown {
		t.Fatalf("compaction to 15 of 20 left %d bytes of %d", l.Size(), grown)
	}
	mustSave(t, l, nil, ents(21, 22, 1)...)
	check(l, "appending after the rewrite")
	mustSave(t, l, &HardState{Term: 2, Vote: "b", Commit: 22})
	if err := l.Install(40, 2); err != nil {
		t.Fatal(err)
	}
	check(l, "an installed boundary")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, _, err = Open(path, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	check(l, "reopened")
}
