package raftnode

import (
	"bytes"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/snapshot"
)

// TestARestoreReadsOnlyTheSnapshotItWasAskedFor: the state machine is restored
// from the published file only if that file IS the snapshot the caller names
// (S4: the restore streams the published file, so a file other than the one
// the core installed — or the one recovery validated — is refused, never
// restored at the wrong index or term).
func TestARestoreReadsOnlyTheSnapshotItWasAskedFor(t *testing.T) {
	files := snapshot.Files{FS: fault.NewMemFS(), Base: "/n/raft.log"}
	conf := replication.VotersOf([]NodeID{"n0", "n1", "n2"})
	sm := &snapSM{}
	_ = sm.Apply(1, []byte("c1"))
	_, data, _ := sm.EncodeSnapshot()
	if err := files.PublishFrom(snapshot.Meta{Conf: conf, Index: 1, Term: 2}, snapshot.BytesSource(data)); err != nil {
		t.Fatal(err)
	}
	fresh := &snapSM{}
	for _, at := range [][2]uint64{{1, 3}, {2, 2}} {
		if err := restorePublished(files, fresh, at[0], at[1]); err == nil {
			t.Fatalf("the published snapshot (1, 2) restored as (%d, %d)", at[0], at[1])
		}
	}
	if idx, _, restores := fresh.state(); idx != 0 || restores != 0 {
		t.Fatalf("a refused restore changed the machine: index %d, %d restores", idx, restores)
	}
	if err := restorePublished(files, fresh, 1, 2); err != nil {
		t.Fatal(err)
	}
	if idx, cmds, _ := fresh.state(); idx != 1 || !bytes.Equal([]byte(cmds[0]), []byte("c1")) {
		t.Fatalf("restored %d %v", idx, cmds)
	}
}
