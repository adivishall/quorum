package raft

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
)

// Configuration history across snapshots and compaction (docs/MEMBERSHIP.md
// §2, §6). Three things are distinct:
//
//   - the configuration a SNAPSHOT represents: the configuration in effect at
//     the snapshot's index, carried in the snapshot file;
//   - the configuration KNOWN AT A LOG INDEX (ConfAt): the latest
//     configuration entry at or below it, else the base configuration — but
//     only with evidence that the base holds there;
//   - the CURRENT configuration (Conf): the latest configuration entry in the
//     log, committed or not, else the base.
//
// The base configuration holds at its own index (the snapshot's; 0 for the
// genesis), which may lie ABOVE the log's compaction boundary — a recovered
// log keeps entries below its snapshot. Below the base's index the core knows
// the configuration only if no configuration entry lies between: unknown is
// an error, never a guess from the newest state.

// conf configurations: A the genesis, B with the learner n4, J the joint
// configuration promoting n4, F its final configuration.
func confsABJF(t *testing.T) (a, b, j, f Configuration) {
	t.Helper()
	a = voters("n1", "n2", "n3")
	b, err := ConfChange{Type: AddLearner, Member: Member{ID: "n4"}}.Apply(a)
	if err != nil {
		t.Fatal(err)
	}
	j, err = ConfChange{Type: Promote, Member: Member{ID: "n4"}}.Apply(b)
	if err != nil {
		t.Fatal(err)
	}
	return a, b, j, Final(j)
}

func normal(i, t uint64) Entry { return Entry{Index: i, Term: t, Data: []byte{byte(i)}} }
func confEntry(i, t uint64, c Configuration) Entry {
	return Entry{Index: i, Term: t, Type: replication.EntryConfig, Data: replication.EncodeConfiguration(c)}
}

// recovered builds a core as recovery does: a log compacted through boundary,
// holding entries after it, committed through commit, with base
// configuration base holding at baseIndex (the snapshot's index).
func recovered(t *testing.T, boundary uint64, entries []Entry, commit uint64, base Configuration, baseIndex uint64) (*Raft, error) {
	t.Helper()
	lg := replication.NewMemoryLog()
	if boundary > 0 {
		if err := lg.InstallSnapshot(boundary, 1); err != nil {
			t.Fatal(err)
		}
	}
	if len(entries) > 0 {
		if err := lg.Append(entries...); err != nil {
			t.Fatal(err)
		}
	}
	if commit > 0 {
		if err := lg.Commit(commit); err != nil {
			t.Fatal(err)
		}
	}
	return New(Config{ID: "n1", Conf: &base, ConfIndex: baseIndex, Log: lg, Rand: rand.New(rand.NewSource(1)), Term: 9})
}

func mustRecover(t *testing.T, boundary uint64, entries []Entry, commit uint64, base Configuration, baseIndex uint64) *Raft {
	t.Helper()
	r, err := recovered(t, boundary, entries, commit, base, baseIndex)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func wantConfAt(t *testing.T, r *Raft, index uint64, want Configuration) {
	t.Helper()
	got, err := r.ConfAt(index)
	if err != nil || !got.Equal(want) {
		t.Fatalf("ConfAt(%d) = %s, %v; want %s", index, got, err, want)
	}
}

func wantUnknown(t *testing.T, r *Raft, index uint64) {
	t.Helper()
	if got, err := r.ConfAt(index); !errors.Is(err, ErrConfUnknown) {
		t.Fatalf("ConfAt(%d) = %s, %v; want ErrConfUnknown", index, got, err)
	}
}

func TestConfAtAnswersOnlyWithEvidence(t *testing.T) {
	a, b, j, f := confsABJF(t)

	// A snapshot at 5 carries B, set by the entry at 4; the log was compacted
	// only through 2 (retain), so it still holds 3..6.
	retained := []Entry{normal(3, 1), confEntry(4, 1, b), normal(5, 1), normal(6, 1)}

	t.Run("1 index == the base's index", func(t *testing.T) {
		r := mustRecover(t, 2, retained, 6, b, 5)
		wantConfAt(t, r, 5, b)
	})
	t.Run("2 index > the base's index", func(t *testing.T) {
		r := mustRecover(t, 2, retained, 6, b, 5)
		wantConfAt(t, r, 6, b)
	})
	t.Run("3 index < the base's index", func(t *testing.T) {
		r := mustRecover(t, 2, retained, 6, b, 5)
		wantConfAt(t, r, 4, b) // the entry at 4 is the evidence
		wantUnknown(t, r, 3)   // the entry at 4 changed it; what came before is unknown
		wantUnknown(t, r, 2)   // the boundary itself
		if _, err := r.ConfAt(1); !errors.Is(err, replication.ErrOutOfRange) {
			t.Fatalf("ConfAt below the boundary: %v", err)
		}
		// Below the base's index with NO configuration entry in between, the
		// log itself is the evidence that nothing changed.
		plain := mustRecover(t, 2, []Entry{normal(3, 1), normal(4, 1), normal(5, 1)}, 5, a, 5)
		wantConfAt(t, plain, 2, a)
		wantConfAt(t, plain, 3, a)
	})
	t.Run("4 index == the snapshot's index", func(t *testing.T) {
		r := mustRecover(t, 5, []Entry{normal(6, 1)}, 6, b, 5)
		wantConfAt(t, r, 5, b)
	})
	t.Run("5 snapshot index > the index its configuration was set at", func(t *testing.T) {
		// The snapshot at 5 carries B, set at 4; compacted through 5.
		r := mustRecover(t, 5, []Entry{normal(6, 1)}, 6, b, 5)
		wantConfAt(t, r, 6, b)
		if c, idx := r.Conf(); !c.Equal(b) || idx != 0 {
			t.Fatalf("current configuration %s at %d", c, idx)
		}
	})
	t.Run("6 snapshot index == the index its configuration was set at", func(t *testing.T) {
		r := mustRecover(t, 2, []Entry{normal(3, 1), confEntry(4, 1, b)}, 4, b, 4)
		wantConfAt(t, r, 4, b)
		wantUnknown(t, r, 3)
	})
	t.Run("7 snapshot recovery with a suffix", func(t *testing.T) {
		r := mustRecover(t, 5, []Entry{normal(6, 1), normal(7, 1)}, 5, b, 5)
		wantConfAt(t, r, 7, b)
		if c, _ := r.Conf(); !c.Equal(b) || r.ConfPending() {
			t.Fatalf("current %s pending=%v", c, r.ConfPending())
		}
	})
	t.Run("8 a suffix holding a configuration change", func(t *testing.T) {
		r := mustRecover(t, 5, []Entry{normal(6, 1), confEntry(7, 1, j), normal(8, 1)}, 6, b, 5)
		wantConfAt(t, r, 5, b)
		wantConfAt(t, r, 6, b)
		wantConfAt(t, r, 7, j)
		wantConfAt(t, r, 8, j)
		if c, idx := r.Conf(); !c.Equal(j) || idx != 7 || !r.ConfPending() {
			t.Fatalf("current %s at %d pending=%v", c, idx, r.ConfPending())
		}
	})
	t.Run("9 a joint configuration spanning the snapshot's boundary", func(t *testing.T) {
		// The snapshot at 8 carries J (set at 7); its final entry follows it.
		r := mustRecover(t, 8, []Entry{confEntry(9, 1, f), normal(10, 1)}, 8, j, 8)
		wantConfAt(t, r, 8, j)
		wantConfAt(t, r, 9, f)
		if c, idx := r.Conf(); !c.Equal(f) || idx != 9 || !r.ConfPending() {
			t.Fatalf("current %s at %d pending=%v", c, idx, r.ConfPending())
		}
		// Without the final entry, the node is still joint — and a leader of
		// it completes the transition (TestNewLeaderCompletesAnInterruptedTransition).
		r2 := mustRecover(t, 8, []Entry{normal(9, 1)}, 9, j, 8)
		if c, _ := r2.Conf(); !c.Equal(j) || !r2.ConfPending() {
			t.Fatalf("without the final entry: %s pending=%v", c, r2.ConfPending())
		}
		// The same history reaching a follower by an INSTALL: its base holds
		// at the snapshot's index, and the suffix follows.
		fol := mustRecover(t, 0, []Entry{normal(1, 1)}, 1, a, 0)
		if err := fol.Step(Message{Type: MsgSnapshot, From: "n2", To: "n1", Term: 9, SnapshotIndex: 8, SnapshotTerm: 1, Conf: &j}); err != nil {
			t.Fatal(err)
		}
		fol.Ready()
		fol.Advance()
		wantConfAt(t, fol, 8, j)
		if _, err := fol.ConfAt(7); !errors.Is(err, replication.ErrOutOfRange) {
			t.Fatalf("below an installed snapshot: %v", err)
		}
		if err := fol.Step(Message{Type: MsgAppendRequest, From: "n2", To: "n1", Term: 9, PrevLogIndex: 8, PrevLogTerm: 1,
			Entries: []Entry{confEntry(9, 9, f)}}); err != nil {
			t.Fatal(err)
		}
		fol.Ready()
		fol.Advance()
		wantConfAt(t, fol, 9, f)
		wantConfAt(t, fol, 8, j)
	})
	t.Run("10 a joiner that knows no configuration yet", func(t *testing.T) {
		empty := Configuration{}
		r, err := New(Config{ID: "n4", Conf: &empty, Log: replication.NewMemoryLog(), Rand: rand.New(rand.NewSource(1))})
		if err != nil {
			t.Fatal(err)
		}
		wantUnknown(t, r, 0)
		if err := r.Step(Message{Type: MsgAppendRequest, From: "n1", To: "n4", Term: 2, LeaderCommit: 2,
			Entries: []Entry{normal(1, 2), confEntry(2, 2, b)}}); err != nil {
			t.Fatal(err)
		}
		r.Ready()
		r.Advance()
		wantUnknown(t, r, 1)
		wantConfAt(t, r, 2, b)
	})
	t.Run("11 voterless and invalid configurations are refused", func(t *testing.T) {
		learnersOnly := Configuration{Learners: members("n4")}
		if _, err := recovered(t, 0, nil, 0, learnersOnly, 0); !errors.Is(err, replication.ErrInvalidConfiguration) {
			t.Fatalf("a voterless base: %v", err)
		}
		if _, err := recovered(t, 5, []Entry{normal(6, 1)}, 6, Configuration{}, 5); !errors.Is(err, replication.ErrInvalidConfiguration) {
			t.Fatalf("an empty base claimed at a snapshot's index: %v", err)
		}
		if _, err := recovered(t, 0, []Entry{normal(1, 1), confEntry(2, 1, learnersOnly)}, 1, a, 0); !errors.Is(err, replication.ErrInvalidConfiguration) {
			t.Fatalf("a voterless configuration entry in the log: %v", err)
		}
		bad := Configuration{Voters: members("n2", "n1")} // unsorted
		if _, err := recovered(t, 0, nil, 0, bad, 0); !errors.Is(err, replication.ErrInvalidConfiguration) {
			t.Fatalf("an invalid base: %v", err)
		}
		// On the wire: a message carrying a voterless configuration entry
		// does not decode.
		wire := Message{Type: MsgAppendRequest, From: "n1", To: "n2", Term: 1, Entries: []Entry{confEntry(1, 1, learnersOnly)}}.Marshal()
		if _, err := Unmarshal(wire); !errors.Is(err, ErrMalformedMessage) {
			t.Fatalf("a voterless configuration entry decoded: %v", err)
		}
	})
	t.Run("12 conflicting configuration metadata is refused", func(t *testing.T) {
		// The snapshot claims A at 5, the log's entry at 4 says B.
		if _, err := recovered(t, 2, retained, 6, a, 5); !errors.Is(err, ErrConfMismatch) {
			t.Fatalf("a base contradicting the log: %v", err)
		}
		// A base claimed past the log's end.
		if _, err := recovered(t, 2, retained, 6, b, 9); !errors.Is(err, ErrConfMismatch) {
			t.Fatalf("a base past the log: %v", err)
		}
		// An entry ABOVE the base's index may differ: it is a later change.
		if _, err := recovered(t, 2, retained, 6, a, 3); err != nil {
			t.Fatalf("a later change above the base's index: %v", err)
		}
	})
}
