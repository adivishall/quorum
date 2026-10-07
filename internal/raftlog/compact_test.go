package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/vfs"
)

// Phase 14: the boundary record, Install and Compact (docs/SNAPSHOTS.md §5, §6, §8).

func ents(from, to, term uint64) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("e%d.%d", i, term))})
	}
	return out
}

func mustSave(t *testing.T, l *Log, hs *HardState, es ...Entry) {
	t.Helper()
	if err := l.Save(hs, es); err != nil {
		t.Fatal(err)
	}
}

// logical is what a recovered log means: its boundary, entries and HardState.
type logical struct {
	b  Boundary
	es []Entry
	hs HardState
}

func (m logical) last() uint64 { return m.b.Index + uint64(len(m.es)) }

func (m logical) term(i uint64) uint64 {
	if i == m.b.Index {
		return m.b.Term
	}
	return m.es[i-m.b.Index-1].Term
}

// commit is the commit replay reports: clamped to the log, raised to the boundary.
func (m logical) commit() uint64 { return max(min(m.hs.Commit, m.last()), m.b.Index) }

func requireRecovered(t *testing.T, rec *Recovered, want logical, context string) {
	t.Helper()
	if err := sameRecovered(rec, want); err != nil {
		t.Fatalf("%s: %v", context, err)
	}
}

func sameRecovered(rec *Recovered, want logical) error {
	if rec.Boundary != want.b {
		return fmt.Errorf("boundary %+v, want %+v", rec.Boundary, want.b)
	}
	if len(rec.Entries) != len(want.es) {
		return fmt.Errorf("%d entries after the boundary, want %d", len(rec.Entries), len(want.es))
	}
	for i := range rec.Entries {
		a, b := rec.Entries[i], want.es[i]
		if a.Index != b.Index || a.Term != b.Term || !bytes.Equal(a.Data, b.Data) {
			return fmt.Errorf("entry %d is (%d,%d), want (%d,%d)", i, a.Index, a.Term, b.Index, b.Term)
		}
	}
	if rec.HardState.Term != want.hs.Term || rec.HardState.Vote != want.hs.Vote || rec.HardState.Commit != want.commit() {
		return fmt.Errorf("hardstate %+v, want term %d vote %q commit %d", rec.HardState, want.hs.Term, want.hs.Vote, want.commit())
	}
	return nil
}

// TestCompactKeepsOnlyTheSuffix: after Compact the file holds the boundary, the
// HardState and the entries after the boundary — it shrinks — and reopening
// recovers exactly that; later appends extend it normally.
func TestCompactKeepsOnlyTheSuffix(t *testing.T) {
	mem := fault.NewMemFS()
	l, _ := openMem(t, mem)
	mustSave(t, l, &HardState{Term: 1, Vote: "n1"}, ents(1, 6, 1)...)
	mustSave(t, l, &HardState{Term: 2, Vote: "n2", Commit: 9}, ents(7, 12, 2)...)
	before, _ := mem.Cached(memPath)
	if err := l.Compact(8, 2); err != nil {
		t.Fatal(err)
	}
	after, _ := mem.Cached(memPath)
	if len(after) >= len(before) {
		t.Fatalf("compaction did not shrink the log: %d -> %d bytes", len(before), len(after))
	}
	if l.Boundary() != (Boundary{8, 2}) {
		t.Fatalf("boundary %+v", l.Boundary())
	}
	want := logical{b: Boundary{8, 2}, es: ents(9, 12, 2), hs: HardState{Term: 2, Vote: "n2", Commit: 9}}
	rec, err := InspectFS(mem, memPath)
	if err != nil {
		t.Fatal(err)
	}
	requireRecovered(t, rec, want, "inspect after compact")

	// The compacted log keeps working: appends, a second compaction, a power loss.
	mustSave(t, l, &HardState{Term: 2, Vote: "n2", Commit: 13}, ents(13, 14, 2)...)
	if err := l.Compact(13, 2); err != nil {
		t.Fatal(err)
	}
	if err := l.Compact(13, 2); err != nil {
		t.Fatalf("compacting to the current boundary: %v", err)
	}
	mem.CrashPowerLoss(0)
	l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	requireRecovered(t, rec, logical{b: Boundary{13, 2}, es: ents(14, 14, 2), hs: HardState{Term: 2, Vote: "n2", Commit: 13}}, "after a power loss")
}

// TestInstallAppliesTheInstallRule: a Boundary record keeps the entries after
// the snapshot only when the log holds the snapshot's index with its term;
// otherwise the whole log goes (Raft §7). Either way the commit becomes the
// snapshot's index.
func TestInstallAppliesTheInstallRule(t *testing.T) {
	for _, tc := range []struct {
		name        string
		index, term uint64
		kept        []Entry
	}{
		{"holds the snapshot entry: keep the suffix", 7, 1, ents(8, 10, 1)},
		{"holds another term there: discard", 7, 2, nil},
		{"does not reach it: discard", 15, 2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := fault.NewMemFS()
			l, _ := openMem(t, mem)
			mustSave(t, l, &HardState{Term: 1, Vote: "n1", Commit: 3}, ents(1, 10, 1)...)
			// The driver makes the snapshot's term durable first.
			mustSave(t, l, &HardState{Term: 2, Vote: "", Commit: 3})
			if err := l.Install(tc.index, tc.term); err != nil {
				t.Fatal(err)
			}
			want := logical{b: Boundary{tc.index, tc.term}, es: tc.kept, hs: HardState{Term: 2, Commit: 3}}
			rec, err := InspectFS(mem, memPath)
			if err != nil {
				t.Fatal(err)
			}
			requireRecovered(t, rec, want, "after install")
			if rec.HardState.Commit != tc.index {
				t.Fatalf("commit %d, want the snapshot index %d", rec.HardState.Commit, tc.index)
			}
			// Replication resumes after the snapshot, and survives a reopen.
			next := want.last() + 1
			mustSave(t, l, &HardState{Term: 2, Commit: next}, ents(next, next+1, 2)...)
			l.Close()
			_, rec = reopenMem(t, mem)
			want.es = append(want.es, ents(next, next+1, 2)...)
			want.hs.Commit = next
			requireRecovered(t, rec, want, "after appends and a reopen")
		})
	}
}

func reopenMem(t *testing.T, fsys vfs.FS) (*Log, *Recovered) {
	t.Helper()
	return openMem(t, fsys)
}

// TestCompactAndInstallRefuseImpossibleBoundaries: a compaction or install the
// log's own state contradicts is refused with ErrBoundary, writes nothing, and
// leaves the Log healthy.
func TestCompactAndInstallRefuseImpossibleBoundaries(t *testing.T) {
	mem := fault.NewMemFS()
	l, _ := openMem(t, mem)
	mustSave(t, l, &HardState{Term: 2, Vote: "n1", Commit: 6}, append(ents(1, 5, 1), ents(6, 10, 2)...)...)
	if err := l.Compact(3, 1); err != nil {
		t.Fatal(err)
	}
	image, _ := mem.Cached(memPath)
	for name, try := range map[string]func() error{
		"compact below the boundary":        func() error { return l.Compact(2, 1) },
		"compact at the boundary, new term": func() error { return l.Compact(3, 2) },
		"compact past the durable commit":   func() error { return l.Compact(7, 2) },
		"compact past the log":              func() error { return l.Compact(11, 2) },
		"compact with the wrong term":       func() error { return l.Compact(5, 2) },
		"install above the durable term":    func() error { return l.Install(12, 3) },
		"install at the boundary":           func() error { return l.Install(3, 1) },
		"install within the durable commit": func() error { return l.Install(6, 2) },
		"install at index 0":                func() error { return l.Install(0, 1) },
		"install with term 0":               func() error { return l.Install(12, 0) },
	} {
		if err := try(); !errors.Is(err, ErrBoundary) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if l.Err() != nil {
		t.Fatalf("a refused boundary failed the log: %v", l.Err())
	}
	if now, _ := mem.Cached(memPath); !bytes.Equal(now, image) {
		t.Fatal("a refused boundary changed the file")
	}
}

// TestReplayRefusesImpossibleBoundaryRecords: a boundary record no sequence of
// Install and Compact writes is corruption, not something to repair.
func TestReplayRefusesImpossibleBoundaryRecords(t *testing.T) {
	type rec struct {
		k record.Kind
		p []byte
	}
	bnd := func(i, term uint64) rec { return rec{kindBoundary, encodeBoundary(Boundary{i, term})} }
	ent := func(i, term uint64) rec { return rec{kindEntry, encodeEntry(Entry{Index: i, Term: term})} }
	hs := func(term, commit uint64) rec {
		return rec{kindHardState, encodeHardState(HardState{Term: term, Commit: commit})}
	}
	for name, file := range map[string][]rec{
		"boundary moves back":               {bnd(5, 1), bnd(4, 1)},
		"boundary repeated with a new term": {bnd(5, 1), bnd(5, 2)},
		"boundary replaces a committed entry": {
			hs(2, 0), ent(1, 1), ent(2, 1), ent(3, 1), hs(2, 3), bnd(2, 2)},
		"entry at the boundary":           {bnd(5, 1), ent(5, 1)},
		"entry below the boundary":        {bnd(5, 1), ent(3, 1)},
		"entry past the boundary's gap":   {bnd(5, 1), ent(7, 1)},
		"boundary at index 0":             {{kindBoundary, encodeBoundary(Boundary{0, 1})}},
		"boundary with term 0":            {{kindBoundary, encodeBoundary(Boundary{4, 0})}},
		"boundary payload trailing bytes": {{kindBoundary, append(encodeBoundary(Boundary{4, 1}), 0)}},
		"boundary payload truncated":      {{kindBoundary, []byte{4}}},
	} {
		var b []byte
		for _, r := range file {
			var err error
			if b, err = record.Encode(b, r.k, r.p); err != nil {
				t.Fatal(err)
			}
		}
		mem := fault.NewMemFS()
		writeFile(t, mem, memPath, b)
		if _, err := InspectFS(mem, memPath); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
		if _, _, err := Open(memPath, Options{Sync: true, FS: mem}); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s (open): %v", name, err)
		}
	}
	// And the legitimate forms replay: a repeated identical boundary, a boundary
	// at an uncommitted conflicting entry, a boundary before any entry.
	for name, file := range map[string][]rec{
		"repeated identical boundary":   {bnd(5, 1), bnd(5, 1), ent(6, 1)},
		"replaces an uncommitted entry": {hs(2, 1), ent(1, 1), ent(2, 1), bnd(2, 2), ent(3, 2)},
		"a compacted file":              {bnd(9, 3), hs(3, 9), ent(10, 3)},
	} {
		var b []byte
		for _, r := range file {
			b, _ = record.Encode(b, r.k, r.p)
		}
		mem := fault.NewMemFS()
		writeFile(t, mem, memPath, b)
		if _, err := InspectFS(mem, memPath); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func writeFile(t *testing.T, fsys vfs.FS, path string, b []byte) {
	t.Helper()
	f, err := fsys.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

var errDied = errors.New("the process died at the crash point")

// untilDeath runs op until a crash point kills "the process" (panics errDied).
func untilDeath(op func() error) (err error, died bool) {
	defer func() {
		if v := recover(); v != nil {
			if v != errDied {
				panic(v)
			}
			died = true
		}
	}()
	return op(), false
}

type ioOp struct {
	kind fault.Op
	path string
	nth  int
}

// opsOf lists the I/O operations do performs, each with its occurrence number.
func opsOf(recs []fault.OpRecord) []ioOp {
	var out []ioOp
	seen := map[[2]any]int{}
	for _, r := range recs {
		k := [2]any{r.Op, r.Path}
		seen[k]++
		out = append(out, ioOp{r.Op, r.Path, seen[k]})
	}
	return out
}

// TestCompactIsAtomicUnderEveryCrash: a process crash or a power loss before
// every I/O operation of a compaction leaves a log that recovers to either the
// whole old log or the whole compacted one — same HardState, same entries after
// the boundary — never a mixture, never nothing; the orphaned temporary is
// removed on Open; and the reopened log's appends are durable. The operations
// are enumerated from a clean run, so a change to Compact's I/O is covered.
func TestCompactIsAtomicUnderEveryCrash(t *testing.T) {
	hs := HardState{Term: 2, Vote: "n1", Commit: 9}
	setup := func(fsys vfs.FS) *Log {
		l, _, err := Open(memPath, Options{Sync: true, FS: fsys})
		if err != nil {
			t.Fatal(err)
		}
		mustSave(t, l, &HardState{Term: 1, Vote: "n1"}, ents(1, 5, 1)...)
		mustSave(t, l, &hs, ents(6, 12, 2)...)
		return l
	}
	oldLog := logical{es: append(ents(1, 5, 1), ents(6, 12, 2)...), hs: hs}
	newLog := logical{b: Boundary{7, 2}, es: ents(8, 12, 2), hs: hs}

	clean := fault.NewInjectFS(fault.NewMemFS())
	l := setup(clean)
	before := len(clean.Ops())
	if err := l.Compact(7, 2); err != nil {
		t.Fatal(err)
	}
	ops := opsOf(clean.Ops()[before:])
	if len(ops) < 5 {
		t.Fatalf("compaction performed only %v", ops)
	}
	outcomes := map[string]int{}
	for _, o := range ops {
		for _, mode := range []string{"process", "power"} {
			mem := fault.NewMemFS()
			inj := fault.NewInjectFS(mem)
			l := setup(inj)
			inj.Arm(fault.Injection{Op: o.kind, Path: o.path, Nth: o.nth, At: func() {
				if mode == "power" {
					mem.CrashPowerLoss(0)
				} else {
					mem.CrashProcess()
				}
				panic(errDied)
			}})
			if _, died := untilDeath(func() error { return l.Compact(7, 2) }); !died {
				t.Fatalf("%s#%d of %s never reached", o.kind, o.nth, o.path)
			}
			l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
			if err != nil {
				t.Fatalf("crash (%s) before %s#%d: reopen: %v", mode, o.kind, o.nth, err)
			}
			outcome := "old"
			if sameRecovered(rec, oldLog) != nil {
				outcome = "new"
				requireRecovered(t, rec, newLog, fmt.Sprintf("crash (%s) before %s#%d of %s", mode, o.kind, o.nth, o.path))
			}
			outcomes[mode+":"+outcome]++
			if _, ok := mem.Cached(TmpPath(memPath)); ok {
				t.Fatalf("crash (%s) before %s#%d: the temporary survived Open", mode, o.kind, o.nth)
			}
			mustSave(t, l2, nil, ents(13, 13, 2)...)
			mem.CrashPowerLoss(0)
			rec, err = InspectFS(mem, memPath)
			if err != nil || rec.LastIndex() != 13 {
				t.Fatalf("crash (%s) before %s#%d: an append after recovery was lost (%v)", mode, o.kind, o.nth, err)
			}
		}
	}
	if outcomes["process:old"] == 0 || outcomes["process:new"] == 0 || outcomes["power:old"] == 0 {
		t.Fatalf("outcomes %v: the crash points do not straddle the rename", outcomes)
	}
	t.Logf("%d crash points x 2 modes: %v", len(ops), outcomes)
}

// TestCompactFailureFailsTheLog: an I/O failure at any operation of a
// compaction fails the Log (INV-F1) — no later Save touches the disk — and a
// fresh Open recovers the old log or the compacted one, whole.
func TestCompactFailureFailsTheLog(t *testing.T) {
	setup := func(fsys vfs.FS) *Log {
		l, _, err := Open(memPath, Options{Sync: true, FS: fsys})
		if err != nil {
			t.Fatal(err)
		}
		mustSave(t, l, &HardState{Term: 1, Vote: "n1", Commit: 6}, ents(1, 8, 1)...)
		return l
	}
	oldLog := logical{es: ents(1, 8, 1), hs: HardState{Term: 1, Vote: "n1", Commit: 6}}
	newLog := logical{b: Boundary{4, 1}, es: ents(5, 8, 1), hs: oldLog.hs}
	clean := fault.NewInjectFS(fault.NewMemFS())
	l := setup(clean)
	before := len(clean.Ops())
	if err := l.Compact(4, 1); err != nil {
		t.Fatal(err)
	}
	for _, o := range opsOf(clean.Ops()[before:]) {
		mem := fault.NewMemFS()
		inj := fault.NewInjectFS(mem)
		l := setup(inj)
		inj.Arm(fault.Injection{Op: o.kind, Path: o.path, Nth: o.nth})
		if err := l.Compact(4, 1); !errors.Is(err, ErrFailed) || !errors.Is(err, fault.ErrInjected) {
			t.Fatalf("%s#%d failed: Compact returned %v", o.kind, o.nth, err)
		}
		n := len(inj.Ops())
		if err := l.Save(nil, ents(9, 9, 1)); !errors.Is(err, ErrFailed) {
			t.Fatalf("%s#%d failed: a later Save returned %v", o.kind, o.nth, err)
		}
		if len(inj.Ops()) != n {
			t.Fatalf("%s#%d failed: a failed log still touched the disk", o.kind, o.nth)
		}
		mem.CrashProcess()
		_, rec, err := Open(memPath, Options{Sync: true, FS: mem})
		if err != nil {
			t.Fatalf("%s#%d failed: reopen: %v", o.kind, o.nth, err)
		}
		if sameRecovered(rec, oldLog) != nil {
			requireRecovered(t, rec, newLog, fmt.Sprintf("%s#%d failed", o.kind, o.nth))
		}
	}
}

// TestInstallSurvivesEveryCrash: a crash before the boundary record's write or
// fsync, or a boundary record torn at any byte, recovers the log as it was
// before the install or as installed — never refused.
func TestInstallSurvivesEveryCrash(t *testing.T) {
	setup := func(fsys vfs.FS) *Log {
		l, _, err := Open(memPath, Options{Sync: true, FS: fsys})
		if err != nil {
			t.Fatal(err)
		}
		mustSave(t, l, &HardState{Term: 1, Vote: "n1", Commit: 2}, ents(1, 6, 1)...)
		mustSave(t, l, &HardState{Term: 3, Vote: "n3", Commit: 2})
		return l
	}
	hs := HardState{Term: 3, Vote: "n3", Commit: 2}
	oldLog := logical{es: ents(1, 6, 1), hs: hs}
	newLog := logical{b: Boundary{9, 3}, hs: hs}
	check := func(mem *fault.MemFS, context string) string {
		t.Helper()
		l, rec, err := Open(memPath, Options{Sync: true, FS: mem})
		if err != nil {
			t.Fatalf("%s: reopen: %v", context, err)
		}
		defer l.Close()
		if sameRecovered(rec, oldLog) == nil {
			return "old"
		}
		requireRecovered(t, rec, newLog, context)
		return "new"
	}
	outcomes := map[string]int{}
	for _, op := range []fault.Op{fault.OpWrite, fault.OpSync} {
		for _, mode := range []string{"process", "power"} {
			mem := fault.NewMemFS()
			inj := fault.NewInjectFS(mem)
			l := setup(inj)
			inj.Arm(fault.Injection{Op: op, Path: memPath, At: func() {
				if mode == "power" {
					mem.CrashPowerLoss(0)
				} else {
					mem.CrashProcess()
				}
				panic(errDied)
			}})
			if _, died := untilDeath(func() error { return l.Install(9, 3) }); !died {
				t.Fatalf("%s never reached", op)
			}
			outcomes[check(mem, fmt.Sprintf("crash (%s) before %s", mode, op))]++
		}
	}
	whole := len(encodeBoundary(Boundary{9, 3})) + 9 // payload + the framing's header
	for short := 1; short < whole; short++ {
		mem := fault.NewMemFS()
		inj := fault.NewInjectFS(mem)
		l := setup(inj)
		inj.Arm(fault.Injection{Op: fault.OpWrite, Path: memPath, Short: short})
		if err := l.Install(9, 3); !errors.Is(err, ErrFailed) {
			t.Fatalf("torn at %d: %v", short, err)
		}
		mem.CrashProcess()
		if got := check(mem, fmt.Sprintf("boundary record torn at byte %d", short)); got != "old" {
			t.Fatalf("a torn boundary record recovered as %s", got)
		}
	}
	if outcomes["new"] == 0 || outcomes["old"] == 0 {
		t.Fatalf("outcomes %v", outcomes)
	}
}

// TestOpenRemovesACompactionOrphan: a temporary left by a compaction that
// crashed before its rename is removed, and the log it would have replaced is
// the log.
func TestOpenRemovesACompactionOrphan(t *testing.T) {
	mem := fault.NewMemFS()
	l, _ := openMem(t, mem)
	mustSave(t, l, &HardState{Term: 1, Commit: 2}, ents(1, 3, 1)...)
	l.Close()
	writeFile(t, mem, TmpPath(memPath), []byte("half a compaction"))
	l, rec := reopenMem(t, mem)
	defer l.Close()
	requireRecovered(t, rec, logical{es: ents(1, 3, 1), hs: HardState{Term: 1, Commit: 2}}, "reopen")
	if _, ok := mem.Cached(TmpPath(memPath)); ok {
		t.Fatal("the orphan survived Open")
	}
}

// TestRandomLogHistoriesMatchTheModel drives random histories of saves (with
// suffix replacements), compactions, installs, reopens, process crashes and
// power losses against a log and an independent logical model, and requires
// every recovery to be exactly the model's log. Sync is on, so a power loss
// loses nothing a returned call wrote.
func TestRandomLogHistoriesMatchTheModel(t *testing.T) {
	rng := rand.New(rand.NewSource(1414))
	counts := map[string]int{}
	for run := 0; run < 300; run++ {
		mem := fault.NewMemFS()
		l, _ := openMem(t, mem)
		var m logical
		for step := 0; step < 60; step++ {
			switch op := rng.Intn(10); {
			case op < 5: // save: maybe a new term, entries from above the commit, a commit advance
				hs := m.hs
				if rng.Intn(4) == 0 {
					hs.Term++
					hs.Vote = replication.NodeID(fmt.Sprintf("n%d", rng.Intn(3)))
				}
				from := m.commit() + 1 + uint64(rng.Intn(int(m.last()-m.commit())+1))
				es := ents(from, from+uint64(rng.Intn(4))-1, max(hs.Term, 1))
				if hs.Term == 0 {
					hs.Term = 1
				}
				mustSave(t, l, &hs, es...)
				for _, e := range es {
					m.es = append(m.es[:e.Index-m.b.Index-1], e)
				}
				last := m.last()
				hs.Commit = m.commit() + uint64(rng.Intn(int(last-m.commit())+1))
				mustSave(t, l, &hs)
				m.hs = hs
				counts["save"]++
			case op < 7: // compact within the commit
				if m.commit() == m.b.Index {
					continue
				}
				i := m.b.Index + 1 + uint64(rng.Intn(int(m.commit()-m.b.Index)))
				if err := l.Compact(i, m.term(i)); err != nil {
					t.Fatalf("run %d step %d: compact(%d): %v", run, step, i, err)
				}
				term := m.term(i)
				m.es = append([]Entry(nil), m.es[i-m.b.Index:]...)
				m.b = Boundary{i, term}
				counts["compact"]++
			case op < 8: // install above the commit: at a held entry (either term) or past the log
				i := m.commit() + 1 + uint64(rng.Intn(int(m.last()-m.commit())+3))
				term := m.hs.Term + uint64(rng.Intn(2))
				if i <= m.last() && rng.Intn(2) == 0 {
					term = m.term(i)
				}
				term = max(term, 1)
				if term > m.hs.Term { // the driver persists the new term first
					m.hs.Term, m.hs.Vote = term, ""
					mustSave(t, l, &HardState{Term: m.hs.Term, Commit: m.hs.Commit})
				}
				if err := l.Install(i, term); err != nil {
					t.Fatalf("run %d step %d: install(%d,%d): %v", run, step, i, term, err)
				}
				if i <= m.last() && m.term(i) == term {
					m.es = append([]Entry(nil), m.es[i-m.b.Index:]...)
					counts["install-keep"]++
				} else {
					m.es = nil
					counts["install-discard"]++
				}
				m.b = Boundary{i, term}
			default: // restart: clean, process crash, or power loss
				switch rng.Intn(3) {
				case 0:
					l.Close()
				case 1:
					mem.CrashProcess()
				default:
					mem.CrashPowerLoss(rng.Intn(8))
				}
				var rec *Recovered
				l, rec = reopenMem(t, mem)
				requireRecovered(t, rec, m, fmt.Sprintf("run %d step %d", run, step))
				m.hs.Commit = rec.HardState.Commit // Open adopts the clamped commit
				counts["restart"]++
			}
		}
		l.Close()
		_, rec := reopenMem(t, mem)
		requireRecovered(t, rec, m, fmt.Sprintf("run %d end", run))
	}
	for _, k := range []string{"save", "compact", "install-keep", "install-discard", "restart"} {
		if counts[k] == 0 {
			t.Fatalf("no %s in %v", k, counts)
		}
	}
	t.Logf("%v", counts)
}

func FuzzDecodeBoundary(f *testing.F) {
	f.Add(encodeBoundary(Boundary{7, 2}))
	f.Add([]byte{0, 1})
	f.Fuzz(func(t *testing.T, p []byte) {
		b, err := decodeBoundary(p)
		if err != nil {
			return
		}
		if b.Index == 0 || b.Term == 0 {
			t.Fatalf("decoded an impossible boundary %+v", b)
		}
	})
}

// TestInstallIsDurableWhenItReturns: an Install that returned has fsynced its
// boundary — a power loss right after it keeps the snapshot's boundary, so the
// node never reports a snapshot installed that a power loss could take back.
func TestInstallIsDurableWhenItReturns(t *testing.T) {
	mem := fault.NewMemFS()
	l, _, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatal(err)
	}
	mustSave(t, l, &HardState{Term: 3, Vote: "n3", Commit: 2}, ents(1, 3, 1)...)
	if err := l.Install(9, 3); err != nil {
		t.Fatal(err)
	}
	mem.CrashPowerLoss(0)
	l2, rec, err := Open(memPath, Options{Sync: true, FS: mem})
	if err != nil {
		t.Fatalf("reopen after a power loss: %v", err)
	}
	defer l2.Close()
	if rec.Boundary != (Boundary{9, 3}) {
		t.Fatalf("after a power loss the boundary is %+v: the returned Install was not durable", rec.Boundary)
	}
}

// TestAFailedLogRefusesInstallAndCompact (INV-F1): once the log has failed,
// Install and Compact — like Save — return the failure and touch nothing; a
// log that cannot vouch for its file must not rewrite or extend it.
func TestAFailedLogRefusesInstallAndCompact(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	l, _ := openMem(t, inj)
	mustSave(t, l, &HardState{Term: 3, Vote: "n3", Commit: 4}, ents(1, 5, 1)...)
	inj.Arm(fault.Injection{Op: fault.OpSync, Nth: 1})
	if err := l.Save(&HardState{Term: 3, Vote: "n3", Commit: 5}, nil); !errors.Is(err, ErrFailed) {
		t.Fatalf("the failing Save: %v, want ErrFailed", err)
	}
	ops := len(inj.Ops())
	if err := l.Install(9, 3); !errors.Is(err, ErrFailed) {
		t.Fatalf("Install on a failed log: %v, want ErrFailed", err)
	}
	if err := l.Compact(3, 1); !errors.Is(err, ErrFailed) {
		t.Fatalf("Compact on a failed log: %v, want ErrFailed", err)
	}
	if done := inj.Ops()[ops:]; len(done) != 0 {
		t.Fatalf("the failed log still performed %v", done)
	}
}
