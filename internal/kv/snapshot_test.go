package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/snapshot"
)

// Phase 14: the store's snapshot state (docs/SNAPSHOTS.md §2–§3).

func applyAll(t *testing.T, s *Store, first uint64, cmds []Command) []Result {
	t.Helper()
	var out []Result
	for i, c := range cmds {
		r, err := s.ApplyResult(first+uint64(i), c.Encode())
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r.(Result))
	}
	return out
}

func encoded(t *testing.T, s *Store) (uint64, []byte) {
	t.Helper()
	idx, b, err := s.EncodeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	return idx, b
}

// TestSnapshotRoundTripIsCanonical: a restored store is the same store — map,
// session table and applied index — and encodes to the very same bytes.
func TestSnapshotRoundTripIsCanonical(t *testing.T) {
	limits := Limits{MaxSessions: 3, MaxUnacked: 4}
	rng := rand.New(rand.NewSource(14))
	for run := 0; run < 300; run++ {
		a := NewStoreWithLimits(limits)
		applyAll(t, a, 1, genSessionCommands(rng, 1+rng.Intn(80)))
		idx, b := encoded(t, a)
		r := NewStoreWithLimits(limits)
		if err := r.RestoreSnapshot(idx, 0, b); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if idx2, b2 := encoded(t, r); idx2 != idx || !bytes.Equal(b, b2) {
			t.Fatalf("run %d: re-encoding differs", run)
		}
		if fmt.Sprint(r.Snapshot()) != fmt.Sprint(a.Snapshot()) || fmt.Sprint(r.Sessions()) != fmt.Sprint(a.Sessions()) || r.Applied() != a.Applied() {
			t.Fatalf("run %d: restored state differs", run)
		}
	}
}

// TestSnapshotPlusSuffixEqualsFullReplay is INV-SN4 at the state machine: for
// random command sequences cut at EVERY index, restoring a snapshot taken at the
// cut and applying the rest makes exactly the decisions an uninterrupted replay
// makes — duplicates answered with the same original index, conflicts, stale and
// expired requests, evictions, limits — and ends in the identical state. This is
// what makes a retry after a snapshot, a compaction and a restart a duplicate
// and not a second execution.
func TestSnapshotPlusSuffixEqualsFullReplay(t *testing.T) {
	limits := Limits{MaxSessions: 3, MaxUnacked: 4}
	rng := rand.New(rand.NewSource(1414))
	decided := map[Decision]int{}
	for run := 0; run < 120; run++ {
		cmds := genSessionCommands(rng, 50)
		full := NewStoreWithLimits(limits)
		want := applyAll(t, full, 1, cmds)
		_, wantState := encoded(t, full)
		for cut := 1; cut < len(cmds); cut++ {
			a := NewStoreWithLimits(limits)
			applyAll(t, a, 1, cmds[:cut])
			idx, snap := encoded(t, a)
			b := NewStoreWithLimits(limits)
			if err := b.RestoreSnapshot(idx, 0, snap); err != nil {
				t.Fatalf("run %d cut %d: %v", run, cut, err)
			}
			got := applyAll(t, b, uint64(cut+1), cmds[cut:])
			for i, r := range got {
				if r != want[cut+i] {
					t.Fatalf("run %d cut %d: entry %d decided %+v after the snapshot, %+v without it", run, cut, cut+i+1, r, want[cut+i])
				}
				decided[r.Decision]++
			}
			if _, st := encoded(t, b); !bytes.Equal(st, wantState) {
				t.Fatalf("run %d cut %d: final state differs from the full replay", run, cut)
			}
		}
	}
	for _, d := range []Decision{Executed, Duplicate, Conflict, Stale, Expired, Limit, Registered} {
		if decided[d] == 0 {
			t.Fatalf("no %s decision after a restore: %v", d, decided)
		}
	}
	t.Logf("decisions after a restore: %v", decided)
}

// TestRestoredStateMatchesTheModel is INV-SN1 at the state machine: the state
// a snapshot carries, once restored, is the independent reference model's state
// at the same index — every key, every session's watermark and remembered
// requests.
func TestRestoredStateMatchesTheModel(t *testing.T) {
	limits := Limits{MaxSessions: 3, MaxUnacked: 4}
	rng := rand.New(rand.NewSource(141))
	for run := 0; run < 200; run++ {
		a := NewStoreWithLimits(limits)
		model := lincheck.NewSessionModel(lincheck.SessionLimits{MaxSessions: limits.MaxSessions, MaxUnacked: limits.MaxUnacked})
		cmds := genSessionCommands(rng, 1+rng.Intn(60))
		for i, c := range cmds {
			if _, err := a.ApplyResult(uint64(i+1), c.Encode()); err != nil {
				t.Fatal(err)
			}
			model.Apply(modelCommand(uint64(i+1), c))
		}
		idx, b := encoded(t, a)
		r := NewStoreWithLimits(limits)
		if err := r.RestoreSnapshot(idx, 0, b); err != nil {
			t.Fatal(err)
		}
		requireSameSessions(t, r, model)
		for _, k := range []string{"k0", "k1", "k2"} {
			v, ok := r.Get([]byte(k))
			ms := model.State(k)
			if ok != ms.Present || (ok && string(v) != ms.Value) {
				t.Fatalf("run %d: key %s restored as %q (%v), model %+v", run, k, v, ok, ms)
			}
		}
	}
}

// --- invalid states ---

type resSpec struct {
	rid, index uint64
	fp         byte
}

type sessSpec struct {
	id, last, acked uint64
	results         []resSpec
}

// stateSpec writes a state encoding field by field, so a test can write what
// EncodeSnapshot never would.
type stateSpec struct {
	version, applied, maxS, maxU uint64
	keys                         [][2]string
	sessions                     []sessSpec
	trailing                     []byte
}

func (s stateSpec) encode() []byte {
	b := binary.AppendUvarint(nil, s.version)
	b = binary.AppendUvarint(b, s.applied)
	b = binary.AppendUvarint(b, s.maxS)
	b = binary.AppendUvarint(b, s.maxU)
	b = binary.AppendUvarint(b, uint64(len(s.keys)))
	for _, kv := range s.keys {
		b = appendBytes(b, []byte(kv[0]))
		b = appendBytes(b, []byte(kv[1]))
	}
	b = binary.AppendUvarint(b, uint64(len(s.sessions)))
	for _, ss := range s.sessions {
		b = binary.AppendUvarint(b, ss.id)
		b = binary.AppendUvarint(b, ss.last)
		b = binary.AppendUvarint(b, ss.acked)
		b = binary.AppendUvarint(b, uint64(len(ss.results)))
		for _, r := range ss.results {
			b = binary.AppendUvarint(b, r.rid)
			b = binary.AppendUvarint(b, r.index)
			b = append(b, bytes.Repeat([]byte{r.fp}, 32)...)
		}
	}
	return append(b, s.trailing...)
}

var corpusLimits = Limits{MaxSessions: 3, MaxUnacked: 2}

// validSpec is a small valid state under corpusLimits at index 20: two keys (one
// holding an empty value), two sessions, one with two remembered requests.
func validSpec() stateSpec {
	return stateSpec{version: 1, applied: 20, maxS: 3, maxU: 2,
		keys: [][2]string{{"a", ""}, {"b", "v"}},
		sessions: []sessSpec{
			{id: 3, last: 9, acked: 2, results: []resSpec{{rid: 2, index: 5, fp: 1}, {rid: 3, index: 9, fp: 2}}},
			{id: 10, last: 18, acked: 1},
		}}
}

// badState is a known-bad state and the text of the rule refusing it: each
// breaks exactly one rule a valid table always keeps, and is refused by that
// rule, not by an earlier accident of the encoding.
type badState struct {
	spec stateSpec
	rule string
}

func invalidStates() map[string]badState {
	type mut struct {
		f    func(*stateSpec)
		rule string
	}
	m := map[string]mut{
		"future-version": {func(s *stateSpec) { s.version = 2 }, "version 2"},
		"other-limits":   {func(s *stateSpec) { s.maxS = 4 }, "built under limits 4 sessions"},
		"empty-key":      {func(s *stateSpec) { s.keys[0][0] = "" }, "an empty key"},
		"keys-unsorted":  {func(s *stateSpec) { s.keys[0], s.keys[1] = s.keys[1], s.keys[0] }, `keys not strictly ascending at "a"`},
		"duplicate-key":  {func(s *stateSpec) { s.keys[1][0] = "a" }, `keys not strictly ascending at "a"`},
		"oversized-key":  {func(s *stateSpec) { s.keys[1][0] = "b" + strings.Repeat("x", MaxKeyLen) }, "out of range"},
		"too-many-sessions": {func(s *stateSpec) {
			s.sessions = append(s.sessions, sessSpec{id: 11, last: 11, acked: 1}, sessSpec{id: 12, last: 12, acked: 1})
		}, "4 sessions, limit 3"},
		"session-id-zero":         {func(s *stateSpec) { s.sessions[0].id = 0 }, "session id 0 outside"},
		"session-id-past-applied": {func(s *stateSpec) { s.sessions[1].id, s.sessions[1].last = 21, 21 }, "session id 21 outside"},
		"session-ids-unsorted":    {func(s *stateSpec) { s.sessions[0], s.sessions[1] = s.sessions[1], s.sessions[0] }, "session ids not strictly ascending at 3"},
		"duplicate-session":       {func(s *stateSpec) { s.sessions[1].id = 3 }, "session ids not strictly ascending at 3"},
		"last-before-register":    {func(s *stateSpec) { s.sessions[1].last = 8 }, "last command 8 outside"},
		"last-past-applied":       {func(s *stateSpec) { s.sessions[1].last = 21 }, "last command 21 outside"},
		"two-sessions-same-last":  {func(s *stateSpec) { s.sessions[1].id, s.sessions[1].last = 5, 9 }, "two sessions last touched at index 9"},
		"watermark-zero":          {func(s *stateSpec) { s.sessions[1].acked = 0 }, "watermark 0"},
		"too-many-results": {func(s *stateSpec) {
			s.sessions[0].results = append(s.sessions[0].results, resSpec{rid: 4, index: 9, fp: 3})
		}, "holds 3 results, limit 2"},
		"result-below-watermark":    {func(s *stateSpec) { s.sessions[0].results[0].rid = 1 }, "request 1 below its watermark 2"},
		"results-unsorted":          {func(s *stateSpec) { r := s.sessions[0].results; r[0], r[1] = r[1], r[0] }, "request ids not strictly ascending at 2"},
		"duplicate-result":          {func(s *stateSpec) { s.sessions[0].results[1].rid = 2 }, "request ids not strictly ascending at 2"},
		"result-before-register":    {func(s *stateSpec) { s.sessions[0].results[0].index = 3 }, "executed at 3, outside"},
		"result-after-last-command": {func(s *stateSpec) { s.sessions[0].results[1].index = 10 }, "executed at 10, outside"},
		"trailing-bytes":            {func(s *stateSpec) { s.trailing = []byte{0} }, "trailing bytes"},
	}
	out := map[string]badState{}
	for name, x := range m {
		s := validSpec() // fresh slices on every call
		x.f(&s)
		out[name] = badState{s, x.rule}
	}
	return out
}

// TestRestoreRefusesImpossibleState: every known-bad state is refused with
// ErrSnapshotState — plus a truncation at every byte and a non-canonical
// integer — and a refused restore leaves the store untouched.
func TestRestoreRefusesImpossibleState(t *testing.T) {
	good := validSpec().encode()
	if err := NewStoreWithLimits(corpusLimits).RestoreSnapshot(20, 0, good); err != nil {
		t.Fatalf("the valid base state: %v", err)
	}
	s := NewStoreWithLimits(corpusLimits)
	applyAll(t, s, 1, []Command{{Op: OpPut, Key: []byte("keep"), Value: []byte("me")}})
	_, before := encoded(t, s)
	for name, bad := range invalidStates() {
		err := s.RestoreSnapshot(bad.spec.applied, 0, bad.spec.encode())
		if !errors.Is(err, ErrSnapshotState) || !strings.Contains(err.Error(), bad.rule) {
			t.Errorf("%s: %v (want the rule %q)", name, err, bad.rule)
		}
	}
	if err := s.RestoreSnapshot(19, 0, good); !errors.Is(err, ErrSnapshotState) {
		t.Errorf("state at 20 restored as a snapshot at 19: %v", err)
	}
	for n := 0; n < len(good); n++ {
		if err := s.RestoreSnapshot(20, 0, good[:n]); err == nil {
			t.Fatalf("a state truncated to %d bytes restored", n)
		}
	}
	nc := append([]byte{1, 0x94, 0x00}, good[2:]...) // applied 20 written in two bytes
	if err := s.RestoreSnapshot(20, 0, nc); !errors.Is(err, ErrSnapshotState) {
		t.Errorf("non-canonical integer: %v", err)
	}
	if _, after := encoded(t, s); !bytes.Equal(before, after) {
		t.Fatal("a refused restore changed the store")
	}
}

var updateCorpus = flag.Bool("update-snapshots", false, "rewrite the snapshot state corpus in testdata/snapshots")

// corpusFiles are the known-good and known-bad snapshot files for the store's
// state, whole (snapshot format around the state), under corpusLimits.
func corpusFiles(t *testing.T) map[string][]byte {
	t.Helper()
	wrap := func(index uint64, state []byte) []byte {
		b, err := snapshot.Encode(snapshot.Meta{Conf: replication.VotersOf([]replication.NodeID{"n1", "n2", "n3"}), Index: index, Term: 2}, state)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	store := func(cmds []Command) []byte {
		s := NewStoreWithLimits(corpusLimits)
		applyAll(t, s, 1, cmds)
		idx, b := encoded(t, s)
		return wrap(idx, b)
	}
	put := func(k, v string) Command { return Command{Op: OpPut, Key: []byte(k), Value: []byte(v)} }
	del := func(k string) Command { return Command{Op: OpDelete, Key: []byte(k)} }
	reg := Command{Op: OpRegister}
	idPut := func(cid, rid, acked uint64, k, v string) Command {
		return Command{Op: OpPut, ClientID: cid, RequestID: rid, AckedBelow: acked, Key: []byte(k), Value: []byte(v)}
	}
	large := []Command{}
	for i := 0; i < 1100; i++ { // > 1 MiB of state: more than one data record
		large = append(large, put(fmt.Sprintf("key-%05d", i), strings.Repeat(string(rune('a'+i%26)), 1000)))
	}
	files := map[string][]byte{
		"good/empty-store":        store([]Command{put("a", "1"), del("a")}),
		"good/keys":               store([]Command{put("a", "1"), put("b", "2"), put("c", "3")}),
		"good/deleted-keys":       store([]Command{put("a", "1"), put("b", "2"), del("a"), del("zz")}),
		"good/empty-values":       store([]Command{put("a", ""), put("b", "")}),
		"good/sessions":           store([]Command{reg, idPut(1, 1, 1, "a", "x"), idPut(1, 2, 1, "b", "y"), reg, idPut(4, 1, 1, "a", "z")}),
		"good/evicted-sessions":   store([]Command{reg, reg, reg, reg, reg, idPut(5, 1, 1, "k", "v")}),
		"good/session-at-limit":   store([]Command{reg, idPut(1, 1, 1, "a", "1"), idPut(1, 2, 1, "a", "2"), idPut(1, 3, 1, "a", "3")}),
		"good/watermark-advanced": store([]Command{reg, idPut(1, 1, 1, "a", "1"), idPut(1, 2, 1, "a", "2"), idPut(1, 3, 3, "a", "3")}),
		"good/large-kv":           store(large),
	}
	for name, bad := range invalidStates() {
		files["bad/"+name] = wrap(bad.spec.applied, bad.spec.encode())
	}
	return files
}

// TestSnapshotStateCorpus: every known-good snapshot file restores (and
// re-encodes to the same state bytes); every known-bad one is refused. The
// files are committed (testdata/snapshots); -update-snapshots rewrites them.
func TestSnapshotStateCorpus(t *testing.T) {
	files, bad := corpusFiles(t), invalidStates()
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		path := filepath.Join("testdata", "snapshots", name+".snap")
		if *updateCorpus {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, files[name], 0o644); err != nil {
				t.Fatal(err)
			}
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v (run with -update-snapshots)", name, err)
		}
		if !bytes.Equal(b, files[name]) {
			t.Fatalf("%s: the committed file differs from what is generated now", name)
		}
		m, state, err := snapshot.Decode(b)
		if err != nil {
			t.Fatalf("%s: snapshot format: %v", name, err)
		}
		s := NewStoreWithLimits(corpusLimits)
		verr := s.ValidateSnapshot(m.Index, state)
		err = s.RestoreSnapshot(m.Index, 0, state)
		if (verr == nil) != (err == nil) {
			t.Errorf("%s: ValidateSnapshot says %v, RestoreSnapshot %v", name, verr, err)
		}
		switch {
		case strings.HasPrefix(name, "good/") && err != nil:
			t.Errorf("%s: known-good refused: %v", name, err)
		case strings.HasPrefix(name, "good/"):
			if _, again := encoded(t, s); !bytes.Equal(again, state) {
				t.Errorf("%s: re-encoding differs", name)
			}
		case !errors.Is(err, ErrSnapshotState) || !strings.Contains(err.Error(), bad[strings.TrimPrefix(name, "bad/")].rule):
			t.Errorf("%s: known-bad: %v", name, err)
		}
	}
}

// FuzzRestoreSnapshot: RestoreSnapshot is total, and whatever it accepts
// re-encodes to the same bytes.
func FuzzRestoreSnapshot(f *testing.F) {
	f.Add(uint64(20), validSpec().encode())
	for _, bad := range invalidStates() {
		f.Add(bad.spec.applied, bad.spec.encode())
	}
	f.Fuzz(func(t *testing.T, index uint64, b []byte) {
		s := NewStoreWithLimits(corpusLimits)
		if err := s.RestoreSnapshot(index, 0, b); err != nil {
			return
		}
		if _, again, _ := s.EncodeSnapshot(); !bytes.Equal(again, b) {
			t.Fatal("accepted a non-canonical state")
		}
	})
}
