package kv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/snapshot"
)

// S4 (docs/STORAGE_INTEGRATION.md §10.2–§10.6): the LSM machine's snapshots
// are its engine streamed — a checkpoint encoded in the store's format, byte
// for byte; a state validated and ingested from a stream, by the store's
// rules, of any size the format allows.

// TestLSMMachineSnapshotIsTheStoresEncodingOnSeededScripts: on random scripts
// — sessions, retries, deletions, evictions, no-ops — with a memtable small
// enough that the engine flushes and compacts, the snapshot the machine
// streams from its checkpoint is the reference store's encoding of the same
// state, byte for byte; and restoring it into a fresh machine gives that
// machine the same state, which in turn snapshots to the same bytes.
func TestLSMMachineSnapshotIsTheStoresEncodingOnSeededScripts(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			p := newPair(t, Limits{MaxSessions: 3, MaxUnacked: 3}, nil)
			p.opts.MemTableSize = 512 // flushes, tables, tombstones
			p.opts.DisableAutoCompaction = false
			p.opts.L0CompactionTrigger = 2 // and compactions underneath
			_ = p.lsm.Close()
			p.open()
			var sessions []uint64
			nextRID := map[uint64]uint64{}
			keys := []string{"a", "b", "c", "d", "e", "f"}
			for step := 0; step < 80; step++ {
				var cmds [][]byte
				for i := 0; i < 1+rng.Intn(4); i++ {
					switch r := rng.Intn(10); {
					case r == 0 || len(sessions) == 0:
						cmds = append(cmds, register())
						sessions = append(sessions, p.next+uint64(len(cmds))-1)
					case r == 1:
						cmds = append(cmds, nil)
					case r == 2:
						cmds = append(cmds, anonPut(keys[rng.Intn(len(keys))], strings.Repeat("x", rng.Intn(40))))
					default:
						cid := sessions[rng.Intn(len(sessions))]
						rid := nextRID[cid] + 1
						nextRID[cid] = rid
						k := keys[rng.Intn(len(keys))]
						if rng.Intn(3) == 0 {
							cmds = append(cmds, idDel(cid, rid, 1, k))
						} else {
							cmds = append(cmds, idPut(cid, rid, 1, k, fmt.Sprint(step, i)))
						}
					}
				}
				p.cycle(cmds...)
				if step%10 != 9 {
					continue
				}
				gi, gd, err := p.lsm.EncodeSnapshot()
				ri, rd, _ := p.mem.EncodeSnapshot()
				if err != nil || gi != ri || !bytes.Equal(gd, rd) {
					t.Fatalf("step %d: the machine's streamed snapshot at %d differs from the store's at %d (%v)", step, gi, ri, err)
				}
				fresh, err := OpenLSMMachine(filepath.Join(t.TempDir(), "fresh"), p.limits, engineOpts(nil))
				if err != nil {
					t.Fatal(err)
				}
				if err := fresh.RestoreSnapshotFrom(gi, p.term, bytes.NewReader(gd)); err != nil {
					t.Fatalf("step %d: restoring the machine's own snapshot into a fresh machine: %v", step, err)
				}
				sameMachines(t, fmt.Sprintf("step %d, the restored machine", step), p.mem, fresh, false)
				if fi, fd, err := fresh.EncodeSnapshot(); err != nil || fi != gi || !bytes.Equal(fd, gd) {
					t.Fatalf("step %d: the restored machine's snapshot differs (%v)", step, err)
				}
				_ = fresh.Close()
			}
		})
	}
}

// TestBothMachinesJudgeTheSnapshotCorpusAlike: every known-good and known-bad
// snapshot of the corpus, streamed from its file as raftnode streams it, is
// accepted or refused by the LSM machine exactly as by the store, with the
// same reason; a known-good one, restored, is the store's state.
func TestBothMachinesJudgeTheSnapshotCorpusAlike(t *testing.T) {
	for name, file := range corpusFiles(t) {
		stream := func() *snapshot.Reader {
			rd, err := snapshot.NewReader(bytes.NewReader(file), int64(len(file)))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			return rd
		}
		index := stream().Meta().Index
		store := NewStoreWithLimits(corpusLimits)
		lsm, err := OpenLSMMachine(filepath.Join(t.TempDir(), "lsm"), corpusLimits, engineOpts(nil))
		if err != nil {
			t.Fatal(err)
		}
		e1 := store.ValidateSnapshotFrom(index, stream())
		e2 := lsm.ValidateSnapshotFrom(index, stream())
		if (e1 == nil) != (e2 == nil) || (e1 != nil && e1.Error() != e2.Error()) {
			t.Fatalf("%s: the store says %v, the LSM machine %v", name, e1, e2)
		}
		r1 := store.RestoreSnapshotFrom(index, 2, stream())
		r2 := lsm.RestoreSnapshotFrom(index, 2, stream())
		good := strings.HasPrefix(name, "good/")
		if (r1 == nil) != good || (r2 == nil) != good {
			t.Fatalf("%s: restore: the store %v, the LSM machine %v", name, r1, r2)
		}
		if r1 == nil {
			sameMachines(t, name, store, lsm, false)
		}
		_ = lsm.Close()
	}
}

// bigState builds, in the reference store, a state of more than min bytes of
// user data: registered sessions with remembered results, identified and
// anonymous writes, deletions. It returns the store and the identified
// requests whose retries the restored machine must recognize.
func bigState(t *testing.T, limits Limits, min int) (*Store, [][]byte) {
	t.Helper()
	s := NewStoreWithLimits(limits)
	idx := uint64(0)
	apply := func(cmd []byte) {
		idx++
		if _, err := s.ApplyResult(idx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	apply(register()) // session 1
	apply(register()) // session 2
	val := strings.Repeat("v", MaxValueLen/2)
	newest := map[uint64][]byte{} // each session's newest request: its result is remembered
	for total, i := 0, 0; total <= min; i++ {
		k := fmt.Sprintf("big%05d", i)
		cmd := anonPut(k, val[:len(val)-i%97])
		if i%5 == 0 {
			cid := uint64(1 + i%2)
			cmd = idPut(cid, uint64(i/5+1), uint64(max(1, i/5-1)), k, val[:len(val)-i%97])
			newest[cid] = cmd
		}
		apply(cmd)
		total += len(val)
		if i%17 == 0 {
			apply(anonPut(fmt.Sprintf("gone%05d", i), "x"))
			apply(Command{Op: OpDelete, Key: []byte(fmt.Sprintf("gone%05d", i))}.Encode())
		}
	}
	return s, [][]byte{newest[1], newest[2]}
}

// TestLSMMachineInstallsAStateAboveTheS3Limit: a state larger than one WAL
// record — the ~63 MiB S3 refused — published as a snapshot file and restored
// from a stream over it, as raftnode installs one: the machine holds exactly
// the store's state (its streamed snapshot is the store's bytes), at the
// snapshot's index and term, its sessions answer a retried request as a
// duplicate, it survives reopening, and the restore's live heap grows by a few
// tables' worth, not by the state.
func TestLSMMachineInstallsAStateAboveTheS3Limit(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and restores a ~70 MiB state")
	}
	limits := Limits{MaxSessions: 4, MaxUnacked: 4}
	ref, retries := bigState(t, limits, s3RestoreLimit+4<<20)
	index, term := ref.Applied(), uint64(7)
	dir := t.TempDir()
	files := snapshot.Files{Base: filepath.Join(dir, "raft.log")}
	src, err := ref.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := files.PublishFrom(snapshot.Meta{Conf: replication.VotersOf([]replication.NodeID{"n1"}), Index: index, Term: term}, src); err != nil {
		t.Fatal(err)
	}
	_ = src.Close()
	src = nil
	rd, file, found, err := files.Open()
	if err != nil || !found {
		t.Fatalf("opening the published snapshot: %v (found %v)", err, found)
	}
	defer file.Close()
	if rd.DataLen() <= s3RestoreLimit {
		t.Fatalf("premise: the state is %d bytes, not above the S3 limit of %d", rd.DataLen(), s3RestoreLimit)
	}

	m, err := OpenLSMMachine(filepath.Join(dir, "lsm"), limits, engineOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer debug.SetGCPercent(debug.SetGCPercent(20))
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base, peak := ms.HeapInuse, uint64(0)
	sampled := &sampleReader{r: rd, every: 2 << 20, sample: func() {
		runtime.ReadMemStats(&ms)
		if ms.HeapInuse > base && ms.HeapInuse-base > peak {
			peak = ms.HeapInuse - base
		}
	}}
	if err := m.RestoreSnapshotFrom(index, term, sampled); err != nil {
		t.Fatalf("restoring a %d MiB state: %v", rd.DataLen()>>20, err)
	}
	const bound = 24 << 20
	if peak > bound {
		t.Fatalf("the restore grew the live heap by %d MiB for a %d MiB state; bound %d MiB", peak>>20, rd.DataLen()>>20, bound>>20)
	}
	t.Logf("restored a %d MiB state from a stream; peak live-heap growth %.1f MiB", rd.DataLen()>>20, float64(peak)/(1<<20))

	check := func(m *LSMMachine, when string) {
		t.Helper()
		if m.Applied() != index || m.EngineApplied().Term != term || m.DurableAppliedIndex() != index {
			t.Fatalf("%s: applied %d, engine %+v, durable %d; want %d at term %d, durable", when, m.Applied(), m.EngineApplied(), m.DurableAppliedIndex(), index, term)
		}
		gi, gd, err := m.EncodeSnapshot()
		ri, rdata, _ := ref.EncodeSnapshot()
		if err != nil || gi != ri || !bytes.Equal(gd, rdata) {
			t.Fatalf("%s: the machine's state is not the store's (%v)", when, err)
		}
		if !reflectEqualSessions(m.Sessions(), ref.Sessions()) {
			t.Fatalf("%s: the session tables differ", when)
		}
	}
	check(m, "after the restore")
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = OpenLSMMachine(filepath.Join(dir, "lsm"), limits, engineOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	check(m, "after reopening")
	// A retry of a request the snapshot covers is a duplicate, on both.
	for _, cmd := range retries {
		want, _ := ref.ApplyResult(index+1, cmd)
		got, err := m.ApplyEntry(index+1, term, cmd)
		if err != nil {
			t.Fatal(err)
		}
		if r, ok := got.(Result); !ok || r.Decision != Duplicate || want.(Result).Decision != Duplicate {
			t.Fatalf("a retry of a request the snapshot covers: the machine decided %v, the store %v; want duplicates", got, want)
		}
		index++
		if err := m.EndCycle(); err != nil {
			t.Fatal(err)
		}
	}
}

// sampleReader samples the heap every so many bytes read.
type sampleReader struct {
	r      io.Reader
	every  int
	n      int
	sample func()
}

func (s *sampleReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	s.n += n
	if s.n >= s.every {
		s.n = 0
		s.sample()
	}
	return n, err
}

func reflectEqualSessions(a, b map[uint64]SessionState) bool {
	if len(a) != len(b) {
		return false
	}
	ids := make([]uint64, 0, len(a))
	for id := range a {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if fmt.Sprint(a[id]) != fmt.Sprint(b[id]) {
			return false
		}
	}
	return true
}

// TestLSMMachineSnapshotIsRefusedWhenTheEngineIsNotTheMachine: the source
// checks, while counting, that the engine's session records are the
// machine's session table; a checkpoint that is not the machine's state is
// never written into a snapshot.
func TestLSMMachineSnapshotIsRefusedWhenTheEngineIsNotTheMachine(t *testing.T) {
	p := newPair(t, Limits{MaxSessions: 3, MaxUnacked: 3}, nil)
	p.cycle(register(), idPut(1, 1, 1, "a", "1"))
	if _, err := p.lsm.OpenSnapshot(); err != nil {
		t.Fatalf("premise: a consistent machine snapshots: %v", err)
	}
	p.lsm.mu.Lock()
	p.lsm.sessions.sessions[1].last++ // the mirror diverges from the engine's record
	p.lsm.mu.Unlock()
	if src, err := p.lsm.OpenSnapshot(); err == nil {
		_ = src.Close()
		t.Fatal("a machine whose engine disagrees with its session table produced a snapshot")
	}
	p.lsm.mu.Lock()
	p.lsm.sessions.sessions[1].last--
	delete(p.lsm.sessions.sessions, 1) // a session the engine records and the machine does not
	p.lsm.mu.Unlock()
	if src, err := p.lsm.OpenSnapshot(); err == nil {
		_ = src.Close()
		t.Fatal("a machine missing a session its engine records produced a snapshot")
	}
}

// TestLSMMachineSourceRefusesAStagedCycle: a snapshot is taken between cycles
// (the driver's rule), so a source opened with entries staged is refused.
func TestLSMMachineSourceRefusesAStagedCycle(t *testing.T) {
	m, err := OpenLSMMachine(filepath.Join(t.TempDir(), "lsm"), DefaultLimits, engineOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.ApplyEntry(1, 1, anonPut("a", "1")); err != nil {
		t.Fatal(err)
	}
	if src, err := m.OpenSnapshot(); err == nil {
		_ = src.Close()
		t.Fatal("a snapshot was opened inside a cycle")
	}
	if err := m.EndCycle(); err != nil {
		t.Fatal(err)
	}
	src, err := m.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if src.Index() != 1 || m.DurableAppliedIndex() != 1 {
		t.Fatalf("the source is at %d and the engine durable at %d; want both 1 — the checkpoint is durable", src.Index(), m.DurableAppliedIndex())
	}
	if err := m.RestoreSnapshotFrom(5, 1, errReader{}); !errors.Is(err, errRead) {
		t.Fatalf("a restore from a failing stream: %v", err)
	}
}

var errRead = errors.New("the stream failed")

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errRead }

// TestLSMMachineOpenRemovesAnS3SetAsideEngine: S3's restore set the old engine
// aside as <dir>.restore-aside before deleting it, and a crash between the two
// could leave it there. S4's restore — an ingest — sets nothing aside, but a
// directory an S3 binary wrote may still hold one: open removes it (nothing
// ever reads it) and the engine opens.
func TestLSMMachineOpenRemovesAnS3SetAsideEngine(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "lsm")
	aside := asidePath(dir)
	if err := os.MkdirAll(filepath.Join(aside, "wal"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(aside, "CURRENT"), []byte("MANIFEST-000009\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := OpenLSMMachine(dir, DefaultLimits, engineOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := os.Stat(aside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the engine an S3 restore set aside is still there: %v", err)
	}
}

// TestLSMMachineRefusesASessionItsEngineCannotStore: under a very large
// MaxUnacked a session can remember more results than one engine value holds.
// The in-memory store keeps such a session; the LSM machine refuses the
// snapshot at validation — before the core could install it and leave a node
// that fails to restore it at every start — and a restore of it changes
// nothing (S4 review).
func TestLSMMachineRefusesASessionItsEngineCannotStore(t *testing.T) {
	limits := Limits{MaxSessions: 2, MaxUnacked: 30000}
	ref := NewStoreWithLimits(limits)
	if _, err := ref.ApplyResult(1, register()); err != nil {
		t.Fatal(err)
	}
	n := (MaxValueLen-17-3)/sessionResultLen + 1 // results whose record exceeds one value
	for i := 0; i < n; i++ {
		if _, err := ref.ApplyResult(uint64(i+2), idPut(1, uint64(i+1), 1, "k", "v")); err != nil {
			t.Fatal(err)
		}
	}
	idx, data, err := ref.EncodeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := ref.ValidateSnapshot(idx, data); err != nil {
		t.Fatalf("premise: the in-memory store accepts the state: %v", err)
	}
	m, err := OpenLSMMachine(filepath.Join(t.TempDir(), "lsm"), limits, engineOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.ValidateSnapshot(idx, data); !errors.Is(err, ErrSnapshotState) || !strings.Contains(err.Error(), "value limit") {
		t.Fatalf("validating a session the engine cannot store: %v", err)
	}
	if err := m.RestoreSnapshot(idx, 1, data); !errors.Is(err, ErrSnapshotState) {
		t.Fatalf("restoring it: %v", err)
	}
	if m.Applied() != 0 {
		t.Fatalf("a refused restore moved the machine to %d", m.Applied())
	}
}
