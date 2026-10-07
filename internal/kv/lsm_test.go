package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// The LSM machine against the in-memory store, the executable reference (S2,
// docs/STORAGE_INTEGRATION.md §8): the same entries, decided and applied by
// both, must leave the same externally meaningful state after every cycle,
// across reopen, crash and restore.

// entry is one log entry: its index and term, and its command (nil: a no-op).
type entry struct {
	index, term uint64
	cmd         []byte
}

// pair drives both machines in lockstep.
type pair struct {
	t      *testing.T
	limits Limits
	opts   storage.Options
	dir    string
	mem    *Store
	lsm    *LSMMachine
	next   uint64 // the next index
	term   uint64
	log    []entry    // every entry applied, in order: the model replays it
	total  ApplyStats // the machine's counters of every incarnation so far
}

func addStats(a, b ApplyStats) ApplyStats {
	return ApplyStats{Executed: a.Executed + b.Executed, Duplicate: a.Duplicate + b.Duplicate, Conflict: a.Conflict + b.Conflict,
		Stale: a.Stale + b.Stale, Expired: a.Expired + b.Expired, Limit: a.Limit + b.Limit, Registered: a.Registered + b.Registered, Evicted: a.Evicted + b.Evicted}
}

func engineOpts(fs *fault.MemFS) storage.Options {
	o := storage.DefaultOptions()
	o.WAL.SyncMode = wal.SyncBatch
	o.WAL.SyncInterval = 1 << 62 // no timer: every fsync is the test's
	o.WAL.SyncBytes = 1 << 40
	o.DisableAutoCompaction = true
	if fs != nil {
		o.WAL.FS = fs
	}
	return o
}

func newPair(t *testing.T, limits Limits, fs *fault.MemFS) *pair {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "lsm")
	if fs != nil {
		// The engine's directory is durable in the model, as a data directory
		// on a real disk is; what the engine makes under it is up to it.
		if err := fs.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for d := dir; filepath.Dir(d) != d; d = filepath.Dir(d) {
			if err := fs.SyncDir(filepath.Dir(d)); err != nil {
				t.Fatal(err)
			}
		}
	}
	p := &pair{t: t, limits: limits, opts: engineOpts(fs), dir: dir, mem: NewStoreWithLimits(limits), next: 1, term: 1}
	p.open()
	return p
}

func (p *pair) open() {
	p.t.Helper()
	m, err := OpenLSMMachine(p.dir, p.limits, p.opts)
	if err != nil {
		p.t.Fatalf("OpenLSMMachine: %v", err)
	}
	p.lsm = m
}

// cycle applies entries as one cycle to both machines and compares them. It
// also checks the batch's size from the outside: the engine numbers every
// mutation, so the cycle's batch must have taken exactly one sequence per
// executed write, per session whose table entry changed (registered, marked
// used, its watermark raised, a result recorded) and per session evicted —
// no mutation twice, none left out, none the cycle did not cause.
func (p *pair) cycle(cmds ...[]byte) {
	p.t.Helper()
	before := p.mem.Sessions()
	seqBefore := p.lsm.db.Sequence()
	writes := 0
	for _, cmd := range cmds {
		e := entry{index: p.next, term: p.term, cmd: cmd}
		p.next++
		p.log = append(p.log, e)
		want, werr := p.mem.ApplyResult(e.index, e.cmd)
		if werr != nil {
			p.t.Fatalf("entry %d: the script's command is not valid: %v", e.index, werr)
		}
		got, gerr := p.lsm.ApplyEntry(e.index, e.term, e.cmd)
		if gerr != nil || !reflect.DeepEqual(want, got) {
			p.t.Fatalf("entry %d: the store decided %v, the machine %v (%v)", e.index, want, got, gerr)
		}
		if r, ok := want.(Result); ok && r.Decision == Executed {
			writes++
		}
	}
	if err := p.lsm.EndCycle(); err != nil {
		p.t.Fatalf("EndCycle after entry %d: %v", p.next-1, err)
	}
	after := p.mem.Sessions()
	changed, removed := 0, 0
	for id, st := range after {
		if old, ok := before[id]; !ok || !reflect.DeepEqual(old, st) {
			changed++
		}
	}
	for id := range before {
		if _, ok := after[id]; !ok {
			removed++
		}
	}
	if got, want := p.lsm.db.Sequence()-seqBefore, uint64(writes+changed+removed); got != want {
		p.t.Fatalf("the cycle through %d took %d sequence numbers: %d writes, %d sessions changed, %d evicted want %d",
			p.next-1, got, writes, changed, removed, want)
	}
	p.same("after the cycle through " + fmt.Sprint(p.next-1))
}

// same compares every externally meaningful fact of the two machines.
func (p *pair) same(when string) {
	p.t.Helper()
	sameMachines(p.t, when, p.mem, p.lsm, true)
}

// sameMachines compares the contents, every key's Lookup, the session table
// and the applied index of two machines, and their decision counters when
// stats is set. The counters are observability, not replicated state: the
// snapshot omits them, the in-memory store has them after a restart only
// because it replays the log, and a machine that recovered its state from its
// engine starts them at zero. So they are compared within one incarnation.
func sameMachines(t *testing.T, when string, ref, got Machine, stats bool) {
	t.Helper()
	a, err := ref.Contents()
	if err != nil {
		t.Fatal(err)
	}
	b, err := got.Contents()
	if err != nil {
		t.Fatalf("%s: Contents: %v", when, err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("%s: contents differ\n ref %v\n got %v", when, a, b)
	}
	for k, v := range a {
		gv, ok, err := got.Lookup([]byte(k))
		if err != nil || !ok || !bytes.Equal(gv, v) {
			t.Fatalf("%s: Lookup(%q) = %q, %v, %v; want %q", when, k, gv, ok, err, v)
		}
	}
	if _, ok, err := got.Lookup([]byte("never-written")); ok || err != nil {
		t.Fatalf("%s: Lookup of an absent key: %v, %v", when, ok, err)
	}
	if rs, gs := ref.Sessions(), got.Sessions(); !reflect.DeepEqual(rs, gs) {
		t.Fatalf("%s: sessions differ\n ref %v\n got %v", when, rs, gs)
	}
	if ra, ga := ref.Applied(), got.Applied(); ra != ga {
		t.Fatalf("%s: applied %d, want %d", when, ga, ra)
	}
	if rs, gs := ref.Stats(), got.Stats(); stats && rs != gs {
		t.Fatalf("%s: stats %+v, want %+v", when, gs, rs)
	}
}

// reopen closes the machine cleanly and opens it again on its engine. The
// reference's counters restart too, as a restarted machine's do, so the
// cycles that follow compare them again.
func (p *pair) reopen() {
	p.t.Helper()
	p.total = addStats(p.total, p.lsm.Stats())
	if err := p.lsm.Close(); err != nil {
		p.t.Fatal(err)
	}
	p.open()
	p.mem.sessions.stats = ApplyStats{}
	p.same("after a reopen")
}

// modelAfter replays the first n entries of the log into a fresh store: the
// state a machine that recovered exactly n entries must show.
func (p *pair) modelAfter(n int) *Store {
	s := NewStoreWithLimits(p.limits)
	for _, e := range p.log[:n] {
		_, _ = s.ApplyResult(e.index, e.cmd)
	}
	return s
}

func register() []byte { return Command{Op: OpRegister}.Encode() }
func idPut(cid, rid, acked uint64, k, v string) []byte {
	return Command{Op: OpPut, Key: []byte(k), Value: []byte(v), ClientID: cid, RequestID: rid, AckedBelow: acked}.Encode()
}
func idDel(cid, rid, acked uint64, k string) []byte {
	return Command{Op: OpDelete, Key: []byte(k), ClientID: cid, RequestID: rid, AckedBelow: acked}.Encode()
}
func anonPut(k, v string) []byte {
	return Command{Op: OpPut, Key: []byte(k), Value: []byte(v)}.Encode()
}
func anonDel(k string) []byte { return Command{Op: OpDelete, Key: []byte(k)}.Encode() }

// TestLSMMachineMatchesTheStoreOnAScript: a hand-written script that reaches
// every decision — registered, executed, duplicate, conflict, stale, expired,
// limit, evicted — with overwrites, deletes, no-ops and an empty value, cycle
// by cycle, and across reopens.
func TestLSMMachineMatchesTheStoreOnAScript(t *testing.T) {
	p := newPair(t, Limits{MaxSessions: 2, MaxUnacked: 2}, nil)
	p.cycle(register())                                 // 1: session 1
	p.cycle(idPut(1, 1, 1, "a", "1"), nil)              // 2 executed; 3 no-op
	p.cycle(idPut(1, 1, 1, "a", "1"))                   // 4 duplicate
	p.cycle(idPut(1, 1, 1, "a", "other"))               // 5 conflict
	p.cycle(idPut(1, 2, 1, "b", ""), anonPut("c", "3")) // 6 executed (empty value); 7 anonymous
	p.cycle(idPut(1, 3, 1, "d", "4"))                   // 8 limit: two results held
	p.cycle(idPut(1, 3, 3, "d", "4"))                   // 9 executed: watermark 3 forgets 1 and 2
	p.cycle(idPut(1, 1, 1, "a", "1"))                   // 10 stale: request 1 is below the watermark 3
	p.reopen()
	p.cycle(register(), idDel(11, 1, 1, "a"), anonDel("c"))               // 11 session; 12 executed; 13 anonymous delete
	p.cycle(register())                                                   // 14: a third session evicts the least recently used (1)
	p.cycle(idPut(1, 4, 3, "e", "5"))                                     // 15 expired
	p.cycle(idPut(14, 1, 1, "a", "again"), idPut(14, 1, 1, "a", "again")) // 16 executed, 17 duplicate in one cycle
	p.reopen()
	p.cycle(nil, nil, nil) // 18-20 no-ops only: the index still advances
	p.reopen()
	if st := addStats(p.total, p.lsm.Stats()); st.Executed == 0 || st.Duplicate == 0 || st.Conflict == 0 || st.Stale == 0 || st.Expired == 0 || st.Limit == 0 || st.Registered == 0 || st.Evicted == 0 {
		t.Fatalf("the script did not reach every decision: %+v", st)
	}
}

// TestLSMMachineMatchesTheStoreOnSeededScripts: random scripts over a few
// sessions, keys and request ids, with retries, watermarks, anonymous writes,
// no-ops, cycles of varying length and a reopen every so often.
func TestLSMMachineMatchesTheStoreOnSeededScripts(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			p := newPair(t, Limits{MaxSessions: 3, MaxUnacked: 3}, nil)
			var sessions []uint64
			nextRID := map[uint64]uint64{}
			keys := []string{"a", "b", "c", "d"}
			for step := 0; step < 60; step++ {
				n := 1 + rng.Intn(4)
				var cmds [][]byte
				for i := 0; i < n; i++ {
					switch r := rng.Intn(10); {
					case r == 0 || len(sessions) == 0:
						cmds = append(cmds, register())
						sessions = append(sessions, p.next+uint64(len(cmds))-1)
					case r == 1:
						cmds = append(cmds, nil)
					case r == 2:
						cmds = append(cmds, anonPut(keys[rng.Intn(len(keys))], fmt.Sprint(step, i)))
					default:
						cid := sessions[rng.Intn(len(sessions))]
						rid := nextRID[cid] + 1
						if rng.Intn(3) == 0 && rid > 1 {
							rid-- // a retry: a duplicate, or a conflict with another value
						} else {
							nextRID[cid] = rid
						}
						acked := uint64(1)
						if rid > 2 && rng.Intn(2) == 0 {
							acked = rid - 1
						}
						k := keys[rng.Intn(len(keys))]
						if rng.Intn(4) == 0 {
							cmds = append(cmds, idDel(cid, rid, acked, k))
						} else {
							cmds = append(cmds, idPut(cid, rid, acked, k, fmt.Sprint(step, i, rng.Intn(2))))
						}
					}
				}
				if rng.Intn(10) == 0 {
					p.term++
				}
				p.cycle(cmds...)
				if rng.Intn(12) == 0 {
					p.reopen()
				}
			}
			p.reopen()
		})
	}
}

// TestLSMMachineSkipsWhatItsEngineHolds: after a reopen the engine's applied
// index is the authority — an entry at or below it is skipped with no result
// and no effect, so the driver's at-least-once replay applies each entry once.
func TestLSMMachineSkipsWhatItsEngineHolds(t *testing.T) {
	p := newPair(t, Limits{MaxSessions: 4, MaxUnacked: 4}, nil)
	p.cycle(register(), idPut(1, 1, 1, "a", "1"))
	p.cycle(idPut(1, 2, 1, "b", "2"), nil)
	p.reopen()
	if e := p.lsm.EngineApplied(); e.Index != 4 || e.Term != 1 {
		t.Fatalf("engine applied %+v, want (4, 1)", e)
	}
	// Replaying the log from the start, as the driver does from a snapshot
	// below the engine's index, changes nothing — not even the counters.
	for _, e := range p.log {
		r, err := p.lsm.ApplyEntry(e.index, e.term, e.cmd)
		if r != nil || err != nil {
			t.Fatalf("replaying entry %d: %v, %v; want nothing", e.index, r, err)
		}
	}
	if err := p.lsm.EndCycle(); err != nil {
		t.Fatal(err)
	}
	p.same("after replaying the recovered prefix")
	if e := p.lsm.EngineApplied(); e.Index != 4 {
		t.Fatalf("a replayed prefix moved the engine to %+v", e)
	}
	// The next entry is applied: replaying a REGISTER at 1 would have reset
	// session 1 and turned request 3 into a first execution with no history.
	p.cycle(idPut(1, 1, 1, "a", "1")) // 5: a duplicate of index 2
	if st := p.lsm.Stats(); st.Duplicate != 1 {
		t.Fatalf("stats %+v: the retry must be a duplicate", st)
	}
}

// TestLSMMachineRecoversAPrefixAfterAPowerLoss (R1 through the machine): with
// the engine's WAL on the crash model, a power loss keeps the cycles fsynced
// and drops the rest, and the machine recovers exactly the state after that
// prefix of entries — contents, sessions and index together — then goes on.
func TestLSMMachineRecoversAPrefixAfterAPowerLoss(t *testing.T) {
	for _, crash := range []string{"process", "power"} {
		t.Run(crash, func(t *testing.T) {
			mem := fault.NewMemFS()
			p := newPair(t, Limits{MaxSessions: 4, MaxUnacked: 4}, mem)
			p.cycle(register(), idPut(1, 1, 1, "a", "1"))
			p.cycle(idPut(1, 2, 1, "b", "2"))
			if err := p.lsm.db.Sync(); err != nil {
				t.Fatal(err)
			}
			synced := len(p.log)
			p.cycle(register(), idPut(4, 1, 1, "a", "3"), anonDel("b"))
			p.cycle(nil, idPut(1, 3, 2, "c", "4"))
			if crash == "process" {
				mem.CrashProcess()
			} else {
				mem.CrashPowerLoss(0)
			}
			_ = p.lsm.Close()
			p.open()
			want := len(p.log)
			if crash == "power" {
				want = synced
			}
			if got := int(p.lsm.Applied()); got != want {
				t.Fatalf("recovered through index %d, want %d", got, want)
			}
			// A second restart right after the recovery reads the same state.
			_ = p.lsm.Close()
			p.open()
			if got := int(p.lsm.Applied()); got != want {
				t.Fatalf("the second restart recovered through index %d, want %d", got, want)
			}
			sameMachines(t, "after the "+crash+" crash", p.modelAfter(want), p.lsm, false)
			// The lost entries come back from the log; the kept ones are skipped.
			for _, e := range p.log {
				if _, err := p.lsm.ApplyEntry(e.index, e.term, e.cmd); err != nil {
					t.Fatal(err)
				}
			}
			if err := p.lsm.EndCycle(); err != nil {
				t.Fatal(err)
			}
			p.mem.sessions.stats = ApplyStats{}
			p.lsm.sessions.stats = ApplyStats{}
			p.same("after replaying the log")
			p.reopen()
		})
	}
}

// TestLSMMachineAFailedCyclePublishesNothingDurable: a cycle whose engine
// batch fails reports the failure, records nothing (a reopen shows the state
// before it), and refuses further work — the node fail-stops on it.
func TestLSMMachineAFailedCyclePublishesNothingDurable(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	p := newPair(t, Limits{MaxSessions: 4, MaxUnacked: 4}, nil)
	p.opts.WAL.FS = inj
	_ = p.lsm.Close()
	p.open()
	p.cycle(register(), idPut(1, 1, 1, "a", "1"))
	before := p.modelAfter(len(p.log))
	inj.Arm(fault.Injection{Op: fault.OpWrite})
	for _, cmd := range [][]byte{idPut(1, 2, 1, "b", "2"), register()} {
		if _, err := p.lsm.ApplyEntry(p.next, 1, cmd); err != nil {
			t.Fatal(err)
		}
		p.next++
	}
	if err := p.lsm.EndCycle(); err == nil {
		t.Fatal("a cycle whose batch failed ended without error")
	}
	if _, err := p.lsm.ApplyEntry(p.next, 1, anonPut("c", "3")); err == nil {
		if err := p.lsm.EndCycle(); err == nil {
			t.Fatal("the engine's failure did not latch")
		}
	}
	mem.CrashProcess()
	_ = p.lsm.Close()
	p.opts.WAL.FS = mem
	p.open()
	sameMachines(t, "after the failed cycle", before, p.lsm, false)
}

// TestLSMMachineRestoresASnapshot: a snapshot the store encoded restores into
// the machine — the state, the sessions, the index and the term — and the
// machine goes on from it; a snapshot at or below the engine's index changes
// nothing; a restore whose batch fails leaves an empty engine at index 0,
// which the next restore replaces.
func TestLSMMachineRestoresASnapshot(t *testing.T) {
	limits := Limits{MaxSessions: 4, MaxUnacked: 4}
	ref := NewStoreWithLimits(limits)
	for i, cmd := range [][]byte{register(), idPut(1, 1, 1, "a", "1"), register(), idPut(3, 1, 1, "b", "2"), idDel(1, 2, 1, "a"), idPut(1, 3, 3, "c", "")} {
		if _, err := ref.ApplyResult(uint64(i+1), cmd); err != nil {
			t.Fatal(err)
		}
	}
	idx, data, err := ref.EncodeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	p := newPair(t, limits, nil)
	p.opts.WAL.FS = inj
	_ = p.lsm.Close()
	p.open()
	if err := p.lsm.ValidateSnapshot(idx, data); err != nil {
		t.Fatal(err)
	}
	// The restore's batch fails: the engine is empty at 0.
	inj.Arm(fault.Injection{Op: fault.OpWrite})
	if err := p.lsm.RestoreSnapshot(idx, 3, data); err == nil {
		t.Fatal("a restore whose batch failed succeeded")
	}
	mem.CrashProcess()
	_ = p.lsm.Close()
	p.opts.WAL.FS = mem
	p.open()
	if e := p.lsm.EngineApplied(); e.Index != 0 {
		t.Fatalf("after a failed restore the engine is at %+v, want 0", e)
	}
	if err := p.lsm.RestoreSnapshot(idx, 3, data); err != nil {
		t.Fatal(err)
	}
	if e := p.lsm.EngineApplied(); e.Index != idx || e.Term != 3 {
		t.Fatalf("engine applied %+v, want (%d, 3)", e, idx)
	}
	sameMachines(t, "after the restore", ref, p.lsm, false)
	p.mem, p.next, p.term = ref, idx+1, 3
	p.log = nil
	p.reopen()
	// Below or at the engine's index: nothing changes.
	if err := p.lsm.RestoreSnapshot(idx, 3, data); err != nil {
		t.Fatal(err)
	}
	p.same("after a restore at the engine's index")
	p.cycle(idPut(1, 4, 3, "d", "4"), idPut(3, 1, 1, "b", "2")) // executed; duplicate of the restored session's result
	if st := p.lsm.Stats(); st.Duplicate != 1 {
		t.Fatalf("stats %+v: the restored session must remember its result", st)
	}
	p.reopen()
	// The machine's own snapshot is the store's, byte for byte.
	gi, gd, err := p.lsm.EncodeSnapshot()
	ri, rd, _ := p.mem.EncodeSnapshot()
	if err != nil || gi != ri || !bytes.Equal(gd, rd) {
		t.Fatalf("the machine's snapshot at %d differs from the store's at %d (%v)", gi, ri, err)
	}
	// Too large for one batch: refused before the core could install it.
	big := NewStoreWithLimits(limits)
	v := bytes.Repeat([]byte("v"), MaxValueLen/2) // a value that fits one command with room for its key
	for i, total := 0, 0; total <= maxRestoreBytes; i, total = i+1, total+len(v) {
		if _, err := big.ApplyResult(uint64(i+1), anonPut(fmt.Sprintf("k%03d", i), string(v))); err != nil {
			t.Fatal(err)
		}
	}
	bi, bd, _ := big.EncodeSnapshot()
	if err := p.lsm.ValidateSnapshot(bi, bd); !errors.Is(err, ErrSnapshotState) {
		t.Fatalf("a state too large for one batch: %v, want ErrSnapshotState", err)
	}
}

// TestLSMMachineRefusesAForeignSessionRecord: a session record the machine
// did not write — malformed, or a session the limits cannot hold — refuses
// the open rather than being guessed at.
func TestLSMMachineRefusesAForeignSessionRecord(t *testing.T) {
	limits := Limits{MaxSessions: 2, MaxUnacked: 2}
	dir := filepath.Join(t.TempDir(), "lsm")
	p := &pair{t: t, limits: limits, opts: engineOpts(nil), dir: dir, mem: NewStoreWithLimits(limits), next: 1, term: 1}
	p.open()
	p.cycle(register(), idPut(1, 1, 1, "a", "1"))
	_ = p.lsm.Close()
	// Damage the record behind the machine's back, through the engine.
	db, err := storage.OpenLSMStore(dir, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Apply(context.Background(), []storage.Mutation{{Kind: storage.MutationPut, Key: sessionKey(1), Value: []byte{9, 9}}}, storage.AppliedIndex{Index: 3, Term: 1}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := OpenLSMMachine(dir, limits, p.opts); !errors.Is(err, ErrSessionRecord) {
		t.Fatalf("open with a malformed session record: %v, want ErrSessionRecord", err)
	}
	// A machine started as memory never sees the engine, but the directory
	// is there for an operator to notice (dkvd refuses the mismatch).
	if _, err := os.Stat(filepath.Join(dir, "CURRENT")); err != nil {
		t.Fatalf("the engine directory: %v", err)
	}
}
