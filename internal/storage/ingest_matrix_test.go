package storage_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// The ingest crash matrix (S4, docs/STORAGE_INTEGRATION.md §10.6–§10.8): the
// whole engine — tables, directory, MANIFEST, WAL — on the crash model, an
// ingest replacing an old state, then a batch and a flush after it; a crash at
// every filesystem operation from the ingest on (a process crash, a power
// loss, a power loss keeping a torn prefix of what was not fsynced; a crashing
// write whole or torn halfway), and an injected I/O error at every operation of
// the ingest followed by a crash. Every cell must recover EXACTLY the old state,
// exactly the new one, or the new one with the batch after it — never a
// mixture — at least what was acknowledged, with no table the MANIFEST does not
// name left on disk; a second crash right after recovery must change nothing;
// and the engine must go on.

const ingestMatrixKeys = 40

// ingestModels are the states a cell may recover: before the ingest, after
// it, and after the batch that follows it.
type ingestModels struct {
	old, new, post applyModel
	keys           []string
}

func newIngestModels() ingestModels {
	var m ingestModels
	m.old = newApplyModel()
	for _, st := range ingestBaseScript() {
		m.old = m.old.after(st)
	}
	m.new = applyModel{state: map[string]string{}, seq: m.old.seq + ingestMatrixKeys, applied: ai(100, 3)}
	for _, e := range newContents(ingestMatrixKeys) {
		m.new.state[e.k] = e.v
	}
	m.new.appliedSeq = m.new.seq
	m.post = m.new.after(ingestPostBatch())
	seen := map[string]bool{}
	for _, mm := range []applyModel{m.old, m.new, m.post} {
		for k := range mm.state {
			seen[k] = true
		}
	}
	for _, st := range ingestBaseScript() {
		for _, mu := range st.muts {
			seen[string(mu.Key)] = true
		}
	}
	for k := range seen {
		m.keys = append(m.keys, k)
	}
	sort.Strings(m.keys)
	return m
}

// ingestBaseScript is the old state's batches (ingestBase's, with a flush
// after the sixth).
func ingestBaseScript() []applyStep {
	var out []applyStep
	for i := 1; i <= 12; i++ {
		st := applyStep{[]storage.Mutation{mput(fmt.Sprintf("o%02d", i), "old"), mput(fmt.Sprintf("n%02d", i), "stale")}, ai(uint64(i), 1)}
		if i%4 == 0 {
			st.muts = append(st.muts, mdel(fmt.Sprintf("o%02d", i-1)))
		}
		out = append(out, st)
	}
	return out
}

func ingestPostBatch() applyStep {
	return applyStep{[]storage.Mutation{mput("post", "1"), mdel("n03")}, ai(101, 3)}
}

func ingestMatrixOpts(mode wal.SyncMode, fsys *fault.InjectFS) storage.Options {
	o := lsmOpts(256) // several tables per ingest
	o.FS = fsys       // the whole engine on the model
	o.WAL.SyncMode = mode
	o.WAL.SyncInterval = time.Duration(1 << 62) // batch mode: no timer fsync — every one is the script's
	o.WAL.SyncBytes = 1 << 40
	o.WAL.SegmentSize = 96
	return o
}

// ingestOutcome is how far a run got before it died.
type ingestOutcome struct {
	baseDone    bool // every old batch acknowledged and synced: durable
	ingested    bool // Ingest returned nil
	attempted   bool // Ingest was called
	postDone    bool // the batch after it was acknowledged (written)
	postDurable bool // ... and durable: sync mode, or the flush after it succeeded
}

// runIngestScript runs the script until the process dies (or to its end),
// arming armAt just before the ingest.
func runIngestScript(t *testing.T, s *storage.LSMStore, cfs *crashFS, inj *fault.InjectFS, armAt *fault.Injection, afterIngest bool, mode wal.SyncMode) (o ingestOutcome) {
	ctx := context.Background()
	for i, st := range ingestBaseScript() {
		if err := s.Apply(ctx, st.muts, st.applied); err != nil {
			t.Fatalf("premise: the old state's batch %d: %v", i, err)
		}
		if i == 5 {
			if err := s.Flush(); err != nil {
				t.Fatalf("premise: the old state's flush: %v", err)
			}
		}
	}
	if err := s.Sync(); err != nil { // the old state durable, whatever the mode
		t.Fatalf("premise: syncing the old state: %v", err)
	}
	o.baseDone = true
	if armAt != nil {
		inj.Arm(*armAt)
	}
	o.attempted = true
	_, err := s.Ingest(source(newContents(ingestMatrixKeys)...), ai(100, 3))
	o.ingested = err == nil
	if cfs.isDead() || err != nil || afterIngest {
		return o
	}
	post := ingestPostBatch()
	if err := s.Apply(ctx, post.muts, post.applied); err == nil {
		o.postDone = true
		o.postDurable = mode == wal.SyncAlways
	}
	if !cfs.isDead() {
		if err := s.Flush(); err == nil && o.postDone {
			o.postDurable = true
		}
	}
	return o
}

// ingestDryRun runs the script with no crash: the number of crashFS
// operations, their names, and where the ingest's begin; and the InjectFS
// records of the ingest's own operations.
func ingestDryRun(t *testing.T, mode wal.SyncMode) (nOps int, names []string, ingestStart int, ingestOps []fault.OpRecord) {
	t.Helper()
	dir := t.TempDir()
	mem := fault.NewMemFS()
	durableStoreRoot(t, mem, dir)
	cfs := &crashFS{mem: mem}
	inj := fault.NewInjectFS(cfs)
	s := openLSM(t, dir, ingestMatrixOpts(mode, inj))
	defer s.Close()
	ctx := context.Background()
	for i, st := range ingestBaseScript() {
		mustApply(t, s, st)
		if i == 5 {
			mustFlush(t, s)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	ingestStart = cfs.n
	before := len(inj.Ops())
	stats, err := s.Ingest(source(newContents(ingestMatrixKeys)...), ai(100, 3))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tables < 3 || stats.Retired == 0 {
		t.Fatalf("premise: the ingest writes several tables and retires old ones, got %+v", stats)
	}
	ingestOps = append(ingestOps, inj.Ops()[before:]...)
	post := ingestPostBatch()
	if err := s.Apply(ctx, post.muts, post.applied); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)
	return cfs.n, append([]string(nil), cfs.ops...), ingestStart, ingestOps
}

// ingestCell is one run of the script under a fault.
type ingestCell struct {
	mode  wal.SyncMode
	at    int  // crash at this crashFS operation (0: none)
	torn  bool // a crashing write is torn halfway
	after bool // or crash right after Ingest returns
	kind  crashKind
	inj   *fault.Injection // or an injected I/O error during the ingest, then a crash
	what  string           // the operation the fault hit
	phase string           // inj: "before-commit", "commit-write", "commit-sync", "after-commit"
}

func (c ingestCell) String() string {
	switch {
	case c.inj != nil:
		return fmt.Sprintf("%s/error at %s (%s)/%s", c.mode, c.what, c.phase, c.kind.name)
	case c.after:
		return fmt.Sprintf("%s/after the ingest returned/%s", c.mode, c.kind.name)
	}
	torn := ""
	if c.torn {
		torn = " torn"
	}
	return fmt.Sprintf("%s/crash at op %d [%s]%s/%s", c.mode, c.at, c.what, torn, c.kind.name)
}

// classify reports which model s holds, or why it holds none.
func (m ingestModels) classify(s *storage.LSMStore) (string, string) {
	for _, c := range []struct {
		name  string
		model applyModel
	}{{"old", m.old}, {"new", m.new}, {"post", m.post}} {
		if diff(s, c.model, m.keys) == "" {
			return c.name, ""
		}
	}
	return "", fmt.Sprintf("old: %s | new: %s | post: %s",
		diff(s, m.old, m.keys), diff(s, m.new, m.keys), diff(s, m.post, m.keys))
}

// noUnreferencedFiles fails if the directory holds a table the MANIFEST does
// not name, or a temporary, after an open (which sweeps them).
func noUnreferencedFiles(t *testing.T, mem *fault.MemFS, dir string, s *storage.LSMStore, cell ingestCell) {
	t.Helper()
	live := map[string]bool{}
	for _, info := range s.SSTables() {
		live[fmt.Sprintf("%06d.sst", info.Number)] = true
	}
	entries, err := mem.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		n := e.Name()
		if strings.HasSuffix(n, ".tmp") || (strings.HasSuffix(n, ".sst") && !live[n]) {
			t.Fatalf("%s: after recovery the directory still holds %s, which the MANIFEST does not name", cell, n)
		}
	}
	for _, info := range s.SSTables() {
		if _, ok := mem.Cached(filepath.Join(dir, fmt.Sprintf("%06d.sst", info.Number))); !ok {
			t.Fatalf("%s: the live table %d is missing", cell, info.Number)
		}
	}
}

func runIngestCell(t *testing.T, cell ingestCell, models ingestModels) string {
	t.Helper()
	dir := t.TempDir()
	mem := fault.NewMemFS()
	durableStoreRoot(t, mem, dir)
	cfs := &crashFS{mem: mem, at: cell.at, torn: cell.torn}
	inj := fault.NewInjectFS(cfs)
	s, err := storage.OpenLSMStore(dir, ingestMatrixOpts(cell.mode, inj))
	if err != nil {
		t.Fatalf("%s: premise: the first open: %v", cell, err)
	}
	o := runIngestScript(t, s, cfs, inj, cell.inj, cell.after, cell.mode)
	inj.Disarm()
	cfs.kill()
	if cell.kind.power {
		mem.CrashPowerLoss(cell.kind.torn)
	}
	_ = s.Close()

	// 1. The engine opens on what the crash left.
	cfs.revive(0)
	s, err = storage.OpenLSMStore(dir, ingestMatrixOpts(cell.mode, inj))
	if err != nil {
		t.Fatalf("%s: the store does not open after the crash: %v", cell, err)
	}
	// 2. Exactly one whole state — never a mixture of the old and the new.
	got, why := models.classify(s)
	if got == "" {
		t.Fatalf("%s: the recovered state is neither the old, the new, nor the new plus the next batch: %s", cell, why)
	}
	// 3. At least what was acknowledged; nothing never attempted.
	allowed := map[string]bool{"old": true, "new": true}
	switch {
	case o.postDurable, o.postDone && !cell.kind.power: // a process crash keeps what was written
		allowed = map[string]bool{"post": true}
	case o.ingested:
		allowed = map[string]bool{"new": true, "post": true}
	case cell.inj != nil && (cell.phase == "before-commit" || cell.phase == "commit-write"):
		allowed = map[string]bool{"old": true} // the edit never reached the file whole
	}
	if !allowed[got] {
		t.Fatalf("%s: recovered %q; the run acknowledged %+v", cell, got, o)
	}
	noUnreferencedFiles(t, mem, dir, s, cell)
	// 4. Recovered implies durable: a power loss right after the open changes nothing.
	cfs.kill()
	mem.CrashPowerLoss(0)
	_ = s.Close()
	cfs.revive(0)
	s, err = storage.OpenLSMStore(dir, ingestMatrixOpts(cell.mode, inj))
	if err != nil {
		t.Fatalf("%s: after a second, immediate power loss the store does not open: %v", cell, err)
	}
	if again, _ := models.classify(s); again != got {
		t.Fatalf("%s: recovered %q, and after a second crash right after it %q", cell, got, again)
	}
	// 5. The engine goes on.
	m := map[string]applyModel{"old": models.old, "new": models.new, "post": models.post}[got]
	next := applyStep{[]storage.Mutation{mput("after", "crash")}, ai(m.applied.Index+1, m.applied.Term)}
	if err := s.Apply(context.Background(), next.muts, next.applied); err != nil {
		t.Fatalf("%s: the next batch after recovery: %v", cell, err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("%s: a flush after recovery: %v", cell, err)
	}
	if d := diff(s, m.after(next), append(models.keys, "after")); d != "" {
		t.Fatalf("%s: after the next batch: %s", cell, d)
	}
	_ = s.Close()
	return got
}

// TestIngestCrashMatrix runs every cell.
func TestIngestCrashMatrix(t *testing.T) {
	models := newIngestModels()
	var cells []ingestCell
	totalOps, totalInj := 0, 0
	for _, mode := range []wal.SyncMode{wal.SyncAlways, wal.SyncBatch} {
		nOps, names, start, ingestOps := ingestDryRun(t, mode)
		totalOps += nOps - start
		totalInj += len(ingestOps)
		for _, kind := range crashKinds {
			for at := start + 1; at <= nOps; at++ {
				cells = append(cells, ingestCell{mode: mode, at: at, kind: kind, what: names[at-1]})
				if strings.HasPrefix(names[at-1], "write") {
					cells = append(cells, ingestCell{mode: mode, at: at, torn: true, kind: kind, what: names[at-1]})
				}
			}
			cells = append(cells, ingestCell{mode: mode, after: true, kind: kind})
			// An injected error at every operation of the ingest itself, then a crash.
			phase := "before-commit"
			seen := map[string]int{}
			for _, r := range ingestOps {
				key := fmt.Sprintf("%s %s", r.Op, r.Path)
				seen[key]++
				isManifest := strings.HasPrefix(filepath.Base(r.Path), "MANIFEST-")
				p := phase
				switch {
				case isManifest && r.Op == fault.OpWrite && phase == "before-commit":
					p = "commit-write"
				case isManifest && r.Op == fault.OpSync && phase == "before-commit":
					p, phase = "commit-sync", "after-commit"
				}
				if r.Op == fault.OpOpen && !strings.HasSuffix(r.Path, ".tmp") && !strings.HasSuffix(r.Path, ".log") {
					continue // a read-only open of a table; its failure is a read error, covered elsewhere
				}
				inj := fault.Injection{Op: r.Op, Path: r.Path, Nth: seen[key]}
				cells = append(cells, ingestCell{mode: mode, inj: &inj, kind: kind, phase: p, what: fmt.Sprintf("%s %s #%d", r.Op, filepath.Base(r.Path), seen[key])})
			}
		}
	}
	dist := map[string]int{}
	byOp := map[string]map[string]bool{} // the operation a fault hit -> the states recovered
	var opOrder []string
	for _, cell := range cells {
		got := runIngestCell(t, cell, models)
		dist[cell.mode.String()+"/"+cell.kind.name+" -> "+got]++
		if cell.at > 0 {
			key := fmt.Sprintf("%s op %3d %s", cell.mode, cell.at, opClass(cell.what))
			if byOp[key] == nil {
				byOp[key] = map[string]bool{}
				opOrder = append(opOrder, key)
			}
			byOp[key][got] = true
		}
	}
	sort.Strings(opOrder)
	var ops []string
	for _, k := range opOrder {
		var outs []string
		for o := range byOp[k] {
			outs = append(outs, o)
		}
		sort.Strings(outs)
		ops = append(ops, fmt.Sprintf("%s -> %s", k, strings.Join(outs, "|")))
	}
	t.Logf("a crash before each operation (any kind) recovers:\n  %s", strings.Join(ops, "\n  "))
	var lines []string
	for k, v := range dist {
		lines = append(lines, fmt.Sprintf("%s: %d", k, v))
	}
	sort.Strings(lines)
	t.Logf("%d cells (sync and batch modes: %d filesystem operations of the ingest and after it, %d ingest operations with an injected error), "+
		"each recovering exactly one whole state, durably, and going on:\n  %s",
		len(cells), totalOps, totalInj, strings.Join(lines, "\n  "))
}

// opClass names an operation without its byte count, for the summary.
func opClass(what string) string {
	f := strings.Fields(what)
	if len(f) >= 2 {
		return f[0] + " " + f[1]
	}
	return what
}
