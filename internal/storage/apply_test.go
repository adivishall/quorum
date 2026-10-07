package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// S1: apply batches in the engine (docs/STORAGE_INTEGRATION.md §7).

func mput(k, v string) storage.Mutation {
	return storage.Mutation{Kind: storage.MutationPut, Key: []byte(k), Value: []byte(v)}
}
func mdel(k string) storage.Mutation {
	return storage.Mutation{Kind: storage.MutationDelete, Key: []byte(k)}
}
func ai(index, term uint64) storage.AppliedIndex {
	return storage.AppliedIndex{Index: index, Term: term}
}

// applyStep is one apply batch of a script.
type applyStep struct {
	muts    []storage.Mutation
	applied storage.AppliedIndex
}

// applyModel is the reference: what a store holds after a prefix of a script.
type applyModel struct {
	state      map[string]string
	seq        uint64 // every mutation applied, each one a sequence number
	applied    storage.AppliedIndex
	appliedSeq uint64
}

func newApplyModel() applyModel { return applyModel{state: map[string]string{}} }

func (m applyModel) after(st applyStep) applyModel {
	next := applyModel{state: map[string]string{}, seq: m.seq, applied: st.applied}
	for k, v := range m.state {
		next.state[k] = v
	}
	for _, mu := range st.muts {
		next.seq++
		if mu.Kind == storage.MutationDelete {
			delete(next.state, string(mu.Key))
		} else {
			next.state[string(mu.Key)] = string(mu.Value)
		}
	}
	next.appliedSeq = next.seq
	return next
}

// models returns the reference after each prefix of script: models[i] is the
// state after i batches.
func models(script []applyStep) []applyModel {
	out := []applyModel{newApplyModel()}
	for _, st := range script {
		out = append(out, out[len(out)-1].after(st))
	}
	return out
}

func keysOf(script []applyStep) []string {
	seen := map[string]bool{}
	for _, st := range script {
		for _, mu := range st.muts {
			seen[string(mu.Key)] = true
		}
	}
	var keys []string
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// diff reports how s differs from m over keys, or "" if it does not.
func diff(s *storage.LSMStore, m applyModel, keys []string) string {
	if got := s.AppliedIndex(); got != m.applied {
		return fmt.Sprintf("applied index %+v, want %+v", got, m.applied)
	}
	if got := s.AppliedSequence(); got != m.appliedSeq {
		return fmt.Sprintf("applied sequence %d, want %d", got, m.appliedSeq)
	}
	if got := s.Sequence(); got != m.seq {
		return fmt.Sprintf("sequence %d, want %d", got, m.seq)
	}
	for _, k := range keys {
		v, err := s.Get(context.Background(), []byte(k))
		want, present := m.state[k]
		switch {
		case present && err != nil:
			return fmt.Sprintf("key %q: %v, want %q", k, err, want)
		case !present && !errors.Is(err, storage.ErrNotFound):
			return fmt.Sprintf("key %q: %q (%v), want absent", k, v, err)
		case present && string(v) != want:
			return fmt.Sprintf("key %q: %q, want %q", k, v, want)
		}
	}
	return ""
}

func mustApply(t *testing.T, s *storage.LSMStore, st applyStep) {
	t.Helper()
	if err := s.Apply(context.Background(), st.muts, st.applied); err != nil {
		t.Fatalf("Apply(%+v): %v", st.applied, err)
	}
}

func applyScript() []applyStep {
	return []applyStep{
		{[]storage.Mutation{mput("a", "1")}, ai(1, 1)},
		{[]storage.Mutation{mput("b", "2"), mput("c", "3"), mdel("a")}, ai(2, 1)},
		{nil, ai(3, 2)}, // a no-op entry: the index alone advances
		{[]storage.Mutation{mput("a", "4"), mdel("a"), mput("a", "5")}, ai(7, 2)}, // the last write to a key in a batch wins
		{[]storage.Mutation{mdel("b"), mput("d", "")}, ai(8, 3)},                  // an empty value is a present key
	}
}

// TestApplyBatchesAndRecovery: batches of puts, deletes and both, and an empty
// one, are visible as the model says — state, applied index, the sequence the
// index covers and the sequence — before and after a reopen, which replays
// them from the WAL, and again after a flush moved them into a table.
func TestApplyBatchesAndRecovery(t *testing.T) {
	script := applyScript()
	want := models(script)
	keys := keysOf(script)
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	if d := diff(s, want[0], keys); d != "" {
		t.Fatalf("a new store: %s", d)
	}
	for i, st := range script {
		mustApply(t, s, st)
		if d := diff(s, want[i+1], keys); d != "" {
			t.Fatalf("after batch %d: %s", i+1, d)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	if d := diff(s, want[len(script)], keys); d != "" {
		t.Fatalf("after a reopen: %s", d)
	}
	if r := s.Recovery(); r.ApplyBatches != int64(len(script)) || r.Sequence != want[len(script)].seq {
		t.Fatalf("recovery replayed %d apply batches to sequence %d", r.ApplyBatches, r.Sequence)
	}
	mustFlush(t, s)
	more := applyStep{[]storage.Mutation{mput("e", "6")}, ai(9, 3)}
	mustApply(t, s, more)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer s.Close()
	final := want[len(script)].after(more)
	if d := diff(s, final, append(keys, "e")); d != "" {
		t.Fatalf("after a flush and a reopen: %s", d)
	}
	if r := s.Recovery(); r.OpsSkipped != int64(want[len(script)].seq) {
		t.Fatalf("replay skipped %d mutations already in a table, want %d", r.OpsSkipped, want[len(script)].seq)
	}
}

// TestTheAppliedIndexMustAdvance: an index that is not above the current one,
// a term below the current one, or a zero index or term is refused with
// ErrAppliedIndex, and nothing is written — the WAL, the sequence and the
// state are as they were.
func TestTheAppliedIndexMustAdvance(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer s.Close()
	mustApply(t, s, applyStep{[]storage.Mutation{mput("k", "v")}, ai(5, 2)})
	bytesBefore := s.WALStats().ActiveBytes
	for _, bad := range []storage.AppliedIndex{ai(5, 2), ai(4, 2), ai(6, 1), ai(0, 2), ai(6, 0), ai(0, 0)} {
		err := s.Apply(context.Background(), []storage.Mutation{mput("k", "bad")}, bad)
		if !errors.Is(err, storage.ErrAppliedIndex) {
			t.Fatalf("Apply at %+v after (5, 2): %v, want ErrAppliedIndex", bad, err)
		}
	}
	if d := diff(s, newApplyModel().after(applyStep{[]storage.Mutation{mput("k", "v")}, ai(5, 2)}), []string{"k"}); d != "" {
		t.Fatalf("a refused batch changed the store: %s", d)
	}
	if got := s.WALStats().ActiveBytes; got != bytesBefore {
		t.Fatalf("a refused batch wrote %d bytes", got-bytesBefore)
	}
	// The same term, a higher index; and a higher term — both advance.
	mustApply(t, s, applyStep{[]storage.Mutation{mput("k", "w")}, ai(6, 2)})
	mustApply(t, s, applyStep{nil, ai(7, 9)})
}

// TestApplyRefusesInvalidMutations: an unknown kind, a delete carrying a value
// and an invalid key or value are refused before anything is written, and a
// refused batch is never partly applied.
func TestApplyRefusesInvalidMutations(t *testing.T) {
	o := lsmOpts(storage.DefaultMemTableSize)
	o.MaxKeySize, o.MaxValueSize = 8, 8
	s := openLSM(t, t.TempDir(), o)
	defer s.Close()
	for name, c := range map[string]struct {
		mut  storage.Mutation
		want error
	}{
		"unknown kind":         {storage.Mutation{Kind: 9, Key: []byte("k")}, storage.ErrInvalidBatch},
		"zero kind":            {storage.Mutation{Key: []byte("k")}, storage.ErrInvalidBatch},
		"delete with a value":  {storage.Mutation{Kind: storage.MutationDelete, Key: []byte("k"), Value: []byte("v")}, storage.ErrInvalidBatch},
		"empty key":            {mput("", "v"), storage.ErrKeyEmpty},
		"key too large":        {mput("123456789", "v"), storage.ErrKeyTooLarge},
		"value too large":      {mput("k", "123456789"), storage.ErrValueTooLarge},
		"empty key in a del":   {mdel(""), storage.ErrKeyEmpty},
		"key too large in del": {mdel("123456789"), storage.ErrKeyTooLarge},
	} {
		// The bad mutation follows a good one: neither may be applied.
		err := s.Apply(context.Background(), []storage.Mutation{mput("good", "1"), c.mut}, ai(1, 1))
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: %v, want %v", name, err, c.want)
		}
	}
	if d := diff(s, newApplyModel(), []string{"good", "k"}); d != "" || s.WALStats().ActiveBytes != 0 {
		t.Fatalf("a refused batch changed the store (%s) or wrote %d bytes", d, s.WALStats().ActiveBytes)
	}
}

// TestABatchTooLargeForARecordIsRefusedWhole: a batch whose record would
// exceed the framing's 64 MiB is refused with ErrInvalidBatch before anything
// is written, and latches nothing — the next batch is applied.
func TestABatchTooLargeForARecordIsRefusedWhole(t *testing.T) {
	s := openLSM(t, t.TempDir(), lsmOpts(storage.DefaultMemTableSize))
	defer s.Close()
	value := string(bytes.Repeat([]byte("v"), storage.DefaultMaxValueSize))
	var muts []storage.Mutation
	for i := 0; i*storage.DefaultMaxValueSize <= record.MaxRecordSize; i++ {
		muts = append(muts, mput(fmt.Sprintf("k%03d", i), value))
	}
	if err := s.Apply(context.Background(), muts, ai(1, 1)); !errors.Is(err, storage.ErrInvalidBatch) {
		t.Fatalf("a %d-mutation batch of 1 MiB values: %v, want ErrInvalidBatch", len(muts), err)
	}
	if s.WALStats().ActiveBytes != 0 || s.Sequence() != 0 || s.AppliedIndex() != (storage.AppliedIndex{}) {
		t.Fatal("a refused batch left a trace")
	}
	mustApply(t, s, applyStep{muts[:2], ai(1, 1)})
}

// TestAFailedApplyPublishesNothing: a batch whose WAL append fails is not
// visible — no mutation, no sequence, no applied index — and the failure
// latches: the next batch is refused too. After a process crash the store
// recovers the batches before it and none of the failed one: its record never
// reached the file.
func TestAFailedApplyPublishesNothing(t *testing.T) {
	mem := fault.NewMemFS()
	inj := fault.NewInjectFS(mem)
	dir := t.TempDir()
	o := lsmOpts(storage.DefaultMemTableSize)
	o.WAL.FS = inj
	s := openLSM(t, dir, o)
	first := applyStep{[]storage.Mutation{mput("a", "1")}, ai(1, 1)}
	mustApply(t, s, first)
	inj.Arm(fault.Injection{Op: fault.OpWrite})
	err := s.Apply(context.Background(), []storage.Mutation{mput("a", "2"), mput("b", "3")}, ai(2, 1))
	if err == nil {
		t.Fatal("an Apply whose append failed succeeded")
	}
	want := newApplyModel().after(first)
	if d := diff(s, want, []string{"a", "b"}); d != "" {
		t.Fatalf("a failed Apply published: %s", d)
	}
	if err := s.Apply(context.Background(), nil, ai(3, 1)); err == nil {
		t.Fatal("the WAL failure did not latch")
	}
	mem.CrashProcess()
	_ = s.Close()
	o.WAL.FS = mem
	s = openLSM(t, dir, o)
	defer s.Close()
	if d := diff(s, want, []string{"a", "b"}); d != "" {
		t.Fatalf("after a crash: %s", d)
	}
}

// TestPublicationOrder: the mutations of a batch are visible before
// AppliedIndex reports it. A reader that sees index i must find the value the
// batch at i wrote to the key it wrote last.
func TestPublicationOrder(t *testing.T) {
	s := openLSM(t, t.TempDir(), lsmOpts(64<<20))
	defer s.Close()
	const keys, batches = 400, 60
	var stop atomic.Bool
	var wg sync.WaitGroup
	violation := make(chan string, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		last := []byte(fmt.Sprintf("k%03d", keys-1))
		for !stop.Load() {
			a := s.AppliedIndex()
			v, err := s.Get(context.Background(), last)
			if a.Index == 0 {
				continue
			}
			var got uint64
			if err == nil {
				_, _ = fmt.Sscan(string(v), &got)
			}
			if err != nil || got < a.Index {
				select {
				case violation <- fmt.Sprintf("AppliedIndex reported %d, but the batch's last key reads %q (%v)", a.Index, v, err):
				default:
				}
				return
			}
		}
	}()
	for b := uint64(1); b <= batches; b++ {
		muts := make([]storage.Mutation, keys)
		for i := range muts {
			muts[i] = mput(fmt.Sprintf("k%03d", i), fmt.Sprint(b))
		}
		mustApply(t, s, applyStep{muts, ai(b, 1)})
	}
	stop.Store(true)
	wg.Wait()
	select {
	case v := <-violation:
		t.Fatal(v)
	default:
	}
}

// TestLegacyWritesNeverMoveTheAppliedIndex: Put, Delete and SetAppliedIndex
// are the standalone API. A Put between apply batches neither moves the
// applied index nor the sequence an apply batch covers, live or recovered; it
// does take a sequence number, which replay reproduces.
func TestLegacyWritesNeverMoveTheAppliedIndex(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	mustApply(t, s, applyStep{[]storage.Mutation{mput("a", "1"), mput("b", "2")}, ai(4, 1)})
	mustPut(t, s, "legacy", "x")
	mustDelete(t, s, "b")
	check := func(when string, s *storage.LSMStore) {
		t.Helper()
		if s.AppliedIndex() != ai(4, 1) || s.AppliedSequence() != 2 || s.Sequence() != 4 {
			t.Fatalf("%s: applied %+v covering sequence %d, sequence %d; want (4,1), 2, 4",
				when, s.AppliedIndex(), s.AppliedSequence(), s.Sequence())
		}
	}
	check("live", s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer s.Close()
	check("recovered", s)
	mustApply(t, s, applyStep{[]storage.Mutation{mput("c", "3")}, ai(5, 1)})
	if s.AppliedSequence() != 5 {
		t.Fatalf("the next batch covers sequence %d, want 5", s.AppliedSequence())
	}
}

// TestTheLegacyPairSplitsAndApplyDoesNot shows why Apply exists. The only way
// to record an application before S1 — Put, then SetAppliedIndex — writes two
// records, and a crash between them recovers the data without its index. The
// same application through Apply is one record: crashed at the same write, the
// store recovers neither.
func TestTheLegacyPairSplitsAndApplyDoesNot(t *testing.T) {
	for _, useApply := range []bool{false, true} {
		mem := fault.NewMemFS()
		inj := fault.NewInjectFS(mem)
		dir := t.TempDir()
		o := lsmOpts(storage.DefaultMemTableSize)
		o.WAL.FS = inj
		s := openLSM(t, dir, o)
		// Crash the process at the application's second write (the first is
		// the Put's record; Apply has only one).
		inj.Arm(fault.Injection{Op: fault.OpWrite, Nth: 2, At: mem.CrashProcess})
		ctx := context.Background()
		if useApply {
			_ = s.Apply(ctx, []storage.Mutation{mput("k", "v")}, ai(1, 1))
			_ = s.Apply(ctx, []storage.Mutation{mput("k2", "v2")}, ai(2, 1)) // its write is the second
		} else {
			_ = s.Put(ctx, []byte("k"), []byte("v"))
			_ = s.SetAppliedIndex(ctx, ai(1, 1))
		}
		_ = s.Close()
		o.WAL.FS = mem
		s = openLSM(t, dir, o)
		_, err := s.Get(ctx, []byte("k"))
		hasData, index := err == nil, s.AppliedIndex()
		_, err2 := s.Get(ctx, []byte("k2"))
		_ = s.Close()
		switch {
		case !useApply && !(hasData && index == storage.AppliedIndex{}):
			t.Fatalf("premise: the legacy pair crashed between its records should recover the data without its index; got data %v, index %+v", hasData, index)
		case useApply && (!hasData || index != ai(1, 1) || err2 == nil):
			t.Fatalf("Apply: recovered data %v, index %+v, second batch %v; want the first batch whole and none of the second", hasData, index, err2 == nil)
		}
	}
}

// TestApplyReplayRefusesAnIndexThatDoesNotAdvance: a WAL whose apply records
// do not advance — which the store never writes — is refused at open, and the
// refused log is left as it was.
func TestApplyReplayRefusesAnIndexThatDoesNotAdvance(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	mustApply(t, s, applyStep{[]storage.Mutation{mput("a", "1")}, ai(5, 1)})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Append, behind the store's back, a batch at the same index.
	w, err := wal.Create(filepath.Join(dir, "wal"), wal.Options{SyncMode: wal.SyncOff})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendApply(wal.ApplyBatch{Applied: wal.AppliedIndex{Index: 5, Term: 1}, Ops: wal.Batch{{Kind: wal.OpPut, Key: []byte("a"), Value: []byte("2")}}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	seg := filepath.Join(dir, "wal", "000001.log")
	before, _ := os.ReadFile(seg)
	if _, err := storage.OpenLSMStore(dir, lsmOpts(storage.DefaultMemTableSize)); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("open with a non-advancing apply batch: %v, want ErrCorrupt", err)
	}
	if after, _ := os.ReadFile(seg); !bytes.Equal(before, after) {
		t.Fatal("a refused open changed the log")
	}
}

// TestATornApplyBatchIsDroppedWhole and a damaged one mid-log refused: a crash
// mid-append leaves the last record torn, and the store recovers every batch
// before it and nothing of it; damage to a batch with records after it is not
// a crash, and the store refuses to open.
func TestATornApplyBatchIsDroppedWhole(t *testing.T) {
	script := applyScript()
	want := models(script)
	keys := keysOf(script)
	build := func() (string, string) {
		dir := t.TempDir()
		s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
		for _, st := range script {
			mustApply(t, s, st)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		return dir, filepath.Join(dir, "wal", "000001.log")
	}
	dir, seg := build()
	info, _ := os.Stat(seg)
	if err := os.Truncate(seg, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	if d := diff(s, want[len(script)-1], keys); d != "" {
		t.Fatalf("a torn last batch: %s", d)
	}
	if !s.Recovery().Truncated {
		t.Fatal("the torn tail was not reported")
	}
	_ = s.Close()

	dir, seg = build()
	raw, _ := os.ReadFile(seg)
	raw[record.HeaderSize+2] ^= 0x40 // inside the first batch, with batches after it
	if err := os.WriteFile(seg, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenLSMStore(dir, lsmOpts(storage.DefaultMemTableSize)); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("a damaged batch mid-log: %v, want ErrCorrupt", err)
	}
}
