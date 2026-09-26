package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
)

const base = "/node/raft.log"

func files(fs *fault.InjectFS) Files { return Files{FS: fs, Base: base} }

// loadMeta loads the published snapshot and reports its index (0: none).
func loadIndex(t *testing.T, f Files) uint64 {
	t.Helper()
	m, _, _, found, err := f.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !found {
		return 0
	}
	return m.Index
}

// TestPublishIsAtomicUnderEveryCrash: a crash — process crash or power loss —
// at every I/O operation of publishing snapshot 200 over snapshot 100 leaves the
// published snapshot as either 100 or 200, whole; never a torn one, never none.
// The orphaned temporary is removed at startup. The operations are enumerated
// from a clean run, so a change to Publish's I/O is covered automatically.
func TestPublishIsAtomicUnderEveryCrash(t *testing.T) {
	old := mustEncode(t, meta(100, 2), bytes.Repeat([]byte("o"), 3000))
	nw := mustEncode(t, meta(200, 3), bytes.Repeat([]byte("n"), 3000))

	// Enumerate Publish's operations on a clean run.
	clean := fault.NewInjectFS(fault.NewMemFS())
	if err := files(clean).Publish(old); err != nil {
		t.Fatal(err)
	}
	before := len(clean.Ops())
	if err := files(clean).Publish(nw); err != nil {
		t.Fatal(err)
	}
	type op struct {
		kind fault.Op
		path string
	}
	var ops []op
	seen := map[op]int{}
	for _, r := range clean.Ops()[before:] {
		o := op{r.Op, r.Path}
		seen[o]++
		ops = append(ops, o)
	}
	if len(ops) < 4 {
		t.Fatalf("publish performed only %v", ops)
	}
	outcomes := map[string]int{}
	for i, o := range ops {
		nth := 0
		for _, p := range ops[:i+1] {
			if p == o {
				nth++
			}
		}
		for _, mode := range []string{"process", "power"} {
			mem := fault.NewMemFS()
			inj := fault.NewInjectFS(mem)
			if err := files(inj).Publish(old); err != nil {
				t.Fatal(err)
			}
			crashed := false
			inj.Arm(fault.Injection{Op: o.kind, Path: o.path, Nth: nth, At: func() {
				crashed = true
				if mode == "power" {
					mem.CrashPowerLoss(0)
				} else {
					mem.CrashProcess()
				}
				panic(errDied) // the process dies here: nothing after this point runs
			}})
			publishUntilDeath(t, files(inj), nw)
			if !crashed {
				t.Fatalf("%s#%d never reached", o.kind, nth)
			}
			restarted := Files{FS: mem, Base: base}
			if err := restarted.RemoveOrphans(); err != nil {
				t.Fatal(err)
			}
			got := loadIndex(t, restarted)
			if got != 100 && got != 200 {
				t.Fatalf("crash (%s) before %s#%d of %s: published snapshot is %d", mode, o.kind, nth, o.path, got)
			}
			if _, ok := mem.Cached(restarted.TmpPath()); ok {
				t.Fatalf("crash (%s) before %s#%d: the temporary survived RemoveOrphans", mode, o.kind, nth)
			}
			outcomes[fmt.Sprintf("%s:%d", mode, got)]++
		}
	}
	// Both outcomes must occur: before the rename the old one, after the
	// directory fsync the new one (a process crash keeps a rename; a power loss
	// before the directory fsync undoes it).
	if outcomes["process:100"] == 0 || outcomes["process:200"] == 0 || outcomes["power:100"] == 0 {
		t.Fatalf("outcomes %v: the crash points do not straddle publication", outcomes)
	}
	t.Logf("%d crash points x 2 modes: %v", len(ops), outcomes)
}

// TestLoadRefusesACorruptPublishedSnapshot: a published snapshot that does not
// decode is an error — never "no snapshot".
func TestLoadRefusesACorruptPublishedSnapshot(t *testing.T) {
	mem := fault.NewMemFS()
	f := Files{FS: mem, Base: base}
	good := mustEncode(t, meta(7, 1), []byte("x"))
	if err := f.Publish(good[:len(good)-3]); err != nil {
		t.Fatal(err)
	}
	if _, _, _, found, err := f.Load(); !found || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("load of a torn published snapshot: found=%v err=%v", found, err)
	}
	if _, _, _, found, err := (Files{FS: fault.NewMemFS(), Base: base}).Load(); found || err != nil {
		t.Fatalf("no snapshot: found=%v err=%v", found, err)
	}
}

// TestReceiverAcceptsOnlyAnInOrderCompleteSnapshot pins the reassembly rules:
// chunks in order complete a transfer; a duplicate or out-of-order chunk, one
// from another sender or term, is ignored; a chunk at offset 0 restarts the
// transfer; a complete file that does not validate is an error; the staging
// file is published only by the caller.
func TestReceiverAcceptsOnlyAnInOrderCompleteSnapshot(t *testing.T) {
	file := mustEncode(t, meta(50, 4), bytes.Repeat([]byte("s"), 3*MaxChunk+100))
	chunks := Split(6, meta(50, 4), file)
	if len(chunks) != 4 {
		t.Fatalf("%d chunks", len(chunks))
	}
	mem := fault.NewMemFS()
	r := NewReceiver(Files{FS: mem, Base: base})

	feed := func(from string, c Chunk) *Received {
		t.Helper()
		got, err := r.Accept(from, c)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if feed("n1", chunks[1]) != nil {
		t.Fatal("a transfer started without its first chunk")
	}
	feed("n1", chunks[0])
	feed("n1", chunks[0].withOffset(0)) // a duplicate of the first chunk restarts, harmlessly
	feed("n1", chunks[1])
	if feed("n1", chunks[1]) != nil || feed("n1", chunks[3]) != nil {
		t.Fatal("a duplicate or a skipped chunk was taken")
	}
	if feed("n2", chunks[2]) != nil {
		t.Fatal("another sender's chunk continued the transfer")
	}
	stale := chunks[2]
	stale.Term = 5
	if feed("n1", stale) != nil {
		t.Fatal("a chunk from an earlier term continued the transfer")
	}
	if _, _, got, ok := r.Active(); !ok || got != uint64(2*MaxChunk) {
		t.Fatalf("progress %d, want %d", got, 2*MaxChunk)
	}
	feed("n1", chunks[2])
	done := feed("n1", chunks[3])
	if done == nil || done.Meta.Index != 50 || done.From != "n1" || done.Term != 6 || done.Bytes != len(file) {
		t.Fatalf("completion: %+v", done)
	}
	if b, _ := mem.Cached(base + ".snap.recv"); !bytes.Equal(b, file) {
		t.Fatal("the staged file differs from what was sent")
	}
	if _, ok := mem.Cached(base + ".snap"); ok {
		t.Fatal("the receiver published the snapshot itself")
	}
	// The staged file is fsynced: once published (rename + directory fsync) it
	// survives a power loss whole. (Its staging name is never made durable.)
	durable := mem.DurableCopy()
	if err := (Files{FS: mem, Base: base}).PublishReceived(); err != nil {
		t.Fatal(err)
	}
	mem.CrashPowerLoss(0)
	if m, _, _, found, err := (Files{FS: mem, Base: base}).Load(); !found || err != nil || m.Index != 50 {
		t.Fatalf("a published received snapshot after a power loss: %+v found=%v %v", m, found, err)
	}
	if _, ok := durable.Cached(base + ".snap.recv"); ok {
		t.Fatal("the staging name was made durable before publication")
	}
	r = NewReceiver(Files{FS: mem, Base: base})

	// A complete transfer of a file that does not validate is refused.
	bad := append([]byte(nil), file...)
	bad[len(bad)/2] ^= 1
	var err error
	for _, c := range Split(7, meta(50, 4), bad) {
		var got *Received
		got, err = r.Accept("n1", c)
		if got != nil {
			t.Fatal("a corrupt snapshot was reported complete")
		}
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt transfer: %v", err)
	}
	// A file whose metadata disagrees with what its chunks announced.
	other := mustEncode(t, meta(51, 4), []byte("x"))
	for _, c := range Split(8, meta(50, 4), other) {
		_, err = r.Accept("n1", c)
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mislabelled transfer: %v", err)
	}
}

func (c Chunk) withOffset(o uint64) Chunk { c.Offset = o; return c }

var errDied = errors.New("the process died at the crash point")

// publishUntilDeath runs Publish until a crash point kills "the process".
func publishUntilDeath(t *testing.T, f Files, file []byte) {
	t.Helper()
	defer func() {
		if v := recover(); v != nil && v != errDied {
			panic(v)
		}
	}()
	_ = f.Publish(file)
}

// TestChunkCodec: round trip, and the bounds UnmarshalChunk enforces.
func TestChunkCodec(t *testing.T) {
	c := Chunk{Term: 3, Index: 9, SnapTerm: 2, Total: 100, Offset: 90, Data: []byte("0123456789")}
	got, err := UnmarshalChunk(c.Marshal())
	if err != nil || got.Term != 3 || got.Index != 9 || got.SnapTerm != 2 || got.Total != 100 || got.Offset != 90 || string(got.Data) != "0123456789" {
		t.Fatalf("%+v %v", got, err)
	}
	for name, bad := range map[string]Chunk{
		"past the end":  {Term: 3, Index: 9, SnapTerm: 2, Total: 95, Offset: 90, Data: []byte("0123456789")},
		"index 0":       {Term: 3, Index: 0, SnapTerm: 2, Total: 100, Offset: 0, Data: []byte("x")},
		"term 0":        {Term: 3, Index: 9, SnapTerm: 0, Total: 100, Offset: 0, Data: []byte("x")},
		"empty data":    {Term: 3, Index: 9, SnapTerm: 2, Total: 100, Offset: 0},
		"total too big": {Term: 3, Index: 9, SnapTerm: 2, Total: MaxFile + 1, Offset: 0, Data: []byte("x")},
	} {
		if _, err := UnmarshalChunk(bad.Marshal()); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := UnmarshalChunk(append(c.Marshal(), 0)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("trailing byte: %v", err)
	}
}
