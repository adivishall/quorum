package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
)

// S4 (docs/STORAGE_INTEGRATION.md §10.4–§10.5): the streaming writer and reader
// are the same format as Encode and Decode, so every test here is differential
// against them.

// streamDecode reads a whole file through the streaming Reader.
func streamDecode(b []byte) (Meta, []byte, error) {
	rd, err := NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return Meta{}, nil, err
	}
	data, err := io.ReadAll(rd)
	if err != nil {
		return Meta{}, nil, err
	}
	return rd.Meta(), data, nil
}

// errClass names which sentinel an error carries, for comparing two decoders.
func errClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrVersion):
		return "version"
	case errors.Is(err, ErrTooLarge):
		return "too-large"
	case errors.Is(err, ErrCorrupt):
		return "corrupt"
	default:
		return "other: " + err.Error()
	}
}

// agree fails unless Decode and the streaming Reader give the same answer for b.
func agree(t *testing.T, what string, b []byte) {
	t.Helper()
	m1, d1, e1 := Decode(b)
	m2, d2, e2 := streamDecode(b)
	if errClass(e1) != errClass(e2) {
		t.Fatalf("%s: Decode says %v, the Reader says %v", what, e1, e2)
	}
	if e1 == nil && (m1.Index != m2.Index || m1.Term != m2.Term || m1.Group != m2.Group || !bytes.Equal(d1, d2)) {
		t.Fatalf("%s: both accept, but disagree on what: %+v vs %+v", what, m1, m2)
	}
	if e1 != nil && e1.Error() != e2.Error() {
		t.Fatalf("%s: different refusals:\n Decode: %v\n Reader: %v", what, e1, e2)
	}
}

// TestWriteIsByteIdenticalToEncode: Write streams exactly the bytes Encode
// returns, at every data-record boundary — the reason S4 changes no format.
func TestWriteIsByteIdenticalToEncode(t *testing.T) {
	for _, n := range []int{0, 1, 4096, MaxDataRecord - 1, MaxDataRecord, MaxDataRecord + 1, 3*MaxDataRecord + 517} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i * 7)
		}
		m := meta(uint64(n+1), 4)
		want := mustEncode(t, m, data)
		var buf bytes.Buffer
		w, err := Write(&buf, m, BytesSource(data))
		if err != nil {
			t.Fatalf("%d bytes: %v", n, err)
		}
		if w != int64(buf.Len()) || !bytes.Equal(buf.Bytes(), want) {
			t.Fatalf("%d bytes: Write produced %d bytes (reported %d) differing from Encode's %d", n, buf.Len(), w, len(want))
		}
	}
}

// zeros is a source of n zero bytes, written in 1 MiB pieces and never held.
type zeros struct{ n int64 }

func (z zeros) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, 1<<20)
	var done int64
	for done < z.n {
		k, err := w.Write(buf[:min(int64(len(buf)), z.n-done)])
		done += int64(k)
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

// TestWriteRefusesAStateOverTheBoundBeforeWritingAnything: the measuring pass
// stops at MaxData, so nothing of an oversized state reaches the file.
func TestWriteRefusesAStateOverTheBoundBeforeWritingAnything(t *testing.T) {
	var buf bytes.Buffer
	if _, err := Write(&buf, meta(5, 1), zeros{MaxData + 1}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a state of MaxData+1 bytes: %v, want ErrTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("an oversized state wrote %d bytes", buf.Len())
	}
}

// changing is a source that writes a different state each time.
type changing struct{ calls int }

func (c *changing) WriteTo(w io.Writer) (int64, error) {
	c.calls++
	n, err := w.Write([]byte(fmt.Sprintf("state-%d", c.calls)))
	return int64(n), err
}

// TestWriteRefusesASourceThatDoesNotReproduceItself: a state measured is the
// state written, or the write fails — and what it left does not validate.
func TestWriteRefusesASourceThatDoesNotReproduceItself(t *testing.T) {
	var buf bytes.Buffer
	if _, err := Write(&buf, meta(5, 1), &changing{}); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("a changing source: %v, want ErrSourceChanged", err)
	}
	if _, _, err := Decode(buf.Bytes()); err == nil {
		t.Fatal("what a changing source left decodes")
	}
}

// TestReaderAgreesWithDecodeOnTheCorpus: every known-good and known-bad file,
// every truncation and every bit flip of one file — the streaming Reader
// accepts exactly what Decode accepts, with the same refusal.
func TestReaderAgreesWithDecodeOnTheCorpus(t *testing.T) {
	for name, e := range corpus(t) {
		agree(t, name, e.file)
	}
	file := mustEncode(t, meta(9, 3), bytes.Repeat([]byte("state"), 700))
	for n := 0; n <= len(file); n++ {
		agree(t, fmt.Sprintf("truncated to %d", n), file[:n])
	}
	for i := 0; i < len(file); i++ {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), file...)
			bad[i] ^= 1 << bit
			agree(t, fmt.Sprintf("bit %d of byte %d", bit, i), bad)
		}
	}
	agree(t, "a byte after the footer", append(append([]byte(nil), file...), 0))
}

// TestReaderReportsEndOnlyOnceTheFileIsProven: a file whose state hash is wrong
// yields every state byte and then an error — never io.EOF — so a consumer
// that commits only on io.EOF never commits a corrupt state.
func TestReaderReportsEndOnlyOnceTheFileIsProven(t *testing.T) {
	data := bytes.Repeat([]byte("x"), MaxDataRecord+10)
	other := append([]byte(nil), data...)
	other[len(other)-1] = 'y'
	m := meta(9, 3)
	// Every record intact, but the state is not the one the header's hash names.
	spliced := spliceHeader(t, mustEncode(t, m, data), mustEncode(t, m, other))
	rd, err := NewReader(bytes.NewReader(spliced), int64(len(spliced)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rd)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a state whose hash is wrong: %v, want ErrCorrupt", err)
	}
	if len(got) != len(other) {
		t.Fatalf("read %d state bytes before the refusal, want all %d", len(got), len(other))
	}
	if _, err := rd.Read(make([]byte, 1)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a refused Reader read again: %v, want the same refusal", err)
	}
}

// spliceHeader returns good's header record followed by every record of
// wrong after its header.
func spliceHeader(t *testing.T, good, wrong []byte) []byte {
	t.Helper()
	first := func(b []byte) int { // the header record's length
		rd, err := NewReader(bytes.NewReader(b), int64(len(b)))
		if err != nil {
			t.Fatal(err)
		}
		return int(rd.rd.NextOffset())
	}
	return append(append([]byte(nil), good[:first(good)]...), wrong[first(wrong):]...)
}

// FuzzReaderAgreesWithDecode: on any input, the streaming Reader and Decode
// accept the same files, with the same metadata and state, and refuse the
// same files with the same error.
func FuzzReaderAgreesWithDecode(f *testing.F) {
	for _, n := range []int{0, 5, 300} {
		b, _ := Encode(meta(3, 2), bytes.Repeat([]byte("z"), n))
		f.Add(b)
	}
	f.Add([]byte("QSNP"))
	f.Fuzz(func(t *testing.T, b []byte) { agree(t, "fuzz input", b) })
}

// TestPublishFromIsByteIdenticalAndAtomicUnderEveryCrash: a streamed publish
// writes exactly Publish's file, and a crash — process or power loss — before
// any of its operations leaves the old snapshot or the new one published,
// whole, and no temporary after RemoveOrphans.
func TestPublishFromIsByteIdenticalAndAtomicUnderEveryCrash(t *testing.T) {
	oldData, newData := bytes.Repeat([]byte("o"), 3000), bytes.Repeat([]byte("n"), MaxDataRecord+3000)
	newFile := mustEncode(t, meta(200, 3), newData)

	clean := fault.NewInjectFS(fault.NewMemFS())
	if err := files(clean).PublishFrom(meta(100, 2), BytesSource(oldData)); err != nil {
		t.Fatal(err)
	}
	before := len(clean.Ops())
	if err := files(clean).PublishFrom(meta(200, 3), BytesSource(newData)); err != nil {
		t.Fatal(err)
	}
	type op struct {
		kind fault.Op
		path string
	}
	var ops []op
	for _, r := range clean.Ops()[before:] {
		ops = append(ops, op{r.Op, r.Path})
	}
	if _, _, file, _, err := files(clean).Load(); err != nil || !bytes.Equal(file, newFile) {
		t.Fatalf("PublishFrom's file differs from Encode's (%v)", err)
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
			if err := files(inj).PublishFrom(meta(100, 2), BytesSource(oldData)); err != nil {
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
				panic(errDied)
			}})
			func() {
				defer func() {
					if r := recover(); r != nil && r != errDied {
						panic(r)
					}
				}()
				_ = files(inj).PublishFrom(meta(200, 3), BytesSource(newData))
			}()
			if !crashed {
				t.Fatalf("%s#%d never reached", o.kind, nth)
			}
			restarted := Files{FS: mem, Base: base}
			if err := restarted.RemoveOrphans(); err != nil {
				t.Fatal(err)
			}
			m, found, err := restarted.Verify()
			if err != nil || !found || (m.Index != 100 && m.Index != 200) {
				t.Fatalf("crash (%s) before %s#%d of %s: published %+v found=%v err=%v", mode, o.kind, nth, o.path, m, found, err)
			}
			if _, ok := mem.Cached(restarted.TmpPath()); ok {
				t.Fatalf("crash (%s) before %s#%d: the temporary survived RemoveOrphans", mode, o.kind, nth)
			}
			outcomes[fmt.Sprintf("%s:%d", mode, m.Index)]++
		}
	}
	if outcomes["process:100"] == 0 || outcomes["process:200"] == 0 || outcomes["power:100"] == 0 {
		t.Fatalf("outcomes %v: the crash points do not straddle publication", outcomes)
	}
	t.Logf("%d crash points x 2 modes: %v", len(ops), outcomes)
}

// TestARefusedStreamedPublishPublishesNothing: a state over the bound, or a
// source that changes, leaves the previous snapshot published and no
// temporary behind.
func TestARefusedStreamedPublishPublishesNothing(t *testing.T) {
	mem := fault.NewMemFS()
	f := Files{FS: mem, Base: base}
	if err := f.PublishFrom(meta(100, 2), BytesSource("old")); err != nil {
		t.Fatal(err)
	}
	for name, src := range map[string]Source{"too large": zeros{MaxData + 1}, "changing": &changing{}} {
		if err := f.PublishFrom(meta(200, 3), src); err == nil {
			t.Fatalf("%s: published", name)
		}
		if m, _, err := f.Verify(); err != nil || m.Index != 100 {
			t.Fatalf("%s: the published snapshot is now %+v (%v)", name, m, err)
		}
		if _, ok := mem.Cached(f.TmpPath()); ok {
			t.Fatalf("%s: the temporary was left behind", name)
		}
	}
}

// TestASendReadsTheSnapshotItOpenedAcrossALaterPublish: a transfer's chunks
// come from the file it opened; publishing a newer snapshot meanwhile does not
// change them. The chunks reassemble, through a Receiver, into that file.
func TestASendReadsTheSnapshotItOpenedAcrossALaterPublish(t *testing.T) {
	for _, fsys := range []string{"memfs", "os"} {
		t.Run(fsys, func(t *testing.T) {
			f := Files{Base: filepath.Join(t.TempDir(), "raft.log")}
			if fsys == "memfs" {
				f.FS = fault.NewMemFS()
			}
			oldData := bytes.Repeat([]byte("old!"), MaxDataRecord/2+77) // more than two data records
			if err := f.PublishFrom(meta(100, 2), BytesSource(oldData)); err != nil {
				t.Fatal(err)
			}
			want, err := readFile(f.fs(), f.Path())
			if err != nil {
				t.Fatal(err)
			}
			snd, err := f.OpenSend()
			if err != nil {
				t.Fatal(err)
			}
			defer snd.Close()
			if snd.Meta().Index != 100 || snd.Size() != int64(len(want)) {
				t.Fatalf("OpenSend: %+v, %d bytes", snd.Meta(), snd.Size())
			}
			if err := f.PublishFrom(meta(300, 4), BytesSource("new")); err != nil {
				t.Fatal(err)
			}
			if err := snd.Verify(); err != nil {
				t.Fatalf("verifying the opened snapshot after a newer publish: %v", err)
			}
			recvFiles := Files{FS: f.FS, Base: filepath.Join(filepath.Dir(f.Base), "follower.log")}
			r := NewReceiver(recvFiles)
			var got *Received
			n, err := snd.Chunks(7, 100_000, func(c Chunk) error {
				c2, err := UnmarshalChunk(c.Marshal())
				if err != nil {
					return err
				}
				done, err := r.Accept("n1", c2)
				if done != nil {
					got = done
				}
				return err
			})
			if err != nil || got == nil {
				t.Fatalf("after %d chunks: %v (complete: %v)", n, err, got != nil)
			}
			if got.Meta.Index != 100 || got.Bytes != int64(len(want)) || n != (len(want)+99_999)/100_000 {
				t.Fatalf("received %+v (%d bytes) in %d chunks; want snapshot 100, %d bytes", got.Meta, got.Bytes, n, len(want))
			}
			rd, file, err := recvFiles.OpenReceived()
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			state, err := io.ReadAll(rd)
			if err != nil || !bytes.Equal(state, oldData) {
				t.Fatalf("the received state: %v (equal %v)", err, bytes.Equal(state, oldData))
			}
		})
	}
}

// TestStreamingHoldsOneRecordAtATime: writing, receiving and reading back a
// snapshot well above the S3 install limit (~63 MiB) never holds the state in
// memory: the live heap grows by a few records, not by the state. Decode,
// which holds the whole file and the whole state, could not stay under the
// bound. The real filesystem, so the file's bytes are not in the heap either.
func TestStreamingHoldsOneRecordAtATime(t *testing.T) {
	if testing.Short() {
		t.Skip("writes and reads a 96 MiB snapshot")
	}
	const size = 96 << 20
	const bound = 24 << 20 // a few records plus GC slack; the state is 4 times this
	defer debug.SetGCPercent(debug.SetGCPercent(20))
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base0 := ms.HeapInuse
	peak := uint64(0)
	sample := func() {
		runtime.ReadMemStats(&ms)
		if ms.HeapInuse > base0 && ms.HeapInuse-base0 > peak {
			peak = ms.HeapInuse - base0
		}
	}

	dir := t.TempDir()
	f := Files{Base: filepath.Join(dir, "leader.log")}
	if err := f.PublishFrom(meta(1000, 9), sampled{zeros{size}, sample}); err != nil {
		t.Fatal(err)
	}
	snd, err := f.OpenSend()
	if err != nil {
		t.Fatal(err)
	}
	defer snd.Close()
	recv := Files{Base: filepath.Join(dir, "follower.log")}
	r := NewReceiver(recv)
	var got *Received
	if _, err := snd.Chunks(2, MaxChunk, func(c Chunk) error {
		done, err := r.Accept("n1", c)
		if done != nil {
			got = done
		}
		sample()
		return err
	}); err != nil || got == nil {
		t.Fatalf("transfer: %v (complete %v)", err, got != nil)
	}
	rd, file, err := recv.OpenReceived()
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	buf := make([]byte, 64<<10)
	var total int64
	for {
		n, err := rd.Read(buf)
		total += int64(n)
		if total%(4<<20) < int64(n) {
			sample()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if total != size {
		t.Fatalf("read back %d state bytes, want %d", total, size)
	}
	if peak > bound {
		t.Fatalf("the live heap grew by %d MiB while streaming a %d MiB snapshot; bound %d MiB", peak>>20, size>>20, bound>>20)
	}
	t.Logf("a %d MiB snapshot published, sent, received, validated and read back: peak live-heap growth %.1f MiB",
		size>>20, float64(peak)/(1<<20))
	if _, err := os.Stat(recv.RecvPath()); err != nil {
		t.Fatal(err)
	}
}

// sampled is a source that samples the heap as it writes.
type sampled struct {
	src    Source
	sample func()
}

func (s sampled) WriteTo(w io.Writer) (int64, error) {
	return s.src.WriteTo(sampleWriter{w, s.sample})
}

type sampleWriter struct {
	w      io.Writer
	sample func()
}

func (s sampleWriter) Write(p []byte) (int, error) {
	s.sample()
	return s.w.Write(p)
}

// TestARetriedTransferStartsClean: a transfer abandoned part-way — the leader
// changed, or the follower's connection dropped — leaves a partial staging
// file; the next transfer, of a smaller snapshot, starts at offset 0 and must
// not inherit a byte of it.
func TestARetriedTransferStartsClean(t *testing.T) {
	mem := fault.NewMemFS()
	r := NewReceiver(Files{FS: mem, Base: base})
	big := mustEncode(t, meta(90, 3), bytes.Repeat([]byte("b"), 2*MaxChunk+5))
	small := mustEncode(t, meta(95, 4), []byte("the newer, smaller state"))
	for _, c := range Split(5, meta(90, 3), big)[:2] { // abandoned after two chunks
		if got, err := r.Accept("n1", c); got != nil || err != nil {
			t.Fatalf("partial transfer: %v %v", got, err)
		}
	}
	var done *Received
	for _, c := range Split(6, meta(95, 4), small) {
		got, err := r.Accept("n2", c)
		if err != nil {
			t.Fatalf("the retried transfer: %v", err)
		}
		done = got
	}
	if done == nil || done.Meta.Index != 95 || done.Bytes != int64(len(small)) {
		t.Fatalf("the retried transfer completed as %+v", done)
	}
	if b, _ := mem.Cached(base + ".snap.recv"); !bytes.Equal(b, small) {
		t.Fatalf("the staged file is %d bytes, want exactly the %d of the retried snapshot", len(b), len(small))
	}
}
