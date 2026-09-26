package kv

import (
	"bytes"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

// Phase 13 cost measurements (docs/DEDUP.md §8). Run with
//
//	go test ./internal/kv -run '^$' -bench 'Apply|Codec|SessionTable|Forwarding' -benchmem
//
// They measure the state machine and the codecs in isolation; end-to-end
// request latency is dominated by replication (BenchmarkForwarding).

func mustApply(b *testing.B, s *Store, index uint64, cmd []byte) Result {
	r, err := s.ApplyResult(index, cmd)
	if err != nil {
		b.Fatal(err)
	}
	res, _ := r.(Result)
	return res
}

// BenchmarkApply is the cost of applying one committed entry, by decision.
// The identified cases include the session lookup, the SHA-256 fingerprint of
// the command and the result bookkeeping; commands are encoded before the
// timer starts.
func BenchmarkApply(b *testing.B) {
	value := bytes.Repeat([]byte("v"), 100)
	encodeAll := func(n int, f func(i int) Command) [][]byte {
		out := make([][]byte, n)
		for i := range out {
			out[i] = f(i).Encode()
		}
		return out
	}
	b.Run("anonymous-put", func(b *testing.B) {
		s := NewStore()
		cmd := Command{Op: OpPut, Key: []byte("k"), Value: value}.Encode()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mustApply(b, s, uint64(i+1), cmd)
		}
	})
	b.Run("identified-executed", func(b *testing.B) {
		s := NewStore()
		mustApply(b, s, 1, Command{Op: OpRegister}.Encode())
		cmds := encodeAll(b.N, func(i int) Command {
			rid := uint64(i + 1)
			return Command{Op: OpPut, ClientID: 1, RequestID: rid, AckedBelow: rid, Key: []byte("k"), Value: value}
		})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if r := mustApply(b, s, uint64(i+2), cmds[i]); r.Decision != Executed {
				b.Fatalf("%v", r.Decision)
			}
		}
	})
	b.Run("identified-duplicate", func(b *testing.B) {
		s := NewStore()
		mustApply(b, s, 1, Command{Op: OpRegister}.Encode())
		cmd := Command{Op: OpPut, ClientID: 1, RequestID: 1, AckedBelow: 1, Key: []byte("k"), Value: value}.Encode()
		mustApply(b, s, 2, cmd)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if r := mustApply(b, s, uint64(i+3), cmd); r.Decision != Duplicate {
				b.Fatalf("%v", r.Decision)
			}
		}
	})
	// The worst case for a request: the session holds MaxUnacked-1 results and
	// every request raises the watermark by one, so each apply also scans the
	// held results to forget the one below it (O(MaxUnacked)).
	b.Run("identified-executed-127-held", func(b *testing.B) {
		s := NewStore()
		mustApply(b, s, 1, Command{Op: OpRegister}.Encode())
		held := uint64(DefaultLimits.MaxUnacked - 1)
		for rid := uint64(1); rid <= held; rid++ {
			mustApply(b, s, rid+1, Command{Op: OpPut, ClientID: 1, RequestID: rid, AckedBelow: 1, Key: []byte("k"), Value: value}.Encode())
		}
		cmds := encodeAll(b.N, func(i int) Command {
			rid := held + uint64(i) + 1
			return Command{Op: OpPut, ClientID: 1, RequestID: rid, AckedBelow: rid - held + 1, Key: []byte("k"), Value: value}
		})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if r := mustApply(b, s, held+uint64(i)+2, cmds[i]); r.Decision != Executed {
				b.Fatalf("%v", r.Decision)
			}
		}
	})
	// A REGISTER into a full table evicts the least recently used session: a
	// scan of MaxSessions sessions (O(MaxSessions)).
	b.Run("register-evicting-from-1024", func(b *testing.B) {
		s := NewStore()
		reg := Command{Op: OpRegister}.Encode()
		for i := 1; i <= DefaultLimits.MaxSessions; i++ {
			mustApply(b, s, uint64(i), reg)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mustApply(b, s, uint64(DefaultLimits.MaxSessions+i+1), reg)
		}
		if st := s.Stats(); st.Evicted != b.N {
			b.Fatalf("evicted %d, want %d", st.Evicted, b.N)
		}
	})
}

// BenchmarkSessionTableMemory reports the heap a FULL session table holds at
// the default limits — MaxSessions sessions, each with MaxUnacked remembered
// results — and per remembered result. That is the most the table can ever
// hold: the bounds are enforced at apply (TestSessionTableStaysBounded).
func BenchmarkSessionTableMemory(b *testing.B) {
	var perResult, total float64
	for i := 0; i < b.N; i++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		s := NewStore()
		index := uint64(0)
		for c := 0; c < DefaultLimits.MaxSessions; c++ {
			index++
			mustApply(b, s, index, Command{Op: OpRegister}.Encode())
			id := index
			for rid := uint64(1); rid <= uint64(DefaultLimits.MaxUnacked); rid++ {
				index++
				// DELETE: no key-value state, so the delta is the table's alone.
				if r := mustApply(b, s, index, Command{Op: OpDelete, ClientID: id, RequestID: rid, AckedBelow: 1, Key: []byte("k")}.Encode()); r.Decision != Executed {
					b.Fatalf("%v", r.Decision)
				}
			}
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		results := float64(DefaultLimits.MaxSessions * DefaultLimits.MaxUnacked)
		total = float64(after.HeapAlloc) - float64(before.HeapAlloc)
		perResult = total / results
		runtime.KeepAlive(s)
	}
	b.ReportMetric(total/(1<<20), "MiB/full-table")
	b.ReportMetric(perResult, "B/result")
}

// TestSessionTableStaysBounded: whatever is applied — registrations far beyond
// MaxSessions, requests that never acknowledge anything, duplicates, conflicts,
// requests for evicted sessions — the table never holds more than MaxSessions
// sessions or more than MaxUnacked results in one of them.
func TestSessionTableStaysBounded(t *testing.T) {
	limits := Limits{MaxSessions: 8, MaxUnacked: 4}
	s := NewStoreWithLimits(limits)
	rng := rand.New(rand.NewSource(1))
	var ids []uint64
	for index := uint64(1); index <= 20000; index++ {
		var cmd Command
		switch p := rng.Intn(100); {
		case p < 10 || len(ids) == 0:
			cmd = Command{Op: OpRegister}
			ids = append(ids, index)
		default:
			id := ids[rng.Intn(len(ids))] // possibly evicted
			rid := uint64(rng.Intn(40) + 1)
			acked := uint64(1)
			if rng.Intn(4) == 0 {
				acked = uint64(rng.Intn(int(rid))) + 1
			}
			cmd = Command{Op: OpPut, ClientID: id, RequestID: rid, AckedBelow: acked, Key: []byte("k"), Value: []byte{byte(rng.Intn(3))}}
		}
		if _, err := s.ApplyResult(index, cmd.Encode()); err != nil {
			t.Fatal(err)
		}
		table := s.Sessions()
		if len(table) > limits.MaxSessions {
			t.Fatalf("index %d: %d sessions, limit %d", index, len(table), limits.MaxSessions)
		}
		for id, ss := range table {
			if len(ss.Requests) > limits.MaxUnacked {
				t.Fatalf("index %d: session %d holds %d results, limit %d", index, id, len(ss.Requests), limits.MaxUnacked)
			}
		}
	}
	st := s.Stats()
	if st.Evicted == 0 || st.Limit == 0 || st.Duplicate == 0 || st.Conflict == 0 || st.Expired == 0 || st.Stale == 0 {
		t.Fatalf("the sequence did not press on every bound: %+v", st)
	}
}

// BenchmarkCodec is the cost of the Phase 13 encodings: the wire request and
// response, an identified command, and the fingerprint (SHA-256 over the
// command, so linear in the value).
func BenchmarkCodec(b *testing.B) {
	small := bytes.Repeat([]byte("v"), 100)
	large := bytes.Repeat([]byte("v"), 64<<10)
	req := Request{Op: ReqPut, ClientID: 1 << 20, RequestID: 1 << 16, AckedBelow: 1 << 16, Key: []byte("key-0001"), Value: small, Timeout: 2 * time.Second}
	resp := Response{Status: StatusOK, ClientID: 0, Term: 7, Index: 1 << 20, Duplicate: true, Node: "n1", Via: "n2", Value: small}
	cmd := Command{Op: OpPut, ClientID: 1 << 20, RequestID: 1 << 16, AckedBelow: 1 << 16, Key: []byte("key-0001"), Value: small}
	b.Run("request-roundtrip-100B", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := decodeRequest(encodeRequest(req)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("response-roundtrip-100B", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := decodeResponse(encodeResponse(resp)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("command-roundtrip-identified-100B", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := Decode(cmd.Encode()); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, v := range []struct {
		name  string
		value []byte
	}{{"100B", small}, {"64KiB", large}} {
		c := cmd
		c.Value = v.value
		b.Run("fingerprint-"+v.name, func(b *testing.B) {
			b.SetBytes(int64(len(v.value)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = c.Fingerprint()
			}
		})
	}
}
