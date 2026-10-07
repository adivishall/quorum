package raftnode_test

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/snapshot"
	"github.com/adivishall/quorum/internal/transport"
)

// Phase 14 measurements (docs/SNAPSHOTS.md §11). The benchmarks time each
// snapshot operation on the real OS filesystem; TestSnapshotsBoundRecoveryWork
// measures what snapshots are for — the log a node keeps and the entries it
// replays at a restart — and asserts the bounds, which are properties, not
// timings: they hold on any machine. Its timings are reported, never asserted.
//
//	go test ./internal/raftnode -run TestSnapshotsBoundRecoveryWork -raftnode.measure=100000 -v
//	go test ./internal/raftnode -run '^$' -bench 'Snapshot|LogCompact' -benchtime 5x

var flagMeasure = flag.Int("raftnode.measure", 0, "entries for TestSnapshotsBoundRecoveryWork (0: a quick 3000)")

const measureKeys = 1000

func value(i int) []byte { return []byte(fmt.Sprintf("%-100d", i)) }

// countSM is a kv.Store that counts the entries it applies.
type countSM struct {
	*kv.Store
	applied int
}

func (s *countSM) Apply(index uint64, cmd []byte) error {
	s.applied++
	return s.Store.Apply(index, cmd)
}
func (s *countSM) ApplyResult(index uint64, cmd []byte) (any, error) {
	s.applied++
	return s.Store.ApplyResult(index, cmd)
}

// buildLog runs a single-node group on the OS filesystem through entries
// writes over measureKeys keys (so the state stays measureKeys keys while the
// log grows), with the given snapshot policy, and returns its log's path and
// last index. fsync is off: this measures log size and replay work, not
// durability.
func buildLog(t testing.TB, dir string, entries int, every, retain uint64) (string, uint64) {
	t.Helper()
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "n0", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	path := filepath.Join(dir, "raft.log")
	n, err := raftnode.Start(context.Background(), raftnode.Config{
		ID: "n0", Peers: []raftnode.NodeID{"n0"}, Transport: tr, LogPath: path, StateMachine: kv.NewStore(),
		TickInterval: 5 * time.Millisecond, DisableSync: true, SnapshotEvery: every, SnapshotRetain: retain,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	waitFor := func(what string, cond func() bool) {
		deadline := time.Now().Add(30 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("%s: not within 30s", what)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitFor("the single node leads", func() bool { return n.Role() == raft.Leader })
	for i := 0; i < entries; i++ {
		cmd := kv.Command{Op: kv.OpPut, Key: []byte(fmt.Sprintf("key%05d", i%measureKeys)), Value: value(i)}
		if err := n.Propose(context.Background(), cmd.Encode()); err != nil {
			t.Fatal(err)
		}
	}
	last := n.Status().LastIndex
	waitFor("every proposal applied", func() bool { return n.Status().Applied >= last })
	return path, last
}

// recoverAndReplay is a restart: Recover (the exact startup path) and the
// application of every committed entry after the restored snapshot.
func recoverAndReplay(t testing.TB, path string) (took time.Duration, replayed int, restored uint64) {
	t.Helper()
	sm := &countSM{Store: kv.NewStore()}
	start := time.Now()
	rc, err := raftnode.Recover(raftnode.Config{ID: "n0", Peers: []raftnode.NodeID{"n0"}, LogPath: path,
		StateMachine: sm, Rand: rand.New(rand.NewSource(1)), DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := raftnode.ApplyCommitted(rc.Core, sm, nil, nil); err != nil {
		t.Fatal(err)
	}
	took = time.Since(start)
	if rc.Snapshot != nil {
		restored = rc.Snapshot.Index
	}
	rc.Log.Close()
	return took, sm.applied, restored
}

func fileSize(t testing.TB, path string) int64 {
	st, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

// TestSnapshotsBoundRecoveryWork is the point of Phase 14, measured: the same
// workload with and without snapshots. Without, the log holds every entry and
// a restart replays every one; with a snapshot every `every` entries, the log
// holds at most every+retain of them and a restart replays at most every —
// independent of how long the node has run. The bounds are asserted; the
// times are reported.
func TestSnapshotsBoundRecoveryWork(t *testing.T) {
	entries := *flagMeasure
	if entries == 0 {
		entries = 3000
	}
	const every, retain = 500, 50
	fullPath, fullLast := buildLog(t, t.TempDir(), entries, 0, 0)
	snapPath, snapLast := buildLog(t, t.TempDir(), entries, every, retain)
	fullTook, fullReplayed, _ := recoverAndReplay(t, fullPath)
	snapTook, snapReplayed, restored := recoverAndReplay(t, snapPath)

	fullLog, snapLog := fileSize(t, fullPath), fileSize(t, snapPath)
	snapFile := fileSize(t, snapPath+".snap")
	if fullReplayed != int(fullLast) {
		t.Fatalf("without snapshots a restart replayed %d of %d entries", fullReplayed, fullLast)
	}
	if restored == 0 || snapReplayed > every || uint64(snapReplayed) != snapLast-restored {
		t.Fatalf("with snapshots a restart restored %d and replayed %d (bound %d, last %d)", restored, snapReplayed, every, snapLast)
	}
	rec, err := raftlog.Inspect(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if held := rec.LastIndex() - rec.Boundary.Index; held > every+retain {
		t.Fatalf("the compacted log holds %d entries, bound %d", held, every+retain)
	}
	if snapLog*4 > fullLog {
		t.Fatalf("the compacted log is %d bytes, the full one %d", snapLog, fullLog)
	}
	t.Logf("%d writes over %d keys: without snapshots the log is %d bytes and a restart replays %d entries in %s; "+
		"with snapshots every %d (retain %d) the log is %d bytes, the snapshot %d bytes, and a restart restores index %d and replays %d entries in %s",
		entries, measureKeys, fullLog, fullReplayed, fullTook.Round(time.Microsecond), every, retain,
		snapLog, snapFile, restored, snapReplayed, snapTook.Round(time.Microsecond))
}

// --- benchmarks of each snapshot operation ---

var benchKeys = []int{1000, 10000, 100000}

func storeWith(keys int) *kv.Store {
	s := kv.NewStore()
	for i := 0; i < keys; i++ {
		cmd := kv.Command{Op: kv.OpPut, Key: []byte(fmt.Sprintf("key%07d", i)), Value: value(i)}
		if _, err := s.ApplyResult(uint64(i+1), cmd.Encode()); err != nil {
			panic(err)
		}
	}
	return s
}

func fileOf(b *testing.B, s *kv.Store) (snapshot.Meta, []byte) {
	idx, data, err := s.EncodeSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	m := snapshot.Meta{Conf: replication.VotersOf([]replication.NodeID{"n0", "n1", "n2"}), Index: idx, Term: 1}
	file, err := snapshot.Encode(m, data)
	if err != nil {
		b.Fatal(err)
	}
	return m, file
}

// BenchmarkSnapshotEncode: the state machine's canonical encoding plus the
// snapshot file (records, SHA-256) — the serialization cost. Reports the file
// size.
func BenchmarkSnapshotEncode(b *testing.B) {
	for _, keys := range benchKeys {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			s := storeWith(keys)
			_, file := fileOf(b, s)
			b.SetBytes(int64(len(file)))
			for b.Loop() {
				fileOf(b, s)
			}
			b.ReportMetric(float64(len(file)), "file-bytes") // after the loop: b.Loop resets earlier metrics
		})
	}
}

// BenchmarkSnapshotPublish: writing, fsyncing, renaming and directory-fsyncing
// a snapshot file on the OS filesystem — the write throughput.
func BenchmarkSnapshotPublish(b *testing.B) {
	for _, keys := range benchKeys {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			_, file := fileOf(b, storeWith(keys))
			files := snapshot.Files{Base: filepath.Join(b.TempDir(), "raft.log")}
			b.SetBytes(int64(len(file)))
			for b.Loop() {
				if err := files.Publish(file); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSnapshotRestore: loading a published snapshot — reading it,
// validating every record and the SHA-256 — and restoring the state machine
// from it, with every validation rule.
func BenchmarkSnapshotRestore(b *testing.B) {
	for _, keys := range benchKeys {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			_, file := fileOf(b, storeWith(keys))
			files := snapshot.Files{Base: filepath.Join(b.TempDir(), "raft.log")}
			if err := files.Publish(file); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(file)))
			for b.Loop() {
				m, data, _, _, err := files.Load()
				if err != nil {
					b.Fatal(err)
				}
				if err := kv.NewStore().RestoreSnapshot(m.Index, data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkLogCompact: rewriting a durable log of N entries without all but
// the last 100 (fsync on), the compaction a snapshot is followed by.
func BenchmarkLogCompact(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			var ents []raftlog.Entry
			for i := 1; i <= n; i++ {
				ents = append(ents, raftlog.Entry{Index: uint64(i), Term: 1, Data: value(i)})
			}
			for i := 0; b.Loop(); i++ {
				b.StopTimer()
				path := filepath.Join(dir, fmt.Sprintf("raft%d.log", i))
				lg, _, err := raftlog.Open(path, raftlog.Options{Sync: true})
				if err != nil {
					b.Fatal(err)
				}
				if err := lg.Save(&raftlog.HardState{Term: 1, Commit: uint64(n)}, ents); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := lg.Compact(uint64(n-100), 1); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				lg.Close()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkSnapshotInstall: a follower's side of a transfer — reassembling
// the chunks into the staging file, validating the file and the state — and
// the install itself: publication, the log's boundary record (fsync on), the
// state machine's restore.
func BenchmarkSnapshotInstall(b *testing.B) {
	for _, keys := range benchKeys {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			m, file := fileOf(b, storeWith(keys))
			chunks := snapshot.Split(2, m, file)
			dir := b.TempDir()
			b.SetBytes(int64(len(file)))
			for i := 0; b.Loop(); i++ {
				b.StopTimer()
				base := filepath.Join(dir, fmt.Sprintf("raft%d.log", i))
				lg, _, err := raftlog.Open(base, raftlog.Options{Sync: true})
				if err != nil {
					b.Fatal(err)
				}
				if err := lg.Save(&raftlog.HardState{Term: 2}, nil); err != nil {
					b.Fatal(err)
				}
				snaps := &raftnode.Snapshots{Files: snapshot.Files{Base: base}, SM: kv.NewStore(), Group: m.Group}
				d := &raftnode.Durable{Log: lg, Snap: snaps}
				b.StartTimer()
				var msg *raft.Message
				for _, c := range chunks {
					if msg, err = snaps.Receive("n1", c.Marshal()); err != nil {
						b.Fatal(err)
					}
				}
				if msg == nil {
					b.Fatal("the transfer did not complete")
				}
				if err := d.InstallSnapshot(raft.SnapshotMeta{Index: m.Index, Term: m.Term}, &raftlog.HardState{Term: 2, Commit: m.Index}, nil); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				lg.Close()
				b.StartTimer()
			}
		})
	}
}
