package raftnode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/adivishall/quorum/internal/snapshot"
)

// hugeSM is a state machine whose snapshot is one byte over snapshot.MaxData,
// produced on the fly and never held. It counts how often it is opened and
// synced, and records the publish hook.
type hugeSM struct {
	*snapSM
	mu       sync.Mutex
	opens    int
	syncs    int
	publishs int
}

type hugeSource struct{ index uint64 }

func (h hugeSource) Index() uint64 { return h.index }
func (h hugeSource) Close() error  { return nil }
func (h hugeSource) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, 1<<20)
	var n int64
	for n <= snapshot.MaxData {
		k, err := w.Write(buf)
		n += int64(k)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (h *hugeSM) OpenSnapshot() (SnapshotSource, error) {
	idx, _, _ := h.state()
	h.mu.Lock()
	h.opens++
	h.mu.Unlock()
	return hugeSource{idx}, nil
}

func (h *hugeSM) DurableAppliedIndex() uint64 { return 0 }

func (h *hugeSM) SyncApplied() error {
	h.mu.Lock()
	h.syncs++
	h.mu.Unlock()
	return nil
}

func (h *hugeSM) hook(p Point, _ uint64) error {
	if p == BeforeSnapshotPublish {
		h.mu.Lock()
		h.publishs++
		h.mu.Unlock()
	}
	return nil
}

// TestAStateTooLargeToSnapshotIsSkippedBeforeAnySideEffect: a state over the
// snapshot bound is refused by the measuring pass, BEFORE the engine is synced,
// before the publish crash point, and before any file is created — Phase 14's
// order (S4 review: the first S4 cut found the size only while publishing) —
// and no new attempt is made until Every more entries are applied; the node
// keeps working, with nothing published.
func TestAStateTooLargeToSnapshotIsSkippedBeforeAnySideEffect(t *testing.T) {
	if testing.Short() {
		t.Skip("measures 512 MiB per attempt")
	}
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.log")
	sm := &hugeSM{snapSM: &snapSM{}}
	var logMu sync.Mutex
	var logs []string
	logf := func(format string, args ...any) {
		logMu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		logMu.Unlock()
	}
	const every = 8
	n, tr := startSnapSingle(t, ctx, path, sm, every, 0, sm.hook, logf)
	defer tr.Close()
	defer n.Close()
	waitLeaderNode(t, n)
	for i := 0; i < 3*every; i++ {
		if err := n.Propose(ctx, []byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitApplied(t, n, n.Status().LastIndex)
	st := n.Status()
	sm.mu.Lock()
	opens, syncs, publishs := sm.opens, sm.syncs, sm.publishs
	sm.mu.Unlock()
	if opens == 0 {
		t.Fatalf("premise: the trigger never fired (applied %d)", st.Applied)
	}
	if syncs != 0 || publishs != 0 {
		t.Fatalf("an oversized state synced the engine %d times and reached the publish point %d times", syncs, publishs)
	}
	if max := int(st.Applied/every) + 1; opens > max {
		t.Fatalf("opened %d times over %d applied entries: the skip did not hold off the next attempt for %d entries", opens, st.Applied, every)
	}
	if st.Snapshot != 0 {
		t.Fatalf("a snapshot was published: %+v", st)
	}
	for _, name := range []string{path + ".snap", path + ".snap.tmp"} {
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s exists after an oversized state was refused (%v)", filepath.Base(name), err)
		}
	}
	logMu.Lock()
	skipped := strings.Contains(strings.Join(logs, "\n"), "event=raft_snapshot_skipped")
	logMu.Unlock()
	if !skipped {
		t.Fatal("the refusal was not logged")
	}
}
