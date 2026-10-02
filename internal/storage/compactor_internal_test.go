package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestALatchedCompactionErrorStopsTheCompactor (audit M11): once a background
// compaction has failed, the compactor runs no other in this process. It used
// to retry on every flush — appending after an edit whose outcome was unknown
// and burning file numbers — although the failure was latched.
func TestALatchedCompactionErrorStopsTheCompactor(t *testing.T) {
	opts := DefaultOptions()
	opts.L0CompactionTrigger = 2
	s, err := OpenLSMStore(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	s.setCompactErr(errors.New("an earlier compaction failed"))
	for i := 0; i < 5; i++ {
		if err := s.Put(context.Background(), []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// Five L0 files, each flush waking the compactor, at a trigger of two:
	// an unlatched compactor would have run within this.
	time.Sleep(300 * time.Millisecond)
	if runs := s.CompactionStats().Runs; runs != 0 {
		t.Fatalf("the compactor ran %d compactions after its failure was latched", runs)
	}
	if n := len(s.SSTables()); n != 5 {
		t.Fatalf("%d SSTables, want the 5 flushes uncompacted", n)
	}
}
