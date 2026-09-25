package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestExitStatusFollowsTheVerdict: the corpus files check the way the corpus
// test says they do, through the command; a malformed file and a missing
// argument are errors, never a verdict.
func TestExitStatusFollowsTheVerdict(t *testing.T) {
	corpus := filepath.Join("..", "..", "internal", "lincheck", "testdata", "corpus")
	for file, want := range map[string]int{
		filepath.Join(corpus, "good", "retry-recorded-honestly.hist"):    0,
		filepath.Join(corpus, "bad", "stale-leader-read.hist"):           1,
		filepath.Join(corpus, "bad", "retry-collapsed-into-one-op.hist"): 1,
	} {
		var out, errb bytes.Buffer
		if got := run([]string{file}, &out, &errb); got != want {
			t.Fatalf("%s: exit %d, want %d\n%s%s", file, got, want, out.String(), errb.String())
		}
		if want == 1 && !strings.Contains(out.String(), "counterexample") {
			t.Fatalf("%s: a rejection must show its counterexample:\n%s", file, out.String())
		}
	}
	bad := filepath.Join(t.TempDir(), "bad.hist")
	if err := os.WriteFile(bad, []byte("1 c1 put \"k\" 5 3 ok value=\"A\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	if got := run([]string{bad}, &out, &errb); got != 2 {
		t.Fatalf("malformed history: exit %d, want 2", got)
	}
	if got := run(nil, &out, &errb); got != 2 {
		t.Fatalf("no file: exit %d, want 2", got)
	}
}
