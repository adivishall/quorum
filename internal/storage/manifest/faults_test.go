package manifest_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/storage/manifest"
	"github.com/adivishall/quorum/internal/vfs"
)

// The manifest under every I/O fault it can meet (audit M11): a write that
// fails or tears, an fsync, a rename and a directory fsync that fail — at
// each of their first occurrences in an Install and in an Append. Whatever
// the fault, the operation reports it, a failed Writer accepts nothing more,
// and the directory recovers to the state before the operation or the state
// after it: never refused, never a CURRENT naming a manifest that is gone.

// encodeState is a state's canonical bytes, for comparing states.
func encodeState(s manifest.State) []byte {
	e := s.Snapshot()
	return e.AppendTo(nil)
}

func stateWith(files ...uint64) manifest.State {
	s := manifest.State{NextFileNum: 100, LastSequence: 50}
	for i, n := range files {
		s.Files = append(s.Files, meta(n, 1, uint64(10*i+1), uint64(10*i+9)))
	}
	return s
}

func addFile(n uint64) *manifest.Edit {
	var e manifest.Edit
	e.AddFile(meta(n, 0, 60, 69))
	e.SetNextFileNum(n + 1)
	return &e
}

// useFS routes the manifest's writes through f until the test ends.
func useFS(t *testing.T, f vfs.FS) {
	t.Helper()
	prev := manifest.FS
	manifest.FS = f
	t.Cleanup(func() { manifest.FS = prev })
}

// faults are the injections the matrix applies, each at its first, second
// and third occurrence.
var faults = []struct {
	name string
	inj  fault.Injection
}{
	{"write", fault.Injection{Op: fault.OpWrite}},
	{"torn write", fault.Injection{Op: fault.OpWrite, Short: 7}},
	{"fsync", fault.Injection{Op: fault.OpSync}},
	{"rename", fault.Injection{Op: fault.OpRename}},
	{"directory fsync", fault.Injection{Op: fault.OpSyncDir}},
}

// requireOneOf recovers dir and requires its state to be one of the allowed.
func requireOneOf(t *testing.T, dir, context string, allowed ...manifest.State) {
	t.Helper()
	got, _, err := manifest.Recover(dir)
	if err != nil {
		t.Fatalf("%s: the directory does not recover: %v", context, err)
	}
	for _, s := range allowed {
		if bytes.Equal(encodeState(got), encodeState(s)) {
			return
		}
	}
	t.Fatalf("%s: recovered a state that is neither before nor after the operation: %+v", context, got)
}

func TestInstallUnderEveryFault(t *testing.T) {
	reached := map[string]bool{}
	for _, f := range faults {
		for nth := 1; nth <= 3; nth++ {
			name := fmt.Sprintf("%s#%d", f.name, nth)
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				old := stateWith(1, 2)
				w, err := manifest.Install(dir, 1, old)
				if err != nil {
					t.Fatal(err)
				}
				_ = w.Close()
				inj := fault.NewInjectFS(nil)
				useFS(t, inj)
				arm := f.inj
				arm.Nth = nth
				inj.Arm(arm)
				next := stateWith(1, 2, 3)
				w2, err := manifest.Install(dir, 2, next)
				if inj.Armed() != 0 {
					// Not reached: Install did fewer operations of this kind.
					if err != nil {
						t.Fatalf("Install failed with no fault injected: %v", err)
					}
					_ = w2.Close()
					return
				}
				reached[f.name] = true
				if err == nil {
					t.Fatalf("Install succeeded although its %s failed", name)
				}
				manifest.FS = vfs.OS{}
				requireOneOf(t, dir, "after a failed Install", old, next)
			})
		}
	}
	for _, f := range faults {
		if !reached[f.name] {
			t.Errorf("no Install reached a %s: the matrix tests nothing for it", f.name)
		}
	}
}

func TestAppendUnderEveryFault(t *testing.T) {
	reached := map[string]bool{}
	for _, f := range faults {
		for nth := 1; nth <= 3; nth++ {
			name := fmt.Sprintf("%s#%d", f.name, nth)
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				inj := fault.NewInjectFS(nil)
				useFS(t, inj)
				base := stateWith(1, 2)
				w, err := manifest.Install(dir, 1, base)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				arm := f.inj
				arm.Nth = nth
				inj.Arm(arm)
				err = w.Append(addFile(7))
				if inj.Armed() != 0 {
					if err != nil {
						t.Fatalf("Append failed with no fault injected: %v", err)
					}
					return
				}
				reached[f.name] = true
				if !errors.Is(err, manifest.ErrFailed) {
					t.Fatalf("Append whose %s failed: %v, want ErrFailed", name, err)
				}
				// Latched: nothing more is written after an edit that may be partial.
				ops := len(inj.Ops())
				if err := w.Append(addFile(8)); !errors.Is(err, manifest.ErrFailed) {
					t.Fatalf("Append after a failed one: %v, want the latched ErrFailed", err)
				}
				if done := inj.Ops()[ops:]; len(done) != 0 {
					t.Fatalf("a failed Writer still performed %v", done)
				}
				after := base
				if err := after.Apply(*addFile(7)); err != nil {
					t.Fatal(err)
				}
				manifest.FS = vfs.OS{}
				requireOneOf(t, dir, "after a failed Append", base, after)
			})
		}
	}
	for _, name := range []string{"write", "torn write", "fsync"} {
		if !reached[name] {
			t.Errorf("no Append reached a %s: the matrix tests nothing for it", name)
		}
	}
}
