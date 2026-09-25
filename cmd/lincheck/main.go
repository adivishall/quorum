// Command lincheck checks a recorded client history for linearizability
// (Phase 12, docs/LINEARIZABILITY.md). It is how a history saved by a failing
// test — a real-process run, an in-process run, or a simulator run — is
// re-checked, minimized and explained offline, without re-running the cluster:
//
//	go run ./cmd/lincheck [-max-states N] [-no-minimize] history.txt
//
// The file is one operation per line in lincheck's text form (blank lines and
// '#' comments are ignored, so a saved artifact's header — seed, options,
// fault events — travels with it). Exit status: 0 linearizable, 1 not
// linearizable, 2 unchecked (the search budget ran out: NOT a verdict) or a
// malformed input.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/adivishall/quorum/internal/lincheck"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("lincheck", flag.ContinueOnError)
	fs.SetOutput(stderr)
	maxStates := fs.Int("max-states", lincheck.DefaultMaxStates, "per-key search budget; exceeding it reports UNCHECKED")
	noMin := fs.Bool("no-minimize", false, "report the failing key's whole history instead of a minimized one")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: lincheck [-max-states N] [-no-minimize] HISTORY_FILE")
		return 2
	}
	text, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "lincheck: %v\n", err)
		return 2
	}
	h, err := lincheck.ParseHistory(string(text))
	if err != nil {
		fmt.Fprintf(stderr, "lincheck: %v\n", err)
		return 2
	}
	if err := h.Validate(); err != nil {
		fmt.Fprintf(stderr, "lincheck: %v\n", err)
		return 2
	}
	c := h.Summary()
	r := lincheck.Check(h, lincheck.Options{MaxStates: *maxStates, Minimize: !*noMin})
	fmt.Fprintf(stdout, "ops=%d keys=%d clients=%d ok=%d notfound=%d rejected=%d incomplete=%d; searched %d states in %s\n",
		c.Total, c.Keys, c.Clients, c.OK, c.NotFound, c.Rejected, c.Incomplete, r.Stats.States, r.Stats.Duration)
	switch {
	case r.OK:
		fmt.Fprintln(stdout, "LINEARIZABLE")
		return 0
	case r.Unchecked:
		fmt.Fprintf(stdout, "UNCHECKED: %s\n", r.Reason)
		return 2
	default:
		fmt.Fprintf(stdout, "NOT LINEARIZABLE\n%s\n--- counterexample (%d ops) ---\n%s", r.Reason, len(r.Counterexample), lincheck.Format(r.Counterexample))
		return 1
	}
}
