package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRunIsDeterministic: the same flags produce byte-identical output across
// runs, for both formats (docs/ROUTING.md §8, matrix G).
func TestRunIsDeterministic(t *testing.T) {
	for _, format := range []string{"text", "svg"} {
		args := []string{"-shards", "16", "-rf", "3", "-nodes", "n0,n1,n2", "-format", format}
		var a, b bytes.Buffer
		if err := run(args, &a); err != nil {
			t.Fatalf("run 1 (%s): %v", format, err)
		}
		if err := run(args, &b); err != nil {
			t.Fatalf("run 2 (%s): %v", format, err)
		}
		if a.String() != b.String() {
			t.Fatalf("%s output not byte-identical across runs", format)
		}
		if a.Len() == 0 {
			t.Fatalf("%s output is empty", format)
		}
	}
}

func TestRunRejectsBadInput(t *testing.T) {
	cases := [][]string{
		{"-nodes", ""},                                           // missing nodes
		{"-shards", "0", "-nodes", "n0"},                         // invalid shard count
		{"-shards", "16", "-rf", "5", "-nodes", "n0,n1"},         // rf > nodes
		{"-shards", "16", "-nodes", "n0,n0"},                     // duplicate node
		{"-shards", "16", "-nodes", "n0,n1", "-format", "bogus"}, // bad format
	}
	for _, args := range cases {
		var out bytes.Buffer
		if err := run(args, &out); err == nil {
			t.Errorf("run(%v) succeeded, want error", args)
		}
	}
}

func TestRunTextMentionsMembership(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"-nodes", "n0,n1,n2"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "n0") || !strings.Contains(out.String(), "shards=16") {
		t.Errorf("text output missing membership/config: %q", out.String())
	}
}
