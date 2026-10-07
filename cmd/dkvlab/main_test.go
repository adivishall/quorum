package main

import (
	"reflect"
	"strings"
	"testing"
)

// TestIntList: the list-valued flags take one value or a comma list; anything
// else is a usage error, never a silent default.
func TestIntList(t *testing.T) {
	for in, want := range map[string][]int{"16": {16}, "1,4,16": {1, 4, 16}, " 1, 3 ,5": {1, 3, 5}} {
		got, err := intList(in)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%q: %v %v", in, got, err)
		}
	}
	for _, in := range []string{"", "a", "1,,2", "1,-2"} {
		if _, err := intList(in); err == nil {
			t.Fatalf("%q was accepted", in)
		}
	}
}

// TestBadListIsAUsageError: a malformed list exits 2 before anything is built.
func TestBadListIsAUsageError(t *testing.T) {
	var out, errs strings.Builder
	if code := run(t.Context(), []string{"-clients", "4,x", "-repo", t.TempDir()}, &out, &errs); code != 2 || !strings.Contains(errs.String(), "-clients") {
		t.Fatalf("exit %d: %s", code, errs.String())
	}
}
