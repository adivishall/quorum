package cli_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/adivishall/distributed-kv/internal/cli"
	"github.com/adivishall/distributed-kv/internal/storage"
)

// harness runs CLI invocations against one store, capturing streams per call.
type harness struct {
	t     *testing.T
	store storage.Store
	app   *cli.App
}

type result struct {
	code   int
	stdout string
	stderr string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWithOptions(t, storage.DefaultOptions())
}

func newHarnessWithOptions(t *testing.T, opts storage.Options) *harness {
	t.Helper()
	s, err := storage.NewMemStore(opts)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return &harness{t: t, store: s, app: &cli.App{Store: s}}
}

// run executes one command with empty stdin.
func (h *harness) run(args ...string) result {
	return h.runWithStdin("", args...)
}

func (h *harness) runWithStdin(stdin string, args ...string) result {
	h.t.Helper()
	var out, errOut bytes.Buffer
	h.app.Stdin = strings.NewReader(stdin)
	h.app.Stdout = &out
	h.app.Stderr = &errOut
	code := h.app.Run(args)
	return result{code: code, stdout: out.String(), stderr: errOut.String()}
}

// ------------------------------------------------------------ happy path

func TestCLIPutGetDelete(t *testing.T) {
	h := newHarness(t)

	if got := h.run("put", "user:123", "Adi"); got.code != cli.ExitOK {
		t.Fatalf("put: exit %d, want %d (stderr: %s)", got.code, cli.ExitOK, got.stderr)
	}

	got := h.run("get", "user:123")
	if got.code != cli.ExitOK {
		t.Fatalf("get: exit %d, want %d (stderr: %s)", got.code, cli.ExitOK, got.stderr)
	}
	if got.stdout != "Adi\n" {
		t.Fatalf("get stdout = %q, want %q", got.stdout, "Adi\n")
	}

	if got := h.run("delete", "user:123"); got.code != cli.ExitOK {
		t.Fatalf("delete: exit %d, want %d", got.code, cli.ExitOK)
	}

	got = h.run("get", "user:123")
	if got.code != cli.ExitNotFound {
		t.Fatalf("get after delete: exit %d, want %d", got.code, cli.ExitNotFound)
	}
	if got.stdout != "" {
		t.Fatalf("get of a missing key wrote %q to stdout; want nothing", got.stdout)
	}
	if !strings.Contains(got.stderr, "key not found") {
		t.Errorf("get stderr = %q, want it to mention 'key not found'", got.stderr)
	}
}

func TestCLIDeleteAliasAndIdempotency(t *testing.T) {
	h := newHarness(t)

	// `del` is an accepted alias.
	if got := h.run("del", "never-existed"); got.code != cli.ExitOK {
		t.Fatalf("del(missing): exit %d, want %d — delete is idempotent", got.code, cli.ExitOK)
	}
	if got := h.run("delete", "never-existed"); got.code != cli.ExitOK {
		t.Fatalf("delete(missing): exit %d, want %d", got.code, cli.ExitOK)
	}
}

func TestCLIOverwrite(t *testing.T) {
	h := newHarness(t)
	h.run("put", "k", "one")
	h.run("put", "k", "two")
	if got := h.run("get", "k"); got.stdout != "two\n" {
		t.Fatalf("get = %q, want %q", got.stdout, "two\n")
	}
}

func TestCLIBinaryAndWhitespaceValuesSurviveStdout(t *testing.T) {
	h := newHarness(t)
	value := "line1\nline2\ttabbed \x00 \xff end"

	if got := h.run("put", "bin", value); got.code != cli.ExitOK {
		t.Fatalf("put: exit %d (stderr %s)", got.code, got.stderr)
	}
	got := h.run("get", "bin")
	if got.stdout != value+"\n" {
		t.Fatalf("get stdout = %q, want %q", got.stdout, value+"\n")
	}
}

func TestCLIWhitespaceKeyFromArgv(t *testing.T) {
	h := newHarness(t)
	// argv preserves whitespace, so keys with spaces are addressable from the
	// command line even though the interactive shell cannot express them.
	if got := h.run("put", "a key with spaces", "v"); got.code != cli.ExitOK {
		t.Fatalf("put: exit %d (stderr %s)", got.code, got.stderr)
	}
	if got := h.run("get", "a key with spaces"); got.stdout != "v\n" {
		t.Fatalf("get = %q, want %q", got.stdout, "v\n")
	}
}

func TestCLIEmptyValue(t *testing.T) {
	h := newHarness(t)
	if got := h.run("put", "k", ""); got.code != cli.ExitOK {
		t.Fatalf("put(empty value): exit %d (stderr %s)", got.code, got.stderr)
	}
	got := h.run("get", "k")
	if got.code != cli.ExitOK {
		t.Fatalf("get: exit %d, want %d — an empty value is a present key", got.code, cli.ExitOK)
	}
	if got.stdout != "\n" {
		t.Fatalf("get stdout = %q, want just the trailing newline", got.stdout)
	}
}

// ------------------------------------------------------------ exit codes

func TestCLIExitCodes(t *testing.T) {
	bigKey := strings.Repeat("k", storage.DefaultMaxKeySize+1)
	bigValue := strings.Repeat("v", storage.DefaultMaxValueSize+1)

	cases := []struct {
		name     string
		args     []string
		wantCode int
		wantErr  string // substring expected on stderr
	}{
		{"no arguments", nil, cli.ExitUsage, "no command given"},
		{"unknown command", []string{"frobnicate"}, cli.ExitUsage, `unknown command "frobnicate"`},
		{"put with no operands", []string{"put"}, cli.ExitUsage, "requires exactly 2"},
		{"put with one operand", []string{"put", "k"}, cli.ExitUsage, "requires exactly 2"},
		{"put with three operands", []string{"put", "k", "v", "extra"}, cli.ExitUsage, "requires exactly 2"},
		{"get with no operands", []string{"get"}, cli.ExitUsage, "requires exactly 1"},
		{"get with two operands", []string{"get", "a", "b"}, cli.ExitUsage, "requires exactly 1"},
		{"delete with no operands", []string{"delete"}, cli.ExitUsage, "requires exactly 1"},
		{"delete with two operands", []string{"delete", "a", "b"}, cli.ExitUsage, "requires exactly 1"},
		{"shell with operands", []string{"shell", "extra"}, cli.ExitUsage, "takes no arguments"},

		{"get missing key", []string{"get", "absent"}, cli.ExitNotFound, "key not found"},

		{"put empty key", []string{"put", "", "v"}, cli.ExitInvalidInput, "key is empty"},
		{"get empty key", []string{"get", ""}, cli.ExitInvalidInput, "key is empty"},
		{"delete empty key", []string{"delete", ""}, cli.ExitInvalidInput, "key is empty"},
		{"put oversized key", []string{"put", bigKey, "v"}, cli.ExitInvalidInput, "key too large"},
		{"get oversized key", []string{"get", bigKey}, cli.ExitInvalidInput, "key too large"},
		{"put oversized value", []string{"put", "k", bigValue}, cli.ExitInvalidInput, "value too large"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			got := h.run(tc.args...)
			if got.code != tc.wantCode {
				t.Errorf("exit = %d, want %d (stderr: %s)", got.code, tc.wantCode, got.stderr)
			}
			if !strings.Contains(got.stderr, tc.wantErr) {
				t.Errorf("stderr = %q, want it to contain %q", got.stderr, tc.wantErr)
			}
			// Diagnostics belong on stderr. stdout is for data, so that
			// `dkv get k > file` never captures an error message.
			if got.stdout != "" {
				t.Errorf("stdout = %q, want empty: diagnostics must go to stderr", got.stdout)
			}
		})
	}
}

// TestCLIClosedStoreIsInternalError pins the bottom of the error mapping: a
// failure the user did not cause must not be reported as a usage or
// not-found error.
func TestCLIClosedStoreIsInternalError(t *testing.T) {
	s, err := storage.NewMemStore(storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var out, errOut bytes.Buffer
	app := &cli.App{Store: s, Stdin: strings.NewReader(""), Stdout: &out, Stderr: &errOut}

	for _, args := range [][]string{
		{"put", "k", "v"},
		{"get", "k"},
		{"delete", "k"},
	} {
		out.Reset()
		errOut.Reset()
		if code := app.Run(args); code != cli.ExitInternal {
			t.Errorf("%v against a closed store: exit %d, want %d (stderr: %s)",
				args, code, cli.ExitInternal, errOut.String())
		}
		if !strings.Contains(errOut.String(), "store is closed") {
			t.Errorf("%v: stderr = %q, want it to mention the closed store", args, errOut.String())
		}
	}
}

func TestCLIHelpAndVersion(t *testing.T) {
	h := newHarness(t)

	for _, arg := range []string{"help", "-h", "--help"} {
		got := h.run(arg)
		if got.code != cli.ExitOK {
			t.Errorf("%s: exit %d, want %d", arg, got.code, cli.ExitOK)
		}
		if !strings.Contains(got.stdout, "Usage:") {
			t.Errorf("%s: stdout = %q, want usage text on stdout", arg, got.stdout)
		}
		// Help must document the exit codes, since scripts depend on them.
		for _, want := range []string{"Exit codes:", "key not found", "usage error", "invalid input"} {
			if !strings.Contains(got.stdout, want) {
				t.Errorf("%s: usage text is missing %q", arg, want)
			}
		}
	}

	got := h.run("version")
	if got.code != cli.ExitOK {
		t.Errorf("version: exit %d, want %d", got.code, cli.ExitOK)
	}
	if !strings.HasPrefix(got.stdout, "dkv ") {
		t.Errorf("version stdout = %q, want it to start with %q", got.stdout, "dkv ")
	}
}

// TestCLIUsageOnErrorGoesToStderr guards the property that makes the CLI
// scriptable: on failure, stdout stays clean.
func TestCLIUsageOnErrorGoesToStderr(t *testing.T) {
	h := newHarness(t)
	got := h.run("bogus")
	if !strings.Contains(got.stderr, "Usage:") {
		t.Errorf("stderr = %q, want usage text on stderr for an unknown command", got.stderr)
	}
	if got.stdout != "" {
		t.Errorf("stdout = %q, want empty", got.stdout)
	}
}

// ------------------------------------------------------------ ephemeral note

func TestCLIEphemeralNotice(t *testing.T) {
	h := newHarness(t)
	h.app.Ephemeral = true

	got := h.run("put", "k", "v")
	if !strings.Contains(got.stderr, "does not survive process exit") {
		t.Errorf("stderr = %q, want the in-memory notice", got.stderr)
	}
	if got.stdout != "" {
		t.Errorf("the notice leaked to stdout: %q", got.stdout)
	}

	// A read is not a mutation and gets no notice.
	got = h.run("get", "k")
	if strings.Contains(got.stderr, "does not survive") {
		t.Errorf("get printed the ephemeral notice: %q", got.stderr)
	}

	// With a durable store (Phase 2 onward) the notice disappears entirely.
	h.app.Ephemeral = false
	got = h.run("put", "k", "v")
	if strings.Contains(got.stderr, "does not survive") {
		t.Errorf("notice printed with Ephemeral=false: %q", got.stderr)
	}
}

// ------------------------------------------------------------ shell

func TestCLIShellSession(t *testing.T) {
	h := newHarness(t)
	script := strings.Join([]string{
		"put user:1 Adi",
		"get user:1",
		"put user:1 Updated Name With Spaces",
		"get user:1",
		"delete user:1",
		"get user:1",
		"",
		"# a comment is ignored",
		"   ",
		"exit",
	}, "\n") + "\n"

	got := h.runWithStdin(script, "shell")
	if got.code != cli.ExitOK {
		t.Fatalf("shell: exit %d, want %d (stderr: %s)", got.code, cli.ExitOK, got.stderr)
	}

	want := strings.Join([]string{
		"OK",                       // put user:1 Adi
		"Adi",                      // get
		"OK",                       // put with a multi-word value
		"Updated Name With Spaces", // get: the value is the rest of the line
		"OK",                       // delete
		"(not found)",              // get after delete
	}, "\n") + "\n"

	if got.stdout != want {
		t.Fatalf("shell stdout =\n%q\nwant\n%q", got.stdout, want)
	}
}

func TestCLIShellPromptGoesToStderr(t *testing.T) {
	h := newHarness(t)
	got := h.runWithStdin("get nothing\nexit\n", "shell")
	// stdout must contain only results, so the shell can be piped.
	if strings.Contains(got.stdout, "dkv>") {
		t.Errorf("prompt leaked into stdout: %q", got.stdout)
	}
	if !strings.Contains(got.stderr, "dkv>") {
		t.Errorf("stderr = %q, want the prompt", got.stderr)
	}
}

func TestCLIShellErrorsDoNotTerminateSession(t *testing.T) {
	h := newHarness(t)
	script := strings.Join([]string{
		"bogus command",
		"put",     // missing key
		"get",     // missing key
		"get a b", // too many operands
		"delete",  // missing key
		"put k v", // still works afterwards
		"get k",
		"exit",
	}, "\n") + "\n"

	got := h.runWithStdin(script, "shell")
	if got.code != cli.ExitOK {
		t.Fatalf("shell: exit %d, want %d", got.code, cli.ExitOK)
	}
	if got.stdout != "OK\nv\n" {
		t.Fatalf("stdout = %q, want %q: the session must survive bad input", got.stdout, "OK\nv\n")
	}
	for _, want := range []string{
		`unknown command "bogus"`,
		"put requires a key",
		"get requires exactly one key",
		"delete requires exactly one key",
	} {
		if !strings.Contains(got.stderr, want) {
			t.Errorf("stderr is missing %q\ngot: %s", want, got.stderr)
		}
	}
}

func TestCLIShellEOFExitsCleanly(t *testing.T) {
	h := newHarness(t)
	// No trailing "exit": EOF must terminate the shell with success.
	got := h.runWithStdin("put k v\nget k\n", "shell")
	if got.code != cli.ExitOK {
		t.Fatalf("shell: exit %d, want %d", got.code, cli.ExitOK)
	}
	if got.stdout != "OK\nv\n" {
		t.Fatalf("stdout = %q, want %q", got.stdout, "OK\nv\n")
	}
}

func TestCLIShellBareArgumentsAndEmptyValue(t *testing.T) {
	h := newHarness(t)
	// `put k` with no value stores an empty value — a present key, distinct
	// from an absent one, as documented in the shell help.
	got := h.runWithStdin("put k\nget k\nexit\n", "shell")
	if got.stdout != "OK\n\n" {
		t.Fatalf("stdout = %q, want %q (empty value then its newline)", got.stdout, "OK\n\n")
	}
}

// TestCLIShellHandlesMaximumValue is the regression test for bufio.Scanner's
// 64 KiB default token limit. A 1 MiB value is legal per the storage contract,
// so the shell must be able to carry one; before Buffer() was raised this
// failed with "token too long".
func TestCLIShellHandlesMaximumValue(t *testing.T) {
	h := newHarness(t)
	big := strings.Repeat("x", storage.DefaultMaxValueSize)

	got := h.runWithStdin("put big "+big+"\nget big\nexit\n", "shell")
	if got.code != cli.ExitOK {
		t.Fatalf("shell: exit %d, want %d (stderr: %s)", got.code, cli.ExitOK, got.stderr)
	}
	wantLen := len("OK\n") + len(big) + 1
	if len(got.stdout) != wantLen {
		t.Fatalf("stdout is %d bytes, want %d: the maximum legal value did not round-trip",
			len(got.stdout), wantLen)
	}
	if !strings.HasPrefix(got.stdout, "OK\n"+big[:64]) {
		t.Fatal("the value came back altered")
	}
}

func TestCLIShellOversizedLineIsReportedNotSilent(t *testing.T) {
	h := newHarness(t)
	// Beyond the shell's line limit the read fails. It must fail loudly with
	// an internal error, never truncate the value and report success.
	tooBig := strings.Repeat("x", storage.DefaultMaxValueSize+storage.DefaultMaxKeySize+1024)

	got := h.runWithStdin("put big "+tooBig+"\n", "shell")
	if got.code != cli.ExitInternal {
		t.Fatalf("exit = %d, want %d for an unreadable line", got.code, cli.ExitInternal)
	}
	if !strings.Contains(got.stderr, "reading input") {
		t.Errorf("stderr = %q, want it to report the read failure", got.stderr)
	}
	if strings.Contains(got.stdout, "OK") {
		t.Error("the shell reported OK for a line it could not read")
	}
}

func TestCLIShellHelp(t *testing.T) {
	h := newHarness(t)
	got := h.runWithStdin("help\nexit\n", "shell")
	if !strings.Contains(got.stdout, "put <key> <value...>") {
		t.Errorf("shell help missing from stdout: %q", got.stdout)
	}
}

func TestCLIShellQuitAlias(t *testing.T) {
	h := newHarness(t)
	got := h.runWithStdin("quit\nput k v\n", "shell")
	if got.code != cli.ExitOK {
		t.Fatalf("exit = %d, want %d", got.code, cli.ExitOK)
	}
	if strings.Contains(got.stdout, "OK") {
		t.Error("commands after 'quit' were executed")
	}
}

func TestCLIShellRespectsStorageLimits(t *testing.T) {
	h := newHarnessWithOptions(t, storage.Options{MaxKeySize: 8, MaxValueSize: 8})
	got := h.runWithStdin("put waytoolongkey v\nput k waytoolongvalue\nexit\n", "shell")
	if !strings.Contains(got.stderr, "key too large") {
		t.Errorf("stderr = %q, want 'key too large'", got.stderr)
	}
	if !strings.Contains(got.stderr, "value too large") {
		t.Errorf("stderr = %q, want 'value too large'", got.stderr)
	}
	if strings.Contains(got.stdout, "OK") {
		t.Error("a rejected put reported OK")
	}
}

// TestCLIErrorMessageIsNotDoublePrefixed is a regression test.
//
// Found by manually running the binary, not by the suite: storage.OpError used
// to render itself as `dkv: get "k": key not found`, and the CLI prefixed it
// again, producing `dkv: dkv: get "k": key not found`. The original
// conformance test asserted only that the message *started with* "dkv: ", so
// it passed while the output was visibly wrong.
//
// The program name is now added exactly once, by the printer.
func TestCLIErrorMessageIsNotDoublePrefixed(t *testing.T) {
	h := newHarness(t)

	cases := [][]string{
		{"get", "absent"},
		{"put", "", "v"},
		{"get", strings.Repeat("k", storage.DefaultMaxKeySize+1)},
	}

	for _, args := range cases {
		got := h.run(args...)
		if n := strings.Count(got.stderr, "dkv:"); n != 1 {
			t.Errorf("%v: stderr contains %d %q prefixes, want exactly 1:\n%s",
				args, n, "dkv:", got.stderr)
		}
		if strings.Contains(got.stderr, "dkv: dkv:") {
			t.Errorf("%v: doubled prefix in stderr: %s", args, got.stderr)
		}
	}
}

// TestCLIShellErrorMessagesAreSinglePrefixed covers the same property on the
// shell path, which formats errors as "error: %v" rather than "dkv: %v".
func TestCLIShellErrorMessagesAreSinglePrefixed(t *testing.T) {
	h := newHarnessWithOptions(t, storage.Options{MaxKeySize: 4, MaxValueSize: 4})
	got := h.runWithStdin("put toolongkey v\nexit\n", "shell")

	if strings.Contains(got.stderr, "dkv:") {
		t.Errorf("shell error carries a redundant program prefix: %q", got.stderr)
	}
	if !strings.Contains(got.stderr, `error: put "toolongkey": key too large`) {
		t.Errorf("stderr = %q, want a single clean error line", got.stderr)
	}
}
