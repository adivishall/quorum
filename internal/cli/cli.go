// Package cli implements Quorum's dkv command-line interface.
//
// The command logic lives here rather than in cmd/dkv so that it can be tested
// directly: App takes its streams and its Store as fields, and Run returns an
// exit code instead of calling os.Exit. cmd/dkv is a thin main that wires in
// the real streams and exits with what Run returns. Exit codes are part of the
// contract (scripts branch on them), so they get tested like anything else.
package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/version"
)

// Exit codes. These are a public contract: a script may branch on them, so
// they may not be renumbered without a note in docs/CLI.md.
const (
	// ExitOK means the command succeeded.
	ExitOK = 0
	// ExitNotFound means the command was well-formed and the key was absent.
	// It is deliberately distinct from an error: `dkv get missing` is not a
	// malfunction, and a script needs to tell the two apart. This follows
	// grep(1), which exits 1 when it simply found nothing.
	ExitNotFound = 1
	// ExitUsage means the arguments were malformed: unknown command, wrong
	// number of operands.
	ExitUsage = 2
	// ExitInvalidInput means the arguments were well-formed but the key or
	// value violated a storage rule (empty key, oversized key or value).
	ExitInvalidInput = 3
	// ExitInternal means something failed that the user did not cause.
	ExitInternal = 4
)

// App is a runnable dkv CLI bound to a Store and a set of streams.
type App struct {
	Store  storage.Store
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	// Ephemeral marks the backing store as non-durable. When set, mutating
	// commands print a one-line notice to stderr so that nobody mistakes a
	// successful `put` for a stored value. It is true for the whole of Phase 1
	// because MemStore loses everything on process exit; Phase 2 sets it false
	// once there is a write-ahead log.
	Ephemeral bool
}

// maxLineSize bounds a single shell line. It must exceed MaxKeySize plus
// MaxValueSize plus the command word, or a legal large value would be
// unreadable through the shell. bufio.Scanner's default cap is 64 KiB, far
// below the 1 MiB maximum value; without raising it, `put k <1MiB>` would fail
// with a confusing "token too long" rather than working.
const maxLineSize = storage.DefaultMaxKeySize + storage.DefaultMaxValueSize + 64

const usageText = `dkv - Quorum, a distributed key-value database (this CLI is in-memory only)

Usage:
  dkv put <key> <value>     store value under key
  dkv get <key>             print the value stored under key
  dkv delete <key>          remove key (succeeds whether or not it existed)
  dkv shell                 read commands from stdin
  dkv version               print the build version
  dkv help                  print this message

Exit codes:
  0  success
  1  key not found
  2  usage error (unknown command, wrong number of arguments)
  3  invalid input (empty key, key or value too large)
  4  internal error

Notes:
  Keys and values are opaque byte strings; keys are case-sensitive.
  Maximum key size 4 KiB, maximum value size 1 MiB.
  'delete' is idempotent and does not report whether the key existed.
`

const shellHelpText = `Commands:
  put <key> <value...>   store a value; the value is the rest of the line
                         (with no value, an empty value is stored)
  get <key>              print the value, or '(not found)'
  delete <key>           remove the key
  help                   print this message
  exit | quit            leave the shell (EOF also works)

A key entered here may not contain whitespace, because the shell splits on it.
Use 'dkv put "a key" value' from the command line for such keys.
`

// Run executes one invocation and returns the process exit code.
func (a *App) Run(args []string) int {
	if len(args) == 0 {
		a.errf("dkv: no command given\n\n%s", usageText)
		return ExitUsage
	}

	cmd, operands := args[0], args[1:]

	switch cmd {
	case "put":
		return a.cmdPut(operands)
	case "get":
		return a.cmdGet(operands)
	case "delete", "del":
		return a.cmdDelete(operands)
	case "shell":
		return a.cmdShell(operands)
	case "version":
		a.outf("dkv %s\n", version.Version)
		return ExitOK
	case "help", "-h", "--help":
		a.outf("%s", usageText)
		return ExitOK
	default:
		a.errf("dkv: unknown command %q\n\n%s", cmd, usageText)
		return ExitUsage
	}
}

// --------------------------------------------------------------- commands

func (a *App) cmdPut(args []string) int {
	if len(args) != 2 {
		a.errf("dkv: put requires exactly 2 arguments (key and value), got %d\n"+
			"usage: dkv put <key> <value>\n", len(args))
		return ExitUsage
	}
	if err := a.Store.Put(context.Background(), []byte(args[0]), []byte(args[1])); err != nil {
		return a.reportError(err)
	}
	a.noteEphemeral()
	return ExitOK
}

func (a *App) cmdGet(args []string) int {
	if len(args) != 1 {
		a.errf("dkv: get requires exactly 1 argument (key), got %d\n"+
			"usage: dkv get <key>\n", len(args))
		return ExitUsage
	}
	value, err := a.Store.Get(context.Background(), []byte(args[0]))
	if err != nil {
		return a.reportError(err)
	}
	// The value is written verbatim so that binary values survive a pipe, with
	// a trailing newline for terminal legibility. A future --raw flag can drop
	// the newline; it is not needed to make Phase 1 correct.
	a.write(value)
	a.write([]byte("\n"))
	return ExitOK
}

func (a *App) cmdDelete(args []string) int {
	if len(args) != 1 {
		a.errf("dkv: delete requires exactly 1 argument (key), got %d\n"+
			"usage: dkv delete <key>\n", len(args))
		return ExitUsage
	}
	if err := a.Store.Delete(context.Background(), []byte(args[0])); err != nil {
		return a.reportError(err)
	}
	a.noteEphemeral()
	return ExitOK
}

// --------------------------------------------------------------- shell

func (a *App) cmdShell(args []string) int {
	if len(args) != 0 {
		a.errf("dkv: shell takes no arguments, got %d\nusage: dkv shell\n", len(args))
		return ExitUsage
	}
	if a.Stdin == nil {
		a.errf("dkv: shell requires stdin\n")
		return ExitInternal
	}
	a.noteEphemeral()

	scanner := bufio.NewScanner(a.Stdin)
	scanner.Buffer(make([]byte, 0, 64<<10), maxLineSize)

	for {
		// The prompt goes to stderr, not stdout, so that piped output contains
		// only results and stays diffable in tests and usable in pipelines.
		a.errf("dkv> ")
		if !scanner.Scan() {
			break
		}
		if done := a.runShellLine(scanner.Text()); done {
			return ExitOK
		}
	}
	if err := scanner.Err(); err != nil {
		a.errf("\ndkv: reading input: %v\n", err)
		return ExitInternal
	}
	a.errf("\n")
	return ExitOK
}

// runShellLine executes one shell line. It reports whether the shell should
// exit. Errors are printed and the shell continues: a REPL that dies on a typo
// is not usable.
func (a *App) runShellLine(line string) (done bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return false
	}

	verb, rest, _ := strings.Cut(trimmed, " ")
	rest = strings.TrimLeft(rest, " \t")

	switch verb {
	case "exit", "quit":
		return true

	case "help":
		a.outf("%s", shellHelpText)

	case "put":
		// The key is the first token; the value is the remainder of the line
		// verbatim, so values may contain spaces. A bare `put k` stores an
		// empty value, which is a legal state distinct from an absent key.
		key, value, _ := strings.Cut(rest, " ")
		if key == "" {
			a.errf("error: put requires a key\n")
			return false
		}
		if err := a.Store.Put(context.Background(), []byte(key), []byte(value)); err != nil {
			a.errf("error: %v\n", err)
			return false
		}
		a.outf("OK\n")

	case "get":
		if rest == "" || strings.ContainsAny(rest, " \t") {
			a.errf("error: get requires exactly one key with no whitespace\n")
			return false
		}
		value, err := a.Store.Get(context.Background(), []byte(rest))
		switch {
		case errors.Is(err, storage.ErrNotFound):
			a.outf("(not found)\n")
		case err != nil:
			a.errf("error: %v\n", err)
		default:
			a.write(value)
			a.write([]byte("\n"))
		}

	case "delete", "del":
		if rest == "" || strings.ContainsAny(rest, " \t") {
			a.errf("error: delete requires exactly one key with no whitespace\n")
			return false
		}
		if err := a.Store.Delete(context.Background(), []byte(rest)); err != nil {
			a.errf("error: %v\n", err)
			return false
		}
		a.outf("OK\n")

	default:
		a.errf("error: unknown command %q (try 'help')\n", verb)
	}
	return false
}

// --------------------------------------------------------------- plumbing

// reportError prints err and maps it to an exit code. The mapping is total:
// every storage sentinel has a code, and anything unrecognised becomes an
// internal error rather than a silent success.
func (a *App) reportError(err error) int {
	a.errf("dkv: %v\n", err)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return ExitNotFound
	case errors.Is(err, storage.ErrKeyEmpty),
		errors.Is(err, storage.ErrKeyTooLarge),
		errors.Is(err, storage.ErrValueTooLarge):
		return ExitInvalidInput
	default:
		// ErrClosed, ErrInvalidOptions, context errors, and anything a future
		// Store adds. Reaching here with a user-caused error means the mapping
		// above is incomplete, which the CLI tests are written to catch.
		return ExitInternal
	}
}

func (a *App) noteEphemeral() {
	if a.Ephemeral {
		a.errf("note: this store is in-memory; data does not survive process exit (phase 1, see docs/ROADMAP.md)\n")
	}
}

func (a *App) outf(format string, args ...any) {
	if a.Stdout != nil {
		fmt.Fprintf(a.Stdout, format, args...)
	}
}

func (a *App) errf(format string, args ...any) {
	if a.Stderr != nil {
		fmt.Fprintf(a.Stderr, format, args...)
	}
}

func (a *App) write(b []byte) {
	if a.Stdout != nil {
		_, _ = a.Stdout.Write(b)
	}
}
