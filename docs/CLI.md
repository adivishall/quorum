# CLI

Status: **unchanged since Phase 1.** The CLI constructs an in-process, in-memory store, so
nothing survives process exit. The storage layer underneath it has been durable since Phase 2
and LSM-backed since Phase 3 (`docs/LSM.md`), but the CLI is not wired to a data directory
yet — that and a `--data-dir` flag belong to Phase 15, and Phase 7 turns `dkv` into a network
client. The command surface below is intended to survive both changes.

---

## Commands

```
dkv put <key> <value>     store value under key
dkv get <key>             print the value stored under key
dkv delete <key>          remove key (succeeds whether or not it existed)
dkv shell                 read commands from stdin
dkv version               print the build version
dkv help                  print usage
```

`del` is accepted as an alias for `delete`.

## Exit codes

These are a contract. Scripts branch on them, so they are tested
(`TestCLIExitCodes`) and may not be renumbered without a note here.

| Code | Meaning | Example |
|---|---|---|
| 0 | success | `dkv put k v` |
| 1 | key not found | `dkv get absent` |
| 2 | usage error — unknown command, wrong operand count | `dkv get a b` |
| 3 | invalid input — empty key, key or value over the limit | `dkv put "" v` |
| 4 | internal error — anything the user did not cause | store closed |

`1` is separate from the error codes on purpose. `dkv get missing` is not a malfunction, and
a script has to be able to tell "no such key" from "you typed it wrong". This is how
`grep(1)` behaves and for the same reason.

## Stream discipline

- **stdout carries data only**: the value from `get`, `OK` / `(not found)` from the shell,
  usage text from `dkv help`.
- **stderr carries everything else**: diagnostics, the shell prompt, the in-memory notice,
  and usage text when it is printed *because of* an error.

So `dkv get k > value.txt` never captures an error message, and a failing command writes
nothing to stdout at all. Both are asserted in the tests.

Values are written to stdout verbatim followed by a single newline, so binary values survive
a pipe. (A `--raw` flag to drop the trailing newline is a reasonable later addition; it is
not needed for correctness.)

## Interactive shell

```
$ dkv shell
dkv> put user:123 Adi
OK
dkv> get user:123
Adi
dkv> delete user:123
OK
dkv> get user:123
(not found)
dkv> exit
```

Details that are easy to get wrong and are therefore pinned by tests:

- **The prompt goes to stderr.** That is what makes the shell pipeable:
  `printf 'get a\nget b\n' | dkv shell 2>/dev/null` emits only the two values.
- **`put <key> <value...>` takes the rest of the line as the value**, so values may contain
  spaces. `put k` with no value stores an *empty value*, which is a present key — distinct
  from an absent one.
- **Keys entered in the shell cannot contain whitespace**, because the shell splits on it.
  Use the one-shot form (`dkv put "a key" v`) for those; argv preserves whitespace.
- **Errors never end the session.** A typo prints a message and the shell continues.
- **EOF exits with code 0**, as does `exit` / `quit`.
- **A line too long to read is an error, not a truncation.** The shell's line buffer is
  raised to `MaxKeySize + MaxValueSize + 64` so that a maximum-size value fits; beyond that
  the shell reports the read failure and exits 4 rather than silently storing a truncated
  value. (`bufio.Scanner` defaults to a 64 KiB token, far below the 1 MiB legal value — this
  needed an explicit `Buffer` call and has a regression test.)

## A real limit worth knowing: argv cannot carry a maximum-size value

A 1 MiB value is legal for the store but **cannot be passed as a command-line argument** on
macOS, where `ARG_MAX` is 1 MiB: the kernel rejects the `exec` before `dkv` runs at all, and
the shell reports `argument list too long` with exit 127. Measured on the development
machine:

| Value size via argv | Result |
|---|---|
| 64 KiB | ok |
| 256 KiB | ok |
| 1 MiB | `argument list too long` (exit 127, from the shell — dkv never starts) |

This is an operating-system limit, not a dkv limit, which is why `dkv shell` can carry a
value that `dkv put` cannot. It is listed here rather than hidden because a user hitting it
gets an error from their shell that does not mention dkv at all.

## Error messages

The program name is added exactly once, by whatever is printing:

```
$ dkv get user:123
dkv: get "user:123": key not found
```

The library error itself renders as `get "user:123": key not found` — no `dkv:` prefix. This
follows the standard library (`*fs.PathError` renders as `open /foo: no such file or
directory`, and the command adds its own name). Getting this wrong produced
`dkv: dkv: get "user:123": key not found`; there is a regression test for it.

Keys inside messages are quoted, escaped, and truncated at 64 bytes by `storage.SafeKey`, so
a 4 KiB key or one containing newlines or NUL bytes cannot corrupt a log line.
