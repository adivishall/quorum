#!/usr/bin/env sh
# Fail if any Go source file in the tree is excluded by .gitignore.
#
# This exists because an unanchored binary name in .gitignore (e.g. `dkv`) also
# matches the source directory `cmd/dkv`, silently dropping real source from
# every commit. The symptom appears much later, as a clean clone that does not
# build. Cheap to check, expensive to discover.
set -eu

cd "$(dirname "$0")/.."

ignored=$(
  find . -name '*.go' -not -path './dashboard/*' -print \
    | sed 's|^\./||' \
    | git check-ignore --stdin 2>/dev/null || true
)

if [ -n "$ignored" ]; then
  echo "error: these Go source files are excluded by .gitignore:" >&2
  echo "$ignored" | sed 's/^/  /' >&2
  echo "" >&2
  echo "Anchor the offending pattern with a leading slash (e.g. '/dkv' not 'dkv')." >&2
  exit 1
fi

untracked=$(git ls-files --others --exclude-standard -- '*.go' || true)
if [ -n "$untracked" ]; then
  echo "note: untracked Go files (add them before committing the phase):" >&2
  echo "$untracked" | sed 's/^/  /' >&2
fi

echo "check-ignore: ok"
