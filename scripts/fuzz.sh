#!/usr/bin/env bash
#
# fuzz.sh — run every Go fuzz target in the repository for FUZZTIME each
# (default 10s). Targets are discovered, not listed, so a new one cannot be
# forgotten. Fails if any target fails.
#
# Usage: scripts/fuzz.sh            FUZZTIME=60s scripts/fuzz.sh
set -uo pipefail
cd "$(dirname "$0")/.."
FUZZTIME="${FUZZTIME:-10s}"
FAIL=0
N=0
while IFS= read -r file; do
  dir="./$(dirname "$file")"
  for target in $(grep -oE '^func Fuzz[A-Za-z0-9_]+' "$file" | sed 's/^func //'); do
    N=$((N + 1))
    # A short minimize time keeps the fuzzer exploring instead of spending up to
    # a minute minimizing each newly interesting input.
    if go test "$dir" -run '^$' -fuzz "^${target}\$" -fuzztime "$FUZZTIME" -fuzzminimizetime 1s >/tmp/fuzz.$$.log 2>&1; then
      echo "✓ $dir $target"
    else
      echo "✗ $dir $target"
      tail -30 /tmp/fuzz.$$.log
      FAIL=$((FAIL + 1))
    fi
  done
done < <(grep -rlE '^func Fuzz' --include='*_test.go' . | sed 's|^\./||' | sort)
rm -f /tmp/fuzz.$$.log
echo "== $((N - FAIL))/$N fuzz targets passed (FUZZTIME=$FUZZTIME) =="
[ "$FAIL" -eq 0 ]
