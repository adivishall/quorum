# Quorum — a distributed key-value database
#
# Paths are quoted throughout because the checkout directory may contain spaces.

GO      ?= go
PKGS    := ./...
BIN     := bin

.PHONY: all build test race vet fmt fmtcheck checkignore integration mutation faults fuzz bench benchsuite dkvbench tidy clean check

all: check

## build — compile every package and command
build:
	$(GO) build -o "$(BIN)/" $(PKGS)

## test — unit tests
test:
	$(GO) test $(PKGS)

## race — unit tests under the race detector (required before every phase commit)
race:
	$(GO) test -race $(PKGS)

## integration — multi-process tests, including real SIGKILL crash recovery
integration:
	$(GO) test -race -count=1 -v ./tests/integration/

## mutation — mutation testing (Phase 9 Raft rules, Phase 10 failure handling and
## fault-model fidelity, Phase 11 crash-recovery rules, Phase 12 client-visible
## consistency and the checker itself). Applies deliberate rule-violating edits to the source,
## runs the tests that must catch each, and requires every mutant to be killed
## (edits are reverted via git). Needs a clean working tree for the files it
## mutates. See docs/RAFT.md §12a and docs/FAULTS.md.
mutation:
	./scripts/mutation.sh

## faults — Phase 10 deterministic fault schedules at a large seed budget (plain
## `go test` runs a small seed set), the Phase 11 crash matrix (a crash at every
## driver and I/O boundary a scenario reaches, in every crash mode), and the
## Phase 12 client workloads (every KV profile's history checked for
## linearizability, plus INV-X5..X8). Every run is replayable: a failure prints
## the exact command and a minimized script (or, for the matrix, the exact
## crashat event). See docs/FAULTS.md, docs/CRASH_RECOVERY.md and
## docs/LINEARIZABILITY.md.
FAULT_SEEDS ?= 200
faults:
	$(GO) test -count=1 -run 'TestRandomizedFaultSchedules|TestSameSeedSameTrace|TestTraceIsPlatformIndependent|TestCrashMatrix|TestKVSeededHistoriesAreLinearizable|TestKVSameSeedSameHistory' ./internal/raftsim -raftsim.seeds=$(FAULT_SEEDS)

## fuzz — run every fuzz target in the repository for FUZZTIME each (default 10s).
FUZZTIME ?= 10s
fuzz:
	FUZZTIME="$(FUZZTIME)" ./scripts/fuzz.sh

## bench — the Go micro-benchmarks (testing.B). Quick order-of-magnitude checks.
bench:
	$(GO) test -run='^$$' -bench=. -benchmem -benchtime=2000x ./internal/storage/...

## dkvbench — build the Phase 5 workload orchestrator
dkvbench:
	$(GO) build -o "$(BIN)/dkvbench" ./cmd/dkvbench

## benchsuite — run the full Phase 5 benchmark suite and write JSON results.
## Reproduces the numbers in docs/BENCHMARKS.md; see that document for the
## methodology. Override DATASET/VALUE/RUNS/SEED/BENCHDIR/BENCHOUT as needed.
DATASET  ?= 100000
VALUE    ?= 100
RUNS     ?= 5
SEED     ?= 1
BENCHDIR ?= $(shell mktemp -d)/dkvbench
BENCHOUT ?= bench/results/latest.json
benchsuite: dkvbench
	@mkdir -p bench/results
	"$(BIN)/dkvbench" -suite all -dataset $(DATASET) -value $(VALUE) \
		-runs $(RUNS) -seed $(SEED) -dir "$(BENCHDIR)" -out "$(BENCHOUT)"

## vet — static analysis
vet:
	$(GO) vet $(PKGS)

## fmt — format
fmt:
	$(GO) fmt $(PKGS)

## fmtcheck — fail if anything is unformatted (used by CI)
fmtcheck:
	@out="$$(gofmt -l . | grep -v '^dashboard/' || true)"; \
	if [ -n "$$out" ]; then echo "unformatted files:"; echo "$$out"; exit 1; fi

## checkignore — fail if .gitignore excludes real source (see scripts/check-ignore.sh)
checkignore:
	@./scripts/check-ignore.sh

## tidy — sync go.mod
tidy:
	$(GO) mod tidy

## check — the gate every phase must pass
check: fmtcheck checkignore vet race

clean:
	rm -rf "$(BIN)" coverage.out coverage.html
	$(GO) clean -testcache
