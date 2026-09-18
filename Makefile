# dkv — distributed key-value store
#
# Paths are quoted throughout because the checkout directory may contain spaces.

GO      ?= go
PKGS    := ./...
BIN     := bin

.PHONY: all build test race vet fmt fmtcheck checkignore integration bench tidy clean check

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

## bench — indicative measurements; Phase 5 is where benchmarking is done properly
bench:
	$(GO) test -run='^$$' -bench=. -benchtime=2000x ./internal/storage/...

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
