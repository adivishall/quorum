# dkv — distributed key-value store
#
# Paths are quoted throughout because the checkout directory may contain spaces.

GO      ?= go
PKGS    := ./...
BIN     := bin

.PHONY: all build test race vet fmt fmtcheck lint tidy clean check

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

## tidy — sync go.mod
tidy:
	$(GO) mod tidy

## check — the gate every phase must pass
check: fmtcheck vet race

clean:
	rm -rf "$(BIN)" coverage.out coverage.html
	$(GO) clean -testcache
