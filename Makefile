GO ?= go
BIN ?= bin
STATICCHECK_VERSION ?= v0.8.2
FUZZTIME ?= 30s

.PHONY: all build test race vet fmtcheck staticcheck lint check fuzz chaos clean

all: lint race build

build:
	$(GO) build -o $(BIN)/ ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmtcheck:
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

# Runs staticcheck at a pinned version without adding it to go.mod.
staticcheck:
	$(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

lint: fmtcheck vet staticcheck

# The pre-commit gate.
check: lint race

fuzz:
	$(GO) test ./internal/wal -run='^$$' -fuzz=FuzzDecodeAll -fuzztime=$(FUZZTIME)
	$(GO) test ./internal/kv -run='^$$' -fuzz=FuzzDecodeCommand -fuzztime=$(FUZZTIME)
	$(GO) test ./internal/kv -run='^$$' -fuzz=FuzzRestore -fuzztime=$(FUZZTIME)

# Real processes under faults, checked for linearizability; see cmd/kvchaos.
# Not part of check: it takes a minute and depends on real timing.
chaos:
	scripts/chaos.sh $(CHAOS_ARGS)

clean:
	rm -rf $(BIN)
