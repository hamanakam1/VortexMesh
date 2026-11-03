GO ?= go

.PHONY: all check fmt fmt-check vet build test race cover tidy demo clean

all: check

## fmt: rewrite files with gofmt
fmt:
	gofmt -w .

## fmt-check: fail if any file needs formatting
fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "These files are not gofmt'd:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

## vet: run the standard static checks
vet:
	$(GO) vet ./...

## build: compile every package
build:
	$(GO) build ./...

## test: run the test suite
test:
	$(GO) test -count=1 ./...

## race: run the test suite under the race detector (what CI runs)
race:
	$(GO) test -race -count=1 ./...

## cover: write coverage.out and print a per-package summary
cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

## tidy: synchronise go.mod and go.sum
tidy:
	$(GO) mod tidy

## check: everything CI enforces
check: tidy fmt-check vet build race

## demo: run the self-contained demo
demo:
	$(GO) run ./cmd/vortexmesh

## clean: remove local build and coverage output
clean:
	rm -f coverage.out
	rm -rf bin dist
