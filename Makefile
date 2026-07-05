.DEFAULT_GOAL := help
.PHONY: audit build clean coverage-check docs-audit fmt help lint modernize-check test test-integration-cover test-integration-live test-integration-smoke tidy vuln

REMOVED_PUBLIC_TERMS = hermes\x20acp|pro\x78y|compatibilit\x79|deprecat\x65d|legac\x79|migratio\x6e|session/imp\x6frt|sdkMessag\x65|emitRawSDKMessag\x65s|setGoa\x6c|goa\x6cs|\x4e\x45\x53|SSE\x20MCP|mcpCapabilities\x2eacp|ExportSessio\x6e|ImportSessio\x6e|DeleteSessio\x6e|ParseConfi\x67|HermesSessio\x6e

## build: build all packages
build:
	go build ./...

## test: run unit tests
test:
	go test ./...

## coverage-check: enforce repository coverage gate
coverage-check:
	go test -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out | awk 'BEGIN { found = 0; min = 100.0 } /^total:/ { found = 1; coverage = $$3; sub(/%/, "", coverage); if (coverage + 0 < min) { printf "total coverage %s, want at least %.1f%%\n", $$3, min; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-integration-smoke: run live integration tests that do not spend model tokens
test-integration-smoke:
	ACP_GO_HERMES_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=240s -v ./integration/...

## test-integration-live: run live integration tests that spend model tokens
test-integration-live:
	ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_RUN_LIVE_TOKENS=1 go test -race -count=1 -tags=integration -timeout=180s -v ./integration/...

## test-integration-cover: run smoke integration tests with compiled binary coverage
test-integration-cover:
	rm -rf .tmp/integration-cover coverage-integration.out
	mkdir -p .tmp/integration-cover/data
	go build -cover -coverpkg=./... -o .tmp/integration-cover/acp-go-hermes ./cmd/acp-go-hermes
	ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_AGENT_BINARY=$$(pwd)/.tmp/integration-cover/acp-go-hermes GOCOVERDIR=$$(pwd)/.tmp/integration-cover/data go test -race -count=1 -tags=integration -timeout=240s -v ./integration/...
	go tool covdata percent -i=.tmp/integration-cover/data
	go tool covdata textfmt -i=.tmp/integration-cover/data -o coverage-integration.out

## lint: run golangci-lint
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run ./...

## fmt: format code with golangci-lint
fmt:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 fmt ./...

## tidy: tidy go modules
tidy:
	go mod tidy

## vuln: run govulncheck
vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

## modernize-check: check Go modernizations without changing files
modernize-check:
	@tmp=$$(mktemp); if ! go fix -n ./... >"$$tmp" 2>&1; then cat "$$tmp"; rm -f "$$tmp"; exit 1; fi; rm -f "$$tmp"

## docs-audit: check public docs and examples for removed public terms
docs-audit:
	@pattern=$$(printf '%b' '$(REMOVED_PUBLIC_TERMS)'); ! rg -n -- "$$pattern" README.md doc.go docs.json docs examples cmd/acp-go-hermes/*.go AGENTS.md

## audit: run local checks
audit: fmt lint build test coverage-check vuln modernize-check docs-audit
	go mod tidy -diff
	go mod verify

## clean: remove build artifacts
clean:
	rm -rf .tmp coverage.out coverage-integration.out coverage-summary.txt

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
