.DEFAULT_GOAL := help

GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

REMOVED_PUBLIC_TERMS = hermes\x20acp|pro\x78y|compatibilit\x79|deprecat\x65d|legac\x79|migratio\x6e|session/imp\x6frt|sdkMessag\x65|emitRawSDKMessag\x65s|setGoa\x6c|goa\x6cs|\x4e\x45\x53|SSE\x20MCP|mcpCapabilities\x2eacp|ExportSessio\x6e|ImportSessio\x6e|DeleteSessio\x6e|ParseConfi\x67|HermesSessio\x6e

.PHONY: audit build clean coverage-check docs-audit fmt fmt-check help lint modernize-check test test-cross-compile test-integration-cover test-integration-live test-integration-smoke test/cover tidy vuln

## build: compile all packages
build:
	go build ./...

## lint: run pinned golangci-lint
lint:
	$(GOLANGCI_LINT) run ./...

## fmt-check: require gofmt-clean Go files
fmt-check:
	@test -z "$$(gofmt -l .)"

## fmt: format Go files
fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')
	$(GOLANGCI_LINT) fmt ./...

## test: run unit tests with race detector and shuffled order
test:
	go test -race -shuffle=on ./...

## test-cross-compile: compile platform-specific test branches
test-cross-compile:
	rm -rf .tmp/cross
	mkdir -p .tmp/cross
	GOOS=linux GOARCH=amd64 go test -c -o .tmp/cross/hermes-linux.test .
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/hermes-darwin.test .
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/hermes-internal-darwin.test ./internal/hermes
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/hermes-cmd-darwin.test ./cmd/acp-go-hermes
	GOOS=darwin GOARCH=arm64 go build ./...
	GOOS=freebsd GOARCH=amd64 go build ./...
	GOOS=openbsd GOARCH=amd64 go build ./...
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/hermes-internal-windows.test ./internal/hermes
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/hermes-cmd-windows.test ./cmd/acp-go-hermes
	GOOS=windows GOARCH=amd64 go build ./...

## coverage-check: require 100% statement coverage with race instrumentation
coverage-check:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out | awk 'BEGIN { found = 0 } /^total:/ { found = 1; if ($$3 != "100.0%") { printf "total coverage %s, want 100.0%%\n", $$3; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-integration-smoke: compile and run integration tests that can skip without live auth
test-integration-smoke:
	ACP_GO_HERMES_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=240s -v ./integration/... ./internal/hermes

## test-integration-live: run live Hermes CLI integration tests
test-integration-live:
	ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_RUN_LIVE_TOKENS=1 go test -race -count=1 -tags=integration -timeout=300s -v ./integration/... ./internal/hermes

## test-integration-cover: run smoke integration tests with compiled binary coverage
test-integration-cover:
	rm -rf .tmp/integration-cover coverage-integration.out
	mkdir -p .tmp/integration-cover/data
	go build -cover -coverpkg=./... -o .tmp/integration-cover/acp-go-hermes ./cmd/acp-go-hermes
	ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_AGENT_BINARY=$$(pwd)/.tmp/integration-cover/acp-go-hermes GOCOVERDIR=$$(pwd)/.tmp/integration-cover/data go test -race -count=1 -tags=integration -timeout=240s -v ./integration/... ./internal/hermes
	go tool covdata percent -i=.tmp/integration-cover/data
	go tool covdata textfmt -i=.tmp/integration-cover/data -o coverage-integration.out

## docs-audit: check public docs, examples, required files, CLI flags, and removed terms
docs-audit:
	@missing=0; for file in README.md doc.go docs.json example_test.go AGENTS.md docs/overview.mdx docs/core/sessions.mdx docs/core/prompt-streaming.mdx docs/features/authentication.mdx docs/features/elicitation.mdx docs/features/mcp.mdx docs/features/models-config.mdx docs/features/permissions.mdx docs/features/raw-events.mdx docs/features/session-store.mdx docs/get-started/examples.mdx docs/get-started/install.mdx docs/get-started/quickstart.mdx docs/get-started/run-modes.mdx docs/operations/observability.mdx docs/operations/security.mdx docs/operations/troubleshooting.mdx docs/reference/acp-methods.mdx docs/reference/cli.mdx docs/reference/go-api.mdx docs/reference/meta.mdx docs/reference/updates.mdx examples/minimal-client/main.go examples/resume-from-file/main.go examples/interactive-chat/main.go; do if [ ! -f "$$file" ]; then echo "missing required docs file: $$file"; missing=1; fi; done; exit $$missing
	@for flag in -path -home -model -debug -version; do rg -q -- "$$flag" docs/reference/cli.mdx cmd/acp-go-hermes/main.go || { echo "missing CLI flag in docs/code: $$flag"; exit 1; }; done
	@pattern=$$(printf '%b' '$(REMOVED_PUBLIC_TERMS)'); ! rg -n -- "$$pattern" README.md doc.go docs.json docs examples cmd/acp-go-hermes/*.go AGENTS.md
	@! rg -n -- 'options\.(mode|permission)\b|options"?\s*:.*\b(mode|permission)\b' docs README.md doc.go || { echo "removed _meta.hermes.options.mode/permission field reappeared in docs"; exit 1; }

## clean: remove build artifacts
clean:
	rm -rf .tmp coverage.out coverage-integration.out coverage-summary.txt

## tidy: verify module files are tidy
tidy:
	go mod tidy -diff

## vuln: run govulncheck from the go.mod tool directive
# golang.org/x/vuln v1.4.0 panics in x/tools SSA on Go 1.26 generics;
# keep the tool directive pinned at v1.5.0 or newer.
vuln:
	go tool govulncheck ./...

## modernize-check: preview Go modernizations without changing files
modernize-check:
	go fix -n ./...

## audit: run repository checks
audit: fmt-check lint build test coverage-check test-cross-compile tidy vuln modernize-check docs-audit
	go mod verify

## test/cover: open HTML coverage report
test/cover: coverage-check
	go tool cover -html=coverage.out

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
