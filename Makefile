.DEFAULT_GOAL := help

GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: audit build clean coverage-check docs-audit fmt fmt-check help lint modernize-check test test-cross-compile test-integration-attended test-integration-cover test-integration-keystore test-integration-live test-integration-native-browser test-integration-smoke test/cover tidy vuln

## build: compile all packages
build:
	go build ./...

## lint: run pinned golangci-lint
lint:
	$(GOLANGCI_LINT) run --timeout=10m --allow-parallel-runners ./...

## fmt-check: require gofmt-clean Go files
fmt-check:
	@test -z "$$(gofmt -l .)"

## fmt: format Go files
fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')
	$(GOLANGCI_LINT) fmt ./...

# The complete unit suite includes process and filesystem fault matrices.
GO_TEST_TIMEOUT ?= 40m

## test: run unit tests with race detector and shuffled order
test:
	go test -race -shuffle=on -timeout=$(GO_TEST_TIMEOUT) ./...

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
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/hermes-windows.test .
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/hermes-internal-windows.test ./internal/hermes
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/hermes-cmd-windows.test ./cmd/acp-go-hermes
	GOOS=windows GOARCH=amd64 go build ./...

## coverage-check: run shuffled race tests and report statement coverage
coverage-check:
	go test -race -shuffle=on -coverprofile=coverage.out -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) ./...
	@awk 'NR > 1 && $$(NF - 1) > 0 { found = 1 } END { if (!found) { print "coverage profile has no statement blocks"; exit 1 } }' coverage.out
	@report=$$(go tool cover -func=coverage.out) || exit $$?; printf '%s\n' "$$report" | awk '/^total:/ { found = 1; if ($$3 !~ /^[0-9]+([.][0-9]+)?%$$/) { print "invalid total coverage line"; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-integration-smoke: compile and run integration tests that can skip without live auth
test-integration-smoke:
	ACP_GO_HERMES_RUN_LIVE_TOKENS=0 ACP_GO_HERMES_RUN_ATTENDED=0 ACP_GO_HERMES_RUN_KEYSTORE=0 ACP_GO_HERMES_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=240s -v ./integration/... ./internal/hermes

## test-integration-live: run live Hermes CLI integration tests
test-integration-live:
	ACP_GO_HERMES_RUN_ATTENDED=0 ACP_GO_HERMES_RUN_KEYSTORE=0 ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_RUN_LIVE_TOKENS=1 go test -race -count=1 -tags=integration -timeout=300s -v ./integration/... ./internal/hermes

## test-integration-attended: run provider-auth flows a human must approve in real time
# go test exits 0 when -run selects nothing, so the exit status alone reports a
# successful login for a tier that ran none. The run is piped through tee rather
# than redirected so the relayed login URL still reaches the watching operator
# live, and the guard requires a top-level pass line, which no empty selection
# and no skip can produce.
test-integration-attended:
	@set -eu; dir=$$(mktemp -d); trap 'rm -rf "$$dir"' EXIT HUP INT TERM; \
	dir=$$(cd "$$dir" && pwd); \
	export ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_RUN_ATTENDED=1 ACP_GO_HERMES_RUN_LIVE_TOKENS=0 ACP_GO_HERMES_RUN_KEYSTORE=0; \
	go test -race -c -tags=integration -o "$$dir/integration.test" ./integration; \
	"$$dir/integration.test" -test.list '^TestAttendedProviderAuth' >"$$dir/selected"; \
	expected=$$(grep -Ec '^TestAttendedProviderAuth' "$$dir/selected" || true); \
	[ "$$expected" -gt 0 ] || { echo 'attended selector discovered no tests'; exit 1; }; \
	{ status=0; (cd integration && "$$dir/integration.test" -test.v -test.count=1 -test.timeout=1200s -test.run '^TestAttendedProviderAuth') 2>&1 || status=$$?; echo "$$status" >"$$dir/status"; } | tee "$$dir/output"; \
	status=$$(cat "$$dir/status"); passed=$$(grep -Ec '^--- PASS: TestAttendedProviderAuth' "$$dir/output" || true); \
	skipped=$$(grep -Ec '^[[:space:]]*--- SKIP:' "$$dir/output" || true); empty=$$(grep -c 'no tests to run' "$$dir/output" || true); \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq "$$expected" ] || { echo "attended tests passed $$passed of $$expected"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'attended test skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'attended selector ran no tests'; exit 1; }

## test-integration-keystore: run Linux state-boundary and browser probes
test-integration-keystore:
	ACP_GO_HERMES_RUN_LIVE_TOKENS=0 ACP_GO_HERMES_RUN_ATTENDED=0 ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_RUN_KEYSTORE=1 go test -race -count=1 -tags=integration -timeout=900s -v -run TestKeystore ./...

## test-integration-native-browser: require one pinned native Linux provider-auth no-launch proof
test-integration-native-browser:
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ACP_GO_HERMES_RUN_LIVE_TOKENS=0 ACP_GO_HERMES_RUN_ATTENDED=0 ACP_GO_HERMES_RUN_KEYSTORE=0 ACP_GO_HERMES_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration,browsercanary -timeout=1200s -v -run '^TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher$$' ./integration/... 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -c '^--- PASS: TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher ' "$$log" || true); skipped=$$(grep -Ec '^[[:space:]]*--- SKIP: TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher(/| )' "$$log" || true); empty=$$(grep -c 'no tests to run' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq 1 ] || { echo "native browser pass count $$passed, want exactly 1"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'required native browser canary skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'required native browser selector ran no tests'; exit 1; }

## test-integration-cover: run smoke integration tests with compiled binary coverage
test-integration-cover:
	@set -eu; mkdir -p .tmp; dir=$$(mktemp -d "$$(pwd)/.tmp/integration-cover.XXXXXX"); trap 'rm -rf "$$dir"' EXIT HUP INT TERM; \
	mkdir "$$dir/data"; \
	go build -cover -coverpkg=./... -o "$$dir/acp-go-hermes" ./cmd/acp-go-hermes; \
	{ status=0; ACP_GO_HERMES_RUN_LIVE_TOKENS=0 ACP_GO_HERMES_RUN_ATTENDED=0 ACP_GO_HERMES_RUN_KEYSTORE=0 ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_AGENT_BINARY="$$dir/acp-go-hermes" GOCOVERDIR="$$dir/data" go test -race -count=1 -tags=integration -timeout=240s -v ./integration/... ./internal/hermes 2>&1 || status=$$?; echo "$$status" >"$$dir/status"; } | tee "$$dir/output"; \
	status=$$(cat "$$dir/status"); [ "$$status" -eq 0 ] || exit "$$status"; \
	[ -n "$$(find "$$dir/data" -name 'covcounters.*' -type f -size +0c -print -quit)" ] || { echo 'compiled adapter produced no coverage counters'; exit 1; }; \
	go tool covdata percent -i="$$dir/data"; \
	go tool covdata textfmt -i="$$dir/data" -o coverage-integration.out

## docs-audit: check public docs, examples, required files, and CLI flags
docs-audit:
	@missing=0; for file in README.md doc.go docs.json example_test.go AGENTS.md docs/overview.mdx docs/core/sessions.mdx docs/core/prompt-streaming.mdx docs/features/authentication.mdx docs/features/elicitation.mdx docs/features/mcp.mdx docs/features/models-config.mdx docs/features/permissions.mdx docs/features/raw-events.mdx docs/features/session-store.mdx docs/get-started/examples.mdx docs/get-started/install.mdx docs/get-started/quickstart.mdx docs/get-started/run-modes.mdx docs/operations/observability.mdx docs/operations/security.mdx docs/operations/troubleshooting.mdx docs/reference/acp-methods.mdx docs/reference/cli.mdx docs/reference/go-api.mdx docs/reference/meta.mdx docs/reference/updates.mdx examples/minimal-client/main.go examples/resume-from-file/main.go examples/interactive-chat/main.go; do if [ ! -f "$$file" ]; then echo "missing required docs file: $$file"; missing=1; fi; done; exit $$missing
	@for flag in -path -home -scratch-dir -provider-auth-root -shared-hermes-home -model -debug -version; do rg -q -- "$$flag" docs/reference/cli.mdx cmd/acp-go-hermes/main.go || { echo "missing CLI flag in docs/code: $$flag"; exit 1; }; done
	@rg -q 'HostAuthority' docs/reference/go-api.mdx docs/operations/security.mdx

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

## modernize-check: check Go modernizations without changing files
modernize-check:
	go fix -diff ./...

## audit: run repository checks
audit: fmt-check lint build coverage-check test-cross-compile tidy vuln modernize-check docs-audit
	go mod verify

## test/cover: open HTML coverage report
test/cover: coverage-check
	go tool cover -html=coverage.out

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
