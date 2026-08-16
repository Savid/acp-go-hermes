.DEFAULT_GOAL := help

.PHONY: test-trusted-supervisor _privileged-shard-guard _privileged-shard-coverage _privileged-shard-trusted-supervisor _privileged-coverage-gate

GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

REMOVED_PUBLIC_TERMS = hermes\x20acp|pro\x78y|compatibilit\x79|deprecat\x65d|legac\x79|migratio\x6e|session/imp\x6frt|sdkMessag\x65|emitRawSDKMessag\x65s|setGoa\x6c|goa\x6cs|\x4e\x45\x53|SSE\x20MCP|mcpCapabilities\x2eacp|ExportSessio\x6e|ImportSessio\x6e|DeleteSessio\x6e|ParseConfi\x67|HermesSessio\x6e

.PHONY: audit build clean coverage-check docs-audit fmt fmt-check help lint modernize-check test test-cross-compile test-integration-attended test-integration-cover test-integration-keystore test-integration-live test-integration-native-browser test-integration-smoke test/cover tidy vuln

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

# Every isolated native launch claims the standalone agent identity, and that
# claim proves the identity vacant across every task in the PID namespace. In
# the initial namespace the suite requires, that is the whole host, so the
# adapter package runs far past the ten-minute default.
GO_TEST_TIMEOUT ?= 40m

## test: run unit tests with race detector and shuffled order
test:
	go test -race -shuffle=on -timeout=$(GO_TEST_TIMEOUT) ./...

## test-trusted-supervisor: run Linux root-only native authority tests
test-trusted-supervisor:
	@test "$$(uname -s)" = Linux
	@test "$$(id -u)" -eq 0
	@for directory in /var/lib/acp-go /var/lib/acp-go/agent-identities; do if [ ! -e "$$directory" ] && [ ! -L "$$directory" ]; then install -d -o root -g root -m 0700 "$$directory"; fi; [ "$$(stat -c '%F %u %g %a' -- "$$directory")" = 'directory 0 0 700' ] || { echo "unsafe trusted-supervisor authority directory $$directory" >&2; exit 1; }; done
	@selector='^(Test.*(ProcessIsolationActual|TrustedSupervisor|SupervisorGuardianSIGKILL|SupervisorLivenessSIGKILL|GeneratedNative|NativeOwnedDirectory|BorrowedIdentityAdoption|BorrowedDomainAdoption|BorrowedDisposition|AgentIdentityLock|AgentStandalone|AuthorityDomain|IdentityDisposition|PersistentProof|SupervisorConfigIsSealed|CommandCreatorThread|ProviderCreator|SecurityLimits).*)$$'; listing=$$(mktemp); log=$$(mktemp); rc=$$(mktemp); module=$$(go list -mod=readonly -m); status=$$?; \
	[ "$$status" -eq 0 ] || { rm -f "$$listing" "$$log" "$$rc"; exit "$$status"; }; \
	go test -list "$$selector" ./... >"$$listing"; status=$$?; \
	[ "$$status" -eq 0 ] || { rm -f "$$listing" "$$log" "$$rc"; exit "$$status"; }; \
	required='TrustedSupervisor SupervisorGuardianSIGKILL SupervisorGuardianSIGKILLBeforeNativeLaunchRefusesStartAndCompletesAfterECHILD SupervisorLivenessSIGKILL GeneratedNative BorrowedIdentityAdoption BorrowedDomainAdoption BorrowedDisposition AgentIdentityLock AgentStandalone AuthorityDomain IdentityDisposition CommandCreatorThread SecurityLimits ProcessIsolationActual'; case "$$module" in github.com/savid/acp-go-amp|github.com/savid/acp-go-claude|github.com/savid/acp-go-hermes|github.com/savid/acp-go-pi) ;; github.com/savid/acp-go-codex|github.com/savid/acp-go-opencode) required="$$required PersistentProof SupervisorConfigIsSealed ProviderCreator" ;; *) rm -f "$$listing" "$$log" "$$rc"; echo "unrecognized trusted-supervisor module $$module"; exit 1 ;; esac; \
	for class in $$required; do grep -Eq "^Test.*$${class}" "$$listing" || { rm -f "$$listing" "$$log" "$$rc"; echo "trusted-supervisor selector discovered no $${class} tests"; exit 1; }; done; \
	expected=$$(grep -Ec '^Test' "$$listing" || true); rm -f "$$listing"; \
	[ "$$expected" -gt 0 ] || { rm -f "$$log" "$$rc"; echo 'trusted-supervisor selector discovered no tests'; exit 1; }; \
	{ go test -race -count=1 -json -run "$$selector" ./...; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -Ec '"Action":"pass","Package":"[^"]+","Test":"Test[^/"]+"' "$$log" || true); skipped=$$(grep -Ec '"Action":"skip","Package":"[^"]+","Test":"Test[^"]+"' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq "$$expected" ] || { echo "trusted-supervisor pass count $$passed, want $$expected"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo "trusted-supervisor skip count $$skipped, want 0"; exit 1; }

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
	go test -race -coverprofile=coverage.out -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) ./...
	@awk 'NR > 1 && $$(NF - 1) > 0 && $$NF == 0 { print "uncovered statement block: " $$0; missed = 1 } END { if (missed) exit 1 }' coverage.out
	@go tool cover -func=coverage.out | awk 'BEGIN { found = 0 } /^total:/ { found = 1; if ($$3 != "100.0%") { printf "total coverage %s, want 100.0%%\n", $$3; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

# Private container-only shard verbs. Public release targets above always cover
# ./... and enforce their complete gates; only the privileged coordinator calls
# these explicitly partial targets after validating a six-module package map.
_privileged-shard-guard:
	@test '$(ACP_GO_PRIVILEGED_INTERNAL)' = 1
	@case '$(ACP_GO_PRIVILEGED_SHARD)' in root|provider) ;; *) echo 'invalid privileged shard $(ACP_GO_PRIVILEGED_SHARD)' >&2; exit 1 ;; esac
	@test -n '$(ACP_GO_PRIVILEGED_MODULE)'
	@test -n '$(ACP_GO_PRIVILEGED_PACKAGES)'
	@test -n '$(ACP_GO_PRIVILEGED_REQUIRED_CLASSES)'
	@test "$$(go list -mod=readonly -m)" = '$(ACP_GO_PRIVILEGED_MODULE)'

_privileged-shard-coverage: _privileged-shard-guard
	@case '$(ACP_GO_PRIVILEGED_COVERAGE_OUT)' in .tmp/coverage-*.out) ;; *) echo 'invalid privileged coverage output $(ACP_GO_PRIVILEGED_COVERAGE_OUT)' >&2; exit 1 ;; esac
	go test -race -coverprofile='$(ACP_GO_PRIVILEGED_COVERAGE_OUT)' -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) $(ACP_GO_PRIVILEGED_PACKAGES)

_privileged-shard-trusted-supervisor: _privileged-shard-guard
	@test "$$(uname -s)" = Linux
	@test "$$(id -u)" -eq 0
	@for directory in /var/lib/acp-go /var/lib/acp-go/agent-identities; do if [ ! -e "$$directory" ] && [ ! -L "$$directory" ]; then install -d -o root -g root -m 0700 "$$directory"; fi; [ "$$(stat -c '%F %u %g %a' -- "$$directory")" = 'directory 0 0 700' ] || { echo "unsafe trusted-supervisor authority directory $$directory" >&2; exit 1; }; done
	@selector='^(Test.*(ProcessIsolationActual|TrustedSupervisor|SupervisorGuardianSIGKILL|SupervisorLivenessSIGKILL|GeneratedNative|NativeOwnedDirectory|BorrowedIdentityAdoption|BorrowedDomainAdoption|BorrowedDisposition|AgentIdentityLock|AgentStandalone|AuthorityDomain|IdentityDisposition|PersistentProof|SupervisorConfigIsSealed|CommandCreatorThread|ProviderCreator|SecurityLimits).*)$$'; listing=$$(mktemp); log=$$(mktemp); rc=$$(mktemp); \
	go test -list "$$selector" $(ACP_GO_PRIVILEGED_PACKAGES) >"$$listing"; status=$$?; \
	[ "$$status" -eq 0 ] || { rm -f "$$listing" "$$log" "$$rc"; exit "$$status"; }; \
	required='$(ACP_GO_PRIVILEGED_REQUIRED_CLASSES)'; \
	for class in $$required; do grep -Eq "^Test.*$${class}" "$$listing" || { rm -f "$$listing" "$$log" "$$rc"; echo "trusted-supervisor selector discovered no $${class} tests in $(ACP_GO_PRIVILEGED_SHARD) shard"; exit 1; }; done; \
	expected=$$(grep -Ec '^Test' "$$listing" || true); rm -f "$$listing"; \
	[ "$$expected" -gt 0 ] || { rm -f "$$log" "$$rc"; echo 'trusted-supervisor shard selector discovered no tests'; exit 1; }; \
	{ go test -race -count=1 -json -run "$$selector" $(ACP_GO_PRIVILEGED_PACKAGES); echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -Ec '"Action":"pass","Package":"[^"]+","Test":"Test[^/"]+"' "$$log" || true); skipped=$$(grep -Ec '"Action":"skip","Package":"[^"]+","Test":"Test[^"]+"' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq "$$expected" ] || { echo "trusted-supervisor pass count $$passed, want $$expected"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo "trusted-supervisor skip count $$skipped, want 0"; exit 1; }

_privileged-coverage-gate:
	@awk 'NR > 1 && $$(NF - 1) > 0 && $$NF == 0 { print "uncovered statement block: " $$0; missed = 1 } END { if (missed) exit 1 }' coverage.out
	@go tool cover -func=coverage.out | awk 'BEGIN { found = 0 } /^total:/ { found = 1; if ($$3 != "100.0%") { printf "total coverage %s, want 100.0%%\n", $$3; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-integration-smoke: compile and run integration tests that can skip without live auth
test-integration-smoke:
	ACP_GO_HERMES_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=240s -v ./integration/... ./internal/hermes

## test-integration-live: run live Hermes CLI integration tests
test-integration-live:
	ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_RUN_LIVE_TOKENS=1 go test -race -count=1 -tags=integration -timeout=300s -v ./integration/... ./internal/hermes

## test-integration-attended: run provider-auth flows a human must approve in real time
# go test exits 0 when -run selects nothing, so the exit status alone reports a
# successful login for a tier that ran none. The run is piped through tee rather
# than redirected so the relayed login URL still reaches the watching operator
# live, and the guard requires a top-level pass line, which no empty selection
# and no skip can produce.
test-integration-attended:
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_RUN_ATTENDED=1 go test -race -count=1 -tags=integration -timeout=1200s -v -run TestAttended ./integration/... 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); ran=$$(grep -c '^--- PASS: TestAttended' "$$log"); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$ran" -gt 0 ] || { echo 'no attended provider-auth login ran: -run TestAttended selected nothing'; exit 1; }

## test-integration-keystore: run Linux state-boundary and browser probes
test-integration-keystore:
	ACP_GO_HERMES_RUN_INTEGRATION=1 ACP_GO_HERMES_RUN_KEYSTORE=1 go test -race -count=1 -tags=integration -timeout=900s -v -run TestKeystore ./...

## test-integration-native-browser: require one pinned native Linux provider-auth no-launch proof
test-integration-native-browser:
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ACP_GO_HERMES_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=1200s -v -run '^TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher$$' ./integration/... 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -c '^--- PASS: TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher ' "$$log" || true); skipped=$$(grep -Ec '^[[:space:]]*--- SKIP: TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher(/| )' "$$log" || true); empty=$$(grep -c 'no tests to run' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq 1 ] || { echo "native browser pass count $$passed, want exactly 1"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'required native browser canary skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'required native browser selector ran no tests'; exit 1; }

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
	@for flag in -path -home -scratch-dir -provider-auth-root -shared-hermes-home -model -debug -version; do rg -q -- "$$flag" docs/reference/cli.mdx cmd/acp-go-hermes/main.go || { echo "missing CLI flag in docs/code: $$flag"; exit 1; }; done
	@pattern=$$(printf '%b' '$(REMOVED_PUBLIC_TERMS)'); ! rg -n -- "$$pattern" README.md doc.go docs.json docs examples cmd/acp-go-hermes/*.go AGENTS.md
	@! rg -n -- 'options\.(mode|permission)\b|options"?\s*:.*\b(mode|permission)\b' docs README.md doc.go || { echo "removed _meta.hermes.options.mode/permission field reappeared in docs"; exit 1; }
	@rg -q 'SupervisorGuardianSIGKILL' Makefile
	@rg -q 'SupervisorLivenessSIGKILL' Makefile
	@rg -q 'BorrowedIdentityAdoption' Makefile
	@rg -q 'BorrowedDomainAdoption' Makefile
	@rg -q 'validateInheritedAgentIdentityFlock' internal/hermes/agent_identity_lock_linux.go
	@rg -q '/proc/self/fdinfo/' docs/operations/security.mdx

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
