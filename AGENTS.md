# AGENTS.md

Shared instructions for automated coding agents working in this repository.

## Purpose

This project is a Go implementation of an ACP agent for Hermes. It runs one
isolated `hermes serve` process per ACP session and builds directly on
`github.com/coder/acp-go-sdk`. Hermes owns model execution and native state;
this package owns ACP dispatch, process launch, stream hygiene, per-session
XDG isolation, options, storage, and embeddability.

## Project Map

Organized by domain. Public surface lives in the root package; implementation
details live in `internal/`.

- **Entrypoint** (`cmd/acp-go-hermes`): process entrypoint, ACP stdio mode,
  OpenTelemetry setup, and signal handling.
- **ACP agent surface** (root package, e.g. `agent.go`,
  `agent_connection.go`, `agent_session.go`, `options.go`, `ids.go`,
  `request_builders.go`, `validation.go`): ACP method handlers, request
  dispatch, agent options, request builders, and ID helpers.
- **Session orchestration** (`session.go`, `session_prompt.go`,
  `session_config.go`, `session_meta.go`, `raw_events.go`): Hermes turn
  lifecycle, prompts, cancellation, permissions, elicitation, usage updates,
  native message replay, config options, and raw gateway event handling.
- **Session storage** (`session_store.go`, `session_state_store.go`): the
  host-facing `SessionStore` API and the `hermes-state-db-v1` durable store.
- **Hermes gateway client** (`hermes_client.go`, `hermes_process_*.go`,
  `internal/hermes`): loopback WebSocket JSON-RPC client, process launch, and
  platform-specific process control for the `hermes serve` subprocess.
- **Observability** (`internal/observer`): OpenTelemetry span, metric, and
  trace-context propagation helpers.
- **Live tests** (`integration`): integration tests that launch the real local
  `hermes` CLI or a deterministic fake harness.
- **Docs** (`docs/`, `docs.json`): Mintlify guide. Update alongside public API,
  CLI flag, ACP method, or `_meta` field changes.

## Commands

```sh
go build ./...
go test ./...
go test -race ./...
golangci-lint run ./...
```

Lint details live in `.golangci.yml`.

The Makefile wraps the main development checks:

```sh
make test
make lint
make audit
```

Run live integration tests only when a local Hermes CLI is installed and
authenticated:

```sh
ACP_GO_HERMES_RUN_INTEGRATION=1 go test -race -tags=integration -timeout=240s -v ./integration/... ./internal/hermes
```

`make test-integration-smoke` runs the tier that can skip without live auth.
`make test-integration-live` adds `ACP_GO_HERMES_RUN_LIVE_TOKENS=1` and may
spend model tokens. Use `make test-integration-cover` for compiled
`acp-go-hermes` coverage through `GOCOVERDIR`. Set `ACP_GO_HERMES_MODEL` to
override the model used by live tests. Live tests always launch Hermes with an
isolated temp `HERMES_HOME` and never touch the user's real Hermes home.

## Coding Rules

- Follow standard Go idioms: `ctx` first, no `ctx` in structs, and `%w` for
  wrapped errors.
- Keep the public root package small; implementation details belong in
  `internal/` unless they are part of the public API.
- Prefer structured protocol types and JSON decoding over ad hoc string parsing.
- Preserve ACP method names, request/response shapes, and validation behavior.
- Keep gateway glue narrow, documented, and close to the ACP method it serves.
- Keep shared code next to the domain it serves; avoid generic catch-all
  packages such as `utils`, `helpers`, or `common`.
- Follow existing package patterns before introducing new abstractions.
- Keep stdout reserved for ACP JSON-RPC in the CLI; logs and diagnostics belong
  on stderr.
- Prefer new wrapper options before translating more native Hermes behavior.

## Ask Before

Unless explicitly requested, ask before:

- Changing the permission or elicitation flow shape.
- Adding new ACP extension methods or `_meta` fields.
- Changing the session-store contract or store format.
- Changing per-session home isolation behavior.

## Testing Rules

- Prefer table-driven tests for mapper and protocol cases.
- Run `go test ./...` for ordinary changes.
- Run `go test -race ./...` or `make test` for session, gateway, concurrency, or
  cancellation changes.
- Run `golangci-lint run ./...` before considering work complete.
- Live integration tests launch the actual `hermes` binary from `PATH`.
- Unit tests may use in-memory transports and the deterministic fake harness.
- Keep live prompts deterministic with exact sentinel replies, and assert the
  ACP stop reason plus streamed updates where practical.
- Maintain 100% statement coverage; `make coverage-check` is part of `make
  audit`.

## Security And Boundaries

- **IMPORTANT**: Do not silently bypass permission prompts. Permission flow is
  load-bearing for user trust in this agent.
- **IMPORTANT**: Do not manage the user's real Hermes authentication state. Each
  session runs against an isolated temp `HERMES_HOME`.
- Do not log auth material, user secrets, prompts, tool input, tool output, or
  raw Hermes gateway event bodies by default.
- Keep permission rules session-scoped. Copy them only through intentional
  session fork behavior.
- Reject unsupported ACP extension methods with explicit protocol errors unless
  this agent implements a namespaced extension.
- Avoid broad filesystem or network behavior in tests unless the test is
  explicitly about that boundary.
