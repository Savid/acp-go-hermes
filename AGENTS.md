# AGENTS.md

Shared instructions for automated coding agents working in this repository.

## Purpose

This project is a Go implementation of an ACP agent for Hermes. It runs one
isolated `hermes serve` process per ACP session and builds directly on
`github.com/coder/acp-go-sdk`. Hermes owns model execution and native state;
this package owns ACP dispatch, process launch, stream hygiene, per-session
XDG isolation, options, storage, and embeddability. `WithSharedHermesHome`
explicitly shares a durable native home in ordinary mode while retaining
independent session processes. `WithHome` remains unsupported at session start.

## Project Map

Organized by domain. Public surface lives in the root package; implementation
details live in `internal/`.

- **Entrypoint** (`cmd/acp-go-hermes`): process entrypoint, ACP stdio mode,
  OpenTelemetry setup, and signal handling.
- **ACP agent surface** (root package, e.g. `agent.go`,
  `agent_connection.go`, `agent_session.go`, `options.go`, `ids.go`,
  `request_builders.go`, `session_validation.go`): ACP method handlers, request
  dispatch, agent options, request builders, and ID helpers.
- **Session orchestration** (`session.go`, `session_prompt.go`,
  `session_config.go`, `session_meta.go`, `raw_events.go`): Hermes turn
  lifecycle, prompts, cancellation, permissions, elicitation, usage updates,
  native message replay, config options, and raw gateway event handling.
- **Session storage** (`session_store.go`, `session_state_store.go`): the
  host-facing `SessionStore` API and the `hermes-state-db-v1` durable store.
- **Provider auth** (`auth*.go`): native OAuth coordination and the values-free
  ledger, enabled only with a shared native home and provider-auth root.
- **Lifecycle** (`session_lifecycle_stream.go`, `session_settlement.go`,
  `internal/lifecycle`): ordered lifecycle updates, settlement, and fixture
  reduction. Preserve canonical `testdata/lifecycle` bytes.
- **Hermes gateway client** (`internal/hermes/client.go`,
  `internal/hermes/process*.go`): loopback WebSocket JSON-RPC client, process
  launch, and platform-specific process control for the `hermes serve`
  subprocess.
- **Observability** (`internal/observer`): OpenTelemetry span, metric, and
  trace-context propagation helpers.
- **Integration tests** (`integration`): integration tests that launch the real local
  `hermes` CLI or a deterministic fake harness.
- **Docs** (`docs/`, `docs.json`): Mintlify guide. Update alongside public API,
  CLI flag, ACP method, or `_meta` field changes.

## Commands

- `go build ./...`: compile all packages.
- `make test`: deterministic unit suite with race detection and shuffled order.
- `make lint`: pinned golangci-lint; rules live in `.golangci.yml`.
- `make coverage-check`: full race/shuffle suite with coverage reporting.
- `make audit`: complete local gate, including module tidy. Select focused
  checks during edits and run the full gate when the combined change is ready.

Native tests require explicit task authorization. Use
`make test-integration-smoke` for native smoke and
`make test-integration-live` for model turns that may spend tokens. The
targets set execution gates; those gates do not authorize native or account
activity. Use `make test-integration-cover` for compiled
`acp-go-hermes` coverage through `GOCOVERDIR`. Set `ACP_GO_HERMES_MODEL` to the
provider-qualified `provider/model` live tests should route through; the native
config those tiers seed takes its provider and model from that same value. Set
`ACP_GO_HERMES_LIVE_KEY_ENV` to the environment variable holding that provider's
key (default `OPENROUTER_API_KEY`); the tier forwards that one variable as
explicit launch environment and never writes a key into a seeded file. Live
tests use disposable temp homes and never touch the user's real Hermes home; the
explicit shared-home lane may share one temp residence across processes.

`make test-integration-attended` sets `ACP_GO_HERMES_RUN_ATTENDED=1` and runs the
provider-auth flows a human must approve at the provider.
`make test-integration-keystore` sets `ACP_GO_HERMES_RUN_KEYSTORE=1` and runs the
Linux state-boundary and browser-launcher probes; it fails rather than skips
when no container runtime is available.
`make test-integration-native-browser` runs the separate native browser probe.
None of these native targets joins `make audit`.

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
- Preserve the supported public surface and native programmatic semantics.
  Add options only when the task requires the new behavior.
- Honor authorization already supplied. Ask before expanding the task to change
  permission/elicitation flow, ACP methods or metadata, the session-store
  contract/format, or home isolation when that change is not already authorized.

## Testing Rules

- Prefer table-driven tests for mapper and protocol cases.
- Run `go test ./...` for ordinary changes.
- Run `go test -race ./...` or `make test` for session, gateway, concurrency, or
  cancellation changes.
- Run `make lint` before considering code changes complete.
- Native behavior claims require the actual `hermes` CLI. In-memory transports
  and deterministic fake harnesses prove wrapper behavior, not native fidelity.
- Protect observable behavior and concrete failure boundaries. Synchronize
  concurrent tests with explicit barriers; use deadlines to bound failure, not
  sleeps to guess ordering.
- Keep live prompts deterministic with exact sentinel replies, and assert the
  ACP stop reason plus streamed updates where practical.
- Complete the required behavioral/race suite and review reported coverage. Do
  not add production seams or artificial tests solely to raise a percentage.
- Preserve canonical `testdata/lifecycle` fixture bytes.

## Security And Boundaries

- **IMPORTANT**: Do not silently bypass permission prompts. Permission flow is
  load-bearing for user trust in this agent.
- Ordinary sessions use isolated homes unless `WithSharedHermesHome` explicitly
  selects a durable production home. Preserve the configured provider-auth
  surface; tests use disposable homes and never manage real user credentials.
- Ordinary execution inherits only the allowlist in
  `internal/hermes/process_ordinary.go`. Never widen that list with a
  credential-bearing name; a key reaches Hermes through `WithEnv` or
  session `env` alone.
- Do not log auth material, user secrets, prompts, tool input, tool output, or
  raw Hermes gateway event bodies by default.
- Reject `ACP_GO_HERMES_INTERNAL_*` and `ACP_GO_HERMES_PATH_DIR_*` in caller
  environments under every spelling; managed launch strips the internal prefix
  from its authority-provided base.
- Respect borrowed host authority and opaque native-tree ownership, including
  every failed prepare attempt. Keep managed handoff reads outside the reserved
  scratch domain; see [security](docs/operations/security.mdx).
- Keep permission rules session-scoped. Copy them only through intentional
  session fork behavior.
- Reject unsupported ACP extension methods with explicit protocol errors unless
  this agent implements a namespaced extension.
- Avoid broad filesystem or network behavior in tests unless the test is
  explicitly about that boundary.
