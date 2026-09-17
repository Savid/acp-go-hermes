# AGENTS.md

## Purpose

This Go module exposes the local `hermes` CLI as an Agent Client Protocol agent.
Each ACP session drives one `hermes serve` process that inherits the
adapter's environment and keeps its session in hermes's own home, so a session
started over ACP can be continued natively with `hermes chat --cli --resume NATIVE_SESSION_ID` afterwards.

## Project Map

- `cmd/acp-go-hermes`: stdio entrypoint, OpenTelemetry setup, signals, flags.
- Root `agent*.go`, `options.go`, `request_builders.go`: the public ACP
  surface and option validation; the shared ACP transport orders publication.
- Root `session*.go`: one session's process, event pump,
  prompt turns, permissions and elicitation, lifecycle stream, store mirror,
  replay, config options, and image input.
- `internal/hermes`: WebSocket JSON-RPC client, HTTP session import/export,
  and launch arguments.
- `integration`: gated tests against the installed hermes.

## Commands

```sh
make build
make test
make lint
make audit
make test-integration-smoke
make test-integration-live
```

`make test` runs with race detection and shuffled order. `make audit` is the
full local gate. Integration targets need an installed `hermes`; the live target
spends model tokens and requires explicit operator intent.

## Coding Rules

- Follow Go idioms: `ctx` first, `%w` for wrapped errors, small interfaces at
  the consumer. Keep native protocol details in `internal/hermes` and ACP glue
  beside its handler.
- Shared family behavior comes from `github.com/savid/acp-go-core`; never copy
  it here.
- The adapter does no isolation: hermes inherits the process environment, the
  agent overlay, then the session env, then the adapter-owned home and
  gateway token. Only `ACP_GO_HERMES_INTERNAL_*` markers are dropped.
- Native state is never deleted. The session store is the durability
  boundary; Hermes's own database is the native copy.
- Unit tests never require an installed hermes: the test binary doubles as a
  scripted fake hermes. Keep the fake's protocol in step with `internal/hermes`.
- A comment states what the code does or why a constraint exists.

## Verification

Run `go test ./...` for ordinary changes and `make lint` for Go edits. Run
`make audit` once changes settle. Run the integration smoke target after
changing anything hermes-facing.

## Boundaries

- The permission bridge is the session permission system. Never bypass its
  dialog or fail open on a denied or cancelled answer.
- Do not log prompts, tool input or output, or raw native event bodies by
  default.
- Reject every ACP extension method; the only extension surface is the
  outbound raw-event notification.
