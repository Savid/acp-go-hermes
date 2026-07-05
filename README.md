# acp-go-hermes

`acp-go-hermes` is a Go ACP agent for Hermes. It speaks ACP on the
provided stdin/stdout streams and runs one isolated `hermes serve` process for
each ACP session.

Hermes owns model execution and native state. This package owns ACP dispatch,
process launch, per-session HERMES_HOME isolation, WebSocket JSON-RPC event mapping, permission
requests, config options, and `hermes-state-db-v1` session storage.

## Install

```sh
go install github.com/savid/acp-go-hermes/cmd/acp-go-hermes@latest
```

Local run:

```sh
go run ./cmd/acp-go-hermes -path "$(command -v hermes)"
```

The command reserves stdout for ACP JSON-RPC. Diagnostics go to stderr.

## Embedded Go

```go
err := hermesacp.Serve(ctx, input, output,
	hermesacp.WithExecutablePath("hermes"),
	hermesacp.WithHome("/tmp/hermes-acp-home"),
	hermesacp.WithDefaultModel("hermes/big-pickle"),
)
```

## Public Surface

The package exports `NewAgent`, `Serve`, common process options, Hermes
options, request builders, `ForkSessionMethod`, `RawEventMethod`, and the
`SessionStore` API. Session durability uses:

```go
const SessionStoreFormat = "hermes-state-db-v1"
```

Forking is available only through `_hermes/session/fork`.

## Examples

```sh
go run ./examples/minimal-client "Reply with hello from ACP"
go run ./examples/interactive-chat
go run ./examples/resume-from-file
```

## Development

```sh
make test
make test-integration-smoke
make audit
```

Live prompt, permission, and elicitation checks are guarded by
`ACP_GO_HERMES_RUN_LIVE_TOKENS=1`.
