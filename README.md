# acp-go-hermes

Go ACP agent for the local Hermes CLI. It runs one isolated `hermes serve`
process per ACP session, speaks
[Agent Client Protocol](https://agentclientprotocol.com/) over JSON-RPC
streams, and is built on
[`github.com/coder/acp-go-sdk`](https://github.com/coder/acp-go-sdk).

Use it as either:

- a standalone ACP subprocess: `acp-go-hermes`
- an embedded Go adapter through `hermesacp.Serve`

## Install

```sh
go install github.com/savid/acp-go-hermes/cmd/acp-go-hermes@latest
```

For local development:

```sh
go run ./cmd/acp-go-hermes -path "$(command -v hermes)"
```

The process speaks ACP over stdin/stdout and reserves stdout for ACP JSON-RPC.
Diagnostics go to stderr. In normal use, an editor or ACP host launches it as a
subprocess rather than a human-facing chat UI.

## Quickstart

Run a tiny local client against the agent:

```sh
go run ./examples/minimal-client "Reply with hello from ACP"
```

Or try the interactive example:

```sh
go run ./examples/interactive-chat
```

Load and resume a stored session from its transcript file:

```sh
go run ./examples/resume-from-file -session-id <id> < transcript.jsonl
```

## Embedded Go

```go
package main

import (
	"context"
	"log"
	"os"

	hermesacp "github.com/savid/acp-go-hermes"
)

func main() {
	err := hermesacp.Serve(context.Background(), os.Stdin, os.Stdout,
		hermesacp.WithExecutablePath("hermes"),
		hermesacp.WithHome("/tmp/hermes-acp-home"),
		hermesacp.WithDefaultModel("openai/gpt-5.5"),
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

See [Go API docs](docs/reference/go-api.mdx) for options such as the Hermes
executable path, isolated home root, default model, environment, session
storage, concurrency limits, and OpenTelemetry providers.

## What It Provides

- ACP session lifecycle: create, prompt, cancel, close, list, load, resume,
  delete, and fork.
- One isolated `hermes serve` subprocess per session with a dedicated
  per-session `HERMES_HOME`.
- Authenticated loopback WebSocket JSON-RPC event mapping into ACP methods and
  notifications.
- Prompt streaming for messages, tool calls, diffs, usage, and session
  metadata.
- Command, file, and generic permission prompts plus MCP elicitation bridging.
- MCP stdio and streamable HTTP server configuration through the session
  request builders.
- Native Hermes message replay for `session/load`, replay-free
  `session/resume`, and `session/delete` store tombstones.
- Forking through `_hermes/session/fork` and optional raw gateway events
  through `_hermes/rawEvent` after per-session opt-in.
- Durable mirroring through a host-provided `SessionStore`; the store format is
  `hermes-state-db-v1`.
- OpenTelemetry adapter telemetry through injected tracer, meter, and
  propagator providers without recording prompt or tool secrets by default.

The root package exports `NewAgent`, `Serve`, the process and session options,
the request builders, and the `SessionStore` API.

## Docs

- [Overview](docs/overview.mdx)
- [Run modes](docs/get-started/run-modes.mdx)
- [Go API](docs/reference/go-api.mdx)
- [ACP methods](docs/reference/acp-methods.mdx)
- [Observability](docs/operations/observability.mdx)

## Development

```sh
make audit
make test-integration-smoke
make test-integration-live
make test-integration-cover
```

Live integration tests require a local authenticated `hermes` CLI. The live
target sets `ACP_GO_HERMES_RUN_INTEGRATION=1` and
`ACP_GO_HERMES_RUN_LIVE_TOKENS=1` and may spend model tokens. Set
`ACP_GO_HERMES_MODEL` to override the model used by live tests. Live tests
always launch Hermes with an isolated temp `HERMES_HOME` and never touch the
user's real Hermes home.
