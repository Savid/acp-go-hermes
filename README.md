# acp-go-hermes

Go ACP agent that exposes the local Hermes CLI as an [Agent Client Protocol](https://agentclientprotocol.com/) agent.

[![Go Reference](https://pkg.go.dev/badge/github.com/savid/acp-go-hermes.svg)](https://pkg.go.dev/github.com/savid/acp-go-hermes)
[![CI](https://github.com/savid/acp-go-hermes/actions/workflows/go-test.yml/badge.svg)](https://github.com/savid/acp-go-hermes/actions/workflows/go-test.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)

Use it as either:

- a standalone ACP subprocess: `acp-go-hermes`
- an embedded Go adapter through `hermesacp.Serve`

## Install

Library:

```sh
go get github.com/savid/acp-go-hermes
```

CLI:

```sh
go install github.com/savid/acp-go-hermes/cmd/acp-go-hermes@latest
```

For local development, run the command straight from a checkout:

```sh
go run ./cmd/acp-go-hermes -path "$(command -v hermes)"
```

The process speaks ACP over stdin/stdout and reserves stdout for ACP JSON-RPC;
diagnostics go to stderr. In normal use an editor or ACP host launches it as a
subprocess rather than a human-facing chat UI.

## Quickstart

The example programs run from a checkout of this repo, so clone it first:

```sh
git clone https://github.com/savid/acp-go-hermes && cd acp-go-hermes
```

Run a tiny local client that launches the agent, sends one prompt, and prints
the reply (the prompt argument is optional):

```sh
go run ./examples/minimal-client "Reply with hello from ACP"
```

Or drive the agent from an interactive client session:

```sh
go run ./examples/interactive-chat
```

Load and resume a stored session transcript:

```sh
go run ./examples/resume-from-file -file ./examples/resume-from-file/session.jsonl
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
		hermesacp.WithScratchDir("/tmp/hermes-acp-scratch"),
		hermesacp.WithDefaultModel("openai/gpt-5.5"),
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

See [Go API docs](docs/reference/go-api.mdx) for options such as the Hermes
executable path, the scratch directory for ephemeral per-session state, default
model, environment, session storage, concurrency limits, and OpenTelemetry
providers. Hermes has no native config or auth root, so `WithHome` is
unsupported: a non-empty `Home` is rejected as an unsupported option when a
session is established. Use `WithScratchDir` to control where ephemeral state is
materialized.

## What It Provides

- ACP session lifecycle: create, prompt, cancel, close, list, load, resume,
  delete, and fork.
- One isolated `hermes serve` subprocess per session, each with a dedicated
  `HERMES_HOME`.
- Gateway event mapping from the loopback Hermes WebSocket into ACP methods and
  notifications.
- Prompt streaming for messages, tool calls, diffs, usage, and session
  metadata.
- Native image forwarding through Hermes `image.attach_bytes`, including
  image-MIME embedded resource blobs.
- Command, file, and generic permission prompts, plus MCP elicitation bridging.
- MCP stdio and streamable HTTP server configuration through the session
  request builders.
- Native message replay on `session/load`, replay-free `session/resume`, and
  store tombstones on `session/delete`.
- Forking through `_hermes/session/fork`, and optional raw gateway events
  through `_hermes/rawEvent` after per-session opt-in.
- Durable mirroring through a host-provided `SessionStore` in the
  `hermes-state-db-v1` format with sequenced tar+zstd+base64 archive chunks.
- OpenTelemetry telemetry through injected tracer, meter, and propagator
  providers, recording no prompt or tool secrets by default.

## Docs

- [Overview](docs/overview.mdx)
- [Run modes](docs/get-started/run-modes.mdx)
- [Go API](docs/reference/go-api.mdx)
- [ACP methods](docs/reference/acp-methods.mdx)
- [Observability](docs/operations/observability.mdx)
- [Go package reference](https://pkg.go.dev/github.com/savid/acp-go-hermes)

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

## License

[GNU General Public License v3.0](LICENSE).
