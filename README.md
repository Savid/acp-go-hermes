# acp-go-hermes

Go ACP agent that exposes the local Hermes CLI as an [Agent Client Protocol](https://agentclientprotocol.com/) agent.

[![Go Reference](https://pkg.go.dev/badge/github.com/savid/acp-go-hermes.svg)](https://pkg.go.dev/github.com/savid/acp-go-hermes)
[![CI](https://github.com/savid/acp-go-hermes/actions/workflows/go-test.yml/badge.svg)](https://github.com/savid/acp-go-hermes/actions/workflows/go-test.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)

Use it as either:

- a standalone ACP subprocess: `acp-go-hermes`
- an embedded Go adapter through `hermesacp.Serve`, or in-process through `hermesacp.NewAgent` with `hermesacp.WithClient`

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
providers. `WithHome` is unsupported and rejects a non-empty value at session
start. Use `WithScratchDir` for ephemeral
state. `WithSharedHermesHome` explicitly opts official Hermes into one durable
native home shared by this adapter's otherwise independent per-session
processes; that adapter claims the home root exclusively and a second adapter
asking for the same root is refused.
`WithProviderAuthRoot` names the durable directory that holds the values-free
provider-auth ledger. Provider auth is advertised only when that ledger root
and the shared Hermes home are configured; naming the ledger root without a
shared home fails `initialize` instead. Configuring both also canonicalizes the
shared home — the adapter resolves its symlinks and uses the resolved path as
`HERMES_HOME`.

A host that embeds the `Agent` and calls its ACP methods in-process builds it
with `NewAgent` and supplies the ACP client itself through `WithClient`; that
client is what the agent streams session updates, permission requests, and
elicitations to. `Serve` installs the connection it builds as that client and
refuses an option set carrying one.

An embedded host can pass `WithHostAuthority` to supply the complete native
environment, prepare and reclaim native trees, and launch every managed Hermes
process. A supplied authority is mandatory for that agent instance: errors do
not fall back to direct execution. Provider-auth extensions are not advertised
in this mode; ordinary ACP sessions remain available.

## What It Provides

- ACP session lifecycle: create, prompt, cancel, close, list, load, resume,
  delete, and fork.
- Provider OAuth brokered through seven session-scoped `_hermes/auth/*`
  extension methods over the `hermes serve` REST auth API. Native Hermes owns
  credential bytes in the explicit shared durable `HERMES_HOME`; the adapter
  keeps only values-free connection lineage. A login has one hard precondition:
  the authorize leg refuses before any native call unless the session's process
  runs behind the private browser-launcher shim, because Hermes accepts
  `--no-browser` and then ignores it. Hermes returns its authorization
  URL on this API path without executing a browser launcher; a required pinned
  Linux canary verifies that no-launch behavior through the production adapter.
- One `hermes serve` process per session. By default each has a freshly
  generated `HERMES_HOME` and runs as the adapter's operating-system account.
  Embedded hosts can supply `WithHostAuthority` to route the version probe and
  session server through a host-owned process and filesystem boundary. The
  adapter materializes each native tree before preparing it, then reclaims it
  before snapshot reads or removal. The explicit shared-home mode remains an
  ordinary standalone residence with separate processes, ports, tokens,
  browser shims, event streams, environment, and wrapper control roots.
- Gateway event mapping from the loopback Hermes WebSocket into ACP methods and
  notifications.
- Prompt streaming for messages, tool calls, diffs, usage, and session
  metadata.
- Static PNG, JPEG, GIF, and WebP prompt images through Hermes
  `image.attach_bytes`, including image-MIME embedded resource blobs, with the
  enforced byte and format bounds advertised at initialize under
  `_meta["acp-go.dev/mediaEnvelope"]`.
- Two inbound image transports: embedded base64, and — for a co-located host
  that sets `WithInputHandoffRoot` — digest-verified local files read under that
  read-only root, contained by the kernel and never named in the native request.
- Input-only image transport: Hermes image artifacts are not projected as
  typed ACP image output.
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

Live integration tests use disposable temporary Hermes homes. The official
shared-home proof plants a fake xAI OAuth fixture, makes no provider request,
and never reads or mutates the operator's Hermes home.

## License

[GNU General Public License v3.0](LICENSE).
