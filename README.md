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
providers. `WithHome` and `WithProviderAuthDirectHome` are unsupported and
reject a non-empty value at session start. Use `WithScratchDir` for ephemeral
state and `WithProviderAuthHome` for durable provider credentials.
`WithProviderAuthRoot` names the durable directory that holds the values-free
provider-auth ledger. Provider auth is advertised only when both provider-auth
directories are configured.

## What It Provides

- ACP session lifecycle: create, prompt, cancel, close, list, load, resume,
  delete, and fork.
- Provider OAuth brokered through six session-scoped `_hermes/auth/*`
  extension methods over the `hermes serve` REST auth API. Native Hermes owns
  credential bytes in a shared durable `HERMES_AUTH_HOME`; the adapter keeps
  only values-free connection lineage.
- One isolated `hermes serve` runtime per session, each with a dedicated,
  freshly generated `HERMES_HOME`. Linux uses authoritative OS containment.
  Windows native launch fails closed because its process API cannot apply the
  mandatory Unix UID/GID identity boundary with empty supplementary groups;
  cross-compilation proves only that this refusal path builds, not runtime
  support. Darwin is disabled unless its explicitly risky best-effort
  process-group mode is selected.
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

Live integration tests require a local authenticated `hermes` CLI. The live
target sets `ACP_GO_HERMES_RUN_INTEGRATION=1` and
`ACP_GO_HERMES_RUN_LIVE_TOKENS=1` and may spend model tokens. Set
`ACP_GO_HERMES_MODEL` to override the model used by live tests. Live tests
always launch Hermes with an isolated temp `HERMES_HOME`. Credentialed tests
pass a durable `HERMES_AUTH_HOME` explicitly and never copy credential files
into a session home.

## License

[GNU General Public License v3.0](LICENSE).
