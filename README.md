# acp-go-hermes

`acp-go-hermes` exposes the [Hermes](https://github.com/NousResearch/hermes-agent) as an [Agent Client Protocol](https://agentclientprotocol.com) agent.
It launches one `hermes serve` process per ACP session, connects to its
authenticated loopback WebSocket, and streams ACP session updates back to the client.

hermes inherits the adapter's environment and keeps sessions in its own home. A
session started over ACP can be continued natively:

```sh
acp-go-hermes              # host runs a session in /work
cd /work && hermes chat --cli --resume NATIVE_SESSION_ID
```

New, load, and resume responses and session-list entries expose the current
native ID as `_meta.hermes.nativeSessionId`. Use it for native CLI continuation.
ACP requests continue to use the stable ACP `sessionId`. The store's configuration
record saves both IDs with the matching native history.

## Install

```sh
go install github.com/savid/acp-go-hermes/cmd/acp-go-hermes@latest
```

Verified against `hermes` 0.21.3, found on `PATH` or named with `-path`.

## Run

```sh
acp-go-hermes [-path hermes] [-home DIR] [-scratch-dir DIR] [-model provider/id] [-seed-file rel=host]... [-debug]
```

| Flag | Meaning |
|---|---|
| `-path` | hermes executable; a bare name is searched on `PATH` |
| `-home` | hermes config root, passed as `HERMES_HOME`; empty inherits hermes's own resolution |
| `-scratch-dir` | accepted and ignored; this adapter allocates no ephemeral state |
| `-model` | default model for new sessions as `provider/id` |
| `-seed-file` | `<relpath>=<hostpath>` written into hermes's config root before launch; repeatable |
| `-debug` | debug logs to stderr |
| `-version` | print the adapter version |

OpenTelemetry exporters are configured from the standard `OTEL_*` variables.

## Embed

```go
err := hermesacp.Serve(ctx, os.Stdin, os.Stdout,
    hermesacp.WithHome("/srv/hermes"),
    hermesacp.WithSessionStore(store),
)
```

Options: `WithExecutablePath`, `WithHome`, `WithScratchDir`,
`WithInputHandoffRoot`, `WithDefaultModel`, `WithConfiguredModels`, `WithEnv`,
`WithSeedFiles`, `WithSessionStore`, `WithConcurrencyLimits`, `WithImageLimits`,
`WithLogger`, `WithTracerProvider`, `WithMeterProvider`, `WithTextMapPropagator`,
`WithAgentName`, `WithAgentTitle`, `WithAgentVersion`.

### Session options

`_meta.hermes.options` on `session/new`, `session/load`, and `session/resume`, or
`WithSessionHermesOptions` from Go:

| Field | Meaning |
|---|---|
| `model` | `provider/id` for the session |
| `env` | environment overlay for the session's hermes process |
| `extraPathDirs` | absolute directories prepended to `PATH`, in order |
| `effort` | session reasoning effort: `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`, or `ultra` |

The session environment reaches Hermes and its direct subprocesses. The native
terminal starts a login shell, whose system and user startup files can reorder
`PATH`.

Unknown option fields and nonempty `mcpServers` are invalid parameters.
`outputSchema` is unsupported. Authentication uses Hermes's native configuration.
Permissions are native approvals; an unavailable or cancelled host answer denies
that request. Native clarify requests use ACP form elicitation. Sudo, secret,
and terminal-buffer requests receive an empty value; desktop, vault, and setup
bridges are unsupported.

`_meta.hermes.rawEvent.enabled` forwards native events on `_hermes/rawEvent`.
Optional lifecycle negotiation enables ordered lifecycle updates.

### Config options

`session/set_config_option` accepts `model` (`provider/id`) and `effort`.
The model menu contains Hermes's catalog and any `WithConfiguredModels` entries.

### Image input

Images are accepted as inline data or through `WithInputHandoffRoot` and passed
to Hermes's image attachment API. Put all text, resource links, and text
resources before the images; images alone and multiple images are accepted.
Forwarded text after the first image fails with
`{"error":"unsupported","field":"prompt"}` before any native image upload
or prompt dispatch. User-only text excluded from native input does not affect
ordering. An image blob's URI is provenance and is not sent as prompt text.
Image output is not advertised.

### Session store

`WithSessionStore` commits the native per-conversation JSON export under the main
subpath and the session configuration under `config`, format
`hermes-session-json-v1`. Native auth files are not part of the snapshot.
The default store is in memory; provide a durable store to restore across adapter
restarts.

`session/load` restores and replays history; `session/resume` restores without
replay. A missing conversation is imported through Hermes's native HTTP API.
Existing native history must contain the stored history as a prefix; divergent
or shorter native histories fail restore. Each completed prompt commits its
snapshot before returning. Close preserves Hermes's native state, and delete
removes the store entry without deleting the native conversation.

The native binding identifies the durable Hermes conversation. Transient gateway
IDs stay internal. Native compression that changes the durable ID poisons the session
instead of storing a different conversation under the original ID.

## Development

```sh
make test
make lint
make audit
make test-integration-smoke   # needs hermes installed, spends no tokens
make test-integration-live    # spends model tokens
```

Unit tests run the test binary as a scripted fake hermes and need no installed
hermes, credentials, or network.

## Account usage

`AccountUsageMethod` (`_hermes/accountUsage`) accepts `sessionId` and
`providerId` (`opencode-go`, `openrouter`, `openai-codex`, or `anthropic`).
It requires the native gateway's `session.provider_access` method. That method
resolves effective credentials, routes, and headers inside the addressed
session's profile without starting a model turn. The adapter holds the
foreground gate, verifies the official route, and revalidates the binding after
reading usage. Credentials never enter ACP output.

Shared `github.com/savid/acp-go-core/usage` readers return subscription windows,
reported monetary spending and balances, and request limits. OpenAI API keys
and ordinary Anthropic API keys do not establish a subscription allowance.
