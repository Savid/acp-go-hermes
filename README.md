# acp-go-hermes

`acp-go-hermes` exposes the [Hermes](https://github.com/NousResearch/hermes-agent) as an [Agent Client Protocol](https://agentclientprotocol.com) agent.
It launches one `hermes serve --isolated` process per ACP session, connects to its
authenticated loopback WebSocket, and streams ACP session updates back to the client.
`--isolated` starts each session's backend beside any `hermes serve` the user
already runs instead of attaching to it.

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

`hermes` is found on `PATH` or named with `-path`.

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

### Usage

Each launch writes the adapter's Hermes plugin as `plugins/acp-go-hermes/` into
the home Hermes uses: `HERMES_HOME`, else `~/.hermes`, or the profile a sticky
`active_profile` file selects. Only when such a file names a profile other than
`default` does the launch ask `hermes config path` for the home. When that
home's `config.yaml` neither enables nor disables the plugin, the gateway
enables it through Hermes's plugin manager, which records it in
`plugins.enabled`; listing it in `plugins.disabled` turns it off. The first
enable in a home also has Hermes's plugin manager record its plugin selection
and nudge a running `hermes gateway` to rediscover its plugins. Every Hermes
process using the home then loads the plugin, but it registers only where
`ACP_GO_HERMES_CALL_REPORTS=1`, which the adapter sets for the `hermes serve` it
starts, and reports only from a process whose gateway server is running. A
launch that cannot resolve the home or write the plugin logs a warning and runs
without call reports.

The plugin wraps Hermes's `llm_execution` middleware. For each Chat Completions
response of the session's own conversation, it sends the gateway's response id
and the usage members the gateway returned over the session's gateway
connection, ahead of the native events that follow from that response. Each
such response yields one `usage_update`. `used` is the context Hermes counts
after it, and `size` is the context window of the model that served it. Its
`_meta["acp-go.dev/callUsage"]` carries the `acp-go-core` `wire.CallUsage`
breakdown, with only the members the gateway sent. Each figure is read from
the members Hermes's usage normalization reads, the first non-zero one, else a
reported zero:

- prompt: `prompt_tokens`, then `input_tokens`.
- `cachedReadTokens`: `prompt_tokens_details.cached_tokens`, then
  `cache_read_input_tokens`, `prompt_cache_hit_tokens`, `cached_tokens`.
- `cachedWriteTokens`: `prompt_tokens_details.cache_write_tokens`, then
  `prompt_tokens_details.cache_creation_input_tokens`,
  `cache_creation_input_tokens`, `cache_write_tokens`.
- `outputTokens`: `completion_tokens`, then `output_tokens`, reasoning included.
- `inputTokens` is the prompt less the cache reads, and less the cache writes
  where sent, when the gateway sent both the prompt and the cache reads and the
  difference is not negative.
- `used` is the prompt, or the cache tokens when they exceed it, as Hermes
  counts the uncached input plus both cache buckets.
- `responseId` is the gateway's id. It is absent when the gateway sent none,
  so Hermes's own `stream-…` and partial-stream ids are never reported.

Every attempt of a retried request is its own call. A response that arrives
before a usage reading has stated its model's context window waits for the
first reading that does; one whose window no reading of its cycle states
reports nothing. An empty report, or a response after a cancel, reports
nothing.

Covered calls are the session conversation's main model calls on the Chat
Completions wire. Auxiliary calls (title generation, compression summaries,
vision and other side tasks), background review forks and delegated subagents
are not reported. Neither are responses on other wires or ones whose usage a
native adapter assembled. A `session.usage` or `message.complete` reading
whose `prompt` counter moved by exactly the prompts of a run of consecutive
reported calls reports nothing more; any other reading that moved the counter
reports its context without a breakdown, so a response no call report covers
still reports. The prompt result's `usage` sums Hermes's counters for the turn.

Message and thought chunks carry no `messageId`: the id reaches the plugin only
once the response has finished streaming.

### Session store

`WithSessionStore` mirrors the conversation Hermes keeps in its home's
`state.db`, read and restored through `hermes serve`'s
`/api/sessions/{id}/export` and `/api/sessions/import`. The adapter imports only
a conversation the home does not hold, so each import carries its own
`request_id` and `expected_revision` 0, the identity Hermes requires of every
session mutation. Each exported message is one row under the main subpath, and the adapter's session record is under
`config`, format `hermes-session-json-v2`. Commit, restore, and recovery follow
[Sessions and the Session Store](https://github.com/savid/acp-go-core/blob/master/docs/03-sessions-and-store.md).
The record holds the session id, the native session id, `cwd`, additional
directories, the session environment and ordered path directories, the model,
the effort, the title, and the export's session object without its messages.

A commit continues after the messages it committed; a message Hermes changed in
place, as an in-place compaction or a model switch does, republishes the
conversation, and archive flags do not count as a conflict on restore.
Hermes's import refuses a conversation past 10,000 messages or 5 MiB, so the
adapter warns once a session's conversation passes either. Rotating compaction
(`compression.in_place: false`) changes the durable id, which poisons the
session.

### Account usage

`_hermes/accountUsage` with `{"providerId": "<id>"}` reads one provider's
allowance through the gateways `config.yaml` routes it to: `anthropic`,
`openai-codex`, `opencode-go`, or `openrouter`. Hermes exposes no provider
credentials natively, so a provider no configured gateway reports answers
`{"available": false, "reason": "not_authenticated"}`. Initialize advertises
the read under `_meta.hermes.accountUsage` as
`{"method": "_hermes/accountUsage", "scope": "agent", "providers": [...]}`.

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

## Context compaction

Gateway progress has no reliable terminal outcome, so the adapter publishes
outcomes only. The `compressions` counter of each `message.complete` usage
counts the compactions Hermes committed; every increase since the runtime's
previous `message.complete` publishes one `completed` notification, without a
trigger or context counts. A mid-turn `session.usage` can count an attempt Hermes
later rolls back, so it publishes nothing. Usage accounting remains independent.

## Memory

Each time Hermes starts an agent's session, the plugin unfreezes, collects, and
freezes the `hermes serve` process's garbage collector, so the collection Hermes
runs after each turn never traverses the agent's long-lived objects and a
rebuilt agent leaves no frozen graph behind.

Hermes runs auxiliary model work and background review forks after turns. The
adapter leaves them to the home's `config.yaml`; a host that does not need them
can turn them off there, for example through a seed file:

| Key | Default | Off |
|---|---|---|
| `auxiliary.background_review.enabled` | `true` | `false` |
| `auxiliary.title_generation.enabled` | `true` | `false` |
| `memory.memory_enabled` | `true` | `false` |
| `memory.user_profile_enabled` | `true` | `false` |
| `memory.nudge_interval` | `10` | `0` |
| `skills.creation_nudge_interval` | `10` | `0` |
| `curator.enabled` | `true` | `false` |
