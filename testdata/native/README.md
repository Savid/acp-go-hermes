Captured from Hermes 0.21.2 (release 2026.9.11) on 2026-09-14.

An installed `hermes serve` ran with its built-in
`HERMES_ISO_CERTIFY_SYNTH_TURN=1` driver in a temporary home. A direct native
`prompt.submit` call, while no ACP prompt was active, produced these gateway
frames. The upstream synthetic driver replaces model execution; the gateway,
WebSocket transport, and event publisher are native. No model service was called.

The fixture retains the message frames in delivery order. The live session ID
is normalized to `fixture-session`; payloads and native sequence values are
unchanged. The test proves adapter ownership and settlement for those frames,
not provider behavior or spontaneous scheduling.

`usage-routes.json` holds the usage frames of three turns captured from Hermes
`git.9fc7f17` (2026.9.24) on 2026-10-01. Each ran under `hermes serve` in a
temporary home with one prompt asking for terminal tool calls. `openrouter`
used `qwen/qwen3.8-flash` on OpenRouter directly; `gateway` used the same model
and `gateway-anthropic` used `anthropic/claude-sonnet-5-5` at medium effort,
both through an OpenAI-compatible gateway in `chat_completions` mode. Each
`session.usage` tick and the closing `message.complete` keep the usage members
the capture recorded; the final text is replaced and other members are
dropped. The test proves per-response context and summed consumption for
those shapes, not provider behavior or tick timing.

`replayed` holds the closing frames of three one-response turns in one
session, captured from the same Hermes on 2026-10-01 against a local
OpenAI-compatible stub in `chat_completions` mode. The stub answered the
second turn with all-zero usage, as a gateway does when it replays a response
from its response cache, and the other turns with cached and uncached input.
Hermes counted the replayed call, left its token counters unchanged, and
dropped the context. The test proves such a turn reports nothing and the next
response reports again.

`response-turn.json` holds one turn captured from the same Hermes on
2026-10-02 under `hermes serve` in a temporary home, using
`qwen/qwen3.8-flash` on OpenRouter in `chat_completions` mode through a
logging reverse proxy. `live` keeps every gateway event from `message.start`
through `message.complete` in delivery order with its payload unchanged;
`export` keeps the `id` and `messages` of the session's HTTP export. The proxy
recorded the response's `gen-` id; no frame or exported message contains it,
and Hermes wrote it only to its agent log text. The test proves chunks carry
no `messageId` and usage updates no call usage report, live and replayed.
