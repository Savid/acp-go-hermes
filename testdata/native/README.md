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
