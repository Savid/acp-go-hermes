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
