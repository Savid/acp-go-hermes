# acp-go-hermes

This repository provides a Go ACP agent around `hermes serve`. Keep the
adapter focused: Hermes owns model behavior; this package owns ACP dispatch,
process launch, stream hygiene, per-session XDG isolation, options, storage,
and embeddability.

## Development

- Run `make test` for local verification.
- Run `make audit` before larger changes when time allows.
- Keep stdout reserved for ACP JSON-RPC in the CLI. Logs and diagnostics belong
  on stderr.
- Prefer new wrapper options before translating more native Hermes behavior.
