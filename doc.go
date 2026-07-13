// Package hermesacp exposes the local Hermes CLI as an Agent Client Protocol
// agent.
//
// Most hosts run the agent over a pair of JSON-RPC streams using [Serve].
// Serve launches one isolated, authenticated loopback `hermes serve` process
// per ACP session, maps ACP requests into native gateway WebSocket JSON-RPC
// calls, streams gateway events back to the client as ACP session updates,
// and tears the process down when the session closes. Hosts must complete ACP
// initialization before issuing session or other agent methods.
//
// Hosts should use [Serve] for the JSON-RPC transport; hosts that embed the
// agent directly construct one with [NewAgent] and the same [Option] values.
// Hermes authentication and provider credentials remain owned by the local
// Hermes installation. Each session runs under its own `HERMES_HOME`
// materialized beneath the scratch parent from [WithScratchDir] (the system
// temp directory by default), so native state never leaks between sessions.
// [WithHome] is unsupported: Hermes has no native config or auth root the
// adapter may target, so a non-empty Home is rejected when a session is
// established.
//
// Hosts that need durable remote resume can provide [WithSessionStore]. A
// session store receives `hermes-state-db-v1` snapshots keyed by the
// ACP-visible session ID and subpath, can back session/list, and can hydrate
// a snapshot into a fresh per-session Hermes home for session/load or
// session/resume when the local native state is absent.
//
// Hosts can call [CallForkSession] for the Hermes fork extension method
// _hermes/session/fork. Raw Hermes gateway events are emitted as
// [RawEventMethod] notifications only when a session request opts in with
// [WithSessionRawEvents].
//
// Hosts that need adapter telemetry can provide OpenTelemetry providers with
// [WithTracerProvider] and [WithMeterProvider]. The package never configures
// global OpenTelemetry providers; the acp-go-hermes binary handles env-based
// exporter setup for command-line use. Caller-supplied providers remain owned
// by the caller, including ForceFlush and Shutdown.
package hermesacp
