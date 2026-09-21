// Package hermesacp exposes Hermes as an Agent Client Protocol agent.
//
// [Serve] starts one authenticated loopback Hermes gateway per ACP session.
// The harness inherits the adapter environment and keeps its native state in
// HERMES_HOME, so the conversation can also continue through Hermes's CLI.
//
// [WithSessionStore] supplies durable per-conversation snapshots. Load restores
// and replays history; resume restores without replay. The adapter preserves
// native state when sessions close.
//
// Hosts supply telemetry providers through [WithTracerProvider] and
// [WithMeterProvider]; the package never configures global providers.
package hermesacp
