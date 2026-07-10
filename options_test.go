package hermesacp

import (
	"log/slog"
	"testing"
	"time"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestApplyOptions(t *testing.T) {
	store := NewInMemorySessionStore()
	seed := map[string]string{"config.yaml": "model: {}\n"}
	opts := applyOptions([]Option{
		WithLogger(slog.New(slog.DiscardHandler)),
		WithAgentName("name"),
		WithAgentTitle("title"),
		WithAgentVersion("version"),
		WithExecutablePath("hermes"),
		WithHome("/tmp/home"),
		WithDefaultModel("openai/gpt"),
		WithEnv(map[string]string{"A": "1"}),
		WithTracerProvider(tracenoop.NewTracerProvider()),
		WithMeterProvider(metricnoop.NewMeterProvider()),
		WithTextMapPropagator(propagation.TraceContext{}),
		WithSessionStore(store),
		WithSessionStoreLoadTimeout(time.Second),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1, MaxConcurrentClientCalls: 3}),
		WithSeedFiles(seed),
	})
	if opts.AgentName != "name" || opts.AgentTitle != "title" || opts.ExecutablePath != "hermes" ||
		opts.Env["A"] != "1" || opts.SessionStore != store {
		t.Fatalf("options = %#v", opts)
	}
	if opts.SeedFiles["config.yaml"] != "model: {}\n" {
		t.Fatalf("seed files = %#v", opts.SeedFiles)
	}
	seed["config.yaml"] = "mutated"
	seed["extra"] = "late"
	if opts.SeedFiles["config.yaml"] != "model: {}\n" || len(opts.SeedFiles) != 1 {
		t.Fatalf("WithSeedFiles did not clone source map: %#v", opts.SeedFiles)
	}
}
