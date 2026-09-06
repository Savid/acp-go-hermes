package hermesacp

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		WithHome(absTestPath("tmp", "home")),
		WithScratchDir(absTestPath("tmp", "scratch")),
		WithProviderAuthRoot(absTestPath("tmp", "provider-ledger")),
		WithSharedHermesHome(absTestPath("tmp", "provider-home")),
		WithDefaultModel("openai/gpt"),
		WithEnv(map[string]string{"A": "1"}),
		WithTracerProvider(tracenoop.NewTracerProvider()),
		WithMeterProvider(metricnoop.NewMeterProvider()),
		WithTextMapPropagator(propagation.TraceContext{}),
		WithSessionStore(store),
		WithSessionStoreLoadTimeout(time.Second),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1, MaxConcurrentClientCalls: 3}),
		WithImageLimits(ImageLimits{
			MaxInputBytesPerImage:     1,
			MaxInputBytesPerPrompt:    2,
			MaxOutputBytesPerImage:    3,
			MaxOutputBytesPerToolCall: 4,
		}),
		WithSeedFiles(seed),
	})
	if opts.AgentName != "name" || opts.AgentTitle != "title" || opts.ExecutablePath != "hermes" ||
		opts.Env["A"] != "1" || opts.SessionStore != store {
		t.Fatalf("options = %#v", opts)
	}
	if opts.Home != absTestPath("tmp", "home") || opts.ScratchDir != absTestPath("tmp", "scratch") {
		t.Fatalf("home/scratch options = %q / %q", opts.Home, opts.ScratchDir)
	}
	if opts.ProviderAuthRoot != absTestPath("tmp", "provider-ledger") || opts.SharedHermesHome != absTestPath("tmp", "provider-home") {
		t.Fatalf("provider auth options = %q / %q",
			opts.ProviderAuthRoot,
			opts.SharedHermesHome,
		)
	}
	if opts.ImageLimits != (ImageLimits{
		MaxInputBytesPerImage:     1,
		MaxInputBytesPerPrompt:    2,
		MaxOutputBytesPerImage:    3,
		MaxOutputBytesPerToolCall: 4,
	}) {
		t.Fatalf("image limits = %#v", opts.ImageLimits)
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

func TestImageLimitDefaults(t *testing.T) {
	limits := applyOptions(nil).ImageLimits
	want := ImageLimits{
		MaxInputBytesPerImage:     defaultImageLimitBytes,
		MaxInputBytesPerPrompt:    defaultImageLimitBytes,
		MaxOutputBytesPerImage:    defaultImageLimitBytes,
		MaxOutputBytesPerToolCall: defaultImageLimitBytes,
	}
	if limits != want {
		t.Fatalf("default image limits = %#v, want %#v", limits, want)
	}
}

func TestAgentEnvironmentNamesFailAtAgentConstruction(t *testing.T) {
	for key, reason := range map[string]string{
		"BASH_ENV":                 "reserved for the session PATH carrier",
		"ENV":                      "reserved for the session PATH carrier",
		"acp_go_hermes_path_dir_1": "reserved for the session PATH carrier",
		"NODE_OPTIONS":             "is an injection vector",
		"LD_PRELOAD":               "is an injection vector",
		"":                         "is not a variable name",
		"A=B":                      "is not a variable name",
	} {
		require.ErrorContains(t, NewAgent(WithEnv(map[string]string{key: "x"})).optionsErr, reason, key)
	}

	require.ErrorContains(t, NewAgent(WithEnv(map[string]string{"A": "x\x00y"})).optionsErr, "is not a variable name")
	require.NoError(t, NewAgent(WithEnv(map[string]string{"PATH": "/usr/bin", "https_proxy": "", "ld_preload": "own"})).optionsErr)
}
