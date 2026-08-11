package hermesacp

import (
	"log/slog"
	"os"
	"path/filepath"
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
		WithHome("/tmp/home"),
		WithScratchDir("/tmp/scratch"),
		WithProviderAuthRoot("/tmp/provider-ledger"),
		WithSharedHermesHome("/tmp/provider-home"),
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
	if opts.Home != "/tmp/home" || opts.ScratchDir != "/tmp/scratch" {
		t.Fatalf("home/scratch options = %q / %q", opts.Home, opts.ScratchDir)
	}
	if opts.ProviderAuthRoot != "/tmp/provider-ledger" || opts.SharedHermesHome != "/tmp/provider-home" {
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

func TestProcessIsolationOptionClonesAndFailsClosed(t *testing.T) {
	base := map[string]string{"PATH": "/policy/bin", "CANARY": "base"}
	opts := applyOptions([]Option{WithProcessIsolation(ProcessIsolation{UID: 10, GID: 20, BaseEnvironment: base})})
	base["CANARY"] = "mutated"
	require.Equal(t, "base", opts.ProcessIsolation.BaseEnvironment["CANARY"])
	internal := nativeProcessIsolation(opts.ProcessIsolation, false, "")
	opts.ProcessIsolation.BaseEnvironment["CANARY"] = "later"
	require.Equal(t, "base", internal.BaseEnvironment["CANARY"])
	require.Nil(t, nativeProcessIsolation(nil, false, ""))
	require.Error(t, validateProcessIsolationOption(nil))
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: 0, GID: 1}))
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: 1, GID: 0}))

	original := agentRuntimePlatform
	agentRuntimePlatform = "windows"
	t.Cleanup(func() { agentRuntimePlatform = original })
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: 1, GID: 1}))
}

func TestSharedHermesHomeRejectsProcessIsolation(t *testing.T) {
	opts := applyOptions([]Option{
		WithSharedHermesHome("/var/lib/hermes"),
		WithProcessIsolation(ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{}}),
	})
	require.ErrorContains(t, validateSharedHermesHomeOptions(opts), "ordinary same-identity execution")
}

func TestInvalidSharedHermesHomeIsolationHasNoProviderAuthFilesystemSideEffects(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "must-not-exist")
	ledger := filepath.Join(root, "ledger-must-not-exist")
	agent := NewAgent(
		WithSharedHermesHome(home),
		WithProviderAuthRoot(ledger),
		WithProcessIsolation(ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{}}),
	)
	require.ErrorContains(t, agent.optionsErr, "ordinary same-identity execution")
	require.Nil(t, agent.providerAuth)
	for _, path := range []string{home, ledger} {
		_, err := os.Stat(path)
		require.ErrorIs(t, err, os.ErrNotExist, path)
	}
}
