package hermesacp

import (
	"context"
	"log/slog"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// valACPGoHermes is the adapter identity used for agent metadata defaults and
// the fallback home root.
const valACPGoHermes = "acp-go-hermes"

// Option configures the Hermes ACP agent.
type Option func(*Options)

// ConcurrencyLimits bounds work accepted by one Agent.
type ConcurrencyLimits struct {
	MaxActiveSessions        int
	MaxConcurrentClientCalls int
}

// Options configures the ACP agent process and Hermes sessions it starts.
type Options struct {
	AgentName    string
	AgentTitle   string
	AgentVersion string

	ExecutablePath string
	// Home is unsupported: Hermes has no native config or auth root that the
	// adapter may target, so a non-empty Home is rejected as an unsupported
	// option when a session is established. Use ScratchDir to control where
	// ephemeral per-session state is materialized.
	Home string
	// ScratchDir is the sole parent directory for all ephemeral on-disk
	// materialization: isolated per-session Hermes homes, sqlite temp
	// directories, and process/server temp roots. An empty value means the
	// system temp directory. It is created with 0700 permissions when missing.
	ScratchDir   string
	DefaultModel string
	Env          map[string]string

	Logger            *slog.Logger
	TracerProvider    trace.TracerProvider
	MeterProvider     metric.MeterProvider
	TextMapPropagator propagation.TextMapPropagator

	SessionStore            SessionStore
	SessionStoreLoadTimeout time.Duration
	ConcurrencyLimits       ConcurrencyLimits
	SeedFiles               map[string]string
	TurnTimeout             time.Duration

	clientFactory func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error)
}

func applyOptions(opts []Option) Options {
	options := Options{
		AgentName:               valACPGoHermes,
		AgentTitle:              valACPGoHermes,
		AgentVersion:            "0.1.0",
		SessionStoreLoadTimeout: 10 * time.Second,
		clientFactory:           nativehermes.StartServer,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return options
}

func WithLogger(logger *slog.Logger) Option {
	return func(options *Options) {
		options.Logger = logger
	}
}

func WithAgentName(name string) Option {
	return func(options *Options) {
		options.AgentName = name
	}
}

func WithAgentTitle(title string) Option {
	return func(options *Options) {
		options.AgentTitle = title
	}
}

func WithAgentVersion(version string) Option {
	return func(options *Options) {
		options.AgentVersion = version
	}
}

func WithExecutablePath(path string) Option {
	return func(options *Options) {
		options.ExecutablePath = path
	}
}

// WithHome is unsupported. Hermes has no native config or auth root that the
// adapter may target, so establishing a session with a non-empty Home is
// rejected as an unsupported option. Use WithScratchDir to control where
// ephemeral per-session state is materialized.
func WithHome(path string) Option {
	return func(options *Options) {
		options.Home = path
	}
}

// WithScratchDir sets the sole parent directory for all ephemeral on-disk
// materialization: isolated per-session Hermes homes, sqlite temp directories,
// and process/server temp roots. An empty value (the default) means the system
// temp directory. The directory is created with 0700 permissions when missing.
func WithScratchDir(dir string) Option {
	return func(options *Options) {
		options.ScratchDir = dir
	}
}

func WithDefaultModel(model string) Option {
	return func(options *Options) {
		options.DefaultModel = model
	}
}

func WithEnv(env map[string]string) Option {
	return func(options *Options) {
		options.Env = cloneStringMap(env)
	}
}

func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(options *Options) {
		options.TracerProvider = provider
	}
}

func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(options *Options) {
		options.MeterProvider = provider
	}
}

func WithTextMapPropagator(propagator propagation.TextMapPropagator) Option {
	return func(options *Options) {
		options.TextMapPropagator = propagator
	}
}

func WithSessionStore(store SessionStore) Option {
	return func(options *Options) {
		options.SessionStore = store
	}
}

func WithSessionStoreLoadTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.SessionStoreLoadTimeout = timeout
	}
}

func WithConcurrencyLimits(limits ConcurrencyLimits) Option {
	return func(options *Options) {
		options.ConcurrencyLimits = limits
	}
}

// WithTurnTimeout bounds how long a single native turn may run before the
// wrapper aborts it and fails the prompt with a hermes_turn_failed error whose
// cause is "timeout". The default of 0 disables the deadline. A timeout is a
// failure, not a user cancel, so it is never reported as StopReason cancelled.
func WithTurnTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.TurnTimeout = timeout
	}
}

// WithSeedFiles maps relative paths to file contents that the adapter writes
// into each session's isolated Hermes config root before launching hermes, so
// hermes reads them as its own config (for example config.yaml). Paths are
// confined to that root: absolute paths, ".." segments, and empty keys are
// rejected at session start. Contents are written verbatim, so secrets belong
// in WithEnv and are referenced from seeded files by env-var indirection (for
// example hermes key_env), never written into a seeded file.
func WithSeedFiles(files map[string]string) Option {
	return func(options *Options) {
		options.SeedFiles = cloneStringMap(files)
	}
}
