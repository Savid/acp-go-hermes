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
// the per-session home root directory name under the scratch parent.
const valACPGoHermes = "acp-go-hermes"

// Option configures the Hermes ACP agent.
type Option func(*Options)

// ConcurrencyLimits bounds work accepted by one Agent.
type ConcurrencyLimits struct {
	MaxActiveSessions        int
	MaxConcurrentClientCalls int
}

// RuntimeResourceKind identifies the lifecycle scope consuming a host-managed resource.
type RuntimeResourceKind string

const (
	RuntimeResourceRuntime   RuntimeResourceKind = "runtime"
	RuntimeResourceSession   RuntimeResourceKind = "session"
	RuntimeResourcePrompt    RuntimeResourceKind = "prompt"
	RuntimeResourceDiscovery RuntimeResourceKind = "discovery"
)

type RuntimeProcessKind string

const (
	RuntimeProcessHomeLockSupervisor RuntimeProcessKind = "home_lock_supervisor"
	RuntimeProcessProviderDescendant RuntimeProcessKind = "provider_descendant"
)

type RuntimeContainmentMode string

const (
	RuntimeContainmentAuthoritative RuntimeContainmentMode = "authoritative"
	RuntimeContainmentBestEffort    RuntimeContainmentMode = "best_effort"
	RuntimeContainmentUnavailable   RuntimeContainmentMode = "unavailable"
)

type RuntimeStartupStage string

const (
	RuntimeStartupSpawn         RuntimeStartupStage = "spawn"
	RuntimeStartupReadiness     RuntimeStartupStage = "readiness"
	RuntimeStartupConfiguration RuntimeStartupStage = "configuration"
	RuntimeStartupSession       RuntimeStartupStage = "session"
)

// RuntimeResourceHooks lets an embedding host enforce native-root and scratch-root limits.
// A nil callback leaves that resource unbounded for standalone use.
type RuntimeResourceHooks struct {
	AcquireNativeRoot      func(context.Context, RuntimeResourceKind) (func(), error)
	ReserveScratchRoot     func(context.Context, RuntimeResourceKind) (func(), error)
	ObserveProcess         func(context.Context, RuntimeProcessKind, int64)
	ObserveProcessSnapshot func(context.Context, RuntimeProcessKind, int)
	ObserveStartupStage    func(context.Context, RuntimeResourceKind, RuntimeStartupStage, time.Duration, error)
	ObserveContainment     func(context.Context, RuntimeContainmentMode)
}

type promptTimer struct {
	C    <-chan time.Time
	Stop func() bool
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

	SessionStore                SessionStore
	SessionStoreLoadTimeout     time.Duration
	ConcurrencyLimits           ConcurrencyLimits
	SeedFiles                   map[string]string
	TurnTimeout                 time.Duration
	RuntimeResourceHooks        RuntimeResourceHooks
	DarwinBestEffortContainment bool

	clientFactory        func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error)
	newPromptTimer       func(time.Duration) promptTimer
	storeWriteTTL        time.Duration
	beforeTerminalCommit func()
}

func applyOptions(opts []Option) Options {
	options := Options{
		AgentName:               valACPGoHermes,
		AgentTitle:              valACPGoHermes,
		AgentVersion:            "0.1.0",
		SessionStoreLoadTimeout: 10 * time.Second,
		storeWriteTTL:           sessionStoreWriteTimeout,
		clientFactory:           nativehermes.StartServer,
		newPromptTimer: func(timeout time.Duration) promptTimer {
			timer := time.NewTimer(timeout)

			return promptTimer{C: timer.C, Stop: timer.Stop}
		},
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

func WithDarwinBestEffortContainment() Option {
	return func(options *Options) {
		options.DarwinBestEffortContainment = true
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
// wrapper interrupts it, closes and proves the whole native process boundary,
// and fails the prompt with a hermes_turn_failed error whose cause is "timeout".
// The default of 0 disables the deadline. A timeout is a failure, not a user
// cancel, so it is never reported as StopReason cancelled.
func WithTurnTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.TurnTimeout = timeout
	}
}

// WithRuntimeResourceHooks installs host-facing native-root and scratch-root admission hooks.
func WithRuntimeResourceHooks(hooks RuntimeResourceHooks) Option {
	return func(options *Options) {
		options.RuntimeResourceHooks = hooks
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
