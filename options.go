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

// defaultImageLimitBytes is 6 MiB decoded, the default for every ImageLimits
// field. It keeps a maximal single-image prompt inside the pinned ACP SDK's
// 10 MiB frame bound with headroom for JSON overhead and surrounding text.
const defaultImageLimitBytes int64 = 6 * 1024 * 1024

// Option configures the Hermes ACP agent.
type Option func(*Options)

// ProcessIsolation is the mandatory operating-system identity and complete
// base environment for every native Hermes process.
type ProcessIsolation struct {
	UID             uint32
	GID             uint32
	BaseEnvironment map[string]string
}

// ConcurrencyLimits bounds work accepted by one Agent.
type ConcurrencyLimits struct {
	MaxActiveSessions        int
	MaxConcurrentClientCalls int
}

// ImageLimits bounds decoded image bytes. Every field counts decoded bytes,
// never base64 characters or enclosing JSON, and defaults to 6 MiB decoded
// (6,291,456 bytes). A field explicitly set to zero in a supplied ImageLimits
// disables that adapter policy limit; it never bypasses native framing,
// provider, memory, or host request limits. Negative fields are rejected at
// agent construction. The two output fields are accepted for the uniform
// option surface; this adapter emits no typed image output, so no output
// limit is ever consulted.
type ImageLimits struct {
	MaxInputBytesPerImage     int64
	MaxInputBytesPerPrompt    int64
	MaxOutputBytesPerImage    int64
	MaxOutputBytesPerToolCall int64
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

	ExecutablePath   string
	ProcessIsolation *ProcessIsolation
	// Home is unsupported because each session runtime root is isolated. Use
	// ScratchDir for ephemeral state and ProviderAuthHome for durable provider
	// credentials.
	Home string
	// ScratchDir is the sole parent directory for all ephemeral on-disk
	// materialization: isolated per-session Hermes homes, sqlite temp
	// directories, and process/server temp roots. An empty value means the
	// system temp directory. It is created with 0700 permissions when missing.
	ScratchDir string
	// InputHandoffRoot is the read-only root under which the host materializes
	// handoff image files. An empty value (the default) leaves the local-handoff
	// prompt form rejected. It is not a materialization option: the adapter
	// never writes, moves, or removes anything under it.
	InputHandoffRoot string
	// ProviderAuthRoot is the absolute, host-owned, durable directory that
	// houses the values-free provider-auth ledger. It is not ephemeral
	// materialization: the ledger deliberately outlives every session and every
	// native generation, which is the one class of state a scratch parent must
	// not hold. Provider auth is enabled only when ProviderAuthHome is also set.
	ProviderAuthRoot string
	// ProviderAuthDirectHome is unsupported. A non-empty value is rejected when
	// a session is established.
	ProviderAuthDirectHome string
	// ProviderAuthHome is the absolute, durable native Hermes credential
	// residence shared by every isolated session runtime. Hermes alone reads and
	// writes credential material there; the adapter supplies it to native
	// processes through HERMES_AUTH_HOME.
	ProviderAuthHome string
	DefaultModel     string
	Env              map[string]string

	Logger            *slog.Logger
	TracerProvider    trace.TracerProvider
	MeterProvider     metric.MeterProvider
	TextMapPropagator propagation.TextMapPropagator

	SessionStore                SessionStore
	SessionStoreLoadTimeout     time.Duration
	ConcurrencyLimits           ConcurrencyLimits
	ImageLimits                 ImageLimits
	SeedFiles                   map[string]string
	TurnTimeout                 time.Duration
	RuntimeResourceHooks        RuntimeResourceHooks
	DarwinBestEffortContainment bool

	clientFactory        func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error)
	newPromptTimer       func(time.Duration) promptTimer
	storeWriteTTL        time.Duration
	beforeTerminalCommit func()
	testOnlyNoCredential bool
}

func applyOptions(opts []Option) Options {
	options := Options{
		AgentName:               valACPGoHermes,
		AgentTitle:              valACPGoHermes,
		AgentVersion:            "0.1.0",
		SessionStoreLoadTimeout: 10 * time.Second,
		ImageLimits: ImageLimits{
			MaxInputBytesPerImage:     defaultImageLimitBytes,
			MaxInputBytesPerPrompt:    defaultImageLimitBytes,
			MaxOutputBytesPerImage:    defaultImageLimitBytes,
			MaxOutputBytesPerToolCall: defaultImageLimitBytes,
		},
		storeWriteTTL: sessionStoreWriteTimeout,
		clientFactory: nativehermes.StartServer,
		newPromptTimer: func(timeout time.Duration) promptTimer {
			timer := time.NewTimer(timeout)

			return promptTimer{C: timer.C, Stop: timer.Stop}
		},
	}
	for _, opt := range opts {
		opt(&options)
	}

	if options.ProcessIsolation != nil {
		cloned := *options.ProcessIsolation
		cloned.BaseEnvironment = cloneStringMap(options.ProcessIsolation.BaseEnvironment)
		options.ProcessIsolation = &cloned
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

// WithProcessIsolation requires every Hermes process and version probe to run
// as the supplied non-root identity with no supplementary
// groups. BaseEnvironment replaces the adapter environment; WithEnv and
// session values overlay it.
func WithProcessIsolation(isolation ProcessIsolation) Option {
	return func(options *Options) {
		cloned := isolation
		cloned.BaseEnvironment = cloneStringMap(isolation.BaseEnvironment)
		options.ProcessIsolation = &cloned
	}
}

// WithHome is unsupported because each session runtime root is isolated. Use
// WithScratchDir for ephemeral state and WithProviderAuthHome for durable
// provider credentials.
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

// WithInputHandoffRoot sets the read-only root under which the host has
// materialized handoff image files, enabling the local-handoff prompt form: an
// image block with empty data, a file URI under this root, and an
// acp-go.dev/handoff envelope carrying the file's sha256 digest and size. The
// path must be absolute; a relative path is rejected at agent construction.
// Omitting the option leaves the form rejected as invalid_handoff, and the
// acp-go.dev/handoff capability is then not advertised. Files under the root
// stay host-owned: the adapter resolves, reads, and verifies them, and never
// writes, moves, or removes anything there.
func WithInputHandoffRoot(dir string) Option {
	return func(options *Options) {
		options.InputHandoffRoot = dir
	}
}

// WithProviderAuthRoot sets the durable directory that houses the values-free
// provider-auth ledger. The path must be absolute; a relative path is rejected
// at agent construction. The directory is created 0700 when missing and ledger
// entries are written 0600. Omitting the option, or supplying a root that is
// not a writable directory, leaves every provider-auth method unadvertised and
// answering method-not-found: a leg that cannot record what it did is never
// offered. Provider auth is enabled only when WithProviderAuthHome is also set.
// The root carries no config or auth-resolution semantics and is never a
// scratch parent.
func WithProviderAuthRoot(path string) Option {
	return func(options *Options) {
		options.ProviderAuthRoot = path
	}
}

// WithProviderAuthDirectHome is unsupported. Establishing a session with a
// non-empty value fails closed.
func WithProviderAuthDirectHome(path string) Option {
	return func(options *Options) {
		options.ProviderAuthDirectHome = path
	}
}

// WithProviderAuthHome sets the durable native credential residence shared by
// every isolated Hermes session runtime. The path must be absolute. Hermes owns
// all credential reads and writes there; the adapter never parses or copies its
// credential files. Provider auth is enabled only when WithProviderAuthRoot is
// also set.
func WithProviderAuthHome(path string) Option {
	return func(options *Options) {
		options.ProviderAuthHome = path
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

// WithImageLimits replaces every decoded-byte image limit with the supplied
// values. A zero field disables that adapter policy limit; a negative field
// is rejected at agent construction. Omitting the option leaves all four
// fields at their default of 6 MiB decoded.
func WithImageLimits(limits ImageLimits) Option {
	return func(options *Options) {
		options.ImageLimits = limits
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
