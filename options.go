package hermesacp

import (
	"context"
	"log/slog"
	"os"
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

type ProcessIdentityLockCapability interface {
	Duplicate() (*os.File, error)
}

// ProcessIsolation is the optional explicit operating-system identity and
// complete base environment for every native Hermes process. Leaving
// Options.ProcessIsolation nil selects ordinary same-identity execution; a
// non-nil value selects the strict Linux boundary described on
// WithProcessIsolation.
type ProcessIsolation struct {
	UID             uint32
	GID             uint32
	BaseEnvironment map[string]string
	// IdentityLock is an optional trusted-supervisor descriptor for the
	// host-global UID lock. Linux supervisors validate it and never expose it to
	// the native Hermes process. Standalone embeddings should leave it nil.
	IdentityLock        ProcessIdentityLockCapability
	AuthorityDomain     ProcessIdentityLockCapability
	StandaloneOwnerID   string
	StandaloneStateRoot string
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
	// RuntimeContainmentSharedIdentity is the ordinary default, reported
	// whenever WithProcessIsolation is omitted. Native work runs as the
	// adapter's current operating-system identity, root or non-root alike, on
	// every supported platform.
	//
	// It is a non-authoritative posture rather than a containment achievement.
	// The wrapper completes the direct child and process group it started and
	// nothing beyond them, so it reports no provider-descendant inventory — not
	// even a terminal zero — and makes no whole-tree quiescence or
	// credential-separation claim.
	RuntimeContainmentSharedIdentity RuntimeContainmentMode = "shared_identity"
	RuntimeContainmentUnavailable    RuntimeContainmentMode = "unavailable"
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
	// Home is unsupported because native residence selection is explicit. Use
	// ScratchDir for isolated ephemeral state or SharedHermesHome for the
	// official-Hermes shared durable mode.
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
	// not hold. Provider auth is enabled only when SharedHermesHome is also set.
	ProviderAuthRoot string
	// SharedHermesHome selects official shared-home mode. Every native
	// process uses this one durable home, so per-session Hermes-home isolation is
	// intentionally disabled. Per-session processes and adapter-owned control
	// generations remain independent.
	SharedHermesHome string
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

	clientFactory            func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error)
	newPromptTimer           func(time.Duration) promptTimer
	storeWriteTTL            time.Duration
	beforeTerminalCommit     func()
	testOnlyNoCredential     bool
	testOnlyIdentityLockRoot string
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

// WithProcessIsolation explicitly selects the hardened Linux identity
// boundary: every Hermes process and version probe runs as the supplied
// nonzero non-root identity with no supplementary groups, under a trusted root
// supervisor that remains a distinct identity. BaseEnvironment is the complete
// native environment rather than an overlay on the adapter's own; WithEnv and
// session values overlay it. BASH_ENV and ACP_GO_HERMES_PATH_DIR_* are reserved
// for the adapter's native terminal PATH carrier.
//
// The option fails closed. Construction or launch refuses when the platform is
// not Linux, the supervisor is not root, the native identity is root, or the
// two identities are not distinct, and it never falls back to ordinary
// same-identity or Darwin best-effort execution. It cannot be combined with
// WithDarwinBestEffortContainment.
//
// Omitting this option is the ordinary default and is not a configuration
// error: Hermes then runs as the adapter's current UID/GID, root or non-root
// alike, on every supported platform, and the Agent reports shared_identity.
func WithProcessIsolation(isolation ProcessIsolation) Option {
	return func(options *Options) {
		cloned := isolation
		cloned.BaseEnvironment = cloneStringMap(isolation.BaseEnvironment)
		options.ProcessIsolation = &cloned
	}
}

// WithHome is unsupported because native residence selection is explicit. Use
// WithScratchDir for isolated ephemeral state or WithSharedHermesHome for the
// official-Hermes shared durable mode.
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
// offered. Provider auth is enabled only when WithSharedHermesHome is also set.
// The root carries no config or auth-resolution semantics and is never a
// scratch parent.
func WithProviderAuthRoot(path string) Option {
	return func(options *Options) {
		options.ProviderAuthRoot = path
	}
}

// WithSharedHermesHome explicitly selects official shared-home mode: every
// native gateway uses path as its exact durable HERMES_HOME. This
// intentionally gives up per-session Hermes-home isolation so credentials and
// native sessions survive adapter restarts with the official runtime.
//
// The path must be absolute and requires ordinary same-identity execution;
// combining it with WithProcessIsolation is rejected. Provider-auth extension
// methods additionally require WithProviderAuthRoot.
// Each ACP session retains its own native process, environment, PATH additions,
// event stream, and adapter-owned control generation. Native database and auth
// state are intentionally shared through path. Managed MCP configuration must
// be identical for every session owned by the Agent.
func WithSharedHermesHome(path string) Option {
	return func(options *Options) {
		options.SharedHermesHome = path
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

// WithEnv supplies the static Agent-scoped native environment overlay.
// BASH_ENV and ACP_GO_HERMES_PATH_DIR_* are reserved for the adapter's native
// terminal PATH carrier and fail Agent construction.
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
