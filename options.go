package hermesacp

import (
	"context"
	"log/slog"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
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

// WithHostAuthority routes native processes and tree ownership through authority.
func WithHostAuthority(authority HostAuthority) Option {
	return func(options *Options) {
		options.hostAuthoritySupplied = true
		options.HostAuthority = authority
	}
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
	HostAuthority  HostAuthority
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
	// generations remain independent. When ProviderAuthRoot is also set, NewAgent
	// replaces this field with the symlink-resolved residence it prepared, and
	// that resolved path is what every native process and every home-keyed claim
	// then uses.
	SharedHermesHome string
	DefaultModel     string
	Env              map[string]string
	// AmbientEnvironment replaces the adapter's own process environment as the
	// block ordinary execution inherits from. Its names are judged exactly as
	// inherited names are; WithEnv and session environments overlay it. Nil
	// inherits from the adapter's process. Managed execution never reads it.
	AmbientEnvironment map[string]string

	Logger            *slog.Logger
	TracerProvider    trace.TracerProvider
	MeterProvider     metric.MeterProvider
	TextMapPropagator propagation.TextMapPropagator

	// Client is the ACP client an embedded agent streams to. It is set by
	// WithClient and is the embedding host's counterpart to the JSON-RPC
	// transport Serve builds: without it a directly constructed Agent has
	// nowhere to publish session updates, permission requests, or
	// elicitations. Serve refuses an Options carrying one, because Serve
	// installs the connection it owns.
	Client                  acp.Client
	SessionStore            SessionStore
	SessionStoreLoadTimeout time.Duration
	ConcurrencyLimits       ConcurrencyLimits
	ImageLimits             ImageLimits
	SeedFiles               map[string]string
	TurnTimeout             time.Duration
	hostAuthoritySupplied   bool

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
// writes, moves, or removes anything there. Managed mode pins this read root
// before native preparation; it must be disjoint from the complete scratch parent.
func WithInputHandoffRoot(dir string) Option {
	return func(options *Options) {
		options.InputHandoffRoot = dir
	}
}

// WithProviderAuthRoot sets the durable directory that houses the values-free
// provider-auth ledger. The directory is created 0700 when missing and ledger
// entries are written 0600. Provider auth is enabled only when
// WithSharedHermesHome is also set. The root carries no config or
// auth-resolution semantics and is never a scratch parent.
//
// Three wrong configurations answer differently. Omitting this option leaves
// every provider-auth method unadvertised and answering method-not-found: a leg
// that cannot record what it did is never offered. A relative root, or this
// option without WithSharedHermesHome, is a construction failure that every
// Initialize reports as an internal error. An absolute root the agent cannot
// prepare — one it cannot create, restrict to 0700, or confirm as a writable
// directory — is logged at warn level and leaves the surface unadvertised while
// the rest of the agent works.
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
// The path must be absolute and already clean, and requires ordinary
// same-identity execution.
// Provider-auth extension methods additionally require WithProviderAuthRoot,
// and configuring both canonicalizes this path: the agent creates the directory
// 0700 when absent, resolves its symlinks, and adopts the resolved path for the
// rest of its life. That resolved path — not the spelling passed here — is the
// HERMES_HOME and shared XDG root every native process receives, the root the
// exclusive home-root claim fences, and the value the ledger's per-home key
// hashes, so a caller that named the home through a symlink must compare
// against the resolved form. Without WithProviderAuthRoot the path is used
// verbatim.
// Each ACP session retains its own native process, environment, PATH additions,
// event stream, and adapter-owned control generation. Native database and auth
// state are intentionally shared through path. Managed MCP configuration must
// be identical for every session owned by the Agent.
func WithSharedHermesHome(path string) Option {
	return func(options *Options) {
		options.SharedHermesHome = path
	}
}

func WithDefaultModel(model string) Option {
	return func(options *Options) {
		options.DefaultModel = model
	}
}

// WithEnv supplies the static Agent-scoped native environment overlay.
// Shell and loader injection names and the ACP_GO_HERMES_PATH_DIR_* and
// ACP_GO_HERMES_INTERNAL_* prefixes fail Agent construction. PATH sets the
// static native search path; session paths prepend through WithHermesExtraPathDirs.
func WithEnv(env map[string]string) Option {
	return func(options *Options) {
		options.Env = cloneStringMap(env)
	}
}

// WithAmbientEnvironment supplies the block ordinary execution inherits from in
// place of the adapter's own process environment. Entries are filtered like
// inherited entries; an entry that could not be an environment entry fails
// Agent construction. Managed execution reads nothing from it.
func WithAmbientEnvironment(env map[string]string) Option {
	return func(options *Options) {
		options.AmbientEnvironment = cloneStringMap(env)
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

// WithClient supplies the ACP client a directly constructed Agent streams to.
//
// Serve builds a JSON-RPC connection and installs it as the agent's client, so
// a host that runs the agent over stdio never sets this. A host that embeds the
// Agent and calls its ACP methods in-process gets the same outbound surface
// only through this option: session updates, permission requests, and
// elicitations are agent-to-client traffic, and an Agent with no client cannot
// deliver them. The supplied client is called from the agent's own goroutines
// and must be safe for concurrent use.
//
// Elicitation is delivered only when the client also implements
// UnstableCreateElicitation (the SDK's ClientExperimental surface); a client
// without it refuses elicitations instead of dropping them. Extension
// notifications, including RawEventMethod, are delivered only when the client
// implements ExtensionNotificationHandler.
func WithClient(client acp.Client) Option {
	return func(options *Options) {
		options.Client = client
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
