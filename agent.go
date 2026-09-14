package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	// RawEventMethod is the notification carrying one raw hermes event when a
	// session opted in through _meta.hermes.rawEvent.enabled.
	RawEventMethod = "_hermes/rawEvent"
	// SessionStoreFormat identifies the store layout this package writes: native
	// per-conversation JSON exports under main plus the adapter's session
	// record under the config subpath.
	SessionStoreFormat = "hermes-session-json-v1"

	vendor = "hermes"

	capabilityMethodKey = "method"
)

// client is the host side of the connection, as the sessions use it.
type client interface {
	SessionUpdate(ctx context.Context, params acp.SessionNotification) error
	RequestPermission(ctx context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error)
	UnstableCreateElicitation(ctx context.Context, params acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error)
	NotifyExtension(ctx context.Context, method string, params any) error
}

// Agent exposes the hermes coding agent through ACP.
type Agent struct {
	options   Options
	log       *slog.Logger
	observe   *observer.Observer
	optionErr *acp.RequestError
	// processEnv is the adapter's own environment, read once at construction.
	processEnv []string
	store      acpcore.SessionStore

	mu                 sync.Mutex
	conn               client
	transport          *wire.Transport
	closed             bool
	clientCapabilities acp.ClientCapabilities
	positionEncoding   acp.PositionEncodingKind
	// lifecycle is the answer this connection gave at initialize. An absent
	// answer leaves the extension dormant for every session on it.
	lifecycle    lifecycle.Negotiated
	restores     wire.SessionRequests
	sessions     map[acp.SessionId]*session
	deleted      map[acp.SessionId]struct{}
	clientCalls  chan struct{}
	incarnations uint64

	versionOnce sync.Once
	versionErr  error
	executable  string
}

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

// NewAgent creates an ACP agent for the hermes coding agent CLI. Construction
// never fails; a refused option is reported by Initialize and every
// session-establishing method as pi_invalid_options.
func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)

	log := options.Logger
	if log == nil {
		log = slog.Default()
	}

	store := options.SessionStore
	if store == nil {
		store = acpcore.NewInMemorySessionStore()
	}

	agent := &Agent{
		options: options,
		log:     log,
		observe: observer.New(observer.Config{
			Vendor: vendor, NativeClient: "hermes-coding-agent",
			MeterProvider:  options.MeterProvider,
			Propagator:     options.TextMapPropagator,
			TracerProvider: options.TracerProvider,
			Version:        options.AgentVersion,
		}),
		processEnv:       os.Environ(),
		store:            store,
		sessions:         make(map[acp.SessionId]*session),
		deleted:          make(map[acp.SessionId]struct{}),
		clientCalls:      make(chan struct{}, max(0, options.ConcurrencyLimits.MaxConcurrentClientCalls)),
		positionEncoding: acp.PositionEncodingKindUtf16,
	}
	agent.optionErr = agent.validateOptions()

	return agent
}

// validateOptions reports the first refused option. The reason goes to the
// log; the wire answer names only the option.
func (a *Agent) validateOptions() *acp.RequestError {
	options := a.options

	checks := []struct {
		field string
		err   error
	}{
		{"home", validateOptionalAbsolute(options.Home)},
		{"inputHandoffRoot", validateHandoffRoot(options.InputHandoffRoot)},
		{"defaultModel", validateOptionalModel(options.DefaultModel)},
		{"configuredModels", validateConfiguredModels(options.ConfiguredModels)},
		{metaEnvKey, process.ValidateNames(options.Env)},
		{"concurrencyLimits", validateConcurrencyLimits(options.ConcurrencyLimits)},
		{"imageLimits", options.ImageLimits.core().Validate()},
	}

	for _, check := range checks {
		if check.err == nil {
			continue
		}

		a.log.Error("hermes agent option rejected", slog.String("field", check.field), slog.String("reason", check.err.Error()))

		return wire.InvalidOptions(vendor, check.field)
	}

	return nil
}

func validateOptionalAbsolute(path string) error {
	if path == "" || filepath.IsAbs(path) {
		return nil
	}

	return errors.New("path must be absolute")
}

func validateHandoffRoot(root string) error {
	if root == "" {
		return nil
	}

	return image.ValidateHandoffRoot(root)
}

func validateOptionalModel(model string) error {
	if model == "" {
		return nil
	}

	err := hermes.ModelSelectionShapeError(model)

	return err
}

func validateConfiguredModels(ids []string) error {
	seen := make(map[string]struct{}, len(ids))

	for index, id := range ids {
		if err := hermes.ModelSelectionShapeError(id); err != nil || id != strings.TrimSpace(id) {
			return fmt.Errorf("configured model %d %q is not a model id", index, id)
		}

		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("configured model %q is listed twice", id)
		}

		seen[id] = struct{}{}
	}

	return nil
}

func validateConcurrencyLimits(limits ConcurrencyLimits) error {
	if limits.MaxActiveSessions < 0 || limits.MaxConcurrentClientCalls < 0 {
		return errors.New("concurrency limits must not be negative")
	}

	return nil
}

// Serve runs an ACP agent over the provided streams. It blocks until the
// context is cancelled or the peer closes the connection, then closes the
// agent.
func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	agent := NewAgent(opts...)
	defer func() {
		if closeErr := agent.Close(); closeErr != nil {
			returnErr = closeErr
		}
	}()

	transport := wire.NewTransport(input, output)
	conn := acp.NewAgentSideConnection(agent, transport.Writer(), transport.Reader())
	conn.SetLogger(agent.log)
	agent.attach(conn, transport)
	transport.Start()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

// attach binds the host connection the sessions emit through.
func (a *Agent) attach(conn client, transport *wire.Transport) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
	a.transport = transport
}

func (a *Agent) connection() client {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.conn
}

// Close runs the shutdown ladder for every session and refuses every later
// request.
func (a *Agent) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return nil
	}

	a.closed = true
	sessions := slices.Collect(func(yield func(*session) bool) {
		for _, s := range a.sessions {
			if !yield(s) {
				return
			}
		}
	})
	a.conn = nil
	a.mu.Unlock()

	var errs []error

	for _, s := range sessions {
		if err := s.close(context.Background()); err != nil {
			errs = append(errs, err)
		}
	}

	a.mu.Lock()
	clear(a.sessions)
	a.mu.Unlock()

	return errors.Join(errs...)
}

func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return errAgentClosed()
	}

	return nil
}

// errAgentClosed answers every request after Close.
func errAgentClosed() *acp.RequestError {
	return acp.NewInvalidRequest(map[string]any{wire.FieldError: "agent closed"})
}

// Initialize implements ACP initialize.
func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	_, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodInitialize)
	defer func() { finish(err) }()

	if a.optionErr != nil {
		return acp.InitializeResponse{}, a.optionErr
	}

	meta := params.Meta
	if t := a.transportRef(); t != nil {
		meta = lifecycle.RetainRequestMetadata(meta, t.TakeRaw(acp.AgentMethodInitialize))
	}

	offer, present, paramErr := lifecycle.DecodeOffer(meta)
	if paramErr != nil {
		return acp.InitializeResponse{}, invalidParam(paramErr)
	}

	var negotiated lifecycle.Negotiated
	if present {
		negotiated = offer.Answer(lifecycle.Negotiated{UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}})
	}

	encoding := selectPositionEncoding(params.ClientCapabilities.PositionEncodings)

	a.mu.Lock()
	a.clientCapabilities = params.ClientCapabilities
	a.positionEncoding = encoding
	a.lifecycle = negotiated
	a.mu.Unlock()

	title := a.options.AgentTitle

	capabilityMeta := map[string]any{
		vendor: map[string]any{
			"elicitation": map[string]any{"unstable": true, "scope": nativeScopeSession, "tracks": "ACP v1 elicitation"},
			metaRawEventKey: map[string]any{
				capabilityMethodKey: RawEventMethod, "enabledBy": "_meta.hermes.rawEvent.enabled",
				"maxBytes": wire.RawEventMaxBytes, "defaultEnabled": false,
			},
			"sessionStore": map[string]any{"format": SessionStoreFormat, "key": []string{"sessionId", "subpath"}},
		},
		wire.MediaEnvelopeKey: image.MediaEnvelope(a.options.ImageLimits.core(), image.Envelope{DocumentFormats: []string{}}),
	}
	if a.options.InputHandoffRoot != "" {
		capabilityMeta[wire.HandoffKey] = image.HandoffAdvertisement()
	}

	var responseMeta map[string]any
	if negotiated.Present() {
		responseMeta = map[string]any{wire.LifecycleKey: negotiated.Advertisement()}
	}

	return acp.InitializeResponse{
		Meta:            responseMeta,
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			Meta:             capabilityMeta,
			LoadSession:      true,
			PositionEncoding: &encoding,
			PromptCapabilities: acp.PromptCapabilities{
				EmbeddedContext: true,
				Image:           true,
			},
			SessionCapabilities: acp.SessionCapabilities{
				Close:                 &acp.SessionCloseCapabilities{},
				Delete:                &acp.SessionDeleteCapabilities{},
				List:                  &acp.SessionListCapabilities{},
				Resume:                &acp.SessionResumeCapabilities{},
				AdditionalDirectories: &acp.SessionAdditionalDirectoriesCapabilities{},
			},
		},
	}, nil
}

func selectPositionEncoding(encodings []acp.PositionEncodingKind) acp.PositionEncodingKind {
	if slices.Contains(encodings, acp.PositionEncodingKindUtf8) {
		return acp.PositionEncodingKindUtf8
	}

	return acp.PositionEncodingKindUtf16
}

// Authenticate exists because the SDK interface requires it. The harness
// authenticates itself in its own home, outside ACP.
func (a *Agent) Authenticate(_ context.Context, params acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.AuthenticateResponse{}, invalidParam(refusal)
	}

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

// Logout exists because the SDK interface requires it.
func (a *Agent) Logout(_ context.Context, params acp.LogoutRequest) (acp.LogoutResponse, error) {
	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.LogoutResponse{}, invalidParam(refusal)
	}

	return acp.LogoutResponse{}, acp.NewMethodNotFound(acp.AgentMethodLogout)
}

// SetSessionMode exists because the SDK interface requires it. Native modes
// are config options, never ACP session modes.
func (a *Agent) SetSessionMode(_ context.Context, params acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.SetSessionModeResponse{}, invalidParam(refusal)
	}

	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

// HandleExtensionMethod answers every extension method with method-not-found.
// The only extension surface is the outbound RawEventMethod notification.
func (a *Agent) HandleExtensionMethod(_ context.Context, method string, params json.RawMessage) (any, error) {
	var envelope struct {
		Meta map[string]any `json:"_meta"` //nolint:tagliatelle // ACP reserves this wire spelling.
	}

	if err := json.Unmarshal(params, &envelope); err == nil {
		if refusal := lifecycle.RejectKey(envelope.Meta); refusal != nil {
			return nil, invalidParam(refusal)
		}
	}

	return nil, acp.NewMethodNotFound(method)
}

func (a *Agent) transportRef() *wire.Transport {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.transport
}

func (a *Agent) lifecycleNegotiated() lifecycle.Negotiated {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lifecycle
}

func (a *Agent) clientSupportsFormElicitation() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.clientCapabilities.Elicitation != nil && a.clientCapabilities.Elicitation.Form != nil
}

// nextIncarnation mints a stream identity no earlier incarnation of any
// session on this agent used.
func (a *Agent) nextIncarnation() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.incarnations++

	return a.incarnations
}

// acquireClientCall takes one slot of the server-to-client call budget
// without waiting.
func (a *Agent) acquireClientCall() (func(), error) {
	select {
	case a.clientCalls <- struct{}{}:
		return func() { <-a.clientCalls }, nil
	default:
		return nil, wire.Backpressure("client_calls")
	}
}

// invalidParam renders a lifecycle negotiation refusal as the uniform
// invalid-params verdict.
func invalidParam(err *lifecycle.ParamError) *acp.RequestError {
	if err.Verdict == lifecycle.VerdictMissing {
		return wire.Missing(err.Field)
	}

	return wire.Unsupported(err.Field)
}

// ensureExecutable resolves the hermes executable against the base environment
// and probes its version once per agent.
func (a *Agent) ensureExecutable(ctx context.Context) (string, error) {
	a.versionOnce.Do(func() {
		base, err := a.environment(nil, nil).Base()
		if err != nil {
			a.versionErr = err

			return
		}

		selector := a.options.ExecutablePath
		if selector == "" {
			selector = vendor
		}

		executable, err := process.ResolveExecutable(selector, base)
		if err != nil {
			a.versionErr = err

			return
		}

		version, err := hermes.ProbeVersion(ctx, executable, base)
		if err != nil {
			a.versionErr = err

			return
		}

		if err := hermes.CheckMinimumVersion(version, hermes.MinimumVersion); err != nil {
			a.versionErr = err

			return
		}

		a.executable = executable
	})

	if a.versionErr != nil {
		a.log.ErrorContext(ctx, "hermes version probe failed", slog.String("reason", a.versionErr.Error()))

		return "", wire.InternalFailure(vendor, internalClassNativeStart)
	}

	return a.executable, nil
}

// environment builds the merge for one launch: the inherited process
// environment, the agent overlay, the session env, then the keys this
// adapter owns because of how it launches hermes.
func (a *Agent) environment(sessionEnv map[string]string, owned map[string]string) process.Environment {
	merged := make(map[string]string, len(owned)+1)
	if a.options.Home != "" {
		merged[hermes.EnvAgentDir] = a.options.Home
	}

	maps.Copy(merged, owned)

	return process.Environment{
		Process:        a.processEnv,
		Agent:          a.options.Env,
		Session:        sessionEnv,
		Owned:          merged,
		InternalPrefix: hermes.InternalEnvPrefix,
	}
}

// internalClassNativeStart is the one documented hermes_internal_failure class: a
// native hermes process that could not be started or configured for a session.
const internalClassNativeStart = "native_start"
