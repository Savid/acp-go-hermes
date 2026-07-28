//nolint:wsl_v5 // Agent construction keeps dependent lifecycle state adjacent.
package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/observer"
)

const (
	listSessionsPageSize            = 50
	defaultMaxActiveSessions        = 32
	sessionTurnCapacity             = 1
	defaultMaxConcurrentClientCalls = 16
	closeTimeout                    = 5 * time.Second
	mcpReloadTimeout                = 2 * time.Minute

	valElicitation     = "elicitation"
	valBackpressure    = "backpressure"
	valUnknownSession  = "unknown session"
	agentClosedMessage = "agent closed"
	keyLimit           = "limit"
)

var (
	agentJSONMarshal   = json.Marshal
	agentJSONUnmarshal = json.Unmarshal
	newAgentForServe   = NewAgent
)

// Agent exposes Hermes through ACP.
type Agent struct {
	options         Options
	log             *slog.Logger
	observe         *observer.Observer
	optionsErr      error
	processes       *providerProcessTracker
	containmentMode RuntimeContainmentMode
	providerAuth    *providerAuth

	mu                 sync.Mutex
	closed             bool
	closeOnce          sync.Once
	closeErr           error
	containmentErr     error
	constructions      sync.WaitGroup
	conn               agentClient
	sessions           map[acp.SessionId]*session
	deleted            map[acp.SessionId]struct{}
	deleteCleanup      map[acp.SessionId]deleteCleanupRecord
	incompleteRoots    map[acp.SessionId]map[string]struct{}
	clientCalls        chan struct{}
	clientCapabilities acp.ClientCapabilities
	positionEncoding   acp.PositionEncodingKind
}

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)
	limits, optionsErr := normalizeConcurrencyLimits(options.ConcurrencyLimits)
	optionsErr = errors.Join(optionsErr, validateContainmentOptions(options), validateImageLimits(options.ImageLimits),
		validateInputHandoffRoot(options.InputHandoffRoot), validateProviderAuthRoots(options))
	options.ConcurrencyLimits = limits

	log := options.Logger
	if log == nil {
		log = slog.Default()
	}

	if options.SessionStore == nil {
		options.SessionStore = NewInMemorySessionStore()
	}

	observe := observer.New(observer.Config{
		MeterProvider:  options.MeterProvider,
		Propagator:     options.TextMapPropagator,
		TracerProvider: options.TracerProvider,
		Version:        options.AgentVersion,
	})
	options.RuntimeResourceHooks = instrumentRuntimeResourceHooks(options.RuntimeResourceHooks, observe)
	mode := containmentMode(options)
	if options.RuntimeResourceHooks.ObserveContainment != nil {
		options.RuntimeResourceHooks.ObserveContainment(context.Background(), mode)
	}
	if mode == RuntimeContainmentBestEffort {
		log.Warn("Darwin best-effort process containment is enabled; escaped descendants may survive, numeric PGID reuse can cause collateral signalling, marker correlation is not ownership, markers can be scrubbed, and native-root permits do not bound escaped provider work",
			slog.String("containment", string(mode)),
		)
	}

	agent := &Agent{
		options:         options,
		log:             log,
		optionsErr:      optionsErr,
		observe:         observe,
		sessions:        make(map[acp.SessionId]*session),
		deleted:         make(map[acp.SessionId]struct{}),
		deleteCleanup:   make(map[acp.SessionId]deleteCleanupRecord),
		incompleteRoots: make(map[acp.SessionId]map[string]struct{}),
		clientCalls:     make(chan struct{}, limits.MaxConcurrentClientCalls),
		containmentMode: mode,
	}
	agent.processes = newProviderProcessTracker(options.RuntimeResourceHooks, mode == RuntimeContainmentAuthoritative)
	agent.providerAuth = newProviderAuth(agent)

	return agent
}

func (a *Agent) ContainmentMode() RuntimeContainmentMode {
	if a == nil {
		return RuntimeContainmentUnavailable
	}

	return a.containmentMode
}

func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	agent := newAgentForServe(opts...)
	defer func() {
		if closeErr := agent.Close(); closeErr != nil {
			agent.log.DebugContext(context.Background(), "close Hermes ACP agent failed", slog.String(jsonFieldError, closeErr.Error()))
			returnErr = closeErr
		}
	}()

	conn := newLocalAgentConnection(agent, output, input)
	agent.setAgentClient(conn)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

func (a *Agent) setAgentClient(conn agentClient) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
}

func (a *Agent) connection() agentClient {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.conn
}

// turnTimeout is the per-turn native deadline, or 0 when no deadline applies.
func (a *Agent) turnTimeout() time.Duration {
	return a.options.TurnTimeout
}

func (a *Agent) Close() error {
	a.closeOnce.Do(func() { a.closeErr = a.close() })

	return a.closeErr
}

func (a *Agent) close() error {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()

	a.constructions.Wait()

	a.mu.Lock()
	sessions := make([]*session, 0, len(a.sessions))
	for _, session := range a.sessions {
		sessions = append(sessions, session)
	}

	a.sessions = make(map[acp.SessionId]*session)
	a.conn = nil
	a.mu.Unlock()

	var err error

	for _, session := range sessions {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		closeErr := session.Close(ctx)
		a.recordIncompleteContainment(closeErr, session.id, hermesServerRoot(session.client))
		err = errors.Join(err, closeErr)

		cancel()
	}

	a.observe.AddActiveSession(context.Background(), -int64(len(sessions)))
	a.mu.Lock()
	err = errors.Join(err, a.containmentErr)
	a.mu.Unlock()

	return err
}

func (a *Agent) beginSessionConstruction() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	a.constructions.Add(1)

	return nil
}

func (a *Agent) endSessionConstruction() {
	a.constructions.Done()
}

// optionsError reports a construction-time option failure as the uniform
// invalid-params error, or nil when every option validated.
func (a *Agent) optionsError() error {
	if a.optionsErr == nil {
		return nil
	}

	return acp.NewInvalidParams(map[string]any{jsonFieldError: a.optionsErr.Error()})
}

func (a *Agent) Initialize(_ context.Context, params acp.InitializeRequest) (acp.InitializeResponse, error) {
	if err := a.optionsError(); err != nil {
		return acp.InitializeResponse{}, err
	}

	title := a.options.AgentTitle
	positionEncoding := selectPositionEncoding(params.ClientCapabilities.PositionEncodings)

	a.mu.Lock()
	a.clientCapabilities = cloneClientCapabilities(params.ClientCapabilities)
	a.positionEncoding = positionEncoding
	a.mu.Unlock()

	hermesMeta := map[string]any{
		"fork": map[string]any{
			"unstable":      true,
			jsonFieldMethod: ForkSessionMethod,
			keyRequest:      "acp.UnstableForkSessionRequest JSON payload only",
			"response":      "acp.UnstableForkSessionResponse JSON payload only",
		},
		valElicitation: map[string]any{
			"unstable": true,
			"scope":    string(nativehermes.PermissionRouteSession),
			"tracks":   "in-progress ACP elicitation RFD",
		},
		rawEventCapabilityKey: map[string]any{
			jsonFieldMethod:  RawEventMethod,
			"enabledBy":      rawEventEnabledByPath,
			"maxBytes":       rawEventMaxBytes,
			"defaultEnabled": false,
		},
		"sessionStore": map[string]any{
			"format":     SessionStoreFormat,
			jsonFieldKey: []string{jsonFieldSessionID, "subpath"},
		},
	}

	if a.providerAuth != nil {
		hermesMeta[providerAuthCapabilityKey] = a.providerAuth.capability()
	}

	capabilityMeta := capabilityMediaMeta(a.options)
	capabilityMeta[hermesMetaKey] = hermesMeta
	capabilityMeta[routeMetaKey] = map[string]any{keyVersions: []int{routeVersion}}

	return acp.InitializeResponse{
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			Meta:        capabilityMeta,
			LoadSession: true,
			McpCapabilities: acp.McpCapabilities{
				Http: true,
			},
			PositionEncoding: &positionEncoding,
			PromptCapabilities: acp.PromptCapabilities{
				EmbeddedContext: true,
				Image:           true,
			},
			SessionCapabilities: acp.SessionCapabilities{
				AdditionalDirectories: &acp.SessionAdditionalDirectoriesCapabilities{},
				Close:                 &acp.SessionCloseCapabilities{},
				Delete:                &acp.SessionDeleteCapabilities{},
				List:                  &acp.SessionListCapabilities{},
				Resume:                &acp.SessionResumeCapabilities{},
			},
		},
	}, nil
}

func (a *Agent) Authenticate(_ context.Context, params acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

func (a *Agent) Logout(context.Context, acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, nil
}

func (a *Agent) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, error) {
	if err := a.ensureOpen(); err != nil {
		return nil, err
	}

	switch method {
	case ForkSessionMethod:
		var req acp.UnstableForkSessionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
		}

		if err := req.Validate(); err != nil {
			return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
		}

		return a.forkSession(ctx, req)
	default:
		if result, handled, err := a.handleAuthExtensionMethod(ctx, method, params); handled {
			return result, err
		}

		return nil, acp.NewMethodNotFound(method)
	}
}

func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	return nil
}

func (a *Agent) sessionStore() SessionStore {
	if a.options.SessionStore == nil {
		return NewInMemorySessionStore()
	}

	return a.options.SessionStore
}

func (a *Agent) sessionStoreContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := a.options.SessionStoreLoadTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	return context.WithTimeout(ctx, timeout)
}

func (a *Agent) acquireClientCall(ctx context.Context) (func(), error) {
	select {
	case a.clientCalls <- struct{}{}:
		return func() { <-a.clientCalls }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: valBackpressure, keyLimit: "client_calls"})
	}
}

func (a *Agent) session(id acp.SessionId) (*session, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.deleted[id]; ok {
		return nil, unknownSessionError()
	}

	session := a.sessions[id]
	if session == nil {
		return nil, unknownSessionError()
	}

	return session, nil
}

// unknownSessionError is the uniform rejection every surface gives an id nobody
// knows, so a caller cannot tell an unknown session from a deleted one.
func unknownSessionError() error {
	return acp.NewInvalidParams(map[string]any{jsonFieldError: valUnknownSession, keyField: jsonFieldSessionID})
}

func (a *Agent) activeSession(id acp.SessionId) *session {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.sessions[id]
}

func (a *Agent) storeStartedSession(session *session) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	if len(a.sessions) >= a.options.ConcurrencyLimits.MaxActiveSessions {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: valBackpressure, keyLimit: "active_sessions"})
	}

	a.sessions[session.id] = session
	delete(a.deleted, session.id)
	a.observe.AddActiveSession(context.Background(), 1)

	return nil
}

func (a *Agent) removeSessionIf(id acp.SessionId, target *session) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.sessions[id] != target {
		return false
	}

	delete(a.sessions, id)

	return true
}

func (a *Agent) isDeleted(id acp.SessionId) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, ok := a.deleted[id]

	return ok
}

func (a *Agent) clientElicitationCapabilities() *acp.ElicitationCapabilities {
	a.mu.Lock()
	defer a.mu.Unlock()

	caps := a.clientCapabilities.Elicitation
	if caps == nil {
		return nil
	}

	encoded, err := agentJSONMarshal(caps)
	if err != nil {
		return caps
	}

	var cloned acp.ElicitationCapabilities
	if err := agentJSONUnmarshal(encoded, &cloned); err != nil {
		return caps
	}

	return &cloned
}

func (a *Agent) clientSupportsFormElicitation() bool {
	caps := a.clientElicitationCapabilities()
	if caps == nil {
		return false
	}

	return caps.Form != nil || caps.Url == nil
}

func (a *Agent) clientSupportsURLElicitation() bool {
	caps := a.clientElicitationCapabilities()

	return caps != nil && caps.Url != nil
}

func selectPositionEncoding(values []acp.PositionEncodingKind) acp.PositionEncodingKind {
	for _, value := range values {
		if value == acp.PositionEncodingKindUtf8 {
			return value
		}
	}

	for _, value := range values {
		if value == acp.PositionEncodingKindUtf16 {
			return value
		}
	}

	return acp.PositionEncodingKindUtf16
}

func cloneClientCapabilities(caps acp.ClientCapabilities) acp.ClientCapabilities {
	encoded, err := agentJSONMarshal(caps)
	if err != nil {
		return caps
	}

	var cloned acp.ClientCapabilities
	if err := agentJSONUnmarshal(encoded, &cloned); err != nil {
		return caps
	}

	return cloned
}
