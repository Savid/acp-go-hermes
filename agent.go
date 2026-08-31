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

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/savid/acp-go-hermes/internal/observer"
)

const capabilityScopeSession = "session"

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
	options      Options
	log          *slog.Logger
	observe      *observer.Observer
	optionsErr   error
	providerAuth *providerAuth
	// ambientEnv is the adapter's environment as it stood at construction. It is
	// the base ordinary same-identity execution sanitizes; managed execution
	// uses the authority's environment instead.
	ambientEnv map[string]string
	nativeEnv  map[string]string

	mu                 sync.Mutex
	closed             bool
	closeOnce          sync.Once
	closeErr           error
	containmentErr     error
	authorityErr       error
	constructions      sync.WaitGroup
	constructing       int
	constructionSeq    uint64
	constructionCancel map[uint64]context.CancelCauseFunc
	conn               agentClient
	sessions           map[acp.SessionId]*session
	deleted            map[acp.SessionId]struct{}
	deleteCleanup      map[acp.SessionId]deleteCleanupRecord
	incompleteRoots    map[acp.SessionId]map[string]struct{}
	clientCalls        chan struct{}
	clientCapabilities acp.ClientCapabilities
	positionEncoding   acp.PositionEncodingKind
	lifecycleAnswer    lifecycle.Negotiated

	streamOpenMu   sync.Mutex
	streamOpens    []*deferredStreamOpen
	streamOpenWait sync.WaitGroup

	sharedConfigMu          sync.Mutex
	sharedConfigInitialized bool
	sharedMCPServers        []acp.McpServer
}

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)
	limits, optionsErr := normalizeConcurrencyLimits(options.ConcurrencyLimits)
	optionsErr = errors.Join(optionsErr, validateHostAuthority(options), validateImageLimits(options.ImageLimits),
		validateInputHandoffRoot(options.InputHandoffRoot), validateProviderAuthRoots(options),
		validateSharedHermesHomeOptions(options), validatePathCarrierOptions(options))
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
	var nativeEnv map[string]string
	if options.hostAuthoritySupplied && optionsErr == nil {
		nativeEnv, optionsErr = readHostEnvironment(options.HostAuthority)
	}

	agent := &Agent{
		options:            options,
		log:                log,
		optionsErr:         optionsErr,
		observe:            observe,
		sessions:           make(map[acp.SessionId]*session),
		deleted:            make(map[acp.SessionId]struct{}),
		deleteCleanup:      make(map[acp.SessionId]deleteCleanupRecord),
		incompleteRoots:    make(map[acp.SessionId]map[string]struct{}),
		constructionCancel: make(map[uint64]context.CancelCauseFunc),
		clientCalls:        make(chan struct{}, limits.MaxConcurrentClientCalls),
		ambientEnv:         ambientEnvironment(),
		nativeEnv:          nativeEnv,
	}
	// Invalid option combinations must be side-effect free. In particular,
	// provider-auth initialization prepares the durable Hermes residence, which
	// must never happen after shared-home or host-authority validation failed.
	if optionsErr == nil && options.HostAuthority == nil {
		agent.providerAuth = newProviderAuth(agent)
	}

	return agent
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
	constructionCancellations := make([]context.CancelCauseFunc, 0, len(a.constructionCancel))
	for _, cancel := range a.constructionCancel {
		constructionCancellations = append(constructionCancellations, cancel)
	}
	conn := a.conn
	a.mu.Unlock()
	for _, cancel := range constructionCancellations {
		cancel(acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage}))
	}

	a.cancelStreamOpens()
	var err error
	a.constructions.Wait()
	if preparer, ok := conn.(interface{ PrepareTransportClose(context.Context) error }); ok {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		err = errors.Join(err, preparer.PrepareTransportClose(ctx))
		cancel()
	}
	a.awaitStreamOpens()
	if closer, ok := conn.(interface{ CloseTransport(context.Context) error }); ok {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		err = errors.Join(err, closer.CloseTransport(ctx))
		cancel()
	}

	a.mu.Lock()
	sessions := make([]*session, 0, len(a.sessions))
	for _, session := range a.sessions {
		sessions = append(sessions, session)
	}

	a.sessions = make(map[acp.SessionId]*session)
	a.mu.Unlock()

	// The shutdown ladder applies identically here, and that includes the durable
	// rung: an embedded shutdown owes every commit a wire session/close would have
	// made, rather than dropping retained state along with the wrapper. The
	// connection is already gone, so the boundary's emission rungs have nowhere to
	// speak; its containment proof and its commits run exactly as they do on the
	// wire, and a commit the store refuses fails this close.
	for _, session := range sessions {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)

		session.prepareClose()

		waitErr := session.awaitSettlement(ctx)

		session.lifecycleMu.Lock()
		closeErr := session.settleClosedSession(ctx)
		session.lifecycleMu.Unlock()

		a.recordIncompleteContainment(closeErr, session.id, hermesServerRoot(session.client))
		err = errors.Join(err, waitErr, closeErr)

		cancel()
	}
	a.mu.Lock()
	a.conn = nil
	a.mu.Unlock()
	a.observe.AddActiveSession(context.Background(), -int64(len(sessions)))
	a.mu.Lock()
	err = errors.Join(err, a.containmentErr)
	a.mu.Unlock()

	return err
}

func (a *Agent) beginActiveReuse(ctx context.Context, id acp.SessionId) (*session, context.Context, func(), error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return nil, nil, nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	existing := a.sessions[id]
	a.mu.Unlock()
	if existing == nil {
		return nil, nil, nil, nil
	}

	admissionCtx, release, err := existing.beginReuse(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if hook, ok := ctx.Value(activeReuseAdmissionHookKey{}).(func(context.Context)); ok {
		hook(admissionCtx)
	}

	return existing, admissionCtx, release, nil
}

type activeReuseAdmissionHookKey struct{}

func (a *Agent) completeActiveReuse(
	ctx context.Context,
	id acp.SessionId,
	existing *session,
	replay bool,
	release func(),
) (*deferredStreamOpen, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed || context.Cause(ctx) != nil {
		return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}
	if a.sessions[id] != existing {
		return nil, unknownSessionError()
	}

	owed, deferred, err := a.deferStreamOpenLocked(ctx, existing)
	if err != nil {
		return nil, err
	}

	identity, orderedResponse := ctx.Value(lifecycleRequestIdentityKey{}).(lifecycleRequestIdentity)
	if deferred && orderedResponse && identity.token != "" {
		owed.afterResponse = func() {
			var replayErr error
			if replay {
				replayErr = existing.replayMessages(ctx)
			}
			release()
			if replayErr != nil {
				existing.failReuseAfterResponse(replayErr)
			}
		}
		owed.onCancel = release
	}

	return owed, nil
}

func (a *Agent) beginSessionConstruction(ctx context.Context) (context.Context, func(), error) {
	a.mu.Lock()

	if a.closed {
		a.mu.Unlock()

		return nil, nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}
	if len(a.sessions)+a.constructing >= a.options.ConcurrencyLimits.MaxActiveSessions {
		a.mu.Unlock()

		return nil, nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: valBackpressure, keyLimit: "active_sessions"})
	}

	a.constructionSeq++
	constructionID := a.constructionSeq
	constructionCtx, cancel := context.WithCancelCause(ctx)
	a.constructionCancel[constructionID] = cancel
	a.constructing++
	a.constructions.Add(1)
	a.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			a.mu.Lock()
			delete(a.constructionCancel, constructionID)
			a.constructing--
			a.mu.Unlock()
			cancel(nil)
			a.constructions.Done()
		})
	}

	return constructionCtx, release, nil
}

// optionsError reports a construction-time option failure as the uniform
// internal error, or nil when every option validated. The code is -32603
// because the caller's params are blameless: the embedding host built an agent
// this adapter refuses, so no request it can phrase would be served. The data
// carries only the joined validation prose, since no wire field is at fault to
// name.
func (a *Agent) optionsError() error {
	if a.optionsErr == nil {
		return nil
	}

	return acp.NewInternalError(map[string]any{jsonFieldError: a.optionsErr.Error()})
}

func (a *Agent) Initialize(_ context.Context, params acp.InitializeRequest) (acp.InitializeResponse, error) {
	if err := a.optionsError(); err != nil {
		return acp.InitializeResponse{}, err
	}

	// The lifecycle answer is resolved before anything else this handshake
	// records, so a malformed offer is refused before the connection adopts a
	// client capability set it would then have to unwind.
	lifecycleMeta, err := a.negotiateLifecycle(params.Meta)
	if err != nil {
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
			"scope":    capabilityScopeSession,
			"tracks":   "ACP v1 elicitation",
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
	capabilityMeta[routeMetaKey] = map[string]any{keyVersion: routeVersion}

	return acp.InitializeResponse{
		Meta:            lifecycleMeta,
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

// Authenticate advertises no method, so every call is refused. The reserved
// lifecycle literal is inspected first: a request naming a key this surface
// never carries is malformed before it is unauthenticated.
func (a *Agent) Authenticate(_ context.Context, params acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	if err := rejectLifecycleMeta(params.Meta); err != nil {
		return acp.AuthenticateResponse{}, err
	}

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

func (a *Agent) Logout(_ context.Context, params acp.LogoutRequest) (acp.LogoutResponse, error) {
	if err := rejectLifecycleMeta(params.Meta); err != nil {
		return acp.LogoutResponse{}, err
	}

	return acp.LogoutResponse{}, nil
}

// SetSessionMode carries no native mode surface, so it answers method-not-found.
// The lifecycle refusal still precedes that answer, for the same reason
// Authenticate's does.
func (a *Agent) SetSessionMode(_ context.Context, params acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	if err := rejectLifecycleMeta(params.Meta); err != nil {
		return acp.SetSessionModeResponse{}, err
	}

	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, error) {
	if err := a.ensureOpen(); err != nil {
		return nil, err
	}

	// Every extension route inspects the reserved lifecycle literal before its
	// own side effects or its own refusal. An unconfigured provider-auth leg
	// answers method-not-found, and a family literal is never foreign, so the
	// refusal that names the key has to come first or a host would learn the
	// leg is absent instead of learning its request was malformed.
	if err := rejectLifecycleRawMeta(params); err != nil {
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
	if lease, ok := ctx.Value(clientCallLeaseKey{}).(*clientCallLease); ok && lease.agent == a {
		return func() {}, nil
	}

	select {
	case a.clientCalls <- struct{}{}:
		return func() { <-a.clientCalls }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: valBackpressure, keyLimit: "client_calls"})
	}
}

type clientCallLeaseKey struct{}

type clientCallLease struct {
	agent *Agent
}

func (a *Agent) beginClientOperation(ctx context.Context) (context.Context, func(), error) {
	if lease, ok := ctx.Value(clientCallLeaseKey{}).(*clientCallLease); ok && lease.agent == a {
		return ctx, func() {}, nil
	}

	release, err := a.acquireClientCall(ctx)
	if err != nil {
		return nil, nil, err
	}

	return context.WithValue(ctx, clientCallLeaseKey{}, &clientCallLease{agent: a}), release, nil
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

// storeStartedSession publishes one fully prepared session under its id, and it
// is the only place an id becomes live. Every reason the id may not be published
// is re-read here, under the lock that installs it: the entry checks ran before a
// hydration, an ownership acquisition, and a `hermes serve` launch that take as
// long as they take, so a verdict reached there is only a guess by the time there
// is something to install.
//
// The deletion tombstone is the reason that guess is load-bearing. A delete that
// completes while a load or resume is preparing wins, however far the preparation
// got: the marker is re-read here and the replacement is refused, and the marker
// is never cleared as a side effect of installing. Clearing it would un-hide the
// id for every later reader and let the next publish rewrite the very row the
// delete removed, since the durable publish guard is that same marker.
func (a *Agent) storeStartedSession(session *session) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.storeStartedSessionLocked(session)
}

func (a *Agent) storeStartedSessionWithOpening(ctx context.Context, session *session) (*deferredStreamOpen, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	owed, _, err := a.deferStreamOpenLocked(ctx, session)
	if err != nil {
		return nil, err
	}
	if err := a.storeStartedSessionLocked(session); err != nil {
		a.abandonStreamOpen(owed)

		return nil, err
	}

	return owed, nil
}

func (a *Agent) storeStartedSessionLocked(session *session) error {
	if a.closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	if _, deleted := a.deleted[session.id]; deleted {
		return unknownSessionError()
	}

	if len(a.sessions) >= a.options.ConcurrencyLimits.MaxActiveSessions {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: valBackpressure, keyLimit: "active_sessions"})
	}

	a.sessions[session.id] = session

	// This is the one place an id becomes live, so it is where the provider-auth
	// closed mark is cleared. session/close leaves the durable snapshot in place,
	// so the same id can be hydrated again, and a closed mark that outlived the
	// reopen would refuse every leg on it for the agent's life.
	if a.providerAuth != nil {
		a.providerAuth.reopenSession(session.id)
	}

	a.observe.AddActiveSession(context.Background(), 1)

	return nil
}

// refuseStartedSession tears down a fully prepared session the install lock
// refused and answers with the refusal itself. Nothing else can reach that
// session — it never became live, so no id names it and no close will ever be
// addressed to it — which is why the launched process, its scratch generation,
// and its ownership claims are released here.
//
// The refusal is returned unwrapped: the SDK maps a handler error onto its
// JSON-RPC error by type assertion, so joining a teardown result into it would
// answer a tombstoned id with an internal error rather than the uniform
// unknown-session error every other door gives it. A teardown that could not
// prove containment is recorded on the agent, which is where that verdict is
// reported from.
func (a *Agent) refuseStartedSession(ctx context.Context, session *session, refusal error) error {
	closeErr := session.Close(context.Background())

	a.recordIncompleteContainment(closeErr, session.id, hermesServerRoot(session.client))
	a.log.DebugContext(ctx, "close a Hermes session the install refused",
		slog.String(jsonFieldError, refusal.Error()),
		slog.Any(jsonFieldCause, closeErr),
	)

	return refusal
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

	return caps.Form != nil
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
