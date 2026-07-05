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
)

const (
	listSessionsPageSize            = 50
	defaultMaxActiveSessions        = 32
	defaultMaxConcurrentPrompts     = 1
	defaultMaxConcurrentClientCalls = 16
	closeTimeout                    = 5 * time.Second
)

var (
	agentJSONMarshal   = json.Marshal
	agentJSONUnmarshal = json.Unmarshal
	newAgentForServe   = NewAgent
)

// Agent exposes Hermes through ACP.
type Agent struct {
	options    Options
	log        *slog.Logger
	optionsErr error

	mu                 sync.Mutex
	closed             bool
	conn               agentClient
	sessions           map[acp.SessionId]*session
	deleted            map[acp.SessionId]struct{}
	deleteCleanup      map[acp.SessionId]deleteCleanupRecord
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
	options.ConcurrencyLimits = limits

	log := options.Logger
	if log == nil {
		log = slog.Default()
	}
	if options.SessionStore == nil {
		options.SessionStore = NewInMemorySessionStore()
	}

	return &Agent{
		options:       options,
		log:           log,
		optionsErr:    optionsErr,
		sessions:      make(map[acp.SessionId]*session),
		deleted:       make(map[acp.SessionId]struct{}),
		deleteCleanup: make(map[acp.SessionId]deleteCleanupRecord),
		clientCalls:   make(chan struct{}, limits.MaxConcurrentClientCalls),
	}
}

func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	agent := newAgentForServe(opts...)
	defer func() {
		if err := agent.Close(); err != nil {
			agent.log.DebugContext(context.Background(), "close Hermes ACP agent failed", slog.String("error", err.Error()))
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

func (a *Agent) Close() error {
	a.mu.Lock()
	sessions := make([]*session, 0, len(a.sessions))
	for _, session := range a.sessions {
		sessions = append(sessions, session)
	}
	a.sessions = make(map[acp.SessionId]*session)
	a.closed = true
	a.conn = nil
	a.mu.Unlock()

	var err error
	for _, session := range sessions {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		err = errors.Join(err, session.Close(ctx))
		cancel()
	}

	return err
}

func (a *Agent) Initialize(_ context.Context, params acp.InitializeRequest) (acp.InitializeResponse, error) {
	if a.optionsErr != nil {
		return acp.InitializeResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldError: a.optionsErr.Error()})
	}
	title := a.options.AgentTitle
	positionEncoding := selectPositionEncoding(params.ClientCapabilities.PositionEncodings)
	a.mu.Lock()
	a.clientCapabilities = cloneClientCapabilities(params.ClientCapabilities)
	a.positionEncoding = positionEncoding
	a.mu.Unlock()

	hermesMeta := map[string]any{
		"fork": map[string]any{
			"unstable": true,
			"method":   ForkSessionMethod,
			"request":  "acp.UnstableForkSessionRequest JSON payload only",
			"response": "acp.UnstableForkSessionResponse JSON payload only",
		},
		"elicitation": map[string]any{
			"unstable": true,
			"scope":    "session",
			"tracks":   "in-progress ACP elicitation RFD",
		},
		rawEventCapabilityKey: map[string]any{
			"method":         RawEventMethod,
			"enabledBy":      rawEventEnabledByPath,
			"maxBytes":       rawEventMaxBytes,
			"defaultEnabled": false,
		},
		"sessionStore": map[string]any{
			"format": SessionStoreFormat,
			"key":    []string{"sessionId", "subpath"},
		},
	}

	return acp.InitializeResponse{
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			Meta:        map[string]any{hermesMetaKey: hermesMeta},
			LoadSession: true,
			McpCapabilities: acp.McpCapabilities{
				Http: true,
			},
			PositionEncoding: &positionEncoding,
			PromptCapabilities: acp.PromptCapabilities{
				Image:           true,
				EmbeddedContext: true,
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
		return nil, acp.NewMethodNotFound(method)
	}
}

func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: "agent closed"})
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
		return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: "backpressure", "limit": "client_calls"})
	}
}

func (a *Agent) session(id acp.SessionId) (*session, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.deleted[id]; ok {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldSessionID: "deleted"})
	}
	session := a.sessions[id]
	if session == nil {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldSessionID: "unknown"})
	}

	return session, nil
}

func (a *Agent) storeStartedSession(session *session) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: "agent closed"})
	}
	if len(a.sessions) >= a.options.ConcurrencyLimits.MaxActiveSessions {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: "backpressure", "limit": "active_sessions"})
	}
	a.sessions[session.id] = session
	delete(a.deleted, session.id)

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
