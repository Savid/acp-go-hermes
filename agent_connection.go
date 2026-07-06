package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/acp-go-sdk"
)

type agentClient interface {
	Done() <-chan struct{}
	CreateElicitation(context.Context, acp.UnstableCreateElicitationRequest, elicitationScope) (acp.UnstableCreateElicitationResponse, error)
	UnstableCreateElicitation(context.Context, acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error)
	RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error)
	SessionUpdate(context.Context, acp.SessionNotification) error
	NotifyExtension(context.Context, string, any) error
}

type elicitationScope struct {
	SessionID  acp.SessionId
	ToolCallID acp.ToolCallId
	RequestID  *acp.RequestId
}

type localAgentConnection struct {
	agent       *Agent
	conn        *acp.Connection
	initialized atomic.Bool
}

type localAgentHandler func(context.Context, *Agent, json.RawMessage) (any, *acp.RequestError)

type localAgentParams[Req any] interface {
	*Req
	Validate() error
}

var (
	_ agentClient = (*localAgentConnection)(nil)

	localAgentHandlers = map[string]localAgentHandler{
		acp.AgentMethodAuthenticate:           localResponse((*Agent).Authenticate),
		acp.AgentMethodInitialize:             localResponse((*Agent).Initialize),
		acp.AgentMethodLogout:                 localResponse((*Agent).Logout),
		acp.AgentMethodSessionCancel:          localNotification((*Agent).Cancel),
		acp.AgentMethodSessionClose:           localResponse((*Agent).CloseSession),
		acp.AgentMethodSessionDelete:          localResponse((*Agent).UnstableDeleteSession),
		acp.AgentMethodSessionList:            localResponse((*Agent).ListSessions),
		acp.AgentMethodSessionLoad:            localLifecycleResponse((*Agent).LoadSession),
		acp.AgentMethodSessionNew:             localLifecycleResponse((*Agent).NewSession),
		acp.AgentMethodSessionPrompt:          localResponse((*Agent).Prompt),
		acp.AgentMethodSessionResume:          localLifecycleResponse((*Agent).ResumeSession),
		acp.AgentMethodSessionSetConfigOption: localResponse((*Agent).SetSessionConfigOption),
		acp.AgentMethodSessionSetMode:         localResponse((*Agent).SetSessionMode),
	}
)

func newLocalAgentConnection(agent *Agent, output io.Writer, input io.Reader) *localAgentConnection {
	conn := &localAgentConnection{agent: agent}
	inputGate := newConnectionInputGate(input)
	conn.conn = acp.NewConnection(conn.handle, output, inputGate)
	conn.conn.SetLogger(agent.log)
	inputGate.open()

	return conn
}

type connectionInputGate struct {
	reader io.Reader
	ready  chan struct{}
	once   sync.Once
}

func newConnectionInputGate(reader io.Reader) *connectionInputGate {
	return &connectionInputGate{reader: reader, ready: make(chan struct{})}
}

func (g *connectionInputGate) open() {
	g.once.Do(func() { close(g.ready) })
}

func (g *connectionInputGate) Read(p []byte) (int, error) {
	<-g.ready

	return g.reader.Read(p)
}

func (c *localAgentConnection) Done() <-chan struct{} {
	return c.conn.Done()
}

func (c *localAgentConnection) handle(ctx context.Context, method string, params json.RawMessage) (any, *acp.RequestError) {
	if method != acp.AgentMethodInitialize && !c.initialized.Load() {
		return nil, acp.NewInvalidRequest(map[string]any{
			jsonFieldMethod: method,
			jsonFieldError:  "initialize must be called before other ACP methods",
		})
	}

	if strings.HasPrefix(method, "_") {
		result, err := c.agent.HandleExtensionMethod(ctx, method, params)

		return result, requestError(err)
	}

	handler, ok := localAgentHandlers[method]
	if !ok {
		return nil, acp.NewMethodNotFound(method)
	}

	result, reqErr := handler(ctx, c.agent, params)
	if method == acp.AgentMethodInitialize && reqErr == nil {
		c.initialized.Store(true)
	}

	return result, reqErr
}

func localResponse[Req any, ReqPtr localAgentParams[Req], Resp any](
	call func(*Agent, context.Context, Req) (Resp, error),
) localAgentHandler {
	return func(ctx context.Context, agent *Agent, params json.RawMessage) (any, *acp.RequestError) {
		value, reqErr := decodeLocalAgentParams[Req, ReqPtr](params)
		if reqErr != nil {
			return nil, reqErr
		}

		resp, err := call(agent, ctx, value)
		if err != nil {
			return nil, requestError(err)
		}

		return resp, nil
	}
}

func localLifecycleResponse[Req any, ReqPtr localAgentParams[Req], Resp any](
	call func(*Agent, context.Context, Req) (Resp, error),
) localAgentHandler {
	return func(ctx context.Context, agent *Agent, params json.RawMessage) (any, *acp.RequestError) {
		value, reqErr := decodeLocalAgentParams[Req, ReqPtr](params)
		if reqErr != nil {
			return nil, reqErr
		}

		resp, err := call(agent, ctx, value)
		if err != nil {
			return nil, requestError(err)
		}

		return resp, nil
	}
}

func localNotification[Req any, ReqPtr localAgentParams[Req]](
	call func(*Agent, context.Context, Req) error,
) localAgentHandler {
	return func(ctx context.Context, agent *Agent, params json.RawMessage) (any, *acp.RequestError) {
		value, reqErr := decodeLocalAgentParams[Req, ReqPtr](params)
		if reqErr != nil {
			return nil, reqErr
		}

		if err := call(agent, ctx, value); err != nil {
			return nil, requestError(err)
		}

		return nil, nil
	}
}

func decodeLocalAgentParams[Req any, ReqPtr localAgentParams[Req]](params json.RawMessage) (Req, *acp.RequestError) {
	var value Req
	if err := json.Unmarshal(params, &value); err != nil {
		return value, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	if err := ReqPtr(&value).Validate(); err != nil {
		return value, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	return value, nil
}

func (c *localAgentConnection) UnstableCreateElicitation(
	ctx context.Context,
	params acp.UnstableCreateElicitationRequest,
) (acp.UnstableCreateElicitationResponse, error) {
	return c.CreateElicitation(ctx, params, elicitationScope{})
}

func (c *localAgentConnection) CreateElicitation(
	ctx context.Context,
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	raw, err := scopedElicitationParams(params, scope)
	if err != nil {
		return acp.UnstableCreateElicitationResponse{}, err
	}

	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return acp.UnstableCreateElicitationResponse{}, err
	}
	defer release()

	return acp.SendRequest[acp.UnstableCreateElicitationResponse](c.conn, ctx, acp.ClientMethodElicitationCreate, raw)
}

func (c *localAgentConnection) RequestPermission(
	ctx context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return acp.RequestPermissionResponse{}, err
	}
	defer release()

	return acp.SendRequest[acp.RequestPermissionResponse](c.conn, ctx, acp.ClientMethodSessionRequestPermission, params)
}

func (c *localAgentConnection) SessionUpdate(ctx context.Context, params acp.SessionNotification) error {
	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()

	return c.conn.SendNotification(ctx, acp.ClientMethodSessionUpdate, params)
}

func (c *localAgentConnection) NotifyExtension(ctx context.Context, method string, params any) error {
	if method == "" || !strings.HasPrefix(method, "_") {
		return fmt.Errorf("extension method name must start with '_' (got %q)", method)
	}

	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()

	return c.conn.SendNotification(ctx, method, params)
}

func requestError(err error) *acp.RequestError {
	if err == nil {
		return nil
	}

	var reqErr *acp.RequestError
	if errors.As(err, &reqErr) {
		return reqErr
	}

	if errors.Is(err, context.Canceled) {
		return acp.NewRequestCancelled(map[string]any{jsonFieldError: err.Error()})
	}

	return acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
}

func scopedElicitationParams(
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (json.RawMessage, error) {
	var payload map[string]any

	switch {
	case params.Form != nil:
		payload = map[string]any{
			jsonFieldMessage:  params.Form.Message,
			keyMode:           valForm,
			"requestedSchema": params.Form.RequestedSchema,
		}
		if len(params.Form.Meta) > 0 {
			payload["_meta"] = params.Form.Meta
		}
	case params.Url != nil:
		payload = map[string]any{
			"elicitationId":  params.Url.ElicitationId,
			jsonFieldMessage: params.Url.Message,
			keyMode:          valURL,
			valURL:           params.Url.Url,
		}
		if len(params.Url.Meta) > 0 {
			payload["_meta"] = params.Url.Meta
		}
	default:
		return nil, errors.New("elicitation request must include form or url")
	}

	if scope.SessionID != "" {
		payload[jsonFieldSessionID] = scope.SessionID
	}

	if scope.ToolCallID != "" {
		payload["toolCallId"] = scope.ToolCallID
	}

	if scope.RequestID != nil {
		payload["requestId"] = scope.RequestID
	}

	return json.Marshal(payload)
}
