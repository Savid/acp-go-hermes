package hermesacp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/acp-go-sdk"
)

// Elicitation request shape vocabulary shared with the prompt mapping.
const (
	keyMode = "mode"
	valForm = "form"
	valURL  = "url"
)

type agentClient interface {
	Done() <-chan struct{}
	CreateElicitation(context.Context, acp.UnstableCreateElicitationRequest, elicitationScope) (acp.UnstableCreateElicitationResponse, error)
	CreateElicitationRegistered(context.Context, acp.UnstableCreateElicitationRequest, elicitationScope, chan<- error) (acp.UnstableCreateElicitationResponse, error)
	UnstableCreateElicitation(context.Context, acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error)
	RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error)
	RequestPermissionRegistered(context.Context, acp.RequestPermissionRequest, chan<- error) (acp.RequestPermissionResponse, error)
	SessionUpdate(context.Context, acp.SessionNotification) error
	NotifyExtension(context.Context, string, any) error
}

type elicitationScope struct {
	SessionID  acp.SessionId
	TurnNonce  string
	ToolCallID acp.ToolCallId
	RequestID  *string
}

type localAgentConnection struct {
	agent       *Agent
	conn        *acp.Connection
	writer      *responseOrderedWriter
	initialized atomic.Bool

	outboundMu     sync.Mutex
	outboundWrites map[string]*hostWriteRegistration
	inputGate      *connectionInputGate
}

type lifecycleRequestIdentity struct {
	token string
}

type lifecycleRequestIdentityKey struct{}

const lifecycleRequestMarkerField = "_acpGoHermesLifecycleRequestMarker"

type hostWriteRegistration struct {
	written chan<- error
	once    sync.Once
}

func (r *hostWriteRegistration) resolve(err error) {
	if r == nil || r.written == nil {
		return
	}

	r.once.Do(func() { r.written <- err })
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
		acp.AgentMethodSessionLoad:            localResponse((*Agent).LoadSession),
		acp.AgentMethodSessionNew:             localResponse((*Agent).NewSession),
		acp.AgentMethodSessionPrompt:          localResponse((*Agent).Prompt),
		acp.AgentMethodSessionResume:          localResponse((*Agent).ResumeSession),
		acp.AgentMethodSessionSetConfigOption: localResponse((*Agent).SetSessionConfigOption),
		acp.AgentMethodSessionSetMode:         localResponse((*Agent).SetSessionMode),
	}
)

func newLocalAgentConnection(agent *Agent, output io.Writer, input io.Reader) *localAgentConnection {
	conn := &localAgentConnection{agent: agent}
	inputGate := newConnectionInputGate(input)
	conn.inputGate = inputGate
	ordered := &responseOrderedWriter{
		writer:   output,
		starting: agent.beginStreamOpenWrite,
		completed: func(frame []byte, err error) {
			agent.completeStreamOpenWrite(frame, err)

			if err == nil {
				conn.resolveOutboundWrite(frame)
			}
		},
	}
	conn.writer = ordered
	conn.conn = acp.NewConnection(conn.handle, ordered, inputGate)
	conn.conn.SetLogger(protocolSafeLogger(agent.log))
	inputGate.open()

	return conn
}

type connectionInputGate struct {
	reader  *bufio.Reader
	ready   chan struct{}
	once    sync.Once
	pending []byte

	mu         sync.Mutex
	nextToken  uint64
	requestIDs map[string]struct{}
}

const connectionInputFrameLimit = 10 * 1024 * 1024

var (
	errConnectionInputOversized    = errors.New("ACP input frame exceeds the 10 MiB protocol limit")
	errConnectionInputUnterminated = errors.New("ACP input frame is not newline terminated")
)

func newConnectionInputGate(reader io.Reader) *connectionInputGate {
	return &connectionInputGate{
		reader:     bufio.NewReader(reader),
		ready:      make(chan struct{}),
		requestIDs: make(map[string]struct{}),
	}
}

func (g *connectionInputGate) open() {
	g.once.Do(func() { close(g.ready) })
}

func (g *connectionInputGate) Read(p []byte) (int, error) {
	<-g.ready

	if len(g.pending) == 0 {
		line, err := g.readFrame()
		if err != nil {
			return 0, err
		}

		g.pending, err = g.stampLifecycleRequest(line)
		if err != nil {
			return 0, err
		}
	}

	n := copy(p, g.pending)
	g.pending = g.pending[n:]

	return n, nil
}

// readFrame incrementally assembles one newline-delimited ACP frame without
// allowing bufio.Reader.ReadBytes to allocate an unbounded attacker-controlled
// slice. Partial EOF is rejected rather than repaired with a newline: no
// lifecycle token can be minted and no handler can open a session from an
// unterminated body.
func (g *connectionInputGate) readFrame() ([]byte, error) {
	frame := make([]byte, 0, g.reader.Size())

	for {
		fragment, err := g.reader.ReadSlice('\n')
		if len(frame)+len(fragment) > connectionInputFrameLimit {
			return nil, errConnectionInputOversized
		}

		frame = append(frame, fragment...)

		switch {
		case err == nil:
			return frame, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF) && len(frame) == 0:
			return nil, io.EOF
		case errors.Is(err, io.EOF):
			return nil, errConnectionInputUnterminated
		default:
			return nil, err
		}
	}
}

func (g *connectionInputGate) stampLifecycleRequest(line []byte) ([]byte, error) {
	var envelope struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if json.Unmarshal(line, &envelope) != nil || len(bytes.TrimSpace(envelope.ID)) == 0 ||
		bytes.Equal(bytes.TrimSpace(envelope.ID), []byte("null")) || !lifecycleResponseMethod(envelope.Method) {
		return line, nil //nolint:nilerr // Malformed frames remain the SDK parser's responsibility.
	}

	var params map[string]json.RawMessage
	if json.Unmarshal(envelope.Params, &params) != nil || params == nil {
		return line, nil //nolint:nilerr // Invalid params remain the SDK/handler parser's responsibility.
	}

	g.mu.Lock()
	g.nextToken++
	token := fmt.Sprintf("lifecycle-request-%d", g.nextToken)
	g.mu.Unlock()

	encodedToken, _ := json.Marshal(token) // A Go string always has a JSON encoding.
	params[lifecycleRequestMarkerField] = encodedToken

	var object map[string]json.RawMessage

	_ = json.Unmarshal(line, &object) // The envelope decode above proved a JSON object with an id and method.

	encodedParams, _ := json.Marshal(params) // Every member came from the already-valid input object, plus one string.

	object["params"] = encodedParams

	stamped, _ := json.Marshal(object) // The object contains only already-valid raw members.

	stamped = append(stamped, '\n')

	if len(stamped) > connectionInputFrameLimit {
		return nil, errConnectionInputOversized
	}

	g.mu.Lock()
	g.requestIDs[token] = struct{}{}
	g.mu.Unlock()

	return stamped, nil
}

func (g *connectionInputGate) claimLifecycleRequest(token string) (lifecycleRequestIdentity, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	_, ok := g.requestIDs[token]
	delete(g.requestIDs, token)

	return lifecycleRequestIdentity{token: token}, ok
}

func lifecycleResponseMethod(method string) bool {
	switch method {
	case acp.AgentMethodSessionNew, acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume, ForkSessionMethod:
		return true
	default:
		return false
	}
}

func (c *localAgentConnection) bindLifecycleRequest(ctx context.Context, params *json.RawMessage) context.Context {
	if c == nil || c.inputGate == nil || params == nil {
		return ctx
	}

	var object map[string]json.RawMessage
	if json.Unmarshal(*params, &object) != nil {
		return ctx
	}

	var token string

	_ = json.Unmarshal(object[lifecycleRequestMarkerField], &token)
	if token == "" {
		return ctx
	}

	delete(object, lifecycleRequestMarkerField)

	// Every RawMessage came from the successfully decoded object above, so
	// deleting the private member cannot make the remaining object invalid.
	clean, _ := json.Marshal(object)

	*params = clean

	identity, ok := c.inputGate.claimLifecycleRequest(token)
	if !ok {
		return ctx
	}

	return context.WithValue(ctx, lifecycleRequestIdentityKey{}, identity)
}

type protocolSafeHandler struct {
	target slog.Handler
}

func protocolSafeLogger(target *slog.Logger) *slog.Logger {
	if target == nil {
		target = slog.Default()
	}

	return slog.New(protocolSafeHandler{target: target.Handler()})
}

func (h protocolSafeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.target.Enabled(ctx, level)
}

func (h protocolSafeHandler) Handle(ctx context.Context, record slog.Record) error {
	safe := slog.NewRecord(record.Time, record.Level, "Hermes ACP protocol diagnostic", record.PC)
	safe.AddAttrs(slog.String("classification", protocolLogClassification(record.Message)))

	return h.target.Handle(ctx, safe)
}

func (h protocolSafeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return protocolSafeHandler{target: h.target}
}

func (h protocolSafeHandler) WithGroup(name string) slog.Handler {
	return protocolSafeHandler{target: h.target}
}

func protocolLogClassification(message string) string {
	switch message {
	case "failed to parse incoming message":
		return "malformed_frame"
	case "connection closed":
		return "connection_closed"
	default:
		return "protocol_failure"
	}
}

func (c *localAgentConnection) Done() <-chan struct{} {
	return c.conn.Done()
}

func (c *localAgentConnection) CloseTransport(ctx context.Context) error {
	return c.writer.Close(ctx)
}

func (c *localAgentConnection) PrepareTransportClose(ctx context.Context) error {
	return c.writer.PrepareClose(ctx)
}

func (c *localAgentConnection) handle(ctx context.Context, method string, params json.RawMessage) (result any, reqErr *acp.RequestError) {
	ctx = c.bindLifecycleRequest(ctx, &params)
	ctx, finish := c.agent.observe.StartACPRequest(ctx, method)

	defer func() {
		if reqErr != nil {
			finish(reqErr)
		} else {
			finish(nil)
		}
	}()

	if err := c.agent.ensureOpen(); err != nil {
		reqErr = requestError(ctx, err)

		return nil, reqErr
	}

	if method != acp.AgentMethodInitialize && !c.initialized.Load() {
		reqErr = acp.NewInvalidRequest(map[string]any{
			jsonFieldMethod: method,
			jsonFieldError:  "initialize must be called before other ACP methods",
		})

		return nil, reqErr
	}

	if strings.HasPrefix(method, "_") {
		var err error

		result, err = c.agent.HandleExtensionMethod(ctx, method, params)
		reqErr = requestError(ctx, err)

		if reqErr == nil && lifecycleResponseMethod(method) {
			result, err = markLifecycleResponse(ctx, result)

			reqErr = requestError(ctx, err)
		}

		return result, reqErr
	}

	handler, ok := localAgentHandlers[method]
	if !ok {
		reqErr = acp.NewMethodNotFound(method)

		return nil, reqErr
	}

	result, reqErr = handler(ctx, c.agent, params)
	if reqErr == nil && lifecycleResponseMethod(method) {
		var err error

		result, err = markLifecycleResponse(ctx, result)
		reqErr = requestError(ctx, err)
	}

	if method == acp.AgentMethodInitialize && reqErr == nil {
		c.initialized.Store(true)
	}

	return result, reqErr
}

// markLifecycleResponse carries the exact private request token from handler
// admission to the response writer. The writer removes the marker before the
// frame reaches the peer; its only purpose is to keep two concurrently
// outstanding equal JSON-RPC ids from releasing each other's opening snapshot.
func markLifecycleResponse(ctx context.Context, response any) (any, error) {
	identity, exact := ctx.Value(lifecycleRequestIdentityKey{}).(lifecycleRequestIdentity)
	if !exact || identity.token == "" {
		return response, nil
	}

	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("marshal lifecycle response: %w", err)
	}

	var object map[string]json.RawMessage
	if decodeErr := json.Unmarshal(encoded, &object); decodeErr != nil || object == nil {
		return nil, errors.New("lifecycle response must be a JSON object")
	}

	marker, _ := json.Marshal(identity.token) // A non-empty Go string always has a JSON encoding.

	object[lifecycleRequestMarkerField] = marker

	return object, nil
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
			return nil, requestError(ctx, err)
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
			return nil, requestError(ctx, err)
		}

		return nil, nil
	}
}

// decodeLocalAgentParams refuses a params object this connection cannot decode
// or that fails its own validation as a whole. Both are one verdict about one
// member -- the request's `params` -- so both take the uniform {error, field}
// refusal every other inbound shape takes, and neither carries decoder prose: a
// syntax error quotes the offending byte of the request back at the peer.
func decodeLocalAgentParams[Req any, ReqPtr localAgentParams[Req]](params json.RawMessage) (Req, *acp.RequestError) {
	var value Req
	if err := json.Unmarshal(params, &value); err != nil {
		return value, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, keyField: keyParams})
	}

	if err := ReqPtr(&value).Validate(); err != nil {
		return value, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, keyField: keyParams})
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
	return c.CreateElicitationRegistered(ctx, params, scope, nil)
}

func (c *localAgentConnection) CreateElicitationRegistered(
	ctx context.Context,
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
	written chan<- error,
) (acp.UnstableCreateElicitationResponse, error) {
	raw, err := scopedElicitationParams(params, scope)
	if err != nil {
		signalHostWrite(written, err)

		return acp.UnstableCreateElicitationResponse{}, err
	}

	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		signalHostWrite(written, err)

		return acp.UnstableCreateElicitationResponse{}, err
	}
	defer release()

	key := hostControlRequestKey(acp.ClientMethodElicitationCreate, string(scope.SessionID), pointedString(scope.RequestID))

	registration, err := c.registerOutboundWrite(key, written)
	if err != nil {
		signalHostWrite(written, err)

		return acp.UnstableCreateElicitationResponse{}, err
	}
	defer c.finishOutboundWrite(key, registration)

	response, err := acp.SendRequest[acp.UnstableCreateElicitationResponse](c.conn, ctx, acp.ClientMethodElicitationCreate, raw)
	registration.resolve(err)

	return response, err
}

func (c *localAgentConnection) RequestPermission(
	ctx context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	return c.RequestPermissionRegistered(ctx, params, nil)
}

func (c *localAgentConnection) RequestPermissionRegistered(
	ctx context.Context,
	params acp.RequestPermissionRequest,
	written chan<- error,
) (acp.RequestPermissionResponse, error) {
	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		signalHostWrite(written, err)

		return acp.RequestPermissionResponse{}, err
	}
	defer release()

	key := hostControlRequestKey(acp.ClientMethodSessionRequestPermission, string(params.SessionId), controlRequestID(params.Meta))

	registration, err := c.registerOutboundWrite(key, written)
	if err != nil {
		signalHostWrite(written, err)

		return acp.RequestPermissionResponse{}, err
	}
	defer c.finishOutboundWrite(key, registration)

	response, err := acp.SendRequest[acp.RequestPermissionResponse](c.conn, ctx, acp.ClientMethodSessionRequestPermission, params)
	registration.resolve(err)

	return response, err
}

func pointedString(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}

func signalHostWrite(written chan<- error, err error) {
	if written != nil {
		written <- err
	}
}

func controlRequestID(meta map[string]any) string {
	hermes, _ := meta[hermesMetaKey].(map[string]any)
	requestID, _ := hermes[routeFieldReq].(string)

	return requestID
}

func hostControlRequestKey(method string, sessionID string, requestID string) string {
	if method == "" || sessionID == "" || requestID == "" {
		return ""
	}

	return method + "\x00" + sessionID + "\x00" + requestID
}

func (c *localAgentConnection) registerOutboundWrite(key string, written chan<- error) (*hostWriteRegistration, error) {
	if written == nil {
		return &hostWriteRegistration{}, nil
	}

	if key == "" {
		return nil, errors.New("host control request is missing exact identity")
	}

	registration := &hostWriteRegistration{written: written}

	c.outboundMu.Lock()
	if c.outboundWrites == nil {
		c.outboundWrites = make(map[string]*hostWriteRegistration)
	}

	if c.outboundWrites[key] != nil {
		c.outboundMu.Unlock()

		return nil, errors.New("host control request identity is already registered")
	}

	c.outboundWrites[key] = registration
	c.outboundMu.Unlock()

	return registration, nil
}

func (c *localAgentConnection) finishOutboundWrite(key string, registration *hostWriteRegistration) {
	if registration == nil {
		return
	}

	c.outboundMu.Lock()
	if c.outboundWrites[key] == registration {
		delete(c.outboundWrites, key)
	}
	c.outboundMu.Unlock()
}

func (c *localAgentConnection) resolveOutboundWrite(frame []byte) {
	var envelope struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if json.Unmarshal(frame, &envelope) != nil || envelope.Method == "" {
		return
	}

	meta, _ := envelope.Params["_meta"].(map[string]any)
	sessionID, _ := envelope.Params["sessionId"].(string)
	requestID := controlRequestID(meta)

	if envelope.Method == acp.ClientMethodElicitationCreate {
		route, _ := meta[routeMetaKey].(map[string]any)
		sessionID, _ = route[routeFieldID].(string)
		requestID, _ = route[routeFieldReq].(string)
	}

	key := hostControlRequestKey(envelope.Method, sessionID, requestID)

	c.outboundMu.Lock()
	registration := c.outboundWrites[key]
	c.outboundMu.Unlock()
	registration.resolve(nil)
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

// requestError maps a handler failure onto the wire error the peer receives.
//
// An honored $/cancel_request is the only thing that cancels a request context
// with cause context.Canceled: connection teardown cancels the parent with the
// transport cause, and an adapter deadline yields context.DeadlineExceeded, so
// neither is ever reported as cancelled and a deadline stays an internal
// failure. The cause is therefore what identifies a cancel, and it is read
// ahead of the error: work aborted by a cancel routinely joins a typed
// RequestError on the way out, and reporting that instead of -32800 would
// answer a request the peer withdrew with an error about its parameters.
// errors.Is(err, context.Canceled) cannot make that distinction — it also
// matches a request that merely wrapped an unrelated cancellation — so it is
// deliberately not consulted.
func requestError(ctx context.Context, err error) *acp.RequestError {
	if err == nil {
		return nil
	}

	if context.Cause(ctx) == context.Canceled {
		return acp.NewRequestCancelled(map[string]any{jsonFieldError: valRequestCancelled})
	}

	var mapped interface{ requestError() *acp.RequestError }
	if errors.As(err, &mapped) {
		return mapped.requestError()
	}

	// A typed RequestError reaches the peer exactly as its construction site
	// phrased it. Every such site in this package builds a closed, values-free
	// payload -- a token from this package's own vocabulary plus, where one
	// applies, the dotted path of the offending field -- so the host can act on
	// the refusal without any native prose, tool input, or credential material
	// ever reaching the wire. The guarantee is held at construction rather than
	// by rewriting the payload here: flattening every code to a single token
	// also erased the field path, leaving a host unable to tell a malformed
	// option from a session that no longer exists.
	var reqErr *acp.RequestError
	if errors.As(err, &reqErr) {
		return reqErr
	}

	// An error carrying no wire classification is the only one whose prose is
	// unknown to this package, so it is the only one reduced to the
	// unclassified token.
	return acp.NewInternalError(map[string]any{jsonFieldError: valHermesInternalFailure})
}

func scopedElicitationParams(
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (json.RawMessage, error) {
	var (
		payload map[string]any
		meta    map[string]any
	)

	switch {
	case params.Form != nil:
		payload = map[string]any{
			jsonFieldMessage:  params.Form.Message,
			keyMode:           valForm,
			"requestedSchema": params.Form.RequestedSchema,
		}
		meta = params.Form.Meta
	case params.Url != nil:
		payload = map[string]any{
			"elicitationId":  params.Url.ElicitationId,
			jsonFieldMessage: params.Url.Message,
			keyMode:          valURL,
			valURL:           params.Url.Url,
		}
		meta = params.Url.Meta
	default:
		return nil, errors.New("elicitation request must include form or url")
	}

	stamped, err := stampRouteMeta(meta, scope)
	if err != nil {
		return nil, err
	}

	payload["_meta"] = stamped

	return json.Marshal(payload)
}
