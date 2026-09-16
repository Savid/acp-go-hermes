//nolint:tagliatelle // Hermes gateway JSON fields use snake_case names.
package hermes

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/coder/websocket"
)

const (
	jsonrpcVersion = "2.0"
	methodEvent    = "event"
	fieldSessionID = "session_id"
	fieldID        = "id"
	fieldJSONRPC   = "jsonrpc"
	fieldResult    = "result"

	// readLimitBytes caps a single inbound gateway frame. It must comfortably
	// exceed the advertised rawEvent maxBytes (64 KiB) so an oversize native
	// event is fully read and surfaced as the truncation marker rather than
	// killing the turn on a short read; it stays bounded (16 MiB) so a hostile
	// or wedged gateway cannot drive us out of memory with one giant frame.
	readLimitBytes = 16 * 1024 * 1024

	gatewayEventDeliveryCapacity = 256
)

type Client struct {
	conn       *websocket.Conn
	deliveries chan GatewayDelivery
	done       chan struct{}

	nextID   atomic.Int64
	sequence atomic.Uint64
	writeMu  sync.Mutex

	mu       sync.Mutex
	pending  map[int64]chan rpcResponse
	closed   bool
	terminal error
	close    *clientCloseAttempt
}

type clientCloseAttempt struct {
	done chan struct{}
	err  error
}

type Event struct {
	Type            string          `json:"type"`
	SessionID       string          `json:"session_id,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Raw             json.RawMessage `json:"-"`
	InboundSequence uint64          `json:"-"`
	RequestID       string          `json:"-"`
}

// GatewayDelivery is the gateway reader's single ordered output. A delivery
// contains exactly one event or terminal error; clean EOF closes the channel.
type GatewayDelivery struct {
	Event *Event
	Err   error
}

type rpcResponse struct {
	JSONRPC  string          `json:"jsonrpc"`
	ID       int64           `json:"id"`
	Result   json.RawMessage `json:"result,omitempty"`
	Error    *RPCError       `json:"error,omitempty"`
	sequence uint64
}

// decodeKnownRPCResponse validates the response shape before a pending call can
// observe it. Presence is checked from raw object members: omitempty-backed Go
// fields cannot distinguish an absent result from result:null, or an absent
// error from error:null, and those distinctions decide whether an action RPC
// may terminalize successfully.
func decodeKnownRPCResponse(data []byte, sequence uint64) (rpcResponse, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return rpcResponse{}, err
	}

	var response rpcResponse
	if err := json.Unmarshal(object[fieldJSONRPC], &response.JSONRPC); err != nil || response.JSONRPC != jsonrpcVersion {
		return rpcResponse{}, errors.New("hermes JSON-RPC response has invalid version")
	}

	if err := json.Unmarshal(object[fieldID], &response.ID); err != nil {
		return rpcResponse{}, fmt.Errorf("decode Hermes JSON-RPC response id: %w", err)
	}

	result, hasResult := object[fieldResult]
	errorValue, hasError := object["error"]

	if hasResult == hasError {
		return rpcResponse{}, errors.New("hermes JSON-RPC response requires exactly one of result or error")
	}

	if hasError {
		var rpcErr *RPCError
		if err := json.Unmarshal(errorValue, &rpcErr); err != nil {
			return rpcResponse{}, fmt.Errorf("decode Hermes JSON-RPC response error: %w", err)
		}

		if rpcErr == nil {
			return rpcResponse{}, errors.New("hermes JSON-RPC response error must be an object")
		}

		response.Error = rpcErr
	} else {
		response.Result = append(response.Result[:0], result...)
	}

	response.sequence = sequence

	return response, nil
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}

	return fmt.Sprintf("hermes json-rpc %d: %s", e.Code, e.Message)
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcErrorEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Error   RPCError        `json:"error"`
}

func Dial(ctx context.Context, url string, header http.Header) (*Client, error) {
	conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	if err != nil {
		return nil, err
	}

	// Raise the read limit above coder/websocket's 32 KiB default so a native
	// event larger than the advertised rawEvent cap is read in full and mapped
	// to the oversize marker instead of failing the read (and the turn).
	conn.SetReadLimit(readLimitBytes)

	client := &Client{
		conn:       conn,
		deliveries: make(chan GatewayDelivery, gatewayEventDeliveryCapacity+1),
		done:       make(chan struct{}),
		pending:    make(map[int64]chan rpcResponse),
	}
	go client.readLoop() //nolint:gosec // WebSocket reader owns the connection lifetime, not the dial context.

	return client, nil
}

func (c *Client) Deliveries() <-chan GatewayDelivery {
	return c.deliveries
}

func (c *Client) Close(status websocket.StatusCode, reason string) error {
	c.mu.Lock()
	if attempt := c.close; attempt != nil {
		c.mu.Unlock()
		<-attempt.done

		return attempt.err
	}

	attempt := &clientCloseAttempt{done: make(chan struct{})}
	c.close = attempt
	c.closed = true
	c.pending = map[int64]chan rpcResponse{}
	c.mu.Unlock()

	attempt.err = c.conn.Close(status, reason)

	c.mu.Lock()
	close(attempt.done)
	c.mu.Unlock()

	return attempt.err
}

func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	_, err := c.call(ctx, method, params, out)

	return err
}

// Reply answers a native server request with its original JSON-RPC identity.
func (c *Client) Reply(ctx context.Context, id string, result map[string]any) error {
	data, err := json.Marshal(map[string]any{fieldJSONRPC: jsonrpcVersion, fieldID: id, fieldResult: result})
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return c.conn.Write(ctx, websocket.MessageText, data)
}

func (c *Client) call(ctx context.Context, method string, params any, out any) (uint64, error) {
	watermark, _, _, err := c.callWithWriteState(ctx, method, params, out)

	return watermark, err
}

func (c *Client) callWithWriteState(ctx context.Context, method string, params any, out any) (uint64, bool, bool, error) {
	id := c.nextID.Add(1)
	respCh := make(chan rpcResponse, 1)

	c.mu.Lock()
	if c.closed {
		cause := c.terminal
		c.mu.Unlock()

		return 0, false, false, gatewayTransportCause(cause)
	}

	c.pending[id] = respCh

	c.mu.Unlock()
	defer c.forget(id)

	data, err := json.Marshal(rpcRequest{
		JSONRPC: jsonrpcVersion,
		ID:      id,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return 0, false, false, err
	}

	c.writeMu.Lock()
	err = c.conn.Write(ctx, websocket.MessageText, data)
	c.writeMu.Unlock()

	if err != nil {
		return 0, false, false, gatewayTransportCause(err)
	}

	resolve := func(resp rpcResponse) (uint64, bool, bool, error) {
		if resp.Error != nil {
			return resp.sequence, true, true, resp.Error
		}

		if out == nil {
			return resp.sequence, true, true, nil
		}

		if err := json.Unmarshal(resp.Result, out); err != nil {
			return resp.sequence, true, false, err
		}

		return resp.sequence, true, true, nil
	}

	select {
	case resp := <-respCh:
		return resolve(resp)
	case <-c.done:
		select {
		case resp := <-respCh:
			return resolve(resp)
		default:
		}

		return 0, true, false, gatewayTransportCause(c.terminalCause())
	case <-ctx.Done():
		return 0, true, false, ctx.Err()
	}
}

func (c *Client) forget(id int64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

// claimPending is the response linearization point. The first structurally
// classified response for an outstanding id atomically removes its waiter;
// duplicates and late responses therefore find no channel and can never block
// the sole reader.
func (c *Client) claimPending(id int64) chan rpcResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	waiter := c.pending[id]
	delete(c.pending, id)

	return waiter
}

func (c *Client) publishTerminal(err error) {
	c.mu.Lock()

	first := c.terminal == nil
	if c.terminal == nil {
		c.terminal = err
	}
	c.mu.Unlock()

	if !first {
		return
	}

	select {
	case c.deliveries <- GatewayDelivery{Err: err}:
	default:
	}
}

func (c *Client) terminalCause() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.terminal
}

func (c *Client) readLoop() {
	defer func() {
		c.mu.Lock()
		c.closed = true
		c.pending = map[int64]chan rpcResponse{}
		c.mu.Unlock()

		close(c.deliveries)
		close(c.done)
	}()

	for {
		typ, data, err := c.conn.Read(context.Background())
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return
			}

			c.publishTerminal(err)

			return
		}

		if typ != websocket.MessageText {
			continue
		}

		sequence := c.sequence.Add(1)

		var object map[string]json.RawMessage
		if err := json.Unmarshal(data, &object); err != nil || object == nil {
			if err == nil {
				err = errors.New("hermes gateway JSON-RPC frame must be an object")
			}

			c.publishTerminal(err)

			return
		}

		methodValue, hasMethod := object["method"]
		idValue, hasID := object[fieldID]

		if hasMethod {
			var method string
			if err := json.Unmarshal(methodValue, &method); err != nil || method == "" {
				c.publishTerminal(errors.New("hermes gateway JSON-RPC request has invalid method"))

				return
			}

			params, hasParams := object["params"]
			if (hasID && isServerRequest(method)) || (method == methodEvent && !hasID && hasParams) {
				event, err := decodeGatewayEvent(method, idValue, params)
				if err != nil {
					c.publishTerminal(err)

					return
				}

				event.Raw = bytes.Clone(data)
				event.InboundSequence = sequence

				if len(c.deliveries) >= cap(c.deliveries)-1 {
					c.publishTerminal(errors.New("hermes gateway input overflow"))

					return
				}

				c.deliveries <- GatewayDelivery{Event: &event}

				continue
			}

			// A method member makes this a peer-to-client request or notification,
			// even when its id collides with one of our outbound calls. It can never
			// satisfy that call. Requests receive an explicit Method Not Found;
			// unsupported notifications have no response by JSON-RPC definition.
			if hasID && !isNullJSON(idValue) {
				_ = c.rejectUnsupportedRequest(idValue)
			}

			continue
		}

		if hasID {
			var id int64
			if err := json.Unmarshal(idValue, &id); err != nil {
				c.publishTerminal(fmt.Errorf("decode Hermes JSON-RPC response id: %w", err))

				return
			}

			waiter := c.claimPending(id)
			if waiter == nil {
				continue
			}

			resp, responseErr := decodeKnownRPCResponse(data, sequence)
			if responseErr != nil {
				c.publishTerminal(responseErr)

				return
			}

			select {
			case waiter <- resp:
			default:
			}

			continue
		}

		c.publishTerminal(errors.New("hermes gateway JSON-RPC frame has neither method nor id"))

		return
	}
}

func decodeGatewayEvent(method string, id, params json.RawMessage) (Event, error) {
	var event Event
	if method == methodEvent {
		err := json.Unmarshal(params, &event)

		return event, err
	}

	if err := json.Unmarshal(id, &event.RequestID); err != nil || event.RequestID == "" {
		return Event{}, errors.New("hermes server request has invalid id")
	}

	var session struct {
		ID string `json:"session_id"`
	}

	if err := json.Unmarshal(params, &session); err != nil || session.ID == "" {
		return Event{}, errors.New("hermes server request has no session")
	}

	event.Type, event.SessionID = method, session.ID
	event.Payload = bytes.Clone(params)

	return event, nil
}

func isServerRequest(method string) bool {
	switch method {
	case "approval", "clarify", "sudo", "secret", "terminal.read":
		return true
	default:
		return false
	}
}

func (c *Client) rejectUnsupportedRequest(id json.RawMessage) bool {
	if err := c.writeUnsupportedRequest(id); err != nil {
		c.publishTerminal(err)
		_ = c.conn.CloseNow()

		return false
	}

	return true
}

func isNullJSON(value json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func (c *Client) writeUnsupportedRequest(id json.RawMessage) error {
	data, _ := json.Marshal(rpcErrorEnvelope{
		JSONRPC: jsonrpcVersion,
		ID:      append(json.RawMessage(nil), id...),
		Error:   RPCError{Code: -32601, Message: "Method not found"},
	}) // id is a member of an already-decoded JSON object and the other fields have fixed encodings.

	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	c.writeMu.Lock()
	err := c.conn.Write(ctx, websocket.MessageText, data)
	c.writeMu.Unlock()

	return err
}

// SessionCreateResult and SessionResumeResult name the two identities a new or
// resumed session answers with. History is replayed from the persisted export.
type SessionCreateResult struct {
	SessionID       string `json:"session_id"`
	StoredSessionID string `json:"stored_session_id"`
}

// SessionResumeResult is the session.resume answer. The gateway names the
// stored session key `session_key` on cold, deferred, and lazy resumes, and
// `stored_session_id` when the resume reattaches a live session whose row is
// not yet persisted. Both carry the same identity; read it through StoredKey.
type SessionResumeResult struct {
	SessionID       string `json:"session_id"`
	SessionKey      string `json:"session_key"`
	StoredSessionID string `json:"stored_session_id"`
}

// StoredKey returns the stored session key under either native spelling, or
// "" when the response carried neither.
func (r SessionResumeResult) StoredKey() string {
	if r.SessionKey != "" {
		return r.SessionKey
	}

	return r.StoredSessionID
}

type PromptSubmitResult struct {
	Status string `json:"status"`
}

type ActiveListResult struct {
	Sessions []ActiveSession `json:"sessions"`
}

type ActiveSession struct {
	SessionID  string `json:"id"`
	SessionKey string `json:"session_key"`
}

type ModelOptionsResult struct {
	Model     string     `json:"model"`
	Provider  string     `json:"provider"`
	Providers []Provider `json:"providers"`
}

// Provider is one native provider and its invokable model ids.
type Provider struct {
	Slug   string   `json:"slug"`
	Name   string   `json:"name"`
	Models []string `json:"models"`
}

func (p *Provider) UnmarshalJSON(data []byte) error {
	var object struct {
		Slug   string          `json:"slug"`
		Name   string          `json:"name"`
		Models json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}

	if object.Slug == "" {
		return fmt.Errorf("hermes model.options provider missing slug")
	}

	if len(object.Models) == 0 || string(object.Models) == "null" {
		return fmt.Errorf("hermes model.options provider %q missing models", object.Slug)
	}

	var models []string
	if err := json.Unmarshal(object.Models, &models); err != nil {
		return fmt.Errorf("hermes model.options provider %q models: %w", object.Slug, err)
	}

	p.Slug = object.Slug
	p.Name = object.Name
	p.Models = models

	return nil
}

func (c *Client) CreateSession(ctx context.Context, params map[string]any) (SessionCreateResult, error) {
	var out SessionCreateResult

	err := c.Call(ctx, "session.create", params, &out)

	return out, err
}

func (c *Client) ResumeSession(ctx context.Context, id string, params map[string]any) (SessionResumeResult, error) {
	params[fieldSessionID] = id

	var out SessionResumeResult

	err := c.Call(ctx, "session.resume", params, &out)

	return out, err
}

// AwaitSessionBuild blocks until the gateway has finished building the agent
// for a live session, and reports the build's own failure when it has one.
//
// process.list resolves its session through the gateway's build-aware lookup,
// so the gateway starts the deferred build if it has not started, waits for it,
// and answers only once the agent exists. The adapter needs exactly that
// barrier after a cold resume and needs nothing the call returns.
func (c *Client) AwaitSessionBuild(ctx context.Context, liveSessionID string) error {
	return c.Call(ctx, "process.list", map[string]any{fieldSessionID: liveSessionID}, nil)
}

func (c *Client) SubmitPromptWatermark(
	ctx context.Context,
	liveSessionID string,
	text string,
) (PromptSubmitResult, uint64, bool, error) {
	var result PromptSubmitResult

	watermark, written, proven, err := c.callWithWriteState(ctx, "prompt.submit", map[string]any{fieldSessionID: liveSessionID, valText: text, "queued": true}, &result)

	return result, watermark, written && !proven, err
}

// AttachImageBytes uploads one validated image to the live session. Hermes
// queues it for the immediately following prompt.submit call.
//
// The optional filename parameter is omitted. Hermes reads it only as an
// extension hint and falls back to the image's own magic bytes without one,
// which covers every media type the prompt allowlist admits; neither input form
// derives a filename, so an empty string would declare a hint that does not
// exist.
func (c *Client) AttachImageBytes(ctx context.Context, liveSessionID string, data []byte) error {
	var result struct {
		Attached bool `json:"attached"`
	}

	err := c.Call(ctx, "image.attach_bytes", map[string]any{
		fieldSessionID:   liveSessionID,
		"content_base64": base64.StdEncoding.EncodeToString(data),
	}, &result)
	if err != nil {
		return err
	}

	if !result.Attached {
		return fmt.Errorf("hermes image.attach_bytes did not attach image")
	}

	return nil
}

func (c *Client) Interrupt(ctx context.Context, liveSessionID string) error {
	return c.Call(ctx, "session.interrupt", map[string]any{fieldSessionID: liveSessionID}, nil)
}

func (c *Client) ModelOptions(ctx context.Context, liveSessionID string) (ModelOptionsResult, error) {
	var out ModelOptionsResult

	err := c.Call(ctx, "model.options", map[string]any{fieldSessionID: liveSessionID}, &out)

	return out, err
}

//nolint:goconst // Wire keys remain adjacent to this protocol method for auditability.
func (c *Client) SetModel(ctx context.Context, liveSessionID string, value string) error {
	var out struct {
		Key             string `json:"key"`
		Value           string `json:"value"`
		Scope           string `json:"scope"`
		ConfirmRequired bool   `json:"confirm_required"`
		Deferred        bool   `json:"deferred"`
	}

	command, rawModel, err := modelSwitchCommand(value)
	if err != nil {
		return err
	}

	err = c.Call(ctx, "config.set", map[string]any{
		fieldSessionID:            liveSessionID,
		"key":                     "model",
		"value":                   command,
		"confirm_expensive_model": true,
	}, &out)
	if err != nil {
		return err
	}
	// deferred=true means Hermes accepted the session-scoped choice while a
	// turn was running and will apply it on the next turn. That is successful
	// mutation, not an error: returning an error would leave native state ahead
	// of the ACP model metadata.
	if out.Key != "model" || out.Value != rawModel || out.Scope != "session" || out.ConfirmRequired {
		return fmt.Errorf("hermes config.set model returned an invalid result")
	}

	return nil
}

// SetReasoning applies a session-scoped reasoning effort. config.set with key
// reasoning and no scope pins the live session and updates its agent, and
// Hermes acknowledges with the level it applied. An id Hermes does not hold is
// not refused: the level is written to the global config instead, so the id
// must be live.
func (c *Client) SetReasoning(ctx context.Context, liveSessionID string, value string) (string, error) {
	var out struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	err := c.Call(ctx, "config.set", map[string]any{
		fieldSessionID: liveSessionID,
		"key":          valReasoning,
		"value":        value,
	}, &out)
	if err != nil {
		return "", err
	}

	if out.Key != valReasoning || out.Value == "" {
		return "", fmt.Errorf("hermes config.set reasoning returned an invalid result")
	}

	return out.Value, nil
}

// Reasoning reads the effort the live session runs at: its own pin, else its
// agent's, else the config default.
func (c *Client) Reasoning(ctx context.Context, liveSessionID string) (string, error) {
	var out struct {
		Value string `json:"value"`
	}

	err := c.Call(ctx, "config.get", map[string]any{fieldSessionID: liveSessionID, "key": valReasoning}, &out)
	if err != nil {
		return "", err
	}

	if out.Value == "" {
		return "", fmt.Errorf("hermes config.get reasoning returned no value")
	}

	return out.Value, nil
}

// ModelSelectionShapeError reports why value cannot name a model selection, or
// nil when it can. A selection is provider-qualified: two tokens split on the
// first "/", neither empty, neither a flag, neither carrying whitespace or
// control characters.
//
// The question is the string's shape and never which models exist. Hermes owns
// that: a provider-qualified selection reaches config.set whatever it names,
// and Hermes answers for it. Every door that must turn one host value into a
// provider and a model asks this one predicate, so no door admits a shape
// another door refuses.
func ModelSelectionShapeError(value string) error {
	_, _, err := modelSwitchCommand(value)

	return err
}

func modelSwitchCommand(value string) (string, string, error) {
	provider, rawModel, ok := strings.Cut(value, "/")

	invalidToken := func(token string) bool {
		return token == "" || strings.HasPrefix(token, "--") || strings.IndexFunc(token, func(r rune) bool {
			return unicode.IsSpace(r) || unicode.IsControl(r)
		}) >= 0
	}
	if !ok || invalidToken(provider) || invalidToken(rawModel) {
		return "", "", fmt.Errorf("hermes model selection %q is not provider-qualified", value)
	}

	return rawModel + " --provider " + provider + " --session", rawModel, nil
}

func gatewayTransportCause(err error) error {
	if err == nil {
		return errors.New("hermes gateway disconnected")
	}

	return err
}

// Sequence is the latest frame read in this transport generation.
func (c *Client) Sequence() uint64 { return c.sequence.Load() }
