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

	beforeCallWait        func()
	beforeDoneResultCheck func()
	closeTransport        func(websocket.StatusCode, string) error
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
}

// GatewayDelivery is the gateway reader's single ordered output. A delivery
// contains exactly one event or terminal error; clean EOF closes the channel.
type GatewayDelivery struct {
	Event *Event
	Err   error
}

// GatewayWatermark identifies one response frame in a transport generation.
// The server adds the generation when it installs the connection; Sequence is
// the monotonic position assigned by the WebSocket reader to every inbound text
// frame, including events and responses.
type GatewayWatermark struct {
	TransportGeneration uint64
	Sequence            uint64
}

var ErrGatewayInputOverflow = errors.New("hermes gateway input overflow")

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
	if err := json.Unmarshal(object["jsonrpc"], &response.JSONRPC); err != nil || response.JSONRPC != jsonrpcVersion {
		return rpcResponse{}, errors.New("hermes JSON-RPC response has invalid version")
	}

	if err := json.Unmarshal(object["id"], &response.ID); err != nil {
		return rpcResponse{}, fmt.Errorf("decode Hermes JSON-RPC response id: %w", err)
	}

	result, hasResult := object["result"]
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

func IsNotFound(err error) bool {
	var rpcErr *RPCError

	return errors.As(err, &rpcErr) && rpcErr.Code == 4007
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
		conn:           conn,
		deliveries:     make(chan GatewayDelivery, gatewayEventDeliveryCapacity+1),
		done:           make(chan struct{}),
		pending:        make(map[int64]chan rpcResponse),
		closeTransport: conn.Close,
	}
	go client.readLoop() //nolint:gosec // WebSocket reader owns the connection lifetime, not the dial context.

	return client, nil
}

func (c *Client) Deliveries() <-chan GatewayDelivery {
	return c.deliveries
}

// Done is closed when the read loop exits, i.e. when the underlying WebSocket
// connection has terminated (normal close or disconnect). It carries no value
// and never blocks a producer, so a reconnect loop can watch it without competing
// with ordered delivery consumers.
func (c *Client) Done() <-chan struct{} {
	return c.done
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

	attempt.err = c.closeTransport(status, reason)

	c.mu.Lock()
	close(attempt.done)
	c.mu.Unlock()

	return attempt.err
}

func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	_, err := c.call(ctx, method, params, out)

	return err
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

	if c.beforeCallWait != nil {
		c.beforeCallWait()
	}

	select {
	case resp := <-respCh:
		return resolve(resp)
	case <-c.done:
		if c.beforeDoneResultCheck != nil {
			c.beforeDoneResultCheck()
		}

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
		idValue, hasID := object["id"]

		if hasMethod {
			var method string
			if err := json.Unmarshal(methodValue, &method); err != nil || method == "" {
				c.publishTerminal(errors.New("hermes gateway JSON-RPC request has invalid method"))

				return
			}

			params, hasParams := object["params"]
			if method == methodEvent && !hasID && hasParams {
				var event Event
				if err := json.Unmarshal(params, &event); err != nil {
					c.publishTerminal(err)

					return
				}

				event.Raw = append(event.Raw[:0], data...)

				event.InboundSequence = sequence

				if len(c.deliveries) >= cap(c.deliveries)-1 {
					c.publishTerminal(ErrGatewayInputOverflow)

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
// resumed session answers with. Both responses also carry a message projection
// and a session-info block; replay reads history through session.history, so
// neither is decoded here.
type SessionCreateResult struct {
	SessionID       string `json:"session_id"`
	StoredSessionID string `json:"stored_session_id"`
}

type SessionResumeResult struct {
	SessionID  string `json:"session_id"`
	SessionKey string `json:"session_key"`
}

type PromptSubmitResult struct {
	Status string `json:"status"`
}

type SessionTitleResult struct {
	Pending bool   `json:"pending"`
	Title   string `json:"title"`
}

type SessionHistoryResult struct {
	Messages []Message `json:"messages"`
}

// Message is one row of the gateway's session.history projection. The gateway
// renders every visible row as {"role", "text"}; a tool row carries no text at
// all, so an empty Text is a row with nothing to replay, never a missed key.
type Message struct {
	Role string          `json:"role"`
	Text string          `json:"text"`
	Raw  json.RawMessage `json:"-"`
}

func (m *Message) UnmarshalJSON(data []byte) error {
	type alias Message

	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*m = Message(value)
	m.Raw = append(m.Raw[:0], data...)

	return nil
}

type ActiveListResult struct {
	Sessions []ActiveSession `json:"sessions"`
}

type ActiveSession struct {
	SessionID  string `json:"id"`
	SessionKey string `json:"session_key"`
	Title      string `json:"title"`
	Cwd        string `json:"cwd"`
}

// PersistedSession is one durable state.db row returned by session.list.
type PersistedSession struct {
	SessionID string `json:"id"`
	Title     string `json:"title"`
}

type SessionListResult struct {
	Sessions []PersistedSession `json:"sessions"`
}

type ModelOptionsResult struct {
	Model     string          `json:"model"`
	Provider  string          `json:"provider"`
	Providers []Provider      `json:"providers"`
	Raw       json.RawMessage `json:"-"`
}

func (m *ModelOptionsResult) UnmarshalJSON(data []byte) error {
	type alias ModelOptionsResult

	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*m = ModelOptionsResult(value)
	m.Raw = append(m.Raw[:0], data...)

	return nil
}

// Provider is one row of the gateway's model catalogue. The row also carries
// authentication state, pricing, featured hints, and a per-model capability map
// ({model: {fast, reasoning}}); the config surface this adapter builds publishes
// the model ids and their names, so nothing else is decoded. Raw keeps the whole
// row for callers that need to read the catalogue as the gateway wrote it.
type Provider struct {
	Slug   string          `json:"slug"`
	Name   string          `json:"name"`
	Models []string        `json:"models"`
	Raw    json.RawMessage `json:"-"`
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
	p.Raw = append(p.Raw[:0], data...)

	return nil
}

type BranchResult struct {
	SessionID       string `json:"session_id"`
	StoredSessionID string `json:"stored_session_id"`
	Title           string `json:"title"`
	Parent          string `json:"parent"`
}

func (c *Client) CreateSession(ctx context.Context, params map[string]any) (SessionCreateResult, error) {
	out, _, err := c.CreateSessionWatermark(ctx, params)

	return out, err
}

func (c *Client) CreateSessionWatermark(
	ctx context.Context,
	params map[string]any,
) (SessionCreateResult, uint64, error) {
	var out SessionCreateResult

	watermark, err := c.call(ctx, "session.create", params, &out)

	return out, watermark, err
}

func (c *Client) ResumeSession(ctx context.Context, storedSessionID string, params map[string]any) (SessionResumeResult, error) {
	out, _, err := c.ResumeSessionWatermark(ctx, storedSessionID, params)

	return out, err
}

func (c *Client) ResumeSessionWatermark(
	ctx context.Context,
	storedSessionID string,
	params map[string]any,
) (SessionResumeResult, uint64, error) {
	if params == nil {
		params = map[string]any{}
	}

	params[fieldSessionID] = storedSessionID

	var out SessionResumeResult

	watermark, err := c.call(ctx, "session.resume", params, &out)

	return out, watermark, err
}

func (c *Client) SetSessionTitle(ctx context.Context, liveSessionID string, title string) (SessionTitleResult, error) {
	var out SessionTitleResult

	err := c.Call(ctx, "session.title", map[string]any{
		fieldSessionID: liveSessionID,
		fieldTitle:     title,
	}, &out)

	return out, err
}

func (c *Client) History(ctx context.Context, liveSessionID string) (SessionHistoryResult, error) {
	var out SessionHistoryResult

	err := c.Call(ctx, "session.history", map[string]any{fieldSessionID: liveSessionID}, &out)

	return out, err
}

func (c *Client) ActiveList(ctx context.Context) (ActiveListResult, error) {
	var out ActiveListResult

	err := c.Call(ctx, "session.active_list", map[string]any{}, &out)

	return out, err
}

// PersistedSessions lists durable Hermes sessions, including sessions that are
// not currently resident in this gateway process.
func (c *Client) PersistedSessions(ctx context.Context) (SessionListResult, error) {
	var out SessionListResult

	err := c.Call(ctx, "session.list", map[string]any{"limit": 10000}, &out)

	return out, err
}

func (c *Client) DeleteSession(ctx context.Context, storedSessionID string) error {
	return c.Call(ctx, "session.delete", map[string]any{fieldSessionID: storedSessionID}, nil)
}

func (c *Client) CloseSession(ctx context.Context, liveSessionID string) error {
	return c.Call(ctx, "session.close", map[string]any{fieldSessionID: liveSessionID}, nil)
}

func (c *Client) Branch(ctx context.Context, liveSessionID string, name string) (BranchResult, error) {
	params := map[string]any{fieldSessionID: liveSessionID}
	if name != "" {
		params["name"] = name
	}

	var out BranchResult

	err := c.Call(ctx, "session.branch", params, &out)

	return out, err
}

func (c *Client) SubmitPrompt(ctx context.Context, liveSessionID string, text string) error {
	return c.Call(ctx, "prompt.submit", map[string]any{fieldSessionID: liveSessionID, valText: text}, nil)
}

func (c *Client) SubmitPromptWatermark(
	ctx context.Context,
	liveSessionID string,
	text string,
) (PromptSubmitResult, uint64, bool, error) {
	var result PromptSubmitResult

	watermark, written, proven, err := c.callWithWriteState(ctx, "prompt.submit", map[string]any{fieldSessionID: liveSessionID, valText: text}, &result)

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

// ApprovalRespond answers exactly one native approval: the session's oldest
// waiting one. Hermes's `all` flag answers every approval queued on the session
// with the same choice, including ones no host was ever shown, so this adapter
// never sends it.
func (c *Client) ApprovalRespond(ctx context.Context, liveSessionID string, choice string) error {
	return c.Call(ctx, "approval.respond", map[string]any{fieldSessionID: liveSessionID, "choice": choice, "all": false}, nil)
}

func (c *Client) ClarifyRespond(ctx context.Context, liveSessionID, requestID string, answer any) error {
	if requestID == "" {
		return errors.New("clarify request_id is required")
	}

	return c.Call(ctx, "clarify.respond", map[string]any{
		fieldSessionID: liveSessionID,
		"request_id":   requestID,
		"answer":       answer,
	}, nil)
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
