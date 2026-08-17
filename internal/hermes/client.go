//nolint:tagliatelle // Hermes gateway JSON fields use snake_case names.
package hermes

import (
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
)

type Client struct {
	conn   *websocket.Conn
	events chan Event
	errs   chan error
	done   chan struct{}

	nextID  atomic.Int64
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[int64]chan rpcResponse
	closed  bool
}

type Event struct {
	Type      string          `json:"type"`
	SessionID string          `json:"session_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Raw       json.RawMessage `json:"-"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
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
		conn:    conn,
		events:  make(chan Event, 256),
		errs:    make(chan error, 8),
		done:    make(chan struct{}),
		pending: make(map[int64]chan rpcResponse),
	}
	go client.readLoop() //nolint:gosec // WebSocket reader owns the connection lifetime, not the dial context.

	return client, nil
}

func (c *Client) Events() <-chan Event {
	return c.events
}

func (c *Client) Errors() <-chan error {
	return c.errs
}

// Done is closed when the read loop exits, i.e. when the underlying WebSocket
// connection has terminated (normal close or disconnect). It carries no value
// and never blocks a producer, so a supervisor can watch it without competing
// with Events/Errors consumers.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

func (c *Client) Close(status websocket.StatusCode, reason string) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return nil
	}

	c.closed = true
	pending := c.pending
	c.pending = map[int64]chan rpcResponse{}
	c.mu.Unlock()

	for _, ch := range pending {
		close(ch)
	}

	return c.conn.Close(status, reason)
}

func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	id := c.nextID.Add(1)
	respCh := make(chan rpcResponse, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return errors.New("hermes client closed")
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
		return err
	}

	c.writeMu.Lock()
	err = c.conn.Write(ctx, websocket.MessageText, data)
	c.writeMu.Unlock()

	if err != nil {
		return err
	}

	select {
	case resp, ok := <-respCh:
		if !ok {
			return errors.New("hermes client closed")
		}

		if resp.Error != nil {
			return resp.Error
		}

		if out == nil {
			return nil
		}

		if len(resp.Result) == 0 {
			return nil
		}

		return json.Unmarshal(resp.Result, out)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) forget(id int64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Client) readLoop() {
	defer func() {
		c.mu.Lock()
		c.closed = true
		pending := c.pending
		c.pending = map[int64]chan rpcResponse{}
		c.mu.Unlock()

		for _, ch := range pending {
			close(ch)
		}

		close(c.events)
		close(c.errs)
		close(c.done)
	}()

	for {
		typ, data, err := c.conn.Read(context.Background())
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return
			}

			select {
			case c.errs <- err:
			default:
			}

			return
		}

		if typ != websocket.MessageText {
			continue
		}

		var probe struct {
			ID     *int64           `json:"id,omitempty"`
			Method string           `json:"method,omitempty"`
			Params *json.RawMessage `json:"params,omitempty"`
		}
		if err := json.Unmarshal(data, &probe); err != nil {
			select {
			case c.errs <- err:
			default:
			}

			continue
		}

		if probe.ID != nil {
			var resp rpcResponse
			if err := json.Unmarshal(data, &resp); err != nil {
				select {
				case c.errs <- err:
				default:
				}

				continue
			}

			c.mu.Lock()
			ch := c.pending[resp.ID]
			c.mu.Unlock()

			if ch != nil {
				ch <- resp
			}

			continue
		}

		if probe.Method == methodEvent && probe.Params != nil {
			var event Event
			if err := json.Unmarshal(*probe.Params, &event); err != nil {
				select {
				case c.errs <- err:
				default:
				}

				continue
			}

			event.Raw = append(event.Raw[:0], data...)
			select {
			case c.events <- event:
			default:
			}
		}
	}
}

type SessionCreateResult struct {
	SessionID       string          `json:"session_id"`
	StoredSessionID string          `json:"stored_session_id"`
	MessageCount    int             `json:"message_count"`
	Messages        []Message       `json:"messages"`
	Info            json.RawMessage `json:"info"`
}

type SessionResumeResult struct {
	SessionID    string          `json:"session_id"`
	SessionKey   string          `json:"session_key"`
	MessageCount int             `json:"message_count"`
	Messages     []Message       `json:"messages"`
	Info         json.RawMessage `json:"info"`
}

type SessionTitleResult struct {
	Pending bool   `json:"pending"`
	Title   string `json:"title"`
}

type SessionHistoryResult struct {
	Count    int       `json:"count"`
	Messages []Message `json:"messages"`
}

type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
	Raw     json.RawMessage `json:"-"`
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
	SessionID    string `json:"id"`
	Title        string `json:"title"`
	MessageCount int    `json:"message_count"`
	Source       string `json:"source"`
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

type Provider struct {
	Slug          string                             `json:"slug"`
	Name          string                             `json:"name"`
	AuthType      string                             `json:"auth_type"`
	Authenticated bool                               `json:"authenticated"`
	Capabilities  map[string]ProviderModelCapability `json:"capabilities"`
	IsCurrent     bool                               `json:"is_current"`
	IsUserDefined bool                               `json:"is_user_defined"`
	KeyEnv        string                             `json:"key_env"`
	Models        []string                           `json:"models"`
	Pricing       map[string]ProviderModelPricing    `json:"pricing"`
	Source        string                             `json:"source"`
	TotalModels   int                                `json:"total_models"`
	Warning       string                             `json:"warning"`
	Raw           json.RawMessage                    `json:"-"`
}

func (p *Provider) UnmarshalJSON(data []byte) error {
	var object struct {
		Slug          string                             `json:"slug"`
		Name          string                             `json:"name"`
		AuthType      string                             `json:"auth_type"`
		Authenticated bool                               `json:"authenticated"`
		Capabilities  map[string]ProviderModelCapability `json:"capabilities"`
		IsCurrent     bool                               `json:"is_current"`
		IsUserDefined bool                               `json:"is_user_defined"`
		KeyEnv        string                             `json:"key_env"`
		Models        json.RawMessage                    `json:"models"`
		Pricing       map[string]ProviderModelPricing    `json:"pricing"`
		Source        string                             `json:"source"`
		TotalModels   int                                `json:"total_models"`
		Warning       string                             `json:"warning"`
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
	p.AuthType = object.AuthType
	p.Authenticated = object.Authenticated
	p.Capabilities = object.Capabilities
	p.IsCurrent = object.IsCurrent
	p.IsUserDefined = object.IsUserDefined
	p.KeyEnv = object.KeyEnv
	p.Models = models
	p.Pricing = object.Pricing
	p.Source = object.Source
	p.TotalModels = object.TotalModels
	p.Warning = object.Warning
	p.Raw = append(p.Raw[:0], data...)

	return nil
}

type ProviderModelCapability struct {
	Fast      bool `json:"fast"`
	Reasoning bool `json:"reasoning"`
}

type ProviderModelPricing struct {
	Cache  *string `json:"cache"`
	Free   bool    `json:"free"`
	Input  string  `json:"input"`
	Output string  `json:"output"`
}

type BranchResult struct {
	SessionID       string `json:"session_id"`
	StoredSessionID string `json:"stored_session_id"`
	Title           string `json:"title"`
	Parent          string `json:"parent"`
}

func (c *Client) CreateSession(ctx context.Context, params map[string]any) (SessionCreateResult, error) {
	var out SessionCreateResult

	err := c.Call(ctx, "session.create", params, &out)

	return out, err
}

func (c *Client) ResumeSession(ctx context.Context, storedSessionID string, params map[string]any) (SessionResumeResult, error) {
	if params == nil {
		params = map[string]any{}
	}

	params[fieldSessionID] = storedSessionID

	var out SessionResumeResult

	err := c.Call(ctx, "session.resume", params, &out)

	return out, err
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

func (c *Client) ApprovalRespond(ctx context.Context, liveSessionID string, choice string, all bool) error {
	return c.Call(ctx, "approval.respond", map[string]any{fieldSessionID: liveSessionID, "choice": choice, "all": all}, nil)
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
