//nolint:tagliatelle // Hermes gateway JSON fields use snake_case names.
package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
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
		JSONRPC: "2.0",
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
		if probe.Method == "event" && probe.Params != nil {
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
	SessionID       string          `json:"session_id"`
	StoredSessionID string          `json:"stored_session_id"`
	MessageCount    int             `json:"message_count"`
	Messages        []Message       `json:"messages"`
	Info            json.RawMessage `json:"info"`
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
	SessionID  string `json:"session_id"`
	SessionKey string `json:"session_key"`
	Title      string `json:"title"`
	Cwd        string `json:"cwd"`
}

func (s *ActiveSession) UnmarshalJSON(data []byte) error {
	var object struct {
		ID         string `json:"id"`
		SessionID  string `json:"session_id"`
		SessionKey string `json:"session_key"`
		Title      string `json:"title"`
		Cwd        string `json:"cwd"`
	}
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	s.SessionID = firstNonEmpty(object.SessionID, object.ID)
	s.SessionKey = object.SessionKey
	s.Title = object.Title
	s.Cwd = object.Cwd
	return nil
}

type ModelOptionsResult struct {
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
	ID     string          `json:"id"`
	Slug   string          `json:"slug"`
	Name   string          `json:"name"`
	Models []ProviderModel `json:"models"`
	Raw    json.RawMessage `json:"-"`
}

func (p *Provider) UnmarshalJSON(data []byte) error {
	var object struct {
		ID     string          `json:"id"`
		Slug   string          `json:"slug"`
		Name   string          `json:"name"`
		Models json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	p.ID = firstNonEmpty(object.ID, object.Slug)
	p.Slug = object.Slug
	p.Name = object.Name
	if len(object.Models) > 0 {
		switch object.Models[0] {
		case '[':
			_ = json.Unmarshal(object.Models, &p.Models)
		case '{':
			var modelMap map[string]ProviderModel
			if err := json.Unmarshal(object.Models, &modelMap); err == nil {
				for key, model := range modelMap {
					if model.ID == "" {
						model.ID = key
					}
					p.Models = append(p.Models, model)
				}
			}
		case '"':
			var modelID string
			if err := json.Unmarshal(object.Models, &modelID); err == nil && modelID != "" {
				p.Models = append(p.Models, ProviderModel{ID: modelID, Name: modelID})
			}
		}
	}
	p.Raw = append(p.Raw[:0], data...)
	return nil
}

type ProviderModel struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Context      int             `json:"context_window"`
	MaxOutput    int             `json:"max_output_tokens"`
	Capabilities []string        `json:"capabilities"`
	Raw          json.RawMessage `json:"-"`
}

func (m *ProviderModel) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var id string
		if err := json.Unmarshal(data, &id); err != nil {
			return err
		}
		m.ID = id
		m.Name = id
		m.Raw = append(m.Raw[:0], data...)
		return nil
	}
	type alias ProviderModel
	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*m = ProviderModel(value)
	m.Raw = append(m.Raw[:0], data...)
	return nil
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
	params["session_id"] = storedSessionID
	var out SessionResumeResult
	err := c.Call(ctx, "session.resume", params, &out)
	return out, err
}

func (c *Client) History(ctx context.Context, liveSessionID string) (SessionHistoryResult, error) {
	var out SessionHistoryResult
	err := c.Call(ctx, "session.history", map[string]any{"session_id": liveSessionID}, &out)
	return out, err
}

func (c *Client) ActiveList(ctx context.Context) (ActiveListResult, error) {
	var out ActiveListResult
	err := c.Call(ctx, "session.active_list", map[string]any{}, &out)
	return out, err
}

func (c *Client) DeleteSession(ctx context.Context, storedSessionID string) error {
	return c.Call(ctx, "session.delete", map[string]any{"session_id": storedSessionID}, nil)
}

func (c *Client) CloseSession(ctx context.Context, liveSessionID string) error {
	return c.Call(ctx, "session.close", map[string]any{"session_id": liveSessionID}, nil)
}

func (c *Client) Branch(ctx context.Context, liveSessionID string, name string) (SessionCreateResult, error) {
	params := map[string]any{"session_id": liveSessionID}
	if name != "" {
		params["name"] = name
	}
	var out SessionCreateResult
	err := c.Call(ctx, "session.branch", params, &out)
	return out, err
}

func (c *Client) SubmitPrompt(ctx context.Context, liveSessionID string, text string) error {
	return c.Call(ctx, "prompt.submit", map[string]any{"session_id": liveSessionID, "text": text}, nil)
}

func (c *Client) Interrupt(ctx context.Context, liveSessionID string) error {
	return c.Call(ctx, "session.interrupt", map[string]any{"session_id": liveSessionID}, nil)
}

func (c *Client) ApprovalRespond(ctx context.Context, liveSessionID string, choice string, all bool) error {
	return c.Call(ctx, "approval.respond", map[string]any{"session_id": liveSessionID, "choice": choice, "all": all}, nil)
}

func (c *Client) ClarifyRespond(ctx context.Context, liveSessionID string, answer any) error {
	return c.Call(ctx, "clarify.respond", map[string]any{"session_id": liveSessionID, "answer": answer}, nil)
}

func (c *Client) ModelOptions(ctx context.Context, liveSessionID string) (ModelOptionsResult, error) {
	var out ModelOptionsResult
	err := c.Call(ctx, "model.options", map[string]any{"session_id": liveSessionID}, &out)
	return out, err
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
