//nolint:gocyclo // Fake process method matrices intentionally enumerate the full protocol.
package hermes

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type rpcCall struct {
	Method string
	Params map[string]any
}

type wsGateway struct {
	t      *testing.T
	server *httptest.Server

	mu               sync.Mutex
	calls            []rpcCall
	failMethod       string
	malformedAtStart bool
	malformedEvent   bool
	startupRaw       []byte
	overflowAtStart  bool
	reverseEOF       bool
	reverseRequests  []struct {
		id    int64
		value string
	}
}

func newWSGateway(t *testing.T) *wsGateway {
	t.Helper()
	gateway := &wsGateway{t: t}
	gateway.server = httptest.NewServer(http.HandlerFunc(gateway.handle))
	t.Cleanup(gateway.server.Close)

	return gateway
}

func (g *wsGateway) url() string {
	return "ws" + strings.TrimPrefix(g.server.URL, "http") + "/api/ws"
}

func (g *wsGateway) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/ws" {
		http.NotFound(w, r)

		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		g.t.Errorf("accept websocket: %v", err)

		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")
	_ = conn.Write(r.Context(), websocket.MessageBinary, []byte("ignored"))
	g.writeEvent(r.Context(), conn, Event{Type: "gateway.ready"})
	g.writeEvent(r.Context(), conn, Event{Type: "message.delta", SessionID: "live", Payload: json.RawMessage(`{"text":"hi"}`)})
	if g.malformedAtStart {
		_ = conn.Write(r.Context(), websocket.MessageText, []byte("{"))

		return
	}
	if g.malformedEvent {
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"jsonrpc":"2.0","method":"event","params":"invalid"}`))

		return
	}
	if g.startupRaw != nil {
		_ = conn.Write(r.Context(), websocket.MessageText, g.startupRaw)

		return
	}
	if g.overflowAtStart {
		for range 300 {
			g.writeEvent(r.Context(), conn, Event{Type: "message.delta", SessionID: "live", Payload: json.RawMessage(`{"text":"overflow"}`)})
		}

		return
	}
	for {
		typ, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var req struct {
			ID     int64           `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(data, &req); err != nil {
			return
		}
		params := map[string]any{}
		_ = json.Unmarshal(req.Params, &params)
		g.mu.Lock()
		g.calls = append(g.calls, rpcCall{Method: req.Method, Params: params})
		failMethod := g.failMethod
		g.mu.Unlock()
		if req.Method == failMethod {
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": 4999, "message": "injected"}})

			continue
		}
		switch req.Method {
		case "missing":
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": 4007, "message": "missing"}})
		case "hang":
		case "close":
			_ = conn.Close(websocket.StatusInternalError, "forced close")

			return
		case "empty-result":
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID})
		case "wrong-version":
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "1.0", "id": req.ID, "result": map[string]any{}})
		case "result-and-error":
			g.writeRaw(r.Context(), conn, map[string]any{
				"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{},
				"error": map[string]any{"code": -32000, "message": "ambiguous"},
			})
		case "bad-rpc":
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":"bad","message":"bad"}}`, req.ID)))
		case "overflow":
			for range 300 {
				g.writeEvent(r.Context(), conn, Event{Type: "message.delta", SessionID: "live", Payload: json.RawMessage(`{"text":"overflow"}`)})
			}

			return
		case "bad-result":
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "{"})
		case "reverse-eof":
			value, _ := params["value"].(string)
			g.reverseRequests = append(g.reverseRequests, struct {
				id    int64
				value string
			}{id: req.ID, value: value})
			if g.reverseEOF && len(g.reverseRequests) == 2 {
				for index := len(g.reverseRequests) - 1; index >= 0; index-- {
					pending := g.reverseRequests[index]
					g.writeRaw(r.Context(), conn, map[string]any{
						"jsonrpc": "2.0", "id": pending.id, "result": map[string]any{"value": pending.value},
					})
				}
				_ = conn.CloseNow()

				return
			}
		case "cross-direction":
			g.writeRaw(r.Context(), conn, map[string]any{
				"jsonrpc": "2.0", "id": req.ID, "method": "gateway.unsupported", "params": map[string]any{},
			})
			responseType, responseData, responseErr := conn.Read(r.Context())
			if responseErr != nil || responseType != websocket.MessageText {
				g.t.Errorf("read unsupported-request response: type=%v err=%v", responseType, responseErr)

				return
			}
			var unsupported struct {
				ID    int64     `json:"id"`
				Error *RPCError `json:"error"`
			}
			if err := json.Unmarshal(responseData, &unsupported); err != nil || unsupported.ID != req.ID ||
				unsupported.Error == nil || unsupported.Error.Code != -32601 {
				g.t.Errorf("unsupported-request response = %s err=%v", responseData, err)

				return
			}
			g.writeRaw(r.Context(), conn, map[string]any{
				"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{"value": "first"},
			})
			g.writeRaw(r.Context(), conn, map[string]any{
				"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{"value": "duplicate"},
			})
		default:
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": resultForMethod(req.Method, params)})
		}
	}
}

func TestStartupMethodProbeCleansDurableSessionAfterModelOptionsFailure(t *testing.T) {
	gateway := newWSGateway(t)
	gateway.failMethod = "model.options"
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	client, err := Dial(ctx, gateway.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()
	process := &Process{Client: client, Home: t.TempDir()}
	if err := process.probeGatewayMethods(ctx); err == nil || !strings.Contains(err.Error(), "model.options") {
		t.Fatalf("startup probe error = %v", err)
	}
	gateway.mu.Lock()
	calls := append([]rpcCall(nil), gateway.calls...)
	gateway.mu.Unlock()
	var closed, deleted bool
	for _, call := range calls {
		closed = closed || call.Method == "session.close"
		deleted = deleted || call.Method == "session.delete"
	}
	if !closed || !deleted {
		t.Fatalf("startup cleanup calls missing: %#v", calls)
	}
}

func resultForMethod(method string, params map[string]any) any {
	switch method {
	case "session.create":
		return map[string]any{"session_id": "live", "stored_session_id": "stored"}
	case "session.branch":
		return map[string]any{"session_id": "live-branch", "stored_session_id": "stored-branch", "title": "Branch", "parent": "stored"}
	case "session.resume":
		return map[string]any{"session_id": "live-resume", "session_key": params["session_id"]}
	case "session.title":
		return map[string]any{"pending": false, "title": params["title"]}
	case "session.history":
		return map[string]any{"count": 1, "messages": []map[string]any{{"role": "assistant", "text": "hello"}}}
	case "session.active_list":
		return map[string]any{"sessions": []map[string]any{{"id": "live", "session_key": "stored", "title": "Title", "cwd": "/repo"}}}
	case "session.list":
		return map[string]any{"sessions": []map[string]any{{"id": "stored", "title": "Hermes session"}}}
	case "image.attach_bytes":
		return map[string]any{"attached": true}
	case "config.set":
		value, _ := params["value"].(string)
		raw := strings.Fields(value)[0]
		if raw == "mismatch" {
			raw = "different"
		}
		deferred := raw == "deferred"

		return map[string]any{"key": params["key"], "value": raw, "scope": "session", "confirm_required": false, "deferred": deferred}
	case "model.options":
		return map[string]any{
			"model":    "anthropic/claude-sonnet-4",
			"provider": "",
			"providers": []map[string]any{
				{
					"slug":            "openrouter",
					"name":            "OpenRouter",
					"authenticated":   true,
					"is_current":      false,
					"is_user_defined": false,
					"models":          []string{"anthropic/claude-fable-5", "openai/gpt-5.5"},
					"capabilities":    map[string]any{"anthropic/claude-fable-5": map[string]any{"fast": false, "reasoning": true}},
					"pricing":         map[string]any{"anthropic/claude-fable-5": map[string]any{"cache": "$1.00", "free": false, "input": "$10.00", "output": "$50.00"}},
					"source":          "built-in",
					"total_models":    2,
				},
				{
					"auth_type":       "virtual",
					"authenticated":   true,
					"capabilities":    map[string]any{"default": map[string]any{"fast": false, "reasoning": true}},
					"is_current":      false,
					"is_user_defined": false,
					"models":          []string{"default"},
					"name":            "Mixture of Agents",
					"slug":            "moa",
					"source":          "virtual",
					"total_models":    1,
					"warning":         "Aggregator acts as the selected model.",
				},
			},
		}
	default:
		return map[string]any{}
	}
}

func (g *wsGateway) writeEvent(ctx context.Context, conn *websocket.Conn, event Event) {
	g.writeRaw(ctx, conn, map[string]any{"jsonrpc": "2.0", "method": "event", "params": event})
}

func (g *wsGateway) writeRaw(ctx context.Context, conn *websocket.Conn, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		g.t.Errorf("marshal websocket value: %v", err)

		return
	}
	_ = conn.Write(ctx, websocket.MessageText, data)
}

func waitForGatewayCallCount(t *testing.T, gateway *wsGateway, method string, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if gatewayCallCount(gateway, method) >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("gateway method %q was not called", method)
		case <-tick.C:
		}
	}
}

func gatewayCallCount(gateway *wsGateway, method string) int {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	count := 0
	for _, call := range gateway.calls {
		if call.Method == method {
			count++
		}
	}

	return count
}

func TestProcessRedialAndClientDone(t *testing.T) {
	gateway := newWSGateway(t)
	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(gateway.server.URL, "http://"))
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	proc := &Process{Port: port, Token: "tok"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := proc.Redial(ctx)
	if err != nil {
		t.Fatalf("Redial: %v", err)
	}
	select {
	case <-client.Done():
		t.Fatal("Done closed while connection is live")
	default:
	}
	if err := client.Close(websocket.StatusNormalClosure, "bye"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done not closed after connection close")
	}
}

func TestClientRPCEventsAndWrappers(t *testing.T) {
	gateway := newWSGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, dialErr := Dial(ctx, gateway.url(), nil)
	if dialErr != nil {
		t.Fatalf("Dial: %v", dialErr)
	}

	assertClientEventStream(t, client)
	assertClientWrappers(t, ctx, client)
	gateway.mu.Lock()
	var clarifyParams map[string]any
	var configParams []map[string]any
	for _, call := range gateway.calls {
		if call.Method == "clarify.respond" {
			clarifyParams = call.Params
		}
		if call.Method == "config.set" {
			configParams = append(configParams, call.Params)
		}
	}
	gateway.mu.Unlock()
	if clarifyParams["session_id"] != "live" || clarifyParams["request_id"] != "request-1" || clarifyParams["answer"] != "yes" {
		t.Fatalf("clarify.respond params = %#v", clarifyParams)
	}
	wantAggregator := map[string]any{
		"session_id":              "live",
		"key":                     "model",
		"value":                   "x-ai/grok-4.5 --provider openrouter --session",
		"confirm_expensive_model": true,
	}
	if len(configParams) != 4 || configParams[0]["value"] != "claude-sonnet-4 --provider anthropic --session" ||
		!reflect.DeepEqual(configParams[1], wantAggregator) || configParams[2]["value"] != "deferred --provider provider --session" ||
		configParams[3]["value"] != "mismatch --provider provider --session" {
		t.Fatalf("config.set params = %#v", configParams)
	}
	assertClientCallEdges(t, ctx, client)
	assertClientCloseSemantics(t, ctx, client, gateway)
}

func assertClientEventStream(t *testing.T, client *Client) {
	t.Helper()

	if delivery := <-client.Deliveries(); delivery.Event == nil || delivery.Event.Type != eventGatewayReady {
		t.Fatalf("first delivery = %#v", delivery)
	}
	if delivery := <-client.Deliveries(); delivery.Event == nil || delivery.Event.Type != "message.delta" ||
		delivery.Event.SessionID != "live" || len(delivery.Event.Raw) == 0 {
		t.Fatalf("native delivery = %#v", delivery)
	}
	if client.Deliveries() == nil {
		t.Fatal("delivery accessor returned nil")
	}
}

func TestClientMalformedFrameFencesTransport(t *testing.T) {
	gateway := newWSGateway(t)
	gateway.malformedAtStart = true
	client, err := Dial(t.Context(), gateway.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()

	<-client.Deliveries()
	<-client.Deliveries()
	if delivery := <-client.Deliveries(); delivery.Err == nil {
		t.Fatal("malformed frame did not fence the transport")
	}
	if _, ok := <-client.Deliveries(); ok {
		t.Fatal("delivery stream remained open after malformed frame")
	}
}

func TestClientMalformedEventFencesTransport(t *testing.T) {
	gateway := newWSGateway(t)
	gateway.malformedEvent = true
	client, err := Dial(t.Context(), gateway.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()

	<-client.Deliveries()
	<-client.Deliveries()
	if delivery := <-client.Deliveries(); delivery.Err == nil {
		t.Fatal("malformed event did not fence the transport")
	}
}

func TestClientRejectsEveryInvalidTopLevelGatewayShape(t *testing.T) {
	for name, frame := range map[string]string{
		"non-object":       `null`,
		"invalid method":   `{"jsonrpc":"2.0","method":1}`,
		"invalid response": `{"jsonrpc":"2.0","id":"not-an-integer","result":{}}`,
		"unclassified":     `{"jsonrpc":"2.0"}`,
	} {
		t.Run(name, func(t *testing.T) {
			gateway := newWSGateway(t)
			gateway.startupRaw = []byte(frame)
			client, err := Dial(t.Context(), gateway.url(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()

			<-client.Deliveries()
			<-client.Deliveries()
			if delivery := <-client.Deliveries(); delivery.Err == nil {
				t.Fatalf("invalid gateway frame was not terminal: %s", frame)
			}
		})
	}
}

func TestUnsupportedGatewayRequestWriteFailureIsReturned(t *testing.T) {
	gateway := newWSGateway(t)
	conn, response, err := websocket.Dial(t.Context(), gateway.url(), nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.CloseNow()
	client := &Client{conn: conn, deliveries: make(chan GatewayDelivery, 1)}
	if client.rejectUnsupportedRequest(json.RawMessage(`77`)) {
		t.Fatal("unsupported-request rejection claimed success on a closed transport")
	}
	if delivery := <-client.Deliveries(); delivery.Err == nil {
		t.Fatal("unsupported-request write failure did not terminalize the transport")
	}
	client.publishTerminal(errors.New("duplicate"))
	if len(client.deliveries) != 0 {
		t.Fatal("transport published more than one terminal delivery")
	}
}

func TestClientRawEventOverflowFencesTransport(t *testing.T) {
	gateway := newWSGateway(t)
	gateway.overflowAtStart = true
	client, err := Dial(t.Context(), gateway.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()

	<-client.Done()

	foundTerminal := false
	for delivery := range client.Deliveries() {
		if delivery.Err == nil {
			continue
		}
		if !errors.Is(delivery.Err, ErrGatewayInputOverflow) {
			t.Fatalf("overflow error = %v, want %v", delivery.Err, ErrGatewayInputOverflow)
		}

		foundTerminal = true

		break
	}
	if !foundTerminal {
		t.Fatal("overflow terminal was not delivered through its reserved slot")
	}
}

func TestClientMalformedKnownRPCResponseFencesTransport(t *testing.T) {
	for _, method := range []string{"bad-rpc", "empty-result", "wrong-version", "result-and-error"} {
		t.Run(method, func(t *testing.T) {
			gateway := newWSGateway(t)
			client, err := Dial(t.Context(), gateway.url(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()
			<-client.Deliveries()
			<-client.Deliveries()

			callErr := client.Call(t.Context(), method, nil, nil)
			if callErr == nil {
				t.Fatal("malformed known response terminalized its call successfully")
			}
			delivery := <-client.Deliveries()
			if delivery.Err == nil {
				t.Fatal("malformed known response omitted its terminal error")
			}
			if !errors.Is(callErr, ErrGatewayDisconnected) || !errors.Is(callErr, delivery.Err) {
				t.Fatalf("pending call cause = %v, terminal = %v", callErr, delivery.Err)
			}
		})
	}
}

func TestClientConcurrentCloseJoinsOneExactTransportAttempt(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	want := errors.New("close failed")
	var calls int
	var mu sync.Mutex
	client := &Client{closeTransport: func(websocket.StatusCode, string) error {
		mu.Lock()
		calls++
		mu.Unlock()
		close(entered)
		<-release

		return want
	}}

	closed := make(chan error, 2)
	go func() { closed <- client.Close(websocket.StatusInternalError, "first") }()
	<-entered
	go func() { closed <- client.Close(websocket.StatusNormalClosure, "second") }()

	select {
	case err := <-closed:
		t.Fatalf("close returned while its transport attempt was live: %v", err)
	default:
	}
	close(release)
	first := <-closed
	second := <-closed
	if first != second || !errors.Is(first, want) {
		t.Fatalf("close results = %v and %v, want one exact %v", first, second, want)
	}
	if got := client.Close(websocket.StatusNormalClosure, "later"); got != first {
		t.Fatalf("memoized close result = %v, want exact %v", got, first)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("transport close calls = %d, want 1", calls)
	}
}

func TestClientAcceptedResponseWinsAtDoneResultCheck(t *testing.T) {
	gateway := newWSGateway(t)
	client, err := Dial(t.Context(), gateway.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()
	<-client.Deliveries()
	<-client.Deliveries()

	var waiter chan rpcResponse
	client.beforeCallWait = func() {
		client.mu.Lock()
		waiter = client.pending[1]
		client.mu.Unlock()
		<-client.Done()
	}
	client.beforeDoneResultCheck = func() {
		waiter <- rpcResponse{JSONRPC: jsonrpcVersion, ID: 1, Result: json.RawMessage(`{"value":"accepted"}`)}
	}
	var result struct {
		Value string `json:"value"`
	}
	if err := client.Call(t.Context(), "close", nil, &result); err != nil {
		t.Fatal(err)
	}
	if result.Value != "accepted" {
		t.Fatalf("result = %q, want accepted", result.Value)
	}
}

func TestClientRPCOverflowPreservesTransportCause(t *testing.T) {
	gateway := newWSGateway(t)
	client, err := Dial(t.Context(), gateway.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()
	<-client.Deliveries()
	<-client.Deliveries()

	callErr := client.Call(t.Context(), "overflow", nil, nil)
	if !errors.Is(callErr, ErrGatewayDisconnected) || !errors.Is(callErr, ErrGatewayInputOverflow) {
		t.Fatalf("overflowing pending call = %v", callErr)
	}
}

func TestClientExactEnqueuedResponsesWinOverLaterEOFInReverseIDOrder(t *testing.T) {
	gateway := newWSGateway(t)
	gateway.reverseEOF = true
	client, err := Dial(t.Context(), gateway.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()
	<-client.Deliveries()
	<-client.Deliveries()

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	client.beforeCallWait = func() {
		entered <- struct{}{}
		<-release
	}
	type callResult struct {
		value string
		err   error
	}
	results := make(chan callResult, 2)
	for _, value := range []string{"first", "second"} {
		go func() {
			var out struct {
				Value string `json:"value"`
			}
			err := client.Call(t.Context(), "reverse-eof", map[string]any{"value": value}, &out)
			results <- callResult{value: out.Value, err: err}
		}()
	}
	<-entered
	<-entered
	<-client.Done()
	close(release)

	got := map[string]bool{}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("exact response lost to EOF: %v", result.err)
		}
		got[result.value] = true
	}
	if !got["first"] || !got["second"] {
		t.Fatalf("reverse-ID results = %v", got)
	}
}

func TestClientClassifiesCrossDirectionRequestBeforeResponseID(t *testing.T) {
	gateway := newWSGateway(t)
	client, err := Dial(t.Context(), gateway.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "test") }()
	<-client.Deliveries()
	<-client.Deliveries()

	var out struct {
		Value string `json:"value"`
	}
	if err := client.Call(t.Context(), "cross-direction", nil, &out); err != nil {
		t.Fatal(err)
	}
	if out.Value != "first" {
		t.Fatalf("cross-direction result = %q", out.Value)
	}

	// The duplicate response found no pending waiter and did not wedge the sole
	// reader; a later call still completes normally.
	if err := client.Call(t.Context(), "later", nil, nil); err != nil {
		t.Fatalf("call after duplicate response: %v", err)
	}
}

func assertClientWrappers(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()

	if out, err := client.CreateSession(ctx, map[string]any{"cwd": "/repo"}); err != nil || out.SessionID != "live" {
		t.Fatalf("CreateSession = %#v err=%v", out, err)
	}
	if out, err := client.ResumeSession(ctx, "stored", nil); err != nil || out.SessionKey != "stored" {
		t.Fatalf("ResumeSession nil params = %#v err=%v", out, err)
	}
	if out, err := client.SetSessionTitle(ctx, "live", "Durable"); err != nil || out.Pending || out.Title != "Durable" {
		t.Fatalf("SetSessionTitle = %#v err=%v", out, err)
	}
	if out, err := client.History(ctx, "live"); err != nil || len(out.Messages) != 1 || out.Messages[0].Text != "hello" {
		t.Fatalf("History = %#v err=%v", out, err)
	}
	if out, err := client.ActiveList(ctx); err != nil || len(out.Sessions) != 1 {
		t.Fatalf("ActiveList = %#v err=%v", out, err)
	}
	if err := client.DeleteSession(ctx, "stored"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if err := client.CloseSession(ctx, "live"); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if out, err := client.Branch(ctx, "live", "name"); err != nil || out.SessionID != "live-branch" || out.StoredSessionID != "stored-branch" || out.Title != "Branch" || out.Parent != "stored" {
		t.Fatalf("Branch = %#v err=%v", out, err)
	}
	if err := client.SubmitPrompt(ctx, "live", "hello"); err != nil {
		t.Fatalf("SubmitPrompt: %v", err)
	}
	if err := client.AttachImageBytes(ctx, "live", []byte{0}); err != nil {
		t.Fatalf("AttachImageBytes: %v", err)
	}
	if err := client.Interrupt(ctx, "live"); err != nil {
		t.Fatalf("Interrupt: %v", err)
	}
	if err := client.ApprovalRespond(ctx, "live", "once", false); err != nil {
		t.Fatalf("ApprovalRespond: %v", err)
	}
	if err := client.ClarifyRespond(ctx, "live", "request-1", "yes"); err != nil {
		t.Fatalf("ClarifyRespond: %v", err)
	}
	if err := client.ClarifyRespond(ctx, "live", "", "yes"); err == nil {
		t.Fatal("ClarifyRespond accepted an empty request_id")
	}
	if out, err := client.ModelOptions(ctx, "live"); err != nil || len(out.Providers) != 2 {
		t.Fatalf("ModelOptions = %#v err=%v", out, err)
	}
	if err := client.SetModel(ctx, "live", "anthropic/claude-sonnet-4"); err != nil {
		t.Fatalf("SetModel: %v", err)
	}
	if err := client.SetModel(ctx, "live", "openrouter/x-ai/grok-4.5"); err != nil {
		t.Fatalf("SetModel aggregator: %v", err)
	}
	if err := client.SetModel(ctx, "live", "provider/deferred"); err != nil {
		t.Fatalf("SetModel deferred accepted mutation: %v", err)
	}
	if err := client.SetModel(ctx, "live", "provider/mismatch"); err == nil {
		t.Fatal("SetModel accepted a different returned model")
	}
}

func assertClientCallEdges(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()

	if err := client.Call(ctx, "missing", nil, nil); !IsNotFound(err) {
		t.Fatalf("missing error = %v", err)
	}
	var bad map[string]any
	if err := client.Call(ctx, "bad-result", nil, &bad); err == nil {
		t.Fatal("bad result unexpectedly decoded")
	}
	if err := client.Call(ctx, "marshal", map[string]any{"bad": func() {}}, nil); err == nil {
		t.Fatal("marshal error was nil")
	}
}

func assertClientCloseSemantics(t *testing.T, ctx context.Context, client *Client, gateway *wsGateway) {
	t.Helper()

	rawConn, resp, rawErr := websocket.Dial(ctx, gateway.url(), nil)
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	if rawErr != nil {
		t.Fatalf("dial raw write client: %v", rawErr)
	}
	_ = rawConn.CloseNow()
	writeClient := &Client{conn: rawConn, pending: map[int64]chan rpcResponse{}}
	if err := writeClient.Call(ctx, "session.active_list", nil, nil); err == nil {
		t.Fatal("closed websocket write error was nil")
	}
	shortCtx, shortCancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer shortCancel()
	if err := client.Call(shortCtx, "hang", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hang error = %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	waitDone := make(chan error, 1)
	hangCount := gatewayCallCount(gateway, "hang")
	go func() { waitDone <- client.Call(waitCtx, "hang", nil, nil) }()
	waitForGatewayCallCount(t, gateway, "hang", hangCount+1)
	if err := client.Close(websocket.StatusNormalClosure, "close pending"); err != nil {
		t.Fatalf("Close pending: %v", err)
	}
	if err := <-waitDone; err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("pending close error = %v", err)
	}
	if err := client.Close(websocket.StatusNormalClosure, "test"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(websocket.StatusNormalClosure, "again"); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := client.Call(ctx, "session.active_list", nil, nil); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed Call error = %v", err)
	}
	reconnected, dialErr := Dial(ctx, gateway.url(), nil)
	if dialErr != nil {
		t.Fatalf("redial: %v", dialErr)
	}
	waitLoopDone := make(chan error, 1)
	go func() { waitLoopDone <- reconnected.Call(context.Background(), "close", nil, nil) }()
	if err := <-waitLoopDone; !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("read loop pending close error = %v", err)
	}
}

func TestModelSwitchCommandUsesRawModelAndExplicitSessionProvider(t *testing.T) {
	command, raw, err := modelSwitchCommand("openrouter/x-ai/grok-4.5")
	if err != nil {
		t.Fatal(err)
	}
	if raw != "x-ai/grok-4.5" || command != "x-ai/grok-4.5 --provider openrouter --session" {
		t.Fatalf("model switch command/raw = %q/%q", command, raw)
	}
	for _, invalid := range []string{"", "unqualified", "/model", "provider/", "provider/model\x00suffix", "custom/provider model", "--provider/model", "provider/--session"} {
		if _, _, err := modelSwitchCommand(invalid); err == nil {
			t.Fatalf("invalid model selection %q accepted", invalid)
		}
		// Callers with no command to build ask the same question through the
		// exported predicate, so the wrapper's doors cannot answer the shape of a
		// selection differently from the setter that would have to send it.
		if err := ModelSelectionShapeError(invalid); err == nil {
			t.Fatalf("invalid model selection %q passed the exported shape predicate", invalid)
		}
	}
	if err := ModelSelectionShapeError("openrouter/x-ai/grok-4.5"); err != nil {
		t.Fatalf("provider-qualified selection refused by the exported shape predicate: %v", err)
	}
}

func TestClientDialAndJSONBranches(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
	}{
		{name: "malformed", raw: `{`},
		{name: "invalid id", raw: `{"jsonrpc":"2.0","id":"x","result":{}}`},
		{name: "null error", raw: `{"jsonrpc":"2.0","id":1,"error":null}`},
	} {
		t.Run("known response "+test.name, func(t *testing.T) {
			if _, err := decodeKnownRPCResponse([]byte(test.raw), 1); err == nil {
				t.Fatalf("accepted malformed known response %s", test.raw)
			}
		})
	}

	if (&RPCError{}).Error() == "" {
		t.Fatal("empty RPCError string was empty")
	}
	var nilErr *RPCError
	if nilErr.Error() != "" {
		t.Fatal("nil RPCError string was not empty")
	}
	if IsNotFound(errors.New("no")) {
		t.Fatal("plain error was not found")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("not websocket"))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil); err == nil {
		t.Fatal("Dial unexpectedly succeeded")
	}
	if err := (&Client{closed: true}).Call(ctx, "closed", nil, nil); err == nil {
		t.Fatal("Call on closed client succeeded")
	}
	if err := (&Client{pending: map[int64]chan rpcResponse{}}).Call(ctx, "bad", map[string]float64{"inf": math.Inf(1)}, nil); err == nil {
		t.Fatal("Call accepted unmarshalable params")
	}

	var message Message
	if err := message.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("Message accepted malformed JSON")
	}
	var options ModelOptionsResult
	if err := options.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("ModelOptionsResult accepted malformed JSON")
	}
	var active ActiveSession
	if err := json.Unmarshal([]byte(`{"id":"live-child","session_key":"stored-child","title":"Child","cwd":"/repo"}`), &active); err != nil ||
		active.SessionID != "live-child" || active.SessionKey != "stored-child" {
		t.Fatalf("ActiveSession real shape = %#v err=%v", active, err)
	}
	active = ActiveSession{}
	if err := json.Unmarshal([]byte(`{"session_id":"live-old","session_key":"stored-old"}`), &active); err != nil ||
		active.SessionID != "" || active.SessionKey != "stored-old" {
		t.Fatalf("ActiveSession accepted wrong method shape = %#v err=%v", active, err)
	}
	if err := json.Unmarshal([]byte("{"), &active); err == nil {
		t.Fatal("ActiveSession accepted malformed JSON")
	}
	var provider Provider
	if err := provider.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("Provider accepted malformed JSON")
	}
	if err := provider.UnmarshalJSON([]byte(`{"slug":"openrouter","name":"OpenRouter","authenticated":true,"is_current":false,"is_user_defined":false,"source":"built-in","total_models":1,"models":["anthropic/claude-fable-5"],"capabilities":{"anthropic/claude-fable-5":{"fast":false,"reasoning":true}},"pricing":{"anthropic/claude-fable-5":{"cache":"$1.00","free":false,"input":"$10.00","output":"$50.00"}}}`)); err != nil ||
		provider.Slug != "openrouter" || provider.Name != "OpenRouter" || len(provider.Models) != 1 || provider.Models[0] != "anthropic/claude-fable-5" {
		t.Fatalf("Provider slug shape = %#v err=%v", provider, err)
	}
	for _, raw := range []string{
		`{"id":"openrouter","models":["anthropic/claude-fable-5"]}`,
		`{"slug":"openrouter","models":[{"id":"anthropic/claude-fable-5"}]}`,
		`{"slug":"openrouter","models":{"anthropic/claude-fable-5":{}}}`,
		`{"slug":"openrouter","models":"anthropic/claude-fable-5"}`,
		`{"slug":"openrouter"}`,
		`{"slug":"openrouter","models":null}`,
	} {
		if err := provider.UnmarshalJSON([]byte(raw)); err == nil {
			t.Fatalf("Provider accepted non-real model.options shape: %s", raw)
		}
	}
}

func TestProcessStartCloseAndHelpers(t *testing.T) {
	// This case starts three separately-pathed executables, and every contained
	// start re-executes this very test binary as its supervisor. Under -race that
	// binary costs about a second of race-runtime startup per exec — measured at
	// 1011ms against 2ms for the same binary built without -race — so the three
	// starts and their version probes spend the better part of ten seconds doing
	// nothing but bringing supervisors up. The coverage gate runs -race in the
	// initial PID namespace, where the descendant and vacancy sweeps also walk
	// the host's full process table. The work is bounded by the fixed number of
	// launches this case makes, so the budget is sized for those launches rather
	// than for the wall clock a smaller one would allow.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	usedConfigure := false
	proc, startErr := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
		Cwd:            t.TempDir(),
		Env:            map[string]string{"BASE_ENV": "1", "HERMES_WEB_DIST": "1"},
		Timeout:        5 * time.Second,
		LogWriter:      io.Discard,
		Configure: func(cmd *exec.Cmd) {
			usedConfigure = true
			cmd.Env = append(cmd.Env, "CONFIGURED=1")
		},
	}))
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}
	if !usedConfigure {
		t.Fatal("Configure hook was not called")
	}
	if proc.Client == nil || proc.Port <= 0 || proc.Token == "" || !strings.Contains(proc.StatusURL, "/api/status") {
		t.Fatalf("process = %#v", proc)
	}
	if err := proc.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	resumeDomainProc, resumeErr := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, "probe-domain:session.resume"),
		Home:           t.TempDir(),
		Timeout:        5 * time.Second,
	}))
	if resumeErr != nil {
		t.Fatalf("Start with resume domain probe: %v", resumeErr)
	}
	if err := resumeDomainProc.Close(ctx); err != nil {
		t.Fatalf("Close resume domain proc: %v", err)
	}
	deleteDomainProc, deleteErr := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, "probe-domain:session.delete"),
		Home:           t.TempDir(),
		Timeout:        5 * time.Second,
	}))
	if deleteErr != nil {
		t.Fatalf("Start with delete domain probe: %v", deleteErr)
	}
	if err := deleteDomainProc.Close(ctx); err != nil {
		t.Fatalf("Close delete domain proc: %v", err)
	}
	if err := (&Process{}).Close(ctx); err != nil {
		t.Fatalf("empty Close: %v", err)
	}

	assertProcessScalarHelpers(t, ctx)
	assertProcessStartSeams(t, ctx)
}

func TestProcessSupervisorShutdownRetainsContainmentProof(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	proc, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
		Timeout:        5 * time.Second,
		LogWriter:      io.Discard,
	}))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = proc.Close(context.Background()) })

	if err := proc.tree.kill(proc.Cmd); err != nil {
		t.Fatalf("kill spontaneous process: %v", err)
	}

	select {
	case <-proc.waitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("spontaneously exited Hermes process was not reaped")
	}

	if proc.Cmd.ProcessState == nil {
		t.Fatal("process waiter completed without recording process state")
	}
	if err := proc.Close(ctx); err != nil {
		t.Fatalf("Close after supervised shutdown = %v", err)
	}
}

func TestProcessBackedSessionCLICarrierAtNativeBoundary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the process-backed fixture uses a POSIX executable shim")
	}

	operationOne := t.TempDir()
	operationTwo := t.TempDir()
	nativeOne := t.TempDir()
	nativeTwo := t.TempDir()
	capturePath := filepath.Join(t.TempDir(), "carrier.json")
	wagiePath := filepath.Join(operationOne, "wagie")
	wagieBody := []byte("#!/bin/sh\nprintf '%s:%s' \"$WAGIE_OPERATION_ID\" \"$WAGIE_API_TOKEN\"\n")
	if err := os.WriteFile(wagiePath, wagieBody, 0o700); err != nil {
		t.Fatal(err)
	}

	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeSessionCLI),
		Home:           t.TempDir(),
		SessionEnv: map[string]string{
			"ACP_GO_HERMES_TEST_ROOT": capturePath,
			"WAGIE_API_TOKEN":         "bearer-one",
			"WAGIE_OPERATION_ID":      "operation-one",
		},
		ExtraPathDirs: []string{operationOne, operationTwo},
		Timeout:       10 * time.Second,
		LogWriter:     io.Discard,
	})
	options.AmbientEnvironment = map[string]string{
		"PATH": string(os.PathListSeparator) + nativeOne + string(os.PathListSeparator) + string(os.PathListSeparator) + nativeTwo + string(os.PathListSeparator),
	}

	proc, err := Start(t.Context(), options)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = proc.Close(context.Background()) })

	data, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatalf("read native capture: %v", err)
	}
	var capture fakeSessionCLICapture
	if err := json.Unmarshal(data, &capture); err != nil {
		t.Fatalf("decode native capture: %v", err)
	}
	if capture.Resolved != wagiePath || capture.Token != "bearer-one" || capture.OperationID != "operation-one" || capture.Output != "operation-one:bearer-one" {
		t.Fatalf("native carrier capture = %#v", capture)
	}

	wantPath := []string{operationOne, operationTwo}
	if proc.shim != nil {
		wantPath = append(wantPath, proc.shim.dir)
	}
	wantPath = append(wantPath, nativeOne, nativeTwo)
	if capture.Path != strings.Join(wantPath, string(os.PathListSeparator)) {
		t.Fatalf("native PATH = %q, want %q", capture.Path, strings.Join(wantPath, string(os.PathListSeparator)))
	}
	if slices := strings.Split(capture.Path, string(os.PathListSeparator)); slices[0] != operationOne || containsString(slices, "") {
		t.Fatalf("native PATH components = %#v", slices)
	}
}

func assertProcessScalarHelpers(t *testing.T, ctx context.Context) {
	t.Helper()

	if port, err := freePort(); err != nil || port <= 0 {
		t.Fatalf("freePort = %d err=%v", port, err)
	}
	if token, err := randomToken(); err != nil || token == "" {
		t.Fatalf("randomToken = %q err=%v", token, err)
	}
	if compareVersions("1.2.3", "1.2.2") <= 0 || compareVersions("1.2.3", "1.2.3") != 0 {
		t.Fatal("compareVersions mismatch")
	}
	markExecutableProbed("already-probed")
	if err := ensureExecutableVersion(ctx, "already-probed", ProcessOptions{}); err != nil {
		t.Fatalf("cached executable probe err=%v", err)
	}
	if err := methodPresent("domain.method", &RPCError{Code: 4001, Message: "domain"}); err != nil {
		t.Fatalf("methodPresent rejected domain RPC error: %v", err)
	}
	if err := methodPresent("missing.method", &RPCError{Code: -32601, Message: "missing"}); err == nil {
		t.Fatal("methodPresent accepted missing method")
	}
	if err := methodPresent("broken.method", errors.New("broken")); err == nil {
		t.Fatal("methodPresent accepted ordinary error")
	}
}

func TestExecutableVersionProbeUsesGenerationHome(t *testing.T) {
	restoreProcessSeams(t)
	originalCommand := command
	originalRemoveAll := removeAll
	t.Cleanup(func() {
		command = originalCommand
		removeAll = originalRemoveAll
	})

	probeRoot := filepath.Join(t.TempDir(), "probe-root")
	mkdirTemp = func(string, string) (string, error) { return probeRoot, nil }
	command = func(string, ...string) *exec.Cmd { return &exec.Cmd{} }
	wantStart := errors.New("probe start failed")
	var capturedEnv []string
	startHermesContainedProcess = func(cmd *exec.Cmd, specs ...ContainmentSpec) (*processContainment, error) {
		capturedEnv = append([]string(nil), cmd.Env...)
		if len(specs) != 1 || specs[0].GenerationRoot != probeRoot {
			t.Fatalf("containment specs = %+v", specs)
		}

		return nil, wantStart
	}
	var handedOff string
	processNativeTreeHandoff = func(root string, isolation *ProcessIsolation) error {
		handedOff = root
		// Ordinary mode owns its generation already, so the handoff is reached
		// with no policy and surrenders nothing.
		if isolation != nil {
			t.Fatalf("ordinary probe handoff carried a policy %+v", isolation)
		}

		return nil
	}
	removed := ""
	removeAll = func(root string) error {
		removed = root

		return nil
	}
	nativeReleases, scratchReleases := 0, 0
	err := ensureExecutableVersion(t.Context(), t.Name(), ProcessOptions{
		// An ambient HERMES_HOME is adapter-managed state: it is scrubbed out
		// of the inherited environment and replaced with the probe generation.
		AmbientEnvironment: map[string]string{"PATH": os.Getenv("PATH"), "HERMES_HOME": "/account-home"},
		Env:                map[string]string{"STATIC_CARRIER": "base"},
		SessionEnv:         map[string]string{"WAGIE_API_TOKEN": "must-not-reach-probe"},
		ExtraPathDirs:      []string{t.TempDir()},
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) {
			return func() { nativeReleases++ }, func() { scratchReleases++ }, nil
		},
		RetainDiscoveryRoot: func(string, error) {},
	})
	if !errors.Is(err, wantStart) {
		t.Fatalf("version probe error = %v", err)
	}
	if got := envValue(capturedEnv, "HERMES_HOME"); got != probeRoot {
		t.Fatalf("HERMES_HOME = %q, want %q", got, probeRoot)
	}
	if got := envValue(capturedEnv, "STATIC_CARRIER"); got != "base" {
		t.Fatalf("STATIC_CARRIER = %q, want base", got)
	}
	if got := envValue(capturedEnv, "WAGIE_API_TOKEN"); got != "" {
		t.Fatalf("session bearer reached version probe: %q", got)
	}
	if handedOff != probeRoot || removed != probeRoot {
		t.Fatalf("handoff=%q remove=%q, want %q", handedOff, removed, probeRoot)
	}
	if nativeReleases != 1 || scratchReleases != 1 {
		t.Fatalf("releases native=%d scratch=%d", nativeReleases, scratchReleases)
	}
}

func TestExecutableVersionProbeHandoffFailure(t *testing.T) {
	for _, removeErr := range []error{nil, errors.New("remove failed")} {
		t.Run(fmt.Sprint(removeErr), func(t *testing.T) {
			restoreProcessSeams(t)
			originalCommand := command
			originalRemoveAll := removeAll
			t.Cleanup(func() {
				command = originalCommand
				removeAll = originalRemoveAll
			})

			probeRoot := filepath.Join(t.TempDir(), "probe-root")
			mkdirTemp = func(string, string) (string, error) { return probeRoot, nil }
			command = func(string, ...string) *exec.Cmd { return &exec.Cmd{} }
			wantHandoff := errors.New("handoff failed")
			processNativeTreeHandoff = func(root string, _ *ProcessIsolation) error {
				if root != probeRoot {
					t.Fatalf("handoff root = %q", root)
				}

				return wantHandoff
			}
			startHermesContainedProcess = func(*exec.Cmd, ...ContainmentSpec) (*processContainment, error) {
				t.Fatal("probe started after handoff failure")

				return nil, errors.New("unreachable")
			}
			removeAll = func(root string) error {
				if root != probeRoot {
					t.Fatalf("remove root = %q", root)
				}

				return removeErr
			}
			nativeReleases, scratchReleases := 0, 0
			err := ensureExecutableVersion(t.Context(), t.Name(), ProcessOptions{
				AmbientEnvironment: testAmbientEnvironment(),
				AcquireDiscoveryResources: func(context.Context) (func(), func(), error) {
					return func() { nativeReleases++ }, func() { scratchReleases++ }, nil
				},
				RetainDiscoveryRoot: func(string, error) {},
			})
			if !errors.Is(err, wantHandoff) || (removeErr != nil && !errors.Is(err, removeErr)) {
				t.Fatalf("version probe handoff error = %v", err)
			}
			if nativeReleases != 1 {
				t.Fatalf("native releases = %d", nativeReleases)
			}
			wantScratchReleases := 1
			if removeErr != nil {
				wantScratchReleases = 0
			}
			if scratchReleases != wantScratchReleases {
				t.Fatalf("scratch releases = %d, want %d", scratchReleases, wantScratchReleases)
			}
		})
	}
}

func assertProcessStartSeams(t *testing.T, ctx context.Context) {
	t.Helper()

	restoreProcessSeams(t)
	commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if filepath.Base(name) != "hermes" {
			t.Fatalf("default executable = %q", name)
		}

		return exec.CommandContext(ctx, filepath.Join(t.TempDir(), "missing-hermes"), args...)
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{Home: t.TempDir()})); err == nil {
		t.Fatal("Start default executable seam unexpectedly succeeded")
	}

	restoreProcessSeams(t)
	commandContext = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "--version" {
			return exec.CommandContext(ctx, "sh", "-c", "printf 'Hermes Agent v0.20.0\\n'")
		}

		return exec.CommandContext(ctx, filepath.Join(t.TempDir(), "missing-hermes"), args...)
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: "start-fails", Home: t.TempDir()})); err == nil {
		t.Fatal("Start command failure was ignored")
	}
}

func TestProcessFaultBranches(t *testing.T) {
	ctx := context.Background()

	assertStartFaultModes(t, ctx)

	restoreProcessSeams(t)
	processNativeTreeHandoff = func(string, *ProcessIsolation) error { return errors.New("handoff failed") }
	handoffExecutable := fakeHermesExecutable(t, fakeProcessModeOK)
	markExecutableProbed(handoffExecutable)
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: handoffExecutable, Home: t.TempDir(),
	})); err == nil || !strings.Contains(err.Error(), "handoff failed") {
		t.Fatalf("Start handoff error = %v", err)
	}

	restoreProcessSeams(t)
	mkdirTemp = func(string, string) (string, error) { return "", errors.New("mktemp failed") }
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK)})); err == nil {
		t.Fatal("mktemp error ignored")
	}

	restoreProcessSeams(t)
	listenTCP = func(string, string) (net.Listener, error) { return nil, errors.New("listen failed") }
	if _, err := freePort(); err == nil {
		t.Fatal("freePort listen error ignored")
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK), Home: t.TempDir()})); err == nil {
		t.Fatal("Start ignored freePort error")
	}

	resetProcessSeams()
	restoreProcessSeams(t)
	randReader = errorReader{err: errors.New("entropy failed")}
	if _, err := randomToken(); err == nil {
		t.Fatal("randomToken entropy error ignored")
	}
	entropyExecutable := fakeHermesExecutable(t, fakeProcessModeOK)
	markExecutableProbed(entropyExecutable)
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: entropyExecutable, Home: t.TempDir()})); err == nil {
		t.Fatal("Start ignored randomToken error")
	}

	resetProcessSeams()
	restoreProcessSeams(t)
	userHomeDir = func() (string, error) { return "", errors.New("home failed") }
	if defaultWebDistExists() {
		t.Fatal("defaultWebDistExists ignored home error")
	}
	restoreProcessSeams(t)
	home := t.TempDir()
	userHomeDir = func() (string, error) { return home, nil }
	statPath = func(string) (os.FileInfo, error) { return nil, errors.New("stat failed") }
	if defaultWebDistExists() {
		t.Fatal("defaultWebDistExists ignored stat error")
	}
	restoreProcessSeams(t)
	userHomeDir = func() (string, error) { return home, nil }
	statPath = func(string) (os.FileInfo, error) { return fakeFileInfo{dir: false}, nil }
	if defaultWebDistExists() {
		t.Fatal("defaultWebDistExists accepted file")
	}
	restoreProcessSeams(t)
	userHomeDir = func() (string, error) { return home, nil }
	statPath = func(string) (os.FileInfo, error) { return fakeFileInfo{dir: true}, nil }
	if !defaultWebDistExists() {
		t.Fatal("defaultWebDistExists rejected dir")
	}

	resetProcessSeams()
	assertProcessWaitBranches(t, ctx)
}

func assertStartFaultModes(t *testing.T, ctx context.Context) {
	t.Helper()

	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: filepath.Join(t.TempDir(), "missing-hermes"), Home: t.TempDir()})); err == nil {
		t.Fatal("missing executable unexpectedly started")
	}
	fileHome := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(fileHome, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK), Home: fileHome})); err == nil {
		t.Fatal("file home unexpectedly started")
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeStatusOnly), Home: t.TempDir(), Timeout: 5 * time.Second})); err == nil {
		t.Fatal("missing websocket unexpectedly passed")
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeNoGatewayReady), Home: t.TempDir(), Timeout: 5 * time.Second})); err == nil {
		t.Fatal("missing gateway.ready unexpectedly passed")
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeBadStatus), Home: t.TempDir(), Timeout: 5 * time.Second})); err == nil {
		t.Fatal("bad status unexpectedly passed")
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOldVersion), Home: t.TempDir(), Timeout: 10 * time.Second})); err == nil ||
		!strings.Contains(err.Error(), "below minimum") {
		t.Fatalf("old version error = %v", err)
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeBadVersion), Home: t.TempDir(), Timeout: 10 * time.Second})); err == nil ||
		!strings.Contains(err.Error(), "missing semantic version") {
		t.Fatalf("bad version error = %v", err)
	}
	if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeMissingMethod), Home: t.TempDir(), Timeout: 10 * time.Second})); err == nil ||
		!strings.Contains(err.Error(), "model.options") {
		t.Fatalf("missing method probe error = %v", err)
	}
	sentinelProcess, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeProbeSentinel),
		Home:           t.TempDir(),
		Timeout:        10 * time.Second,
	}))
	if err != nil {
		t.Fatalf("presence probes activated the real startup session: %v", err)
	}
	if err := sentinelProcess.Close(ctx); err != nil {
		t.Fatalf("close sentinel probe process: %v", err)
	}
	for _, tt := range []struct {
		mode string
		want string
	}{
		{"probe-error:session.create", "session.create"},
		{"probe-empty:session.create", "session.create schema drift"},
		{"probe-error:session.resume", "session.resume"},
		{"probe-empty:session.resume", "session.resume schema drift"},
		{"probe-error:session.active_list", "session.active_list"},
		{"probe-empty:session.active_list", "session.active_list schema drift"},
		{"probe-empty:model.options", "model.options schema drift"},
		{"probe-error:image.attach_bytes", "image.attach_bytes"},
		{"probe-empty:image.attach_bytes", "image.attach_bytes did not attach image"},
		{"probe-error:prompt.submit", "prompt.submit"},
		{"probe-error:approval.respond", "approval.respond"},
		{"probe-error:clarify.respond", "clarify.respond"},
		{"probe-error:session.close", "session.close"},
		{"probe-error:session.delete", "session.delete"},
	} {
		t.Run(tt.mode, func(t *testing.T) {
			if _, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, tt.mode), Home: t.TempDir(), Timeout: 10 * time.Second})); err == nil ||
				!strings.Contains(err.Error(), tt.want) {
				t.Fatalf("probe mode %s error = %v", tt.mode, err)
			}
		})
	}
}

func assertProcessWaitBranches(t *testing.T, ctx context.Context) {
	t.Helper()

	if err := (&Process{StatusURL: ":// bad"}).waitReady(ctx); err == nil {
		t.Fatal("waitReady accepted invalid status URL")
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := (&Process{StatusURL: "http://127.0.0.1:1/api/status"}).waitReady(shortCtx); err == nil ||
		!strings.Contains(err.Error(), "status check failed") {
		t.Fatalf("waitReady connection error = %v", err)
	}
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer statusServer.Close()
	statusCtx, cancelStatus := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelStatus()
	if err := (&Process{StatusURL: statusServer.URL}).waitReady(statusCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitReady bad status error = %v", err)
	}
	waitDone := make(chan struct{})
	close(waitDone)
	exitCtx, exitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer exitCancel()
	if err := (&Process{
		StatusURL: "http://127.0.0.1:1/api/status",
		waitDone:  waitDone,
	}).waitReady(exitCtx); err == nil || !strings.Contains(err.Error(), "process exited before readiness") {
		t.Fatalf("waitReady exited process error = %v", err)
	}
	testExecutable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exited := exec.Command(testExecutable, "-test.run=^$")
	if output, err := exited.CombinedOutput(); err != nil {
		t.Fatalf("run exited process fixture: %v: %s", err, output)
	}
	exitedDone := make(chan struct{})
	close(exitedDone)
	stateCtx, stateCancel := context.WithTimeout(ctx, 5*time.Second)
	defer stateCancel()
	if err := (&Process{
		Cmd:       exited,
		StatusURL: "http://127.0.0.1:1/api/status",
		waitDone:  exitedDone,
	}).waitReady(stateCtx); err == nil || !strings.Contains(err.Error(), "process exited before readiness:") {
		t.Fatalf("waitReady process-state error = %v", err)
	}

	closedEvents := &Client{deliveries: make(chan GatewayDelivery)}
	close(closedEvents.deliveries)
	if err := (&Process{Client: closedEvents}).waitGatewayReady(ctx); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("waitGatewayReady closed events err = %v", err)
	}
	errorClient := &Client{deliveries: make(chan GatewayDelivery, 1)}
	errorClient.deliveries <- GatewayDelivery{Err: errors.New("gateway failed")}
	if err := (&Process{Client: errorClient}).waitGatewayReady(ctx); err == nil || !strings.Contains(err.Error(), "gateway failed") {
		t.Fatalf("waitGatewayReady error err = %v", err)
	}
}

func TestProcessCloseFaultBranches(t *testing.T) {
	restoreProcessSeams(t)
	waitProcessCommand = func(*exec.Cmd) error {
		select {}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	firstTimer := true
	afterCalls := 0
	after = func(time.Duration) <-chan time.Time {
		afterCalls++
		ch := make(chan time.Time, 1)
		if firstTimer {
			firstTimer = false

			return ch
		}
		ch <- time.Now()

		return ch
	}
	if err := (&Process{Cmd: fakeStartedCommand()}).Close(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Close error = %v", err)
	}
	if afterCalls != 2 {
		t.Fatalf("cancelled Close after calls = %d, want post-kill wait", afterCalls)
	}

	restoreProcessSeams(t)
	releaseWait := make(chan struct{})
	processTreeClose = func(*processContainment) error { return nil }
	waitProcessCommand = func(*exec.Cmd) error {
		<-releaseWait

		return nil
	}
	firstTimerFired := false
	after = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		if firstTimerFired {
			return ch
		}
		firstTimerFired = true
		ch <- time.Now()
		go func() {
			time.Sleep(10 * time.Millisecond)
			close(releaseWait)
		}()

		return ch
	}
	if err := (&Process{
		Cmd:  fakeStartedCommand(),
		tree: provedProcessContainment(),
	}).Close(context.Background()); err != nil {
		t.Fatalf("proved kill-wait Close error = %v", err)
	}

	restoreProcessSeams(t)
	waitProcessCommand = func(*exec.Cmd) error {
		select {}
	}
	after = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()

		return ch
	}
	if err := (&Process{Cmd: fakeStartedCommand()}).Close(context.Background()); err == nil || !strings.Contains(err.Error(), "did not exit") {
		t.Fatalf("timeout Close error = %v", err)
	}
}

func fakeStartedCommand() *exec.Cmd {
	return &exec.Cmd{Process: &os.Process{Pid: 999999}}
}

const (
	fakeProcessModeOK             = "ok"
	fakeProcessModeStatusOnly     = "status-only"
	fakeProcessModeNoGatewayReady = "no-ready"
	fakeProcessModeBadStatus      = "bad-status"
	fakeProcessModeOldVersion     = "old-version"
	fakeProcessModeBadVersion     = "bad-version"
	fakeProcessModeMissingMethod  = "missing-method"
	fakeProcessModeProbeSentinel  = "probe-sentinel"
	fakeProcessModeSessionCLI     = "session-cli"
)

type fakeSessionCLICapture struct {
	Path        string `json:"path"`
	Resolved    string `json:"resolved"`
	Token       string `json:"token"`
	OperationID string `json:"operationId"`
	Output      string `json:"output"`
}

func TestFakeHermesProcessHelper(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_INTERNAL_HELPER") != "1" {
		return
	}
	if err := runFakeHermesProcess(os.Args, os.Getenv("ACP_GO_HERMES_INTERNAL_MODE")); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func fakeHermesExecutable(t *testing.T, mode string) string {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	path := filepath.Join(t.TempDir(), "hermes")
	body := fmt.Sprintf("#!/bin/sh\nACP_GO_HERMES_INTERNAL_HELPER=1 ACP_GO_HERMES_INTERNAL_MODE=%s exec %q -test.run=TestFakeHermesProcessHelper -- \"$@\"\n", mode, testBinary)
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}

	return path
}

func runFakeHermesProcess(args []string, mode string) error {
	for _, arg := range args {
		if arg == "--version" {
			switch mode {
			case fakeProcessModeOldVersion:
				_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.17.9 (test)")

				return nil
			case fakeProcessModeBadVersion:
				_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent test build")

				return nil
			}
			_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.20.0 (test)")

			return nil
		}
	}
	port := ""
	for i, arg := range args {
		if arg == "--port" && i+1 < len(args) {
			port = args[i+1]

			break
		}
	}
	if port == "" {
		return fmt.Errorf("missing --port in %q", strings.Join(args, " "))
	}
	if mode == fakeProcessModeSessionCLI {
		if err := captureFakeSessionCLI(); err != nil {
			return err
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		if mode == fakeProcessModeBadStatus {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	if mode != fakeProcessModeStatusOnly {
		mux.HandleFunc("/api/ws", func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "done")
			if mode != fakeProcessModeNoGatewayReady {
				data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "event", "params": Event{Type: "gateway.ready"}})
				_ = conn.Write(r.Context(), websocket.MessageText, data)
			}
			for {
				typ, data, err := conn.Read(r.Context())
				if err != nil {
					return
				}
				if typ != websocket.MessageText {
					continue
				}
				var req struct {
					ID     int64           `json:"id"`
					Method string          `json:"method"`
					Params json.RawMessage `json:"params"`
				}
				if err := json.Unmarshal(data, &req); err != nil {
					return
				}
				params := map[string]any{}
				_ = json.Unmarshal(req.Params, &params)
				var response []byte
				if target, ok := strings.CutPrefix(mode, "probe-error:"); ok && req.Method == target {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "method not found"}})
				} else if target, ok := strings.CutPrefix(mode, "probe-domain:"); ok && req.Method == target {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": 4007, "message": "session not found"}})
				} else if target, ok := strings.CutPrefix(mode, "probe-empty:"); ok && req.Method == target {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{}})
				} else if mode == fakeProcessModeProbeSentinel &&
					(req.Method == "approval.respond" || req.Method == "clarify.respond") &&
					params["session_id"] != missingProbeSessionID {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "presence probe activated a real session"}})
				} else if mode == fakeProcessModeProbeSentinel &&
					(req.Method == "approval.respond" || req.Method == "clarify.respond") {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": 4007, "message": "session not found"}})
				} else if mode == fakeProcessModeMissingMethod && req.Method == "model.options" {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "method not found"}})
				} else {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": resultForMethod(req.Method, params)})
				}
				_ = conn.Write(r.Context(), websocket.MessageText, response)
			}
		})
	}
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	return server.ListenAndServe()
}

func captureFakeSessionCLI() error {
	capturePath := os.Getenv("ACP_GO_HERMES_TEST_ROOT")
	if capturePath == "" {
		return errors.New("session CLI capture path is empty")
	}

	resolved, err := exec.LookPath("wagie")
	if err != nil {
		return fmt.Errorf("resolve wagie: %w", err)
	}
	output, err := exec.Command(resolved).Output()
	if err != nil {
		return fmt.Errorf("execute wagie: %w", err)
	}
	capture := fakeSessionCLICapture{
		Path:        os.Getenv("PATH"),
		Resolved:    resolved,
		Token:       os.Getenv("WAGIE_API_TOKEN"),
		OperationID: os.Getenv("WAGIE_OPERATION_ID"),
		Output:      string(output),
	}
	encoded, err := json.Marshal(capture)
	if err != nil {
		return err
	}

	return os.WriteFile(capturePath, encoded, 0o600)
}

func restoreProcessSeams(t *testing.T) {
	t.Helper()
	oldCommandContext := commandContext
	oldListenTCP := listenTCP
	oldRandReader := randReader
	oldMkdirTemp := mkdirTemp
	oldMkdirAll := mkdirAll
	oldUserHomeDir := userHomeDir
	oldStatPath := statPath
	oldAfter := after
	oldHTTPClient := newStatusHTTPClient
	oldWait := waitProcessCommand
	oldStartContained := startHermesContainedProcess
	oldNativeTreeHandoff := processNativeTreeHandoff
	oldAfterOwnerSpawn := afterHermesSpawnBeforeOwnerBind
	oldProbed, oldGateway := cloneExecutableProbeCaches()
	t.Cleanup(func() {
		commandContext = oldCommandContext
		listenTCP = oldListenTCP
		randReader = oldRandReader
		mkdirTemp = oldMkdirTemp
		mkdirAll = oldMkdirAll
		userHomeDir = oldUserHomeDir
		statPath = oldStatPath
		after = oldAfter
		newStatusHTTPClient = oldHTTPClient
		waitProcessCommand = oldWait
		startHermesContainedProcess = oldStartContained
		processNativeTreeHandoff = oldNativeTreeHandoff
		afterHermesSpawnBeforeOwnerBind = oldAfterOwnerSpawn
		executableProbeMu.Lock()
		executableProbed = oldProbed
		gatewayProbed = oldGateway
		executableProbeMu.Unlock()
	})
}

func resetProcessSeams() {
	commandContext = exec.CommandContext
	listenTCP = net.Listen
	randReader = rand.Reader
	mkdirTemp = os.MkdirTemp
	mkdirAll = os.MkdirAll
	userHomeDir = os.UserHomeDir
	statPath = os.Stat
	after = time.After
	newStatusHTTPClient = func() *http.Client { return &http.Client{Timeout: 2 * time.Second} }
	waitProcessCommand = func(cmd *exec.Cmd) error { return cmd.Wait() }
	executableProbeMu.Lock()
	executableProbed = map[string]bool{}
	gatewayProbed = map[string]bool{}
	executableProbes = map[string]chan struct{}{}
	executableProbeMu.Unlock()
}

// markExecutableProbed marks an executable fully proven — version read and
// gateway sweep answered — which is what a fixture that must not spawn either
// probe process needs.
func markExecutableProbed(executable string) {
	executableProbeMu.Lock()
	executableProbed[executable] = true
	gatewayProbed[executable] = true
	executableProbeMu.Unlock()
}

// executableVersionProven reports the version marker alone, which is the fact
// the version probe settles.
func executableVersionProven(executable string) bool {
	executableProbeMu.Lock()
	defer executableProbeMu.Unlock()

	return executableProbed[executable]
}

func cloneExecutableProbeCaches() (map[string]bool, map[string]bool) {
	executableProbeMu.Lock()
	defer executableProbeMu.Unlock()

	return cloneBoolMap(executableProbed), cloneBoolMap(gatewayProbed)
}

func cloneBoolMap(input map[string]bool) map[string]bool {
	result := make(map[string]bool, len(input))
	for key, value := range input {
		result[key] = value
	}

	return result
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type fakeFileInfo struct {
	dir bool
}

func (f fakeFileInfo) Name() string       { return "fake" }
func (f fakeFileInfo) Size() int64        { return 0 }
func (f fakeFileInfo) Mode() os.FileMode  { return 0 }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return f.dir }
func (f fakeFileInfo) Sys() any           { return nil }

func darwinTestProcessOptions(t *testing.T, options ProcessOptions) ProcessOptions {
	t.Helper()
	options.AmbientEnvironment = testAmbientEnvironment()
	if options.AcquireDiscoveryResources == nil {
		options.AcquireDiscoveryResources = testDiscoveryResourceAdmission
	}
	if options.RetainDiscoveryRoot == nil {
		options.RetainDiscoveryRoot = func(string, error) {}
	}
	if runtime.GOOS != "darwin" {
		return options
	}
	parent := t.TempDir()
	if info, err := os.Stat(options.Home); options.Home != "" && err == nil && info.IsDir() {
		dirs, createErr := CreateGenerationXDGDirs(parent)
		if createErr != nil {
			t.Fatalf("CreateGenerationXDGDirs: %v", createErr)
		}
		options.Home = dirs.Root
	}
	options.ScratchParent = parent
	options.DarwinBestEffortContainment = true

	return options
}

func testDiscoveryResourceAdmission(context.Context) (func(), func(), error) {
	return func() {}, func() {}, nil
}
