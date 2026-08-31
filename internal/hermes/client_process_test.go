//nolint:gocyclo // Fake process method matrices intentionally enumerate the full protocol.
package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
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
	if err := client.ApprovalRespond(ctx, "live", "once"); err != nil {
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
