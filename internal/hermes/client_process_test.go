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

	mu    sync.Mutex
	calls []rpcCall
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
	_ = conn.Write(r.Context(), websocket.MessageText, []byte("{"))
	g.writeEvent(r.Context(), conn, Event{Type: "gateway.ready"})
	g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "method": "event", "params": map[string]any{"type": 1}})
	g.writeEvent(r.Context(), conn, Event{Type: "message.delta", SessionID: "live", Payload: json.RawMessage(`{"text":"hi"}`)})
	for range 16 {
		_ = conn.Write(r.Context(), websocket.MessageText, []byte("{"))
	}
	for range 300 {
		g.writeEvent(r.Context(), conn, Event{Type: "message.delta", SessionID: "live", Payload: json.RawMessage(`{"text":"overflow"}`)})
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
		g.mu.Unlock()
		switch req.Method {
		case "missing":
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": 4007, "message": "missing"}})
		case "hang":
		case "close":
			_ = conn.Close(websocket.StatusInternalError, "forced close")

			return
		case "empty-result":
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID})
		case "bad-rpc":
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":"bad","message":"bad"}}`, req.ID)))
		case "bad-result":
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "{"})
		default:
			g.writeRaw(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": resultForMethod(req.Method, params)})
		}
	}
}

func resultForMethod(method string, params map[string]any) any {
	switch method {
	case "session.create":
		return map[string]any{"session_id": "live", "stored_session_id": "stored"}
	case "session.branch":
		return map[string]any{"session_id": "live-branch", "title": "Branch", "parent": "stored"}
	case "session.resume":
		return map[string]any{"session_id": "live-resume", "session_key": params["session_id"]}
	case "session.history":
		return map[string]any{"count": 1, "messages": []map[string]any{{"role": "assistant", "content": "hello"}}}
	case "session.active_list":
		return map[string]any{"sessions": []map[string]any{{"id": "live", "session_key": "stored", "title": "Title", "cwd": "/repo"}}}
	case "image.attach_bytes":
		return map[string]any{"attached": true}
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
	for _, call := range gateway.calls {
		if call.Method == "clarify.respond" {
			clarifyParams = call.Params
		}
	}
	gateway.mu.Unlock()
	if clarifyParams["session_id"] != "live" || clarifyParams["request_id"] != "request-1" || clarifyParams["answer"] != "yes" {
		t.Fatalf("clarify.respond params = %#v", clarifyParams)
	}
	assertClientCallEdges(t, ctx, client)
	assertClientCloseSemantics(t, ctx, client, gateway)
}

func assertClientEventStream(t *testing.T, client *Client) {
	t.Helper()

	if event := <-client.Events(); event.Type != eventGatewayReady {
		t.Fatalf("first event = %#v", event)
	}
	if err := <-client.Errors(); err == nil {
		t.Fatal("malformed frame did not reach error channel")
	}
	if err := <-client.Errors(); err == nil {
		t.Fatal("malformed event did not reach error channel")
	}
	if event := <-client.Events(); event.Type != "message.delta" || event.SessionID != "live" || len(event.Raw) == 0 {
		t.Fatalf("native event = %#v", event)
	}
	if client.Events() == nil || client.Errors() == nil {
		t.Fatal("event accessors returned nil")
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
	if out, err := client.History(ctx, "live"); err != nil || out.Count != 1 || len(out.Messages) != 1 {
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
	if out, err := client.Branch(ctx, "live", "name"); err != nil || out.SessionID != "live-branch" || out.Title != "Branch" || out.Parent != "stored" {
		t.Fatalf("Branch = %#v err=%v", out, err)
	}
	if err := client.SubmitPrompt(ctx, "live", "hello"); err != nil {
		t.Fatalf("SubmitPrompt: %v", err)
	}
	if err := client.AttachImageBytes(ctx, "live", "AA==", "image.png"); err != nil {
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
	if err := client.Call(ctx, "empty-result", nil, &bad); err != nil {
		t.Fatalf("empty result with output: %v", err)
	}
	if err := client.Call(ctx, "marshal", map[string]any{"bad": func() {}}, nil); err == nil {
		t.Fatal("marshal error was nil")
	}
	shortBadRPC, cancelBadRPC := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelBadRPC()
	if err := client.Call(shortBadRPC, "bad-rpc", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bad rpc frame error = %v", err)
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
	if err := <-waitLoopDone; err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("read loop pending close error = %v", err)
	}
}

func TestClientDialAndJSONBranches(t *testing.T) {
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
		provider.Slug != "openrouter" || provider.Models[0] != "anthropic/claude-fable-5" || !provider.Capabilities["anthropic/claude-fable-5"].Reasoning {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	usedConfigure := false
	proc, startErr := Start(ctx, ProcessOptions{
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
	})
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
	resumeDomainProc, resumeErr := Start(ctx, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, "probe-domain:session.resume"),
		Home:           t.TempDir(),
		Timeout:        5 * time.Second,
	})
	if resumeErr != nil {
		t.Fatalf("Start with resume domain probe: %v", resumeErr)
	}
	if err := resumeDomainProc.Close(ctx); err != nil {
		t.Fatalf("Close resume domain proc: %v", err)
	}
	deleteDomainProc, deleteErr := Start(ctx, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, "probe-domain:session.delete"),
		Home:           t.TempDir(),
		Timeout:        5 * time.Second,
	})
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

func TestProcessFailsClosedWhenSupervisorDiesBeforeProof(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	proc, err := Start(ctx, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
		Timeout:        5 * time.Second,
		LogWriter:      io.Discard,
	})
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
	if err := proc.Close(ctx); !errors.Is(err, ErrProcessTreeUnproven) {
		t.Fatalf("Close after forced supervisor death = %v, want process-tree proof failure", err)
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
	if !IsStateDB("/tmp/state.db") || !IsStateDB("/tmp/state.db-wal") || !IsStateDB("/tmp/state.db-shm") || IsStateDB("/tmp/other.db") {
		t.Fatal("IsStateDB mismatch")
	}
	if compareVersions("1.2.3", "1.2.2") <= 0 || compareVersions("1.2.3", "1.2.3") != 0 {
		t.Fatal("compareVersions mismatch")
	}
	markExecutableProbed("already-probed")
	if needed, err := ensureExecutableVersion(ctx, "already-probed"); err != nil || needed {
		t.Fatalf("cached executable probe needed=%v err=%v", needed, err)
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

func assertProcessStartSeams(t *testing.T, ctx context.Context) {
	t.Helper()

	restoreProcessSeams(t)
	commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if name != "hermes" {
			t.Fatalf("default executable = %q", name)
		}

		return exec.CommandContext(ctx, filepath.Join(t.TempDir(), "missing-hermes"), args...)
	}
	if _, err := Start(ctx, ProcessOptions{Home: t.TempDir()}); err == nil {
		t.Fatal("Start default executable seam unexpectedly succeeded")
	}

	restoreProcessSeams(t)
	commandContext = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "--version" {
			return exec.CommandContext(ctx, "sh", "-c", "printf 'Hermes Agent v0.18.2\\n'")
		}

		return exec.CommandContext(ctx, filepath.Join(t.TempDir(), "missing-hermes"), args...)
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: "start-fails", Home: t.TempDir()}); err == nil {
		t.Fatal("Start command failure was ignored")
	}
}

func TestProcessFaultBranches(t *testing.T) {
	ctx := context.Background()

	assertStartFaultModes(t, ctx)

	restoreProcessSeams(t)
	mkdirTemp = func(string, string) (string, error) { return "", errors.New("mktemp failed") }
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK)}); err == nil {
		t.Fatal("mktemp error ignored")
	}

	restoreProcessSeams(t)
	listenTCP = func(string, string) (net.Listener, error) { return nil, errors.New("listen failed") }
	if _, err := freePort(); err == nil {
		t.Fatal("freePort listen error ignored")
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK), Home: t.TempDir()}); err == nil {
		t.Fatal("Start ignored freePort error")
	}

	resetProcessSeams()
	restoreProcessSeams(t)
	randReader = errorReader{err: errors.New("entropy failed")}
	if _, err := randomToken(); err == nil {
		t.Fatal("randomToken entropy error ignored")
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK), Home: t.TempDir()}); err == nil {
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

	if _, err := Start(ctx, ProcessOptions{ExecutablePath: filepath.Join(t.TempDir(), "missing-hermes"), Home: t.TempDir()}); err == nil {
		t.Fatal("missing executable unexpectedly started")
	}
	fileHome := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(fileHome, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK), Home: fileHome}); err == nil {
		t.Fatal("file home unexpectedly started")
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeStatusOnly), Home: t.TempDir(), Timeout: 5 * time.Second}); err == nil {
		t.Fatal("missing websocket unexpectedly passed")
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeNoGatewayReady), Home: t.TempDir(), Timeout: 5 * time.Second}); err == nil {
		t.Fatal("missing gateway.ready unexpectedly passed")
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeBadStatus), Home: t.TempDir(), Timeout: 5 * time.Second}); err == nil {
		t.Fatal("bad status unexpectedly passed")
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOldVersion), Home: t.TempDir(), Timeout: 10 * time.Second}); err == nil ||
		!strings.Contains(err.Error(), "below minimum") {
		t.Fatalf("old version error = %v", err)
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeBadVersion), Home: t.TempDir(), Timeout: 10 * time.Second}); err == nil ||
		!strings.Contains(err.Error(), "missing semantic version") {
		t.Fatalf("bad version error = %v", err)
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeMissingMethod), Home: t.TempDir(), Timeout: 10 * time.Second}); err == nil ||
		!strings.Contains(err.Error(), "model.options") {
		t.Fatalf("missing method probe error = %v", err)
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
			if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, tt.mode), Home: t.TempDir(), Timeout: 10 * time.Second}); err == nil ||
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

	closedEvents := &Client{events: make(chan Event), errs: make(chan error)}
	close(closedEvents.events)
	if err := (&Process{Client: closedEvents}).waitGatewayReady(ctx); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("waitGatewayReady closed events err = %v", err)
	}
	errorClient := &Client{events: make(chan Event), errs: make(chan error, 1)}
	errorClient.errs <- errors.New("gateway failed")
	if err := (&Process{Client: errorClient}).waitGatewayReady(ctx); err == nil || !strings.Contains(err.Error(), "gateway failed") {
		t.Fatalf("waitGatewayReady error err = %v", err)
	}
	closedErrorsCtx, cancelClosedErrors := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelClosedErrors()
	closedErrors := &Client{events: make(chan Event), errs: make(chan error)}
	close(closedErrors.errs)
	if err := (&Process{Client: closedErrors}).waitGatewayReady(closedErrorsCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitGatewayReady closed errors err = %v", err)
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
	if err := (&Process{Cmd: fakeStartedCommand()}).Close(context.Background()); err == nil || !strings.Contains(err.Error(), "did not exit") {
		t.Fatalf("kill-wait Close error = %v", err)
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
)

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
			_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.18.2 (test)")

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
	oldProbed := cloneExecutableProbeCache()
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
		executableProbeMu.Lock()
		executableProbed = oldProbed
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
	executableProbed = map[string]struct{}{}
	executableProbeMu.Unlock()
}

func cloneExecutableProbeCache() map[string]struct{} {
	executableProbeMu.Lock()
	defer executableProbeMu.Unlock()
	out := make(map[string]struct{}, len(executableProbed))
	for key, value := range executableProbed {
		out[key] = value
	}

	return out
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
