package hermes

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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
	case "session.create", "session.branch":
		return map[string]any{"session_id": "live", "stored_session_id": "stored"}
	case "session.resume":
		return map[string]any{"session_id": "live-resume", "stored_session_id": params["session_id"]}
	case "session.history":
		return map[string]any{"count": 1, "messages": []map[string]any{{"role": "assistant", "content": "hello"}}}
	case "session.active_list":
		return map[string]any{"sessions": []map[string]any{{"session_id": "live", "session_key": "stored", "title": "Title", "cwd": "/repo"}}}
	case "model.options":
		return map[string]any{"providers": []map[string]any{
			{"id": "array", "models": []map[string]any{{"id": "m1", "name": "M1"}}},
			{"id": "map", "models": map[string]any{"m2": map[string]any{"name": "M2"}}},
			{"id": "string", "models": "m3"},
		}}
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

func TestClientRPCEventsAndWrappers(t *testing.T) {
	gateway := newWSGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := Dial(ctx, gateway.url(), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	if event := <-client.Events(); event.Type != "gateway.ready" {
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

	if out, err := client.CreateSession(ctx, map[string]any{"cwd": "/repo"}); err != nil || out.SessionID != "live" {
		t.Fatalf("CreateSession = %#v err=%v", out, err)
	}
	if out, err := client.ResumeSession(ctx, "stored", nil); err != nil || out.StoredSessionID != "stored" {
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
	if out, err := client.Branch(ctx, "live", "name"); err != nil || out.StoredSessionID != "stored" {
		t.Fatalf("Branch = %#v err=%v", out, err)
	}
	if err := client.SubmitPrompt(ctx, "live", "hello"); err != nil {
		t.Fatalf("SubmitPrompt: %v", err)
	}
	if err := client.Interrupt(ctx, "live"); err != nil {
		t.Fatalf("Interrupt: %v", err)
	}
	if err := client.ApprovalRespond(ctx, "live", "once", false); err != nil {
		t.Fatalf("ApprovalRespond: %v", err)
	}
	if err := client.ClarifyRespond(ctx, "live", "yes"); err != nil {
		t.Fatalf("ClarifyRespond: %v", err)
	}
	if out, err := client.ModelOptions(ctx, "live"); err != nil || len(out.Providers) != 3 {
		t.Fatalf("ModelOptions = %#v err=%v", out, err)
	}
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
	writeClient, err := Dial(ctx, gateway.url(), nil)
	if err != nil {
		t.Fatalf("dial write client: %v", err)
	}
	cancelledWrite, cancelWrite := context.WithCancel(context.Background())
	cancelWrite()
	if err := writeClient.Call(cancelledWrite, "session.active_list", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("write canceled error = %v", err)
	}
	_ = writeClient.Close(websocket.StatusNormalClosure, "done")
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
	client, err = Dial(ctx, gateway.url(), nil)
	if err != nil {
		t.Fatalf("redial: %v", err)
	}
	waitLoopDone := make(chan error, 1)
	go func() { waitLoopDone <- client.Call(context.Background(), "close", nil, nil) }()
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

	var message Message
	if err := message.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("Message accepted malformed JSON")
	}
	var options ModelOptionsResult
	if err := options.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("ModelOptionsResult accepted malformed JSON")
	}
	var provider Provider
	if err := provider.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("Provider accepted malformed JSON")
	}
	if err := provider.UnmarshalJSON([]byte(`{"id":"bad","models":1}`)); err != nil {
		t.Fatalf("Provider numeric models: %v", err)
	}
	var model ProviderModel
	if err := model.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("ProviderModel accepted malformed JSON")
	}
	if err := model.UnmarshalJSON([]byte(`"model-id"`)); err != nil || model.ID != "model-id" || len(model.Raw) == 0 {
		t.Fatalf("ProviderModel string = %#v err=%v", model, err)
	}
	if err := model.UnmarshalJSON([]byte(`"unterminated`)); err == nil {
		t.Fatal("ProviderModel accepted invalid string")
	}
	if err := model.UnmarshalJSON([]byte(`1`)); err == nil {
		t.Fatal("ProviderModel numeric accepted")
	}
}

func TestProcessStartCloseAndHelpers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proc, err := Start(ctx, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
		Cwd:            t.TempDir(),
		Env:            map[string]string{"BASE_ENV": "1"},
		Timeout:        5 * time.Second,
		LogWriter:      io.Discard,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if proc.Client == nil || proc.Port <= 0 || proc.Token == "" || !strings.Contains(proc.StatusURL, "/api/status") {
		t.Fatalf("process = %#v", proc)
	}
	if err := proc.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := (&Process{}).Close(ctx); err != nil {
		t.Fatalf("empty Close: %v", err)
	}
	if port, err := freePort(); err != nil || port <= 0 {
		t.Fatalf("freePort = %d err=%v", port, err)
	}
	if token, err := randomToken(); err != nil || token == "" {
		t.Fatalf("randomToken = %q err=%v", token, err)
	}
	if !IsStateDB("/tmp/state.db") || !IsStateDB("/tmp/state.db-wal") || !IsStateDB("/tmp/state.db-shm") || IsStateDB("/tmp/other.db") {
		t.Fatal("IsStateDB mismatch")
	}

	restoreProcessSeams(t)
	usedConfigure := false
	commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if name != "hermes" {
			t.Fatalf("default executable = %q", name)
		}
		return exec.CommandContext(ctx, filepath.Join(t.TempDir(), "missing-hermes"), args...)
	}
	if _, err := Start(ctx, ProcessOptions{
		Home: t.TempDir(),
		Configure: func(cmd *exec.Cmd) {
			usedConfigure = true
			cmd.Env = append(cmd.Env, "CONFIGURED=1")
		},
	}); err == nil {
		t.Fatal("Start default executable seam unexpectedly succeeded")
	}
	if !usedConfigure {
		t.Fatal("Configure hook was not called")
	}
}

func TestProcessFaultBranches(t *testing.T) {
	ctx := context.Background()
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
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeStatusOnly), Home: t.TempDir(), Timeout: 300 * time.Millisecond}); err == nil {
		t.Fatal("missing websocket unexpectedly passed")
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeNoGatewayReady), Home: t.TempDir(), Timeout: 300 * time.Millisecond}); err == nil {
		t.Fatal("missing gateway.ready unexpectedly passed")
	}
	if _, err := Start(ctx, ProcessOptions{ExecutablePath: fakeHermesExecutable(t, fakeProcessModeBadStatus), Home: t.TempDir(), Timeout: 300 * time.Millisecond}); err == nil {
		t.Fatal("bad status unexpectedly passed")
	}

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
	if err := (&Process{StatusURL: ":// bad"}).waitReady(ctx); err == nil {
		t.Fatal("waitReady accepted invalid status URL")
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := (&Process{StatusURL: "http://127.0.0.1:1/api/status"}).waitReady(shortCtx); err == nil ||
		!strings.Contains(err.Error(), "status check failed") {
		t.Fatalf("waitReady connection error = %v", err)
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
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	restoreProcessSeams(t)
	waitProcessCommand = func(*exec.Cmd) error {
		select {}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&Process{Cmd: cmd}).Close(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("context Close error = %v", err)
	}

	cmd = exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep timeout: %v", err)
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
	if err := (&Process{Cmd: cmd}).Close(context.Background()); err == nil || !strings.Contains(err.Error(), "did not exit") {
		t.Fatalf("timeout Close error = %v", err)
	}
}

const (
	fakeProcessModeOK             = "ok"
	fakeProcessModeStatusOnly     = "status-only"
	fakeProcessModeNoGatewayReady = "no-ready"
	fakeProcessModeBadStatus      = "bad-status"
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
			<-r.Context().Done()
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
