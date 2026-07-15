package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
)

type gatewayRPCCall struct {
	Method string
	Params map[string]any
}

type fakeGatewayServer struct {
	t      *testing.T
	server *httptest.Server

	mu                  sync.Mutex
	calls               []gatewayRPCCall
	closeAfterResult    string
	closeNowAfterResult string
	failMethods         map[string]struct{}
	promptEvents        *[]Event
	promptEventDelay    time.Duration
	promptRawFrames     []string
	branchNotFound      int
	branchCreated       bool
	branchNoSession     bool
	branchNoActive      bool
	branchNoKey         bool
	createNoLive        bool
	createNoStored      bool
	resumeNoLive        bool
	resumeNoKey         bool
	resumeKey           string
	activeNoID          bool
	activeNoKey         bool
}

func newFakeGatewayServer(t *testing.T) *fakeGatewayServer {
	t.Helper()
	fake := &fakeGatewayServer{t: t, failMethods: map[string]struct{}{}}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)

	return fake
}

func TestMCPServersWithSecretEnv(t *testing.T) {
	servers := []acp.McpServer{
		{Stdio: &acp.McpServerStdio{Name: "stdio", Command: "tool"}},
		{Http: &acp.McpServerHttpInline{
			Name: "http",
			Url:  "https://example.test/mcp",
			Headers: []acp.HttpHeader{
				{Name: "Authorization", Value: "Bearer secret"},
				{Name: "X-API-Key", Value: "secret-key"},
			},
		}},
	}

	materialized, env, err := mcpServersWithSecretEnv(servers, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := materialized[1].Http.Headers[0].Value; got != "${ACP_GO_HERMES_MCP_HEADER_2_1}" {
		t.Fatalf("authorization placeholder = %q", got)
	}
	if got := materialized[1].Http.Headers[1].Value; got != "${ACP_GO_HERMES_MCP_HEADER_2_2}" {
		t.Fatalf("API key placeholder = %q", got)
	}
	if env["ACP_GO_HERMES_MCP_HEADER_2_1"] != "Bearer secret" || env["ACP_GO_HERMES_MCP_HEADER_2_2"] != "secret-key" {
		t.Fatalf("secret environment = %#v", env)
	}
	if servers[1].Http.Headers[0].Value != "Bearer secret" {
		t.Fatalf("input server was mutated: %#v", servers[1])
	}

	_, _, err = mcpServersWithSecretEnv(servers, map[string]string{"ACP_GO_HERMES_MCP_HEADER_2_1": "occupied"})
	if err == nil {
		t.Fatal("reserved MCP environment collision was accepted")
	}
}

func TestStartServerRejectsReservedMCPSecretEnvironment(t *testing.T) {
	_, err := StartServer(t.Context(), StartOptions{
		ACPSessionID:  "session-1",
		Root:          t.TempDir(),
		ScratchParent: t.TempDir(),
		Cwd:           t.TempDir(),
		Env:           map[string]string{"ACP_GO_HERMES_MCP_HEADER_1_1": "occupied"},
		MCPServers: []acp.McpServer{{Http: &acp.McpServerHttpInline{
			Name: "http", Url: "https://example.test", Headers: []acp.HttpHeader{{Name: "Authorization", Value: "secret"}},
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("StartServer collision error = %v", err)
	}
}

func (s *fakeGatewayServer) dialClient(t *testing.T) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := Dial(ctx, "ws"+strings.TrimPrefix(s.server.URL, "http")+"/api/ws", nil)
	if err != nil {
		t.Fatalf("dial fake gateway: %v", err)
	}

	return client
}

func (s *fakeGatewayServer) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/ws" {
		http.NotFound(w, r)

		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.t.Errorf("accept websocket: %v", err)

		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")
	s.writeEvent(r.Context(), conn, Event{Type: "gateway.ready"})
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
			s.t.Errorf("decode request: %v", err)

			return
		}
		params := map[string]any{}
		_ = json.Unmarshal(req.Params, &params)
		s.mu.Lock()
		s.calls = append(s.calls, gatewayRPCCall{Method: req.Method, Params: params})
		closeAfterResult := s.closeAfterResult == req.Method
		closeNowAfterResult := s.closeNowAfterResult == req.Method
		_, fail := s.failMethods[req.Method]
		s.mu.Unlock()
		if fail {
			s.writeError(r.Context(), conn, req.ID, -32000, req.Method+" failed")

			continue
		}
		s.respond(r.Context(), conn, req.ID, req.Method, params)
		if closeNowAfterResult {
			// Abrupt TCP close (no close frame): the client read loop parks the
			// real transport error before closing its channels.
			_ = conn.CloseNow()

			return
		}
		if closeAfterResult {
			_ = conn.Close(websocket.StatusNormalClosure, "forced close")

			return
		}
	}
}

func (s *fakeGatewayServer) respond(ctx context.Context, conn *websocket.Conn, id int64, method string, params map[string]any) {
	switch method {
	case "session.create":
		s.mu.Lock()
		createNoLive := s.createNoLive
		createNoStored := s.createNoStored
		s.mu.Unlock()
		result := map[string]any{
			"session_id":        "live-1",
			"stored_session_id": "stored-1",
		}
		if createNoLive {
			delete(result, "session_id")
		}
		if createNoStored {
			delete(result, "stored_session_id")
		}
		s.writeResult(ctx, conn, id, result)
	case "session.resume":
		stored, _ := params["session_id"].(string)
		s.mu.Lock()
		resumeNoLive := s.resumeNoLive
		resumeNoKey := s.resumeNoKey
		resumeKey := s.resumeKey
		s.mu.Unlock()
		if resumeKey == "" {
			resumeKey = stored
		}
		result := map[string]any{
			"session_id":  "live-" + resumeKey,
			"session_key": resumeKey,
		}
		if resumeNoLive {
			delete(result, "session_id")
		}
		if resumeNoKey {
			delete(result, "session_key")
		}
		s.writeResult(ctx, conn, id, result)
	case "session.active_list":
		s.mu.Lock()
		branchCreated := s.branchCreated
		branchNoActive := s.branchNoActive
		branchNoKey := s.branchNoKey
		activeNoID := s.activeNoID
		activeNoKey := s.activeNoKey
		s.mu.Unlock()
		sessionID := "live-1"
		if activeNoID {
			sessionID = ""
		}
		sessionKey := "stored-1"
		if activeNoKey {
			sessionKey = ""
		}
		sessions := []map[string]any{{
			"id":          sessionID,
			"session_key": sessionKey,
			"title":       "Listed",
			"cwd":         "/repo",
		}}
		if branchCreated && !branchNoActive {
			sessionKey := "stored-branch"
			if branchNoKey {
				sessionKey = ""
			}
			sessions = append(sessions, map[string]any{
				"id":          "live-branch",
				"session_key": sessionKey,
				"title":       "branch",
				"cwd":         "/repo",
			})
		}
		s.writeResult(ctx, conn, id, map[string]any{"sessions": sessions})
	case "session.delete":
		sessionID, _ := params["session_id"].(string)
		if sessionID == "missing" {
			s.writeError(ctx, conn, id, 4007, "not found")

			return
		}
		s.writeResult(ctx, conn, id, map[string]any{})
	case "prompt.submit":
		live, _ := params["session_id"].(string)
		s.writeResult(ctx, conn, id, map[string]any{})
		for _, frame := range s.promptRawFrameScript() {
			_ = conn.Write(ctx, websocket.MessageText, []byte(frame))
		}
		script := s.promptEventScript(live)
		for index, event := range script {
			// An optional inter-event gap lets a test hold back a later frame
			// (e.g. message.complete) until the client has drained an earlier
			// one, making streaming-vs-completion ordering deterministic.
			if delay := s.promptEventDelayValue(); delay > 0 && index > 0 {
				time.Sleep(delay)
			}

			s.writeEvent(ctx, conn, event)
		}
	case "approval.respond", "clarify.respond", "terminal.read.respond", "sudo.respond", "secret.respond", "session.interrupt", "session.close":
		s.writeResult(ctx, conn, id, map[string]any{})
	case "session.history":
		s.writeResult(ctx, conn, id, map[string]any{"count": 2, "messages": []map[string]any{
			{"role": "user", "content": "hi"},
			{"role": "assistant", "content": map[string]any{"text": "history"}},
		}})
	case "session.branch":
		s.mu.Lock()
		branchNotFound := s.branchNotFound > 0
		if branchNotFound {
			s.branchNotFound--
		}
		s.mu.Unlock()
		if branchNotFound {
			s.writeError(ctx, conn, id, 4007, "not found")

			return
		}
		s.mu.Lock()
		s.branchCreated = true
		branchNoSession := s.branchNoSession
		s.mu.Unlock()
		if branchNoSession {
			s.writeResult(ctx, conn, id, map[string]any{
				"title":  "branch",
				"parent": "stored-1",
			})

			return
		}
		s.writeResult(ctx, conn, id, map[string]any{
			"session_id": "live-branch",
			"title":      "branch",
			"parent":     "stored-1",
		})
	case "model.options":
		s.writeResult(ctx, conn, id, map[string]any{
			"model":    "anthropic/claude-sonnet-4",
			"provider": "",
			"providers": []map[string]any{
				{
					"slug":            "openrouter",
					"name":            "OpenRouter",
					"authenticated":   true,
					"is_current":      false,
					"is_user_defined": false,
					"models":          []string{"openai/gpt-test", "anthropic/claude-fable-5"},
					"capabilities": map[string]any{
						"openai/gpt-test":          map[string]any{"fast": true, "reasoning": true},
						"anthropic/claude-fable-5": map[string]any{"fast": false, "reasoning": true},
					},
					"pricing": map[string]any{
						"openai/gpt-test": map[string]any{"cache": nil, "free": false, "input": "$1.00", "output": "$2.00"},
					},
					"source":       "built-in",
					"total_models": 2,
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
		})
	default:
		s.writeError(ctx, conn, id, -32601, "missing")
	}
}

func (s *fakeGatewayServer) promptEventScript(live string) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.promptEvents != nil {
		events := append([]Event(nil), (*s.promptEvents)...)
		for index := range events {
			if events[index].SessionID == "" {
				events[index].SessionID = live
			}
		}

		return events
	}

	return []Event{
		{Type: "approval.request", SessionID: live, Payload: json.RawMessage(`{"id":"approval-1","title":"Edit file","command":"write"}`)},
		{Type: "clarify.request", SessionID: live, Payload: json.RawMessage(`{"id":"clarify-1","question":"Continue?"}`)},
		{Type: "terminal.read.request", SessionID: live, Payload: json.RawMessage(`{}`)},
		{Type: "sudo.request", SessionID: live, Payload: json.RawMessage(`{}`)},
		{Type: "secret.request", SessionID: live, Payload: json.RawMessage(`{}`)},
		{Type: "thinking.delta", SessionID: live, Payload: json.RawMessage(`{"text":"thinking"}`)},
		{Type: "message.delta", SessionID: "other-live", Payload: json.RawMessage(`{"text":"ignored"}`)},
		{Type: "message.delta", SessionID: live, Payload: json.RawMessage(`{"text":""}`)},
		{Type: "message.delta", SessionID: live, Payload: json.RawMessage(`{"delta":"hello "}`)},
		{Type: "message.delta", SessionID: live, Payload: json.RawMessage(`["world"]`)},
		{Type: "message.complete", SessionID: live, Payload: json.RawMessage(`{"text":"hello world","usage":{"total_tokens":7,"input_tokens":3,"output_tokens":4,"reasoning_tokens":1}}`)},
	}
}

func (s *fakeGatewayServer) writeResult(ctx context.Context, conn *websocket.Conn, id int64, result any) {
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	if err != nil {
		s.t.Errorf("marshal result: %v", err)

		return
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		s.t.Errorf("write result: %v", err)
	}
}

func (s *fakeGatewayServer) writeError(ctx context.Context, conn *websocket.Conn, id int64, code int, message string) {
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
	if err != nil {
		s.t.Errorf("marshal error: %v", err)

		return
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		s.t.Errorf("write error: %v", err)
	}
}

func (s *fakeGatewayServer) writeEvent(ctx context.Context, conn *websocket.Conn, event Event) {
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "event", "params": event})
	if err != nil {
		s.t.Errorf("marshal event: %v", err)

		return
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		s.t.Errorf("write event: %v", err)
	}
}

func (s *fakeGatewayServer) callMethods() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.calls))
	for _, call := range s.calls {
		out = append(out, call.Method)
	}

	return out
}

func (s *fakeGatewayServer) setCloseAfterResult(method string) {
	s.mu.Lock()
	s.closeAfterResult = method
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setCloseNowAfterResult(method string) {
	s.mu.Lock()
	s.closeNowAfterResult = method
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setFail(method string) {
	s.mu.Lock()
	s.failMethods[method] = struct{}{}
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setBranchNotFoundOnce() {
	s.setBranchNotFoundCount(1)
}

func (s *fakeGatewayServer) setBranchNotFoundCount(count int) {
	s.mu.Lock()
	s.branchNotFound = count
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setCreateNoStored() {
	s.mu.Lock()
	s.createNoStored = true
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setCreateNoLive() {
	s.mu.Lock()
	s.createNoLive = true
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setResumeNoKey() {
	s.mu.Lock()
	s.resumeNoKey = true
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setResumeKey(key string) {
	s.mu.Lock()
	s.resumeKey = key
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setResumeNoLive() {
	s.mu.Lock()
	s.resumeNoLive = true
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setActiveNoKey() {
	s.mu.Lock()
	s.activeNoKey = true
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setActiveNoID() {
	s.mu.Lock()
	s.activeNoID = true
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setPromptEvents(events ...Event) {
	s.mu.Lock()
	copied := append([]Event(nil), events...)
	s.promptEvents = &copied
	s.mu.Unlock()
}

func (s *fakeGatewayServer) promptEventDelayValue() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.promptEventDelay
}

// setPromptRawFrames queues raw text frames written verbatim (bypassing the
// event envelope) right after the prompt.submit result, letting a test inject a
// malformed gateway line the internal client must skip without tearing down the
// turn.
func (s *fakeGatewayServer) setPromptRawFrames(frames ...string) {
	s.mu.Lock()
	s.promptRawFrames = append([]string(nil), frames...)
	s.mu.Unlock()
}

func (s *fakeGatewayServer) promptRawFrameScript() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.promptRawFrames...)
}

func (s *fakeGatewayServer) callsFor(method string) []gatewayRPCCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []gatewayRPCCall{}
	for _, call := range s.calls {
		if call.Method == method {
			out = append(out, call)
		}
	}

	return out
}

func newGatewayBackedHermesServer(t *testing.T, fake *fakeGatewayServer, defaultModel string) *hermesServer {
	t.Helper()

	return &hermesServer{
		cmd:          &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}},
		xdg:          testXDGDirs(t),
		log:          slog.New(slog.DiscardHandler),
		events:       make(chan TurnEvent, 32),
		errs:         make(chan error, 16),
		closed:       make(chan struct{}),
		gateway:      fake.dialClient(t),
		liveByStored: map[string]string{},
		storedByLive: map[string]string{},
		cwd:          "/repo",
		defaultModel: defaultModel,
	}
}

func TestHermesGatewayServerMethods(t *testing.T) {
	fake := newFakeGatewayServer(t)
	client := fake.dialClient(t)
	server := &hermesServer{
		cmd:          &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}},
		xdg:          testXDGDirs(t),
		log:          slog.New(slog.DiscardHandler),
		events:       make(chan TurnEvent, 16),
		errs:         make(chan error, 16),
		closed:       make(chan struct{}),
		gateway:      client,
		liveByStored: map[string]string{},
		storedByLive: map[string]string{},
		cwd:          "/repo",
		defaultModel: "openai/gpt-test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	created, err := server.CreateSession(ctx, "Created")
	if err != nil || created.ID != "stored-1" || created.Title != "Created" || created.Model.ProviderID != "openai" {
		t.Fatalf("CreateSession = %#v err=%v", created, err)
	}
	if got, err2 := server.GetSession(ctx, "stored-1"); err2 != nil || got.ID != "stored-1" {
		t.Fatalf("GetSession existing = %#v err=%v", got, err2)
	}
	if got, err3 := server.GetSession(ctx, "restored"); err3 != nil || got.ID != "restored" {
		t.Fatalf("GetSession resume = %#v err=%v", got, err3)
	}
	list, err := server.ListSessions(ctx, "/repo")
	if err != nil || len(list) != 1 || list[0].ID != "stored-1" {
		t.Fatalf("ListSessions = %#v err=%v", list, err)
	}
	if err4 := server.DeleteSession(ctx, "missing"); err4 != nil {
		t.Fatalf("DeleteSession missing: %v", err4)
	}
	if err5 := server.DeleteSession(ctx, "stored-1"); err5 != nil {
		t.Fatalf("DeleteSession: %v", err5)
	}
	if live := server.liveSessionID("stored-1"); live != "" {
		t.Fatalf("deleted session still mapped to %q", live)
	}
	if perms, err6 := server.PendingPermissions(ctx); err6 != nil || perms != nil {
		t.Fatalf("PendingPermissions = %#v err=%v", perms, err6)
	}
	if questions, err7 := server.PendingQuestions(ctx); err7 != nil || questions != nil {
		t.Fatalf("PendingQuestions = %#v err=%v", questions, err7)
	}
	if todos, err8 := server.Todos(ctx, "stored-1"); err8 != nil || todos != nil {
		t.Fatalf("Todos = %#v err=%v", todos, err8)
	}

	testGatewayServerMessageForkAndClose(ctx, t, server, fake)
}

func testGatewayServerMessageForkAndClose(ctx context.Context, t *testing.T, server *hermesServer, fake *fakeGatewayServer) {
	t.Helper()

	message, err := server.SendMessage(ctx, "stored-1", MessageRequest{Parts: []map[string]any{{"text": "hello"}, {"text": "world"}}})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if got := [2]string{message.Parts[0].Text, message.Parts[0].StreamedText}; got != [2]string{"hello world", "hello world"} {
		t.Fatalf("message and streamed text = %q", got)
	}
	if message.Info.Tokens.Total != 7 || message.Info.Tokens.Input != 3 || message.Info.Tokens.Output != 4 || message.Info.Tokens.Reasoning != 1 {
		t.Fatalf("tokens = %#v", message.Info.Tokens)
	}
	for _, want := range []string{"approval.request", "clarify.request", "message.part.updated"} {
		if !drainHermesEventType(server.events, want) {
			t.Fatalf("missing forwarded event %q", want)
		}
	}
	if err9 := server.ReplyPermission(ctx, PermissionRequest{SessionID: "stored-1"}, "always", "ignored"); err9 != nil {
		t.Fatalf("ReplyPermission: %v", err9)
	}
	if err10 := server.ReplyPermission(ctx, PermissionRequest{SessionID: "stored-1"}, "reject", "ignored"); err10 != nil {
		t.Fatalf("ReplyPermission reject: %v", err10)
	}
	if err11 := server.ReplyQuestion(ctx, QuestionRequest{ID: "question-1", SessionID: "stored-1"}, [][]string{{"yes"}}); err11 != nil {
		t.Fatalf("ReplyQuestion: %v", err11)
	}
	if err12 := server.RejectQuestion(ctx, QuestionRequest{ID: "question-2", SessionID: "stored-1"}); err12 != nil {
		t.Fatalf("RejectQuestion: %v", err12)
	}
	if err13 := server.Abort(ctx, "missing-live"); err13 != nil {
		t.Fatalf("Abort missing live: %v", err13)
	}
	if err14 := server.Abort(ctx, "stored-1"); err14 != nil {
		t.Fatalf("Abort: %v", err14)
	}
	history, err := server.Messages(ctx, "stored-1")
	if err != nil || len(history) != 2 || history[1].Parts[0].Text != "history" {
		t.Fatalf("Messages = %#v err=%v", history, err)
	}
	fork, err := server.Fork(ctx, "stored-1", "ignored-message")
	if err != nil || fork.ID != "stored-branch" {
		t.Fatalf("Fork = %#v err=%v", fork, err)
	}
	fake.setBranchNotFoundOnce()
	retryFork, err := server.Fork(ctx, "stored-1", "")
	if err != nil || retryFork.ID != "stored-branch" {
		t.Fatalf("Fork retry = %#v err=%v", retryFork, err)
	}
	fake.setBranchNotFoundCount(2)
	if _, err15 := server.Fork(ctx, "stored-1", ""); err15 == nil {
		t.Fatal("Fork succeeded after repeated live session not found")
	}
	providers, err := server.ConfigProviders(ctx)
	if err != nil || len(providers.Providers) != 2 || !providers.Providers[0].Models["openai/gpt-test"].Reasoning {
		t.Fatalf("ConfigProviders = %#v err=%v", providers, err)
	}
	if err := server.Close(ctx); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Close: %v", err)
	}

	methods := fake.callMethods()
	for _, want := range []string{
		"session.create", "session.resume", "session.active_list", "session.delete", "prompt.submit",
		"approval.respond", "clarify.respond", "terminal.read.respond", "sudo.respond", "secret.respond",
		"session.interrupt", "session.history", "session.branch", "model.options",
	} {
		if !containsString(methods, want) {
			t.Fatalf("method %q not called; methods=%v", want, methods)
		}
	}
}

func drainHermesEventType(ch <-chan TurnEvent, want string) bool {
	for {
		select {
		case event := <-ch:
			if event.Type == want {
				return true
			}
		default:
			return false
		}
	}
}

func TestHermesGatewayTextHelpersAndErrors(t *testing.T) {
	if got := textFromHermesParts([]map[string]any{{"text": "one"}, {"other": "skip"}, {"text": "two"}}); got != "one\n\ntwo" {
		t.Fatalf("textFromHermesParts = %q", got)
	}
	for _, raw := range []json.RawMessage{
		json.RawMessage(`"plain"`),
		json.RawMessage(`{"content":[{"delta":"nested"}]}`),
		json.RawMessage(`{"missing":true}`),
		json.RawMessage(`{`),
	} {
		_ = gatewayEventText(raw)
	}
	if got := gatewayCompleteText(json.RawMessage(`{"text":"raw","rendered":"ansi"}`)); got != "raw" {
		t.Fatalf("complete raw text = %q", got)
	}
	if got := gatewayCompleteText(json.RawMessage(`{"rendered":"fallback"}`)); got != "fallback" {
		t.Fatalf("complete rendered text = %q", got)
	}
	if got := gatewayCompleteText(json.RawMessage(`{"reasoning":"not final","status":"complete"}`)); got != "" {
		t.Fatalf("complete unrelated text = %q", got)
	}
	if got := gatewayCompleteText(json.RawMessage(`{`)); got != "" {
		t.Fatalf("malformed complete text = %q", got)
	}
	tokens := gatewayUsageTokens(json.RawMessage(`{"usage":{"total":1,"input":2,"output":3,"reasoning":4}}`))
	if tokens.Total != 1 || tokens.Input != 2 || tokens.Output != 3 || tokens.Reasoning != 4 {
		t.Fatalf("fallback usage tokens = %#v", tokens)
	}
	if err := assistantMessageError(NativeMessage{Info: NativeMessageInfo{Finish: "error"}}); err == nil {
		t.Fatal("assistant finish error accepted")
	}
	err := assistantMessageError(NativeMessage{Info: NativeMessageInfo{Error: &nativeError{Message: "provider failed"}}})
	if err == nil || !strings.Contains(err.Error(), "provider failed") {
		t.Fatalf("assistant message error = %v", err)
	}
	if err := assistantMessageError(NativeMessage{Info: NativeMessageInfo{Error: &nativeError{}}}); err == nil {
		t.Fatal("empty assistant error accepted")
	}
	if err := assistantMessageError(NativeMessage{Info: NativeMessageInfo{Finish: "stop"}}); err != nil {
		t.Fatalf("assistant stop rejected: %v", err)
	}
	messages := nativeMessagesFromGateway("stored", []Message{{Role: "assistant", Content: json.RawMessage(`{"text":"mapped"}`)}})
	if messages[0].Info.SessionID != "stored" || messages[0].Parts[0].Text != "mapped" {
		t.Fatalf("nativeMessagesFromGateway = %#v", messages)
	}
	if text := gatewayPayloadString(json.RawMessage(`{`), "id"); text != "" {
		t.Fatalf("invalid payload string = %q", text)
	}
	if text := gatewayPayloadString(json.RawMessage(`{"id":1}`), "id"); text != "" {
		t.Fatalf("numeric payload string = %q", text)
	}
	if got := numberValue("skip"); got != 0 {
		t.Fatalf("numberValue string = %v", got)
	}
	if got := numberValue(int(4)); got != 4 {
		t.Fatalf("numberValue int = %v", got)
	}
	if got := numberValue(json.Number("12.5")); got != 12.5 {
		t.Fatalf("numberValue json = %v", got)
	}
	if got := gatewayMessageText(Message{}); got != "" {
		t.Fatalf("empty gateway message text = %q", got)
	}
	if got := gatewayMessageText(Message{Content: json.RawMessage(`not-json`)}); got != "not-json" {
		t.Fatalf("fallback gateway message text = %q", got)
	}
	testGatewayProvidersAndConfigHelpers(t)
}

func TestHermesGatewayCompletionOnlyText(t *testing.T) {
	t.Parallel()

	fake := newFakeGatewayServer(t)
	fake.setPromptEvents(
		Event{Type: evtThinkingDelta, Payload: json.RawMessage(`{"text":"thinking"}`)},
		Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"final answer"}`)},
	)
	server := newGatewayBackedHermesServer(t, fake, "")
	server.rememberGatewaySession("stored", "live-stored")

	message, err := server.SendMessage(t.Context(), "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if len(message.Parts) != 1 || message.Parts[0].Text != "final answer" || message.Parts[0].StreamedText != "" {
		t.Fatalf("completion-only message = %#v", message)
	}
}

func testGatewayProvidersAndConfigHelpers(t *testing.T) {
	t.Helper()

	if got := errors.Unwrap(StreamError{epoch: 7, err: errors.New("wrapped")}); got == nil || got.Error() != "wrapped" {
		t.Fatalf("stream error unwrap = %v", got)
	}
	var providers ProvidersResponse
	if err := json.Unmarshal([]byte(`{"providers":[]}`), &providers); err != nil || len(providers.Raw) == 0 {
		t.Fatalf("ProvidersResponse valid = %#v err=%v", providers, err)
	}
	mapped := providersFromGateway(ModelOptionsResult{Providers: []Provider{{
		Slug:         "p",
		Models:       []string{"", "named"},
		Capabilities: map[string]ProviderModelCapability{"named": {Reasoning: true}},
	}}})
	if model, ok := mapped.Providers[0].Models["named"]; !ok || len(mapped.Providers[0].Models) != 1 || !model.Reasoning {
		t.Fatalf("providersFromGateway empty model handling = %#v", mapped)
	}
	if SafePathName(" \t ") != "session" {
		t.Fatal("SafePathName did not default empty input")
	}
	if err := materializeHermesConfig(t.TempDir(), nil, nil); err != nil {
		t.Fatalf("empty config: %v", err)
	}
	originalMarshalIndent := hermesMarshalIndent
	hermesMarshalIndent = func(any, string, string) ([]byte, error) {
		return nil, errors.New("marshal failed")
	}
	if err := materializeHermesConfig(t.TempDir(), []acp.McpServer{stdioMCPServer("s", "cmd", nil, nil)}, nil); err == nil {
		t.Fatal("materializeHermesConfig ignored marshal error")
	}
	if err := WriteLease(t.TempDir(), ServerLease{}); err == nil {
		t.Fatal("WriteLease ignored marshal error")
	}
	hermesMarshalIndent = originalMarshalIndent
	homeFile := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(homeFile, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := materializeHermesConfig(homeFile, []acp.McpServer{stdioMCPServer("s", "cmd", nil, nil)}, nil); err == nil {
		t.Fatal("materializeHermesConfig accepted file home")
	}
}

func TestMaterializeHermesConfig(t *testing.T) {
	t.Run("writes non-config seeds verbatim and seeds config.yaml as-is", func(t *testing.T) {
		home := t.TempDir()
		files := map[string]string{
			"config.yaml":               "model:\n  provider: custom\n",
			"providers/litellm.yaml":    "base_url: http://localhost:4000/v1\n",
			filepath.FromSlash("a/b/c"): "nested",
		}
		if err := materializeHermesConfig(home, nil, files); err != nil {
			t.Fatalf("materializeHermesConfig: %v", err)
		}
		for relative, want := range files {
			got, err := os.ReadFile(filepath.Join(home, relative))
			if err != nil {
				t.Fatalf("read %q: %v", relative, err)
			}
			if string(got) != want {
				t.Fatalf("seed %q = %q, want %q", relative, got, want)
			}
			info, err := os.Stat(filepath.Join(home, relative))
			if err != nil {
				t.Fatalf("stat %q: %v", relative, err)
			}
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("seed %q mode = %v, want 0600", relative, info.Mode().Perm())
			}
		}
	})

	t.Run("merges wrapper mcp_servers on top of seeded config.yaml", func(t *testing.T) {
		home := t.TempDir()
		seed := "model:\n  provider: custom\n  base_url: http://localhost:4000/v1\n  key_env: LITELLM_API_KEY\n  default: gpt-4o\nmcp_servers:\n  seeded:\n    url: http://seed.example\n"
		servers := []acp.McpServer{httpMCPServer("wrapper", "https://wrapper.example/mcp", nil)}
		if err := materializeHermesConfig(home, servers, map[string]string{"config.yaml": seed}); err != nil {
			t.Fatalf("materializeHermesConfig merge: %v", err)
		}
		var config map[string]any
		data, err := os.ReadFile(filepath.Join(home, "config.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &config); err != nil {
			t.Fatalf("merged config not valid: %v (%s)", err, data)
		}
		model, ok := config["model"].(map[string]any)
		if !ok || model["provider"] != "custom" || model["default"] != "gpt-4o" || model["key_env"] != "LITELLM_API_KEY" {
			t.Fatalf("seed model block lost after merge: %#v", config["model"])
		}
		mcp, ok := config["mcp_servers"].(map[string]any)
		if !ok {
			t.Fatalf("wrapper mcp_servers missing after merge: %#v", config)
		}
		if _, ok := mcp["wrapper"].(map[string]any); !ok {
			t.Fatalf("wrapper mcp server missing after merge: %#v", mcp)
		}
		if _, ok := mcp["seeded"].(map[string]any); !ok {
			t.Fatalf("seeded mcp server dropped after merge: %#v", mcp)
		}
	})

	t.Run("writes wrapper mcp_servers without a seed", func(t *testing.T) {
		home := t.TempDir()
		servers := []acp.McpServer{httpMCPServer("wrapper", "https://wrapper.example/mcp", nil)}
		if err := materializeHermesConfig(home, servers, nil); err != nil {
			t.Fatalf("materializeHermesConfig mcp-only: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(home, "config.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var config map[string]any
		if err := json.Unmarshal(data, &config); err != nil {
			t.Fatalf("mcp-only config not valid: %v", err)
		}
		if _, ok := config["mcp_servers"].(map[string]any); !ok {
			t.Fatalf("mcp_servers missing: %#v", config)
		}
	})

	t.Run("rejects invalid seeded config.yaml when merging", func(t *testing.T) {
		home := t.TempDir()
		servers := []acp.McpServer{httpMCPServer("wrapper", "https://wrapper.example/mcp", nil)}
		err := materializeHermesConfig(home, servers, map[string]string{"config.yaml": "model: [unterminated"})
		if err == nil {
			t.Fatal("invalid seeded config.yaml accepted")
		}
		var reqErr *acp.RequestError
		if !errors.As(err, &reqErr) {
			t.Fatalf("invalid seed error = %T, want *acp.RequestError", err)
		}
	})

	t.Run("empty is a no-op", func(t *testing.T) {
		if err := materializeHermesConfig(t.TempDir(), nil, nil); err != nil {
			t.Fatalf("nil seed files: %v", err)
		}
		if err := materializeHermesConfig(t.TempDir(), nil, map[string]string{}); err != nil {
			t.Fatalf("empty seed files: %v", err)
		}
	})

	t.Run("rejects confinement escapes", func(t *testing.T) {
		absolute := filepath.Join(t.TempDir(), "abs")
		for name, relative := range map[string]string{
			"empty":           "",
			"whitespace-only": "   ",
			"absolute":        absolute,
			"parent":          "..",
			"parent-prefix":   filepath.FromSlash("../escape"),
			"parent-embedded": filepath.FromSlash("nested/../../escape"),
			"parent-trailing": filepath.FromSlash("nested/.."),
		} {
			t.Run(name, func(t *testing.T) {
				home := t.TempDir()
				err := materializeHermesConfig(home, nil, map[string]string{relative: "x"})
				if err == nil {
					t.Fatalf("seed path %q accepted", relative)
				}
				var reqErr *acp.RequestError
				if !errors.As(err, &reqErr) {
					t.Fatalf("seed path %q error = %T, want *acp.RequestError", relative, err)
				}
			})
		}
	})
}

func TestMaterializeHermesConfigSeedGuard(t *testing.T) {
	t.Run("propagates mkdir errors", func(t *testing.T) {
		home := t.TempDir()
		if err := os.WriteFile(filepath.Join(home, "config.yaml"), []byte("dir-block"), 0o600); err != nil {
			t.Fatal(err)
		}
		// "config.yaml/child" cannot create a parent dir over an existing file.
		if err := materializeHermesConfig(home, nil, map[string]string{
			filepath.FromSlash("config.yaml/child"): "x",
		}); err == nil {
			t.Fatal("materializeHermesConfig ignored mkdir error")
		}
	})

	t.Run("seed into empty root records a sorted manifest", func(t *testing.T) {
		home := t.TempDir()
		files := map[string]string{
			"config.yaml":                   "model: {}\n",
			filepath.FromSlash("a/b.json"):  "{}",
			filepath.FromSlash("providers"): "p",
		}
		if err := materializeHermesConfig(home, nil, files); err != nil {
			t.Fatalf("materializeHermesConfig: %v", err)
		}
		manifest := readHermesSeedManifest(t, home)
		want := []string{"a/b.json", "config.yaml", "providers"}
		if !reflect.DeepEqual(manifest, want) {
			t.Fatalf("manifest = %#v, want %#v", manifest, want)
		}
		for relative := range files {
			if _, err := os.Stat(filepath.Join(home, relative)); err != nil {
				t.Fatalf("seed %q not written: %v", relative, err)
			}
		}
	})

	t.Run("re-seed identical content is idempotent", func(t *testing.T) {
		home := t.TempDir()
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "same"}); err != nil {
			t.Fatalf("first seed: %v", err)
		}
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "same"}); err != nil {
			t.Fatalf("second seed: %v", err)
		}
		if _, err := os.Stat(filepath.Join(home, "foo"+hermesSeedBackupSuffix)); !os.IsNotExist(err) {
			t.Fatalf("identical re-seed created a .seed.bak (err=%v)", err)
		}
	})

	t.Run("re-seed changed content backs up prior bytes", func(t *testing.T) {
		home := t.TempDir()
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "v1"}); err != nil {
			t.Fatalf("first seed: %v", err)
		}
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "v2"}); err != nil {
			t.Fatalf("second seed: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(home, "foo"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "v2" {
			t.Fatalf("foo = %q, want v2", got)
		}
		backup, err := os.ReadFile(filepath.Join(home, "foo"+hermesSeedBackupSuffix))
		if err != nil {
			t.Fatalf("read backup: %v", err)
		}
		if string(backup) != "v1" {
			t.Fatalf("backup = %q, want v1", backup)
		}
	})

	t.Run("fails closed on a pre-existing unmanaged file", func(t *testing.T) {
		home := t.TempDir()
		if err := os.WriteFile(filepath.Join(home, "config.yaml"), []byte("operator"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := materializeHermesConfig(home, nil, map[string]string{
			"config.yaml": "model: {}\n",
			"other.txt":   "data",
		})
		if err == nil {
			t.Fatal("seed over unmanaged config.yaml accepted")
		}
		var reqErr *acp.RequestError
		if !errors.As(err, &reqErr) {
			t.Fatalf("unmanaged seed error = %T, want *acp.RequestError", err)
		}
		if !strings.Contains(err.Error(), "config.yaml") {
			t.Fatalf("error %q does not name config.yaml", err.Error())
		}
		// Nothing was written or changed: operator file intact, sibling absent,
		// no manifest.
		got, err := os.ReadFile(filepath.Join(home, "config.yaml"))
		if err != nil || string(got) != "operator" {
			t.Fatalf("operator config.yaml mutated: got=%q err=%v", got, err)
		}
		if _, err := os.Stat(filepath.Join(home, "other.txt")); !os.IsNotExist(err) {
			t.Fatalf("sibling seed written despite fail-closed (err=%v)", err)
		}
		if _, err := os.Stat(filepath.Join(home, hermesSeedManifestName)); !os.IsNotExist(err) {
			t.Fatalf("manifest written despite fail-closed (err=%v)", err)
		}
	})

	t.Run("manifest survives across passes", func(t *testing.T) {
		home := t.TempDir()
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "v1"}); err != nil {
			t.Fatalf("first seed: %v", err)
		}
		// A second pass loads the persisted manifest and treats foo as managed,
		// so re-seeding does not fail closed.
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "v2"}); err != nil {
			t.Fatalf("second seed: %v", err)
		}
		if want := []string{"foo"}; !reflect.DeepEqual(readHermesSeedManifest(t, home), want) {
			t.Fatalf("manifest = %#v, want %#v", readHermesSeedManifest(t, home), want)
		}
	})
}

func TestMaterializeHermesConfigSeedGuardErrors(t *testing.T) {
	t.Run("rejects a corrupt manifest", func(t *testing.T) {
		home := t.TempDir()
		if err := os.WriteFile(filepath.Join(home, hermesSeedManifestName), []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "x"}); err == nil {
			t.Fatal("corrupt manifest accepted")
		}
	})

	t.Run("propagates a manifest read error", func(t *testing.T) {
		home := t.TempDir()
		if err := os.Mkdir(filepath.Join(home, hermesSeedManifestName), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "x"}); err == nil {
			t.Fatal("materializeHermesConfig ignored manifest read error")
		}
	})

	t.Run("propagates a manifest write error", func(t *testing.T) {
		home := t.TempDir()
		original := hermesMarshalIndent
		hermesMarshalIndent = func(any, string, string) ([]byte, error) {
			return nil, errors.New("marshal failed")
		}
		defer func() { hermesMarshalIndent = original }()
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "x"}); err == nil {
			t.Fatal("materializeHermesConfig ignored manifest marshal error")
		}
	})

	t.Run("propagates a backup write error", func(t *testing.T) {
		home := t.TempDir()
		if err := os.WriteFile(filepath.Join(home, "foo"), []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(home, hermesSeedManifestName), []byte(`["foo"]`), 0o600); err != nil {
			t.Fatal(err)
		}
		// A directory at the backup path blocks the backup copy.
		if err := os.Mkdir(filepath.Join(home, "foo"+hermesSeedBackupSuffix), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "new"}); err == nil {
			t.Fatal("materializeHermesConfig ignored backup write error")
		}
	})

	t.Run("propagates a read error for a managed directory target", func(t *testing.T) {
		home := t.TempDir()
		if err := os.WriteFile(filepath.Join(home, hermesSeedManifestName), []byte(`["mdir"]`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(home, "mdir"), 0o700); err != nil {
			t.Fatal(err)
		}
		// mdir is managed, so the guard proceeds and the read of a directory fails.
		if err := materializeHermesConfig(home, nil, map[string]string{"mdir": "x"}); err == nil {
			t.Fatal("materializeHermesConfig ignored managed-target read error")
		}
	})
}

// readHermesSeedManifest decodes the ownership manifest under home for tests.
func readHermesSeedManifest(t *testing.T, home string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(home, hermesSeedManifestName))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var entries []string
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}

	return entries
}

func TestHermesGatewayServerEdgeBranches(t *testing.T) {
	t.Run("identity schema drift", func(t *testing.T) {
		for _, tt := range []struct {
			name  string
			want  string
			setup func(*fakeGatewayServer)
			call  func(context.Context, *hermesServer) error
		}{
			{
				name: "create missing live",
				want: "session.create response missing session_id",
				setup: func(fake *fakeGatewayServer) {
					fake.setCreateNoLive()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.CreateSession(ctx, "")

					return err
				},
			},
			{
				name: "create missing stored",
				want: "session.create response missing stored_session_id",
				setup: func(fake *fakeGatewayServer) {
					fake.setCreateNoStored()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.CreateSession(ctx, "")

					return err
				},
			},
			{
				name: "get missing resume live",
				want: "session.resume response missing session_id",
				setup: func(fake *fakeGatewayServer) {
					fake.setResumeNoLive()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.GetSession(ctx, "stored")

					return err
				},
			},
			{
				name: "get missing resume key",
				want: "session.resume response missing session_key",
				setup: func(fake *fakeGatewayServer) {
					fake.setResumeNoKey()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.GetSession(ctx, "stored")

					return err
				},
			},
			{
				name: "ensure missing resume live",
				want: "session.resume response missing session_id",
				setup: func(fake *fakeGatewayServer) {
					fake.setResumeNoLive()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.Messages(ctx, "stored")

					return err
				},
			},
			{
				name: "ensure missing resume key",
				want: "session.resume response missing session_key",
				setup: func(fake *fakeGatewayServer) {
					fake.setResumeNoKey()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.Messages(ctx, "stored")

					return err
				},
			},
			{
				name: "get active missing id",
				want: "active_list response missing id",
				setup: func(fake *fakeGatewayServer) {
					fake.setActiveNoID()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.GetSession(ctx, "stored-1")

					return err
				},
			},
			{
				name: "get active missing session key",
				want: "active_list response missing session_key",
				setup: func(fake *fakeGatewayServer) {
					fake.setActiveNoKey()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.GetSession(ctx, "stored-1")

					return err
				},
			},
			{
				name: "list missing id",
				want: "active_list response missing id",
				setup: func(fake *fakeGatewayServer) {
					fake.setActiveNoID()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.ListSessions(ctx, "")

					return err
				},
			},
			{
				name: "list missing session key",
				want: "active_list response missing session_key",
				setup: func(fake *fakeGatewayServer) {
					fake.setActiveNoKey()
				},
				call: func(ctx context.Context, server *hermesServer) error {
					_, err := server.ListSessions(ctx, "")

					return err
				},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				fake := newFakeGatewayServer(t)
				tt.setup(fake)
				server := newGatewayBackedHermesServer(t, fake, "")
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				err := tt.call(ctx, server)
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("schema drift error = %v, want %q", err, tt.want)
				}
			})
		}
	})
}

func TestHermesGatewayServerMappingAndAccessorBranches(t *testing.T) {
	t.Run("get session from active list mapping", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		server := newGatewayBackedHermesServer(t, fake, "")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		session, err := server.GetSession(ctx, "stored-1")
		if err != nil || session.ID != "stored-1" || server.liveSessionID("stored-1") != "live-1" {
			t.Fatalf("GetSession active mapping = %#v live=%q err=%v", session, server.liveSessionID("stored-1"), err)
		}
		if len(fake.callsFor("session.resume")) != 0 {
			t.Fatalf("GetSession active mapping called resume: %#v", fake.callsFor("session.resume"))
		}
	})

	t.Run("resume rotation uses returned session key", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setResumeKey("stored-rotated")
		server := newGatewayBackedHermesServer(t, fake, "")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		session, err := server.GetSession(ctx, "stored")
		if err != nil || session.ID != "stored-rotated" || server.liveSessionID("stored-rotated") != "live-stored-rotated" {
			t.Fatalf("GetSession rotated resume = %#v live=%q err=%v", session, server.liveSessionID("stored-rotated"), err)
		}
		if live := server.liveSessionID("stored"); live != "" {
			t.Fatalf("old session key kept live mapping %q", live)
		}
	})

	t.Run("accessors and missing live mapping replies", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		server := newGatewayBackedHermesServer(t, fake, "")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if server.Events() == nil || server.EventErrors() == nil || server.XDGDirs().Root == "" {
			t.Fatal("server accessors returned empty values")
		}
		if live := server.anyLiveSessionID(); live != "" {
			t.Fatalf("unexpected live session = %q", live)
		}
		server.rememberGatewaySession("", "live")
		server.rememberGatewaySession("stored", "")
		if live := server.liveSessionID("stored"); live != "" {
			t.Fatalf("incomplete remember stored live %q", live)
		}
		created, err := server.CreateSession(ctx, "")
		if err != nil || created.ID != "stored-1" || created.Model.ProviderID != "" {
			t.Fatalf("CreateSession no model = %#v err=%v", created, err)
		}
		if list, err := server.ListSessions(ctx, "/other"); err != nil || len(list) != 0 {
			t.Fatalf("ListSessions cwd mismatch = %#v err=%v", list, err)
		}
		if err := server.ReplyPermission(ctx, PermissionRequest{SessionID: "unmapped"}, "once", "ignored"); err == nil {
			t.Fatal("ReplyPermission accepted missing live mapping")
		}
		if err := server.ReplyQuestion(ctx, QuestionRequest{SessionID: "unmapped"}, [][]string{{"a"}}); err == nil {
			t.Fatal("ReplyQuestion accepted missing live mapping")
		}
		if err := server.RejectQuestion(ctx, QuestionRequest{SessionID: "unmapped"}); err == nil {
			t.Fatal("RejectQuestion accepted missing live mapping")
		}
		for _, method := range []string{"approval.respond", "clarify.respond"} {
			calls := fake.callsFor(method)
			if len(calls) != 0 {
				t.Fatalf("%s calls with missing mapping = %#v", method, calls)
			}
		}
	})

	t.Run("nonblocking event drops", func(t *testing.T) {
		server := &hermesServer{events: make(chan TurnEvent, 1)}
		server.events <- TurnEvent{Type: "filled"}
		server.forwardGatewayPart("stored", "message", Event{Type: "message.delta"}, "text")
		server.forwardGatewayPermission("stored", "live", Event{Payload: json.RawMessage(`{}`)})
		server.forwardGatewayQuestion("stored", "live", Event{Payload: json.RawMessage(`{}`)})
		if got := len(server.events); got != 1 {
			t.Fatalf("event channel len = %d", got)
		}
	})
}

func TestHermesGatewayServerFailureBranches(t *testing.T) {
	for _, tt := range []struct {
		name   string
		method string
		call   func(context.Context, *hermesServer) error
	}{
		{
			name:   "create",
			method: "session.create",
			call: func(ctx context.Context, server *hermesServer) error {
				_, err := server.CreateSession(ctx, "title")

				return err
			},
		},
		{
			name:   "get resume",
			method: "session.resume",
			call: func(ctx context.Context, server *hermesServer) error {
				_, err := server.GetSession(ctx, "stored")

				return err
			},
		},
		{
			name:   "list",
			method: "session.active_list",
			call: func(ctx context.Context, server *hermesServer) error {
				_, err := server.ListSessions(ctx, "")

				return err
			},
		},
		{
			name:   "ensure live",
			method: "session.resume",
			call: func(ctx context.Context, server *hermesServer) error {
				_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})

				return err
			},
		},
		{
			name:   "submit",
			method: "prompt.submit",
			call: func(ctx context.Context, server *hermesServer) error {
				server.rememberGatewaySession("stored", "live-stored")
				_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})

				return err
			},
		},
		{
			name:   "history",
			method: "session.history",
			call: func(ctx context.Context, server *hermesServer) error {
				server.rememberGatewaySession("stored", "live-stored")
				_, err := server.Messages(ctx, "stored")

				return err
			},
		},
		{
			name:   "messages ensure live",
			method: "session.resume",
			call: func(ctx context.Context, server *hermesServer) error {
				_, err := server.Messages(ctx, "stored")

				return err
			},
		},
		{
			name:   "branch",
			method: "session.branch",
			call: func(ctx context.Context, server *hermesServer) error {
				server.rememberGatewaySession("stored", "live-stored")
				_, err := server.Fork(ctx, "stored", "")

				return err
			},
		},
		{
			name:   "fork ensure live",
			method: "session.resume",
			call: func(ctx context.Context, server *hermesServer) error {
				_, err := server.Fork(ctx, "stored", "")

				return err
			},
		},
		{
			name:   "model options",
			method: "model.options",
			call: func(ctx context.Context, server *hermesServer) error {
				_, err := server.ConfigProviders(ctx)

				return err
			},
		},
	} {
		t.Run(tt.name+" error", func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			fake.setFail(tt.method)
			server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := tt.call(ctx, server); err == nil {
				t.Fatalf("%s error was nil", tt.name)
			}
		})
	}

	t.Run("fork retry resume error", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setBranchNotFoundOnce()
		fake.setFail("session.resume")
		server := newGatewayBackedHermesServer(t, fake, "")
		server.rememberGatewaySession("stored", "live-stored")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := server.Fork(ctx, "stored", ""); err == nil {
			t.Fatal("fork retry resume error was nil")
		}
	})

	for _, tt := range []struct {
		name      string
		configure func(*fakeGatewayServer)
	}{
		{
			name: "missing branch session id",
			configure: func(fake *fakeGatewayServer) {
				fake.branchNoSession = true
			},
		},
		{
			name: "missing branch active session",
			configure: func(fake *fakeGatewayServer) {
				fake.branchNoActive = true
			},
		},
		{
			name: "branch active list failure",
			configure: func(fake *fakeGatewayServer) {
				fake.setFail("session.active_list")
			},
		},
		{
			name: "missing branch stored key",
			configure: func(fake *fakeGatewayServer) {
				fake.branchNoKey = true
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			tt.configure(fake)
			server := newGatewayBackedHermesServer(t, fake, "")
			server.rememberGatewaySession("stored", "live-stored")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := server.Fork(ctx, "stored", ""); err == nil {
				t.Fatal("fork schema drift error was nil")
			}
		})
	}

	t.Run("event stream closed mid turn", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setPromptEvents()
		fake.setCloseAfterResult("prompt.submit")
		server := newGatewayBackedHermesServer(t, fake, "")
		server.rememberGatewaySession("stored", "live-stored")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
		if !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("mid-turn close error = %v", err)
		}
		// The disconnect is wired into the server error channel so the prompt
		// loop can fence the turn with the uniform hermes_turn_failed error.
		select {
		case fed := <-server.EventErrors():
			if fed == nil {
				t.Fatal("mid-turn disconnect fed nil error")
			}
		default:
			t.Fatal("mid-turn disconnect not fed into EventErrors")
		}
	})

	t.Run("context deadline while waiting for events", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setPromptEvents()
		server := newGatewayBackedHermesServer(t, fake, "")
		server.rememberGatewaySession("stored", "live-stored")
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline error = %v", err)
		}
	})
}

func TestStartHermesServerGatewayFakeExecutable(t *testing.T) {
	helper := fakeHermesGatewayExecutable(t, fakeGatewayModeOK)
	root := t.TempDir()
	cwd := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := StartServer(ctx, StartOptions{
		ACPSessionID:   "session/one",
		Root:           root,
		Cwd:            cwd,
		ExecutablePath: helper,
		DefaultModel:   "openai/gpt-test",
		Env:            map[string]string{"BASE_ENV": "base"},
		HealthTimeout:  5 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
		MCPServers: []acp.McpServer{
			stdioMCPServer("stdio", "cmd", []string{"arg"}, map[string]string{"A": "1"}),
			httpMCPServer("http", "https://example.test/mcp", map[string]string{"Authorization": "token"}),
		},
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	server, serverOK := client.(*hermesServer)
	if !serverOK {
		t.Fatalf("client type = %T", client)
	}
	if server.xdg.Root == "" || !strings.Contains(filepath.Base(server.xdg.Root), "session_one") {
		t.Fatalf("xdg dirs = %#v", server.xdg)
	}
	leasePath := filepath.Join(server.xdg.State, LeaseFileName)
	leaseData, err := os.ReadFile(leasePath)
	if err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if !strings.Contains(string(leaseData), `"tokenHash"`) || strings.Contains(string(leaseData), "PasswordHash") {
		t.Fatalf("lease = %s", leaseData)
	}
	configData, err := os.ReadFile(filepath.Join(server.xdg.Root, "config.yaml"))
	if err != nil {
		t.Fatalf("read mcp config: %v", err)
	}
	if !strings.Contains(string(configData), "mcp_servers") || !strings.Contains(string(configData), "https://example.test/mcp") {
		t.Fatalf("mcp config = %s", configData)
	}
	if _, err := client.CreateSession(ctx, "Created"); err != nil {
		t.Fatalf("CreateSession through fake executable: %v", err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(leasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease after close = %v", err)
	}
}

func TestStartHermesServerGatewayFaults(t *testing.T) {
	ctx := context.Background()
	if _, err := StartServer(ctx, StartOptions{ExecutablePath: filepath.Join(t.TempDir(), "missing-hermes")}); err == nil {
		t.Fatal("missing executable unexpectedly started")
	}
	if _, err := StartServer(ctx, StartOptions{Root: "["}); err == nil {
		t.Fatal("invalid reap glob unexpectedly succeeded")
	}
	if _, err := StartServer(ctx, StartOptions{Root: t.TempDir(), ACPSessionID: ACPSessionIDString(string([]byte{0}))}); err == nil {
		t.Fatal("invalid session path unexpectedly succeeded")
	}
	if _, err := StartServer(ctx, StartOptions{ExistingXDG: XDGDirs{Root: filepath.Join(t.TempDir(), "root")}}); err == nil {
		t.Fatal("incomplete existing xdg unexpectedly succeeded")
	}

	restoreHermesClientSeams(t)
	hermesWriteLease = func(string, ServerLease) error {
		return errors.New("lease failed")
	}
	if _, err := StartServer(ctx, StartOptions{
		ExecutablePath: fakeHermesGatewayExecutable(t, fakeGatewayModeOK),
		ExistingXDG:    testXDGDirs(t),
		HealthTimeout:  5 * time.Second,
	}); err == nil || !strings.Contains(err.Error(), "lease failed") {
		t.Fatalf("lease failure error = %v", err)
	}

	if _, err := StartServer(ctx, StartOptions{
		ExecutablePath: fakeHermesGatewayExecutable(t, fakeGatewayModeStatusOnly),
		ExistingXDG:    testXDGDirs(t),
		HealthTimeout:  500 * time.Millisecond,
	}); err == nil {
		t.Fatal("gateway readiness failure unexpectedly succeeded")
	}
}

// TestGatewaySupervisorReconnectsOnIdleDisconnect proves HW4 idle reconnect:
// when the WebSocket drops while no turn is in progress, the supervisor redials
// the still-running process and swaps in the new connection.
func TestGatewaySupervisorReconnectsOnIdleDisconnect(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	redialed := make(chan struct{}, 2)
	server.enableReconnect(func(context.Context) (*Client, error) {
		client := fake.dialClient(t)
		redialed <- struct{}{}

		return client, nil
	})
	original := server.gatewayClient()

	// Simulate an idle disconnect by dropping the current connection.
	_ = original.Close(websocket.StatusNormalClosure, "drop")

	select {
	case <-redialed:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not reconnect after idle disconnect")
	}
	swapDeadline := time.After(2 * time.Second)
	swapPoll := time.NewTicker(5 * time.Millisecond)
	defer swapPoll.Stop()
	for server.gatewayClient() == original {
		select {
		case <-swapDeadline:
			t.Fatal("gateway not swapped after reconnect")
		case <-swapPoll.C:
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := server.CreateSession(ctx, ""); err != nil {
		t.Fatalf("CreateSession after reconnect: %v", err)
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestGatewaySupervisorWaitsForTurnBeforeReconnect(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	reconnies := make(chan struct{}, 4)
	server.enableReconnect(func(context.Context) (*Client, error) {
		client := fake.dialClient(t)
		reconnies <- struct{}{}

		return client, nil
	})

	server.beginGatewayTurn()
	original := server.gatewayClient()
	_ = original.Close(websocket.StatusNormalClosure, "drop")

	select {
	case <-reconnies:
		t.Fatal("reconnected while a turn was in progress")
	case <-time.After(100 * time.Millisecond):
	}
	server.endGatewayTurn()
	select {
	case <-reconnies:
	case <-time.After(2 * time.Second):
		t.Fatal("did not reconnect after the turn ended")
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSuperviseGatewayStopsAfterTurnWhenClosed(t *testing.T) {
	restoreLeaseReapSeams(t)
	reachedIdle := make(chan struct{})
	releaseIdle := make(chan struct{})
	superviseGatewayAfterTurnIdle = func() {
		close(reachedIdle)
		<-releaseIdle
	}

	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	reconnied := make(chan struct{}, 1)
	server.enableReconnect(func(context.Context) (*Client, error) {
		reconnied <- struct{}{}

		return fake.dialClient(t), nil
	})

	server.beginGatewayTurn()
	original := server.gatewayClient()
	_ = original.Close(websocket.StatusNormalClosure, "drop")
	server.endGatewayTurn()
	<-reachedIdle

	// Shut down after the turn becomes idle but before the supervisor can
	// redial; the supervisor must stop without reconnecting.
	close(server.closed)
	close(releaseIdle)

	select {
	case <-reconnied:
		t.Fatal("supervisor reconnected during shutdown")
	case <-time.After(150 * time.Millisecond):
	}
	_ = original.Close(websocket.StatusNormalClosure, "done")
}

func TestReconnectGatewayRedialErrorBranches(t *testing.T) {
	restoreLeaseReapSeams(t)
	leaseReapSleep = func(time.Duration) {}
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	server.turnIdle = sync.NewCond(&server.connMu)
	original := server.gatewayClient()
	server.redial = func(context.Context) (*Client, error) {
		return nil, errors.New("no dial")
	}
	// Redial error with the server still open: logs and backs off.
	server.reconnectGateway()

	// Redial succeeds but the server has since closed: the new connection is
	// discarded instead of being installed.
	close(server.closed)
	server.redial = func(context.Context) (*Client, error) {
		return fake.dialClient(t), nil
	}
	server.reconnectGateway()
	if server.gatewayClient() != original {
		t.Fatal("reconnect installed a connection after the server closed")
	}
	_ = original.Close(websocket.StatusNormalClosure, "done")

	// endGatewayTurn is safe with no active turn and without reconnect wired.
	plain := &hermesServer{}
	plain.endGatewayTurn()
}

func TestXDGLeaseAndHelpers(t *testing.T) {
	root := t.TempDir()
	xdg, err := CreateXDGDirs(root, "")
	if err != nil {
		t.Fatalf("CreateXDGDirs: %v", err)
	}
	if filepath.Base(xdg.Root) != "session" {
		t.Fatalf("default xdg root = %#v", xdg)
	}
	if err := ensureXDGDirs(XDGDirs{Root: "", Data: "x", Config: "x", Cache: "x", State: "x"}); err == nil {
		t.Fatal("ensureXDGDirs accepted empty root")
	}
	if err := WriteLease(xdg.State, ServerLease{PID: 0, Port: 1, TokenHash: PasswordHash("token")}); err != nil {
		t.Fatalf("WriteLease: %v", err)
	}
	badRoot := string([]byte{0})
	if err := WriteLease(badRoot, ServerLease{}); err == nil {
		t.Fatal("WriteLease accepted invalid path")
	}
	if err := os.MkdirAll(filepath.Join(root, "bad", "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bad", "state", LeaseFileName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reapStaleLeases(root, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("reapStaleLeases: %v", err)
	}
	if err := reapStaleLeases("", nil); err != nil {
		t.Fatalf("empty reapStaleLeases: %v", err)
	}
	if got := SafePathName("../a:b"); got != "__a_b" {
		t.Fatalf("SafePathName = %q", got)
	}
	for _, value := range []any{float64(-1), int(-1), json.Number("bad")} {
		if got, ok := IntFromNumber(value); ok || got != 0 {
			t.Fatalf("IntFromNumber(%#v) = %d, %v", value, got, ok)
		}
	}
	if got, ok := IntFromNumber(json.Number("12")); !ok || got != 12 {
		t.Fatalf("IntFromNumber json number = %d, %v", got, ok)
	}
}

func TestLeaseReaperVerifiesProcessIdentity(t *testing.T) {
	root := t.TempDir()
	xdg, err := CreateXDGDirs(root, "lease")
	if err != nil {
		t.Fatal(err)
	}
	leasePath := filepath.Join(xdg.State, LeaseFileName)
	baseIdentity := ProcessIdentity{
		StartTime: "start",
		Cmdline:   []string{"/usr/bin/hermes", "serve"},
		Env: map[string]string{
			"HERMES_HOME":                    xdg.Root,
			"HERMES_DASHBOARD_SESSION_TOKEN": "token",
		},
	}
	baseLease := ServerLease{
		PID:              999999,
		TokenHash:        PasswordHash("token"),
		XDGRoot:          xdg.Root,
		ProcessStartTime: "start",
	}

	restoreHermesClientSeams(t)
	InspectProcess = func(int) (ProcessIdentity, error) {
		return baseIdentity, nil
	}
	if !leaseMatchesProcess(leasePath, baseLease) {
		t.Fatal("matching lease did not match")
	}
	if !cmdlineLooksLikeHermesServe([]string{"/tmp/hermes"}) || cmdlineLooksLikeHermesServe([]string{"node"}) {
		t.Fatal("cmdline Hermes detection mismatch")
	}
	if leaseMatchesProcess(leasePath, ServerLease{PID: 0, ProcessStartTime: "start"}) {
		t.Fatal("zero pid lease matched")
	}
	if leaseMatchesProcess(leasePath, ServerLease{PID: 1}) {
		t.Fatal("missing start time lease matched")
	}

	for _, tt := range []struct {
		name     string
		identity ProcessIdentity
		lease    ServerLease
		err      error
	}{
		{name: "inspect error", identity: baseIdentity, lease: baseLease, err: errors.New("inspect failed")},
		{name: "start mismatch", identity: ProcessIdentity{StartTime: "other", Cmdline: baseIdentity.Cmdline, Env: baseIdentity.Env}, lease: baseLease},
		{name: "home mismatch", identity: ProcessIdentity{StartTime: "start", Cmdline: baseIdentity.Cmdline, Env: map[string]string{"HERMES_HOME": t.TempDir(), "HERMES_DASHBOARD_SESSION_TOKEN": "token"}}, lease: baseLease},
		{name: "token mismatch", identity: ProcessIdentity{StartTime: "start", Cmdline: baseIdentity.Cmdline, Env: map[string]string{"HERMES_HOME": xdg.Root, "HERMES_DASHBOARD_SESSION_TOKEN": "wrong"}}, lease: baseLease},
		{name: "root mismatch", identity: baseIdentity, lease: ServerLease{PID: baseLease.PID, TokenHash: baseLease.TokenHash, XDGRoot: t.TempDir(), ProcessStartTime: baseLease.ProcessStartTime}},
		{name: "cmdline mismatch", identity: ProcessIdentity{StartTime: "start", Cmdline: []string{"node"}, Env: baseIdentity.Env}, lease: baseLease},
	} {
		t.Run(tt.name, func(t *testing.T) {
			InspectProcess = func(int) (ProcessIdentity, error) {
				return tt.identity, tt.err
			}
			if leaseMatchesProcess(leasePath, tt.lease) {
				t.Fatal("mismatched lease matched")
			}
		})
	}

	restoreLeaseReapSeams(t)
	LeaseReapTimeout = 40 * time.Millisecond
	LeaseReapPollInterval = time.Millisecond

	// Confirmed dead: the process matches for identity but is gone when the
	// ladder verifies it, so the lease is removed.
	inspectCalls := 0
	InspectProcess = func(int) (ProcessIdentity, error) {
		inspectCalls++
		if inspectCalls == 1 {
			return baseIdentity, nil
		}

		return ProcessIdentity{}, os.ErrNotExist
	}
	if err := WriteLease(xdg.State, baseLease); err != nil {
		t.Fatal(err)
	}
	ReapLeaseFile(leasePath, slog.New(slog.DiscardHandler))
	if _, err := os.Stat(leasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease after reap of dead process = %v", err)
	}

	// Survives termination: the process stays alive and identity-matched, so
	// the lease is KEPT for the next startup retry.
	InspectProcess = func(int) (ProcessIdentity, error) {
		return baseIdentity, nil
	}
	if err := WriteLease(xdg.State, baseLease); err != nil {
		t.Fatal(err)
	}
	ReapLeaseFile(leasePath, slog.New(slog.DiscardHandler))
	if _, err := os.Stat(leasePath); err != nil {
		t.Fatalf("lease of surviving process was removed: %v", err)
	}
	_ = os.Remove(leasePath)
	ReapLeaseFile(t.TempDir(), nil)
}

func TestNativeUnmarshalErrors(t *testing.T) {
	var part Part
	if err := part.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("Part accepted malformed JSON")
	}
	var event TurnEvent
	if err := event.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("TurnEvent accepted malformed JSON")
	}
	var providers ProvidersResponse
	if err := providers.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("ProvidersResponse accepted malformed JSON")
	}
}

const (
	fakeGatewayModeOK         = "ok"
	fakeGatewayModeStatusOnly = "status-only"
)

func TestFakeHermesGatewayProcessHelper(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_GATEWAY_HELPER") != "1" {
		return
	}
	if err := runFakeHermesGatewayProcess(os.Args, os.Getenv("ACP_GO_HERMES_GATEWAY_MODE")); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func fakeHermesGatewayExecutable(t *testing.T, mode string) string {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	script := filepath.Join(t.TempDir(), "fake-hermes")
	body := fmt.Sprintf("#!/bin/sh\nACP_GO_HERMES_GATEWAY_HELPER=1 ACP_GO_HERMES_GATEWAY_MODE=%s exec %q -test.run=TestFakeHermesGatewayProcessHelper -- \"$@\"\n", mode, testBinary)
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}

	return script
}

func runFakeHermesGatewayProcess(args []string, mode string) error {
	for _, arg := range args {
		if arg == "--version" {
			_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.18.2 (fake)")

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
		return fmt.Errorf("missing --port in args %q", strings.Join(args, " "))
	}
	_, _ = fmt.Fprintln(os.Stdout, "native stdout noise before websocket readiness")
	_, _ = fmt.Fprintln(os.Stderr, "native stderr noise before websocket readiness")
	handler := http.NewServeMux()
	handler.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	if mode != fakeGatewayModeStatusOnly {
		handler.HandleFunc("/api/ws", func(w http.ResponseWriter, r *http.Request) {
			token := os.Getenv("HERMES_DASHBOARD_SESSION_TOKEN")
			if token != "" && r.URL.Query().Get("token") != token && r.Header.Get("X-Hermes-Session-Token") != token {
				w.WriteHeader(http.StatusUnauthorized)

				return
			}
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "done")
			writeGatewayEvent(r.Context(), conn, Event{Type: "gateway.ready"})
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
				result := gatewayProcessResult(req.Method, params)
				if err := writeGatewayResult(r.Context(), conn, req.ID, result); err != nil {
					return
				}
			}
		})
	}
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	return server.ListenAndServe()
}

func gatewayProcessResult(method string, params map[string]any) any {
	switch method {
	case "session.create":
		return map[string]any{"session_id": "live-fake", "stored_session_id": "stored-fake"}
	case "session.resume":
		stored, _ := params["session_id"].(string)

		return map[string]any{"session_id": "live-" + stored, "session_key": stored}
	case "session.active_list":
		return map[string]any{"sessions": []any{}}
	case "session.history":
		return map[string]any{"count": 0, "messages": []any{}}
	case "model.options":
		return map[string]any{"model": "anthropic/claude-sonnet-4", "provider": "", "providers": []any{}}
	default:
		return map[string]any{}
	}
}

func writeGatewayResult(ctx context.Context, conn *websocket.Conn, id int64, result any) error {
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	if err != nil {
		return err
	}

	return conn.Write(ctx, websocket.MessageText, data)
}

func writeGatewayEvent(ctx context.Context, conn *websocket.Conn, event Event) {
	data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "event", "params": event})
	_ = conn.Write(ctx, websocket.MessageText, data)
}

func restoreHermesClientSeams(t *testing.T) {
	t.Helper()
	marshalIndent := hermesMarshalIndent
	writeLease2 := hermesWriteLease
	inspectProcess := InspectProcess
	t.Cleanup(func() {
		hermesMarshalIndent = marshalIndent
		hermesWriteLease = writeLease2
		InspectProcess = inspectProcess
	})
}

func restoreLeaseReapSeams(t *testing.T) {
	t.Helper()
	timeout := LeaseReapTimeout
	interval := LeaseReapPollInterval
	sleep := leaseReapSleep
	now := leaseReapNow
	afterTurnIdle := superviseGatewayAfterTurnIdle
	t.Cleanup(func() {
		LeaseReapTimeout = timeout
		LeaseReapPollInterval = interval
		leaseReapSleep = sleep
		leaseReapNow = now
		superviseGatewayAfterTurnIdle = afterTurnIdle
	})
}

func testXDGDirs(t *testing.T) XDGDirs {
	t.Helper()
	root := t.TempDir()

	return XDGDirs{
		Root:   root,
		Data:   filepath.Join(root, "data"),
		Config: filepath.Join(root, "config"),
		Cache:  filepath.Join(root, "cache"),
		State:  filepath.Join(root, "state"),
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}

func stdioMCPServer(name string, command string, args []string, env map[string]string) acp.McpServer {
	variables := make([]acp.EnvVariable, 0, len(env))
	for key, value := range env {
		variables = append(variables, acp.EnvVariable{Name: key, Value: value})
	}

	return acp.McpServer{Stdio: &acp.McpServerStdio{
		Name:    name,
		Command: command,
		Args:    append([]string(nil), args...),
		Env:     variables,
	}}
}

func httpMCPServer(name string, url string, headers map[string]string) acp.McpServer {
	values := make([]acp.HttpHeader, 0, len(headers))
	for key, value := range headers {
		values = append(values, acp.HttpHeader{Name: key, Value: value})
	}

	return acp.McpServer{Http: &acp.McpServerHttpInline{
		Name:    name,
		Url:     url,
		Headers: values,
	}}
}

// T1 — provider error → structured failure (native boundary = fake gateway).
func TestTurnFailureProviderErrorAtGatewayBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, tt := range []struct {
		name     string
		events   []Event
		message  string
		status   int
		provider string
	}{
		{
			name: "message.complete finish error (rate limit)",
			events: []Event{{
				Type:    evtMessageComplete,
				Payload: json.RawMessage(`{"finish":"error","error":{"message":"rate limited by upstream","statusCode":429,"providerCode":"rate_limit"}}`),
			}},
			message:  "rate limited by upstream",
			status:   429,
			provider: "rate_limit",
		},
		{
			name: "session.error event (auth)",
			events: []Event{{
				Type:    evtSessionError,
				Payload: json.RawMessage(`{"error":{"message":"invalid api key","statusCode":401,"providerCode":"auth_error"}}`),
			}},
			message:  "invalid api key",
			status:   401,
			provider: "auth_error",
		},
		{
			name: "session.error event (flat fields)",
			events: []Event{{
				Type:    evtSessionError,
				Payload: json.RawMessage(`{"message":"gateway exploded","statusCode":500,"providerCode":"explode"}`),
			}},
			message:  "gateway exploded",
			status:   500,
			provider: "explode",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			fake.setPromptEvents(tt.events...)
			server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
			server.rememberGatewaySession("stored", "live-stored")

			_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})

			var failure *TurnFailureError
			if !errors.As(err, &failure) {
				t.Fatalf("SendMessage error = %v (%T), want *TurnFailureError", err, err)
			}

			if failure.cause != CauseProvider {
				t.Fatalf("cause = %q, want provider", failure.cause)
			}

			if !strings.Contains(failure.message, tt.message) {
				t.Fatalf("message = %q, want substring %q", failure.message, tt.message)
			}

			if failure.statusCode != tt.status {
				t.Fatalf("statusCode = %d, want %d", failure.statusCode, tt.status)
			}

			if failure.providerCode != tt.provider {
				t.Fatalf("providerCode = %q, want %q", failure.providerCode, tt.provider)
			}
		})
	}
}

// reportGatewayDisconnect substitutes the stream-closed sentinel when the read
// loop closed without a specific error (a clean close).
func TestReportGatewayDisconnectNilCause(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")

	err := server.reportGatewayDisconnect(nil)

	var failure *TurnFailureError
	if !errors.As(err, &failure) || failure.cause != CauseTransport {
		t.Fatalf("nil-cause disconnect = %v, want transport", err)
	}

	if failure.message != errGatewayStreamClosed.Error() {
		t.Fatalf("nil-cause message = %q, want %q", failure.message, errGatewayStreamClosed.Error())
	}
}

// An abrupt (frameless) disconnect surfaces the real transport read error the
// gateway read loop parked, not the clean stream-closed sentinel.
func TestTurnFailureAbruptDisconnectRecoversRealCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fake := newFakeGatewayServer(t)
	fake.setPromptEvents() // no completion: the turn waits, then the peer drops
	fake.setCloseNowAfterResult("prompt.submit")
	server := newGatewayBackedHermesServer(t, fake, "")
	server.rememberGatewaySession("stored", "live-stored")

	_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
	if !IsGatewayDisconnect(err) {
		t.Fatalf("abrupt-close error = %v, want a disconnect", err)
	}

	var failure *TurnFailureError
	if !errors.As(err, &failure) || failure.cause != CauseTransport {
		t.Fatalf("abrupt-close failure = %v, want transport TurnFailureError", err)
	}

	if failure.message == errGatewayStreamClosed.Error() {
		t.Fatalf("abrupt disconnect surfaced the sentinel instead of the real read error")
	}
}

// T4 — one malformed gateway line is skipped without hanging or misreporting the
// turn: the internal client records it and the turn completes normally.
func TestTurnFailureMalformedLineNotFatal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fake := newFakeGatewayServer(t)
	fake.setPromptRawFrames("this is not json{")
	fake.setPromptEvents(Event{
		Type:    evtMessageComplete,
		Payload: json.RawMessage(`{"usage":{"total_tokens":3}}`),
	})
	server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
	server.rememberGatewaySession("stored", "live-stored")

	message, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
	if err != nil {
		t.Fatalf("malformed line was fatal to the turn: %v", err)
	}

	if message.Info.Finish != valStop || message.Info.Tokens.Total != 3 {
		t.Fatalf("turn did not complete cleanly after malformed line: %#v", message.Info)
	}
}

// TestReportGatewayDisconnectCarriesRealCause proves a transport disconnect
// carries the real cause (never a bare EOF / generic string) at the gateway
// boundary: reportGatewayDisconnect classifies it as a transport turn failure
// and feeds the real cause into EventErrors.
func TestReportGatewayDisconnectCarriesRealCause(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")

	err := server.reportGatewayDisconnect(errors.New("read tcp 127.0.0.1: connection reset by peer"))
	if !IsGatewayDisconnect(err) {
		t.Fatalf("reportGatewayDisconnect error is not a disconnect: %v", err)
	}

	var failure *TurnFailureError
	if !errors.As(err, &failure) || failure.cause != CauseTransport {
		t.Fatalf("disconnect failure = %v, want transport TurnFailureError", err)
	}

	if !strings.Contains(failure.message, "connection reset by peer") {
		t.Fatalf("disconnect message = %q, want real cause", failure.message)
	}

	select {
	case fed := <-server.EventErrors():
		if !strings.Contains(fed.Error(), "connection reset by peer") {
			t.Fatalf("fed error = %v, want real cause", fed)
		}
	default:
		t.Fatal("disconnect cause not fed into EventErrors")
	}
}
