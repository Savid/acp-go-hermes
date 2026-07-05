package hermesacp

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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

type gatewayRPCCall struct {
	Method string
	Params map[string]any
}

type fakeGatewayServer struct {
	t      *testing.T
	server *httptest.Server

	mu               sync.Mutex
	calls            []gatewayRPCCall
	closeAfterResult string
	failMethods      map[string]struct{}
	promptEvents     *[]nativehermes.Event
	malformedAfter   map[string]struct{}
	branchNotFound   int
	branchCreated    bool
	branchNoSession  bool
	branchNoActive   bool
	branchNoKey      bool
}

func newFakeGatewayServer(t *testing.T) *fakeGatewayServer {
	t.Helper()
	fake := &fakeGatewayServer{t: t, failMethods: map[string]struct{}{}, malformedAfter: map[string]struct{}{}}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)
	return fake
}

func (s *fakeGatewayServer) dialClient(t *testing.T) *nativehermes.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := nativehermes.Dial(ctx, "ws"+strings.TrimPrefix(s.server.URL, "http")+"/api/ws", nil)
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
	s.writeEvent(r.Context(), conn, nativehermes.Event{Type: "gateway.ready"})
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
		_, fail := s.failMethods[req.Method]
		s.mu.Unlock()
		if fail {
			s.writeError(r.Context(), conn, req.ID, -32000, req.Method+" failed")
			continue
		}
		s.respond(r.Context(), conn, req.ID, req.Method, params)
		s.mu.Lock()
		_, malformedAfter := s.malformedAfter[req.Method]
		s.mu.Unlock()
		if malformedAfter {
			_ = conn.Write(r.Context(), websocket.MessageText, []byte("{"))
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
		s.writeResult(ctx, conn, id, map[string]any{
			"session_id":        "live-1",
			"stored_session_id": "stored-1",
		})
	case "session.resume":
		stored, _ := params["session_id"].(string)
		s.writeResult(ctx, conn, id, map[string]any{
			"session_id":        "live-" + stored,
			"stored_session_id": stored,
		})
	case "session.active_list":
		s.mu.Lock()
		branchCreated := s.branchCreated
		branchNoActive := s.branchNoActive
		branchNoKey := s.branchNoKey
		s.mu.Unlock()
		sessions := []map[string]any{{
			"id":          "live-1",
			"session_key": "stored-1",
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
		for _, event := range s.promptEventScript(live) {
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
		s.writeResult(ctx, conn, id, map[string]any{"providers": []map[string]any{
			{"id": "openai", "name": "OpenAI", "models": []map[string]any{{
				"id":                "gpt-test",
				"name":              "GPT Test",
				"context_window":    128000,
				"max_output_tokens": 4096,
				"capabilities":      []string{"tools", "reasoning"},
			}}},
			{"id": "stringy", "models": "string-model"},
			{"id": "mapped", "models": map[string]any{"map-key": map[string]any{"name": "Map Model"}}},
		}})
	default:
		s.writeError(ctx, conn, id, -32601, "missing")
	}
}

func (s *fakeGatewayServer) promptEventScript(live string) []nativehermes.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.promptEvents != nil {
		events := append([]nativehermes.Event(nil), (*s.promptEvents)...)
		for index := range events {
			if events[index].SessionID == "" {
				events[index].SessionID = live
			}
		}
		return events
	}
	return []nativehermes.Event{
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
		{Type: "message.complete", SessionID: live, Payload: json.RawMessage(`{"usage":{"total_tokens":7,"input_tokens":3,"output_tokens":4,"reasoning_tokens":1}}`)},
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

func (s *fakeGatewayServer) writeEvent(ctx context.Context, conn *websocket.Conn, event nativehermes.Event) {
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

func (s *fakeGatewayServer) setFail(method string) {
	s.mu.Lock()
	s.failMethods[method] = struct{}{}
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setMalformedAfter(method string) {
	s.mu.Lock()
	s.malformedAfter[method] = struct{}{}
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

func (s *fakeGatewayServer) setPromptEvents(events ...nativehermes.Event) {
	s.mu.Lock()
	copied := append([]nativehermes.Event(nil), events...)
	s.promptEvents = &copied
	s.mu.Unlock()
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
		events:       make(chan hermesEvent, 32),
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
		events:       make(chan hermesEvent, 16),
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
	if got, err := server.GetSession(ctx, "stored-1"); err != nil || got.ID != "stored-1" {
		t.Fatalf("GetSession existing = %#v err=%v", got, err)
	}
	if got, err := server.GetSession(ctx, "restored"); err != nil || got.ID != "restored" {
		t.Fatalf("GetSession resume = %#v err=%v", got, err)
	}
	list, err := server.ListSessions(ctx, "/repo")
	if err != nil || len(list) != 1 || list[0].ID != "stored-1" {
		t.Fatalf("ListSessions = %#v err=%v", list, err)
	}
	if err := server.DeleteSession(ctx, "missing"); err != nil {
		t.Fatalf("DeleteSession missing: %v", err)
	}
	if err := server.DeleteSession(ctx, "stored-1"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if live := server.liveSessionID("stored-1"); live != "" {
		t.Fatalf("deleted session still mapped to %q", live)
	}
	if agents, err := server.Agents(ctx); err != nil || agents != nil {
		t.Fatalf("Agents = %#v err=%v", agents, err)
	}
	if perms, err := server.PendingPermissions(ctx); err != nil || perms != nil {
		t.Fatalf("PendingPermissions = %#v err=%v", perms, err)
	}
	if questions, err := server.PendingQuestions(ctx); err != nil || questions != nil {
		t.Fatalf("PendingQuestions = %#v err=%v", questions, err)
	}
	if todos, err := server.Todos(ctx, "stored-1"); err != nil || todos != nil {
		t.Fatalf("Todos = %#v err=%v", todos, err)
	}

	message, err := server.SendMessage(ctx, "stored-1", hermesMessageRequest{Parts: []map[string]any{{"text": "hello"}, {"text": "world"}}})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if got := message.Parts[0].Text; got != "hello world" {
		t.Fatalf("message text = %q", got)
	}
	if message.Info.Tokens.Total != 7 || message.Info.Tokens.Input != 3 || message.Info.Tokens.Output != 4 || message.Info.Tokens.Reasoning != 1 {
		t.Fatalf("tokens = %#v", message.Info.Tokens)
	}
	for _, want := range []string{"permission.v2.asked", "question.v2.asked", "message.part.updated"} {
		if !drainHermesEventType(server.events, want) {
			t.Fatalf("missing forwarded event %q", want)
		}
	}
	if err := server.ReplyPermission(ctx, permissionRequest{SessionID: "stored-1"}, "always", "ignored"); err != nil {
		t.Fatalf("ReplyPermission: %v", err)
	}
	if err := server.ReplyPermission(ctx, permissionRequest{SessionID: "stored-1"}, "reject", "ignored"); err != nil {
		t.Fatalf("ReplyPermission reject: %v", err)
	}
	if err := server.ReplyQuestion(ctx, questionRequest{SessionID: "stored-1"}, [][]string{{"yes"}}); err != nil {
		t.Fatalf("ReplyQuestion: %v", err)
	}
	if err := server.RejectQuestion(ctx, questionRequest{SessionID: "stored-1"}); err != nil {
		t.Fatalf("RejectQuestion: %v", err)
	}
	if err := server.Abort(ctx, "missing-live"); err != nil {
		t.Fatalf("Abort missing live: %v", err)
	}
	if err := server.Abort(ctx, "stored-1"); err != nil {
		t.Fatalf("Abort: %v", err)
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
	if _, err := server.Fork(ctx, "stored-1", ""); err == nil {
		t.Fatal("Fork succeeded after repeated live session not found")
	}
	providers, err := server.ConfigProviders(ctx)
	if err != nil || len(providers.Providers) != 3 || providers.Providers[0].Models["gpt-test"].Limit["context"] != 128000 {
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

func drainHermesEventType(ch <-chan hermesEvent, want string) bool {
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
	tokens := gatewayUsageTokens(json.RawMessage(`{"usage":{"total":1,"input":2,"output":3,"reasoning":4}}`))
	if tokens.Total != 1 || tokens.Input != 2 || tokens.Output != 3 || tokens.Reasoning != 4 {
		t.Fatalf("fallback usage tokens = %#v", tokens)
	}
	if err := assistantMessageError(nativeMessage{Info: nativeMessageInfo{Finish: "error"}}); err == nil {
		t.Fatal("assistant finish error accepted")
	}
	err := assistantMessageError(nativeMessage{Info: nativeMessageInfo{Error: &nativeError{Message: "provider failed"}}})
	if err == nil || !strings.Contains(err.Error(), "provider failed") {
		t.Fatalf("assistant message error = %v", err)
	}
	if err := assistantMessageError(nativeMessage{Info: nativeMessageInfo{Error: &nativeError{}}}); err == nil {
		t.Fatal("empty assistant error accepted")
	}
	if err := assistantMessageError(nativeMessage{Info: nativeMessageInfo{Finish: "stop"}}); err != nil {
		t.Fatalf("assistant stop rejected: %v", err)
	}
	messages := nativeMessagesFromGateway("stored", []nativehermes.Message{{Role: "assistant", Content: json.RawMessage(`{"text":"mapped"}`)}})
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
	if got := gatewayMessageText(nativehermes.Message{}); got != "" {
		t.Fatalf("empty gateway message text = %q", got)
	}
	if got := gatewayMessageText(nativehermes.Message{Content: json.RawMessage(`not-json`)}); got != "not-json" {
		t.Fatalf("fallback gateway message text = %q", got)
	}
	if got := errors.Unwrap(streamError{epoch: 7, err: errors.New("wrapped")}); got == nil || got.Error() != "wrapped" {
		t.Fatalf("stream error unwrap = %v", got)
	}
	var providers providersResponse
	if err := json.Unmarshal([]byte(`{"providers":[]}`), &providers); err != nil || len(providers.Raw) == 0 {
		t.Fatalf("providersResponse valid = %#v err=%v", providers, err)
	}
	mapped := providersFromGateway(nativehermes.ModelOptionsResult{Providers: []nativehermes.Provider{{
		ID:     "p",
		Models: []nativehermes.ProviderModel{{}, {Name: "named"}},
	}}})
	if _, ok := mapped.Providers[0].Models["named"]; !ok || len(mapped.Providers[0].Models) != 1 {
		t.Fatalf("providersFromGateway empty model handling = %#v", mapped)
	}
	if safePathName(" \t ") != "session" {
		t.Fatal("safePathName did not default empty input")
	}
	if err := materializeHermesMCPConfig(t.TempDir(), nil); err != nil {
		t.Fatalf("empty MCP config: %v", err)
	}
	originalMarshalIndent := hermesMarshalIndent
	hermesMarshalIndent = func(any, string, string) ([]byte, error) {
		return nil, errors.New("marshal failed")
	}
	if err := materializeHermesMCPConfig(t.TempDir(), []acp.McpServer{StdioMCPServer("s", "cmd", nil, nil)}); err == nil {
		t.Fatal("materializeHermesMCPConfig ignored marshal error")
	}
	if err := writeLease(t.TempDir(), serverLease{}); err == nil {
		t.Fatal("writeLease ignored marshal error")
	}
	hermesMarshalIndent = originalMarshalIndent
	homeFile := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(homeFile, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := materializeHermesMCPConfig(homeFile, []acp.McpServer{StdioMCPServer("s", "cmd", nil, nil)}); err == nil {
		t.Fatal("materializeHermesMCPConfig accepted file home")
	}
}

func TestHermesGatewayServerEdgeBranches(t *testing.T) {
	t.Run("accessors and fallback replies", func(t *testing.T) {
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
		if err := server.ReplyPermission(ctx, permissionRequest{SessionID: "unmapped"}, "once", "ignored"); err != nil {
			t.Fatalf("ReplyPermission fallback: %v", err)
		}
		if err := server.ReplyQuestion(ctx, questionRequest{SessionID: "unmapped"}, [][]string{{"a"}}); err != nil {
			t.Fatalf("ReplyQuestion fallback: %v", err)
		}
		if err := server.RejectQuestion(ctx, questionRequest{SessionID: "unmapped"}); err != nil {
			t.Fatalf("RejectQuestion fallback: %v", err)
		}
		for _, method := range []string{"approval.respond", "clarify.respond"} {
			calls := fake.callsFor(method)
			if len(calls) == 0 || calls[len(calls)-1].Params["session_id"] != "unmapped" {
				t.Fatalf("%s fallback calls = %#v", method, calls)
			}
		}
	})

	t.Run("nonblocking event drops", func(t *testing.T) {
		server := &hermesServer{events: make(chan hermesEvent, 1)}
		server.events <- hermesEvent{Type: "filled"}
		server.forwardGatewayPart("stored", "message", nativehermes.Event{Type: "message.delta"}, "text")
		server.forwardGatewayPermission("stored", "live", nativehermes.Event{Payload: json.RawMessage(`{}`)})
		server.forwardGatewayQuestion("stored", "live", nativehermes.Event{Payload: json.RawMessage(`{}`)})
		if got := len(server.events); got != 1 {
			t.Fatalf("event channel len = %d", got)
		}
	})

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
				_, err := server.SendMessage(ctx, "stored", hermesMessageRequest{Parts: []map[string]any{{"text": "hi"}}})
				return err
			},
		},
		{
			name:   "submit",
			method: "prompt.submit",
			call: func(ctx context.Context, server *hermesServer) error {
				server.rememberGatewaySession("stored", "live-stored")
				_, err := server.SendMessage(ctx, "stored", hermesMessageRequest{Parts: []map[string]any{{"text": "hi"}}})
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
		_, err := server.SendMessage(ctx, "stored", hermesMessageRequest{Parts: []map[string]any{{"text": "hi"}}})
		if err == nil || !strings.Contains(err.Error(), "event stream closed") {
			t.Fatalf("mid-turn close error = %v", err)
		}
	})

	t.Run("gateway error mid turn", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setPromptEvents()
		fake.setMalformedAfter("prompt.submit")
		server := newGatewayBackedHermesServer(t, fake, "")
		server.rememberGatewaySession("stored", "live-stored")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := server.SendMessage(ctx, "stored", hermesMessageRequest{Parts: []map[string]any{{"text": "hi"}}})
		if err == nil {
			t.Fatal("gateway malformed frame error was nil")
		}
	})

	t.Run("context deadline while waiting for events", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setPromptEvents()
		server := newGatewayBackedHermesServer(t, fake, "")
		server.rememberGatewaySession("stored", "live-stored")
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		_, err := server.SendMessage(ctx, "stored", hermesMessageRequest{Parts: []map[string]any{{"text": "hi"}}})
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

	client, err := startHermesServer(ctx, hermesStartOptions{
		ACPSessionID:   "session/one",
		Root:           root,
		Cwd:            cwd,
		ExecutablePath: helper,
		DefaultModel:   "openai/gpt-test",
		Env:            map[string]string{"BASE_ENV": "base"},
		HealthTimeout:  5 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
		MCPServers: []acp.McpServer{
			StdioMCPServer("stdio", "cmd", []string{"arg"}, map[string]string{"A": "1"}),
			HTTPMCPServer("http", "https://example.test/mcp", map[string]string{"Authorization": "token"}),
		},
	})
	if err != nil {
		t.Fatalf("startHermesServer: %v", err)
	}
	server := client.(*hermesServer)
	if server.xdg.Root == "" || !strings.Contains(filepath.Base(server.xdg.Root), "session_one") {
		t.Fatalf("xdg dirs = %#v", server.xdg)
	}
	leasePath := filepath.Join(server.xdg.State, leaseFileName)
	leaseData, err := os.ReadFile(leasePath)
	if err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if !strings.Contains(string(leaseData), `"tokenHash"`) || strings.Contains(string(leaseData), "passwordHash") {
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
	if _, err := startHermesServer(ctx, hermesStartOptions{ExecutablePath: filepath.Join(t.TempDir(), "missing-hermes")}); err == nil {
		t.Fatal("missing executable unexpectedly started")
	}
	if _, err := startHermesServer(ctx, hermesStartOptions{Root: "["}); err == nil {
		t.Fatal("invalid reap glob unexpectedly succeeded")
	}
	if _, err := startHermesServer(ctx, hermesStartOptions{Root: t.TempDir(), ACPSessionID: acpSessionIDString(string([]byte{0}))}); err == nil {
		t.Fatal("invalid session path unexpectedly succeeded")
	}
	if _, err := startHermesServer(ctx, hermesStartOptions{ExistingXDG: xdgDirs{Root: filepath.Join(t.TempDir(), "root")}}); err == nil {
		t.Fatal("incomplete existing xdg unexpectedly succeeded")
	}
	if _, err := startHermesServer(ctx, hermesStartOptions{
		ExistingXDG: testXDGDirs(t),
		MCPServers:  []acp.McpServer{{}},
	}); err == nil {
		t.Fatal("invalid mcp server unexpectedly started")
	}

	restoreHermesClientSeams(t)
	hermesWriteLease = func(string, serverLease) error {
		return errors.New("lease failed")
	}
	if _, err := startHermesServer(ctx, hermesStartOptions{
		ExecutablePath: fakeHermesGatewayExecutable(t, fakeGatewayModeOK),
		ExistingXDG:    testXDGDirs(t),
		HealthTimeout:  5 * time.Second,
	}); err == nil || !strings.Contains(err.Error(), "lease failed") {
		t.Fatalf("lease failure error = %v", err)
	}

	if _, err := startHermesServer(ctx, hermesStartOptions{
		ExecutablePath: fakeHermesGatewayExecutable(t, fakeGatewayModeStatusOnly),
		ExistingXDG:    testXDGDirs(t),
		HealthTimeout:  500 * time.Millisecond,
	}); err == nil {
		t.Fatal("gateway readiness failure unexpectedly succeeded")
	}
}

func TestXDGLeaseAndHelpers(t *testing.T) {
	root := t.TempDir()
	xdg, err := createXDGDirs(root, "")
	if err != nil {
		t.Fatalf("createXDGDirs: %v", err)
	}
	if filepath.Base(xdg.Root) != "session" {
		t.Fatalf("default xdg root = %#v", xdg)
	}
	if err := ensureXDGDirs(xdgDirs{Root: "", Data: "x", Config: "x", Cache: "x", State: "x"}); err == nil {
		t.Fatal("ensureXDGDirs accepted empty root")
	}
	if err := writeLease(xdg.State, serverLease{PID: 0, Port: 1, TokenHash: passwordHash("token")}); err != nil {
		t.Fatalf("writeLease: %v", err)
	}
	badRoot := string([]byte{0})
	if err := writeLease(badRoot, serverLease{}); err == nil {
		t.Fatal("writeLease accepted invalid path")
	}
	if err := os.MkdirAll(filepath.Join(root, "bad", "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bad", "state", leaseFileName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reapStaleLeases(root, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("reapStaleLeases: %v", err)
	}
	if err := reapStaleLeases("", nil); err != nil {
		t.Fatalf("empty reapStaleLeases: %v", err)
	}
	if got := safePathName("../a:b"); got != "__a_b" {
		t.Fatalf("safePathName = %q", got)
	}
	for _, value := range []any{float64(-1), int(-1), json.Number("bad")} {
		if got, ok := intFromNumber(value); ok || got != 0 {
			t.Fatalf("intFromNumber(%#v) = %d, %v", value, got, ok)
		}
	}
	if got, ok := intFromNumber(json.Number("12")); !ok || got != 12 {
		t.Fatalf("intFromNumber json number = %d, %v", got, ok)
	}
}

func TestLeaseReaperVerifiesProcessIdentity(t *testing.T) {
	root := t.TempDir()
	xdg, err := createXDGDirs(root, "lease")
	if err != nil {
		t.Fatal(err)
	}
	leasePath := filepath.Join(xdg.State, leaseFileName)
	baseIdentity := processIdentity{
		StartTime: "start",
		Cmdline:   []string{"/usr/bin/hermes", "serve"},
		Env: map[string]string{
			"HERMES_HOME":                    xdg.Root,
			"HERMES_DASHBOARD_SESSION_TOKEN": "token",
		},
	}
	baseLease := serverLease{
		PID:              999999,
		TokenHash:        passwordHash("token"),
		XDGRoot:          xdg.Root,
		ProcessStartTime: "start",
	}

	restoreHermesClientSeams(t)
	hermesInspectProcess = func(int) (processIdentity, error) {
		return baseIdentity, nil
	}
	if !leaseMatchesProcess(leasePath, baseLease) {
		t.Fatal("matching lease did not match")
	}
	if !cmdlineLooksLikeHermesServe([]string{"/tmp/hermes"}) || cmdlineLooksLikeHermesServe([]string{"node"}) {
		t.Fatal("cmdline Hermes detection mismatch")
	}
	if leaseMatchesProcess(leasePath, serverLease{PID: 0, ProcessStartTime: "start"}) {
		t.Fatal("zero pid lease matched")
	}
	if leaseMatchesProcess(leasePath, serverLease{PID: 1}) {
		t.Fatal("missing start time lease matched")
	}

	for _, tt := range []struct {
		name     string
		identity processIdentity
		lease    serverLease
		err      error
	}{
		{name: "inspect error", identity: baseIdentity, lease: baseLease, err: errors.New("inspect failed")},
		{name: "start mismatch", identity: processIdentity{StartTime: "other", Cmdline: baseIdentity.Cmdline, Env: baseIdentity.Env}, lease: baseLease},
		{name: "home mismatch", identity: processIdentity{StartTime: "start", Cmdline: baseIdentity.Cmdline, Env: map[string]string{"HERMES_HOME": t.TempDir(), "HERMES_DASHBOARD_SESSION_TOKEN": "token"}}, lease: baseLease},
		{name: "token mismatch", identity: processIdentity{StartTime: "start", Cmdline: baseIdentity.Cmdline, Env: map[string]string{"HERMES_HOME": xdg.Root, "HERMES_DASHBOARD_SESSION_TOKEN": "wrong"}}, lease: baseLease},
		{name: "root mismatch", identity: baseIdentity, lease: serverLease{PID: baseLease.PID, TokenHash: baseLease.TokenHash, XDGRoot: t.TempDir(), ProcessStartTime: baseLease.ProcessStartTime}},
		{name: "cmdline mismatch", identity: processIdentity{StartTime: "start", Cmdline: []string{"node"}, Env: baseIdentity.Env}, lease: baseLease},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hermesInspectProcess = func(int) (processIdentity, error) {
				return tt.identity, tt.err
			}
			if leaseMatchesProcess(leasePath, tt.lease) {
				t.Fatal("mismatched lease matched")
			}
		})
	}

	hermesInspectProcess = func(int) (processIdentity, error) {
		return baseIdentity, nil
	}
	if err := writeLease(xdg.State, baseLease); err != nil {
		t.Fatal(err)
	}
	reapLeaseFile(leasePath, slog.New(slog.DiscardHandler))
	if _, err := os.Stat(leasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease after reap = %v", err)
	}
	reapLeaseFile(t.TempDir(), nil)
}

func TestNativeUnmarshalErrors(t *testing.T) {
	var part nativePart
	if err := part.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("nativePart accepted malformed JSON")
	}
	var event hermesEvent
	if err := event.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("hermesEvent accepted malformed JSON")
	}
	var providers providersResponse
	if err := providers.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("providersResponse accepted malformed JSON")
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
			_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.18.0 (fake)")
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
			writeGatewayEvent(r.Context(), conn, nativehermes.Event{Type: "gateway.ready"})
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
		return map[string]any{"session_id": "live-" + stored, "stored_session_id": stored}
	case "session.active_list":
		return map[string]any{"sessions": []any{}}
	case "session.history":
		return map[string]any{"count": 0, "messages": []any{}}
	case "model.options":
		return map[string]any{"providers": []any{}}
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

func writeGatewayEvent(ctx context.Context, conn *websocket.Conn, event nativehermes.Event) {
	data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "event", "params": event})
	_ = conn.Write(ctx, websocket.MessageText, data)
}

func restoreHermesClientSeams(t *testing.T) {
	t.Helper()
	marshalIndent := hermesMarshalIndent
	writeLease := hermesWriteLease
	inspectProcess := hermesInspectProcess
	t.Cleanup(func() {
		hermesMarshalIndent = marshalIndent
		hermesWriteLease = writeLease
		hermesInspectProcess = inspectProcess
	})
}

func testXDGDirs(t *testing.T) xdgDirs {
	t.Helper()
	root := t.TempDir()
	return xdgDirs{
		Root:   root,
		Data:   filepath.Join(root, "data"),
		Config: filepath.Join(root, "config"),
		Cache:  filepath.Join(root, "cache"),
		State:  filepath.Join(root, "state"),
	}
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type errorReadCloser struct {
	err error
}

func (r errorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r errorReadCloser) Close() error {
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
