//nolint:gocyclo // Stateful gateway/config matrices intentionally enumerate protocol branches.
package hermes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
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
	failAfterCalls      map[string]int
	promptEvents        *[]Event
	promptBeforeResult  []Event
	resumeBeforeResult  []Event
	resumeAfterResult   []Event
	createBeforeResult  []Event
	createAfterResult   []Event
	promptEventDelay    time.Duration
	promptRawFrames     []string
	promptStatus        string
	promptResult        any
	promptResultEntered chan struct{}
	promptResultRelease chan struct{}
	rpcGateMethod       string
	rpcGateEntered      chan struct{}
	rpcGateRelease      chan struct{}
	ignoreWriteErrors   bool
	branchNotFound      int
	branchCreated       bool
	branchTitle         string
	branchWrongTitle    bool
	branchFailAfterSave bool
	branchNoSession     bool
	branchNoActive      bool
	branchNoKey         bool
	createNoLive        bool
	createNoStored      bool
	titlePending        bool
	titleMissing        bool
	titleDoesNotPersist bool
	durableCreated      bool
	durableTitle        string
	persistedCount      int
	persistedMissingID  bool
	deleteKeepsBranch   bool
	requireDurable      bool
	resumeNoLive        bool
	resumeNoKey         bool
	resumeStoredOnly    bool
	resumeKey           string
	resumeBuildDefault  string
	expectedPromptModel string
	liveModels          map[string]string
	lazyResumeBuilds    map[string]bool
	buildBarrierStalls  int
	buildBarrierFails   bool
	startingLives       map[string]bool
	reloadStatus        string
	activeNoID          bool
	activeNoKey         bool
	activeEmpty         bool
	activeNil           bool
	modelProvidersNil   bool
	imageAttachedFalse  bool
	notFoundMethods     map[string]int
	malformedResponses  map[string]string
}

func newFakeGatewayServer(t *testing.T) *fakeGatewayServer {
	t.Helper()
	fake := &fakeGatewayServer{
		t:                  t,
		failMethods:        map[string]struct{}{},
		failAfterCalls:     map[string]int{},
		notFoundMethods:    map[string]int{},
		malformedResponses: map[string]string{},
		liveModels:         map[string]string{},
		lazyResumeBuilds:   map[string]bool{},
		startingLives:      map[string]bool{},
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)

	return fake
}

func TestMCPServersWithSecretEnv(t *testing.T) {
	servers := []acp.McpServer{
		stdioMCPServer("stdio", "tool", nil, map[string]string{"TOKEN": "stdio-secret"}),
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
	if got := materialized[0].Stdio.Env[0].Value; got != "${ACP_GO_HERMES_MCP_ENV_1_1}" {
		t.Fatalf("stdio placeholder = %q", got)
	}
	if env["ACP_GO_HERMES_MCP_ENV_1_1"] != "stdio-secret" {
		t.Fatalf("stdio secret environment = %#v", env)
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
	if servers[0].Stdio.Env[0].Value != "stdio-secret" {
		t.Fatalf("input stdio server was mutated: %#v", servers[0])
	}

	_, _, err = mcpServersWithSecretEnv(servers, map[string]string{"ACP_GO_HERMES_MCP_HEADER_2_1": "occupied"})
	if err == nil {
		t.Fatal("reserved MCP environment collision was accepted")
	}
}

func TestStartServerRejectsReservedMCPSecretEnvironment(t *testing.T) {
	_, err := StartServer(t.Context(), darwinTestStartOptions(t, StartOptions{
		ACPSessionID:  "session-1",
		ScratchParent: durableTempDir(t),
		Cwd:           durableTempDir(t),
		Env:           map[string]string{"ACP_GO_HERMES_MCP_HEADER_1_1": "occupied"},
		MCPServers: []acp.McpServer{{Http: &acp.McpServerHttpInline{
			Name: "http", Url: "https://example.test", Headers: []acp.HttpHeader{{Name: "Authorization", Value: "secret"}},
		}}},
	}))
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
		if remaining, delayed := s.failAfterCalls[req.Method]; delayed {
			if remaining == 0 {
				fail = true
			} else {
				s.failAfterCalls[req.Method] = remaining - 1
			}
		}
		notFound := s.notFoundMethods[req.Method] > 0
		if notFound {
			s.notFoundMethods[req.Method]--
		}
		gateMethod := s.rpcGateMethod
		gateEntered := s.rpcGateEntered
		gateRelease := s.rpcGateRelease
		malformedResponse := s.malformedResponses[req.Method]
		s.mu.Unlock()
		if req.Method == gateMethod {
			close(gateEntered)
			<-gateRelease
		}
		if fail {
			s.writeError(r.Context(), conn, req.ID, -32000, req.Method+" failed")

			continue
		}
		if notFound {
			s.writeError(r.Context(), conn, req.ID, 4007, "session not found")

			continue
		}
		if malformedResponse != "" {
			s.writeMalformedResponse(r.Context(), conn, req.ID, malformedResponse)

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

func (s *fakeGatewayServer) writeMalformedResponse(
	ctx context.Context,
	conn *websocket.Conn,
	id int64,
	shape string,
) {
	var payload map[string]any
	switch shape {
	case "id-only":
		payload = map[string]any{"jsonrpc": "2.0", "id": id}
	case "wrong-version":
		payload = map[string]any{"jsonrpc": "1.0", "id": id, "result": map[string]any{}}
	default:
		s.t.Fatalf("unknown malformed response shape %q", shape)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		s.t.Fatalf("marshal malformed response: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil && !s.writeErrorsIgnored() {
		s.t.Errorf("write malformed response: %v", err)
	}
}

//nolint:gocyclo // The fake intentionally enumerates the complete native gateway method matrix.
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
		for _, event := range s.createEventScript("live-1", true) {
			s.writeEvent(ctx, conn, event)
		}
		s.writeResult(ctx, conn, id, result)
		for _, event := range s.createEventScript("live-1", false) {
			s.writeEvent(ctx, conn, event)
		}
	case "session.resume":
		stored, _ := params["session_id"].(string)
		s.mu.Lock()
		resumeNoLive := s.resumeNoLive
		resumeNoKey := s.resumeNoKey
		resumeStoredOnly := s.resumeStoredOnly
		resumeKey := s.resumeKey
		requireDurable := s.requireDurable
		durableCreated := s.durableCreated
		s.mu.Unlock()
		if requireDurable && !durableCreated {
			s.writeError(ctx, conn, id, 4007, "session not found")

			return
		}
		if resumeKey == "" {
			resumeKey = stored
		}
		liveID := "live-" + resumeKey
		s.mu.Lock()
		if s.resumeBuildDefault != "" {
			s.lazyResumeBuilds[liveID] = true
		}
		s.mu.Unlock()
		result := map[string]any{
			"session_id":  liveID,
			"session_key": resumeKey,
		}
		if resumeNoLive {
			delete(result, "session_id")
		}
		if resumeNoKey {
			delete(result, "session_key")
		}
		if resumeStoredOnly {
			delete(result, "session_key")
			result["stored_session_id"] = resumeKey
		}
		for _, event := range s.resumeEventScript(liveID, true) {
			s.writeEvent(ctx, conn, event)
		}
		s.writeResult(ctx, conn, id, result)
		for _, event := range s.resumeEventScript(liveID, false) {
			s.writeEvent(ctx, conn, event)
		}
	case "session.active_list":
		s.mu.Lock()
		branchCreated := s.branchCreated
		branchNoActive := s.branchNoActive
		branchNoKey := s.branchNoKey
		activeNoID := s.activeNoID
		activeNoKey := s.activeNoKey
		activeEmpty := s.activeEmpty
		activeNil := s.activeNil
		starting := make([]string, 0, len(s.startingLives))
		for live := range s.startingLives {
			starting = append(starting, live)
		}
		s.mu.Unlock()
		slices.Sort(starting)
		if activeNil {
			s.writeResult(ctx, conn, id, map[string]any{"sessions": nil})

			return
		}
		if activeEmpty {
			s.writeResult(ctx, conn, id, map[string]any{"sessions": []map[string]any{}})

			return
		}
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
			"status":      "idle",
		}}
		for _, live := range starting {
			sessions = append(sessions, map[string]any{
				"id":          live,
				"session_key": live,
				"title":       "starting",
				"cwd":         "/repo",
				"status":      ActiveSessionStarting,
			})
		}
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
				"status":      "idle",
			})
		}
		s.writeResult(ctx, conn, id, map[string]any{"sessions": sessions})
	case "session.list":
		s.mu.Lock()
		durableCreated := s.durableCreated
		durableTitle := s.durableTitle
		branchCreated := s.branchCreated
		branchTitle := firstNonEmpty(s.branchTitle, "branch")
		persistedCount := s.persistedCount
		persistedMissingID := s.persistedMissingID
		s.mu.Unlock()
		if persistedMissingID {
			s.writeResult(ctx, conn, id, map[string]any{"sessions": []map[string]any{{"title": "missing id"}}})

			return
		}
		if persistedCount > 0 {
			sessions := make([]map[string]any, persistedCount)
			for index := range sessions {
				sessions[index] = map[string]any{"id": fmt.Sprintf("stored-%d", index)}
			}
			s.writeResult(ctx, conn, id, map[string]any{"sessions": sessions})

			return
		}
		sessions := make([]map[string]any, 0, 2)
		if durableCreated {
			sessions = append(sessions, map[string]any{"id": "stored-1", "title": durableTitle})
		}
		if branchCreated {
			sessions = append(sessions, map[string]any{"id": "stored-branch", "title": branchTitle})
		}
		s.writeResult(ctx, conn, id, map[string]any{"sessions": sessions})
	case "session.delete":
		sessionID, _ := params["session_id"].(string)
		if sessionID == "missing" {
			s.writeError(ctx, conn, id, 4007, "not found")

			return
		}
		s.mu.Lock()
		deleteKeepsBranch := s.deleteKeepsBranch
		s.mu.Unlock()
		if sessionID == "stored-branch" && !deleteKeepsBranch {
			s.mu.Lock()
			s.branchCreated = false
			s.mu.Unlock()
		}
		s.writeResult(ctx, conn, id, map[string]any{})
	case "reload.mcp":
		s.mu.Lock()
		status := s.reloadStatus
		s.mu.Unlock()
		if status == "" {
			status = "reloaded"
		}
		s.writeResult(ctx, conn, id, map[string]any{"status": status})
	case "session.title":
		title, _ := params["title"].(string)
		s.mu.Lock()
		pending := s.titlePending
		missing := s.titleMissing
		doesNotPersist := s.titleDoesNotPersist
		if !pending && !doesNotPersist {
			s.durableCreated = true
			s.durableTitle = title
		}
		s.mu.Unlock()
		result := map[string]any{"pending": pending, "title": title}
		if missing {
			delete(result, "title")
		}
		s.writeResult(ctx, conn, id, result)
	case "image.attach_bytes":
		s.mu.Lock()
		attached := !s.imageAttachedFalse
		s.mu.Unlock()
		s.writeResult(ctx, conn, id, map[string]any{"attached": attached})
	case "prompt.submit":
		live, _ := params["session_id"].(string)
		s.mu.Lock()
		if s.lazyResumeBuilds[live] {
			s.liveModels[live] = s.resumeBuildDefault
			delete(s.lazyResumeBuilds, live)
		}
		expectedModel := s.expectedPromptModel
		actualModel := s.liveModels[live]
		status := s.promptStatus
		s.mu.Unlock()
		if expectedModel != "" && actualModel != expectedModel {
			s.writeError(ctx, conn, id, -32000, fmt.Sprintf("prompt used model %q, want %q", actualModel, expectedModel))

			return
		}
		for _, event := range s.promptBeforeResultScript(live) {
			s.writeEvent(ctx, conn, event)
		}
		if status == "" {
			status = "streaming"
		}
		s.mu.Lock()
		resultEntered := s.promptResultEntered
		resultRelease := s.promptResultRelease
		s.mu.Unlock()
		if resultEntered != nil {
			close(resultEntered)
		}
		if resultRelease != nil {
			<-resultRelease
		}
		s.mu.Lock()
		result := s.promptResult
		s.mu.Unlock()
		if result == nil {
			result = map[string]any{"status": status}
		}
		s.writeResult(ctx, conn, id, result)
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
	case "process.list":
		live, _ := params["session_id"].(string)
		s.mu.Lock()
		stall := s.buildBarrierStalls
		if stall > 0 {
			s.buildBarrierStalls--
			s.startingLives[live] = true
		}
		fails := s.buildBarrierFails
		if stall == 0 && !fails {
			delete(s.startingLives, live)
			if s.lazyResumeBuilds[live] {
				s.liveModels[live] = s.resumeBuildDefault
				delete(s.lazyResumeBuilds, live)
			}
		}
		s.mu.Unlock()
		if stall > 0 || fails {
			s.writeError(ctx, conn, id, 5032, "agent initialization timed out")

			return
		}
		s.writeResult(ctx, conn, id, map[string]any{"processes": []map[string]any{}})
	case "approval.respond", "clarify.respond", "terminal.read.respond", "sudo.respond", "secret.respond", "session.interrupt", "session.close":
		s.writeResult(ctx, conn, id, map[string]any{})
	case "config.set":
		value, _ := params["value"].(string)
		fields := strings.Fields(value)
		raw := strings.Trim(fields[0], "'")
		qualified := raw
		for index := 0; index+1 < len(fields); index++ {
			if fields[index] == "--provider" {
				qualified = strings.Trim(fields[index+1], "'") + "/" + raw

				break
			}
		}
		live, _ := params[fieldSessionID].(string)
		s.mu.Lock()
		if s.resumeBuildDefault != "" && !s.lazyResumeBuilds[live] {
			s.liveModels[live] = qualified
		}
		s.mu.Unlock()
		s.writeResult(ctx, conn, id, map[string]any{"key": params["key"], "value": raw, "scope": "session", "confirm_required": false})
	case "session.history":
		s.writeResult(ctx, conn, id, map[string]any{"count": 2, "messages": []map[string]any{
			{"role": "user", "text": "hi"},
			{"role": "assistant", "text": "history"},
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
		branchTitle, _ := params["name"].(string)
		if branchTitle == "" {
			branchTitle = "branch"
		}
		if s.branchWrongTitle {
			branchTitle = "other"
		}
		s.branchTitle = branchTitle
		branchFailAfterSave := s.branchFailAfterSave
		branchNoSession := s.branchNoSession
		branchNoKey := s.branchNoKey
		s.mu.Unlock()
		if branchFailAfterSave {
			s.writeError(ctx, conn, id, -32000, "branch failed after save")

			return
		}
		if branchNoSession {
			s.writeResult(ctx, conn, id, map[string]any{
				"title":  branchTitle,
				"parent": "stored-1",
			})

			return
		}
		result := map[string]any{
			"session_id":        "live-branch",
			"stored_session_id": "stored-branch",
			"title":             branchTitle,
			"parent":            "stored-1",
		}
		if branchNoKey {
			delete(result, "stored_session_id")
		}
		s.writeResult(ctx, conn, id, result)
	case "model.options":
		s.mu.Lock()
		providersNil := s.modelProvidersNil
		s.mu.Unlock()
		if providersNil {
			s.writeResult(ctx, conn, id, map[string]any{"providers": nil})

			return
		}
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
		{Type: "tool.start", SessionID: live, Payload: json.RawMessage(`{"tool_id":"native-tool-1","name":"terminal","context":"write","args_text":"terminal(command=write)"}`)},
		{Type: "approval.request", SessionID: live, Payload: json.RawMessage(`{"command":"write","description":"Edit file"}`)},
		{Type: "clarify.request", SessionID: live, Payload: json.RawMessage(`{"request_id":"clarify-1","question":"Continue?"}`)},
		{Type: "terminal.read.request", SessionID: live, Payload: json.RawMessage(`{}`)},
		{Type: "sudo.request", SessionID: live, Payload: json.RawMessage(`{}`)},
		{Type: "secret.request", SessionID: live, Payload: json.RawMessage(`{}`)},
		{Type: "thinking.delta", SessionID: live, Payload: json.RawMessage(`{"text":"thinking"}`)},
		{Type: "message.delta", SessionID: live, Payload: json.RawMessage(`{"text":""}`)},
		{Type: "message.delta", SessionID: live, Payload: json.RawMessage(`{"text":"hello ","rendered":"\u001b[0mhello "}`)},
		{Type: "message.delta", SessionID: live, Payload: json.RawMessage(`{"text":"world"}`)},
		{Type: "tool.complete", SessionID: live, Payload: json.RawMessage(`{"tool_id":"native-tool-1","name":"terminal","args":{"command":"write"},"result":"done"}`)},
		{Type: "message.complete", SessionID: live, Payload: json.RawMessage(`{"text":"hello world","usage":{"total_tokens":7,"input_tokens":3,"output_tokens":4,"reasoning_tokens":1,"context_max":200000}}`)},
	}
}

func (s *fakeGatewayServer) writeResult(ctx context.Context, conn *websocket.Conn, id int64, result any) {
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	if err != nil {
		s.t.Errorf("marshal result: %v", err)

		return
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil && !s.writeErrorsIgnored() {
		s.t.Errorf("write result: %v", err)
	}
}

func (s *fakeGatewayServer) writeError(ctx context.Context, conn *websocket.Conn, id int64, code int, message string) {
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
	if err != nil {
		s.t.Errorf("marshal error: %v", err)

		return
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil && !s.writeErrorsIgnored() {
		s.t.Errorf("write error: %v", err)
	}
}

func (s *fakeGatewayServer) writeEvent(ctx context.Context, conn *websocket.Conn, event Event) {
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "event", "params": event})
	if err != nil {
		s.t.Errorf("marshal event: %v", err)

		return
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil && !s.writeErrorsIgnored() {
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

func (s *fakeGatewayServer) setFailAfter(method string, successfulCalls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAfterCalls[method] = successfulCalls
}

func (s *fakeGatewayServer) setNotFound(method string, count int) {
	s.mu.Lock()
	s.notFoundMethods[method] = count
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setMalformedResponse(method string, shape string) {
	s.mu.Lock()
	s.malformedResponses[method] = shape
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

func (s *fakeGatewayServer) setResumeStoredOnly() {
	s.mu.Lock()
	s.resumeStoredOnly = true
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

func (s *fakeGatewayServer) setPromptStatus(status string) {
	s.mu.Lock()
	s.promptStatus = status
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setPromptResult(result any) {
	s.mu.Lock()
	s.promptResult = result
	s.mu.Unlock()
}

func (s *fakeGatewayServer) gatePromptResult() (<-chan struct{}, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entered := make(chan struct{})
	release := make(chan struct{})
	s.promptResultEntered = entered
	s.promptResultRelease = release
	s.ignoreWriteErrors = true

	return entered, func() { close(release) }
}

func (s *fakeGatewayServer) gateRPC(method string) (<-chan struct{}, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entered := make(chan struct{})
	release := make(chan struct{})
	s.rpcGateMethod = method
	s.rpcGateEntered = entered
	s.rpcGateRelease = release

	return entered, func() { close(release) }
}

func (s *fakeGatewayServer) writeErrorsIgnored() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ignoreWriteErrors
}

func (s *fakeGatewayServer) setPromptBeforeResult(events ...Event) {
	s.mu.Lock()
	s.promptBeforeResult = append([]Event(nil), events...)
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setResumeEvents(before []Event, after []Event) {
	s.mu.Lock()
	s.resumeBeforeResult = append([]Event(nil), before...)
	s.resumeAfterResult = append([]Event(nil), after...)
	s.mu.Unlock()
}

func (s *fakeGatewayServer) setCreateEvents(before []Event, after []Event) {
	s.mu.Lock()
	s.createBeforeResult = append([]Event(nil), before...)
	s.createAfterResult = append([]Event(nil), after...)
	s.mu.Unlock()
}

func (s *fakeGatewayServer) createEventScript(live string, before bool) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.createAfterResult
	if before {
		events = s.createBeforeResult
	}

	events = append([]Event(nil), events...)
	for index := range events {
		if events[index].SessionID == "" {
			events[index].SessionID = live
		}
	}

	return events
}

func (s *fakeGatewayServer) resumeEventScript(live string, before bool) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.resumeAfterResult
	if before {
		events = s.resumeBeforeResult
	}

	events = append([]Event(nil), events...)
	for index := range events {
		if events[index].SessionID == "" {
			events[index].SessionID = live
		}
	}

	return events
}

func (s *fakeGatewayServer) promptBeforeResultScript(live string) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := append([]Event(nil), s.promptBeforeResult...)
	for index := range events {
		if events[index].SessionID == "" {
			events[index].SessionID = live
		}
	}

	return events
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

	server := &hermesServer{
		xdg:            testXDGDirs(t),
		log:            slog.New(slog.DiscardHandler),
		deliveries:     make(chan TurnDelivery, 32),
		closed:         make(chan struct{}),
		actorsByStored: make(map[string]*gatewaySessionActor),
		dispatchers:    make(map[uint64]*gatewayTransportDispatcher),
		cwd:            "/repo",
		defaultModel:   defaultModel,
	}
	server.installGatewayDispatcher(fake.dialClient(t))

	return server
}

func withTestPromptDispatch(ctx context.Context) context.Context {
	return WithPromptDispatch(ctx, func(context.Context, PromptDispatchInfo) error { return nil })
}

func TestHermesGatewayServerMethods(t *testing.T) {
	fake := newFakeGatewayServer(t)
	client := fake.dialClient(t)
	server := &hermesServer{
		xdg:            testXDGDirs(t),
		log:            slog.New(slog.DiscardHandler),
		deliveries:     make(chan TurnDelivery, 32),
		closed:         make(chan struct{}),
		actorsByStored: make(map[string]*gatewaySessionActor),
		dispatchers:    make(map[uint64]*gatewayTransportDispatcher),
		cwd:            "/repo",
		defaultModel:   "openai/gpt-test",
	}
	server.installGatewayDispatcher(client)
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
	if err3 := server.ReloadMCP(ctx, "restored"); err3 != nil {
		t.Fatalf("ReloadMCP: %v", err3)
	}
	fake.mu.Lock()
	reloadCall := fake.calls[len(fake.calls)-1]
	fake.mu.Unlock()
	if reloadCall.Method != "reload.mcp" || reloadCall.Params["session_id"] != "live-restored" || reloadCall.Params["confirm"] != true {
		t.Fatalf("reload.mcp call = %#v", reloadCall)
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
	testGatewayServerMessageForkAndClose(ctx, t, server, fake)
}

func TestMalformedApprovalResponseCannotTerminalizePermission(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	t.Cleanup(func() { _ = server.Close(context.Background()) })
	bindTestGatewaySession(t, server, "stored", "live-stored")

	permission, _ := testGatewayControlRequests(t, server, "stored", "live-stored")
	fake.setMalformedResponse("approval.respond", "id-only")
	transport := server.gatewayTransport()

	err := server.ReplyPermission(t.Context(), permission, valOnce, "")
	if !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("id-only approval response = %v, want disconnected", err)
	}
	select {
	case <-permission.route.actor.done:
	case <-t.Context().Done():
		t.Fatal("malformed approval response did not fence its actor generation")
	}
	if cause := transport.dispatcher.terminalCause(); cause == nil ||
		!strings.Contains(cause.Error(), "exactly one of result or error") {
		t.Fatalf("malformed approval terminal cause = %v", cause)
	}

	// A malformed response did not resolve the control. The dead generation
	// refuses a retry locally and no second native approval response is sent.
	if retryErr := server.ReplyPermission(t.Context(), permission, valOnce, ""); retryErr == nil {
		t.Fatal("terminal permission actor false-succeeded a retry")
	}
	if calls := fake.callsFor("approval.respond"); len(calls) != 1 {
		t.Fatalf("approval.respond calls after malformed response = %d, want 1", len(calls))
	}
}

func TestHermesServerCloseRetriesContainmentAndJoinsConcurrentCallers(t *testing.T) {
	want := errors.New("containment attempt failed")
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	attempts := 0
	server := &hermesServer{
		deliveries: make(chan TurnDelivery, 1),
		closed:     make(chan struct{}),
		closeNative: func(context.Context) error {
			mu.Lock()
			attempts++
			attempt := attempts
			mu.Unlock()
			if attempt == 1 {
				close(entered)
				<-release

				return want
			}

			return nil
		},
	}

	results := make(chan error, 2)
	go func() { results <- server.Close(t.Context()) }()
	<-entered
	joined := make(chan struct{})
	joinCtx := context.WithValue(t.Context(), serverCloseJoinHookKey{}, func() { close(joined) })
	go func() { results <- server.Close(joinCtx) }()
	<-joined
	close(release)

	for range 2 {
		if err := <-results; !errors.Is(err, want) {
			t.Fatalf("concurrent close result = %v", err)
		}
	}
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("memoized close: %v", err)
	}

	mu.Lock()
	if attempts != 2 {
		t.Fatalf("containment attempts = %d, want 2", attempts)
	}
	mu.Unlock()
}

func TestHermesServerCloseJoinerCancellationDoesNotCancelOwner(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := &hermesServer{
		deliveries: make(chan TurnDelivery, 1),
		closed:     make(chan struct{}),
		closeNative: func(context.Context) error {
			close(entered)
			<-release

			return nil
		},
	}

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- server.Close(t.Context()) }()
	<-entered

	joined := make(chan struct{})
	joinCtx, cancel := context.WithCancel(context.WithValue(t.Context(), serverCloseJoinHookKey{}, func() {
		close(joined)
	}))
	joinerDone := make(chan error, 1)
	go func() { joinerDone <- server.Close(joinCtx) }()
	<-joined
	cancel()
	if err := <-joinerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("joining close cancellation = %v", err)
	}

	select {
	case err := <-ownerDone:
		t.Fatalf("joining caller cancellation stopped owner attempt: %v", err)
	default:
	}

	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner close = %v", err)
	}
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("memoized close = %v", err)
	}
}

func TestHermesGatewayReloadMCPFailures(t *testing.T) {
	t.Run("resume failure", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setFail("session.resume")
		server := newGatewayBackedHermesServer(t, fake, "")
		if err := server.ReloadMCP(t.Context(), "stored"); err == nil || !strings.Contains(err.Error(), "session.resume failed") {
			t.Fatalf("ReloadMCP resume error = %v", err)
		}
	})

	t.Run("native rpc failure", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setFail("reload.mcp")
		server := newGatewayBackedHermesServer(t, fake, "")
		if err := server.ReloadMCP(t.Context(), "stored"); err == nil || !strings.Contains(err.Error(), "reload.mcp failed") {
			t.Fatalf("ReloadMCP RPC error = %v", err)
		}
	})

	t.Run("unexpected status", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.mu.Lock()
		fake.reloadStatus = "confirm_required"
		fake.mu.Unlock()
		server := newGatewayBackedHermesServer(t, fake, "")
		if err := server.ReloadMCP(t.Context(), "stored"); err == nil || !strings.Contains(err.Error(), "confirm_required") {
			t.Fatalf("ReloadMCP status error = %v", err)
		}
	})
}

func testGatewayServerMessageForkAndClose(ctx context.Context, t *testing.T, server *hermesServer, fake *fakeGatewayServer) {
	t.Helper()

	assertGatewayTextMessage(ctx, t, server)
	assertGatewayImageMessage(ctx, t, server, fake)
	permission := assertGatewayPermissionCorrelation(t, server.deliveries)
	var question *QuestionRequest
	messageSeen := false
	for !messageSeen || question == nil {
		select {
		case delivery := <-server.deliveries:
			event := turnEventFromDelivery(t, delivery)
			switch event.Type {
			case evtClarifyRequest:
				question = event.Question
			case evtMessagePartUpdated:
				messageSeen = true
			}
		default:
			t.Fatal("missing forwarded question or message event")
		}
	}
	if question == nil {
		t.Fatal("clarify event omitted its actor-owned question identity")
	}
	if err9 := server.ReplyPermission(ctx, *permission, "always", "ignored"); !errors.Is(err9, ErrGatewayAmbiguousTurn) {
		t.Fatalf("late ReplyPermission: %v", err9)
	}
	if err10 := server.ReplyQuestion(ctx, *question, [][]string{{"yes"}}); !errors.Is(err10, ErrGatewayAmbiguousTurn) {
		t.Fatalf("late ReplyQuestion: %v", err10)
	}
	activePermission, activeQuestion := testGatewayControlRequests(t, server, "stored-1", server.liveSessionID("stored-1"))
	if err11 := server.ReplyPermission(ctx, activePermission, "always", "ignored"); err11 != nil {
		t.Fatalf("ReplyPermission: %v", err11)
	}
	if err12 := server.RejectQuestion(ctx, activeQuestion); err12 != nil {
		t.Fatalf("RejectQuestion: %v", err12)
	}
	if err13 := server.Abort(ctx, "missing-live"); err13 != nil {
		t.Fatalf("Abort missing live: %v", err13)
	}
	if err14 := server.Abort(ctx, "stored-1"); err14 != nil {
		t.Fatalf("Abort: %v", err14)
	}
	// session.history names each row's body `text`. Replay carries that text,
	// so a decoder reading any other key would replay every row as empty.
	history, err := server.Messages(ctx, "stored-1")
	if err != nil || len(history) != 2 {
		t.Fatalf("Messages = %#v err=%v", history, err)
	}
	if history[0].Info.Role != "user" || history[0].Parts[0].Text != "hi" {
		t.Fatalf("Messages user row = %#v", history[0])
	}
	if history[1].Info.Role != "assistant" || history[1].Parts[0].Text != "history" {
		t.Fatalf("Messages assistant row = %#v", history[1])
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
	if err != nil || len(providers.Providers) != 2 || providers.Providers[0].Models["openai/gpt-test"].ID != "openai/gpt-test" {
		t.Fatalf("ConfigProviders = %#v err=%v", providers, err)
	}
	if err := server.Close(ctx); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Close: %v", err)
	}

	methods := fake.callMethods()
	for _, want := range []string{
		"session.create", "session.title", "session.resume", "session.active_list", "session.delete", "prompt.submit", "image.attach_bytes",
		"approval.respond", "clarify.respond", "terminal.read.respond", "sudo.respond", "secret.respond",
		"session.interrupt", "session.history", "session.branch", "model.options",
	} {
		if !containsString(methods, want) {
			t.Fatalf("method %q not called; methods=%v", want, methods)
		}
	}
}

func TestHermesGatewayForkDetachesParentRuntimeBeforePublishingChild(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored-parent", "live-parent")

	child, err := server.Fork(t.Context(), "stored-parent", "")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if child.ID != "stored-branch" || server.liveSessionID(child.ID) != "" {
		t.Fatalf("published child/runtime map = %#v/%q", child, server.liveSessionID(child.ID))
	}

	fake.mu.Lock()
	calls := append([]gatewayRPCCall(nil), fake.calls...)
	fake.mu.Unlock()
	branchIndex, closeIndex := -1, -1
	for index, call := range calls {
		switch call.Method {
		case "session.branch":
			branchIndex = index
		case "session.close":
			if call.Params[fieldSessionID] == "live-branch" {
				closeIndex = index
			}
		}
	}
	if branchIndex < 0 || closeIndex <= branchIndex {
		t.Fatalf("branch runtime was not detached in order: %#v", calls)
	}
}

func TestHermesGatewayForkCloseFailureDeletesDurableChild(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setFail("session.close")
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored-parent", "live-parent")

	if _, err := server.Fork(t.Context(), "stored-parent", ""); err == nil || !strings.Contains(err.Error(), "close Hermes branch runtime") {
		t.Fatalf("Fork close failure = %v", err)
	}
	fake.mu.Lock()
	calls := append([]gatewayRPCCall(nil), fake.calls...)
	fake.mu.Unlock()
	for _, call := range calls {
		if call.Method == "session.delete" && call.Params[fieldSessionID] == "stored-branch" {
			return
		}
	}
	t.Fatalf("durable branch was not deleted after detach failure: %#v", calls)
}

func TestHermesGatewayCreatePublishesOnlyDurableSession(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.mu.Lock()
	fake.requireDurable = true
	fake.activeEmpty = true
	fake.mu.Unlock()

	creator := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
	created, err := creator.CreateSession(t.Context(), "")
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if created.ID != "stored-1" || created.Title != "Hermes session" {
		t.Fatalf("created session = %#v", created)
	}

	// A second server has no runtime-only live-id map. It can recover only if
	// session.create forced Hermes's native DB row before returning.
	loader := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
	loaded, err := loader.GetSession(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("fresh-process GetSession: %v", err)
	}
	if loaded.ID != created.ID {
		t.Fatalf("loaded session = %#v", loaded)
	}

	methods := fake.callMethods()
	for _, want := range []string{"session.create", "session.title", "session.resume"} {
		if !containsString(methods, want) {
			t.Fatalf("durability method %q missing from %v", want, methods)
		}
	}
}

func TestHermesGatewayCreateBindsDraftBeforeDurableTitle(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	creator := DraftSessionCreator(server)
	callbackCalled := false
	created, err := creator.CreateSessionWithDraft(t.Context(), "unique-title", func(draft SessionDraft) error {
		callbackCalled = true
		if draft.LiveSessionID != "live-1" || draft.StoredSessionID != "stored-1" {
			t.Fatalf("draft = %#v", draft)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		for _, call := range fake.calls {
			if call.Method == "session.title" {
				t.Fatal("session.title ran before draft recovery callback")
			}
		}

		return nil
	})
	if err != nil || !callbackCalled || created.ID != "stored-1" {
		t.Fatalf("CreateSessionWithDraft = %#v, %v callback=%t", created, err, callbackCalled)
	}
}

func TestHermesGatewayPersistedInventoryFailsClosedAtLimit(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.persistedCount = 10000
	server := newGatewayBackedHermesServer(t, fake, "")
	if _, err := server.PersistedSessions(t.Context()); err == nil || !strings.Contains(err.Error(), "not exhaustive") {
		t.Fatalf("PersistedSessions saturation error = %v", err)
	}
}

func TestHermesGatewayFailedBranchCleansUnknownLiveDurableChild(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.branchFailAfterSave = true
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored-parent", "live-parent")

	_, err := server.ForkWithBaseline(t.Context(), "stored-parent", "unique-marker", nil)
	if err == nil || !strings.Contains(err.Error(), "branch failed after save") {
		t.Fatalf("ForkWithBaseline error = %v", err)
	}
	fake.mu.Lock()
	calls := append([]gatewayRPCCall(nil), fake.calls...)
	fake.mu.Unlock()
	var closed, deleted bool
	for _, call := range calls {
		closed = closed || call.Method == "session.close" && call.Params[fieldSessionID] == "live-branch"
		deleted = deleted || call.Method == "session.delete" && call.Params[fieldSessionID] == "stored-branch"
	}
	if closed || !deleted {
		t.Fatalf("failed branch cleanup calls = %#v", calls)
	}
}

func TestHermesGatewayCreateRejectsUnprovenDurability(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*fakeGatewayServer)
		want      string
	}{
		{
			name: "title rpc error",
			configure: func(fake *fakeGatewayServer) {
				fake.setFail("session.title")
			},
			want: "persist Hermes session",
		},
		{
			name: "title pending",
			configure: func(fake *fakeGatewayServer) {
				fake.titlePending = true
			},
			want: "remained pending",
		},
		{
			name: "title schema drift",
			configure: func(fake *fakeGatewayServer) {
				fake.titleMissing = true
			},
			want: "missing durable title",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			tc.configure(fake)
			server := newGatewayBackedHermesServer(t, fake, "")
			if _, err := server.CreateSession(t.Context(), ""); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CreateSession error = %v, want %q", err, tc.want)
			}
			if live := server.liveSessionID("stored-1"); live != "" {
				t.Fatalf("failed create retained live mapping %q", live)
			}
		})
	}
}

func TestGetSessionReadsStoredSessionIDSpellingOnResume(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.mu.Lock()
	fake.activeEmpty = true
	fake.durableCreated = true
	fake.mu.Unlock()
	fake.setResumeStoredOnly()
	server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")

	session, err := server.GetSession(t.Context(), "stored")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	if session.ID != "stored" {
		t.Fatalf("session ID = %q, want %q", session.ID, "stored")
	}
}

func TestHermesGatewayRebindsStaleLiveSessionAtReloadAndPromptAdmission(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.mu.Lock()
	fake.activeEmpty = true
	fake.durableCreated = true
	fake.mu.Unlock()
	server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")

	if _, err := server.GetSession(t.Context(), "stored"); err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	fake.setNotFound("reload.mcp", 1)
	if err := server.ReloadMCP(t.Context(), "stored"); err != nil {
		t.Fatalf("ReloadMCP stale live rebind: %v", err)
	}

	fake.setNotFound("prompt.submit", 1)
	message, err := server.SendMessage(withTestPromptDispatch(t.Context()), "stored", MessageRequest{Parts: []map[string]any{{"text": "after rebind"}}})
	if err != nil {
		t.Fatalf("SendMessage stale live rebind: %v", err)
	}
	if message.Info.SessionID != "stored" {
		t.Fatalf("rebound message = %#v", message)
	}

	methods := fake.callMethods()
	wantSubsequence := []string{
		"session.resume",
		"reload.mcp", "session.resume", "reload.mcp",
		"prompt.submit", "session.resume", "prompt.submit",
	}
	position := 0
	for _, method := range methods {
		if position < len(wantSubsequence) && method == wantSubsequence[position] {
			position++
		}
	}
	if position != len(wantSubsequence) {
		t.Fatalf("rebind calls = %v, missing subsequence %v at %d", methods, wantSubsequence, position)
	}
}

func assertGatewayTextMessage(ctx context.Context, t *testing.T, server *hermesServer) {
	t.Helper()

	message, err := server.SendMessage(withTestPromptDispatch(ctx), "stored-1", MessageRequest{Parts: []map[string]any{{"text": "hello"}, {"text": "world"}}})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if len(message.Parts) != 3 {
		t.Fatalf("message parts = %#v, want tool start, tool completion, and text", message.Parts)
	}
	if got := [2]string{message.Parts[2].Text, message.Parts[2].StreamedText}; got != [2]string{"hello world", "hello world"} {
		t.Fatalf("message and streamed text = %q", got)
	}
	assertGatewayNativeToolParts(t, message.Parts[:2])
	if message.Info.Tokens.Total != 7 || message.Info.Tokens.Input != 3 || message.Info.Tokens.Output != 4 || message.Info.Tokens.Reasoning != 1 {
		t.Fatalf("tokens = %#v", message.Info.Tokens)
	}
	if message.Info.ContextWindow != 200000 {
		t.Fatalf("context window = %d, want 200000", message.Info.ContextWindow)
	}
}

func assertGatewayPermissionCorrelation(t *testing.T, deliveries <-chan TurnDelivery) *PermissionRequest {
	t.Helper()

	var permission *PermissionRequest
	toolStartSeen := false
	for {
		select {
		case delivery := <-deliveries:
			if delivery.Err != nil || delivery.Event == nil {
				t.Fatalf("unexpected ordered delivery: %#v", delivery)
			}
			event := *delivery.Event
			if event.Type == evtMessagePartUpdated {
				var part Part
				if err := json.Unmarshal(event.Properties, &part); err != nil {
					t.Fatalf("decode tool part: %v", err)
				}
				if part.Type == valTool && part.CallID == "native-tool-1" {
					toolStartSeen = true
				}

				continue
			}
			if event.Type != evtApprovalRequest {
				t.Fatalf("forwarded event before approval = %q", event.Type)
			}
			permission = event.Permission
			if permission == nil {
				t.Fatal("approval event omitted its actor-owned permission identity")
			}
		default:
			t.Fatal("missing forwarded approval.request")
		}

		break
	}
	if !toolStartSeen {
		t.Fatal("approval was not preceded by the native tool start")
	}
	if permission.ID == "" || permission.Tool.CallID != "native-tool-1" ||
		!strings.HasPrefix(permission.Tool.MessageID, "hermes/stored-1/cycle-") {
		t.Fatalf("permission correlation = %#v", permission)
	}

	return permission
}

func assertGatewayNativeToolParts(t *testing.T, parts []Part) {
	t.Helper()

	if len(parts) != 2 {
		t.Fatalf("tool parts = %#v", parts)
	}
	if parts[0].Type != valTool || parts[0].CallID != "native-tool-1" || parts[0].Tool != "terminal" ||
		parts[1].Type != valTool || parts[1].CallID != parts[0].CallID || parts[1].Tool != parts[0].Tool {
		t.Fatalf("tool identity was not stable: %#v", parts)
	}
	if !json.Valid(parts[0].Raw) || !json.Valid(parts[1].Raw) || string(parts[0].Raw) == string(parts[1].Raw) {
		t.Fatalf("tool part raw values = %q, %q", parts[0].Raw, parts[1].Raw)
	}

	var startState, completeState map[string]any
	if err := json.Unmarshal(parts[0].State, &startState); err != nil {
		t.Fatalf("decode start state: %v", err)
	}
	if err := json.Unmarshal(parts[1].State, &completeState); err != nil {
		t.Fatalf("decode complete state: %v", err)
	}
	if startState["status"] != "running" || completeState["status"] != valCompleted {
		t.Fatalf("tool statuses = %#v, %#v", startState, completeState)
	}
	if input, _ := startState["rawInput"].(map[string]any); input["context"] != "write" {
		t.Fatalf("tool raw input = %#v", startState["rawInput"])
	}
	if input, _ := completeState["rawInput"].(map[string]any); input["command"] != "write" {
		t.Fatalf("completed tool raw input = %#v", completeState["rawInput"])
	}
	if completeState["rawOutput"] != "done" {
		t.Fatalf("tool raw output = %#v", completeState["rawOutput"])
	}
}

func assertGatewayImageMessage(ctx context.Context, t *testing.T, server *hermesServer, fake *fakeGatewayServer) {
	t.Helper()

	before := len(fake.callMethods())
	// Exactly the part shape the prompt mapper builds: a type, the validated
	// media type, and the decoded bytes. Nothing on either input form carries a
	// filename.
	imageMessage, err := server.SendMessage(withTestPromptDispatch(ctx), "stored-1", MessageRequest{Parts: []map[string]any{
		{"type": "text", "text": "first"},
		{"type": "file", "mime": "image/png", "data": []byte{0, 1}},
		{"type": "text", "text": "second"},
		{"type": "file", "mime": "image/webp", "data": []byte{2, 3}},
	}})
	if err != nil || imageMessage.Info.SessionID != "stored-1" {
		t.Fatalf("image SendMessage = %#v err=%v", imageMessage, err)
	}

	fake.mu.Lock()
	calls := append([]gatewayRPCCall(nil), fake.calls[before:]...)
	fake.mu.Unlock()
	if len(calls) < 3 || calls[0].Method != "image.attach_bytes" ||
		calls[1].Method != "image.attach_bytes" || calls[2].Method != "prompt.submit" {
		t.Fatalf("attach-then-submit calls = %#v", calls)
	}
	if calls[0].Params["content_base64"] != "AAE=" || calls[1].Params["content_base64"] != "AgM=" {
		t.Fatalf("attachment params = %#v", calls[:2])
	}
	for _, call := range calls[:2] {
		// An empty hint is not an absent one: Hermes sniffs the extension from
		// the bytes, so the upload must not claim a name the adapter never had.
		if _, present := call.Params["filename"]; present {
			t.Fatalf("attachment declared a filename hint: %#v", call.Params)
		}
	}
	if calls[2].Params["text"] != "first\n\nsecond" {
		t.Fatalf("flattened prompt text = %#v", calls[2].Params)
	}

	if _, imageErr := imageAttachmentsFromHermesParts([]map[string]any{{"type": "file"}}); imageErr == nil {
		t.Fatal("image parts accepted missing decoded data")
	}
	if _, imageErr := server.SendMessage(withTestPromptDispatch(ctx), "stored-1", MessageRequest{Parts: []map[string]any{{
		"type": "file",
	}}}); imageErr == nil {
		t.Fatal("SendMessage accepted missing decoded image data")
	}

	failing := newFakeGatewayServer(t)
	failing.setFail("image.attach_bytes")
	failingServer := newGatewayBackedHermesServer(t, failing, "")
	bindTestGatewaySession(t, failingServer, "stored-1", "live-1")
	if _, imageErr := failingServer.SendMessage(ctx, "stored-1", MessageRequest{Parts: []map[string]any{{
		"type": "file", "data": []byte{0},
	}}}); imageErr == nil {
		t.Fatal("SendMessage ignored image.attach_bytes failure")
	}
}

func TestHermesGatewayTextHelpersAndErrors(t *testing.T) {
	if got := textFromHermesParts([]map[string]any{{"text": "one"}, {"other": "skip"}, {"text": "two"}}); got != "one\n\ntwo" {
		t.Fatalf("textFromHermesParts = %q", got)
	}
	// A delta names its chunk `text` and nothing else. The ANSI-rendered copy
	// message.delta may carry alongside it is for terminal display.
	if got := gatewayPayloadString(json.RawMessage(`{"text":"chunk","rendered":"ansi"}`), valText); got != "chunk" {
		t.Fatalf("delta chunk = %q", got)
	}
	if got := gatewayPayloadString(json.RawMessage(`{"rendered":"ansi"}`), valText); got != "" {
		t.Fatalf("rendered-only delta chunk = %q", got)
	}
	// The ANSI-rendered copy is for terminal display, never the message text.
	if got := gatewayCompleteText(json.RawMessage(`{"text":"raw","rendered":"ansi"}`)); got != "raw" {
		t.Fatalf("complete raw text = %q", got)
	}
	if got := gatewayCompleteText(json.RawMessage(`{"rendered":"ansi","reasoning":"not final","status":"complete"}`)); got != "" {
		t.Fatalf("complete unrelated text = %q", got)
	}
	if got := gatewayCompleteText(json.RawMessage(`{`)); got != "" {
		t.Fatalf("malformed complete text = %q", got)
	}
	tokens := gatewayUsageTokens(json.RawMessage(`{"usage":{"total":1,"input":2,"output":3,"reasoning":4}}`))
	if tokens.Total != 1 || tokens.Input != 2 || tokens.Output != 3 || tokens.Reasoning != 4 {
		t.Fatalf("fallback usage tokens = %#v", tokens)
	}
	testGatewayContextAndToolHelpers(t)

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
	// session.history renders every visible row as {"role","text"}; a tool row
	// carries no text, and replays as a row with nothing to say.
	messages := nativeMessagesFromGateway("stored", []Message{
		{Role: "assistant", Text: "mapped"},
		{Role: "tool"},
	})
	if messages[0].Info.SessionID != "stored" || messages[0].Parts[0].Text != "mapped" {
		t.Fatalf("nativeMessagesFromGateway = %#v", messages)
	}
	if messages[1].Parts[0].Text != "" {
		t.Fatalf("tool row text = %q", messages[1].Parts[0].Text)
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
	testGatewayProvidersAndConfigHelpers(t)
}

func testGatewayContextAndToolHelpers(t *testing.T) {
	t.Helper()

	if failure := gatewayCompleteFailure(json.RawMessage(`{"finish":"error"}`)); failure == nil || failure.message != "hermes assistant error" {
		t.Fatalf("finish-only complete failure = %#v", failure)
	}

	if got := gatewayContextWindow(json.RawMessage(`{"usage":{"context_max":200000}}`)); got != 200000 {
		t.Fatalf("context window = %d, want 200000", got)
	}
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"usage":{"context_max":0}}`),
		json.RawMessage(`{"usage":{"context_max":-1}}`),
		json.RawMessage(`{"usage":{"context_max":1.5}}`),
		json.RawMessage(`{"usage":{"context_max":"200000"}}`),
		json.RawMessage(`{`),
	} {
		if got := gatewayContextWindow(raw); got != 0 {
			t.Fatalf("invalid context window %s = %d", raw, got)
		}
	}
	if got := gatewayToolCallID(json.RawMessage(`{"tool_id":"native-tool"}`)); got != "native-tool" {
		t.Fatalf("tool call id = %q", got)
	}
	if got := uniqueGatewayToolCallID(map[string]struct{}{"one": {}}); got != "one" {
		t.Fatalf("unique tool call = %q", got)
	}
	if got := uniqueGatewayToolCallID(map[string]struct{}{"one": {}, "two": {}}); got != "" {
		t.Fatalf("ambiguous tool call = %q", got)
	}
	testGatewayToolPartMapping(t)
}

func testGatewayToolPartMapping(t *testing.T) {
	t.Helper()

	for _, event := range []Event{
		{Type: evtToolStart, Payload: json.RawMessage(`{`)},
		{Type: evtToolStart, Payload: json.RawMessage(`{"name":"missing-id"}`)},
	} {
		if part, ok := gatewayToolPart("stored", "message", event, gatewayActiveTool{}); ok || !reflect.DeepEqual(part, Part{}) {
			t.Fatalf("invalid gateway tool event mapped to %#v", part)
		}
	}

	startPayload := json.RawMessage(`{"tool_id":"actual","name":"terminal","context":"preview only","args_text":"terminal(command=pwd)"}`)
	start, ok := gatewayToolPart("stored", "message", Event{Type: evtToolStart, Payload: startPayload}, gatewayActiveTool{})
	if !ok {
		t.Fatal("actual Hermes tool.start was not mapped")
	}
	if start.Tool != "terminal" {
		t.Fatalf("actual Hermes tool.start name = %q", start.Tool)
	}

	active := gatewayActiveTool{rawInput: startPayload, name: "terminal"}
	complete, ok := gatewayToolPart("stored", "message", Event{
		Type:    evtToolComplete,
		Payload: json.RawMessage(`{"tool_id":"actual","args":{"command":"pwd"},"result":{"stdout":"/repo","exit_code":0}}`),
	}, active)
	if !ok {
		t.Fatal("actual Hermes tool.complete was not mapped")
	}
	var state map[string]any
	if err := json.Unmarshal(complete.State, &state); err != nil {
		t.Fatal(err)
	}
	if complete.Tool != "terminal" {
		t.Fatalf("completion missing name did not preserve start name: %q", complete.Tool)
	}
	if input, _ := state["rawInput"].(map[string]any); input["command"] != "pwd" || input["context"] != nil {
		t.Fatalf("completion did not prefer authoritative args: %#v", state["rawInput"])
	}
	if output, _ := state["rawOutput"].(map[string]any); output["stdout"] != "/repo" {
		t.Fatalf("actual Hermes raw output = %#v", state["rawOutput"])
	}

	conflict, ok := gatewayToolPart("stored", "message", Event{
		Type:    evtToolComplete,
		Payload: json.RawMessage(`{"tool_id":"actual","name":"read_file","args":{"command":"pwd"},"result":"done"}`),
	}, active)
	if !ok || conflict.Tool != "terminal" {
		t.Fatalf("conflicting completion name changed lifecycle identity: %#v", conflict)
	}

	withoutArgs, ok := gatewayToolPart("stored", "message", Event{
		Type:    evtToolComplete,
		Payload: json.RawMessage(`{"tool_id":"actual","result":"done"}`),
	}, active)
	if !ok {
		t.Fatal("completion without args was not mapped")
	}
	if err := json.Unmarshal(withoutArgs.State, &state); err != nil {
		t.Fatal(err)
	}
	if input, _ := state["rawInput"].(map[string]any); input["context"] != "preview only" {
		t.Fatalf("completion without args did not retain start input: %#v", state["rawInput"])
	}
	testGatewayToolFailureMapping(t)

	noOutput, ok := gatewayToolPart("stored", "message", Event{
		Type:    evtToolComplete,
		Payload: json.RawMessage(`{"tool_id":"no-output","args":{"command":"true"}}`),
	}, gatewayActiveTool{})
	if !ok {
		t.Fatal("completion without output was not mapped")
	}
	state = map[string]any{}
	if err := json.Unmarshal(noOutput.State, &state); err != nil {
		t.Fatal(err)
	}
	if _, exists := state["rawOutput"]; exists {
		t.Fatalf("completion envelope leaked as raw output: %#v", state["rawOutput"])
	}

	explicitOutput, ok := gatewayToolPart("stored", "message", Event{
		Type:    evtToolComplete,
		Payload: json.RawMessage(`{"tool_id":"output","output":{"text":"explicit"}}`),
	}, gatewayActiveTool{})
	if !ok || !strings.Contains(string(explicitOutput.State), `"text":"explicit"`) {
		t.Fatalf("explicit output was not preserved: %#v", explicitOutput)
	}

	unnamed, ok := gatewayToolPart("stored", "message", Event{
		Type:    evtToolStart,
		Payload: json.RawMessage(`{"tool_id":"unnamed"}`),
	}, gatewayActiveTool{})
	if !ok || unnamed.Tool != "tool" {
		t.Fatalf("unnamed start fallback = %#v", unnamed)
	}
	unnamedCompletion, ok := gatewayToolPart("stored", "message", Event{
		Type:    evtToolComplete,
		Payload: json.RawMessage(`{"tool_id":"unnamed","name":"terminal","result":"done"}`),
	}, gatewayActiveTool{name: unnamed.Tool})
	if !ok || unnamedCompletion.Tool != "tool" {
		t.Fatalf("completion changed unnamed start identity: %#v", unnamedCompletion)
	}
}

func testGatewayToolFailureMapping(t *testing.T) {
	t.Helper()

	for _, test := range []struct {
		name    string
		payload string
		active  gatewayActiveTool
	}{
		{name: "canonical executor error", payload: `{"tool_id":"failed","name":"terminal","result":"Error executing tool 'terminal': boom"}`},
		{name: "structured success false", payload: `{"tool_id":"failed","result":{"success":false}}`},
		{name: "structured ok false", payload: `{"tool_id":"failed","result":{"ok":false}}`},
		{name: "integer exit code", payload: `{"tool_id":"failed","result":{"exit_code":1}}`},
		{name: "integer return code", payload: `{"tool_id":"failed","result":{"returncode":2}}`},
		{name: "polished error", payload: `{"tool_id":"failed","name":"terminal","result":{"error":{"message":"structured"}}}`},
		{
			name:    "preserved polished name",
			payload: `{"tool_id":"failed","name":"plugin_tool","result":{"error":"structured"}}`,
			active:  gatewayActiveTool{name: "terminal"},
		},
		{name: "JSON string", payload: `{"tool_id":"failed","result":"{\"success\":false}"}`},
		{
			name:    "JSON string with appended hint",
			payload: `{"tool_id":"failed","result":"  {\"ok\":false}\n\n[Hint: Results truncated]"}`,
		},
		{name: "output fallback", payload: `{"tool_id":"failed","output":{"success":false}}`},
	} {
		part, mapped := gatewayToolPart(
			"stored",
			"message",
			Event{Type: evtToolComplete, Payload: json.RawMessage(test.payload)},
			test.active,
		)
		if !mapped || !strings.Contains(string(part.State), `"status":"failed"`) {
			t.Fatalf("%s payload %s mapped to %#v", test.name, test.payload, part)
		}
	}

	for _, test := range []struct {
		name    string
		payload string
	}{
		{
			name:    "successful result",
			payload: `{"tool_id":"success","name":"terminal","result":{"success":true,"ok":true,"error":null,"exit_code":0,"returncode":1}}`,
		},
		{name: "generic plain error", payload: `{"tool_id":"success","name":"plugin_tool","result":"plugin error: optional diagnostic"}`},
		{name: "generic structured error", payload: `{"tool_id":"success","name":"plugin_tool","result":{"error":"optional diagnostic"}}`},
		{
			name:    "polished error with content",
			payload: `{"tool_id":"success","name":"terminal","result":{"error":"command diagnostic","content":"useful output"}}`,
		},
		{name: "noncanonical error string", payload: `{"tool_id":"success","name":"terminal","result":"error executing tool 'terminal': boom"}`},
		{name: "float exit code", payload: `{"tool_id":"success","result":{"exit_code":1.0}}`},
		{name: "result preferred to output", payload: `{"tool_id":"success","result":{"success":true},"output":{"success":false}}`},
		{name: "is error is not classifier contract", payload: `{"tool_id":"success","result":{"is_error":true}}`},
		{name: "nonobject JSON string", payload: `{"tool_id":"success","result":"[1,2] trailing hint"}`},
	} {
		part, ok := gatewayToolPart("stored", "message", Event{
			Type:    evtToolComplete,
			Payload: json.RawMessage(test.payload),
		}, gatewayActiveTool{})
		if !ok || strings.Contains(string(part.State), `"status":"failed"`) {
			t.Fatalf("%s classified as failed: %#v", test.name, part)
		}
	}

	testGatewayToolFailureHelpers(t)
}

func testGatewayToolFailureHelpers(t *testing.T) {
	t.Helper()

	value, ok := gatewayJSONValue([]byte(`{"value":1} trailing hint`))
	object, isObject := value.(map[string]any)
	if !ok || !isObject || object["value"] != json.Number("1") {
		t.Fatalf("prefixed JSON value = %#v, %v", value, ok)
	}
	if value, ok := gatewayJSONValue([]byte(`not JSON`)); ok || value != nil {
		t.Fatalf("invalid JSON value = %#v, %v", value, ok)
	}

	for _, test := range []struct {
		name  string
		value any
		want  bool
	}{
		{name: "nil", value: nil, want: false},
		{name: "false", value: false, want: false},
		{name: "true", value: true, want: true},
		{name: "empty string", value: "", want: false},
		{name: "string", value: "error", want: true},
		{name: "integer zero", value: json.Number("0"), want: false},
		{name: "decimal zero", value: json.Number("-0.0"), want: false},
		{name: "exponent zero", value: json.Number("0e20"), want: false},
		{name: "nonzero number", value: json.Number("1e-999"), want: true},
		{name: "empty array", value: []any{}, want: false},
		{name: "array", value: []any{"error"}, want: true},
		{name: "empty object", value: map[string]any{}, want: false},
		{name: "object", value: map[string]any{"message": "error"}, want: true},
		{name: "other", value: struct{}{}, want: true},
	} {
		if got := gatewayTruthy(test.value); got != test.want {
			t.Fatalf("%s gatewayTruthy(%#v) = %v, want %v", test.name, test.value, got, test.want)
		}
	}

	for _, test := range []struct {
		name  string
		value any
		want  bool
	}{
		{name: "not number", value: "1", want: false},
		{name: "boolean true is a Python integer", value: true, want: true},
		{name: "boolean false is a Python integer", value: false, want: false},
		{name: "zero", value: json.Number("0"), want: false},
		{name: "negative zero", value: json.Number("-0"), want: false},
		{name: "positive", value: json.Number("1"), want: true},
		{name: "negative", value: json.Number("-2"), want: true},
		{name: "float", value: json.Number("1.0"), want: false},
		{name: "exponent", value: json.Number("1e0"), want: false},
	} {
		if got := gatewayNonzeroInteger(test.value); got != test.want {
			t.Fatalf("%s gatewayNonzeroInteger(%#v) = %v, want %v", test.name, test.value, got, test.want)
		}
	}

	if gatewayToolResultFailed(nil, "terminal") {
		t.Fatal("missing result classified as failed")
	}
	if gatewayToolResultFailed(json.RawMessage(`{`), "terminal") {
		t.Fatal("malformed result classified as failed")
	}
	if !gatewayPolishedTool("yb_send_sticker") || gatewayPolishedTool("plugin_tool") {
		t.Fatal("installed Hermes polished-tool set was not mirrored")
	}
}

func TestHermesGatewayCompletionOnlyText(t *testing.T) {
	t.Parallel()

	fake := newFakeGatewayServer(t)
	fake.setPromptEvents(
		Event{Type: evtThinkingDelta, Payload: json.RawMessage(`{"text":"thinking"}`)},
		Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"final answer"}`)},
	)
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored", "live-stored")

	message, err := server.SendMessage(withTestPromptDispatch(t.Context()), "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if len(message.Parts) != 1 || message.Parts[0].Text != "final answer" || message.Parts[0].StreamedText != "" {
		t.Fatalf("completion-only message = %#v", message)
	}
}

// TestHermesGatewayApprovalDuringParallelToolsReachesTheHost pins the shape a
// hermes turn running tools concurrently produces: the approval callback names
// no tool, and the turn it blocks still completes with the approval delivered.
func TestHermesGatewayApprovalDuringParallelToolsReachesTheHost(t *testing.T) {
	t.Parallel()

	fake := newFakeGatewayServer(t)
	fake.setPromptEvents(
		Event{Type: evtToolStart, Payload: json.RawMessage(`{"tool_id":"native-tool-1"}`)},
		Event{Type: evtToolStart, Payload: json.RawMessage(`{"tool_id":"native-tool-2"}`)},
		Event{Type: evtApprovalRequest, Payload: json.RawMessage(`{"command":"read"}`)},
		Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"blocked"}`)},
	)
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored", "live-stored")

	if _, err := server.SendMessage(withTestPromptDispatch(t.Context()), "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	projected := 0
	for len(server.deliveries) > 0 {
		delivery := <-server.deliveries
		if delivery.Err != nil {
			continue
		}
		event := turnEventFromDelivery(t, delivery)
		if event.Type != evtApprovalRequest {
			continue
		}
		projected++
		if event.Permission == nil || event.Permission.Tool.CallID != event.Permission.ID {
			t.Fatalf("unattributable permission = %#v", event.Permission)
		}
	}
	if projected != 1 {
		t.Fatalf("projected %d approvals, want 1", projected)
	}
}

func testGatewayProvidersAndConfigHelpers(t *testing.T) {
	t.Helper()

	var providers ProvidersResponse
	if err := json.Unmarshal([]byte(`{"providers":[]}`), &providers); err != nil || len(providers.Raw) == 0 {
		t.Fatalf("ProvidersResponse valid = %#v err=%v", providers, err)
	}
	mapped := providersFromGateway(ModelOptionsResult{Providers: []Provider{{
		Slug:   "p",
		Models: []string{"", "named"},
	}}})
	if model, ok := mapped.Providers[0].Models["named"]; !ok || len(mapped.Providers[0].Models) != 1 || model.ID != "named" {
		t.Fatalf("providersFromGateway empty model handling = %#v", mapped)
	}
	if err := materializeHermesConfig(durableTempDir(t), nil, nil); err != nil {
		t.Fatalf("empty config: %v", err)
	}
	originalMarshalIndent := hermesMarshalIndent
	hermesMarshalIndent = func(any, string, string) ([]byte, error) {
		return nil, errors.New("marshal failed")
	}
	if err := materializeHermesConfig(durableTempDir(t), []acp.McpServer{stdioMCPServer("s", "cmd", nil, nil)}, nil); err == nil {
		t.Fatal("materializeHermesConfig ignored marshal error")
	}
	hermesMarshalIndent = originalMarshalIndent
	homeFile := filepath.Join(durableTempDir(t), "home-file")
	if err := os.WriteFile(homeFile, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := materializeHermesConfig(homeFile, []acp.McpServer{stdioMCPServer("s", "cmd", nil, nil)}, nil); err == nil {
		t.Fatal("materializeHermesConfig accepted file home")
	}
}

func TestMaterializeHermesConfig(t *testing.T) {
	t.Run("writes seeds verbatim and installs managed PATH init", func(t *testing.T) {
		home := durableTempDir(t)
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
			if want := wantRestrictedPerm(false); info.Mode().Perm() != want {
				t.Fatalf("seed %q mode = %v, want %v", relative, info.Mode().Perm(), want)
			}
		}
		if script, err := os.ReadFile(filepath.Join(home, hermesPathInitFileName)); err != nil || !bytes.Equal(script, hermesPathInitScript) {
			t.Fatalf("managed PATH init = %q err=%v", script, err)
		}
	})

	t.Run("merges wrapper mcp_servers on top of seeded config.yaml", func(t *testing.T) {
		home := durableTempDir(t)
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
		home := durableTempDir(t)
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
		home := durableTempDir(t)
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

	t.Run("rejects adapter-owned PATH init seed", func(t *testing.T) {
		err := materializeHermesConfig(durableTempDir(t), nil, map[string]string{hermesPathInitFileName: "untrusted"})
		if err == nil {
			t.Fatal("adapter-owned PATH init seed was accepted")
		}
	})

	t.Run("empty installs only PATH init without config mutation", func(t *testing.T) {
		for _, files := range []map[string]string{nil, {}} {
			home := durableTempDir(t)
			if err := materializeHermesConfig(home, nil, files); err != nil {
				t.Fatalf("empty seed files: %v", err)
			}
			if _, err := os.Stat(filepath.Join(home, hermesConfigFileName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("config.yaml mutated without managed config: %v", err)
			}
			if script, err := os.ReadFile(filepath.Join(home, hermesPathInitFileName)); err != nil || !bytes.Equal(script, hermesPathInitScript) {
				t.Fatalf("managed PATH init = %q err=%v", script, err)
			}
		}
	})

	t.Run("rejects confinement escapes", func(t *testing.T) {
		absolute := filepath.Join(durableTempDir(t), "abs")
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
				home := durableTempDir(t)
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

	t.Run("rejects reserved adapter metadata case-insensitively", func(t *testing.T) {
		for name, relative := range map[string]string{
			"path init":              hermesPathInitFileName,
			"namespace nested":       "nested/.ACP-GO-HERMES-ANYTHING",
			"namespace":              ".acp-go-hermes-reserved",
			"owner subtree":          filepath.FromSlash(".acp-go-hermes-session-owners/claim"),
			"owner subtree case":     filepath.FromSlash(".ACP-GO-HERMES-SESSION-OWNERS/claim"),
			"manifest":               ".seed-manifest.json",
			"pending nested":         filepath.FromSlash("nested/.SEED-PENDING.JSON"),
			"backup suffix":          "config.yaml.seed.bak",
			"backup suffix casefold": "nested/CONFIG.YAML.SEED.BAK",
		} {
			t.Run(name, func(t *testing.T) {
				err := materializeHermesConfig(durableTempDir(t), nil, map[string]string{relative: "hostile"})
				if err == nil {
					t.Fatalf("reserved seed path %q accepted", relative)
				}
				var reqErr *acp.RequestError
				if !errors.As(err, &reqErr) {
					t.Fatalf("reserved seed path %q error = %T, want *acp.RequestError", relative, err)
				}
			})
		}
	})
}

func TestMaterializeHermesConfigSeedGuard(t *testing.T) {
	t.Run("propagates mkdir errors", func(t *testing.T) {
		home := durableTempDir(t)
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
		home := durableTempDir(t)
		files := map[string]string{
			"config.yaml":                   "model: {}\n",
			filepath.FromSlash("a/b.json"):  "{}",
			filepath.FromSlash("providers"): "p",
		}
		if err := materializeHermesConfig(home, nil, files); err != nil {
			t.Fatalf("materializeHermesConfig: %v", err)
		}
		manifest := readHermesSeedManifest(t, home)
		want := []string{hermesPathInitFileName, "a/b.json", "config.yaml", "providers"}
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
		home := durableTempDir(t)
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
		home := durableTempDir(t)
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
		home := durableTempDir(t)
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
		home := durableTempDir(t)
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "v1"}); err != nil {
			t.Fatalf("first seed: %v", err)
		}
		// A second pass loads the persisted manifest and treats foo as managed,
		// so re-seeding does not fail closed.
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "v2"}); err != nil {
			t.Fatalf("second seed: %v", err)
		}
		if want := []string{hermesPathInitFileName, "foo"}; !reflect.DeepEqual(readHermesSeedManifest(t, home), want) {
			t.Fatalf("manifest = %#v, want %#v", readHermesSeedManifest(t, home), want)
		}
	})
}

func TestMaterializeHermesConfigSeedGuardErrors(t *testing.T) {
	t.Run("rejects a corrupt manifest", func(t *testing.T) {
		home := durableTempDir(t)
		if err := os.WriteFile(filepath.Join(home, hermesSeedManifestName), []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "x"}); err == nil {
			t.Fatal("corrupt manifest accepted")
		}
	})

	t.Run("propagates a manifest read error", func(t *testing.T) {
		home := durableTempDir(t)
		if err := os.Mkdir(filepath.Join(home, hermesSeedManifestName), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := materializeHermesConfig(home, nil, map[string]string{"foo": "x"}); err == nil {
			t.Fatal("materializeHermesConfig ignored manifest read error")
		}
	})

	t.Run("propagates a manifest write error", func(t *testing.T) {
		home := durableTempDir(t)
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
		home := durableTempDir(t)
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
		home := durableTempDir(t)
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
				want: "session.resume response missing session_key and stored_session_id",
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

func TestGatewayPublicMethodsRefuseMissingTransport(t *testing.T) { //nolint:gocyclo // The public gateway surface shares one closed ownership boundary.
	server := &hermesServer{closed: make(chan struct{}), deliveries: make(chan TurnDelivery, 1)}
	ctx := t.Context()
	if _, err := server.ListSessions(ctx, ""); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("list = %v", err)
	}
	if _, err := server.PersistedSessions(ctx); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("persisted = %v", err)
	}
	if err := server.DeleteSession(ctx, "stored"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("delete = %v", err)
	}
	if err := server.ReloadMCP(ctx, "stored"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("reload = %v", err)
	}
	if _, err := server.SendMessage(ctx, "stored", MessageRequest{}); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("send = %v", err)
	}
	if _, err := server.Messages(ctx, "stored"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("messages = %v", err)
	}
	if err := server.Abort(ctx, "stored"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("abort = %v", err)
	}
	if _, err := server.Fork(ctx, "stored", "message"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("fork = %v", err)
	}
	if _, err := server.ForkWithBaseline(ctx, "stored", "marker", nil); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("fork baseline = %v", err)
	}
	if _, err := server.ConfigProviders(ctx); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("providers = %v", err)
	}
	if err := server.SetModel(ctx, "stored", "provider/model"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("set model = %v", err)
	}

	server.publishTurnError(nil)
	server.publishTurnError(errors.New("ignored duplicate"))
	if !errors.Is((<-server.deliveries).Err, errGatewayStreamClosed) {
		t.Fatal("nil terminal cause was not normalized")
	}

	fake := newFakeGatewayServer(t)
	backed := newGatewayBackedHermesServer(t, fake, "")
	if _, err := backed.submitGatewayParts(withTestPromptDispatch(ctx), "stored", []map[string]any{{valText: "hello"}}); err != nil {
		t.Fatalf("submit gateway parts: %v", err)
	}
	if err := backed.DeleteSession(ctx, "stored-1"); err != nil {
		t.Fatalf("delete discovered active session: %v", err)
	}

	closedTurn := &hermesServer{
		closed: make(chan struct{}), transport: &gatewayTransport{}, deliveries: make(chan TurnDelivery, 1),
	}
	close(closedTurn.closed)
	if transport := closedTurn.beginGatewayTurn(); transport != nil {
		t.Fatalf("closed server admitted transport %#v", transport)
	}
	closedTurn.endGatewayTurn()
	(&hermesServer{closed: make(chan struct{})}).runGatewayReconnectLoop()

	processClose := &hermesServer{
		closed: make(chan struct{}), deliveries: make(chan TurnDelivery, 1), process: &Process{},
	}
	_ = processClose.Close(ctx)
}

func TestFailedBranchCleanupTreatsNativeNotFoundAsSuccess(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.branchFailAfterSave = true
	fake.setNotFound("session.delete", 1)
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored-parent", "live-parent")
	_, err := server.ForkWithBaseline(t.Context(), "stored-parent", "unique-marker", nil)
	if err == nil || !strings.Contains(err.Error(), "branch failed after save") {
		t.Fatalf("branch cleanup result = %v", err)
	}
}

func TestReconnectLoopCloseAfterClientDonePrecedesDispatcherDone(t *testing.T) {
	clientDone := make(chan struct{})
	close(clientDone)
	dispatcherDone := make(chan struct{})
	server := &hermesServer{
		closed: make(chan struct{}),
		transport: &gatewayTransport{
			client: &Client{done: clientDone}, dispatcher: &gatewayTransportDispatcher{done: dispatcherDone},
		},
	}
	observed := make(chan struct{})
	server.afterGatewayClientDone = func() { close(observed) }
	returned := make(chan struct{})
	go func() {
		server.runGatewayReconnectLoop()
		close(returned)
	}()
	<-observed
	close(server.closed)
	<-returned
}

func TestHermesGatewayServerMappingAndAccessorBranches(t *testing.T) {
	t.Run("get session requires committed resume handshake", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		server := newGatewayBackedHermesServer(t, fake, "")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		session, err := server.GetSession(ctx, "stored-1")
		if err != nil || session.ID != "stored-1" || server.liveSessionID("stored-1") != "live-stored-1" {
			t.Fatalf("GetSession active mapping = %#v live=%q err=%v", session, server.liveSessionID("stored-1"), err)
		}
		if len(fake.callsFor("session.resume")) != 1 || len(fake.callsFor("session.active_list")) != 0 {
			t.Fatalf("GetSession bypassed committed resume: resume=%#v active=%#v",
				fake.callsFor("session.resume"), fake.callsFor("session.active_list"))
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
		if server.Deliveries() == nil || server.XDGDirs().Root == "" {
			t.Fatal("server accessors returned empty values")
		}
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
}

func TestHermesGatewayBarrieredResumePreventsForkModelMutationLoss(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.activeEmpty = true
	fake.resumeBuildDefault = "z-ai/glm-4.7"
	fake.expectedPromptModel = "xai-oauth/grok-code-fast-1"
	server := newGatewayBackedHermesServer(t, fake, "z-ai/glm-4.7")

	child, err := server.GetSession(t.Context(), "stored-branch")
	if err != nil || child.ID != "stored-branch" {
		t.Fatalf("resume fork child = %#v err=%v", child, err)
	}
	if err := server.SetModel(t.Context(), child.ID, fake.expectedPromptModel); err != nil {
		t.Fatalf("bind fork model: %v", err)
	}
	if _, err := server.SendMessage(withTestPromptDispatch(t.Context()), child.ID, MessageRequest{Parts: []map[string]any{{valText: "prove model"}}}); err != nil {
		t.Fatalf("prompt after fork model bind: %v", err)
	}

	resumeCalls := fake.callsFor("session.resume")
	if len(resumeCalls) != 1 {
		t.Fatalf("fork child resume calls = %#v", resumeCalls)
	}
	calls := fake.callMethods()
	resumeIndex := slices.Index(calls, "session.resume")
	barrierIndex := slices.Index(calls, "process.list")
	modelIndex := slices.Index(calls, "config.set")
	promptIndex := slices.Index(calls, "prompt.submit")
	if resumeIndex < 0 || barrierIndex <= resumeIndex || modelIndex <= barrierIndex || promptIndex <= modelIndex {
		t.Fatalf("fork resume/barrier/model/prompt order = %#v", calls)
	}

	// Operations that obtain a live id without GetSession use the same barriered
	// rebind helper, so a model selection cannot enter the lazy-build window.
	ensureFake := newFakeGatewayServer(t)
	ensureServer := newGatewayBackedHermesServer(t, ensureFake, "")
	if err := ensureServer.SetModel(t.Context(), "stored-direct", "xai-oauth/grok-code-fast-1"); err != nil {
		t.Fatalf("ensure-live model bind: %v", err)
	}
	ensureResumeCalls := ensureFake.callsFor("session.resume")
	if len(ensureResumeCalls) != 1 {
		t.Fatalf("ensure-live resume calls = %#v", ensureResumeCalls)
	}
	if len(ensureFake.callsFor("process.list")) != 1 {
		t.Fatalf("ensure-live barrier calls = %#v", ensureFake.callsFor("process.list"))
	}
}

// TestHermesGatewayResumeBarrierFailureBranches covers the two ways the barrier
// stops waiting on something other than the build's own verdict: the caller
// withdrew, and the gateway can no longer say whether the build is running.
func TestHermesGatewayResumeBarrierFailureBranches(t *testing.T) {
	withdrawn := newFakeGatewayServer(t)
	withdrawnServer := newGatewayBackedHermesServer(t, withdrawn, "")
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	if err := withdrawnServer.awaitGatewayAgentBuild(cancelled, withdrawnServer.gatewayTransport(), "live-1"); err == nil {
		t.Fatal("barrier ignored a withdrawn context")
	}

	unreadable := newFakeGatewayServer(t)
	unreadable.buildBarrierFails = true
	unreadableServer := newGatewayBackedHermesServer(t, unreadable, "")
	unreadable.setFail("session.active_list")

	if err := unreadableServer.awaitGatewayAgentBuild(t.Context(), unreadableServer.gatewayTransport(), "live-1"); err == nil {
		t.Fatal("barrier ignored an unreadable liveness list")
	}
}

// TestHermesGatewayResumeBarrierWaitsOutTheBuildCap proves the barrier treats
// the gateway's own wait cap as "still building" and retries, and reports a
// genuine build failure once the session is no longer starting.
func TestHermesGatewayResumeBarrierWaitsOutTheBuildCap(t *testing.T) {
	stalling := newFakeGatewayServer(t)
	stalling.buildBarrierStalls = 2
	stalling.resumeBuildDefault = "z-ai/glm-4.7"
	stallServer := newGatewayBackedHermesServer(t, stalling, "z-ai/glm-4.7")

	if _, err := stallServer.GetSession(t.Context(), "stored-1"); err != nil {
		t.Fatalf("resume through a stalled build barrier: %v", err)
	}
	if got := len(stalling.callsFor("process.list")); got != 3 {
		t.Fatalf("barrier attempts = %d", got)
	}
	if got := len(stalling.callsFor("session.active_list")); got != 2 {
		t.Fatalf("barrier liveness polls = %d", got)
	}

	failing := newFakeGatewayServer(t)
	failing.buildBarrierFails = true
	failServer := newGatewayBackedHermesServer(t, failing, "")
	if _, err := failServer.GetSession(t.Context(), "stored-1"); err == nil {
		t.Fatal("resume accepted a failed agent build")
	}
	if got := len(failing.callsFor("process.list")); got != 1 {
		t.Fatalf("failed-build barrier attempts = %d", got)
	}
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
				_, err := server.SendMessage(withTestPromptDispatch(ctx), "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})

				return err
			},
		},
		{
			name:   "submit",
			method: "prompt.submit",
			call: func(ctx context.Context, server *hermesServer) error {
				bindTestGatewaySession(t, server, "stored", "live-stored")
				_, err := server.SendMessage(withTestPromptDispatch(ctx), "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})

				return err
			},
		},
		{
			name:   "history",
			method: "session.history",
			call: func(ctx context.Context, server *hermesServer) error {
				bindTestGatewaySession(t, server, "stored", "live-stored")
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
				bindTestGatewaySession(t, server, "stored", "live-stored")
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
		bindTestGatewaySession(t, server, "stored", "live-stored")
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
			bindTestGatewaySession(t, server, "stored", "live-stored")
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
		bindTestGatewaySession(t, server, "stored", "live-stored")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := server.SendMessage(withTestPromptDispatch(ctx), "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
		if !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("mid-turn close error = %v", err)
		}
		if fed := mustTurnError(t, server.Deliveries()); fed == nil {
			t.Fatal("mid-turn disconnect omitted its ordered terminal error")
		}
	})

	t.Run("context deadline while waiting for events", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setPromptEvents()
		server := newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored", "live-stored")
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		_, err := server.SendMessage(withTestPromptDispatch(ctx), "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline error = %v", err)
		}
	})
}

func TestStartHermesServerGatewayFakeExecutable(t *testing.T) {
	helper := fakeHermesGatewayExecutable(t, fakeGatewayModeOK)
	root := testTraversableTempDir(t)
	cwd := durableTempDir(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{
		ACPSessionID:   "session/one",
		ScratchParent:  root,
		Cwd:            cwd,
		ExecutablePath: helper,
		DefaultModel:   "openai/gpt-test",
		Env:            map[string]string{"BASE_ENV": "base"},
		SessionEnv:     map[string]string{"SESSION_ENV": "carrier"},
		ExtraPathDirs:  []string{durableTempDir(t)},
		HealthTimeout:  5 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
		MCPServers: []acp.McpServer{
			stdioMCPServer("stdio", "cmd", []string{"arg"}, map[string]string{"A": "1"}),
			httpMCPServer("http", "https://example.test/mcp", map[string]string{"Authorization": "token"}),
		},
	}))
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	server, serverOK := client.(*hermesServer)
	if !serverOK {
		t.Fatalf("client type = %T", client)
	}
	if server.xdg.Root == "" || !strings.HasPrefix(filepath.Base(server.xdg.Root), "acp-go-hermes-runtime-") {
		t.Fatalf("xdg dirs = %#v", server.xdg)
	}
	if server.controlLock == nil {
		t.Fatal("server did not retain its runtime-owned control lock")
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
}

func TestStartHermesServerUsesFreshGenerationForSameSession(t *testing.T) {
	helper := fakeHermesGatewayExecutable(t, fakeGatewayModeOK)
	root := testTraversableTempDir(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	options := StartOptions{
		ACPSessionID:   "same-session",
		ScratchParent:  root,
		Cwd:            durableTempDir(t),
		ExecutablePath: helper,
		HealthTimeout:  5 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
	}
	first, err := StartServer(ctx, darwinTestStartOptions(t, options))
	if err != nil {
		t.Fatalf("first StartServer: %v", err)
	}
	firstClosed := false
	defer func() {
		if !firstClosed {
			_ = first.Close(context.Background())
		}
	}()
	replacement, err := StartServer(ctx, darwinTestStartOptions(t, options))
	if err != nil {
		t.Fatalf("replacement StartServer: %v", err)
	}
	replacementClosed := false
	defer func() {
		if !replacementClosed {
			_ = replacement.Close(context.Background())
		}
	}()
	if first.XDGDirs().Root == replacement.XDGDirs().Root {
		t.Fatalf("same-session replacement reused generation root %q", first.XDGDirs().Root)
	}

	if _, err := first.CreateSession(ctx, "predecessor"); err != nil {
		t.Fatalf("independent predecessor gateway became unusable: %v", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatalf("predecessor Close: %v", err)
	}
	firstClosed = true
	if _, err := replacement.CreateSession(ctx, "replacement"); err != nil {
		t.Fatalf("replacement CreateSession: %v", err)
	}
	if err := replacement.Close(ctx); err != nil {
		t.Fatalf("replacement Close: %v", err)
	}
	replacementClosed = true
}

func TestStartHermesServerGatewayFaults(t *testing.T) {
	ctx := context.Background()
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{ExecutablePath: filepath.Join(durableTempDir(t), "missing-hermes")})); err == nil {
		t.Fatal("missing executable unexpectedly started")
	}
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{ScratchParent: string([]byte{0})})); err == nil {
		t.Fatal("invalid scratch parent unexpectedly succeeded")
	}
	if _, err := StartServer(ctx, StartOptions{}); err == nil {
		t.Fatal("missing scratch parent unexpectedly succeeded")
	}
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{ACPSessionID: ACPSessionIDString(string([]byte{0}))})); err == nil {
		t.Fatal("invalid session path unexpectedly succeeded")
	}
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{ExtraPathDirs: []string{"relative"}})); err == nil {
		t.Fatal("relative extra path directory unexpectedly succeeded")
	}
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{SessionEnv: map[string]string{"PATH": "/bad"}})); err == nil {
		t.Fatal("session PATH unexpectedly succeeded")
	}
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{Env: map[string]string{"BASH_ENV": "/bad"}})); err == nil {
		t.Fatal("static BASH_ENV unexpectedly succeeded")
	}
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{ExistingXDG: XDGDirs{Root: filepath.Join(durableTempDir(t), "root")}})); err == nil {
		t.Fatal("incomplete existing xdg unexpectedly succeeded")
	}

	restoreHermesClientSeams(t)
	hermesControlMkdir = func(string, os.FileMode) error { return errors.New("control mkdir") }
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{ExistingXDG: testXDGDirs(t)})); err == nil ||
		!strings.Contains(err.Error(), "control mkdir") {
		t.Fatalf("control mkdir error = %v", err)
	}
	hermesControlMkdir = os.MkdirAll
	hermesControlChmod = func(string, os.FileMode) error { return errors.New("control chmod") }
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{ExistingXDG: testXDGDirs(t)})); err == nil ||
		!strings.Contains(err.Error(), "control chmod") {
		t.Fatalf("control chmod error = %v", err)
	}
	hermesControlChmod = os.Chmod
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{
		ExecutablePath: fakeHermesGatewayExecutable(t, fakeGatewayModeStatusOnly),
		ExistingXDG:    testXDGDirs(t),
		HealthTimeout:  500 * time.Millisecond,
	})); err == nil {
		t.Fatal("gateway readiness failure unexpectedly succeeded")
	}
}

// TestGatewayReconnectsOnIdleDisconnect proves HW4 idle reconnect:
// when the WebSocket drops while no turn is in progress, the reconnect loop redials
// the still-running process and swaps in the new connection.
func TestGatewayReconnectsOnIdleDisconnect(t *testing.T) {
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
		t.Fatal("reconnect loop did not run after idle disconnect")
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

func TestIdleReconnectReplacesTerminalStoredSessionActor(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setPromptEvents(Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"complete"}`)})
	server := newGatewayBackedHermesServer(t, fake, "")
	t.Cleanup(func() { _ = server.Close(context.Background()) })

	first, err := server.SendMessage(withTestPromptDispatch(t.Context()), "stored", MessageRequest{
		Parts: []map[string]any{{"text": "generation one"}},
	})
	if err != nil || len(first.Parts) != 1 || first.Parts[0].Text != "complete" {
		t.Fatalf("generation-1 prompt = %#v, %v", first, err)
	}

	server.gatewayMu.Lock()
	actorOne := server.actorsByStored["stored"]
	server.gatewayMu.Unlock()
	if actorOne == nil {
		t.Fatal("generation-1 prompt did not install its stored-session actor")
	}

	redialEntered := make(chan struct{})
	redialRelease := make(chan struct{})
	published := make(chan struct{})
	server.beforeTransportPublish = func() { close(published) }
	server.enableReconnect(func(context.Context) (*Client, error) {
		close(redialEntered)
		<-redialRelease

		return fake.dialClient(t), nil
	})

	original := server.gatewayTransport()
	if closeErr := original.client.Close(websocket.StatusNormalClosure, "idle generation ended"); closeErr != nil {
		t.Fatal(closeErr)
	}
	<-redialEntered
	select {
	case <-actorOne.done:
	case <-t.Context().Done():
		t.Fatal("generation-1 actor did not reach its terminal barrier")
	}
	close(redialRelease)
	<-published

	// installGatewayDispatcher publishes while holding connMu. Acquiring it here
	// is the exact post-publication barrier for generation 2.
	server.connMu.Lock()
	transportTwo := server.transport
	server.connMu.Unlock()
	if transportTwo == nil || transportTwo.generation != original.generation+1 {
		t.Fatalf("reconnected transport generation = %#v", transportTwo)
	}

	second, err := server.SendMessage(withTestPromptDispatch(t.Context()), "stored", MessageRequest{
		Parts: []map[string]any{{"text": "generation two"}},
	})
	if err != nil || len(second.Parts) != 1 || second.Parts[0].Text != "complete" {
		t.Fatalf("generation-2 prompt = %#v, %v", second, err)
	}

	server.gatewayMu.Lock()
	actorTwo := server.actorsByStored["stored"]
	server.gatewayMu.Unlock()
	if actorTwo == nil {
		t.Fatal("generation-2 prompt did not bind a stored-session actor")
	}
	if actorTwo == actorOne || actorTwo.generation != transportTwo.generation {
		t.Fatalf("generation-2 actor = %p generation %d; old=%p transport=%d",
			actorTwo, actorTwo.generation, actorOne, transportTwo.generation)
	}
}

func TestGatewayPromptWaitsForCoherentReconnectTransportTuple(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setPromptEvents(Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"fresh"}`)})
	server := newGatewayBackedHermesServer(t, fake, "")
	original := server.gatewayTransport()
	publishReached := make(chan struct{})
	publishRelease := make(chan struct{})
	server.beforeTransportPublish = func() {
		close(publishReached)
		<-publishRelease
	}
	server.enableReconnect(func(context.Context) (*Client, error) { return fake.dialClient(t), nil })

	if err := original.client.Close(websocket.StatusNormalClosure, "redial"); err != nil {
		t.Fatal(err)
	}
	<-publishReached

	type promptResult struct {
		message NativeMessage
		err     error
		info    PromptDispatchInfo
	}
	result := make(chan promptResult, 1)
	go func() {
		var info PromptDispatchInfo
		ctx := WithPromptDispatch(t.Context(), func(_ context.Context, dispatch PromptDispatchInfo) error {
			info = dispatch

			return nil
		})
		message, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}})
		result <- promptResult{message: message, err: err, info: info}
	}()

	select {
	case got := <-result:
		t.Fatalf("prompt crossed unpublished reconnect tuple: %#v", got)
	default:
	}
	close(publishRelease)

	got := <-result
	if got.err != nil || len(got.message.Parts) != 1 || got.message.Parts[0].Text != "fresh" {
		t.Fatalf("prompt after reconnect = %#v, %v", got.message, got.err)
	}
	transport := server.gatewayTransport()
	if got.info.TransportGeneration != transport.generation || transport.client == original.client ||
		transport.dispatcher == nil || transport.mappings == nil {
		t.Fatalf("prompt tuple = %#v, current generation=%d", got.info, transport.generation)
	}

	server.beforeTransportPublish = nil
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestGatewayOperationsPinOneTransportTupleAcrossRPCs(t *testing.T) {
	tests := []struct {
		name string
		gate string
		want []string
		run  func(context.Context, *hermesServer) error
	}{
		{
			name: "create", gate: "session.title", want: []string{"session.create", "session.title", "session.list"},
			run: func(ctx context.Context, server *hermesServer) error {
				_, err := server.CreateSession(ctx, "pinned")

				return err
			},
		},
		{
			name: "messages", gate: "session.history", want: []string{"session.history"},
			run: func(ctx context.Context, server *hermesServer) error {
				_, err := server.Messages(ctx, "stored")

				return err
			},
		},
		{
			name: "abort", gate: "session.interrupt", want: []string{"session.interrupt"},
			run: func(ctx context.Context, server *hermesServer) error { return server.Abort(ctx, "stored") },
		},
		{
			name: "reload", gate: "reload.mcp", want: []string{"reload.mcp"},
			run: func(ctx context.Context, server *hermesServer) error { return server.ReloadMCP(ctx, "stored") },
		},
		{
			name: "model selection", gate: "config.set", want: []string{"config.set"},
			run: func(ctx context.Context, server *hermesServer) error {
				return server.SetModel(ctx, "stored", "openai/gpt-test")
			},
		},
		{
			name: "delete", gate: "session.close", want: []string{"session.close", "session.delete"},
			run: func(ctx context.Context, server *hermesServer) error { return server.DeleteSession(ctx, "stored") },
		},
		{
			name: "fork", gate: "session.list", want: []string{"session.list", "session.branch", "session.close"},
			run: func(ctx context.Context, server *hermesServer) error {
				_, err := server.Fork(ctx, "stored", "marker")

				return err
			},
		},
		{
			name: "send with model", gate: "config.set", want: []string{"config.set", "prompt.submit"},
			run: func(ctx context.Context, server *hermesServer) error {
				model := ModelSelector{ProviderID: "openai", ModelID: "gpt-test"}
				ctx = WithPromptDispatch(ctx, func(context.Context, PromptDispatchInfo) error { return nil })
				_, err := server.SendMessage(ctx, "stored", MessageRequest{
					Model: &model, Parts: []map[string]any{{"text": "pinned"}},
				})

				return err
			},
		},
		{
			name: "active inventory", gate: "session.active_list", want: []string{"session.active_list"},
			run: func(ctx context.Context, server *hermesServer) error {
				_, err := server.ListSessions(ctx, "")

				return err
			},
		},
		{
			name: "persisted inventory", gate: "session.list", want: []string{"session.list"},
			run: func(ctx context.Context, server *hermesServer) error {
				_, err := server.PersistedSessions(ctx)

				return err
			},
		},
		{
			name: "provider inventory", gate: "model.options", want: []string{"model.options"},
			run: func(ctx context.Context, server *hermesServer) error {
				_, err := server.ConfigProviders(ctx)

				return err
			},
		},
		{
			name: "permission response", gate: "approval.respond", want: []string{"approval.respond"},
			run: func(ctx context.Context, server *hermesServer) error {
				permission, _ := testGatewayControlRequests(t, server, "stored", "live-stored")

				return server.ReplyPermission(ctx, permission, valOnce, "")
			},
		},
		{
			name: "question response", gate: "clarify.respond", want: []string{"clarify.respond"},
			run: func(ctx context.Context, server *hermesServer) error {
				_, question := testGatewayControlRequests(t, server, "stored", "live-stored")

				return server.ReplyQuestion(ctx, question, [][]string{{"yes"}})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldGateway := newFakeGatewayServer(t)
			oldGateway.setPromptEvents(Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"pinned"}`)})
			entered, release := oldGateway.gateRPC(test.gate)
			server := newGatewayBackedHermesServer(t, oldGateway, "")
			bindTestGatewaySession(t, server, "stored", "live-stored")
			original := server.gatewayTransport()

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- test.run(ctx, server) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatalf("%s did not reach pinned RPC: %v", test.name, ctx.Err())
			}

			newGateway := newFakeGatewayServer(t)
			server.installGatewayDispatcher(newGateway.dialClient(t))
			release()
			if err := <-result; err != nil {
				t.Fatalf("%s after tuple replacement: %v", test.name, err)
			}
			if methods := newGateway.callMethods(); len(methods) != 0 {
				t.Fatalf("%s spliced RPCs onto successor transport: %v", test.name, methods)
			}
			methods := oldGateway.callMethods()
			for _, want := range test.want {
				if !containsString(methods, want) {
					t.Fatalf("%s old transport methods = %v, missing %q", test.name, methods, want)
				}
			}

			_ = original.client.Close(websocket.StatusNormalClosure, "retire test predecessor")
			select {
			case <-original.dispatcher.done:
			case <-ctx.Done():
				t.Fatalf("retire %s predecessor: %v", test.name, ctx.Err())
			}
			if err := server.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func TestGatewayReconnectWaitsForTurnBeforeReconnect(t *testing.T) {
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

func TestGatewayReconnectLoopStopsAfterTurnWhenClosed(t *testing.T) {
	reachedIdle := make(chan struct{})
	releaseIdle := make(chan struct{})

	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	server.afterTurnIdle = func() {
		close(reachedIdle)
		<-releaseIdle
	}

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

	// Shut down after the turn becomes idle but before the reconnect loop can
	// redial; the loop must stop without reconnecting.
	close(server.closed)
	close(releaseIdle)
	server.reconnectWG.Wait()

	select {
	case <-reconnied:
		t.Fatal("reconnect loop ran during shutdown")
	case <-time.After(150 * time.Millisecond):
	}
	_ = original.Close(websocket.StatusNormalClosure, "done")
}

func TestGatewayReconnectLoopStopsWhileWaitingForClosedTurn(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	server.enableReconnect(func(context.Context) (*Client, error) {
		return fake.dialClient(t), nil
	})
	server.beginGatewayTurn()
	original := server.gatewayClient()
	if err := original.Close(websocket.StatusNormalClosure, "drop"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	close(server.closed)
	server.endGatewayTurn()
	server.reconnectWG.Wait()
}

func TestReconnectGatewayRedialErrorBranches(t *testing.T) {
	const secret = "redial-secret-sentinel"
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	var logs bytes.Buffer
	server.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	server.turnIdle = sync.NewCond(&server.connMu)
	original := server.gatewayClient()
	server.redial = func(context.Context) (*Client, error) {
		return nil, errors.New(secret)
	}
	// Redial error with the server still open: logs and backs off.
	server.reconnectGateway()
	if !strings.Contains(logs.String(), "dial_failed") || strings.Contains(logs.String(), secret) {
		t.Fatalf("redial log = %q", logs.String())
	}

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

// mcpEnvCapturePrefix marks the argv entry naming where the fake gateway
// generation writes the MCP secret environment it was launched with. The
// destination travels in argv because the adapter's governed environment
// namespace is reserved for real options and test scaffolding must not claim a
// name inside it.
const mcpEnvCapturePrefix = "-capture-mcp-env="

// fakeHermesGatewayExecutable writes a launcher for the fake gateway
// generation. Anything in extraArgs reaches that generation's argv ahead of the
// adapter's own arguments.
func fakeHermesGatewayExecutable(t *testing.T, mode string, extraArgs ...string) string {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	args := append([]string{"-test.run=TestFakeHermesGatewayProcessHelper", "--"}, extraArgs...)

	return writeTestBinaryLauncher(t, durableTempDir(t), "fake-hermes", testBinary,
		map[string]string{
			"ACP_GO_HERMES_GATEWAY_HELPER": "1",
			"ACP_GO_HERMES_GATEWAY_MODE":   mode,
		},
		args)
}

// exitWhenParentTestExits is the fake gateway generation's own teardown. The
// suite exercises closes that deliberately leave a process retained — an
// unproven containment keeps the native tree alive on purpose — so a generation
// that only died when its owner killed it outlived the suite that started it.
// It reaps itself the moment the test binary that launched it is gone.
func exitWhenParentTestExits() {
	owner := os.Getppid()
	if value := os.Getenv(fakeLauncherOwnerPIDEnv); value != "" {
		if pid, convErr := strconv.Atoi(value); convErr == nil {
			owner = pid
		}
	}

	watchFakeLauncherOwner(owner)
}

func runFakeHermesGatewayProcess(args []string, mode string) error {
	if slices.Contains(args, "--version") {
		_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.21.1 (fake)")

		return nil
	}

	exitWhenParentTestExits()

	for _, arg := range args {
		capture, ok := strings.CutPrefix(arg, mcpEnvCapturePrefix)
		if !ok {
			continue
		}
		values := map[string]string{}
		for _, entry := range os.Environ() {
			key, value, found := strings.Cut(entry, "=")
			if found && strings.HasPrefix(key, sharedMCPSecretEnvPrefix) {
				values[key] = value
			}
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return err
		}
		if err := os.WriteFile(capture, encoded, 0o600); err != nil {
			return err
		}

		break
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
	case "session.title":
		return map[string]any{"pending": false, "title": params["title"]}
	case "session.active_list":
		return map[string]any{"sessions": []any{}}
	case "session.list":
		return map[string]any{"sessions": []map[string]any{{"id": "stored-fake", "title": "Hermes session"}}}
	case "session.history":
		return map[string]any{"count": 0, "messages": []any{}}
	case "model.options":
		return map[string]any{"model": "anthropic/claude-sonnet-4", "provider": "", "providers": []any{}}
	case "image.attach_bytes":
		return map[string]any{"attached": true}
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
	controlMkdir := hermesControlMkdir
	controlChmod := hermesControlChmod
	t.Cleanup(func() {
		hermesMarshalIndent = marshalIndent
		hermesControlMkdir = controlMkdir
		hermesControlChmod = controlChmod
	})
}

// testTraversableTempDir is a scratch parent the isolated identity can enter.
// t.TempDir cannot stand in: it nests its leaf under a 0700 directory, so every
// generated tree beneath it is refused for an ancestry the target identity
// cannot traverse.
func testTraversableTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("", "acp-go-hermes-test-")
	if err != nil {
		t.Fatalf("create traversable test directory: %v", err)
	}
	if err = os.Chmod(directory, 0o711); err != nil {
		t.Fatalf("make test directory traversable: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })

	return directory
}

func testXDGDirs(t *testing.T) XDGDirs {
	t.Helper()
	dirs, err := CreateGenerationXDGDirs(testTraversableTempDir(t))
	if err != nil {
		t.Fatalf("CreateGenerationXDGDirs: %v", err)
	}

	return dirs
}

func darwinTestStartOptions(t *testing.T, options StartOptions) StartOptions {
	t.Helper()
	options.AmbientEnvironment = testAmbientEnvironment()
	if strings.ContainsRune(options.ScratchParent, '\x00') {
		return options
	}
	if options.ExistingXDG.Root != "" {
		options.ScratchParent = filepath.Dir(options.ExistingXDG.Root)
	} else if options.ScratchParent == "" {
		options.ScratchParent = durableTempDir(t)
	}

	return options
}

func containsString(values []string, want string) bool {
	return slices.Contains(values, want)
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
			name: "message.complete status error (billing)",
			events: []Event{{
				Type:    evtMessageComplete,
				Payload: json.RawMessage(`{"text":"HTTP 402: This request requires more credits","usage":{"total_tokens":0},"status":"error"}`),
			}},
			message: "HTTP 402: This request requires more credits",
		},
		{
			name: "bare error event (auth)",
			events: []Event{{
				Type:    evtError,
				Payload: json.RawMessage(`{"error":{"message":"invalid api key","statusCode":401,"providerCode":"auth_error"}}`),
			}},
			message:  "invalid api key",
			status:   401,
			provider: "auth_error",
		},
		{
			// The shape hermes actually emits when a turn dies before or outside
			// its own terminal path: an error event with a bare message, and no
			// message.complete behind it.
			name: "bare error event ends the turn",
			events: []Event{{
				Type:    evtError,
				Payload: json.RawMessage(`{"message":"agent init failed: no provider credentials"}`),
			}},
			message: "agent init failed: no provider credentials",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			fake.setPromptEvents(tt.events...)
			server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
			bindTestGatewaySession(t, server, "stored", "live-stored")

			_, err := server.SendMessage(withTestPromptDispatch(ctx), "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})

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

// Provider-looking assistant text is not itself an error. Only Hermes' native
// terminal status (or its structured finish/error fields) classifies the turn,
// so ordinary model content can contain the same words without brittle text
// matching in the adapter.
func TestTurnFailureProviderTextWithCompleteStatusSucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fake := newFakeGatewayServer(t)
	fake.setPromptEvents(Event{
		Type:    evtMessageComplete,
		Payload: json.RawMessage(`{"text":"HTTP 402: This request requires more credits","usage":{"total_tokens":4},"status":"complete"}`),
	})
	server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
	bindTestGatewaySession(t, server, "stored", "live-stored")

	message, err := server.SendMessage(withTestPromptDispatch(ctx), "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
	if err != nil {
		t.Fatalf("complete turn failed from assistant text: %v", err)
	}

	if message.Info.Finish != valStop || message.Info.Tokens.Total != 4 || len(message.Parts) != 1 || message.Parts[0].Text != "HTTP 402: This request requires more credits" {
		t.Fatalf("complete turn = %#v, want exact assistant text", message)
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
	bindTestGatewaySession(t, server, "stored", "live-stored")

	_, err := server.SendMessage(withTestPromptDispatch(ctx), "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
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

func TestTurnFailureMalformedLineFencesTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fake := newFakeGatewayServer(t)
	fake.setPromptRawFrames("this is not json{")
	fake.setPromptEvents(Event{
		Type:    evtMessageComplete,
		Payload: json.RawMessage(`{"usage":{"total_tokens":3}}`),
	})
	server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
	bindTestGatewaySession(t, server, "stored", "live-stored")

	_, err := server.SendMessage(withTestPromptDispatch(ctx), "stored", MessageRequest{Parts: []map[string]any{{"text": "hi"}}})
	if err == nil || !strings.Contains(err.Error(), "invalid character") {
		t.Fatalf("malformed line error = %v", err)
	}
}
