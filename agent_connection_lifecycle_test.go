package hermesacp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestServeContextAndInputDone(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Serve(cancelled, strings.NewReader(""), io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve canceled error = %v", err)
	}
	waitCtx, waitCancel := context.WithCancel(context.Background())
	waitReader, waitWriter := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- Serve(waitCtx, waitReader, io.Discard)
	}()
	waitCancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve wait canceled error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after context cancellation")
	}
	_ = waitReader.Close()
	_ = waitWriter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := Serve(ctx, strings.NewReader(""), io.Discard); err != nil {
		t.Fatalf("Serve EOF error = %v", err)
	}
}

func TestLocalAgentConnectionHandleRoutesAndErrors(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1, MaxConcurrentPrompts: 1, MaxConcurrentClientCalls: 1}))
	conn := &localAgentConnection{agent: agent}

	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionList, json.RawMessage(`{}`)); reqErr == nil {
		t.Fatal("uninitialized session/list unexpectedly succeeded")
	}
	if _, reqErr := conn.handle(ctx, acp.AgentMethodInitialize, json.RawMessage(`{`)); reqErr == nil {
		t.Fatal("malformed initialize unexpectedly succeeded")
	}
	initResp, reqErr := conn.handle(ctx, acp.AgentMethodInitialize, mustJSON(t, acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{
			PositionEncodings: []acp.PositionEncodingKind{acp.PositionEncodingKindUtf8},
		},
	}))
	if reqErr != nil {
		t.Fatalf("initialize reqErr = %v", reqErr)
	}
	if initResp.(acp.InitializeResponse).AgentCapabilities.PositionEncoding == nil {
		t.Fatalf("initialize response = %#v", initResp)
	}
	if _, reqErr := conn.handle(ctx, "missing/method", json.RawMessage(`{}`)); reqErr == nil || reqErr.Code != -32601 {
		t.Fatalf("missing method reqErr = %#v", reqErr)
	}
	if _, reqErr := conn.handle(ctx, "_missing/method", json.RawMessage(`{}`)); reqErr == nil || reqErr.Code != -32601 {
		t.Fatalf("missing extension reqErr = %#v", reqErr)
	}
	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionNew, mustJSON(t, acp.NewSessionRequest{})); reqErr == nil {
		t.Fatal("invalid new-session params unexpectedly succeeded")
	}
	if _, reqErr := conn.handle(ctx, ForkSessionMethod, json.RawMessage(`{`)); reqErr == nil {
		t.Fatal("malformed extension fork unexpectedly succeeded")
	}
	if _, reqErr := conn.handle(ctx, ForkSessionMethod, mustJSON(t, acp.UnstableForkSessionRequest{})); reqErr == nil {
		t.Fatal("invalid extension fork unexpectedly succeeded")
	}
	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionCancel, json.RawMessage(`{`)); reqErr == nil {
		t.Fatal("malformed notification unexpectedly succeeded")
	}
	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionCancel, mustJSON(t, acp.CancelNotification{SessionId: "missing"})); reqErr == nil {
		t.Fatal("cancel notification error was not surfaced")
	}
	fakeClient := newFakeHermesClient()
	fakeSession := testSession(agent, fakeClient)
	agent.mu.Lock()
	agent.sessions[fakeSession.id] = fakeSession
	agent.mu.Unlock()
	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionCancel, mustJSON(t, acp.CancelNotification{SessionId: fakeSession.id})); reqErr != nil {
		t.Fatalf("cancel notification reqErr = %#v", reqErr)
	}
	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionSetMode, mustJSON(t, acp.SetSessionModeRequest{})); reqErr == nil || reqErr.Code != -32601 {
		t.Fatalf("set mode reqErr = %#v", reqErr)
	}
}

func TestLocalAgentConnectionClientCallErrors(t *testing.T) {
	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1, MaxConcurrentPrompts: 1, MaxConcurrentClientCalls: 1}))
	conn := &localAgentConnection{agent: agent}

	if err := conn.NotifyExtension(context.Background(), "bad/method", nil); err == nil {
		t.Fatal("NotifyExtension accepted stable method name")
	}
	agent.clientCalls <- struct{}{}
	if _, err := conn.RequestPermission(context.Background(), acp.RequestPermissionRequest{}); err == nil {
		t.Fatal("RequestPermission ignored client-call backpressure")
	}
	if err := conn.SessionUpdate(context.Background(), acp.SessionNotification{}); err == nil {
		t.Fatal("SessionUpdate ignored client-call backpressure")
	}
	if err := conn.NotifyExtension(context.Background(), "_test/event", nil); err == nil {
		t.Fatal("NotifyExtension ignored client-call backpressure")
	}
	if _, err := conn.CreateElicitation(context.Background(), acp.UnstableCreateElicitationRequest{}, elicitationScope{}); err == nil {
		t.Fatal("CreateElicitation accepted empty request before backpressure")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := conn.RequestPermission(cancelled, acp.RequestPermissionRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RequestPermission canceled error = %v", err)
	}
	<-agent.clientCalls
}

func TestRequestErrorAndCapabilityHelpers(t *testing.T) {
	if requestError(nil) != nil {
		t.Fatal("requestError(nil) returned non-nil")
	}
	reqErr := acp.NewInvalidParams(map[string]any{"x": "y"})
	if requestError(reqErr) != reqErr {
		t.Fatal("requestError did not preserve request error")
	}
	if got := requestError(context.Canceled); got == nil || got.Code != -32800 {
		t.Fatalf("requestError canceled = %#v", got)
	}
	if got := requestError(errors.New("plain")); got == nil || got.Code != -32603 {
		t.Fatalf("requestError plain = %#v", got)
	}
	requestIDStr := acp.RequestIdStr("request")
	requestID := acp.RequestId{Str: &requestIDStr}
	urlParams, err := scopedElicitationParams(acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{
			ElicitationId: "e",
			Message:       "open",
			Url:           "https://example.test",
			Meta:          map[string]any{"k": "v"},
		},
	}, elicitationScope{SessionID: "s", ToolCallID: "tool", RequestID: &requestID})
	if err != nil || !strings.Contains(string(urlParams), `"requestId":"request"`) {
		t.Fatalf("url scoped elicitation = %s err=%v", urlParams, err)
	}
	if selectPositionEncoding([]acp.PositionEncodingKind{acp.PositionEncodingKindUtf8}) != acp.PositionEncodingKindUtf8 {
		t.Fatal("utf8 position encoding not selected")
	}
	if selectPositionEncoding([]acp.PositionEncodingKind{acp.PositionEncodingKindUtf16}) != acp.PositionEncodingKindUtf16 {
		t.Fatal("utf16 position encoding not selected")
	}
	if selectPositionEncoding(nil) != acp.PositionEncodingKindUtf16 {
		t.Fatal("default position encoding mismatch")
	}

	for _, tt := range []struct {
		name     string
		caps     *acp.ElicitationCapabilities
		wantForm bool
		wantURL  bool
	}{
		{name: "nil", caps: nil, wantForm: false, wantURL: false},
		{name: "empty object", caps: &acp.ElicitationCapabilities{}, wantForm: true, wantURL: false},
		{name: "url only", caps: &acp.ElicitationCapabilities{Url: &acp.ElicitationUrlCapabilities{}}, wantForm: false, wantURL: true},
		{name: "form explicit", caps: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}, wantForm: true, wantURL: false},
	} {
		t.Run("elicitation "+tt.name, func(t *testing.T) {
			agent := NewAgent()
			agent.clientCapabilities.Elicitation = tt.caps
			if got := agent.clientSupportsFormElicitation(); got != tt.wantForm {
				t.Fatalf("clientSupportsFormElicitation() = %v, want %v", got, tt.wantForm)
			}
			if got := agent.clientSupportsURLElicitation(); got != tt.wantURL {
				t.Fatalf("clientSupportsURLElicitation() = %v, want %v", got, tt.wantURL)
			}
		})
	}
}

func TestNewLocalAgentConnectionDone(t *testing.T) {
	var output bytes.Buffer
	conn := newLocalAgentConnection(NewAgent(), &output, strings.NewReader(""))
	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("local connection did not close after EOF input")
	}
}

func TestLifecycleCommandUpdateAfterResponseBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	t.Cleanup(func() {
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()
	})

	agent := NewAgent()
	agent.options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
		client := newFakeHermesClient()
		xdg, err := createXDGDirs(t.TempDir(), string(opts.ACPSessionID))
		if err != nil {
			return nil, err
		}
		client.xdg = xdg
		client.createSession = testNativeSession("native-1")
		client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
		return client, nil
	}
	conn := newLocalAgentConnection(agent, a2cW, c2aR)
	agent.setAgentClient(conn)

	lines := make(chan string, 4)
	go func() {
		scanner := bufio.NewScanner(a2cR)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	writeJSONRPC := func(payload string) {
		t.Helper()
		if _, err := io.WriteString(c2aW, payload+"\n"); err != nil {
			t.Fatalf("write request: %v", err)
		}
	}
	readLine := func() string {
		t.Helper()
		select {
		case line := <-lines:
			return line
		case <-ctx.Done():
			t.Fatal("timed out waiting for JSON-RPC line")
			return ""
		}
	}

	writeJSONRPC(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	if line := readLine(); !strings.Contains(line, `"id":1`) || !strings.Contains(line, `"result"`) {
		t.Fatalf("initialize line = %s", line)
	}
	cwd := t.TempDir()
	writeJSONRPC(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":` + strconv.Quote(cwd) + `,"mcpServers":[]}}`)
	responseLine := readLine()
	if !strings.Contains(responseLine, `"id":2`) || !strings.Contains(responseLine, `"result"`) {
		t.Fatalf("session/new response line = %s", responseLine)
	}
	updateLine := readLine()
	if !strings.Contains(updateLine, `"method":"session/update"`) ||
		!strings.Contains(updateLine, `"sessionUpdate":"available_commands_update"`) ||
		!strings.Contains(updateLine, `"name":"review"`) {
		t.Fatalf("post-response update line = %s", updateLine)
	}
}

func TestConcurrentLifecycleCommandUpdatesFollowOwnResponses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	t.Cleanup(func() {
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()
	})

	type createControl struct {
		started  chan struct{}
		release  chan struct{}
		nativeID string
		command  string
	}
	controls := make(chan *createControl, 2)
	var factoryMu sync.Mutex
	factoryCount := 0
	agent := NewAgent()
	agent.options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
		factoryMu.Lock()
		factoryCount++
		index := factoryCount
		factoryMu.Unlock()

		client := newFakeHermesClient()
		xdg, err := createXDGDirs(t.TempDir(), string(opts.ACPSessionID))
		if err != nil {
			return nil, err
		}
		client.xdg = xdg
		control := &createControl{
			started:  make(chan struct{}),
			release:  make(chan struct{}),
			nativeID: "native-" + strconv.Itoa(index),
			command:  "cmd" + strconv.Itoa(index),
		}
		client.commands = []nativeCommand{{Name: control.command, Description: "Command", Source: "command"}}
		client.createSessionFunc = func(ctx context.Context, _ string) (nativeSession, error) {
			close(control.started)
			select {
			case <-control.release:
				return testNativeSession(control.nativeID), nil
			case <-ctx.Done():
				return nativeSession{}, ctx.Err()
			}
		}
		controls <- control
		return client, nil
	}
	conn := newLocalAgentConnection(agent, a2cW, c2aR)
	agent.setAgentClient(conn)

	lines := make(chan string, 8)
	go func() {
		scanner := bufio.NewScanner(a2cR)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	writeJSONRPC := func(payload string) {
		t.Helper()
		if _, err := io.WriteString(c2aW, payload+"\n"); err != nil {
			t.Fatalf("write request: %v", err)
		}
	}
	readLine := func() string {
		t.Helper()
		select {
		case line := <-lines:
			return line
		case <-ctx.Done():
			t.Fatal("timed out waiting for JSON-RPC line")
			return ""
		}
	}

	writeJSONRPC(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	if line := readLine(); !strings.Contains(line, `"id":1`) || !strings.Contains(line, `"result"`) {
		t.Fatalf("initialize line = %s", line)
	}
	writeJSONRPC(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":` + strconv.Quote(t.TempDir()) + `,"mcpServers":[]}}`)
	writeJSONRPC(`{"jsonrpc":"2.0","id":3,"method":"session/new","params":{"cwd":` + strconv.Quote(t.TempDir()) + `,"mcpServers":[]}}`)
	first := <-controls
	second := <-controls
	<-first.started
	<-second.started
	close(first.release)
	close(second.release)

	responseAt := map[acp.SessionId]int{}
	updateAt := map[acp.SessionId]int{}
	for i := 0; i < 4; i++ {
		line := readLine()
		var msg struct {
			ID     *json.RawMessage `json:"id,omitempty"`
			Method string           `json:"method,omitempty"`
			Result struct {
				SessionID acp.SessionId `json:"sessionId"`
			} `json:"result,omitempty"`
			Params struct {
				SessionID acp.SessionId `json:"sessionId"`
				Update    struct {
					SessionUpdate string `json:"sessionUpdate"`
				} `json:"update"`
			} `json:"params,omitempty"`
		}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("parse line %q: %v", line, err)
		}
		if msg.ID != nil && msg.Result.SessionID != "" {
			responseAt[msg.Result.SessionID] = i
		}
		if msg.Method == "session/update" && msg.Params.Update.SessionUpdate == "available_commands_update" {
			updateAt[msg.Params.SessionID] = i
		}
	}
	if len(responseAt) != 2 || len(updateAt) != 2 {
		t.Fatalf("responseAt=%#v updateAt=%#v", responseAt, updateAt)
	}
	for sessionID, updateIndex := range updateAt {
		responseIndex, ok := responseAt[sessionID]
		if !ok {
			t.Fatalf("update for %q had no lifecycle response; responseAt=%#v updateAt=%#v", sessionID, responseAt, updateAt)
		}
		if updateIndex <= responseIndex {
			t.Fatalf("update for %q at %d, response at %d; responseAt=%#v updateAt=%#v", sessionID, updateIndex, responseIndex, responseAt, updateAt)
		}
	}
}

func TestPostResponseWriterIgnoresUnrelatedWriteBeforeLifecycleResponse(t *testing.T) {
	hooks := make(chan acp.SessionId, 1)
	writer := newPostResponseWriter(io.Discard, func(id acp.SessionId) func() {
		return func() { hooks <- id }
	})
	writer.observeRequestLine([]byte(`{"jsonrpc":"2.0","id":7,"method":"session/resume","params":{"sessionId":"session-7","cwd":"/tmp/project","mcpServers":[]}}`))
	if _, err := writer.Write([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"other","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"unrelated"}}}}` + "\n")); err != nil {
		t.Fatalf("write unrelated update: %v", err)
	}
	select {
	case id := <-hooks:
		t.Fatalf("unrelated write fired hook for %q", id)
	case <-time.After(25 * time.Millisecond):
	}

	if _, err := writer.Write([]byte(`{"jsonrpc":"2.0","id":7,"result":{"configOptions":[]}}` + "\n")); err != nil {
		t.Fatalf("write lifecycle response: %v", err)
	}
	select {
	case id := <-hooks:
		if id != "session-7" {
			t.Fatalf("hook id = %q, want session-7", id)
		}
	case <-time.After(time.Second):
		t.Fatal("lifecycle response did not fire hook")
	}
}

func TestPostResponseWriterParserBranches(t *testing.T) {
	if _, ok := postLifecycleRequestFromLine([]byte(`{"jsonrpc":"2.0","id":1,"method":"session/list","params":{}}`)); ok {
		t.Fatal("non-lifecycle request registered post-response hook")
	}
	if _, ok := postLifecycleRequestFromMessage(acp.AgentMethodSessionResume, json.RawMessage(`{`)); ok {
		t.Fatal("malformed lifecycle params registered post-response hook")
	}
	if _, ok := postLifecycleRequestFromMessage(acp.AgentMethodSessionResume, json.RawMessage(`{"sessionId":""}`)); ok {
		t.Fatal("empty lifecycle session id registered post-response hook")
	}
	if _, ok := jsonRPCIDKey(nil); ok {
		t.Fatal("empty JSON-RPC id was accepted")
	}
	if _, ok := jsonRPCIDKey(json.RawMessage(`{`)); ok {
		t.Fatal("malformed JSON-RPC id was accepted")
	}

	nilHookWriter := newPostResponseWriter(io.Discard, nil)
	if hooks := nilHookWriter.hooksForResponseLine([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`)); hooks != nil {
		t.Fatalf("nil hook writer returned hooks: %#v", hooks)
	}

	writer := newPostResponseWriter(io.Discard, func(id acp.SessionId) func() {
		return func() {}
	})
	writer.observeRequestLine([]byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp/project","mcpServers":[]}}`))
	if hooks := writer.hooksForResponseLine([]byte(`{"jsonrpc":"2.0","id":1,"result":"bad"}`)); hooks != nil {
		t.Fatalf("malformed lifecycle result returned hooks: %#v", hooks)
	}
	writer.observeRequestLine([]byte(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp/project","mcpServers":[]}}`))
	if hooks := writer.hooksForResponseLine([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`)); hooks != nil {
		t.Fatalf("empty lifecycle result session id returned hooks: %#v", hooks)
	}
}

func TestPostResponseHookBranches(t *testing.T) {
	ctx := context.Background()
	hooks := make(chan acp.SessionId, 1)
	writer := newPostResponseWriter(errWriter{}, func(id acp.SessionId) func() {
		return func() { hooks <- id }
	})
	writer.observeRequestLine([]byte(`{"jsonrpc":"2.0","id":1,"method":"session/resume","params":{"sessionId":"session-1"}}`))
	if _, err := writer.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n")); err == nil {
		t.Fatal("postResponseWriter write error = nil")
	}
	select {
	case id := <-hooks:
		t.Fatalf("hook ran after failed write for %q", id)
	default:
	}

	handler := localLifecycleResponse(
		func(*Agent, context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
			return acp.NewSessionResponse{}, errors.New("new failed")
		},
	)
	_, reqErr := handler(ctx, NewAgent(), mustJSON(t, acp.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acp.McpServer{}}))
	if reqErr == nil || !strings.Contains(reqErr.Message, "Internal error") {
		t.Fatalf("lifecycle call error = %#v", reqErr)
	}

	emptyIDHandler := localLifecycleResponse(
		func(*Agent, context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
			return acp.NewSessionResponse{}, nil
		},
	)
	result, reqErr := emptyIDHandler(ctx, NewAgent(), mustJSON(t, acp.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acp.McpServer{}}))
	if reqErr != nil {
		t.Fatalf("empty id handler: %#v", reqErr)
	}
	if _, ok := result.(acp.NewSessionResponse); !ok {
		t.Fatalf("empty id handler result = %#v", result)
	}

	missingAgent := NewAgent()
	missingAgent.refreshCommandsAfterResponse("missing")()

	refreshErrClient := newFakeHermesClient()
	refreshErrClient.commandsErr = errors.New("commands failed")
	refreshErrAgent := NewAgent()
	refreshErrSession := testSession(refreshErrAgent, refreshErrClient)
	refreshErrAgent.sessions[refreshErrSession.id] = refreshErrSession
	refreshErrAgent.refreshCommandsAfterResponse(refreshErrSession.id)()

	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	parentAgent := NewAgent()
	parentAgent.options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
		child := newFakeHermesClient()
		xdg, err := createXDGDirs(t.TempDir(), string(opts.ACPSessionID))
		if err != nil {
			return nil, err
		}
		child.xdg = xdg
		child.getSession = testNativeSession("native-child")
		return child, nil
	}
	parent := testSession(parentAgent, parentClient)
	parentAgent.sessions[parent.id] = parent
	conn := &localAgentConnection{agent: parentAgent}
	conn.initialized.Store(true)
	result, reqErr = conn.handle(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, t.TempDir())))
	if reqErr != nil {
		t.Fatalf("fork extension handle: %#v", reqErr)
	}
	if _, ok := result.(acp.UnstableForkSessionResponse); !ok {
		t.Fatalf("fork result = %#v", result)
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestLocalAgentConnectionClientCallsOverPipes(t *testing.T) {
	ctx := context.Background()
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	t.Cleanup(func() {
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()
	})

	client := &pipeACPClient{}
	_ = acp.NewClientSideConnection(client, c2aW, a2cR)
	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 2}))
	conn := newLocalAgentConnection(agent, a2cW, c2aR)
	agent.setAgentClient(conn)

	permission, err := conn.RequestPermission(ctx, acp.RequestPermissionRequest{Options: []acp.PermissionOption{
		{OptionId: "once", Kind: acp.PermissionOptionKindAllowOnce, Name: "Allow once"},
	}})
	if err != nil {
		t.Fatalf("RequestPermission: %v", err)
	}
	if permission.Outcome.Selected == nil || permission.Outcome.Selected.OptionId != "once" {
		t.Fatalf("permission = %#v", permission)
	}
	if err := conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: "s", Update: acp.UpdateAgentMessageText("hello")}); err != nil {
		t.Fatalf("SessionUpdate: %v", err)
	}
	if err := conn.NotifyExtension(ctx, "_hermes/test", map[string]any{"ok": true}); err != nil {
		t.Fatalf("NotifyExtension: %v", err)
	}
	resp, err := conn.UnstableCreateElicitation(ctx, acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{
			Message: "m",
			Mode:    "form",
			RequestedSchema: acp.UnstableElicitationSchema{
				Type: acp.UnstableElicitationSchemaTypeObject,
			},
		},
	})
	if err != nil {
		t.Fatalf("UnstableCreateElicitation: %v", err)
	}
	if resp.Accept == nil {
		t.Fatalf("elicitation resp = %#v", resp)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.updates != 1 || len(client.extensions) != 1 || client.elicitations != 1 {
		t.Fatalf("client state updates=%d extensions=%#v elicitations=%d", client.updates, client.extensions, client.elicitations)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %T: %v", value, err)
	}
	return data
}

type pipeACPClient struct {
	mu           sync.Mutex
	updates      int
	extensions   []string
	elicitations int
}

var _ acp.Client = (*pipeACPClient)(nil)
var _ acp.ClientExperimental = (*pipeACPClient)(nil)
var _ acp.ExtensionMethodHandler = (*pipeACPClient)(nil)

func (*pipeACPClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, nil
}

func (*pipeACPClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, nil
}

func (*pipeACPClient) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")}, nil
}

func (c *pipeACPClient) SessionUpdate(context.Context, acp.SessionNotification) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updates++
	return nil
}

func (*pipeACPClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{TerminalId: "terminal-1"}, nil
}

func (*pipeACPClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (*pipeACPClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (*pipeACPClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (*pipeACPClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func (*pipeACPClient) UnstableCompleteElicitation(context.Context, acp.UnstableCompleteElicitationNotification) error {
	return nil
}

func (c *pipeACPClient) UnstableCreateElicitation(context.Context, acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
	c.mu.Lock()
	c.elicitations++
	c.mu.Unlock()
	return acp.UnstableCreateElicitationResponse{
		Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{}},
	}, nil
}

func (*pipeACPClient) UnstableConnectMcp(context.Context, acp.UnstableConnectMcpRequest) (acp.UnstableConnectMcpResponse, error) {
	return acp.UnstableConnectMcpResponse{}, nil
}

func (*pipeACPClient) UnstableDisconnectMcp(context.Context, acp.UnstableDisconnectMcpRequest) (acp.UnstableDisconnectMcpResponse, error) {
	return acp.UnstableDisconnectMcpResponse{}, nil
}

func (c *pipeACPClient) HandleExtensionMethod(_ context.Context, method string, _ json.RawMessage) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.extensions = append(c.extensions, method)
	return map[string]any{"ok": true}, nil
}
