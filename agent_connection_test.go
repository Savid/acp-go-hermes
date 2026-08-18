package hermesacp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

func TestServeContextAndInputDone(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Serve(cancelled, strings.NewReader(""), io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve canceled error = %v", err)
	}
	startCtx, startCancel := context.WithCancel(context.Background())
	blocking := &signalBlockingReader{started: make(chan struct{}), release: make(chan struct{})}
	startDone := make(chan error, 1)
	go func() {
		startDone <- Serve(startCtx, blocking, io.Discard)
	}()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("Serve reader did not start")
	}
	startCancel()
	select {
	case err := <-startDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve started cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after started context cancellation")
	}
	close(blocking.release)

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

type signalBlockingReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *signalBlockingReader) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release

	return 0, io.EOF
}

func TestLocalAgentConnectionHandleRoutesAndErrors(t *testing.T) {
	ctx := context.Background()
	agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1, MaxConcurrentClientCalls: 1}))
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
	initTyped, initOK := initResp.(acp.InitializeResponse)
	if !initOK || initTyped.AgentCapabilities.PositionEncoding == nil {
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
	closedAgent := newTestAgent()
	if err := closedAgent.Close(); err != nil {
		t.Fatalf("close agent: %v", err)
	}
	closedConn := &localAgentConnection{agent: closedAgent}
	closedConn.initialized.Store(true)
	for _, request := range []struct {
		method string
		params json.RawMessage
	}{
		{method: acp.AgentMethodSessionNew, params: mustJSON(t, NewSessionRequest(t.TempDir()))},
		{method: acp.AgentMethodSessionPrompt, params: json.RawMessage(`{`)},
		{method: "missing/method", params: json.RawMessage(`{}`)},
		{method: "_missing/method", params: json.RawMessage(`{`)},
	} {
		if _, reqErr := closedConn.handle(ctx, request.method, request.params); reqErr == nil || reqErr.Code != -32600 {
			t.Fatalf("closed dispatch %q reqErr = %#v, want -32600", request.method, reqErr)
		}
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
	// A cancel carrying no route envelope fails closed at the agent method; the
	// notification has no response frame, so the refusal is wire-silent and the
	// connection surfaces it for the SDK to log.
	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionCancel, mustJSON(t, acp.CancelNotification{SessionId: fakeSession.id})); reqErr == nil {
		t.Fatal("unrouted cancel notification was applied")
	}
	fakeSession.mu.Lock()
	fakeSession.turnNonce = "conn-turn"
	fakeSession.turnEpoch = 1
	fakeSession.turnInFlight = true
	fakeSession.mu.Unlock()
	routed := mustJSON(t, map[string]any{"sessionId": fakeSession.id, "_meta": turnRouteMeta("conn-turn")})
	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionCancel, routed); reqErr != nil {
		t.Fatalf("cancel notification reqErr = %#v", reqErr)
	}
	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionSetMode, mustJSON(t, acp.SetSessionModeRequest{})); reqErr == nil || reqErr.Code != -32601 {
		t.Fatalf("set mode reqErr = %#v", reqErr)
	}
}

func TestLocalAgentConnectionClientCallErrors(t *testing.T) {
	agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1, MaxConcurrentClientCalls: 1}))
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

// TestLocalAgentConnectionHandleThreadsTheRequestContext proves every dispatch
// path hands requestError the live request context rather than a detached one:
// each of these failures answers with its own code on an uncancelled context
// and with -32800 once the request has been cancelled.
func TestLocalAgentConnectionHandleThreadsTheRequestContext(t *testing.T) {
	closedAgent := newTestAgent()
	if err := closedAgent.Close(); err != nil {
		t.Fatalf("close agent: %v", err)
	}
	closedConn := &localAgentConnection{agent: closedAgent}
	closedConn.initialized.Store(true)

	agent := newTestAgent()
	conn := &localAgentConnection{agent: agent}
	conn.initialized.Store(true)

	for name, test := range map[string]struct {
		conn   *localAgentConnection
		method string
		params json.RawMessage
	}{
		"closed agent": {
			conn:   closedConn,
			method: acp.AgentMethodSessionList,
			params: mustJSON(t, acp.ListSessionsRequest{}),
		},
		"extension method": {
			conn:   conn,
			method: ForkSessionMethod,
			params: mustJSON(t, acp.UnstableForkSessionRequest{}),
		},
		"response handler": {
			conn:   conn,
			method: acp.AgentMethodSessionSetMode,
			params: mustJSON(t, acp.SetSessionModeRequest{}),
		},
		"lifecycle handler": {
			conn:   conn,
			method: acp.AgentMethodSessionLoad,
			params: mustJSON(t, acp.LoadSessionRequest{
				SessionId:  "missing",
				Cwd:        t.TempDir(),
				McpServers: []acp.McpServer{},
			}),
		},
		"notification handler": {
			conn:   conn,
			method: acp.AgentMethodSessionCancel,
			params: mustJSON(t, acp.CancelNotification{SessionId: "missing"}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, live := test.conn.handle(t.Context(), test.method, test.params)
			if live == nil || live.Code == -32800 {
				t.Fatalf("uncancelled dispatch reqErr = %#v", live)
			}

			cancelled, cancel := context.WithCancelCause(context.Background())
			cancel(context.Canceled)

			_, reqErr := test.conn.handle(cancelled, test.method, test.params)
			if reqErr == nil || reqErr.Code != -32800 {
				t.Fatalf("cancelled dispatch reqErr = %#v, want -32800", reqErr)
			}
		})
	}
}

// TestRequestErrorCancelPrecedence pins which signal decides -32800. Only an
// honored $/cancel_request cancels a request context with cause
// context.Canceled, and it outranks whatever error the handler was carrying; a
// connection teardown or an adapter deadline carries a different cause and must
// not be reported as a cancellation even when the error itself wraps
// context.Canceled.
func TestRequestErrorCancelPrecedence(t *testing.T) {
	invalidParams := acp.NewInvalidParams(map[string]any{"x": "y"})

	for name, test := range map[string]struct {
		cause    error
		err      error
		wantCode int
		wantSame bool
	}{
		"honored cancel outranks a request error": {
			cause:    context.Canceled,
			err:      invalidParams,
			wantCode: -32800,
		},
		"honored cancel with a plain error": {
			cause:    context.Canceled,
			err:      context.Canceled,
			wantCode: -32800,
		},
		"connection teardown is not a cancellation": {
			cause:    errors.New("connection closed"),
			err:      fmt.Errorf("write update: %w", context.Canceled),
			wantCode: -32603,
		},
		"live request keeps its request error": {
			err:      invalidParams,
			wantCode: -32602,
			wantSame: true,
		},
		"live request wraps an opaque failure": {
			err:      errors.New("plain"),
			wantCode: -32603,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(errors.New("test cleanup"))

			if test.cause != nil {
				cancel(test.cause)
			}

			got := requestError(ctx, test.err)
			if got == nil || got.Code != test.wantCode {
				t.Fatalf("requestError = %#v, want code %d", got, test.wantCode)
			}
			if test.wantSame && got != invalidParams {
				t.Fatalf("requestError = %#v, want the original request error", got)
			}
		})
	}

	t.Run("adapter deadline is an internal failure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()

		<-ctx.Done()

		if got := requestError(ctx, context.DeadlineExceeded); got == nil || got.Code != -32603 {
			t.Fatalf("requestError deadline = %#v", got)
		}
	})

	if got := requestError(context.Background(), nil); got != nil {
		t.Fatalf("requestError(nil) = %#v", got)
	}
}

func TestCapabilityAndElicitationHelpers(t *testing.T) {
	lifecycle := localLifecycleResponse[acp.CloseSessionRequest, *acp.CloseSessionRequest, acp.CloseSessionResponse](
		func(*Agent, context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
			return acp.CloseSessionResponse{}, errors.New("close failed")
		},
	)
	if _, lifecycleErr := lifecycle(t.Context(), newTestAgent(), json.RawMessage(`{"sessionId":"s"}`)); lifecycleErr == nil {
		t.Fatal("local lifecycle response ignored agent error")
	}
	requestID := "request"
	urlParams, err := scopedElicitationParams(acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{
			ElicitationId: "e",
			Message:       "open",
			Url:           "https://example.test",
			Meta:          map[string]any{"k": "v"},
		},
	}, elicitationScope{SessionID: "s", TurnNonce: "turn-1", RequestID: &requestID})
	if err != nil || !strings.Contains(string(urlParams), `"acp-go.dev/route":{"requestId":"request","sessionId":"s","turnNonce":"turn-1","version":1}`) {
		t.Fatalf("url scoped elicitation = %s err=%v", urlParams, err)
	}
	if _, err := scopedElicitationParams(acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{Meta: map[string]any{routeMetaKey: map[string]any{}}},
	}, elicitationScope{SessionID: "s", TurnNonce: "turn-1"}); err == nil {
		t.Fatal("scoped elicitation accepted reserved route metadata")
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
	if got := selectPositionEncoding([]acp.PositionEncodingKind{"bad", acp.PositionEncodingKindUtf32}); got != acp.PositionEncodingKindUtf16 {
		t.Fatalf("utf32 must never be selected, got %q", got)
	}

	var bothModesNull acp.ElicitationCapabilities
	if err := json.Unmarshal([]byte(`{"form":null,"url":null}`), &bothModesNull); err != nil {
		t.Fatalf("decode explicit-null elicitation capabilities: %v", err)
	}

	for _, tt := range []struct {
		name     string
		caps     *acp.ElicitationCapabilities
		wantForm bool
		wantURL  bool
	}{
		{name: "nil", caps: nil, wantForm: false, wantURL: false},
		{name: "empty object", caps: &acp.ElicitationCapabilities{}, wantForm: false, wantURL: false},
		{name: "both modes null", caps: &bothModesNull, wantForm: false, wantURL: false},
		{name: "url only", caps: &acp.ElicitationCapabilities{Url: &acp.ElicitationUrlCapabilities{}}, wantForm: false, wantURL: true},
		{name: "form explicit", caps: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}, wantForm: true, wantURL: false},
		{name: "form and url", caps: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}, Url: &acp.ElicitationUrlCapabilities{}}, wantForm: true, wantURL: true},
	} {
		t.Run("elicitation "+tt.name, func(t *testing.T) {
			agent := newTestAgent()
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
	conn := newLocalAgentConnection(newTestAgent(), &output, strings.NewReader(""))
	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("local connection did not close after EOF input")
	}
}

func TestLifecycleDoesNotEmitAvailableCommandsUpdate(t *testing.T) {
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

	agent := newTestAgent()
	agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
		client := newFakeHermesClient()
		xdg, err := nativehermes.CreateXDGDirs(t.TempDir(), string(opts.ACPSessionID))
		if err != nil {
			return nil, err
		}
		client.xdg = xdg
		client.createSession = testNativeSession("native-1")

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
	select {
	case line := <-lines:
		if strings.Contains(line, "available_commands_update") {
			t.Fatalf("unexpected command update line = %s", line)
		}
	case <-time.After(50 * time.Millisecond):
	}
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
	agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 2}))
	conn := newLocalAgentConnection(agent, a2cW, c2aR)
	agent.setAgentClient(conn)

	permission, permErr := conn.RequestPermission(ctx, acp.RequestPermissionRequest{Options: []acp.PermissionOption{
		{OptionId: "once", Kind: acp.PermissionOptionKindAllowOnce, Name: "Allow once"},
	}})
	if permErr != nil {
		t.Fatalf("RequestPermission: %v", permErr)
	}
	if permission.Outcome.Selected == nil || permission.Outcome.Selected.OptionId != "once" {
		t.Fatalf("permission = %#v", permission)
	}
	if err2 := conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: "s", Update: acp.UpdateAgentMessageText("hello")}); err2 != nil {
		t.Fatalf("SessionUpdate: %v", err2)
	}
	if err3 := conn.NotifyExtension(ctx, "_hermes/test", map[string]any{"ok": true}); err3 != nil {
		t.Fatalf("NotifyExtension: %v", err3)
	}
	resp, err := conn.CreateElicitation(ctx, acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{
			Message: "m",
			Mode:    "form",
			RequestedSchema: acp.UnstableElicitationSchema{
				Type: acp.UnstableElicitationSchemaTypeObject,
			},
		},
	}, elicitationScope{SessionID: "s", TurnNonce: "turn-1", ToolCallID: "tool-1"})
	if err != nil {
		t.Fatalf("UnstableCreateElicitation: %v", err)
	}
	if resp.Accept == nil {
		t.Fatalf("elicitation resp = %#v", resp)
	}
	requestID := "request-1"
	if _, err := conn.CreateElicitation(ctx, acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{
			ElicitationId: "e1",
			Message:       "open",
			Mode:          "url",
			Url:           "https://example.test",
			Meta:          map[string]any{"url-meta": "kept"},
		},
	}, elicitationScope{SessionID: "s", TurnNonce: "turn-2", RequestID: &requestID}); err != nil {
		t.Fatalf("URL CreateElicitation: %v", err)
	}
	if _, err := conn.UnstableCreateElicitation(ctx, acp.UnstableCreateElicitationRequest{}); err == nil {
		t.Fatal("unscoped elicitation unexpectedly succeeded")
	}
	client.awaitNotifications(t, 1, 1)
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.updates != 1 || len(client.extensions) != 1 || len(client.elicitations) != 2 {
		t.Fatalf("client state updates=%d extensions=%#v elicitations=%d", client.updates, client.extensions, len(client.elicitations))
	}
	wantFormMeta := map[string]any{routeMetaKey: map[string]any{
		routeFieldVer:  float64(1),
		routeFieldID:   "s",
		routeFieldTurn: "turn-1",
		"toolCallId":   "tool-1",
	}}
	if !reflect.DeepEqual(client.elicitations[0].Form.Meta, wantFormMeta) {
		t.Fatalf("decoded form route meta = %#v, want %#v", client.elicitations[0].Form.Meta, wantFormMeta)
	}
	wantURLMeta := map[string]any{
		"url-meta": "kept",
		routeMetaKey: map[string]any{
			routeFieldVer:  float64(1),
			routeFieldID:   "s",
			routeFieldTurn: "turn-2",
			"requestId":    "request-1",
		},
	}
	if !reflect.DeepEqual(client.elicitations[1].Url.Meta, wantURLMeta) {
		t.Fatalf("decoded URL route meta = %#v, want %#v", client.elicitations[1].Url.Meta, wantURLMeta)
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
	elicitations []acp.UnstableCreateElicitationRequest
}

// awaitNotifications waits until the client has handled the notifications the
// agent sent. The connection dispatches requests on goroutines of their own and
// queues notifications for sequential handling, so a request that has already
// answered proves nothing about a notification sent before it. Polling is what
// makes the assertion that follows about delivery rather than about scheduling.
func (c *pipeACPClient) awaitNotifications(t *testing.T, updates int, extensions int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		c.mu.Lock()
		gotUpdates, gotExtensions := c.updates, len(c.extensions)
		c.mu.Unlock()

		if gotUpdates >= updates && gotExtensions >= extensions {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("handled updates=%d extensions=%d, want %d and %d", gotUpdates, gotExtensions, updates, extensions)
		}

		time.Sleep(time.Millisecond)
	}
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

func (c *pipeACPClient) UnstableCreateElicitation(_ context.Context, request acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
	c.mu.Lock()
	c.elicitations = append(c.elicitations, request)
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
