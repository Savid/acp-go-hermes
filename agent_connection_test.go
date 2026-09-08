package hermesacp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
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

// Exercise the real Serve transport: map decoding must not erase malformed
// lifecycle offers before the handshake validates its owned metadata.
func TestServeInitializeRejectsMalformedLifecycleOffer(t *testing.T) {
	for _, tc := range []struct {
		name, offer, field string
		options            []Option
		code               int
	}{
		{"duplicate version", `{"version":2,"version":1}`, ".version", nil, -32602},
		{"equal duplicate", `{"version":1,"version":1}`, ".version", nil, -32602},
		{"missing version", `{}`, ".version", nil, -32602},
		{"wrong version", `{"version":2}`, ".version", nil, -32602},
		{"rounded fraction", `{"version":1.0000000000000001}`, ".version", nil, -32602},
		{"fraction", `{"version":1.5}`, ".version", nil, -32602},
		{"decimal integer", `{"version":1.0}`, ".version", nil, -32602},
		{"exponent integer", `{"version":1e0}`, ".version", nil, -32602},
		{"string", `{"version":"1"}`, ".version", nil, -32602},
		{"boolean", `{"version":true}`, ".version", nil, -32602},
		{"non-object", `null`, "", nil, -32602},
		{"unknown", `{"version":1,"extra":true}`, ".extra", nil, -32602},
		{"construction precedence", `{"version":2,"version":1}`, "", []Option{WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1})}, -32603},
		{"construction overflow precedence", `{"version":1e400}`, "", []Option{WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1})}, -32603},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			input, writer := io.Pipe()
			reader, output := io.Pipe()
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, input, output, tc.options...) }()
			t.Cleanup(func() {
				_ = writer.Close()
				_ = reader.Close()
				_ = input.Close()
				_ = output.Close()
				cancel()
				<-done
			})
			request := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"_meta":{"acp-go.dev/lifecycle":` + tc.offer + `}}}`
			_, err := io.WriteString(writer, request+"\n")
			require.NoError(t, err)
			var response struct {
				Error *acp.RequestError `json:"error"`
			}
			require.NoError(t, json.NewDecoder(reader).Decode(&response))
			require.NotNil(t, response.Error)
			require.EqualValues(t, tc.code, response.Error.Code)
			if tc.code == -32602 {
				require.Equal(t, map[string]any{"error": "unsupported", "field": lifecycle.MetaPath + tc.field}, response.Error.Data)
			} else {
				data, ok := response.Error.Data.(map[string]any)
				require.True(t, ok)
				require.Equal(t, "hermes_invalid_options", data["error"])
			}
		})
	}
}

type signalBlockingReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.b.Write(value)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.b.String()
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
		{method: acp.AgentMethodSessionNew, params: mustJSON(t, NewSessionRequest(durableTempDir(t)))},
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
	fakeSession := testSession(t, agent, fakeClient)
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
				Cwd:        durableTempDir(t),
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
	lifecycle := localResponse[acp.CloseSessionRequest, *acp.CloseSessionRequest, acp.CloseSessionResponse](
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

func TestConnectionInputGateRejectsUnboundedOrIncompleteLifecycleFrames(t *testing.T) {
	t.Run("unterminated", func(t *testing.T) {
		body := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"secret-body"}}`
		gate := newConnectionInputGate(strings.NewReader(body))
		gate.open()

		_, err := gate.Read(make([]byte, 1))
		require.ErrorIs(t, err, errConnectionInputUnterminated)
		require.NotContains(t, err.Error(), "secret-body")
		require.Empty(t, gate.requestIDs)
		require.Zero(t, gate.nextToken)
	})

	t.Run("oversized before stamping", func(t *testing.T) {
		gate := newConnectionInputGate(strings.NewReader(strings.Repeat("x", connectionInputFrameLimit+1) + "\n"))
		gate.open()

		_, err := gate.Read(make([]byte, 1))
		require.ErrorIs(t, err, errConnectionInputOversized)
		require.Empty(t, gate.requestIDs)
		require.Zero(t, gate.nextToken)
	})

	t.Run("stamped frame cannot exceed SDK limit", func(t *testing.T) {
		prefix := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"padding":"`
		suffix := `"}}` + "\n"
		padding := connectionInputFrameLimit - len(prefix) - len(suffix)
		require.Positive(t, padding)
		gate := newConnectionInputGate(strings.NewReader(prefix + strings.Repeat("x", padding) + suffix))
		gate.open()

		_, err := gate.Read(make([]byte, 1))
		require.ErrorIs(t, err, errConnectionInputOversized)
		require.Empty(t, gate.requestIDs)
		_, claimed := gate.claimLifecycleRequest("lifecycle-request-1")
		require.False(t, claimed)
	})
}

func TestPrivateLifecycleConnectionBranchCoverage(t *testing.T) {
	gate := newConnectionInputGate(strings.NewReader(""))
	got, err := gate.stampLifecycleRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":null}`))
	if err != nil || !bytes.Contains(got, []byte(`"params":null`)) {
		t.Fatalf("null params were unexpectedly stamped: %s", got)
	}
	ctx := t.Context()
	raw := json.RawMessage(`{`)
	if (&localAgentConnection{}).bindLifecycleRequest(ctx, &raw) != ctx ||
		((*localAgentConnection)(nil)).bindLifecycleRequest(ctx, &raw) != ctx ||
		(&localAgentConnection{inputGate: gate}).bindLifecycleRequest(ctx, nil) != ctx {
		t.Fatal("unbound lifecycle request changed context")
	}
	if (&localAgentConnection{inputGate: gate}).bindLifecycleRequest(ctx, &raw) != ctx {
		t.Fatal("malformed lifecycle params changed context")
	}
	raw = json.RawMessage(`{}`)
	if (&localAgentConnection{inputGate: gate}).bindLifecycleRequest(ctx, &raw) != ctx {
		t.Fatal("marker-free lifecycle params changed context")
	}
	raw = json.RawMessage(`{"` + lifecycleRequestMarkerField + `":"missing"}`)
	if (&localAgentConnection{inputGate: gate}).bindLifecycleRequest(ctx, &raw) != ctx {
		t.Fatal("unknown lifecycle marker changed context")
	}

	logger := protocolSafeLogger(nil)
	handler := logger.Handler()
	_ = handler.WithAttrs([]slog.Attr{slog.String("ignored", "secret")})
	_ = handler.WithGroup("ignored")
	if protocolLogClassification("failed to parse incoming message") != "malformed_frame" ||
		protocolLogClassification("connection closed") != "connection_closed" ||
		protocolLogClassification("other") != "protocol_failure" {
		t.Fatal("protocol log classifications drifted")
	}

	identityCtx := context.WithValue(ctx, lifecycleRequestIdentityKey{}, lifecycleRequestIdentity{token: "token"})
	if got, err := markLifecycleResponse(ctx, map[string]any{"ok": true}); err != nil || reflect.TypeOf(got).Kind() != reflect.Map {
		t.Fatalf("unmarked response = %#v, %v", got, err)
	}
	if _, err := markLifecycleResponse(identityCtx, func() {}); err == nil {
		t.Fatal("unserializable lifecycle response was marked")
	}
	if _, err := markLifecycleResponse(identityCtx, "scalar"); err == nil {
		t.Fatal("scalar lifecycle response was marked")
	}

	written := make(chan error, 4)
	local := &localAgentConnection{agent: newTestAgent()}
	if _, err := local.CreateElicitationRegistered(ctx, acp.UnstableCreateElicitationRequest{}, elicitationScope{}, written); err == nil {
		t.Fatal("invalid elicitation reached client-call admission")
	}
	if <-written == nil {
		t.Fatal("invalid elicitation did not signal its write failure")
	}
	requestID := "request"
	validElicitation := acp.UnstableCreateElicitationRequest{Form: &acp.UnstableCreateElicitationForm{Message: "m"}}
	elicitationKey := hostControlRequestKey(acp.ClientMethodElicitationCreate, "session", requestID)
	blocker, registrationErr := local.registerOutboundWrite(elicitationKey, make(chan error, 1))
	require.NoError(t, registrationErr)
	if _, err := local.CreateElicitationRegistered(ctx, validElicitation, elicitationScope{
		SessionID: "session", TurnNonce: "turn", RequestID: &requestID,
	}, written); err == nil {
		t.Fatal("duplicate elicitation identity was registered")
	}
	if <-written == nil {
		t.Fatal("duplicate elicitation identity did not signal its write failure")
	}
	local.finishOutboundWrite(elicitationKey, blocker)

	local.agent.clientCalls <- struct{}{}
	if _, err := local.RequestPermissionRegistered(ctx, acp.RequestPermissionRequest{}, written); err == nil {
		t.Fatal("permission ignored client-call backpressure")
	}
	<-local.agent.clientCalls
	if <-written == nil {
		t.Fatal("permission backpressure did not signal its write failure")
	}

	signalHostWrite(nil, errors.New("ignored"))
	registration, registrationErr := local.registerOutboundWrite("", nil)
	require.NoError(t, registrationErr)
	local.finishOutboundWrite("", nil)
	local.finishOutboundWrite("missing", registration)
	if _, err := local.registerOutboundWrite("", written); err == nil {
		t.Fatal("empty outbound identity was registered")
	}
	first, registrationErr := local.registerOutboundWrite("duplicate", written)
	require.NoError(t, registrationErr)
	if _, err := local.registerOutboundWrite("duplicate", written); err == nil {
		t.Fatal("duplicate outbound identity was registered")
	}
	local.finishOutboundWrite("duplicate", first)

	if requestError(ctx, nil) != nil {
		t.Fatal("nil handler error produced a wire error")
	}
}

// TestRequestErrorPreservesTypedPayload pins the wire contract every
// structured refusal depends on: a typed RequestError reaches the peer with its
// own code and its own data map, so the field path a construction site named
// survives to the host. The previous behaviour rewrote every code to a single
// token and is exactly what this asserts is gone.
func TestRequestErrorPreservesTypedPayload(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		err  *acp.RequestError
	}{
		{"unsupported field", acp.NewInvalidParams(map[string]any{
			jsonFieldError: valUnsupported,
			jsonFieldField: hermesModelOptionPath,
		})},
		{"unknown session", acp.NewInvalidParams(map[string]any{
			jsonFieldError: valUnknownSession,
			jsonFieldField: jsonFieldSessionID,
		})},
		{"session closed", acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})},
		{"parse error", acp.NewParseError(map[string]any{jsonFieldError: "parse_error"})},
		{"auth required", acp.NewAuthRequired(map[string]any{jsonFieldError: "authentication_required"})},
		{"cancelled", acp.NewRequestCancelled(map[string]any{jsonFieldError: valRequestCancelled})},
		{"unclassified code", &acp.RequestError{Code: 123, Message: "custom", Data: map[string]any{jsonFieldError: "custom_token"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			mapped := requestError(context.Background(), test.err)
			require.NotNil(t, mapped)
			require.Equal(t, test.err.Code, mapped.Code)
			require.Equal(t, test.err.Data, mapped.Data)
		})
	}
}

// TestRequestErrorMethodNotFoundNamesTheMethod pins the -32601 shape: the
// adapter's own method-not-found carries the method the peer asked for, the
// same data the SDK dispatcher emits for a method it cannot route, so one
// connection never answers the same code with two different conventions.
func TestRequestErrorMethodNotFoundNamesTheMethod(t *testing.T) {
	t.Parallel()

	mapped := requestError(context.Background(), acp.NewMethodNotFound("_hermes/does/not/exist"))
	require.NotNil(t, mapped)
	require.Equal(t, -32601, mapped.Code)
	require.Equal(t, acp.NewMethodNotFound("_hermes/does/not/exist").Data, mapped.Data)
}

// TestRequestErrorReducesUnclassifiedProse pins the one case that is still
// reduced to a bare token: an error carrying no wire classification has prose
// this package cannot vouch for, so none of it reaches the peer.
func TestRequestErrorReducesUnclassifiedProse(t *testing.T) {
	t.Parallel()

	mapped := requestError(context.Background(), errors.New("native prose: /home/someone/secret path"))
	require.NotNil(t, mapped)
	require.Equal(t, -32603, mapped.Code)
	require.Equal(t, map[string]any{jsonFieldError: valHermesInternalFailure}, mapped.Data)
}

// TestRequestErrorCancelWinsOverTypedPayload keeps the cancel precedence the
// pass-through must not disturb: a withdrawn request answers -32800 even when
// the handler was carrying a typed refusal when the cancel landed.
func TestRequestErrorCancelWinsOverTypedPayload(t *testing.T) {
	t.Parallel()

	cancelledCtx, cancel := context.WithCancelCause(context.Background())
	cancel(context.Canceled)

	mapped := requestError(cancelledCtx, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported}))
	require.NotNil(t, mapped)
	require.Equal(t, -32800, mapped.Code)
	require.Equal(t, map[string]any{jsonFieldError: valRequestCancelled}, mapped.Data)
}

func TestExtensionForkResponseCarriesPrivateLifecycleToken(t *testing.T) {
	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	childClient := newFakeHermesClient()
	childClient.getSession = testNativeSession("native-child")
	agent := newTestAgent(WithScratchDir(durableTempDir(t)), WithSessionStore(NewInMemorySessionStore()))
	agent.options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
		childClient.xdg = start.ExistingXDG

		return childClient, nil
	}
	parent := testSession(t, agent, parentClient)
	parent.id = "parent"
	parent.idmap.SessionID = "parent"
	parent.idmap.NativeSessionID = "native-parent"
	agent.sessions[parent.id] = parent

	conn := &localAgentConnection{agent: agent}
	conn.initialized.Store(true)
	ctx := context.WithValue(t.Context(), lifecycleRequestIdentityKey{}, lifecycleRequestIdentity{
		token: "opaque-fork-token",
	})
	result, reqErr := conn.handle(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, durableTempDir(t))))
	require.Nil(t, reqErr)
	marked, ok := result.(map[string]json.RawMessage)
	require.True(t, ok)
	require.JSONEq(t, `"opaque-fork-token"`, string(marked[lifecycleRequestMarkerField]))
}

func TestProtocolBoundaryNeverLeaksMalformedPayloadsOrArbitraryErrors(t *testing.T) {
	const secret = "SECRET_SENTINEL"
	var logs lockedBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	agent := newTestAgent(WithLogger(logger))
	var output lockedBuffer
	conn := newLocalAgentConnection(agent, &output, strings.NewReader(secret+`{"jsonrpc":"2.0"}`))
	<-conn.Done()
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("protocol logger leaked malformed raw frame: %s", logs.String())
	}
	if strings.Contains(output.String(), secret) {
		t.Fatalf("protocol response leaked malformed raw frame: %s", output.String())
	}

	nativeCause := errors.New(secret)
	mapped := mapTurnFailure(fmt.Errorf("native transport: %w", nativeCause))
	if !errors.Is(mapped, ErrGatewayDisconnected) || !errors.Is(mapped, nativeCause) {
		t.Fatalf("embeddable mapping lost exact cause: %v", mapped)
	}
	wire := requestError(context.Background(), mapped)
	encoded, err := json.Marshal(wire)
	require.NoError(t, err)
	require.Contains(t, string(encoded), valHermesTurnFailed)
	require.Contains(t, string(encoded), string(nativehermes.CauseTransport))
	require.Contains(t, string(encoded), "native transport: "+secret, "turn failures preserve the native cause")

	// Unclassified off-prompt errors still carry only the closed token.
	encoded, err = json.Marshal(requestError(context.Background(), nativeCause))
	require.NoError(t, err)
	require.NotContains(t, string(encoded), secret)

	local := &localAgentConnection{agent: newTestAgent()}
	local.initialized.Store(true)
	_, requestErr := local.handle(context.Background(), acp.AgentMethodSessionPrompt,
		json.RawMessage(`{"sessionId":"`+secret+`","prompt":[{"type":"text","text":"`+secret+`"}]}`))
	if requestErr == nil {
		t.Fatal("malformed secret-bearing prompt unexpectedly succeeded")
	}
	encoded, err = json.Marshal(requestErr)
	require.NoError(t, err)
	if strings.Contains(string(encoded), secret) {
		t.Fatalf("prompt error leaked payload: %s", encoded)
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
		xdg, err := testGenerationXDG(durableTempDir(t))
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
	cwd := durableTempDir(t)
	writeJSONRPC(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":` + strconv.Quote(cwd) + `,"mcpServers":[]}}`)
	responseLine := readLine()
	if !strings.Contains(responseLine, `"id":2`) || !strings.Contains(responseLine, `"result"`) {
		t.Fatalf("session/new response line = %s", responseLine)
	}
	writeJSONRPC(`{"jsonrpc":"2.0","id":3,"method":"session/list","params":{}}`)
	line := readLine()
	if strings.Contains(line, "available_commands_update") || !strings.Contains(line, `"id":3`) {
		t.Fatalf("line before session/list barrier = %s", line)
	}
}

// TestLifecycleOpeningFollowsTheEstablishingResponseOverPipes drives the opening
// snapshot the way a host does: over the transport, on the request shape a real
// `session/new` carries — a populated `mcpServers` array and an object-valued
// `_meta` holding a foreign member. The owed snapshot is correlated with the
// session the handler built and with the frame the transport wrote, never with
// anything read back out of the request params, so params carrying members no
// string map can hold open their stream exactly as minimal params do. The
// establishing response leaves first and the snapshot follows it, which is the
// order the write barrier exists to produce.
func TestLifecycleOpeningFollowsTheEstablishingResponseOverPipes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	agent.options.clientFactory = func(_ context.Context, _ nativehermes.StartOptions) (nativehermes.Server, error) {
		client := newFakeHermesClient()
		xdg, err := testGenerationXDG(durableTempDir(t))
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
		_, err := io.WriteString(c2aW, payload+"\n")
		require.NoError(t, err, "write request")
	}
	readLine := func() string {
		t.Helper()
		select {
		case line := <-lines:
			return line
		case <-ctx.Done():
			require.FailNow(t, "timed out waiting for JSON-RPC line")

			return ""
		}
	}

	writeJSONRPC(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,` +
		`"_meta":{"acp-go.dev/lifecycle":{"version":1}}}}`)

	var negotiation struct {
		Result acp.InitializeResponse `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(readLine()), &negotiation), "decode initialize response")
	require.Contains(t, negotiation.Result.Meta, lifecycle.MetaKey, "the offer was answered")

	cwd := durableTempDir(t)
	writeJSONRPC(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":` + strconv.Quote(cwd) + `,` +
		`"mcpServers":[{"name":"docs","command":"/usr/bin/env","args":["mcp-docs","--stdio"],` +
		`"env":[{"name":"DOCS_TOKEN","value":"token"}]}],` +
		`"_meta":{"example.test/host":{"trace":"trace-1","depth":3}}}}`)

	var established struct {
		ID     int                    `json:"id"`
		Result acp.NewSessionResponse `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(readLine()), &established), "decode session/new response")
	require.Equal(t, 2, established.ID)
	require.NotEmpty(t, established.Result.SessionId)

	var opening struct {
		Method string                  `json:"method"`
		Params acp.SessionNotification `json:"params"`
	}
	require.NoError(t, json.Unmarshal([]byte(readLine()), &opening), "decode opening notification")
	require.Equal(t, acp.ClientMethodSessionUpdate, opening.Method)
	require.Equal(t, established.Result.SessionId, opening.Params.SessionId)

	envelope, ok := opening.Params.Meta[lifecycle.MetaKey].(map[string]any)
	require.True(t, ok, "the notification carries the lifecycle envelope")
	require.EqualValues(t, 1, envelope["sequence"], "the snapshot is the incarnation's first event")

	event, ok := envelope["event"].(map[string]any)
	require.True(t, ok, "the envelope carries its event")
	require.Equal(t, string(lifecycle.EventSnapshot), event["type"])
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

	client := &pipeACPClient{changed: make(chan struct{}, 1)}
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

func TestPermissionOperationUsesOneClientCallLeaseOverPipes(t *testing.T) {
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	t.Cleanup(func() {
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()
	})

	permissionEntered := make(chan struct{})
	actionPublished := make(chan struct{})
	client := &pipeACPClient{
		changed:           make(chan struct{}, 1),
		permissionEntered: permissionEntered,
		actionPublished:   actionPublished,
	}
	_ = acp.NewClientSideConnection(client, c2aW, a2cR)
	agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
	require.NoError(t, agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	}))
	conn := newLocalAgentConnection(agent, a2cW, c2aR)
	agent.setAgentClient(conn)

	native := newFakeHermesClient()
	session := testSession(t, agent, native)
	require.NoError(t, session.openLifecycleStream())
	turnCtx := beginTestControlTurn(t, session, t.Context(), "permission-turn")
	defer session.finishTurn()

	done := make(chan error, 1)
	go func() {
		done <- session.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission-1", "tool-1"))
	}()
	<-permissionEntered
	<-actionPublished
	require.NoError(t, <-done)

	client.mu.Lock()
	order := append([]string(nil), client.order...)
	client.mu.Unlock()
	// The connection dispatches a request on a goroutine of its own and queues
	// notifications for sequential handling, so a notification the agent wrote
	// before the request can be handled after it. What the lease proves is
	// that the announcement is published while the request is still pending —
	// RequestPermission above blocks until it arrives — and lands after it.
	permissionIndex := slices.Index(order, "permission")
	actionIndex := slices.Index(order, "action-pending")
	require.NotEqual(t, -1, permissionIndex, "order: %v", order)
	require.Greater(t, actionIndex, permissionIndex, "order: %v", order)
	require.Equal(t, "once", native.permissionReply(0).reply)
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
	changed      chan struct{}
	order        []string

	permissionEntered chan struct{}
	actionPublished   chan struct{}
}

// awaitNotifications waits until the client has handled the notifications the
// agent sent. The connection dispatches requests on goroutines of their own and
// queues notifications for sequential handling, so a request that has already
// answered proves nothing about a notification sent before it. Polling is what
// makes the assertion that follows about delivery rather than about scheduling.
func (c *pipeACPClient) awaitNotifications(t *testing.T, updates int, extensions int) {
	t.Helper()

	for {
		c.mu.Lock()
		gotUpdates, gotExtensions := c.updates, len(c.extensions)
		c.mu.Unlock()

		if gotUpdates >= updates && gotExtensions >= extensions {
			return
		}

		select {
		case <-c.changed:
		case <-t.Context().Done():
			t.Fatalf("handled updates=%d extensions=%d, want %d and %d", gotUpdates, gotExtensions, updates, extensions)
		}
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

func (c *pipeACPClient) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.order = append(c.order, "permission")
	entered := c.permissionEntered
	published := c.actionPublished
	c.mu.Unlock()
	if entered != nil {
		close(entered)
	}
	if published != nil {
		<-published
	}

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")}, nil
}

func (c *pipeACPClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	encoded, _ := json.Marshal(notification)
	label := "update"
	if bytes.Contains(encoded, []byte(`"action_update"`)) && bytes.Contains(encoded, []byte(`"pending"`)) {
		label = "action-pending"
	}
	c.mu.Lock()
	c.updates++
	c.order = append(c.order, label)
	published := c.actionPublished
	shouldPublish := published != nil && label == "action-pending"
	c.mu.Unlock()
	if shouldPublish {
		close(published)
	}
	c.signalChanged()

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
	c.extensions = append(c.extensions, method)
	c.mu.Unlock()
	c.signalChanged()

	return map[string]any{"ok": true}, nil
}

func (c *pipeACPClient) signalChanged() {
	if c.changed == nil {
		return
	}

	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// TestOffPromptInternalErrorVocabularyIsClosed drives every -32603 this adapter
// can produce outside a prompt turn and pins the family shape: one closed
// vendor-prefixed token in data.error, the JSON-RPC constant in message, an
// optional documented class or cause, and no Go or native prose anywhere in the
// payload.
func TestOffPromptInternalErrorVocabularyIsClosed(t *testing.T) {
	t.Parallel()

	refusedOptions := newTestAgent(WithInputHandoffRoot("relative/handoff"))
	require.Error(t, refusedOptions.optionsErr, "the prose stays on the agent's own error")

	poisonedFence := poisonWireError(poisonRuntimeFenceFailed)
	unavailable := poisonWireError(poisonContainmentIncomplete)

	for _, test := range []struct {
		name string
		err  error
		want map[string]any
	}{
		{
			"construction verdict",
			refusedOptions.optionsError(),
			map[string]any{jsonFieldError: valHermesInvalidOptions},
		},
		{
			"unreplayable store entry",
			restoreFailed(errors.New("unsupported hermes store format")),
			map[string]any{jsonFieldError: valHermesRestoreFailed},
		},
		{
			"un-containable runtime",
			unavailable,
			map[string]any{jsonFieldError: valHermesRuntimeUnavailable},
		},
		{
			"poisoned session",
			poisonedFence,
			map[string]any{jsonFieldError: valHermesSessionPoisoned, jsonFieldCause: poisonRuntimeFenceFailed},
		},
		{
			"turn-correlation invariant",
			routeInvalid("stale route turnNonce"),
			map[string]any{jsonFieldError: valHermesInternalFailure, keyClass: classRouteCorrelation},
		},
		{
			"unclassified handler failure",
			errors.New("native prose: /home/someone/secret path"),
			map[string]any{jsonFieldError: valHermesInternalFailure},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			mapped := requestError(context.Background(), test.err)
			require.NotNil(t, mapped)
			require.Equal(t, -32603, mapped.Code)
			require.Equal(t, "Internal error", mapped.Message)
			require.Equal(t, test.want, mapped.Data)

			encoded, err := json.Marshal(mapped.Data)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), `"message"`)
			require.NotContains(t, string(encoded), "prose")
			require.NotContains(t, string(encoded), "/home/someone")
		})
	}
}
