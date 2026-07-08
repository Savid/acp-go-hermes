package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// requireTurnFailure asserts err is the uniform hermes_turn_failed JSON-RPC
// error (-32603, since hermes advertises no auth methods) with the given cause
// and a message carrying the real native cause. It returns the decoded data map
// so callers can pin statusCode/providerCode.
func requireTurnFailure(t *testing.T, err error, cause turnFailureCause, wantMsgSubstr string) map[string]any {
	t.Helper()

	if err == nil {
		t.Fatalf("expected a hermes_turn_failed error, got nil")
	}

	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error type = %T, want *acp.RequestError (%v)", err, err)
	}

	if reqErr.Code != -32603 {
		t.Fatalf("turn failure code = %d, want -32603", reqErr.Code)
	}

	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("turn failure data = %#v, want map", reqErr.Data)
	}

	if data[jsonFieldError] != valHermesTurnFailed {
		t.Fatalf("turn failure error = %v, want %q", data[jsonFieldError], valHermesTurnFailed)
	}

	if data[jsonFieldCause] != string(cause) {
		t.Fatalf("turn failure cause = %v, want %q", data[jsonFieldCause], cause)
	}

	msg, _ := data[jsonFieldMessage].(string)
	if wantMsgSubstr != "" && !strings.Contains(msg, wantMsgSubstr) {
		t.Fatalf("turn failure message = %q, want substring %q", msg, wantMsgSubstr)
	}

	if msg == "" {
		t.Fatalf("turn failure message is empty (never a fixed placeholder is required)")
	}

	return data
}

func promptOnce(ctx context.Context, session *session, text string) (acp.PromptResponse, error) {
	return session.Prompt(ctx, acp.PromptRequest{
		SessionId: session.id,
		Prompt:    []acp.ContentBlock{acp.TextBlock(text)},
	})
}

// T1 — provider error → structured failure (native boundary = fake gateway).
func TestTurnFailureProviderErrorAtGatewayBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, tt := range []struct {
		name     string
		events   []nativehermes.Event
		message  string
		status   int
		provider string
	}{
		{
			name: "message.complete finish error (rate limit)",
			events: []nativehermes.Event{{
				Type:    evtMessageComplete,
				Payload: json.RawMessage(`{"finish":"error","error":{"message":"rate limited by upstream","statusCode":429,"providerCode":"rate_limit"}}`),
			}},
			message:  "rate limited by upstream",
			status:   429,
			provider: "rate_limit",
		},
		{
			name: "session.error event (auth)",
			events: []nativehermes.Event{{
				Type:    evtSessionError,
				Payload: json.RawMessage(`{"error":{"message":"invalid api key","statusCode":401,"providerCode":"auth_error"}}`),
			}},
			message:  "invalid api key",
			status:   401,
			provider: "auth_error",
		},
		{
			name: "session.error event (flat fields)",
			events: []nativehermes.Event{{
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

			_, err := server.SendMessage(ctx, "stored", hermesMessageRequest{Parts: []map[string]any{{"text": "hi"}}})

			var failure *turnFailureError
			if !errors.As(err, &failure) {
				t.Fatalf("SendMessage error = %v (%T), want *turnFailureError", err, err)
			}

			if failure.cause != causeProvider {
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

// T1 (prompt mapping) — a provider failure maps to the uniform error, carries
// statusCode/providerCode, and NEVER returns a PromptResponse/end_turn.
func TestTurnFailureProviderErrorMapsUniformly(t *testing.T) {
	client := newFakeHermesClient()
	client.sendMessage = func(_ context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		return nativeMessage{}, &turnFailureError{
			cause:        causeProvider,
			message:      "hermes assistant error: model overloaded",
			statusCode:   503,
			providerCode: "overloaded",
		}
	}

	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	resp, err := promptOnce(context.Background(), session, "hello")
	if resp.StopReason != "" {
		t.Fatalf("failed turn returned a stop reason %q, want none", resp.StopReason)
	}

	data := requireTurnFailure(t, err, causeProvider, "model overloaded")
	if data[jsonFieldStatusCode] != 503 {
		t.Fatalf("statusCode = %v, want 503", data[jsonFieldStatusCode])
	}

	if data[jsonFieldProviderCode] != "overloaded" {
		t.Fatalf("providerCode = %v, want overloaded", data[jsonFieldProviderCode])
	}
}

// turnFailureError.Error falls back to a cause-derived string only when no
// native message is available.
func TestTurnFailureErrorFallbackMessage(t *testing.T) {
	if got := (&turnFailureError{cause: causeProvider}).Error(); got != "provider turn failure" {
		t.Fatalf("empty-message Error() = %q, want %q", got, "provider turn failure")
	}
}

// reportGatewayDisconnect substitutes the stream-closed sentinel when the read
// loop closed without a specific error (a clean close).
func TestReportGatewayDisconnectNilCause(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")

	err := server.reportGatewayDisconnect(nil)

	var failure *turnFailureError
	if !errors.As(err, &failure) || failure.cause != causeTransport {
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

	_, err := server.SendMessage(ctx, "stored", hermesMessageRequest{Parts: []map[string]any{{"text": "hi"}}})
	if !isGatewayDisconnect(err) {
		t.Fatalf("abrupt-close error = %v, want a disconnect", err)
	}

	var failure *turnFailureError
	if !errors.As(err, &failure) || failure.cause != causeTransport {
		t.Fatalf("abrupt-close failure = %v, want transport turnFailureError", err)
	}

	if failure.message == errGatewayStreamClosed.Error() {
		t.Fatalf("abrupt disconnect surfaced the sentinel instead of the real read error")
	}
}

// A stream error observed via EventErrors while the turn is already cancelled
// stays cancelled: the cancel guard runs before all failure mapping.
func TestTurnFailureStreamErrorWhileCancelledStaysCancelled(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativeMessage{}, ctx.Err()
	}

	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := promptOnce(ctx, session, "hello")
		done <- struct {
			resp acp.PromptResponse
			err  error
		}{resp, err}
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("prompt did not start")
	}

	// Mark the turn cancelled without cancelling the turn context, so only the
	// EventErrors branch is ready when the stream error arrives.
	session.mu.Lock()
	session.cancelled = true
	session.mu.Unlock()

	client.errs <- streamError{err: errors.New("stream died mid cancel")}

	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("cancelled stream error returned a failure: %v", out.err)
		}

		if out.resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("stop reason = %q, want cancelled", out.resp.StopReason)
		}
	case <-ctx.Done():
		t.Fatal("prompt did not return")
	}
}

// T2 — a transport disconnect carries the real cause (never bare EOF / generic
// string) both at the gateway boundary and through the prompt loop.
func TestTurnFailureTransportRecoversCause(t *testing.T) {
	t.Run("gateway reportGatewayDisconnect carries real cause", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		server := newGatewayBackedHermesServer(t, fake, "")

		err := server.reportGatewayDisconnect(errors.New("read tcp 127.0.0.1: connection reset by peer"))
		if !isGatewayDisconnect(err) {
			t.Fatalf("reportGatewayDisconnect error is not a disconnect: %v", err)
		}

		var failure *turnFailureError
		if !errors.As(err, &failure) || failure.cause != causeTransport {
			t.Fatalf("disconnect failure = %v, want transport turnFailureError", err)
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
	})

	t.Run("prompt loop surfaces the real transport cause", func(t *testing.T) {
		client := newFakeHermesClient()
		started := make(chan struct{})
		client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
			close(started)
			<-ctx.Done()

			return nativeMessage{}, ctx.Err()
		}

		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := promptOnce(ctx, session, "hello")
			done <- err
		}()

		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("prompt did not start")
		}

		client.errs <- streamError{err: errors.New("read tcp: unexpected EOF from hermes serve")}
		select {
		case err := <-done:
			requireTurnFailure(t, err, causeTransport, "unexpected EOF from hermes serve")
		case <-ctx.Done():
			t.Fatal("prompt did not fail")
		}
	})
}

// T3 — a native failure leaves the session addressable and retriable, and a
// process_exit cause maps to the uniform error.
func TestTurnFailureLeavesSessionRetriable(t *testing.T) {
	client := newFakeHermesClient()
	attempt := 0
	client.sendMessage = func(_ context.Context, id string, _ hermesMessageRequest) (nativeMessage, error) {
		attempt++
		if attempt == 1 {
			return nativeMessage{}, &turnFailureError{
				cause:   causeProcessExit,
				message: "hermes serve exited: signal: killed (out of memory)",
			}
		}

		return nativeMessage{
			Info:  nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
			Parts: []nativePart{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "ok"}},
		}, nil
	}

	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	_, err := promptOnce(context.Background(), session, "first")
	requireTurnFailure(t, err, causeProcessExit, "signal: killed")

	// The session is neither poisoned nor removed: a follow-up prompt re-drives
	// the turn and succeeds, never returning the unknown-session error.
	resp, err := promptOnce(context.Background(), session, "retry")
	if err != nil {
		t.Fatalf("retry prompt after failure: %v", err)
	}

	if resp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("retry stop reason = %q, want end_turn", resp.StopReason)
	}
}

// T4 — one malformed gateway line is skipped without hanging or misreporting the
// turn: the internal client records it and the turn completes normally.
func TestTurnFailureMalformedLineNotFatal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fake := newFakeGatewayServer(t)
	fake.setPromptRawFrames("this is not json{")
	fake.setPromptEvents(nativehermes.Event{
		Type:    evtMessageComplete,
		Payload: json.RawMessage(`{"usage":{"total_tokens":3}}`),
	})
	server := newGatewayBackedHermesServer(t, fake, "openai/gpt-test")
	server.rememberGatewaySession("stored", "live-stored")

	message, err := server.SendMessage(ctx, "stored", hermesMessageRequest{Parts: []map[string]any{{"text": "hi"}}})
	if err != nil {
		t.Fatalf("malformed line was fatal to the turn: %v", err)
	}

	if message.Info.Finish != valStop || message.Info.Tokens.Total != 3 {
		t.Fatalf("turn did not complete cleanly after malformed line: %#v", message.Info)
	}
}

// T5 — a native error observed while the turn is cancelled maps to cancelled,
// never a turn-failure error (cancel guard runs before all failure mapping).
func TestTurnFailureCancelNotConflated(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	release := make(chan struct{})
	client.sendMessage = func(_ context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		close(started)
		<-release

		return nativeMessage{}, &turnFailureError{cause: causeProvider, message: "provider blew up mid-cancel"}
	}

	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := promptOnce(ctx, session, "hello")
		done <- struct {
			resp acp.PromptResponse
			err  error
		}{resp, err}
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("prompt did not start")
	}

	session.cancelTurn()
	close(release)

	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("cancelled turn returned an error: %v", out.err)
		}

		if out.resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("stop reason = %q, want cancelled", out.resp.StopReason)
		}
	case <-ctx.Done():
		t.Fatal("prompt did not return")
	}
}

// T6 — a turn deadline aborts the native turn and fails with cause timeout, NOT
// cancelled (WithTurnTimeout).
func TestTurnFailureTimeout(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativeMessage{}, ctx.Err()
	}

	conn := newRecordingAgentClient()
	agent := NewAgent(WithTurnTimeout(40 * time.Millisecond))
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := promptOnce(ctx, session, "hang")
	if resp.StopReason != "" {
		t.Fatalf("timed-out turn returned stop reason %q, want none", resp.StopReason)
	}

	requireTurnFailure(t, err, causeTimeout, "deadline")

	if client.abortCount() == 0 {
		t.Fatal("timeout did not abort the native turn")
	}
}

// rawEventPayload decodes the rawEvent notification params into its map form.
func rawEventPayload(t *testing.T, ext extensionNotification) map[string]any {
	t.Helper()

	if ext.method != RawEventMethod {
		t.Fatalf("extension method = %q, want %q", ext.method, RawEventMethod)
	}

	payload, ok := ext.params.(map[string]any)
	if !ok {
		t.Fatalf("rawEvent params = %#v, want map", ext.params)
	}

	return payload
}

func enabledRawSession(t *testing.T, agent *Agent, conn *recordingAgentClient, id acp.SessionId) *session {
	t.Helper()

	client := newFakeHermesClient()
	session := testSession(agent, client)
	session.id = id
	session.rawMessages = rawMessageConfig{enabled: true}
	agent.setAgentClient(conn)

	return session
}

func emitRaw(t *testing.T, session *session, raw string) {
	t.Helper()

	if err := session.handleEvent(context.Background(), hermesEvent{Type: "native.custom", Raw: json.RawMessage(raw)}); err != nil {
		t.Fatalf("handleEvent: %v", err)
	}
}

// Raw-event uniform test spec, cases 1-6.
func TestRawEventOversizeEmitsFixedMarker(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	session := enabledRawSession(t, agent, conn, "session-1")

	big := `{"blob":"` + strings.Repeat("x", 70000) + `"}`
	emitRaw(t, session, big)

	exts := conn.extensionsFor(RawEventMethod)
	if len(exts) != 1 {
		t.Fatalf("rawEvent notifications = %d, want exactly 1", len(exts))
	}

	payload := rawEventPayload(t, exts[0])
	if payload[jsonFieldSessionID] != session.id {
		t.Fatalf("marker sessionId = %v, want %v", payload[jsonFieldSessionID], session.id)
	}

	if payload[keySequence] != int64(1) {
		t.Fatalf("marker sequence = %v, want 1", payload[keySequence])
	}

	if payload[keySource] != valHermesServeSource {
		t.Fatalf("marker source = %v, want hermes-serve", payload[keySource])
	}

	event, ok := payload[keyEvent].(map[string]any)
	if !ok {
		t.Fatalf("marker event = %#v, want map", payload[keyEvent])
	}

	if event[rawEventKeyTruncated] != true || event[rawEventKeyReason] != rawEventReasonOversize {
		t.Fatalf("marker = %#v, want oversize", event)
	}

	if event[rawEventKeyMaxBytes] != rawEventMaxBytes {
		t.Fatalf("marker maxBytes = %v, want %d", event[rawEventKeyMaxBytes], rawEventMaxBytes)
	}

	size, ok := event[rawEventKeySizeBytes].(int)
	if !ok || size <= rawEventMaxBytes {
		t.Fatalf("marker sizeBytes = %v, want int > %d", event[rawEventKeySizeBytes], rawEventMaxBytes)
	}
}

func TestRawEventSequenceContiguousPerSession(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	session := enabledRawSession(t, agent, conn, "session-1")

	emitRaw(t, session, `{"n":1}`)
	emitRaw(t, session, `{"blob":"`+strings.Repeat("y", 70000)+`"}`) // oversized still consumes a sequence
	emitRaw(t, session, `{"n":3}`)

	exts := conn.extensionsFor(RawEventMethod)
	if len(exts) != 3 {
		t.Fatalf("rawEvent notifications = %d, want 3", len(exts))
	}

	for i, ext := range exts {
		payload := rawEventPayload(t, ext)
		if payload[keySequence] != int64(i+1) {
			t.Fatalf("sequence[%d] = %v, want %d", i, payload[keySequence], i+1)
		}
	}
}

func TestRawEventCrossSessionIsolation(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	sessionA := enabledRawSession(t, agent, conn, "session-A")
	sessionB := enabledRawSession(t, agent, conn, "session-B")

	emitRaw(t, sessionA, `{"a":1}`)
	emitRaw(t, sessionB, `{"b":1}`)
	emitRaw(t, sessionA, `{"a":2}`)
	emitRaw(t, sessionB, `{"b":2}`)

	perSession := map[acp.SessionId][]int64{}
	for _, ext := range conn.extensionsFor(RawEventMethod) {
		payload := rawEventPayload(t, ext)
		id, _ := payload[jsonFieldSessionID].(acp.SessionId)
		seq, _ := payload[keySequence].(int64)
		perSession[id] = append(perSession[id], seq)
	}

	for id, seqs := range perSession {
		want := []int64{1, 2}
		if len(seqs) != len(want) {
			t.Fatalf("session %q sequences = %v, want %v", id, seqs, want)
		}

		for i := range want {
			if seqs[i] != want[i] {
				t.Fatalf("session %q sequences = %v, want %v", id, seqs, want)
			}
		}
	}
}

func TestRawEventMarkerAlwaysValidJSON(t *testing.T) {
	oversize := capRawEventPayload(map[string]any{
		jsonFieldSessionID: "s",
		keySequence:        int64(1),
		keySource:          valHermesServeSource,
		keyEvent:           map[string]any{"blob": strings.Repeat("z", 70000)},
	})
	oversizeEvent, _ := oversize[keyEvent].(map[string]any)
	if oversizeEvent[rawEventKeyReason] != rawEventReasonOversize {
		t.Fatalf("oversize marker = %#v", oversizeEvent)
	}

	requireValidJSON(t, oversize)

	// An unserializable event (a channel cannot be marshalled) yields the
	// unserializable marker with no sizeBytes and still valid JSON.
	unserializable := capRawEventPayload(map[string]any{
		jsonFieldSessionID: "s",
		keySequence:        int64(2),
		keySource:          valHermesServeSource,
		keyEvent:           make(chan int),
	})
	marker, _ := unserializable[keyEvent].(map[string]any)
	if marker[rawEventKeyReason] != rawEventReasonUnserialized {
		t.Fatalf("unserializable marker = %#v", marker)
	}

	if _, present := marker[rawEventKeySizeBytes]; present {
		t.Fatalf("unserializable marker carries sizeBytes: %#v", marker)
	}

	requireValidJSON(t, map[string]any{
		jsonFieldSessionID: unserializable[jsonFieldSessionID],
		keySequence:        unserializable[keySequence],
		keySource:          unserializable[keySource],
		keyEvent:           marker,
	})
}

func requireValidJSON(t *testing.T, payload map[string]any) {
	t.Helper()

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marker payload failed to marshal: %v", err)
	}

	if !json.Valid(encoded) {
		t.Fatalf("marker payload is not valid JSON: %s", encoded)
	}
}

func TestRawEventEmitFailureDoesNotFailTurn(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	conn.notifyErr = errors.New("client notify boom")
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.rawMessages = rawMessageConfig{enabled: true}

	release := make(chan struct{})
	client.sendMessage = func(_ context.Context, id string, _ hermesMessageRequest) (nativeMessage, error) {
		<-release

		return nativeMessage{
			Info:  nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
			Parts: []nativePart{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "ok"}},
		}, nil
	}

	// Deliver a raw event mid-turn; its emit fails on the wire but must not
	// abort the authoritative turn.
	client.events <- hermesEvent{Type: "native.custom", Raw: json.RawMessage(`{"type":"native.custom"}`)}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := promptOnce(ctx, session, "hello")
		done <- struct {
			resp acp.PromptResponse
			err  error
		}{resp, err}
	}()

	// Wait until the raw event emit was attempted (recorded even though it
	// errored), then let the native turn complete.
	deadline := time.After(time.Second)
	for conn.extensionCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("raw event was never emitted")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	close(release)

	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("raw emit failure aborted the turn: %v", out.err)
		}

		if out.resp.StopReason != acp.StopReasonEndTurn {
			t.Fatalf("stop reason = %q, want end_turn", out.resp.StopReason)
		}
	case <-ctx.Done():
		t.Fatal("prompt did not return")
	}
}

func TestRawEventDefaultOffEmitsNothing(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client) // rawMessages disabled by default

	for i := 0; i < 5; i++ {
		emitRaw(t, session, `{"n":1}`)
	}

	if got := len(conn.extensionsFor(RawEventMethod)); got != 0 {
		t.Fatalf("rawEvent notifications with feature off = %d, want 0", got)
	}
}
