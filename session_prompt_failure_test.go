package hermesacp

import (
	"context"
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
func requireTurnFailure(t *testing.T, err error, cause nativehermes.TurnFailureCause, wantMsgSubstr string) map[string]any {
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

// T1 (prompt mapping) — a provider failure maps to the uniform error, carries
// statusCode/providerCode, and NEVER returns a PromptResponse/end_turn.
func TestTurnFailureProviderErrorMapsUniformly(t *testing.T) {
	client := newFakeHermesClient()
	client.sendMessage = func(_ context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{}, nativehermes.NewProviderTurnFailure("hermes assistant error: model overloaded", 503, "overloaded")
	}

	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	resp, err := promptOnce(context.Background(), session, "hello")
	if resp.StopReason != "" {
		t.Fatalf("failed turn returned a stop reason %q, want none", resp.StopReason)
	}

	data := requireTurnFailure(t, err, nativehermes.CauseProvider, "model overloaded")
	if data[jsonFieldStatusCode] != 503 {
		t.Fatalf("statusCode = %v, want 503", data[jsonFieldStatusCode])
	}

	if data[jsonFieldProviderCode] != "overloaded" {
		t.Fatalf("providerCode = %v, want overloaded", data[jsonFieldProviderCode])
	}
}

// nativehermes.TurnFailureError.Error falls back to a cause-derived string only when no
// native message is available.
func TestTurnFailureErrorFallbackMessage(t *testing.T) {
	if got := (nativehermes.NewTurnFailure(nativehermes.CauseProvider, "")).Error(); got != "provider turn failure" {
		t.Fatalf("empty-message Error() = %q, want %q", got, "provider turn failure")
	}
}

// A stream error observed via EventErrors while the turn is already cancelled
// stays cancelled: the cancel guard runs before all failure mapping.
func TestTurnFailureStreamErrorWhileCancelledStaysCancelled(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
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

	client.errs <- nativehermes.NewStreamError(0, errors.New("stream died mid cancel"))

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

// T3 — a native failure leaves the session addressable and retriable: the
// classified failure maps to the uniform error, yet the session is neither
// poisoned nor removed and a follow-up prompt re-drives the turn.
func TestTurnFailureLeavesSessionRetriable(t *testing.T) {
	client := newFakeHermesClient()
	attempt := 0
	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		attempt++
		if attempt == 1 {
			return nativehermes.NativeMessage{}, nativehermes.NewTurnFailure(nativehermes.CauseTransport, "hermes gateway disconnected: unexpected EOF")
		}

		return nativehermes.NativeMessage{
			Info:  nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
			Parts: []nativehermes.Part{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "ok"}},
		}, nil
	}

	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	_, err := promptOnce(context.Background(), session, "first")
	requireTurnFailure(t, err, nativehermes.CauseTransport, "unexpected EOF")

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

// T5 — a native error observed while the turn is cancelled maps to cancelled,
// never a turn-failure error (cancel guard runs before all failure mapping).
func TestTurnFailureCancelNotConflated(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	release := make(chan struct{})
	client.sendMessage = func(_ context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-release

		return nativehermes.NativeMessage{}, nativehermes.NewTurnFailure(nativehermes.CauseProvider, "provider blew up mid-cancel")
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
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
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

	requireTurnFailure(t, err, nativehermes.CauseTimeout, "deadline")

	if client.abortCount() == 0 {
		t.Fatal("timeout did not abort the native turn")
	}
}

// T6b — when a user cancel and the WithTurnTimeout expiry coincide, the cancel
// guard wins deterministically: the result is StopReason cancelled, never cause
// timeout, and the native turn is aborted exactly once (no double-send).
func TestTurnTimeoutCoincidesWithCancelYieldsCancelled(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}

	conn := newRecordingAgentClient()
	agent := NewAgent(WithTurnTimeout(40 * time.Millisecond))
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := promptOnce(ctx, session, "hang")
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

	// Mark the turn cancelled without cancelling the turn context, so when the
	// deadline fires only the timeout branch is ready and it observes an active
	// cancel — the coincident case the cancel guard must resolve to cancelled.
	session.mu.Lock()
	session.cancelled = true
	session.mu.Unlock()

	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("coincident cancel+timeout returned a failure: %v", out.err)
		}

		if out.resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("stop reason = %q, want cancelled", out.resp.StopReason)
		}
	case <-ctx.Done():
		t.Fatal("prompt did not return")
	}

	if got := client.abortCount(); got != 1 {
		t.Fatalf("native turn abort count = %d, want exactly 1 (no double-send)", got)
	}
}

// T2 — a transport disconnect surfaces the real cause through the prompt loop:
// a stream error mid-turn maps to the uniform transport turn failure.
func TestTurnFailureTransportRecoversCause(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
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

	client.errs <- nativehermes.NewStreamError(0, errors.New("read tcp: unexpected EOF from hermes serve"))
	select {
	case err := <-done:
		requireTurnFailure(t, err, nativehermes.CauseTransport, "unexpected EOF from hermes serve")
	case <-ctx.Done():
		t.Fatal("prompt did not fail")
	}
}
