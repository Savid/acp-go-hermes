package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestQuestionToolElicitationAcceptDeclineAndNoCapability(t *testing.T) {
	ctx := context.Background()

	t.Run("accept replies to native question", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitation = acp.UnstableCreateElicitationResponse{
			Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{
				"question_1": "Yes",
				"question_2": []any{"Red", "Blue"},
			}},
		}
		agent := NewAgent()
		agent.setAgentClient(conn)
		if _, err := agent.Initialize(ctx, acp.InitializeRequest{ClientCapabilities: acp.ClientCapabilities{
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}},
		}}); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		session := testSession(agent, client)

		req := questionRequest{
			ID:        "question-1",
			SessionID: "native-1",
			Tool:      questionTool{MessageID: "message-1", CallID: "call-1"},
			Questions: []questionInfo{
				{
					Question: "Proceed?",
					Header:   "Decision",
					Options: []questionOption{
						{Label: "Yes", Description: "Continue"},
						{Label: "No"},
					},
				},
				{
					Question: "Colors?",
					Header:   "Palette",
					Multiple: true,
					Options: []questionOption{
						{Label: "Red"},
						{Label: "Blue"},
					},
				},
			},
		}
		if err := session.handleQuestion(ctx, req); err != nil {
			t.Fatalf("handleQuestion: %v", err)
		}
		if len(conn.elicitations) != 1 {
			t.Fatalf("elicitations = %d, want 1", len(conn.elicitations))
		}
		got := conn.elicitations[0]
		if got.Form == nil || got.Form.Mode != "form" || got.Form.Message != "Hermes needs input" {
			t.Fatalf("elicitation form = %#v", got.Form)
		}
		if conn.scopes[0].SessionID != session.id || conn.scopes[0].ToolCallID != "call-1" {
			t.Fatalf("scope = %#v", conn.scopes[0])
		}
		if len(got.Form.RequestedSchema.Required) != 2 {
			t.Fatalf("schema required = %#v", got.Form.RequestedSchema.Required)
		}
		reply := client.questionReply(0)
		if reply.sessionID != "native-1" || reply.requestID != "question-1" {
			t.Fatalf("reply target = %#v", reply)
		}
		if !reflect.DeepEqual(reply.answers, [][]string{{"Yes"}, {"Red", "Blue"}}) {
			t.Fatalf("answers = %#v", reply.answers)
		}
	})

	t.Run("decline rejects native question", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitation = acp.NewUnstableCreateElicitationResponseDecline()
		agent := NewAgent()
		agent.setAgentClient(conn)
		if _, err := agent.Initialize(ctx, acp.InitializeRequest{ClientCapabilities: acp.ClientCapabilities{
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}},
		}}); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		session := testSession(agent, client)
		if err := session.handleQuestion(ctx, questionRequest{ID: "q", SessionID: "native-1"}); err != nil {
			t.Fatalf("handleQuestion: %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %d, want 1", client.questionRejectCount())
		}
	})

	t.Run("missing capability rejects without ACP request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		if err := session.handleQuestion(ctx, questionRequest{ID: "q", SessionID: "native-1"}); err != nil {
			t.Fatalf("handleQuestion: %v", err)
		}
		if len(conn.elicitations) != 0 {
			t.Fatalf("elicitation sent without capability: %#v", conn.elicitations)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %d, want 1", client.questionRejectCount())
		}
	})
}

func TestQuestionToolReconcileAndCancelRejectsPending(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.pendingQuestions = []questionRequest{
		{ID: "foreign", SessionID: "other"},
		{ID: "q1", SessionID: "native-1"},
	}
	agent := NewAgent()
	session := testSession(agent, client)
	if err := session.reconcileQuestions(ctx); err != nil {
		t.Fatalf("reconcileQuestions: %v", err)
	}
	if client.questionRejectCount() != 1 {
		t.Fatalf("question rejects = %d, want 1", client.questionRejectCount())
	}

	turnCtx := session.beginTurn(ctx)
	session.mu.Lock()
	session.questions["q2"] = questionRequest{ID: "q2", SessionID: "native-1"}
	session.pending["p1"] = permissionRequest{ID: "p1", SessionID: "native-1"}
	session.mu.Unlock()
	session.cancelTurn()
	if turnCtx.Err() == nil {
		t.Fatal("turn context was not cancelled")
	}
	if client.questionRejectCount() != 2 {
		t.Fatalf("question rejects after cancel = %d, want 2", client.questionRejectCount())
	}
	reply := client.permissionReply(0)
	if reply.reply != "reject" || reply.message != "cancelled" {
		t.Fatalf("permission cancel reply = %#v", reply)
	}
	session.finishTurn()
}

func TestPermissionV2AskReplyReconcileAndCancelled(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	if err := session.handlePermission(ctx, permissionRequest{
		ID:        "perm-1",
		SessionID: "native-1",
		Action:    "edit",
		Resources: []string{"file.txt"},
		Metadata:  map[string]any{"path": "file.txt"},
	}); err != nil {
		t.Fatalf("handlePermission: %v", err)
	}
	reply := client.permissionReply(0)
	if reply.reply != "once" || reply.sessionID != "native-1" || reply.requestID != "perm-1" {
		t.Fatalf("permission reply = %#v", reply)
	}
	if len(conn.permissions) != 1 || conn.permissions[0].ToolCall.Title == nil || *conn.permissions[0].ToolCall.Title != "edit" {
		t.Fatalf("permission request = %#v", conn.permissions)
	}

	conn.permission = acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}
	if err := session.handlePermission(ctx, permissionRequest{ID: "perm-2", SessionID: "native-1"}); err != nil {
		t.Fatalf("handlePermission cancelled: %v", err)
	}
	if got := client.permissionReply(1).reply; got != "reject" {
		t.Fatalf("cancelled reply = %q, want reject", got)
	}

	client.pendingPermissions = []permissionRequest{
		{ID: "foreign", SessionID: "other"},
		{ID: "perm-3", SessionID: "native-1"},
	}
	if err := session.reconcilePermissions(ctx); err != nil {
		t.Fatalf("reconcilePermissions: %v", err)
	}
	if got := client.permissionReply(2).requestID; got != "perm-3" {
		t.Fatalf("reconciled request id = %q", got)
	}
}

func TestPermissionQuestionDuplicateRequestIDsAreFenced(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	if err := session.handleEvent(ctx, hermesEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{"id":"perm-dup","sessionID":"native-1","action":"edit"}`),
	}); err != nil {
		t.Fatalf("permission event: %v", err)
	}
	client.pendingPermissions = []permissionRequest{{ID: "perm-dup", SessionID: "native-1", Action: "edit"}}
	if err := session.reconcilePermissions(ctx); err != nil {
		t.Fatalf("permission reconcile: %v", err)
	}
	if conn.permissionRequestCount() != 1 || client.permissionReplyCount() != 1 {
		t.Fatalf("duplicate permission was not fenced requests=%d replies=%d", conn.permissionRequestCount(), client.permissionReplyCount())
	}

	if err := session.handleEvent(ctx, hermesEvent{
		Type:       "clarify.request",
		Properties: json.RawMessage(`{"id":"question-dup","sessionID":"native-1","questions":[{"question":"Continue?"}]}`),
	}); err != nil {
		t.Fatalf("question event: %v", err)
	}
	client.pendingQuestions = []questionRequest{{ID: "question-dup", SessionID: "native-1"}}
	if err := session.reconcileQuestions(ctx); err != nil {
		t.Fatalf("question reconcile: %v", err)
	}
	if len(conn.elicitations) != 1 || client.questionReplyCount() != 1 {
		t.Fatalf("duplicate question was not fenced elicitations=%d replies=%d", len(conn.elicitations), client.questionReplyCount())
	}
}

func TestEventMappingMessagePartToolTodoUsageAndRaw(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.rawMessages = rawMessageConfig{enabled: true}

	todoProps := json.RawMessage(`{"sessionID":"native-1","todos":[{"content":"Ship it","status":"in_progress","priority":"high"}]}`)
	if err := session.handleEvent(ctx, eventFromJSON(t, `{"type":"todo.updated","properties":`+string(todoProps)+`}`)); err != nil {
		t.Fatalf("todo event: %v", err)
	}
	textProps := json.RawMessage(`{"id":"part-1","sessionID":"native-1","messageID":"message-1","type":"text","text":"hello"}`)
	if err := session.handleEvent(ctx, hermesEvent{Type: "message.part.created", Properties: textProps, Raw: json.RawMessage(`{"type":"message.part.created"}`)}); err != nil {
		t.Fatalf("text event: %v", err)
	}
	if err := session.handleEvent(ctx, hermesEvent{Type: "message.part.created", Properties: textProps}); err != nil {
		t.Fatalf("duplicate text event: %v", err)
	}
	reasoningProps := json.RawMessage(`{"part":{"id":"part-2","sessionID":"native-1","messageID":"message-1","type":"reasoning","text":"thinking"}}`)
	if err := session.handleEvent(ctx, hermesEvent{Type: "message.part.updated", Properties: reasoningProps}); err != nil {
		t.Fatalf("reasoning event: %v", err)
	}
	toolProps := json.RawMessage(`{"id":"part-3","sessionID":"native-1","messageID":"message-1","type":"tool","tool":"bash","callID":"call-1","state":{"status":"completed","title":"Run"}}`)
	if err := session.handleEvent(ctx, hermesEvent{Type: "message.part.created", Properties: toolProps}); err != nil {
		t.Fatalf("tool event: %v", err)
	}
	if err := session.emitMessage(ctx, nativeMessage{
		Info: nativeMessageInfo{ID: "message-1", SessionID: "native-1", Role: "assistant", Tokens: nativeTokens{Total: 9}},
		Parts: []nativePart{{
			SessionID: "native-1",
			MessageID: "message-1",
			Type:      "step-finish",
			Tokens:    nativeTokens{Input: 2, Output: 3, Reasoning: 1},
		}},
	}, false); err != nil {
		t.Fatalf("emitMessage: %v", err)
	}

	if conn.updateCount() != 6 {
		t.Fatalf("updates = %d, want 6: %#v", conn.updateCount(), conn.updates)
	}
	if conn.updates[0].Update.Plan == nil {
		t.Fatalf("first update = %#v, want plan", conn.updates[0].Update)
	}
	if conn.updates[1].Update.AgentMessageChunk == nil {
		t.Fatalf("second update = %#v, want agent chunk", conn.updates[1].Update)
	}
	if conn.updates[2].Update.AgentThoughtChunk == nil {
		t.Fatalf("third update = %#v, want thought", conn.updates[2].Update)
	}
	if conn.updates[3].Update.ToolCall == nil {
		t.Fatalf("fourth update = %#v, want tool", conn.updates[3].Update)
	}
	if conn.updates[4].Update.UsageUpdate == nil || conn.updates[5].Update.UsageUpdate == nil {
		t.Fatalf("usage updates missing: %#v", conn.updates)
	}
	if len(conn.extensions) == 0 || conn.extensions[0].method != RawEventMethod {
		t.Fatalf("raw events = %#v", conn.extensions)
	}
}

func TestUsageUpdateSizeIsContextWindow(t *testing.T) {
	ctx := context.Background()

	t.Run("advertised context window populates size", func(t *testing.T) {
		client := newFakeHermesClient()
		client.providers = providersResponse{Providers: []providerInfo{{
			ID: "openai",
			Models: map[string]providerModel{
				"gpt-test": {ID: "gpt-test", Limit: map[string]any{"context": float64(200000)}},
			},
		}}}
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)

		if err := session.emitMessage(ctx, nativeMessage{
			Info: nativeMessageInfo{ID: "message-1", SessionID: "native-1", Role: "assistant", Tokens: nativeTokens{Total: 1000}},
		}, false); err != nil {
			t.Fatalf("emitMessage: %v", err)
		}
		if conn.updateCount() != 1 {
			t.Fatalf("updates = %d, want 1: %#v", conn.updateCount(), conn.updates)
		}
		usage := conn.updates[0].Update.UsageUpdate
		if usage == nil {
			t.Fatalf("missing usage update: %#v", conn.updates[0].Update)
		}
		if usage.Used != 1000 || usage.Size != 200000 {
			t.Fatalf("usage used=%d size=%d, want used=1000 size=200000", usage.Used, usage.Size)
		}
	})

	t.Run("unknown context window emits size zero", func(t *testing.T) {
		client := newFakeHermesClient()
		client.providers = providersResponse{Providers: []providerInfo{{
			ID:     "openai",
			Models: map[string]providerModel{"gpt-test": {ID: "gpt-test"}},
		}}}
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)

		if err := session.emitMessage(ctx, nativeMessage{
			Info: nativeMessageInfo{ID: "message-1", SessionID: "native-1", Role: "assistant", Tokens: nativeTokens{Total: 1000}},
		}, false); err != nil {
			t.Fatalf("emitMessage: %v", err)
		}
		usage := conn.updates[0].Update.UsageUpdate
		if usage == nil {
			t.Fatalf("missing usage update: %#v", conn.updates[0].Update)
		}
		if usage.Used != 1000 || usage.Size != 0 {
			t.Fatalf("usage used=%d size=%d, want used=1000 size=0", usage.Used, usage.Size)
		}
	})
}

func TestSessionContextWindowFallbacks(t *testing.T) {
	ctx := context.Background()

	if got := (&session{}).contextWindow(ctx); got != 0 {
		t.Fatalf("nil client window = %d, want 0", got)
	}

	errClient := newFakeHermesClient()
	errClient.providersErr = errors.New("boom")
	if got := testSession(NewAgent(), errClient).contextWindow(ctx); got != 0 {
		t.Fatalf("provider error window = %d, want 0", got)
	}

	mismatchClient := newFakeHermesClient()
	mismatchClient.providers = providersResponse{Providers: []providerInfo{
		{ID: "other", Models: map[string]providerModel{"x": {ID: "x", Limit: map[string]any{"context": float64(10)}}}},
		{ID: "openai", Models: map[string]providerModel{"different": {ID: "different", Limit: map[string]any{"context": float64(20)}}}},
	}}
	if got := testSession(NewAgent(), mismatchClient).contextWindow(ctx); got != 0 {
		t.Fatalf("provider/model mismatch window = %d, want 0", got)
	}
}

func TestPromptSSEDisconnectAbortsNativeTurn(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativeMessage{}, ctx.Err()
	}
	agent := NewAgent()
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Prompt did not start native send")
	}
	client.errs <- errors.New("stream closed")
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "hermes_ws_disconnect") {
			t.Fatalf("Prompt error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Prompt did not return")
	}
	if client.abortCount() != 1 {
		t.Fatalf("abort count = %d, want 1", client.abortCount())
	}
}

// TestPromptGatewayDisconnectSentinelFences proves HW4: when SendMessage itself
// returns the gateway-disconnect sentinel (the read loop wired the disconnect
// into the error channel), the turn is fenced with exactly one
// hermes_ws_disconnect terminal error and one native abort.
func TestPromptGatewayDisconnectSentinelFences(t *testing.T) {
	client := newFakeHermesClient()
	client.sendMessage = func(_ context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		return nativeMessage{}, errGatewayDisconnected
	}
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	_, err := session.Prompt(context.Background(), acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
	if err == nil || !strings.Contains(err.Error(), "hermes_ws_disconnect") {
		t.Fatalf("Prompt error = %v", err)
	}
	if client.abortCount() != 1 {
		t.Fatalf("abort count = %d, want 1", client.abortCount())
	}
}

func TestPromptIdleSSEDisconnectDoesNotPoisonNextTurn(t *testing.T) {
	client := newFakeHermesClient()
	client.errs <- errors.New("idle stream closed")
	client.events <- hermesEvent{Type: "server.connected"}
	client.sendMessage = func(_ context.Context, id string, _ hermesMessageRequest) (nativeMessage, error) {
		return nativeMessage{
			Info:  nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
			Parts: []nativePart{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "ok"}},
		}, nil
	}
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	resp, err := session.Prompt(context.Background(), acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if resp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("resp = %#v", resp)
	}
	if client.abortCount() != 0 {
		t.Fatalf("idle disconnect aborted native turn %d times", client.abortCount())
	}
}

func TestPromptSuppressesLateFailedEpochEvents(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativeMessage{}, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Prompt did not start")
	}
	client.events <- hermesEvent{
		Type:        "message.part.created",
		StreamEpoch: 7,
		Properties:  json.RawMessage(`{"id":"stream-part","sessionID":"native-1","messageID":"assistant","type":"text","text":"stream"}`),
	}
	deadline := time.After(time.Second)
	for conn.updateCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("stream update was not emitted")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	client.errs <- streamError{epoch: 7, err: errors.New("stream failed")}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "hermes_ws_disconnect") {
			t.Fatalf("Prompt error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Prompt did not fail on stream error")
	}

	client.events <- hermesEvent{
		Type:        "message.part.created",
		StreamEpoch: 7,
		Properties:  json.RawMessage(`{"id":"late-part","sessionID":"native-1","messageID":"assistant","type":"text","text":"late"}`),
	}
	client.sendMessage = func(_ context.Context, id string, _ hermesMessageRequest) (nativeMessage, error) {
		return nativeMessage{Info: nativeMessageInfo{ID: "assistant-2", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
	}
	if _, err := session.Prompt(context.Background(), acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("again")}}); err != nil {
		t.Fatalf("second Prompt: %v", err)
	}
	if conn.updateCount() != 1 {
		t.Fatalf("late failed-epoch update was emitted: %#v", conn.updates)
	}
}

func TestPromptCleanEOFSentinelDisconnectAbortsTurn(t *testing.T) {
	client := newFakeHermesClient()
	agent := NewAgent()
	session := testSession(agent, client)
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativeMessage{}, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Prompt did not start")
	}
	client.errs <- streamError{epoch: 11, err: errors.New("websocket closed")}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "hermes_ws_disconnect") {
			t.Fatalf("Prompt error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Prompt did not fail on clean EOF disconnect")
	}
	if client.abortCount() != 1 {
		t.Fatalf("native aborts = %d, want 1", client.abortCount())
	}
}

func TestPromptServerReconnectReconcilesPendingPermissionAndQuestion(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
	session := testSession(agent, client)
	started := make(chan struct{})
	release := make(chan struct{})
	client.sendMessage = func(_ context.Context, id string, _ hermesMessageRequest) (nativeMessage, error) {
		close(started)
		<-release

		return nativeMessage{Info: nativeMessageInfo{ID: "assistant-1", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Prompt did not start")
	}
	client.pendingPermissions = []permissionRequest{{ID: "perm", SessionID: "native-1", Action: "edit"}}
	client.pendingQuestions = []questionRequest{{
		ID:        "question",
		SessionID: "native-1",
		Questions: []questionInfo{{
			Question: "Pick?",
			Header:   "Pick",
			Options:  []questionOption{{Label: "A", Description: "A"}},
		}},
	}}
	client.events <- hermesEvent{Type: "server.connected"}
	deadline := time.After(time.Second)
	for client.permissionReplyCount() == 0 || client.questionReplyCount() == 0 {
		select {
		case <-deadline:
			t.Fatalf("pending queues not reconciled permissions=%d questions=%d", client.permissionReplyCount(), client.questionReplyCount())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Prompt did not finish")
	}
}

func TestPromptServerReconnectReconcileFailures(t *testing.T) {
	for _, tt := range []struct {
		name          string
		setup         func(*recordingAgentClient, *Agent) chan struct{}
		pending       func(*fakeHermesClient)
		cancel        bool
		wantErr       string
		wantCancelled bool
	}{
		{
			name: "permission error",
			setup: func(conn *recordingAgentClient, _ *Agent) chan struct{} {
				conn.permErr = errors.New("permission failed")

				return nil
			},
			pending: func(client *fakeHermesClient) {
				client.pendingPermissions = []permissionRequest{{ID: "perm", SessionID: "native-1", Action: "edit"}}
			},
			wantErr: "permission failed",
		},
		{
			name: "permission cancelled",
			setup: func(conn *recordingAgentClient, _ *Agent) chan struct{} {
				conn.permissionStarted = make(chan struct{}, 1)
				conn.permissionRelease = make(chan struct{})

				return conn.permissionStarted
			},
			pending: func(client *fakeHermesClient) {
				client.pendingPermissions = []permissionRequest{{ID: "perm", SessionID: "native-1", Action: "edit"}}
			},
			cancel:        true,
			wantCancelled: true,
		},
		{
			name: "question error",
			setup: func(conn *recordingAgentClient, agent *Agent) chan struct{} {
				agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
				conn.elicitErr = errors.New("elicitation failed")

				return nil
			},
			pending: func(client *fakeHermesClient) {
				client.pendingQuestions = []questionRequest{{
					ID:        "question",
					SessionID: "native-1",
					Questions: []questionInfo{{
						Question: "Pick?",
						Header:   "Pick",
						Options:  []questionOption{{Label: "A"}},
					}},
				}}
			},
			wantErr: "elicitation failed",
		},
		{
			name: "question cancelled",
			setup: func(conn *recordingAgentClient, agent *Agent) chan struct{} {
				agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
				conn.elicitationStarted = make(chan struct{}, 1)
				conn.elicitationRelease = make(chan struct{})

				return conn.elicitationStarted
			},
			pending: func(client *fakeHermesClient) {
				client.pendingQuestions = []questionRequest{{
					ID:        "question",
					SessionID: "native-1",
					Questions: []questionInfo{{
						Question: "Pick?",
						Header:   "Pick",
						Options:  []questionOption{{Label: "A"}},
					}},
				}}
			},
			cancel:        true,
			wantCancelled: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeHermesClient()
			conn := newRecordingAgentClient()
			agent := NewAgent()
			agent.setAgentClient(conn)
			startedHook := tt.setup(conn, agent)
			session := testSession(agent, client)
			sendStarted := make(chan struct{})
			client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
				close(sendStarted)
				<-ctx.Done()

				return nativeMessage{}, ctx.Err()
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan struct {
				resp acp.PromptResponse
				err  error
			}, 1)
			go func() {
				resp, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
				done <- struct {
					resp acp.PromptResponse
					err  error
				}{resp: resp, err: err}
			}()
			select {
			case <-sendStarted:
			case <-ctx.Done():
				t.Fatal("Prompt did not start")
			}
			tt.pending(client)
			client.events <- hermesEvent{Type: "server.connected"}
			if startedHook != nil {
				select {
				case <-startedHook:
				case <-ctx.Done():
					t.Fatal("reconcile request did not start")
				}
			}
			if tt.cancel {
				cancel()
			}
			select {
			case got := <-done:
				if tt.wantCancelled {
					if got.err != nil || got.resp.StopReason != acp.StopReasonCancelled {
						t.Fatalf("Prompt cancelled resp=%#v err=%v", got.resp, got.err)
					}

					return
				}
				if got.err == nil || !strings.Contains(got.err.Error(), tt.wantErr) {
					t.Fatalf("Prompt error = %v, want %q", got.err, tt.wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("Prompt did not finish")
			}
		})
	}
}

func TestPromptCancelDuringInFlightPermissionAndQuestion(t *testing.T) {
	for _, tt := range []struct {
		name       string
		setup      func(*Agent)
		sendEvent  func(*fakeHermesClient)
		assertDone func(*testing.T, *fakeHermesClient, *recordingAgentClient)
	}{
		{
			name: "permission",
			sendEvent: func(client *fakeHermesClient) {
				client.events <- hermesEvent{
					Type:       "approval.request",
					Properties: json.RawMessage(`{"id":"perm","sessionID":"native-1","action":"edit"}`),
				}
			},
			assertDone: func(t *testing.T, client *fakeHermesClient, conn *recordingAgentClient) {
				t.Helper()
				if conn.permissionRequestCount() != 1 {
					t.Fatalf("permission requests = %#v", conn.permissions)
				}
				if client.permissionReplyCount() != 1 {
					t.Fatalf("permission replies = %#v", client.permissionReplies)
				}
				reply := client.permissionReply(0)
				if reply.reply != "reject" || reply.message != "cancelled" {
					t.Fatalf("permission cancel reply = %#v", reply)
				}
			},
		},
		{
			name: "question",
			setup: func(agent *Agent) {
				agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
			},
			sendEvent: func(client *fakeHermesClient) {
				client.events <- hermesEvent{
					Type:       "clarify.request",
					Properties: json.RawMessage(`{"id":"question","sessionID":"native-1","questions":[{"question":"Pick one","options":[{"label":"Yes"}]}]}`),
				}
			},
			assertDone: func(t *testing.T, client *fakeHermesClient, conn *recordingAgentClient) {
				t.Helper()
				if len(conn.elicitations) != 1 {
					t.Fatalf("elicitations = %#v", conn.elicitations)
				}
				if client.questionRejectCount() != 1 {
					t.Fatalf("question rejects = %#v", client.questionRejects)
				}
				if client.questionRejects[0].route != questionRouteAPI {
					t.Fatalf("question reject route = %#v", client.questionRejects[0])
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeHermesClient()
			conn := newRecordingAgentClient()
			conn.permissionStarted = make(chan struct{}, 1)
			conn.permissionRelease = make(chan struct{})
			conn.elicitationStarted = make(chan struct{}, 1)
			conn.elicitationRelease = make(chan struct{})
			agent := NewAgent()
			agent.setAgentClient(conn)
			if tt.setup != nil {
				tt.setup(agent)
			}
			session := testSession(agent, client)
			agent.mu.Lock()
			agent.sessions[session.id] = session
			agent.mu.Unlock()

			started := make(chan struct{})
			client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
				close(started)
				<-ctx.Done()

				return nativeMessage{}, ctx.Err()
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan acp.PromptResponse, 1)
			go func() {
				resp, _ := agent.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
				done <- resp
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("Prompt did not start")
			}
			tt.sendEvent(client)
			switch tt.name {
			case "permission":
				select {
				case <-conn.permissionStarted:
				case <-ctx.Done():
					t.Fatal("permission request did not start")
				}
			case "question":
				select {
				case <-conn.elicitationStarted:
				case <-ctx.Done():
					t.Fatal("elicitation request did not start")
				}
			}
			if err := agent.Cancel(ctx, acp.CancelNotification{SessionId: session.id}); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
			select {
			case resp := <-done:
				if resp.StopReason != acp.StopReasonCancelled {
					t.Fatalf("prompt resp = %#v", resp)
				}
			case <-ctx.Done():
				t.Fatal("Prompt did not return after cancel")
			}
			tt.assertDone(t, client, conn)
		})
	}
}

func TestMissingLiveSessionMappingPoisonsSession(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.replyErr = missingLiveSessionMappingError{StoredSessionID: "native-1"}
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	err := session.handleEvent(ctx, hermesEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{"id":"perm-missing-live","sessionID":"native-1","action":"edit"}`),
	})
	if err == nil {
		t.Fatal("missing live mapping did not fail permission handling")
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("missing live mapping error type = %T", err)
	}
	data, _ := reqErr.Data.(map[string]any)
	if data["error"] != "hermes_missing_live_session_mapping" {
		t.Fatalf("missing live mapping error data = %#v", data)
	}
	if err := session.ensureNotPoisoned(); err == nil {
		t.Fatal("missing live mapping did not poison session")
	}
}

func TestPromptBacklogCancelledBeforeTurn(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	conn.permErr = context.Canceled
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.mu.Lock()
	session.cancelled = true
	session.mu.Unlock()
	client.events <- hermesEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{"id":"perm","sessionID":"native-1"}`),
	}
	resp, err := session.Prompt(context.Background(), acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
	if err != nil || resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("resp=%#v err=%v", resp, err)
	}
}

func TestPromptBacklogErrorBeforeTurn(t *testing.T) {
	client := newFakeHermesClient()
	session := testSession(NewAgent(), client)
	client.events <- hermesEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{`),
	}
	if _, err := session.Prompt(context.Background(), acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
		t.Fatal("malformed backlog event was ignored")
	}
}

func TestPromptReconcileCancelledBeforeSend(t *testing.T) {
	for _, tt := range []struct {
		name      string
		setup     func(*fakeHermesClient, *recordingAgentClient, *Agent)
		waitStart func(context.Context, *testing.T, *recordingAgentClient)
	}{
		{
			name: "permission",
			setup: func(client *fakeHermesClient, conn *recordingAgentClient, _ *Agent) {
				client.pendingPermissions = []permissionRequest{{ID: "perm", SessionID: "native-1"}}
				conn.permissionStarted = make(chan struct{}, 1)
				conn.permissionRelease = make(chan struct{})
			},
			waitStart: func(ctx context.Context, t *testing.T, conn *recordingAgentClient) {
				t.Helper()
				select {
				case <-conn.permissionStarted:
				case <-ctx.Done():
					t.Fatal("permission request did not start")
				}
			},
		},
		{
			name: "question",
			setup: func(client *fakeHermesClient, conn *recordingAgentClient, agent *Agent) {
				client.pendingQuestions = []questionRequest{{ID: "question", SessionID: "native-1"}}
				conn.elicitationStarted = make(chan struct{}, 1)
				conn.elicitationRelease = make(chan struct{})
				agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
			},
			waitStart: func(ctx context.Context, t *testing.T, conn *recordingAgentClient) {
				t.Helper()
				select {
				case <-conn.elicitationStarted:
				case <-ctx.Done():
					t.Fatal("elicitation request did not start")
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeHermesClient()
			conn := newRecordingAgentClient()
			agent := NewAgent()
			agent.setAgentClient(conn)
			session := testSession(agent, client)
			agent.sessions[session.id] = session
			tt.setup(client, conn, agent)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan acp.PromptResponse, 1)
			go func() {
				resp, _ := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
				done <- resp
			}()
			tt.waitStart(ctx, t, conn)
			session.cancelTurn()
			select {
			case resp := <-done:
				if resp.StopReason != acp.StopReasonCancelled {
					t.Fatalf("resp = %#v", resp)
				}
			case <-ctx.Done():
				t.Fatal("prompt did not return")
			}
		})
	}
}

func TestTurnFenceHelperBranches(t *testing.T) {
	session := testSession(NewAgent(), newFakeHermesClient())
	if !session.claimPermissionRequest("") || !session.claimQuestionRequest("") {
		t.Fatal("empty request ids should not be fenced")
	}
	session.processedPermission = nil
	session.processedQuestion = nil
	if !session.claimPermissionRequest("perm") || !session.claimQuestionRequest("question") {
		t.Fatal("nil processed request maps were not initialized")
	}
	session.markActiveMessageID("")
	session.activeMessageIDs = nil
	session.markActiveMessageID("message-1")
	session.failedMessageIDs = nil
	session.failedStreamEpochs = nil
	session.markStreamFailed(9)
	if !session.shouldSuppressEvent(hermesEvent{StreamEpoch: 9}) {
		t.Fatal("failed stream epoch was not suppressed")
	}
	if !session.shouldSuppressEvent(hermesEvent{
		Properties: json.RawMessage(`{"sessionID":"native-1","messageID":"message-1","type":"text","text":"late"}`),
	}) {
		t.Fatal("failed message id was not suppressed")
	}
	if session.shouldSuppressEvent(hermesEvent{
		Properties: json.RawMessage(`{"sessionID":"native-1","messageID":"message-2","type":"text","text":"ok"}`),
	}) {
		t.Fatal("unfailed message id was suppressed")
	}
	if err := session.handleEvent(context.Background(), hermesEvent{
		StreamEpoch: 9,
		Properties:  json.RawMessage(`{"sessionID":"native-1","messageID":"message-1","type":"text","text":"late"}`),
	}); err != nil {
		t.Fatalf("suppressed handleEvent: %v", err)
	}
}

func TestPermissionCancelledReplyBranches(t *testing.T) {
	t.Run("permission without connection uses background when context cancelled", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(NewAgent(), client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		if err := session.handlePermission(turnCtx, permissionRequest{ID: "perm", SessionID: "native-1"}); err != nil {
			t.Fatalf("handlePermission: %v", err)
		}
		if got := client.permissionReply(0).message; got != "client unavailable" {
			t.Fatalf("permission reply = %q", got)
		}
		session.finishTurn()
	})

	t.Run("permission client error after context cancellation resolves native request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.permErr = context.Canceled
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		err := session.handlePermission(turnCtx, permissionRequest{ID: "perm", SessionID: "native-1"})
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handlePermission err = %v", err)
		}
		if got := client.permissionReply(0).message; got != "cancelled" {
			t.Fatalf("permission reply = %q", got)
		}
		session.finishTurn()
	})

	t.Run("permission client response after context cancellation is rejected", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		err := session.handlePermission(turnCtx, permissionRequest{ID: "perm", SessionID: "native-1"})
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handlePermission err = %v", err)
		}
		if got := client.permissionReply(0).message; got != "cancelled" {
			t.Fatalf("permission reply = %q", got)
		}
		session.finishTurn()
	})

	t.Run("permission cancellation reply error is returned", func(t *testing.T) {
		client := newFakeHermesClient()
		client.replyErr = errors.New("reply failed")
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		if err := session.handlePermission(turnCtx, permissionRequest{ID: "perm", SessionID: "native-1"}); err == nil {
			t.Fatal("reply error was ignored")
		}
		session.finishTurn()
	})

	t.Run("permission client error rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.permErr = errors.New("permission failed")
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		err := session.handlePermission(context.Background(), permissionRequest{ID: "perm", SessionID: "native-1", ReplyRoute: permissionRouteSession})
		if err == nil || !strings.Contains(err.Error(), "permission failed") {
			t.Fatalf("handlePermission err = %v", err)
		}
		reply := client.permissionReply(0)
		if reply.route != permissionRouteSession || reply.reply != "reject" || reply.message != "client permission request failed" {
			t.Fatalf("permission fail-closed reply = %#v", reply)
		}
	})

	t.Run("permission client error returns reject failure", func(t *testing.T) {
		client := newFakeHermesClient()
		client.replyErr = errors.New("reply failed")
		conn := newRecordingAgentClient()
		conn.permErr = errors.New("permission failed")
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		err := session.handlePermission(context.Background(), permissionRequest{ID: "perm", SessionID: "native-1"})
		if err == nil || !strings.Contains(err.Error(), "permission failed") || !strings.Contains(err.Error(), "reply failed") {
			t.Fatalf("handlePermission err = %v", err)
		}
	})

	t.Run("permission late response after cancel is not double-replied", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.permissionStarted = make(chan struct{}, 1)
		conn.permissionRelease = make(chan struct{})
		conn.permissionIgnoreContext = true
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- session.handlePermission(turnCtx, permissionRequest{ID: "perm", SessionID: "native-1"})
		}()
		<-conn.permissionStarted
		session.cancelTurn()
		close(conn.permissionRelease)
		err := <-done
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handlePermission err = %v", err)
		}
		if client.permissionReplyCount() != 1 {
			t.Fatalf("permission replies = %#v", client.permissionReplies)
		}
		session.finishTurn()
	})

	t.Run("permission late client error after cancel is not double-replied", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.permErr = errors.New("permission failed")
		conn.permissionStarted = make(chan struct{}, 1)
		conn.permissionRelease = make(chan struct{})
		conn.permissionIgnoreContext = true
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- session.handlePermission(turnCtx, permissionRequest{ID: "perm", SessionID: "native-1"})
		}()
		<-conn.permissionStarted
		session.cancelTurn()
		close(conn.permissionRelease)
		err := <-done
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handlePermission err = %v", err)
		}
		if client.permissionReplyCount() != 1 {
			t.Fatalf("permission replies = %#v", client.permissionReplies)
		}
		session.finishTurn()
	})
}

func TestQuestionCancelledReplyBranches(t *testing.T) {
	t.Run("question without form support uses background when context cancelled", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(NewAgent(), client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		if err := session.handleQuestion(turnCtx, questionRequest{ID: "question", SessionID: "native-1"}); err != nil {
			t.Fatalf("handleQuestion: %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %#v", client.questionRejects)
		}
		session.finishTurn()
	})

	t.Run("question client error after context cancellation rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitErr = context.Canceled
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		err := session.handleQuestion(turnCtx, questionRequest{ID: "question", SessionID: "native-1"})
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handleQuestion err = %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %#v", client.questionRejects)
		}
		session.finishTurn()
	})

	t.Run("question decline error is returned", func(t *testing.T) {
		client := newFakeHermesClient()
		client.replyErr = errors.New("reject failed")
		conn := newRecordingAgentClient()
		conn.elicitation = acp.NewUnstableCreateElicitationResponseDecline()
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		if err := session.handleQuestion(context.Background(), questionRequest{ID: "question", SessionID: "native-1"}); err == nil {
			t.Fatal("reject error was ignored")
		}
	})

	t.Run("question decline after context cancellation returns cancelled", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitation = acp.NewUnstableCreateElicitationResponseDecline()
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		err := session.handleQuestion(turnCtx, questionRequest{ID: "question", SessionID: "native-1"})
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handleQuestion err = %v", err)
		}
		session.finishTurn()
	})

	t.Run("question late decline after cancel is not double-rejected", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitation = acp.NewUnstableCreateElicitationResponseDecline()
		conn.elicitationStarted = make(chan struct{}, 1)
		conn.elicitationRelease = make(chan struct{})
		conn.elicitationIgnoreContext = true
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, questionRequest{ID: "question", SessionID: "native-1"})
		}()
		<-conn.elicitationStarted
		session.cancelTurn()
		close(conn.elicitationRelease)
		err := <-done
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handleQuestion err = %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %#v", client.questionRejects)
		}
		session.finishTurn()
	})

	t.Run("question client error rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitErr = errors.New("elicitation failed")
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		err := session.handleQuestion(context.Background(), questionRequest{ID: "question", SessionID: "native-1", ReplyRoute: questionRouteAPI})
		if err == nil || !strings.Contains(err.Error(), "elicitation failed") {
			t.Fatalf("handleQuestion err = %v", err)
		}
		if client.questionRejectCount() != 1 || client.questionRejects[0].route != questionRouteAPI {
			t.Fatalf("question fail-closed rejects = %#v", client.questionRejects)
		}
	})

	t.Run("question client error returns reject failure", func(t *testing.T) {
		client := newFakeHermesClient()
		client.replyErr = errors.New("reject failed")
		conn := newRecordingAgentClient()
		conn.elicitErr = errors.New("elicitation failed")
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		err := session.handleQuestion(context.Background(), questionRequest{ID: "question", SessionID: "native-1"})
		if err == nil || !strings.Contains(err.Error(), "elicitation failed") || !strings.Contains(err.Error(), "reject failed") {
			t.Fatalf("handleQuestion err = %v", err)
		}
	})

	t.Run("question accept after context cancellation rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		err := session.handleQuestion(turnCtx, questionRequest{ID: "question", SessionID: "native-1"})
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handleQuestion err = %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %#v", client.questionRejects)
		}
		session.finishTurn()
	})

	t.Run("question accept cancellation reject error is returned", func(t *testing.T) {
		client := newFakeHermesClient()
		client.replyErr = errors.New("reject failed")
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx)
		cancel()
		if err := session.handleQuestion(turnCtx, questionRequest{ID: "question", SessionID: "native-1"}); err == nil {
			t.Fatal("reject error was ignored")
		}
		session.finishTurn()
	})

	t.Run("question late response after cancel is not double-rejected", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitationStarted = make(chan struct{}, 1)
		conn.elicitationRelease = make(chan struct{})
		conn.elicitationIgnoreContext = true
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, questionRequest{ID: "question", SessionID: "native-1"})
		}()
		<-conn.elicitationStarted
		session.cancelTurn()
		close(conn.elicitationRelease)
		err := <-done
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handleQuestion err = %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %#v", client.questionRejects)
		}
		session.finishTurn()
	})

	t.Run("question late client error after cancel is not double-rejected", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitErr = errors.New("elicitation failed")
		conn.elicitationStarted = make(chan struct{}, 1)
		conn.elicitationRelease = make(chan struct{})
		conn.elicitationIgnoreContext = true
		agent := NewAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, questionRequest{ID: "question", SessionID: "native-1"})
		}()
		<-conn.elicitationStarted
		session.cancelTurn()
		close(conn.elicitationRelease)
		err := <-done
		if !errors.Is(err, errPromptCancelled) {
			t.Fatalf("handleQuestion err = %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %#v", client.questionRejects)
		}
		session.finishTurn()
	})
}

func TestPromptImageParts(t *testing.T) {
	imageData, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image", Data: "AA==", MimeType: "image/png"}}})
	if err != nil {
		t.Fatalf("image data prompt: %v", err)
	}
	if len(imageData) != 1 || imageData[0]["type"] != "file" || imageData[0]["mime"] != "image/png" || imageData[0]["url"] != "data:image/png;base64,AA==" {
		t.Fatalf("image data part = %#v", imageData[0])
	}

	imageURI := "file:///tmp/pic.png"
	imageURL, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image", Uri: &imageURI}}})
	if err != nil {
		t.Fatalf("image uri prompt: %v", err)
	}
	if len(imageURL) != 1 || imageURL[0]["mime"] != defaultMimeType || imageURL[0]["url"] != imageURI || imageURL[0]["filename"] != "pic.png" {
		t.Fatalf("image uri part = %#v", imageURL[0])
	}

	if _, err = promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image"}}}); err == nil {
		t.Fatal("image without data or uri accepted")
	}

	invalidURI := "%"
	imageInvalid, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image", Uri: &invalidURI}}})
	if err != nil || imageInvalid[0]["filename"] != nil || imageInvalid[0]["mime"] != defaultMimeType || imageInvalid[0]["url"] != invalidURI {
		t.Fatalf("invalid uri image part = %#v err=%v", imageInvalid, err)
	}

	rootURI := "https://example.com"
	imageRoot, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image", Uri: &rootURI}}})
	if err != nil || imageRoot[0]["filename"] != nil || imageRoot[0]["url"] != rootURI {
		t.Fatalf("root uri image part = %#v err=%v", imageRoot, err)
	}
}

func TestPromptHelpersAndAnswerMapping(t *testing.T) {
	parts, err := promptToHermesParts([]acp.ContentBlock{
		acp.TextBlock("hello"),
		{ResourceLink: &acp.ContentBlockResourceLink{Name: "a", Type: "resource_link", Uri: "file:///tmp/a"}},
		{Resource: &acp.ContentBlockResource{Type: "resource", Resource: acp.EmbeddedResourceResource{
			TextResourceContents: &acp.TextResourceContents{Text: "embedded", Uri: "file:///tmp/b"},
		}}},
		{Resource: &acp.ContentBlockResource{Type: "resource", Resource: acp.EmbeddedResourceResource{
			BlobResourceContents: &acp.BlobResourceContents{Blob: "AA==", Uri: "file:///tmp/blob"},
		}}},
	})
	if err != nil {
		t.Fatalf("promptToHermesParts: %v", err)
	}
	if len(parts) != 4 || parts[0]["text"] != "hello" || parts[1]["text"] != "file:///tmp/a" ||
		parts[2]["text"] != "embedded" || parts[3]["text"] != "file:///tmp/blob" {
		t.Fatalf("parts = %#v", parts)
	}
	if _, err := promptToHermesParts(nil); err == nil {
		t.Fatal("empty prompt accepted")
	}
	if _, err := promptToHermesParts([]acp.ContentBlock{{Audio: &acp.ContentBlockAudio{Type: "audio", Data: "AA==", MimeType: "audio/wav"}}}); err == nil {
		t.Fatal("audio prompt accepted")
	}
	req, ids := questionElicitationRequest(questionRequest{ID: "q", SessionID: "s"})
	if req.Form == nil || req.Form.Message != "Hermes needs input" || !reflect.DeepEqual(ids, []string{"question_1"}) {
		t.Fatalf("empty question elicitation = %#v ids=%#v", req, ids)
	}
	answers := questionAnswersFromContent(map[string]any{
		"question_1": nil,
		"question_2": []string{"a"},
		"question_3": []any{"b", float64(3), nil},
		"question_4": 4,
	}, []string{"question_1", "question_2", "question_3", "question_4"})
	if !reflect.DeepEqual(answers, [][]string{{}, {"a"}, {"b", "3"}, {"4"}}) {
		t.Fatalf("answers = %#v", answers)
	}
}

func TestSlashPromptIsPlainTextAndCommandSilent(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	messageID := "msg-user"
	client.sendMessage = func(_ context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
		if id != "native-1" {
			t.Fatalf("native id = %q", id)
		}
		if req.MessageID != messageID || len(req.Parts) != 1 || req.Parts[0]["text"] != "/review inspect this" {
			t.Fatalf("plain slash request = %#v", req)
		}

		return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
	}

	resp, err := session.Prompt(context.Background(), acp.PromptRequest{
		SessionId: session.id,
		MessageId: &messageID,
		Prompt:    []acp.ContentBlock{acp.TextBlock("/review inspect this")},
	})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if resp.StopReason != acp.StopReasonEndTurn || resp.UserMessageId == nil || *resp.UserMessageId != messageID {
		t.Fatalf("response = %#v", resp)
	}
	for _, notification := range conn.updates {
		if notification.Update.AvailableCommandsUpdate != nil {
			t.Fatalf("unexpected command update = %#v", notification)
		}
	}
}

func TestPromptSuccessCancelAndErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("success through agent", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		agent.mu.Lock()
		agent.sessions[session.id] = session
		agent.mu.Unlock()
		messageID := "user-message"
		client.sendMessage = func(_ context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
			if id != "native-1" || req.MessageID != messageID || len(req.Parts) != 1 {
				t.Fatalf("SendMessage id=%q req=%#v", id, req)
			}
			msg := nativeMessage{Info: nativeMessageInfo{
				ID:        "assistant-1",
				SessionID: id,
				Role:      "assistant",
				Finish:    "length",
				Tokens:    nativeTokens{Total: 3, Input: 1, Output: 2},
			}}
			msg.Parts = []nativePart{{SessionID: id, MessageID: "assistant-1", Type: "text", Text: "hi"}}

			return msg, nil
		}
		resp, err := agent.Prompt(ctx, acp.PromptRequest{SessionId: session.id, MessageId: &messageID, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
		if resp.StopReason != acp.StopReasonMaxTokens || resp.UserMessageId == nil || *resp.UserMessageId != messageID || resp.Usage.TotalTokens != 3 {
			t.Fatalf("prompt resp = %#v", resp)
		}
		if conn.updateCount() != 2 {
			t.Fatalf("updates = %#v", conn.updates)
		}
	})

	t.Run("send error", func(t *testing.T) {
		client := newFakeHermesClient()
		client.sendMessage = func(context.Context, string, hermesMessageRequest) (nativeMessage, error) {
			return nativeMessage{}, errors.New("send failed")
		}
		session := testSession(NewAgent(), client)
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
			t.Fatal("send error prompt succeeded")
		}
	})

	t.Run("snapshot error after final message", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := NewAgent(WithSessionStore(&errorSessionStore{err: errors.New("snapshot failed")}))
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, id string, _ hermesMessageRequest) (nativeMessage, error) {
			return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil ||
			!strings.Contains(err.Error(), "snapshot failed") {
			t.Fatalf("snapshot error = %v", err)
		}
	})

	t.Run("prompt validation and turn backpressure", func(t *testing.T) {
		session := testSession(NewAgent(), newFakeHermesClient())
		session.turnQueue() <- struct{}{}
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
			t.Fatal("prompt backpressure was ignored")
		}
		<-session.turnQueue()
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id}); err == nil {
			t.Fatal("empty prompt was accepted")
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{{Audio: &acp.ContentBlockAudio{Type: "audio", Data: "AA==", MimeType: "audio/wav"}}}}); err == nil {
			t.Fatal("unsupported audio prompt was accepted")
		}
	})

	t.Run("pending permission error", func(t *testing.T) {
		client := newFakeHermesClient()
		client.permissionsErr = errors.New("permissions failed")
		session := testSession(NewAgent(), client)
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
			t.Fatal("permission error prompt succeeded")
		}
	})

	t.Run("pending question error", func(t *testing.T) {
		client := newFakeHermesClient()
		client.questionsErr = errors.New("questions failed")
		session := testSession(NewAgent(), client)
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
			t.Fatal("question error prompt succeeded")
		}
	})

	t.Run("turn context cancellation", func(t *testing.T) {
		client := newFakeHermesClient()
		started := make(chan struct{})
		client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
			close(started)
			<-ctx.Done()

			return nativeMessage{}, ctx.Err()
		}
		session := testSession(NewAgent(), client)
		ctx2, cancel := context.WithCancel(context.Background())
		done := make(chan acp.PromptResponse, 1)
		go func() {
			resp, _ := session.Prompt(ctx2, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
			done <- resp
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("sendMessage did not start")
		}
		cancel()
		select {
		case resp := <-done:
			if resp.StopReason != acp.StopReasonCancelled {
				t.Fatalf("cancel resp = %#v", resp)
			}
		case <-time.After(time.Second):
			t.Fatal("cancelled prompt did not return")
		}
	})

	t.Run("unknown agent prompt and cancel", func(t *testing.T) {
		agent := NewAgent()
		_, err := agent.Prompt(ctx, acp.PromptRequest{SessionId: "missing"})
		if err == nil {
			t.Fatal("unknown agent prompt succeeded")
		}
		var reqErr *acp.RequestError
		if !errors.As(err, &reqErr) {
			t.Fatalf("unknown session error type = %T", err)
		}
		if reqErr.Code != -32602 {
			t.Fatalf("unknown session code = %d, want -32602", reqErr.Code)
		}
		data, ok := reqErr.Data.(map[string]any)
		if !ok || data["error"] != "unknown session" || data["field"] != "sessionId" {
			t.Fatalf("unknown session data = %#v", reqErr.Data)
		}
		if err := agent.Cancel(ctx, acp.CancelNotification{SessionId: "missing"}); err == nil {
			t.Fatal("unknown agent cancel succeeded")
		}
	})
}

func TestNativeSessionIDDriftPoisonsSession(t *testing.T) {
	ctx := context.Background()

	t.Run("mismatched final message info session id", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		store := newCountingSessionStore()
		agent := NewAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
			return nativeMessage{
				Info:  nativeMessageInfo{ID: "assistant", SessionID: "native-other", Role: "assistant", Finish: "stop"},
				Parts: []nativePart{{SessionID: "native-1", MessageID: "assistant", Type: "text", Text: "should not emit"}},
			}, nil
		}

		_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		assertNativeSessionDriftPoison(t, session, conn, store, err, "native-other")
	})

	t.Run("mismatched final message part session id", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		store := newCountingSessionStore()
		agent := NewAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
			return nativeMessage{
				Info:  nativeMessageInfo{ID: "assistant", SessionID: "native-1", Role: "assistant", Finish: "stop"},
				Parts: []nativePart{{SessionID: "native-other", MessageID: "assistant", Type: "text", Text: "should not emit"}},
			}, nil
		}

		_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		assertNativeSessionDriftPoison(t, session, conn, store, err, "native-other")
	})

	t.Run("mismatched replay message session id", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		store := newCountingSessionStore()
		agent := NewAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.messages = []nativeMessage{{
			Info: nativeMessageInfo{ID: "user", SessionID: "native-other", Role: "user"},
			Parts: []nativePart{{
				SessionID: "native-other",
				MessageID: "user",
				Type:      "text",
				Text:      "should not replay",
			}},
		}}

		err := session.replayMessages(ctx)
		assertNativeSessionDriftPoison(t, session, conn, store, err, "native-other")
	})
}

func TestPoisonedSessionRejectsFollowUpOperations(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	store := newCountingSessionStore()
	agent := NewAgent(WithSessionStore(store))
	agent.setAgentClient(conn)
	s := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[s.id] = s
	agent.mu.Unlock()

	if err := s.poison(ctx, "native drift without advertisement"); err == nil ||
		!strings.Contains(err.Error(), "hermes_native_session_id_drift") {
		t.Fatalf("poison error = %v", err)
	}
	if conn.updateCount() != 0 {
		t.Fatalf("poison emitted updates: %#v", conn.updates)
	}
	if err := s.poison(ctx, "second poison"); err == nil ||
		!strings.Contains(err.Error(), "session_poisoned") ||
		!strings.Contains(err.Error(), "native drift without advertisement") {
		t.Fatalf("second poison error = %v", err)
	}
	if _, err := s.acquireTurn(ctx); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("acquire poisoned session error = %v", err)
	}
	if err := agent.Cancel(ctx, acp.CancelNotification{SessionId: s.id}); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("cancel poisoned session error = %v", err)
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetModelRequest(s.id, "openai/gpt-test")); err == nil ||
		!strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("set config poisoned session error = %v", err)
	}
	if err := s.replayMessages(ctx); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("replay poisoned session error = %v", err)
	}
	if err := s.snapshotToStore(ctx); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("snapshot poisoned session error = %v", err)
	}
	if store.replaceCount() != 0 {
		t.Fatalf("store writes after poisoned follow-up = %d, want 0", store.replaceCount())
	}
	if err := (&session{}).validateNativeMessageSession(ctx, nativeMessage{Info: nativeMessageInfo{SessionID: "native-other"}}); err != nil {
		t.Fatalf("empty expected native id validation error = %v", err)
	}
}

func assertNativeSessionDriftPoison(
	t *testing.T,
	session *session,
	conn *recordingAgentClient,
	store *countingSessionStore,
	err error,
	gotNativeID string,
) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "hermes_native_session_id_drift") || !strings.Contains(err.Error(), gotNativeID) {
		t.Fatalf("drift error = %v", err)
	}
	if conn.updateCount() != 0 {
		t.Fatalf("updates after poison = %#v", conn.updates)
	}
	if store.replaceCount() != 0 {
		t.Fatalf("store writes after poison = %d, want 0", store.replaceCount())
	}
	_, nextErr := session.Prompt(context.Background(), acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("again")}})
	if nextErr == nil || !strings.Contains(nextErr.Error(), "session_poisoned") || !strings.Contains(nextErr.Error(), gotNativeID) {
		t.Fatalf("subsequent poison error = %v", nextErr)
	}
}

type countingSessionStore struct {
	*InMemorySessionStore
	mu       sync.Mutex
	replaces int
}

func newCountingSessionStore() *countingSessionStore {
	return &countingSessionStore{InMemorySessionStore: NewInMemorySessionStore()}
}

func (s *countingSessionStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	s.mu.Lock()
	s.replaces++
	s.mu.Unlock()

	return s.InMemorySessionStore.Replace(ctx, main, replacements)
}

func (s *countingSessionStore) replaceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.replaces
}

func TestPromptEventLoopAndEmitErrorBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("server connected is ignored before final message", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		started := make(chan struct{})
		release := make(chan struct{})
		client.sendMessage = func(_ context.Context, id string, _ hermesMessageRequest) (nativeMessage, error) {
			close(started)
			<-release

			return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		done := make(chan error, 1)
		go func() {
			_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
			done <- err
		}()
		<-started
		client.events <- hermesEvent{Type: "server.connected"}
		client.events <- hermesEvent{
			Type:       "message.part.created",
			Properties: json.RawMessage(`{"id":"event-part","sessionID":"native-1","messageID":"assistant","type":"text","text":"stream"}`),
		}
		deadline := time.After(time.Second)
		for conn.updateCount() == 0 {
			select {
			case <-deadline:
				t.Fatalf("stream update was not emitted: %#v", conn.updates)
			default:
				time.Sleep(time.Millisecond)
			}
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("Prompt: %v", err)
		}
		if conn.updateCount() != 1 {
			t.Fatalf("updates = %#v", conn.updates)
		}
	})

	t.Run("event update error returns", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.updateErr = errors.New("update failed")
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		started := make(chan struct{})
		client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
			close(started)
			<-ctx.Done()

			return nativeMessage{}, ctx.Err()
		}
		done := make(chan error, 1)
		go func() {
			_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
			done <- err
		}()
		<-started
		client.events <- hermesEvent{
			Type:       "message.part.created",
			Properties: json.RawMessage(`{"id":"event-part","sessionID":"native-1","messageID":"assistant","type":"text","text":"stream"}`),
		}
		if err := <-done; err == nil || !strings.Contains(err.Error(), "update failed") {
			t.Fatalf("event error = %v", err)
		}
		if client.abortCount() != 1 {
			t.Fatalf("event error aborts = %d, want 1", client.abortCount())
		}
	})

	t.Run("final message update error returns", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.updateErr = errors.New("final update failed")
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, id string, _ hermesMessageRequest) (nativeMessage, error) {
			return nativeMessage{
				Info:  nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
				Parts: []nativePart{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "done", Raw: json.RawMessage(`{"id":"final"}`)}},
			}, nil
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil ||
			!strings.Contains(err.Error(), "final update failed") {
			t.Fatalf("final emit error = %v", err)
		}
	})
}

func TestReplayAndEventEdgeBranches(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	client.messages = []nativeMessage{{
		Info: nativeMessageInfo{ID: "user-1", SessionID: "native-1", Role: "user"},
		Parts: []nativePart{{
			SessionID: "native-1",
			MessageID: "user-1",
			Type:      "text",
			Text:      "user text",
		}},
	}}
	if err := session.replayMessages(ctx); err != nil {
		t.Fatalf("replayMessages: %v", err)
	}
	if conn.updateCount() != 1 || conn.updates[0].Update.UserMessageChunk == nil {
		t.Fatalf("replay updates = %#v", conn.updates)
	}
	client.messagesErr = errors.New("messages failed")
	if err := session.replayMessages(ctx); err == nil {
		t.Fatal("replayMessages ignored client error")
	}
	if err := session.emitMessage(ctx, nativeMessage{Info: nativeMessageInfo{Role: "user"}}, false); err != nil {
		t.Fatalf("emitMessage skipped user: %v", err)
	}
	if err := session.emitUpdate(ctx, acp.UpdatePlan(acp.PlanEntry{Content: "no client"})); err != nil {
		t.Fatalf("emitUpdate with conn: %v", err)
	}
	nilConnSession := testSession(NewAgent(), newFakeHermesClient())
	if err := nilConnSession.emitUpdate(ctx, acp.UpdatePlan(acp.PlanEntry{Content: "no client"})); err != nil {
		t.Fatalf("emitUpdate without conn: %v", err)
	}

	noConnClient := newFakeHermesClient()
	noConnSession := testSession(NewAgent(), noConnClient)
	if err := noConnSession.handlePermission(ctx, permissionRequest{}); err != nil {
		t.Fatalf("empty permission: %v", err)
	}
	noConnSession.pending = nil
	if err := noConnSession.handlePermission(ctx, permissionRequest{ID: "p", SessionID: "native-1"}); err != nil {
		t.Fatalf("nil conn permission: %v", err)
	}
	if got := noConnClient.permissionReply(0).message; got != "client unavailable" {
		t.Fatalf("nil conn permission reply = %q", got)
	}
	session.questions = nil
	if err := session.handleQuestion(ctx, questionRequest{}); err != nil {
		t.Fatalf("empty question: %v", err)
	}
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	conn.elicitErr = errors.New("elicitation failed")
	if err := session.handleQuestion(ctx, questionRequest{ID: "q-error", SessionID: "native-1"}); err == nil {
		t.Fatal("elicitation error was ignored")
	}
	conn.elicitErr = nil
	testApprovalAndClarifyEventBranches(t, ctx, session, client, conn)
	testForeignEventAndPartHelperBranches(t, ctx, session)
}

func testApprovalAndClarifyEventBranches(t *testing.T, ctx context.Context, session *session, client *fakeHermesClient, conn *recordingAgentClient) {
	t.Helper()

	if err := session.handleEvent(ctx, hermesEvent{Type: "approval.request", Properties: json.RawMessage(`{`)}); err == nil {
		t.Fatal("malformed permission event succeeded")
	}
	conn.permission = acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")}
	if err := session.handleEvent(ctx, hermesEvent{
		Type: "approval.request",
		Properties: json.RawMessage(`{
			"id":"p-session",
			"sessionID":"native-1",
			"permission":"edit",
			"patterns":["acp-permission-probe.txt"],
			"metadata":{"filepath":"acp-permission-probe.txt"},
			"tool":{"messageID":"m1","callID":"c1"}
		}`),
	}); err != nil {
		t.Fatalf("approval.request event: %v", err)
	}
	reply := client.permissionReply(client.permissionReplyCount() - 1)
	if reply.route != permissionRouteAPI || reply.requestID != "p-session" || reply.reply != "once" {
		t.Fatalf("approval.request reply = %#v", reply)
	}
	permissionReq := conn.permissions[len(conn.permissions)-1]
	if permissionReq.ToolCall.Title == nil || *permissionReq.ToolCall.Title != "edit" {
		t.Fatalf("approval.request ACP request = %#v", permissionReq)
	}
	rawInput, _ := permissionReq.ToolCall.RawInput.(map[string]any)
	resources, _ := rawInput["resources"].([]string)
	if len(resources) != 1 || resources[0] != "acp-permission-probe.txt" {
		t.Fatalf("approval.request resources = %#v", permissionReq.ToolCall.RawInput)
	}
	if err := session.handleEvent(ctx, hermesEvent{
		Type:       "clarify.request",
		Properties: json.RawMessage(`{"id":"q-v2","sessionID":"native-1","questions":[{"question":"Continue?"}]}`),
	}); err != nil {
		t.Fatalf("clarify.request event: %v", err)
	}
	questionReply := client.questionReply(client.questionReplyCount() - 1)
	if questionReply.route != questionRouteAPI || questionReply.requestID != "q-v2" {
		t.Fatalf("clarify.request reply = %#v", questionReply)
	}
}

func testForeignEventAndPartHelperBranches(t *testing.T, ctx context.Context, session *session) {
	t.Helper()

	if err := session.handleEvent(ctx, hermesEvent{Type: "todo.updated", Properties: json.RawMessage(`{"sessionID":"other","todos":[{"content":"x"}]}`)}); err != nil {
		t.Fatalf("foreign todo event: %v", err)
	}
	if err := session.handleEvent(ctx, hermesEvent{Type: "clarify.request", Properties: json.RawMessage(`{"request":{"id":"q","sessionID":"other"}}`)}); err != nil {
		t.Fatalf("foreign question event: %v", err)
	}
	if part, ok := eventPart(json.RawMessage(`{"part":{"type":"text","text":"x"}}`)); !ok || part.Text != "x" {
		t.Fatalf("wrapped eventPart = %#v ok=%v", part, ok)
	}
	if _, ok := eventPart(json.RawMessage(`{`)); ok {
		t.Fatal("malformed eventPart succeeded")
	}
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"question":{"id":"q1","sessionID":"s"}}`),
		json.RawMessage(`{"data":{"id":"q2","sessionID":"s"}}`),
		json.RawMessage(`{`),
	} {
		eventQuestion(raw)
	}
	if err := session.emitPlan(ctx, []nativeTodo{{Content: ""}}); err != nil {
		t.Fatalf("empty plan: %v", err)
	}
	rawSession := testSession(NewAgent(), newFakeHermesClient())
	rawSession.rawMessages = rawMessageConfig{enabled: true}
	if err := rawSession.emitRawHermesEvent(ctx, hermesEvent{Raw: json.RawMessage(`{"type":"x"}`)}); err != nil {
		t.Fatalf("raw event without conn: %v", err)
	}
	if usageFromTokens(nativeTokens{}) != nil {
		t.Fatal("empty tokens produced usage")
	}
	var emptyResource acp.EmbeddedResourceResource
	if got := embeddedResourceText(emptyResource); got != "" {
		t.Fatalf("empty embeddedResourceText = %q", got)
	}
	if updates := partUpdates("assistant", nativePart{Type: "text"}); updates != nil {
		t.Fatalf("empty text updates = %#v", updates)
	}
	if updates := partUpdates("assistant", nativePart{Type: "reasoning"}); updates != nil {
		t.Fatalf("empty reasoning updates = %#v", updates)
	}
}

func TestPromptRemainingErrorBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("send error after cancelled state", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(NewAgent(), client)
		client.sendMessage = func(context.Context, string, hermesMessageRequest) (nativeMessage, error) {
			session.mu.Lock()
			session.cancelled = true
			session.mu.Unlock()

			return nativeMessage{}, errors.New("cancelled send")
		}
		resp, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		if err != nil || resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("cancelled send resp=%#v err=%v", resp, err)
		}
	})

	t.Run("successful result marked cancelled", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(NewAgent(), client)
		client.sendMessage = func(context.Context, string, hermesMessageRequest) (nativeMessage, error) {
			session.mu.Lock()
			session.cancelled = true
			session.mu.Unlock()

			return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: "native-1", Role: "assistant", Finish: "stop"}}, nil
		}
		resp, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		if err != nil || resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("cancelled success resp=%#v err=%v", resp, err)
		}
	})

	t.Run("replay and emit update errors", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.updateErr = errors.New("update failed")
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.messages = []nativeMessage{{
			Info: nativeMessageInfo{ID: "user", SessionID: "native-1", Role: "user"},
			Parts: []nativePart{{
				ID:        "user-part",
				SessionID: "native-1",
				MessageID: "user",
				Type:      "text",
				Text:      "hello",
				Raw:       json.RawMessage(`{"id":"user-part"}`),
			}},
		}}
		if err := session.replayMessages(ctx); err == nil {
			t.Fatal("replayMessages ignored emit error")
		}
		client = newFakeHermesClient()
		conn = newRecordingAgentClient()
		conn.updateErr = errors.New("step update failed")
		agent = NewAgent()
		agent.setAgentClient(conn)
		session = testSession(agent, client)
		if err := session.emitMessage(ctx, nativeMessage{
			Info: nativeMessageInfo{ID: "assistant", SessionID: "native-1", Role: "assistant"},
			Parts: []nativePart{{
				ID:        "usage",
				SessionID: "native-1",
				MessageID: "assistant",
				Type:      "step-finish",
				Tokens:    nativeTokens{Total: 1},
				Raw:       json.RawMessage(`{"id":"usage"}`),
			}},
		}, false); err == nil {
			t.Fatal("emitMessage ignored step-finish update error")
		}
	})

	t.Run("duplicate part and raw notify error", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		part := nativePart{ID: "dup", SessionID: "native-1", MessageID: "assistant", Type: "text", Text: "hello"}
		if err := session.emitMessage(ctx, nativeMessage{Info: nativeMessageInfo{ID: "assistant", Role: "assistant"}, Parts: []nativePart{part, part}}, false); err != nil {
			t.Fatalf("duplicate emitMessage: %v", err)
		}
		conn.notifyErr = errors.New("notify failed")
		session.rawMessages = rawMessageConfig{enabled: true}
		if err := session.handleEvent(ctx, hermesEvent{Type: "unknown", Raw: json.RawMessage(`{"type":"unknown"}`)}); err == nil {
			t.Fatal("handleEvent ignored raw notify error")
		}
	})

	t.Run("same-session events and reconcile errors", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		conn.permErr = errors.New("permission failed")
		if err := session.handleEvent(ctx, hermesEvent{
			Type:       "approval.request",
			Properties: json.RawMessage(`{"id":"p","sessionID":"native-1"}`),
		}); err == nil {
			t.Fatal("permission event ignored client error")
		}
		client.pendingPermissions = []permissionRequest{{ID: "p2", SessionID: "native-1"}}
		if err := session.reconcilePermissions(ctx); err == nil {
			t.Fatal("reconcilePermissions ignored handle error")
		}
		conn.permErr = nil

		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		conn.elicitErr = errors.New("elicitation failed")
		if err := session.handleEvent(ctx, hermesEvent{
			Type:       "clarify.request",
			Properties: json.RawMessage(`{"id":"q","sessionID":"native-1"}`),
		}); err == nil {
			t.Fatal("question event ignored client error")
		}
		client.pendingQuestions = []questionRequest{{ID: "q2", SessionID: "native-1"}}
		if err := session.reconcileQuestions(ctx); err == nil {
			t.Fatal("reconcileQuestions ignored handle error")
		}
		if req, ok := eventQuestion(json.RawMessage(`{"id":"direct","sessionID":"native-1"}`)); !ok || req.ID != "direct" {
			t.Fatalf("direct eventQuestion = %#v ok=%v", req, ok)
		}
	})
}

func eventFromJSON(t *testing.T, raw string) hermesEvent {
	t.Helper()
	var event hermesEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatal(err)
	}

	return event
}
