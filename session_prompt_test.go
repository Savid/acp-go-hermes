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
		Type:       "permission.v2.asked",
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
		Type:       "question.asked",
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
		if err == nil || !strings.Contains(err.Error(), "hermes_sse_disconnect") {
			t.Fatalf("Prompt error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Prompt did not return")
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
		if err == nil || !strings.Contains(err.Error(), "hermes_sse_disconnect") {
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
	client.errs <- streamError{epoch: 11, err: errHermesSSEDisconnect}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "hermes_sse_disconnect") {
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
					Type:       "permission.v2.asked",
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
					Type:       "question.asked",
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
				if client.questionRejects[0].route != questionRouteSession {
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
		Type:       "permission.v2.asked",
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
		Type:       "permission.v2.asked",
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

func TestPermissionQuestionCancelledReplyBranches(t *testing.T) {
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
		{Image: &acp.ContentBlockImage{Type: "image", Data: "AA==", MimeType: "image/png"}},
	})
	if err != nil {
		t.Fatalf("promptToHermesParts: %v", err)
	}
	if len(parts) != 5 || parts[0]["text"] != "hello" || parts[1]["text"] != "file:///tmp/a" ||
		parts[2]["text"] != "embedded" || parts[3]["text"] != "file:///tmp/blob" ||
		parts[4]["type"] != "file" || parts[4]["mime"] != "image/png" || parts[4]["url"] != "data:image/png;base64,AA==" {
		t.Fatalf("parts = %#v", parts)
	}
	if _, err := promptToHermesParts(nil); err == nil {
		t.Fatal("empty prompt accepted")
	}
	if _, err := promptToHermesParts([]acp.ContentBlock{{Audio: &acp.ContentBlockAudio{Type: "audio", Data: "AA==", MimeType: "audio/wav"}}}); err == nil {
		t.Fatal("audio prompt accepted")
	}
	if _, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image"}}}); err == nil {
		t.Fatal("empty image prompt accepted")
	}
	invalidURI := "%"
	parts, err = promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image", Uri: &invalidURI}}})
	if err != nil || parts[0]["filename"] != nil || parts[0]["mime"] != "application/octet-stream" || parts[0]["url"] != invalidURI {
		t.Fatalf("invalid uri image parts = %#v err=%v", parts, err)
	}
	rootURI := "https://example.com"
	parts, err = promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image", Uri: &rootURI}}})
	if err != nil || parts[0]["filename"] != nil || parts[0]["url"] != rootURI {
		t.Fatalf("root uri image parts = %#v err=%v", parts, err)
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

func TestPromptSendsNativeImageFileParts(t *testing.T) {
	client := newFakeHermesClient()
	agent := NewAgent()
	session := testSession(agent, client)
	imageURI := "file:///tmp/screenshot.png"
	client.sendMessage = func(_ context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
		want := []map[string]any{
			{"type": "text", "text": "look"},
			{"type": "file", "mime": "image/png", "url": "data:image/png;base64,AA=="},
			{"type": "file", "mime": "image/jpeg", "url": "file:///tmp/screenshot.png", "filename": "screenshot.png"},
		}
		if !reflect.DeepEqual(req.Parts, want) {
			t.Fatalf("native parts = %#v, want %#v", req.Parts, want)
		}
		return nativeMessage{Info: nativeMessageInfo{ID: "assistant-1", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
	}
	_, err := session.Prompt(context.Background(), acp.PromptRequest{
		SessionId: session.id,
		Prompt: []acp.ContentBlock{
			acp.TextBlock("look"),
			{Image: &acp.ContentBlockImage{Type: "image", Data: "AA==", MimeType: "image/png"}},
			{Image: &acp.ContentBlockImage{Type: "image", Uri: &imageURI, MimeType: "image/jpeg"}},
		},
	})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
}

func TestSlashCommandRefreshAdvertisesNativeListWithSanitizer(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.commands = []nativeCommand{
		{Name: "init", Description: "Initialize", Source: "command", Template: "must not leak", Hints: []string{"$ARGUMENTS", "$1"}},
		{Name: "mcp:server:prompt", Source: "mcp"},
		{Name: ""},
		{Name: "/bad"},
		{Name: "bad/name"},
		{Name: "bad name"},
		{Name: "bad\nname"},
		{Name: "bad\u0007name"},
		{Name: "bad\u200dname"},
		{Name: string([]byte{0xff})},
	}
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	if err := session.refreshCommands(ctx); err != nil {
		t.Fatalf("refreshCommands: %v", err)
	}
	if conn.updateCount() != 1 {
		t.Fatalf("updates = %#v", conn.updates)
	}
	update := conn.updates[0].Update.AvailableCommandsUpdate
	if update == nil {
		t.Fatalf("update = %#v", conn.updates[0].Update)
	}
	if got := commandNames(update.AvailableCommands); !reflect.DeepEqual(got, []string{"init", "mcp:server:prompt"}) {
		t.Fatalf("advertised names = %#v", got)
	}
	if update.AvailableCommands[0].Input == nil ||
		update.AvailableCommands[0].Input.Unstructured == nil ||
		update.AvailableCommands[0].Input.Unstructured.Hint != "$ARGUMENTS $1" {
		t.Fatalf("hint = %#v", update.AvailableCommands[0].Input)
	}
	if update.AvailableCommands[1].Description != "Hermes mcp command" {
		t.Fatalf("synthesized description = %q", update.AvailableCommands[1].Description)
	}
	raw, err := json.Marshal(conn.updates)
	if err != nil {
		t.Fatalf("marshal updates: %v", err)
	}
	if strings.Contains(string(raw), "template") || strings.Contains(string(raw), "must not leak") {
		t.Fatalf("template leaked in update: %s", raw)
	}
	if _, ok := session.cachedCommand("mcp:server:prompt"); !ok {
		t.Fatal("colon command was not routable")
	}
	for _, invalid := range []string{"", "/bad", "bad/name", "bad name", "bad\nname", "bad\u0007name", "bad\u200dname", string([]byte{0xff})} {
		if _, ok := session.cachedCommand(invalid); ok {
			t.Fatalf("invalid command %q was routable", invalid)
		}
	}
	if got := availableCommandFromNative(nativeCommand{Name: "native"}).Description; got != "Hermes command" {
		t.Fatalf("empty source description = %q", got)
	}
	cloned := cloneAvailableCommands([]acp.AvailableCommand{{Name: "meta", Description: "Meta", Meta: map[string]any{"k": "v"}}})
	cloned[0].Meta["k"] = "changed"
	if cloned[0].Meta["k"] == "v" {
		t.Fatal("metadata clone did not return mutable copy")
	}
}

func TestSlashCommandRefreshEmptyClearAndFailureKeepsCache(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	if err := session.refreshCommands(ctx); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	if conn.updateCount() != 0 {
		t.Fatalf("initial empty update emitted: %#v", conn.updates)
	}

	client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
	if err := session.refreshCommands(ctx); err != nil {
		t.Fatalf("non-empty refresh: %v", err)
	}
	if conn.updateCount() != 1 {
		t.Fatalf("updates after non-empty = %#v", conn.updates)
	}
	if err := session.refreshCommands(ctx); err != nil {
		t.Fatalf("unchanged refresh: %v", err)
	}
	if conn.updateCount() != 1 {
		t.Fatalf("unchanged refresh emitted: %#v", conn.updates)
	}

	client.commandsErr = errors.New("commands failed")
	if err := session.refreshCommands(ctx); err == nil {
		t.Fatal("refresh failure returned nil")
	}
	if conn.updateCount() != 1 {
		t.Fatalf("failed refresh emitted update: %#v", conn.updates)
	}
	if _, ok := session.cachedCommand("review"); !ok {
		t.Fatal("failed refresh did not keep last good command")
	}

	client.commandsErr = nil
	client.commands = nil
	if err := session.refreshCommands(ctx); err != nil {
		t.Fatalf("clear refresh: %v", err)
	}
	if conn.updateCount() != 2 {
		t.Fatalf("clear update missing: %#v", conn.updates)
	}
	clearUpdate := conn.updates[1].Update.AvailableCommandsUpdate
	if clearUpdate == nil || len(clearUpdate.AvailableCommands) != 0 {
		t.Fatalf("clear update = %#v", clearUpdate)
	}
	wire, err := json.Marshal(conn.updates[1])
	if err != nil {
		t.Fatalf("marshal clear update: %v", err)
	}
	if !strings.Contains(string(wire), `"availableCommands":[]`) {
		t.Fatalf("clear update JSON = %s", wire)
	}
	if strings.Contains(string(wire), `"availableCommands":null`) {
		t.Fatalf("clear update JSON used null: %s", wire)
	}
}

func TestPromptSlashCommandRouting(t *testing.T) {
	ctx := context.Background()
	t.Run("exact match routes to native command", func(t *testing.T) {
		client := newFakeHermesClient()
		client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
		agent := NewAgent()
		session := testSession(agent, client)
		messageID := "msg-user"
		client.runCommand = func(_ context.Context, id string, req hermesCommandRequest) (nativeMessage, error) {
			if id != "native-1" {
				t.Fatalf("native id = %q", id)
			}
			if req.MessageID != messageID || req.Agent != "build" || req.Model != "openai/gpt-test" ||
				req.Command != "review" || req.Arguments != " inspect this" || len(req.Parts) != 0 {
				t.Fatalf("command request = %#v", req)
			}
			return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		resp, err := session.Prompt(ctx, acp.PromptRequest{
			SessionId: session.id,
			MessageId: &messageID,
			Prompt:    []acp.ContentBlock{acp.TextBlock("/review  inspect this")},
		})
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
		if resp.StopReason != acp.StopReasonEndTurn || resp.UserMessageId == nil || *resp.UserMessageId != messageID {
			t.Fatalf("response = %#v", resp)
		}
	})

	t.Run("unmatched slash and leading whitespace are plain messages", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			text string
		}{
			{name: "unmatched", text: "/missing args"},
			{name: "escaped", text: " /review args"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				client := newFakeHermesClient()
				client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
				agent := NewAgent()
				session := testSession(agent, client)
				client.sendMessage = func(_ context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
					if req.Parts[0]["text"] != tt.text {
						t.Fatalf("plain text part = %#v", req.Parts)
					}
					return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
				}
				if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock(tt.text)}}); err != nil {
					t.Fatalf("Prompt: %v", err)
				}
			})
		}
	})

	t.Run("refresh failure before slash prompt keeps plain path", func(t *testing.T) {
		client := newFakeHermesClient()
		client.commandsErr = errors.New("commands failed")
		agent := NewAgent()
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
			if req.Parts[0]["text"] != "/missing args" {
				t.Fatalf("plain text part = %#v", req.Parts)
			}
			return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("/missing args")}}); err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	})

	t.Run("pre-prompt refresh removed cached command", func(t *testing.T) {
		client := newFakeHermesClient()
		client.commands = []nativeCommand{{Name: "stale", Description: "Stale", Source: "command"}}
		conn := newRecordingAgentClient()
		agent := NewAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		if err := session.refreshCommands(ctx); err != nil {
			t.Fatalf("initial refresh: %v", err)
		}
		if conn.updateCount() != 1 {
			t.Fatalf("initial updates = %#v", conn.updates)
		}
		client.commands = nil
		client.sendMessage = func(context.Context, string, hermesMessageRequest) (nativeMessage, error) {
			t.Fatal("removed command fell back to plain message")
			return nativeMessage{}, nil
		}
		client.runCommand = func(context.Context, string, hermesCommandRequest) (nativeMessage, error) {
			t.Fatal("removed command was sent to native command endpoint")
			return nativeMessage{}, nil
		}

		_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("/stale now")}})
		if err == nil || !strings.Contains(err.Error(), "hermes_command_removed") || !strings.Contains(err.Error(), "stale") {
			t.Fatalf("removed command error = %v", err)
		}
		if conn.updateCount() != 2 {
			t.Fatalf("updates = %#v", conn.updates)
		}
		clearUpdate := conn.updates[1].Update.AvailableCommandsUpdate
		if clearUpdate == nil || len(clearUpdate.AvailableCommands) != 0 {
			t.Fatalf("refreshed clear update = %#v", clearUpdate)
		}
	})

	t.Run("custom shadowing fixture routes exact name as data", func(t *testing.T) {
		client := newFakeHermesClient()
		client.commands = []nativeCommand{{Name: "init", Description: "Workspace init", Source: "command"}}
		session := testSession(NewAgent(), client)
		client.sendMessage = func(context.Context, string, hermesMessageRequest) (nativeMessage, error) {
			t.Fatal("shadowing command fell back to plain message")
			return nativeMessage{}, nil
		}
		client.runCommand = func(_ context.Context, id string, req hermesCommandRequest) (nativeMessage, error) {
			if req.Command != "init" || req.Arguments != " custom args" {
				t.Fatalf("shadow command request = %#v", req)
			}
			return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("/init  custom args")}}); err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	})
}

func TestPromptSlashCommandMixedContent(t *testing.T) {
	ctx := context.Background()
	t.Run("matched command sends file parts", func(t *testing.T) {
		client := newFakeHermesClient()
		client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
		session := testSession(NewAgent(), client)
		imageURI := "file:///tmp/screenshot.png"
		resourceMime := "text/plain"
		blobMime := "application/octet-stream"
		client.runCommand = func(_ context.Context, id string, req hermesCommandRequest) (nativeMessage, error) {
			want := []map[string]any{
				{"type": "file", "mime": "image/png", "url": "data:image/png;base64,AA=="},
				{"type": "file", "mime": "image/jpeg", "url": "file:///tmp/screenshot.png", "filename": "screenshot.png"},
				{"type": "file", "mime": "text/plain", "url": "file:///tmp/notes.txt", "filename": "notes.txt"},
				{"type": "file", "mime": "application/octet-stream", "url": "data:application/octet-stream;base64,AA==", "filename": "blob.bin"},
			}
			if !reflect.DeepEqual(req.Parts, want) {
				t.Fatalf("command parts = %#v, want %#v", req.Parts, want)
			}
			return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		_, err := session.Prompt(ctx, acp.PromptRequest{
			SessionId: session.id,
			Prompt: []acp.ContentBlock{
				acp.TextBlock("/review"),
				{Image: &acp.ContentBlockImage{Type: "image", Data: "AA==", MimeType: "image/png"}},
				{Image: &acp.ContentBlockImage{Type: "image", Uri: &imageURI, MimeType: "image/jpeg"}},
				{ResourceLink: &acp.ContentBlockResourceLink{Type: "resource_link", Uri: "file:///tmp/notes.txt", MimeType: &resourceMime}},
				{Resource: &acp.ContentBlockResource{Type: "resource", Resource: acp.EmbeddedResourceResource{
					BlobResourceContents: &acp.BlobResourceContents{Blob: "AA==", Uri: "file:///tmp/blob.bin", MimeType: &blobMime},
				}}},
			},
		})
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	})

	t.Run("matched command rejects unconvertible block type", func(t *testing.T) {
		client := newFakeHermesClient()
		client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
		session := testSession(NewAgent(), client)
		client.runCommand = func(context.Context, string, hermesCommandRequest) (nativeMessage, error) {
			t.Fatal("RunCommand called for unconvertible block")
			return nativeMessage{}, nil
		}
		_, err := session.Prompt(ctx, acp.PromptRequest{
			SessionId: session.id,
			Prompt: []acp.ContentBlock{
				acp.TextBlock("/review"),
				{Audio: &acp.ContentBlockAudio{Type: "audio", Data: "AA==", MimeType: "audio/wav"}},
			},
		})
		if err == nil || !strings.Contains(err.Error(), "audio") {
			t.Fatalf("unconvertible block error = %v", err)
		}
	})

	t.Run("command part conversion errors name block types", func(t *testing.T) {
		if _, err := commandPromptParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{Type: "image"}}}); err == nil {
			t.Fatal("empty command image accepted")
		}
		if _, err := commandPromptParts([]acp.ContentBlock{acp.TextBlock("extra text")}); err == nil ||
			!strings.Contains(err.Error(), "text") {
			t.Fatalf("text command part error = %v", err)
		}
		if _, err := commandPromptParts([]acp.ContentBlock{{Resource: &acp.ContentBlockResource{Type: "resource"}}}); err == nil ||
			!strings.Contains(err.Error(), "resource") {
			t.Fatalf("resource command part error = %v", err)
		}
		if _, err := commandPromptParts([]acp.ContentBlock{{}}); err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Fatalf("unknown command part error = %v", err)
		}
		if _, err := blobResourceHermesPart(&acp.BlobResourceContents{}); err == nil {
			t.Fatal("empty blob resource accepted")
		}
		named := resourceLinkHermesPart(&acp.ContentBlockResourceLink{Name: "named.txt", Uri: "file:///tmp/ignored"})
		if named["mime"] != "application/octet-stream" || named["filename"] != "named.txt" {
			t.Fatalf("named resource link part = %#v", named)
		}
		if got := contentBlockType(acp.ContentBlock{Image: &acp.ContentBlockImage{}}); got != "image" {
			t.Fatalf("image block type = %q", got)
		}
		if got := contentBlockType(acp.ContentBlock{ResourceLink: &acp.ContentBlockResourceLink{}}); got != "resource_link" {
			t.Fatalf("resource link block type = %q", got)
		}
	})

	t.Run("unmatched slash keeps supported mixed content as plain message", func(t *testing.T) {
		client := newFakeHermesClient()
		client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
		session := testSession(NewAgent(), client)
		client.sendMessage = func(_ context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
			if len(req.Parts) != 2 || req.Parts[0]["text"] != "/missing" || req.Parts[1]["type"] != "file" {
				t.Fatalf("plain mixed parts = %#v", req.Parts)
			}
			return nativeMessage{Info: nativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		_, err := session.Prompt(ctx, acp.PromptRequest{
			SessionId: session.id,
			Prompt: []acp.ContentBlock{
				acp.TextBlock("/missing"),
				{Image: &acp.ContentBlockImage{Type: "image", Data: "AA==", MimeType: "image/png"}},
			},
		})
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	})
}

func TestPromptSlashCommandStaleRaceRefreshesWithoutPlainRetry(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name       string
		refreshErr bool
	}{
		{name: "refresh clears deleted command"},
		{name: "refresh failure is logged", refreshErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeHermesClient()
			client.commands = []nativeCommand{{Name: "stale", Description: "Stale", Source: "command"}}
			client.sendMessage = func(context.Context, string, hermesMessageRequest) (nativeMessage, error) {
				t.Fatal("stale command retried as plain prompt")
				return nativeMessage{}, nil
			}
			client.runCommand = func(context.Context, string, hermesCommandRequest) (nativeMessage, error) {
				if tt.refreshErr {
					client.commandsErr = errors.New("refresh failed")
				} else {
					client.commands = nil
				}
				return nativeMessage{}, &hermesHTTPError{Method: "POST", Path: "/session/native-1/command", Status: "400 Bad Request", StatusCode: 400, Body: "unknown command"}
			}
			conn := newRecordingAgentClient()
			agent := NewAgent()
			agent.setAgentClient(conn)
			session := testSession(agent, client)

			_, err := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("/stale now")}})
			if err == nil || !strings.Contains(err.Error(), "hermes_command_bad_request") {
				t.Fatalf("stale race error = %v", err)
			}
			if tt.refreshErr {
				if conn.updateCount() != 1 {
					t.Fatalf("failed refresh updates = %#v", conn.updates)
				}
				return
			}
			if conn.updateCount() != 2 {
				t.Fatalf("updates = %#v", conn.updates)
			}
			clearUpdate := conn.updates[1].Update.AvailableCommandsUpdate
			if clearUpdate == nil || len(clearUpdate.AvailableCommands) != 0 {
				t.Fatalf("refreshed clear update = %#v", clearUpdate)
			}
		})
	}
}

func TestPromptSlashCommandExclusiveTurn(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentPrompts: 2}))
	session := testSession(agent, client)
	release, err := session.acquireTurn(ctx)
	if err != nil {
		t.Fatalf("acquire normal turn: %v", err)
	}
	defer release()
	_, err = session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("/review")}})
	if err == nil || !strings.Contains(err.Error(), "backpressure") {
		t.Fatalf("command during active prompt error = %v", err)
	}
}

func commandNames(commands []acp.AvailableCommand) []string {
	names := make([]string, 0, len(commands))
	for _, command := range commands {
		names = append(names, command.Name)
	}
	return names
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
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan acp.PromptResponse, 1)
		go func() {
			resp, _ := session.Prompt(ctx, acp.PromptRequest{SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
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
		if _, err := agent.Prompt(ctx, acp.PromptRequest{SessionId: "missing"}); err == nil {
			t.Fatal("unknown agent prompt succeeded")
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
		client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
		conn := newRecordingAgentClient()
		store := newCountingSessionStore()
		agent := NewAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		if err := session.refreshCommands(ctx); err != nil {
			t.Fatalf("refreshCommands: %v", err)
		}
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
		client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
		conn := newRecordingAgentClient()
		store := newCountingSessionStore()
		agent := NewAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		if err := session.refreshCommands(ctx); err != nil {
			t.Fatalf("refreshCommands: %v", err)
		}
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
		client.commands = []nativeCommand{{Name: "review", Description: "Review", Source: "command"}}
		conn := newRecordingAgentClient()
		store := newCountingSessionStore()
		agent := NewAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		if err := session.refreshCommands(ctx); err != nil {
			t.Fatalf("refreshCommands: %v", err)
		}
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
		t.Fatalf("poison without commands emitted updates: %#v", conn.updates)
	}
	if err := s.poison(ctx, "second poison"); err == nil ||
		!strings.Contains(err.Error(), "session_poisoned") ||
		!strings.Contains(err.Error(), "native drift without advertisement") {
		t.Fatalf("second poison error = %v", err)
	}
	if _, err := s.acquireTurn(ctx); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("acquire poisoned session error = %v", err)
	}
	if err := s.refreshCommands(ctx); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("refresh poisoned session error = %v", err)
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
	if conn.updateCount() != 2 {
		t.Fatalf("updates after poison = %#v", conn.updates)
	}
	clearUpdate := conn.updates[1].Update.AvailableCommandsUpdate
	if clearUpdate == nil || clearUpdate.AvailableCommands == nil || len(clearUpdate.AvailableCommands) != 0 {
		t.Fatalf("poison clear update = %#v", clearUpdate)
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
	if err := session.handleEvent(ctx, hermesEvent{Type: "permission.v2.asked", Properties: json.RawMessage(`{`)}); err == nil {
		t.Fatal("malformed permission event succeeded")
	}
	conn.permission = acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")}
	if err := session.handleEvent(ctx, hermesEvent{
		Type: "permission.asked",
		Properties: json.RawMessage(`{
			"id":"p-session",
			"sessionID":"native-1",
			"permission":"edit",
			"patterns":["acp-permission-probe.txt"],
			"metadata":{"filepath":"acp-permission-probe.txt"},
			"tool":{"messageID":"m1","callID":"c1"}
		}`),
	}); err != nil {
		t.Fatalf("permission.asked event: %v", err)
	}
	reply := client.permissionReply(client.permissionReplyCount() - 1)
	if reply.route != permissionRouteSession || reply.requestID != "p-session" || reply.reply != "once" {
		t.Fatalf("permission.asked reply = %#v", reply)
	}
	permissionReq := conn.permissions[len(conn.permissions)-1]
	if permissionReq.ToolCall.Title == nil || *permissionReq.ToolCall.Title != "edit" {
		t.Fatalf("permission.asked ACP request = %#v", permissionReq)
	}
	rawInput, _ := permissionReq.ToolCall.RawInput.(map[string]any)
	resources, _ := rawInput["resources"].([]string)
	if len(resources) != 1 || resources[0] != "acp-permission-probe.txt" {
		t.Fatalf("permission.asked resources = %#v", permissionReq.ToolCall.RawInput)
	}
	if err := session.handleEvent(ctx, hermesEvent{
		Type:       "question.v2.asked",
		Properties: json.RawMessage(`{"id":"q-v2","sessionID":"native-1","questions":[{"question":"Continue?"}]}`),
	}); err != nil {
		t.Fatalf("question.v2.asked event: %v", err)
	}
	questionReply := client.questionReply(client.questionReplyCount() - 1)
	if questionReply.route != questionRouteAPI || questionReply.requestID != "q-v2" {
		t.Fatalf("question.v2 reply = %#v", questionReply)
	}
	if err := session.handleEvent(ctx, hermesEvent{Type: "todo.updated", Properties: json.RawMessage(`{"sessionID":"other","todos":[{"content":"x"}]}`)}); err != nil {
		t.Fatalf("foreign todo event: %v", err)
	}
	if err := session.handleEvent(ctx, hermesEvent{Type: "question.asked", Properties: json.RawMessage(`{"request":{"id":"q","sessionID":"other"}}`)}); err != nil {
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
			Type:       "permission.v2.asked",
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
			Type:       "question.asked",
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
