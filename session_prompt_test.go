package hermesacp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

type strictHermesPermissionClient struct {
	*recordingAgentClient

	expectedTurnNonce string
	toolStates        map[acp.ToolCallId]acp.ToolCallStatus
	order             []string
}

type sessionUpdateHookClient struct {
	*recordingAgentClient
	afterUpdate func()
}

func (c *sessionUpdateHookClient) SessionUpdate(
	ctx context.Context,
	notification acp.SessionNotification,
) error {
	err := c.recordingAgentClient.SessionUpdate(ctx, notification)
	if err == nil && c.afterUpdate != nil {
		c.afterUpdate()
	}

	return err
}

func newStrictHermesPermissionClient(turnNonce string) *strictHermesPermissionClient {
	return &strictHermesPermissionClient{
		recordingAgentClient: newRecordingAgentClient(),
		expectedTurnNonce:    turnNonce,
		toolStates:           map[acp.ToolCallId]acp.ToolCallStatus{},
	}
}

func (c *strictHermesPermissionClient) SessionUpdate(
	ctx context.Context,
	notification acp.SessionNotification,
) error {
	if !reflect.DeepEqual(notification.Meta, turnRouteMeta(c.expectedTurnNonce)) {
		return fmt.Errorf("session update route = %#v, want %#v", notification.Meta, turnRouteMeta(c.expectedTurnNonce))
	}

	c.mu.Lock()
	if start := notification.Update.ToolCall; start != nil {
		c.toolStates[start.ToolCallId] = start.Status
		c.order = append(c.order, fmt.Sprintf("start:%s:%s", start.ToolCallId, start.Status))
	}

	if update := notification.Update.ToolCallUpdate; update != nil && update.Status != nil {
		c.toolStates[update.ToolCallId] = *update.Status
		c.order = append(c.order, fmt.Sprintf("update:%s:%s", update.ToolCallId, *update.Status))
	}
	c.mu.Unlock()

	return c.recordingAgentClient.SessionUpdate(ctx, notification)
}

func (c *strictHermesPermissionClient) RequestPermission(
	ctx context.Context,
	request acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	status, exists := c.toolStates[request.ToolCall.ToolCallId]
	if !exists || hermesToolStatusTerminal(status) {
		c.mu.Unlock()

		return acp.RequestPermissionResponse{}, fmt.Errorf(
			"permission tool %q is not pending",
			request.ToolCall.ToolCallId,
		)
	}

	c.order = append(c.order, "permission:"+string(request.ToolCall.ToolCallId))
	c.mu.Unlock()

	return c.recordingAgentClient.RequestPermission(ctx, request)
}

func (c *strictHermesPermissionClient) orderSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.order...)
}

func testHermesPermissionRequest(t *testing.T, requestID string, toolCallID string) nativehermes.PermissionRequest {
	t.Helper()

	raw := fmt.Sprintf(
		`{"id":%q,"sessionID":"native-1","action":"edit","tool":{"messageID":"message-1","callID":%q}}`,
		requestID,
		toolCallID,
	)

	var req nativehermes.PermissionRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatalf("decode permission request: %v", err)
	}

	return req
}

func TestPermissionPublishesExactNativeToolPendingBeforeCallback(t *testing.T) {
	const turnNonce = "permission-turn"

	client := newFakeHermesClient()
	conn := newStrictHermesPermissionClient(turnNonce)
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	turnCtx := session.beginTurn(t.Context(), turnNonce)
	defer session.finishTurn()

	req := testHermesPermissionRequest(t, "native-request-distinct", "native-tool-distinct")
	if err := session.handlePermission(turnCtx, req); err != nil {
		t.Fatalf("handlePermission: %v", err)
	}

	if got := conn.orderSnapshot(); !reflect.DeepEqual(got, []string{
		"start:native-tool-distinct:pending",
		"permission:native-tool-distinct",
	}) {
		t.Fatalf("permission order = %#v", got)
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.permissions) != 1 || conn.permissions[0].ToolCall.ToolCallId != "native-tool-distinct" {
		t.Fatalf("permission request = %#v", conn.permissions)
	}

	if len(conn.updates) != 1 || !reflect.DeepEqual(conn.updates[0].Meta, turnRouteMeta(turnNonce)) {
		t.Fatalf("pending notification = %#v", conn.updates)
	}
}

func TestPermissionNativeStartAndSyntheticPendingShareOneLifecycle(t *testing.T) {
	const turnNonce = "permission-turn"

	t.Run("native start wins", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newStrictHermesPermissionClient(turnNonce)
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(t.Context(), turnNonce)
		defer session.finishTurn()

		part := nativehermes.Part{
			ID:     "part-native-first",
			CallID: "native-tool-first",
			Type:   valTool,
			Tool:   valEdit,
			State:  json.RawMessage(`{"status":"pending"}`),
			Raw:    json.RawMessage(`{"callID":"native-tool-first"}`),
		}
		if err := session.emitPartUpdates(turnCtx, valAssistant, part); err != nil {
			t.Fatalf("emit native pending: %v", err)
		}

		if err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "request-first", "native-tool-first")); err != nil {
			t.Fatalf("handlePermission: %v", err)
		}

		if got := conn.orderSnapshot(); !reflect.DeepEqual(got, []string{
			"start:native-tool-first:pending",
			"permission:native-tool-first",
		}) {
			t.Fatalf("permission order = %#v", got)
		}
	})

	t.Run("permission start wins", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newStrictHermesPermissionClient(turnNonce)
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(t.Context(), turnNonce)
		defer session.finishTurn()

		if err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "request-synthetic", "native-tool-synthetic")); err != nil {
			t.Fatalf("handlePermission: %v", err)
		}

		part := nativehermes.Part{
			ID:     "part-synthetic-first",
			CallID: "native-tool-synthetic",
			Type:   valTool,
			Tool:   valEdit,
			State:  json.RawMessage(`{"status":"running"}`),
			Raw:    json.RawMessage(`{"callID":"native-tool-synthetic"}`),
		}
		if err := session.emitPartUpdates(turnCtx, valAssistant, part); err != nil {
			t.Fatalf("emit native running: %v", err)
		}

		if got := conn.orderSnapshot(); !reflect.DeepEqual(got, []string{
			"start:native-tool-synthetic:pending",
			"permission:native-tool-synthetic",
			"update:native-tool-synthetic:in_progress",
		}) {
			t.Fatalf("permission order = %#v", got)
		}
	})
}

func TestPermissionRejectsMissingStaleAndTerminalToolRoutes(t *testing.T) {
	const turnNonce = "permission-turn"

	for _, test := range []struct {
		name  string
		ctx   func(context.Context) context.Context
		setup func(*testing.T, *session, context.Context)
		req   func(*testing.T) nativehermes.PermissionRequest
	}{
		{
			name: "missing native tool id",
			ctx:  func(ctx context.Context) context.Context { return ctx },
			req: func(t *testing.T) nativehermes.PermissionRequest {
				t.Helper()

				req := testHermesPermissionRequest(t, "request-missing", "tool-placeholder")
				req.Tool.CallID = ""

				return req
			},
		},
		{
			name: "missing turn route",
			ctx:  func(context.Context) context.Context { return context.Background() },
			req: func(t *testing.T) nativehermes.PermissionRequest {
				t.Helper()

				return testHermesPermissionRequest(t, "request-missing-route", "tool-missing-route")
			},
		},
		{
			name: "stale turn route",
			ctx:  func(ctx context.Context) context.Context { return withTurnRoute(ctx, "stale-turn") },
			req: func(t *testing.T) nativehermes.PermissionRequest {
				t.Helper()

				return testHermesPermissionRequest(t, "request-stale", "tool-stale")
			},
		},
		{
			name: "terminal tool",
			ctx:  func(ctx context.Context) context.Context { return ctx },
			setup: func(t *testing.T, session *session, ctx context.Context) {
				t.Helper()
				part := nativehermes.Part{CallID: "tool-terminal", Type: valTool, Tool: valEdit, State: json.RawMessage(`{"status":"completed"}`)}
				if err := session.emitPartUpdates(ctx, valAssistant, part); err != nil {
					t.Fatalf("emit terminal tool: %v", err)
				}
			},
			req: func(t *testing.T) nativehermes.PermissionRequest {
				t.Helper()

				return testHermesPermissionRequest(t, "request-terminal", "tool-terminal")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeHermesClient()
			conn := newStrictHermesPermissionClient(turnNonce)
			agent := newTestAgent()
			agent.setAgentClient(conn)
			session := testSession(agent, client)
			turnCtx := session.beginTurn(t.Context(), turnNonce)
			defer session.finishTurn()

			if test.setup != nil {
				test.setup(t, session, turnCtx)
			}

			err := session.handlePermission(test.ctx(turnCtx), test.req(t))
			if err == nil {
				t.Fatal("invalid permission succeeded")
			}

			if conn.permissionRequestCount() != 0 {
				t.Fatalf("permission callback count = %d", conn.permissionRequestCount())
			}
		})
	}
}

func TestPermissionToolStateRemainingBranches(t *testing.T) {
	t.Run("route and status helpers", func(t *testing.T) {
		var nilContext context.Context
		if turnNonceFromContext(nilContext) != "" {
			t.Fatal("nil context produced a turn nonce")
		}
		if hermesToolStatusRank(acp.ToolCallStatusCompleted) != 3 ||
			hermesToolStatusRank(acp.ToolCallStatusFailed) != 3 ||
			hermesToolStatusRank(acp.ToolCallStatus("unknown")) != 0 {
			t.Fatal("tool status ranks are incomplete")
		}

		terminal := hermesToolState{status: acp.ToolCallStatusCompleted}
		if _, merged, changed := updateHermesToolCall("tool", terminal, hermesToolState{status: acp.ToolCallStatusInProgress}); changed || merged != terminal {
			t.Fatalf("terminal state regressed: changed=%v merged=%#v", changed, merged)
		}

		previous := hermesToolState{
			title: "same", kind: acp.ToolKindOther,
			status: acp.ToolCallStatusInProgress, rawInput: map[string]any{"same": true},
		}
		if _, merged, changed := updateHermesToolCall("tool", previous, previous); changed || !reflect.DeepEqual(merged, previous) {
			t.Fatalf("identical state emitted update: changed=%v merged=%#v", changed, merged)
		}

		next := hermesToolState{
			title: "renamed", kind: acp.ToolKindEdit,
			status: acp.ToolCallStatusPending, rawInput: map[string]any{"next": true}, rawOutput: map[string]any{"done": true},
		}
		update, merged, changed := updateHermesToolCall("tool", previous, next)
		if !changed || update.ToolCallUpdate == nil || merged.status != acp.ToolCallStatusInProgress ||
			merged.title != next.title || merged.kind != next.kind || !reflect.DeepEqual(merged.rawInput, next.rawInput) ||
			!reflect.DeepEqual(update.ToolCallUpdate.RawOutput, next.rawOutput) {
			t.Fatalf("merged update = %#v update=%#v changed=%v", merged, update, changed)
		}
	})

	t.Run("tool publication failures and duplicate", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(t.Context(), "tool-turn")
		defer session.finishTurn()

		part := nativehermes.Part{CallID: "tool", Type: valTool, Tool: valEdit, State: json.RawMessage(`{"status":"running"}`)}
		conn.updateErr = errors.New("start failed")
		if err := session.emitToolPartUpdate(turnCtx, part); err == nil || !strings.Contains(err.Error(), "start failed") {
			t.Fatalf("start publication error = %v", err)
		}

		conn.updateErr = nil
		if err := session.emitToolPartUpdate(turnCtx, part); err != nil {
			t.Fatalf("start publication: %v", err)
		}
		if err := session.emitToolPartUpdate(turnCtx, part); err != nil {
			t.Fatalf("identical duplicate: %v", err)
		}

		conn.updateErr = errors.New("update failed")
		part.State = json.RawMessage(`{"status":"completed","title":"done"}`)
		if err := session.emitToolPartUpdate(turnCtx, part); err == nil || !strings.Contains(err.Error(), "update failed") {
			t.Fatalf("update publication error = %v", err)
		}
	})
}

func TestPermissionAdmissionRemainingBranches(t *testing.T) {
	t.Run("pending admission stale and publication error", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(t.Context(), "permission-turn")
		route, active := session.permissionTurnRoute(turnCtx)
		if !active {
			t.Fatal("permission turn was not active")
		}
		req := testHermesPermissionRequest(t, "request", "tool")

		session.mu.Lock()
		session.turnNonce = "new-turn"
		session.turnEpoch++
		session.mu.Unlock()
		if err := session.ensurePermissionToolPending(turnCtx, req, route); err == nil || !strings.Contains(err.Error(), "crossed") {
			t.Fatalf("stale pending admission error = %v", err)
		}

		session.mu.Lock()
		session.turnNonce = "permission-turn"
		session.turnEpoch = route.epoch
		session.mu.Unlock()
		conn.updateErr = errors.New("pending failed")
		if err := session.ensurePermissionToolPending(turnCtx, req, route); err == nil || !strings.Contains(err.Error(), "pending failed") {
			t.Fatalf("pending publication error = %v", err)
		}
		session.finishTurn()
	})

	t.Run("route changes after pending publication", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newTestAgent()
		session := testSession(agent, client)
		base := newRecordingAgentClient()
		conn := &sessionUpdateHookClient{recordingAgentClient: base}
		conn.afterUpdate = func() {
			session.mu.Lock()
			session.turnNonce = "replacement-turn"
			session.turnEpoch++
			session.mu.Unlock()
		}
		agent.setAgentClient(conn)
		turnCtx := session.beginTurn(t.Context(), "permission-turn")
		defer session.finishTurn()

		err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "request", "tool"))
		if err == nil || !strings.Contains(err.Error(), "crossed its active turn") {
			t.Fatalf("post-publication route error = %v", err)
		}
		if base.permissionRequestCount() != 0 {
			t.Fatalf("stale callback reached permission client: %d", base.permissionRequestCount())
		}
	})

	t.Run("connection disappears after pending publication", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newTestAgent()
		session := testSession(agent, client)
		turnCtx := session.beginTurn(t.Context(), "permission-turn")
		defer session.finishTurn()
		conn := &sessionUpdateHookClient{recordingAgentClient: newRecordingAgentClient()}
		conn.afterUpdate = func() {
			agent.setAgentClient(nil)
		}
		agent.setAgentClient(conn)

		if err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "request", "tool")); err != nil {
			t.Fatalf("missing connection rejection: %v", err)
		}
		if got := client.permissionReply(0); got.message != "client unavailable" || got.reply != valReject {
			t.Fatalf("missing connection reply = %#v", got)
		}
	})
}

func TestPermissionCallbackRouteChangeBranches(t *testing.T) {
	for _, test := range []struct {
		name     string
		permErr  error
		replyErr error
	}{
		{name: "permission error after route change", permErr: errors.New("permission failed")},
		{name: "successful response after route change"},
		{name: "successful response after route change with reply failure", replyErr: errors.New("reply failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeHermesClient()
			client.replyErr = test.replyErr
			conn := newRecordingAgentClient()
			conn.permErr = test.permErr
			conn.permissionStarted = make(chan struct{}, 1)
			conn.permissionRelease = make(chan struct{})
			conn.permissionIgnoreContext = true
			agent := newTestAgent()
			agent.setAgentClient(conn)
			session := testSession(agent, client)
			turnCtx := session.beginTurn(t.Context(), "permission-turn")
			defer session.finishTurn()
			done := make(chan error, 1)
			go func() {
				done <- session.handlePermission(turnCtx, testHermesPermissionRequest(t, "request", "tool"))
			}()
			<-conn.permissionStarted
			session.mu.Lock()
			session.turnNonce = "replacement-turn"
			session.turnEpoch++
			session.mu.Unlock()
			close(conn.permissionRelease)
			err := <-done
			if test.replyErr != nil {
				if err == nil || !strings.Contains(err.Error(), test.replyErr.Error()) {
					t.Fatalf("late response reply error = %v", err)
				}
			} else if !errors.Is(err, errPromptCancelled) {
				t.Fatalf("late permission error = %v", err)
			}
			if got := client.permissionReply(0); got.reply != valReject || got.message != valCancelled {
				t.Fatalf("late reply = %#v", got)
			}
		})
	}

	t.Run("empty native permission identity is ignored", func(t *testing.T) {
		session := testSession(newTestAgent(), newFakeHermesClient())
		if err := session.handlePermission(t.Context(), nativehermes.PermissionRequest{}); err != nil {
			t.Fatalf("empty permission request: %v", err)
		}
	})
}

func TestPromptBacklogQuestionCancellationBeforeTurn(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	conn.elicitErr = context.Canceled
	agent := newTestAgent()
	agent.setAgentClient(conn)
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	session := testSession(agent, client)
	session.mu.Lock()
	session.cancelled = true
	session.mu.Unlock()
	client.events <- nativehermes.TurnEvent{
		Type:       evtClarifyRequest,
		Properties: json.RawMessage(`{"id":"question","sessionID":"native-1","questions":[{"question":"Continue?"}]}`),
	}

	resp, err := session.Prompt(t.Context(), acp.PromptRequest{
		Meta: turnRouteMeta("prompt-turn"), SessionId: session.id,
		Prompt: []acp.ContentBlock{acp.TextBlock("hello")},
	})
	if err != nil || resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("cancelled backlog response = %#v err=%v", resp, err)
	}
}

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
		agent := newTestAgent()
		agent.setAgentClient(conn)
		if _, err := agent.Initialize(ctx, acp.InitializeRequest{ClientCapabilities: acp.ClientCapabilities{
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}},
		}}); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		session := testSession(agent, client)
		turnCtx := session.beginTurn(ctx, "turn-question-1")
		defer session.finishTurn()

		req := nativehermes.QuestionRequest{
			ID:        "question-1",
			SessionID: "native-1",
			Tool:      nativehermes.QuestionTool{MessageID: "message-1", CallID: "call-1"},
			Questions: []nativehermes.QuestionInfo{
				{
					Question: "Proceed?",
					Header:   "Decision",
					Options: []nativehermes.QuestionOption{
						{Label: "Yes", Description: "Continue"},
						{Label: "No"},
					},
				},
				{
					Question: "Colors?",
					Header:   "Palette",
					Multiple: true,
					Options: []nativehermes.QuestionOption{
						{Label: "Red"},
						{Label: "Blue"},
					},
				},
			},
		}
		if err := session.handleQuestion(turnCtx, req); err != nil {
			t.Fatalf("handleQuestion: %v", err)
		}
		if len(conn.elicitations) != 1 {
			t.Fatalf("elicitations = %d, want 1", len(conn.elicitations))
		}
		got := conn.elicitations[0]
		if got.Form == nil || got.Form.Mode != "form" || got.Form.Message != "Hermes needs input" {
			t.Fatalf("elicitation form = %#v", got.Form)
		}
		if conn.scopes[0].SessionID != session.id || conn.scopes[0].TurnNonce != "turn-question-1" || conn.scopes[0].RequestID == nil || *conn.scopes[0].RequestID != "question-1" {
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
		agent := newTestAgent()
		agent.setAgentClient(conn)
		if _, err := agent.Initialize(ctx, acp.InitializeRequest{ClientCapabilities: acp.ClientCapabilities{
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}},
		}}); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		session := testSession(agent, client)
		if err := session.handleQuestion(ctx, nativehermes.QuestionRequest{ID: "q", SessionID: "native-1"}); err != nil {
			t.Fatalf("handleQuestion: %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %d, want 1", client.questionRejectCount())
		}
	})

	t.Run("missing capability rejects without ACP request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		if err := session.handleQuestion(ctx, nativehermes.QuestionRequest{ID: "q", SessionID: "native-1"}); err != nil {
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

func TestQuestionToolCancelRejectsPending(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	agent := newTestAgent()
	session := testSession(agent, client)

	turnCtx := session.beginTurn(ctx, "test-turn")
	session.mu.Lock()
	session.questions["q2"] = nativehermes.QuestionRequest{ID: "q2", SessionID: "native-1"}
	session.pending["p1"] = nativehermes.PermissionRequest{ID: "p1", SessionID: "native-1"}
	session.mu.Unlock()
	session.cancelTurn()
	if turnCtx.Err() == nil {
		t.Fatal("turn context was not cancelled")
	}
	if client.questionRejectCount() != 1 {
		t.Fatalf("question rejects after cancel = %d, want 1", client.questionRejectCount())
	}
	reply := client.permissionReply(0)
	if reply.reply != "reject" || reply.message != "cancelled" {
		t.Fatalf("permission cancel reply = %#v", reply)
	}
	session.finishTurn()
}

func TestPermissionV2AskReplyAndCancelled(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	ctx := session.beginTurn(t.Context(), "permission-turn")
	defer session.finishTurn()

	req := testHermesPermissionRequest(t, "perm-1", "tool-1")
	req.Metadata = map[string]any{"path": "file.txt"}
	if err := session.handlePermission(ctx, req); err != nil {
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
	if err := session.handlePermission(ctx, testHermesPermissionRequest(t, "perm-2", "tool-2")); err != nil {
		t.Fatalf("handlePermission cancelled: %v", err)
	}
	if got := client.permissionReply(1).reply; got != "reject" {
		t.Fatalf("cancelled reply = %q, want reject", got)
	}
}

func TestPermissionQuestionDuplicateRequestIDsAreFenced(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	ctx := session.beginTurn(t.Context(), "permission-turn")
	defer session.finishTurn()

	if err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{"id":"perm-dup","sessionID":"native-1","action":"edit","tool":{"callID":"tool-dup"}}`),
	}); err != nil {
		t.Fatalf("permission event: %v", err)
	}
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{"id":"perm-dup","sessionID":"native-1","action":"edit","tool":{"callID":"tool-dup"}}`),
	}); err != nil {
		t.Fatalf("repeated permission event: %v", err)
	}
	if conn.permissionRequestCount() != 1 || client.permissionReplyCount() != 1 {
		t.Fatalf("duplicate permission was not fenced requests=%d replies=%d", conn.permissionRequestCount(), client.permissionReplyCount())
	}

	if err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       "clarify.request",
		Properties: json.RawMessage(`{"id":"question-dup","sessionID":"native-1","questions":[{"question":"Continue?"}]}`),
	}); err != nil {
		t.Fatalf("question event: %v", err)
	}
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       "clarify.request",
		Properties: json.RawMessage(`{"id":"question-dup","sessionID":"native-1","questions":[{"question":"Continue?"}]}`),
	}); err != nil {
		t.Fatalf("repeated question event: %v", err)
	}
	if len(conn.elicitations) != 1 || client.questionReplyCount() != 1 {
		t.Fatalf("duplicate question was not fenced elicitations=%d replies=%d", len(conn.elicitations), client.questionReplyCount())
	}
}

func TestEventMappingMessagePartToolUsageAndRaw(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.rawMessages = rawMessageConfig{enabled: true}

	textProps := json.RawMessage(`{"id":"part-1","sessionID":"native-1","messageID":"message-1","type":"text","text":"hello"}`)
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "message.part.updated", Properties: textProps, Raw: json.RawMessage(`{"type":"message.part.updated"}`)}); err != nil {
		t.Fatalf("text event: %v", err)
	}
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "message.part.updated", Properties: textProps}); err != nil {
		t.Fatalf("duplicate text event: %v", err)
	}
	reasoningProps := json.RawMessage(`{"id":"part-2","sessionID":"native-1","messageID":"message-1","type":"reasoning","text":"thinking"}`)
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "message.part.updated", Properties: reasoningProps}); err != nil {
		t.Fatalf("reasoning event: %v", err)
	}
	toolProps := json.RawMessage(`{"id":"part-3","sessionID":"native-1","messageID":"message-1","type":"tool","tool":"bash","callID":"call-1","state":{"status":"completed","title":"Run","rawOutput":{"result":"done"}}}`)
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "message.part.updated", Properties: toolProps}); err != nil {
		t.Fatalf("tool event: %v", err)
	}
	if err := session.emitMessage(ctx, nativehermes.NativeMessage{
		Info: nativehermes.NativeMessageInfo{ID: "message-1", SessionID: "native-1", Role: "assistant", Tokens: nativehermes.Tokens{Total: 9}},
		Parts: []nativehermes.Part{{
			SessionID: "native-1",
			MessageID: "message-1",
			Type:      "step-finish",
			Tokens:    nativehermes.Tokens{Input: 2, Output: 3, Reasoning: 1},
		}},
	}, false); err != nil {
		t.Fatalf("emitMessage: %v", err)
	}

	if conn.updateCount() != 5 {
		t.Fatalf("updates = %d, want 5: %#v", conn.updateCount(), conn.updates)
	}
	if conn.updates[0].Update.AgentMessageChunk == nil {
		t.Fatalf("first update = %#v, want agent chunk", conn.updates[0].Update)
	}
	if conn.updates[1].Update.AgentThoughtChunk == nil {
		t.Fatalf("second update = %#v, want thought", conn.updates[1].Update)
	}
	if conn.updates[2].Update.ToolCall == nil {
		t.Fatalf("third update = %#v, want tool", conn.updates[2].Update)
	}
	if output, _ := conn.updates[2].Update.ToolCall.RawOutput.(map[string]any); output["result"] != "done" {
		t.Fatalf("tool start raw output = %#v", conn.updates[2].Update.ToolCall.RawOutput)
	}
	if conn.updates[3].Update.UsageUpdate == nil || conn.updates[4].Update.UsageUpdate == nil {
		t.Fatalf("usage updates missing: %#v", conn.updates)
	}
	if len(conn.extensions) == 0 || conn.extensions[0].method != RawEventMethod {
		t.Fatalf("raw events = %#v", conn.extensions)
	}
}

func TestGatewayToolPartsEmitACPStartAndResult(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	startProperties := json.RawMessage(`{"id":"hermes-live-tool-native-1","sessionID":"native-1","messageID":"hermes-live","type":"tool","callID":"native-1","tool":"terminal","state":{"status":"running","rawInput":{"tool_id":"native-1","name":"terminal","context":"mcp__wagie__execute"}}}`)
	completeProperties := json.RawMessage(`{"id":"hermes-live-tool-native-1","sessionID":"native-1","messageID":"hermes-live","type":"tool","callID":"native-1","tool":"terminal","state":{"status":"completed","rawInput":{"tool_id":"native-1","name":"terminal","context":"mcp__wagie__execute"},"rawOutput":{"probe":"authorized","status":"ok"}}}`)

	if err := session.handleEvent(t.Context(), nativehermes.TurnEvent{Type: evtMessagePartUpdated, Properties: startProperties}); err != nil {
		t.Fatalf("gateway tool start: %v", err)
	}

	startPart, ok := eventPart(startProperties)
	if !ok {
		t.Fatal("gateway start part did not decode")
	}
	completePart, ok := eventPart(completeProperties)
	if !ok {
		t.Fatal("gateway completion part did not decode")
	}
	if err := session.emitMessage(t.Context(), nativehermes.NativeMessage{
		Info:  nativehermes.NativeMessageInfo{ID: "hermes-live", SessionID: "native-1", Role: valAssistant},
		Parts: []nativehermes.Part{startPart, completePart},
	}, false); err != nil {
		t.Fatalf("gateway message retrieval: %v", err)
	}

	if conn.updateCount() != 2 {
		t.Fatalf("gateway tool updates = %#v, want one start and one completion", conn.updates)
	}
	start := conn.updates[0].Update.ToolCall
	if start == nil || start.ToolCallId != "native-1" || start.Title != "terminal" ||
		start.Kind != acp.ToolKindExecute || start.Status != acp.ToolCallStatusInProgress {
		t.Fatalf("ACP tool start = %#v", start)
	}
	if input, _ := start.RawInput.(map[string]any); input["context"] != "mcp__wagie__execute" {
		t.Fatalf("ACP tool raw input = %#v", start.RawInput)
	}

	complete := conn.updates[1].Update.ToolCallUpdate
	if complete == nil || complete.ToolCallId != "native-1" || complete.Status == nil ||
		*complete.Status != acp.ToolCallStatusCompleted {
		t.Fatalf("ACP tool completion = %#v", complete)
	}
	if output, _ := complete.RawOutput.(map[string]any); output["probe"] != "authorized" || output["status"] != "ok" {
		t.Fatalf("ACP tool raw output = %#v", complete.RawOutput)
	}
}

func TestHermesToolKindMap(t *testing.T) {
	t.Parallel()

	tests := map[acp.ToolKind][]string{
		acp.ToolKindRead: {
			"read_file", "skill_view", "skills_list", "browser_snapshot", "browser_vision", "browser_get_images", "vision_analyze",
		},
		acp.ToolKindEdit:   {"write_file", "patch", "skill_manage"},
		acp.ToolKindSearch: {"search_files"},
		acp.ToolKindExecute: {
			"terminal", "process", "execute_code", "browser_click", "browser_type", "browser_scroll", "browser_press", "browser_back",
			"delegate_task", "image_generate", "text_to_speech",
		},
		acp.ToolKindFetch: {"web_search", "web_extract", "browser_navigate"},
		acp.ToolKindThink: {"_thinking"},
		acp.ToolKindOther: {"todo", "browser_console", "memory", "plugin_tool", "TERMINAL", ""},
	}

	for want, tools := range tests {
		for _, tool := range tools {
			if got := toolKind(tool); got != want {
				t.Errorf("toolKind(%q) = %q, want %q", tool, got, want)
			}
		}
	}
}

func TestPartUpdatesReconcilesHermesCompleteText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		complete string
		streamed string
		want     string
	}{
		{name: "completion only", complete: "final answer", want: "final answer"},
		{name: "fully streamed", complete: "final answer", streamed: "final answer"},
		{name: "completion suffix", complete: "final answer", streamed: "final ", want: "answer"},
		{name: "inconsistent completion", complete: "replacement", streamed: "already sent"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			updates := partUpdates(valAssistant, nativehermes.Part{
				MessageID:    "message-1",
				Type:         valText,
				Text:         test.complete,
				StreamedText: test.streamed,
			})
			if test.want == "" {
				if updates != nil {
					t.Fatalf("updates = %#v, want nil", updates)
				}

				return
			}

			if len(updates) != 1 || updates[0].AgentMessageChunk == nil ||
				updates[0].AgentMessageChunk.Content.Text == nil ||
				updates[0].AgentMessageChunk.Content.Text.Text != test.want {
				t.Fatalf("updates = %#v, want text %q", updates, test.want)
			}
		})
	}
}

// TestUsageUpdateSizeIsContextWindow pins where the usage size comes from: the
// context window the gateway reports with the completion, and nowhere else. The
// model catalogue advertises no limits, so a message that reports no window
// leaves the size at zero rather than borrowing a number from elsewhere.
func TestUsageUpdateSizeIsContextWindow(t *testing.T) {
	ctx := context.Background()

	t.Run("reported context window populates size", func(t *testing.T) {
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, newFakeHermesClient())

		if err := session.emitMessage(ctx, nativehermes.NativeMessage{
			Info: nativehermes.NativeMessageInfo{
				ID: "message-1", SessionID: "native-1", Role: "assistant",
				Tokens: nativehermes.Tokens{Total: 1000}, ContextWindow: 200000,
			},
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

	t.Run("unreported context window emits size zero", func(t *testing.T) {
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, newFakeHermesClient())

		if err := session.emitMessage(ctx, nativehermes.NativeMessage{
			Info: nativehermes.NativeMessageInfo{ID: "message-1", SessionID: "native-1", Role: "assistant", Tokens: nativehermes.Tokens{Total: 1000}},
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

func TestPromptSSEDisconnectAbortsNativeTurn(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	agent := newTestAgent()
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(ctx, TextPromptRequest(session.id, "turn-disconnect", "hello"))
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
		requireTurnFailure(t, err, nativehermes.CauseTransport, "stream closed")
	case <-ctx.Done():
		t.Fatal("Prompt did not return")
	}
	if client.abortCount() != 1 {
		t.Fatalf("abort count = %d, want 1", client.abortCount())
	}
}

// TestPromptGatewayDisconnectSentinelFences proves that when SendMessage returns
// the transport turn failure carrying the disconnect sentinel (the read loop
// recovered the real cause), the turn is fenced with exactly one uniform
// hermes_turn_failed error (cause transport, real cause in message) and one
// native abort.
func TestPromptGatewayDisconnectSentinelFences(t *testing.T) {
	client := newFakeHermesClient()
	client.sendMessage = func(_ context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{}, nativehermes.NewTurnFailure(nativehermes.CauseTransport, "read tcp 127.0.0.1: connection reset by peer")
	}
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	_, err := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
	requireTurnFailure(t, err, nativehermes.CauseTransport, "connection reset by peer")
	if client.abortCount() != 1 {
		t.Fatalf("abort count = %d, want 1", client.abortCount())
	}
}

func TestPromptIdleSSEDisconnectDoesNotPoisonNextTurn(t *testing.T) {
	client := newFakeHermesClient()
	client.errs <- errors.New("idle stream closed")
	client.events <- nativehermes.TurnEvent{Type: "server.connected"}
	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{
			Info:  nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
			Parts: []nativehermes.Part{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "ok"}},
		}, nil
	}
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	resp, err := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
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
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(store))
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.cwd = t.TempDir()
	if err := session.snapshotToStore(t.Context()); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
	resumedClient := newFakeHermesClient()
	resumedClient.getSession = testNativeSession("native-1")
	agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
		resumedClient.xdg = opts.ExistingXDG

		return resumedClient, nil
	}
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Prompt did not start")
	}
	client.events <- nativehermes.TurnEvent{
		Type:        "message.part.updated",
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
	client.errs <- nativehermes.NewStreamError(7, errors.New("stream failed"))
	select {
	case err := <-done:
		requireTurnFailure(t, err, nativehermes.CauseTransport, "stream failed")
	case <-ctx.Done():
		t.Fatal("Prompt did not fail on stream error")
	}

	client.events <- nativehermes.TurnEvent{
		Type:        "message.part.updated",
		StreamEpoch: 7,
		Properties:  json.RawMessage(`{"id":"late-part","sessionID":"native-1","messageID":"assistant","type":"text","text":"late"}`),
	}
	if _, err := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("again")}}); err != nil {
		t.Fatalf("second Prompt: %v", err)
	}
	if conn.updateCount() != 1 {
		t.Fatalf("late failed-epoch update was emitted: %#v", conn.updates)
	}
}

func TestPromptCleanEOFSentinelDisconnectAbortsTurn(t *testing.T) {
	client := newFakeHermesClient()
	agent := newTestAgent()
	session := testSession(agent, client)
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Prompt did not start")
	}
	client.errs <- nativehermes.NewStreamError(11, errors.New("websocket closed"))
	select {
	case err := <-done:
		requireTurnFailure(t, err, nativehermes.CauseTransport, "websocket closed")
	case <-ctx.Done():
		t.Fatal("Prompt did not fail on clean EOF disconnect")
	}
	if client.abortCount() != 1 {
		t.Fatalf("native aborts = %d, want 1", client.abortCount())
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
				client.events <- nativehermes.TurnEvent{
					Type:       "approval.request",
					Properties: json.RawMessage(`{"id":"perm","sessionID":"native-1","action":"edit","tool":{"callID":"tool-perm"}}`),
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
				agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
			},
			sendEvent: func(client *fakeHermesClient) {
				client.events <- nativehermes.TurnEvent{
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
				if client.questionRejects[0].requestID == "" {
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
			agent := newTestAgent()
			agent.setAgentClient(conn)
			if tt.setup != nil {
				tt.setup(agent)
			}
			session := testSession(agent, client)
			agent.mu.Lock()
			agent.sessions[session.id] = session
			agent.mu.Unlock()

			started := make(chan struct{})
			client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
				close(started)
				<-ctx.Done()

				return nativehermes.NativeMessage{}, ctx.Err()
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan acp.PromptResponse, 1)
			go func() {
				resp, _ := agent.Prompt(ctx, TextPromptRequest(session.id, "turn-cancel", "hello"))
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
			if err := agent.Cancel(ctx, CancelRequest(session.id, "turn-cancel")); err != nil {
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
	client := newFakeHermesClient()
	client.replyErr = nativehermes.MissingLiveSessionMappingError{StoredSessionID: "native-1"}
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	ctx := session.beginTurn(t.Context(), "missing-live-turn")
	defer session.finishTurn()

	err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{"id":"perm-missing-live","sessionID":"native-1","action":"edit","tool":{"callID":"tool-missing-live"}}`),
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

func TestPromptBacklogPermissionBeforeTurnFailsClosed(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	conn.permErr = context.Canceled
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.mu.Lock()
	session.cancelled = true
	session.mu.Unlock()
	client.events <- nativehermes.TurnEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{"id":"perm","sessionID":"native-1","tool":{"callID":"tool-perm"}}`),
	}
	if _, err := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil ||
		!strings.Contains(err.Error(), "outside its active turn") {
		t.Fatalf("Prompt error = %v, want inactive-turn rejection", err)
	}
	if conn.permissionRequestCount() != 0 {
		t.Fatalf("permission requests = %d, want 0", conn.permissionRequestCount())
	}
	if got := client.permissionReply(0).message; got != "stale or unknown tool call" {
		t.Fatalf("permission reply = %q", got)
	}
}

func TestPromptBacklogErrorBeforeTurn(t *testing.T) {
	client := newFakeHermesClient()
	session := testSession(newTestAgent(), client)
	client.events <- nativehermes.TurnEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{`),
	}
	if _, err := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
		t.Fatal("malformed backlog event was ignored")
	}
}

func TestPermissionCancelledReplyBranches(t *testing.T) {
	t.Run("permission without connection rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(newTestAgent(), client)
		turnCtx := session.beginTurn(t.Context(), "test-turn")
		if err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm")); err != nil {
			t.Fatalf("handlePermission: %v", err)
		}
		if got := client.permissionReply(0).message; got != "client unavailable" {
			t.Fatalf("permission reply = %q", got)
		}
		session.finishTurn()
	})

	t.Run("permission arriving after context cancellation fails closed", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.permErr = context.Canceled
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx, "test-turn")
		cancel()
		err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm"))
		if err == nil || !strings.Contains(err.Error(), "outside its active turn") {
			t.Fatalf("handlePermission err = %v", err)
		}
		if got := client.permissionReply(0).message; got != "stale or unknown tool call" {
			t.Fatalf("permission reply = %q", got)
		}
		if conn.permissionRequestCount() != 0 {
			t.Fatalf("permission requests = %d, want 0", conn.permissionRequestCount())
		}
		session.finishTurn()
	})

	t.Run("permission selected response after context cancellation is rejected before callback", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx, "test-turn")
		cancel()
		err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm"))
		if err == nil || !strings.Contains(err.Error(), "outside its active turn") {
			t.Fatalf("handlePermission err = %v", err)
		}
		if got := client.permissionReply(0).message; got != "stale or unknown tool call" {
			t.Fatalf("permission reply = %q", got)
		}
		if conn.permissionRequestCount() != 0 {
			t.Fatalf("permission requests = %d, want 0", conn.permissionRequestCount())
		}
		session.finishTurn()
	})

	t.Run("permission cancellation reply error is returned", func(t *testing.T) {
		client := newFakeHermesClient()
		client.replyErr = errors.New("reply failed")
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx, "test-turn")
		cancel()
		if err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm")); err == nil {
			t.Fatal("reply error was ignored")
		}
		session.finishTurn()
	})

	t.Run("permission client error rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.permErr = errors.New("permission failed")
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(t.Context(), "test-turn")
		defer session.finishTurn()
		err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm"))
		if err == nil || !strings.Contains(err.Error(), "permission failed") {
			t.Fatalf("handlePermission err = %v", err)
		}
		reply := client.permissionReply(0)
		if reply.reply != "reject" || reply.message != "client permission request failed" {
			t.Fatalf("permission fail-closed reply = %#v", reply)
		}
	})

	t.Run("permission client error returns reject failure", func(t *testing.T) {
		client := newFakeHermesClient()
		client.replyErr = errors.New("reply failed")
		conn := newRecordingAgentClient()
		conn.permErr = errors.New("permission failed")
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(t.Context(), "test-turn")
		defer session.finishTurn()
		err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm"))
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
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background(), "test-turn")
		done := make(chan error, 1)
		go func() {
			done <- session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm"))
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
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background(), "test-turn")
		done := make(chan error, 1)
		go func() {
			done <- session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm"))
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
		session := testSession(newTestAgent(), client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx, "test-turn")
		cancel()
		if err := session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"}); err != nil {
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
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx, "test-turn")
		cancel()
		err := session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
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
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		if err := session.handleQuestion(context.Background(), nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"}); err == nil {
			t.Fatal("reject error was ignored")
		}
	})

	t.Run("question decline after context cancellation returns cancelled", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitation = acp.NewUnstableCreateElicitationResponseDecline()
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx, "test-turn")
		cancel()
		err := session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
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
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background(), "test-turn")
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
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
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		err := session.handleQuestion(context.Background(), nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
		if err == nil || !strings.Contains(err.Error(), "elicitation failed") {
			t.Fatalf("handleQuestion err = %v", err)
		}
		if client.questionRejectCount() != 1 {
			t.Fatalf("question fail-closed rejects = %#v", client.questionRejects)
		}
	})

	t.Run("question client error returns reject failure", func(t *testing.T) {
		client := newFakeHermesClient()
		client.replyErr = errors.New("reject failed")
		conn := newRecordingAgentClient()
		conn.elicitErr = errors.New("elicitation failed")
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		err := session.handleQuestion(context.Background(), nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
		if err == nil || !strings.Contains(err.Error(), "elicitation failed") || !strings.Contains(err.Error(), "reject failed") {
			t.Fatalf("handleQuestion err = %v", err)
		}
	})

	t.Run("question accept after context cancellation rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx, "test-turn")
		cancel()
		err := session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
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
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := session.beginTurn(ctx, "test-turn")
		cancel()
		if err := session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"}); err == nil {
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
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background(), "test-turn")
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
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
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(context.Background(), "test-turn")
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
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
	png := fixtureBytes(t, "valid.png")
	imageData, err := promptToHermesParts(t.Context(), []acp.ContentBlock{{Image: &acp.ContentBlockImage{
		Type: "image", Data: base64.StdEncoding.EncodeToString(png), MimeType: "image/png",
	}}}, ImageLimits{}, "")
	if err != nil {
		t.Fatalf("image data prompt: %v", err)
	}
	if len(imageData) != 1 {
		t.Fatalf("image data parts = %#v", imageData)
	}
	decodedImage, imageOK := imageData[0][keyData].([]byte)
	if !imageOK || imageData[0]["type"] != "file" || imageData[0]["mime"] != "image/png" ||
		!bytes.Equal(decodedImage, png) {
		t.Fatalf("image data part = %#v", imageData[0])
	}

	remoteURI := "https://example.com/pic.png"
	imageWithURI, err := promptToHermesParts(t.Context(), []acp.ContentBlock{{Image: &acp.ContentBlockImage{
		Type: "image", Data: base64.StdEncoding.EncodeToString(png), MimeType: "image/png", Uri: &remoteURI,
	}}}, ImageLimits{}, "")
	if err != nil || len(imageWithURI) != 1 {
		t.Fatalf("image data plus URI = %#v err=%v", imageWithURI, err)
	}
	decodedWithURI, imageOK := imageWithURI[0][keyData].([]byte)
	if !imageOK || !bytes.Equal(decodedWithURI, png) {
		t.Fatalf("image data plus URI = %#v err=%v", imageWithURI, err)
	}

	// The URI is provenance the native request never learns, so the two forms
	// of the same bytes stay indistinguishable below the adapter.
	if !reflect.DeepEqual(imageData, imageWithURI) {
		t.Fatalf("a block uri changed the native part: %#v versus %#v", imageData, imageWithURI)
	}
}

func TestPromptHelpersAndAnswerMapping(t *testing.T) {
	parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
		acp.TextBlock("hello"),
		{ResourceLink: &acp.ContentBlockResourceLink{Name: "a", Type: "resource_link", Uri: "file:///tmp/a"}},
		{Resource: &acp.ContentBlockResource{Type: "resource", Resource: acp.EmbeddedResourceResource{
			TextResourceContents: &acp.TextResourceContents{Text: "embedded", Uri: "file:///tmp/b"},
		}}},
		{Resource: &acp.ContentBlockResource{Type: "resource", Resource: acp.EmbeddedResourceResource{
			BlobResourceContents: &acp.BlobResourceContents{Blob: "AA==", Uri: "file:///tmp/blob"},
		}}},
		{Resource: &acp.ContentBlockResource{Type: "resource", Resource: acp.EmbeddedResourceResource{
			BlobResourceContents: &acp.BlobResourceContents{
				Blob: fixtureBase64(t, "valid.png"), MimeType: acp.Ptr("image/png"), Uri: "file:///tmp/image",
			},
		}}},
	}, ImageLimits{}, "")
	if err != nil {
		t.Fatalf("promptToHermesParts: %v", err)
	}
	if len(parts) != 5 {
		t.Fatalf("parts = %#v", parts)
	}
	embeddedImage, imageOK := parts[4][keyData].([]byte)
	if parts[0]["text"] != "hello" || parts[1]["text"] != "file:///tmp/a" ||
		parts[2]["text"] != "embedded" || parts[3]["text"] != "file:///tmp/blob" ||
		parts[4]["type"] != "file" || !imageOK || !bytes.Equal(embeddedImage, fixtureBytes(t, "valid.png")) {
		t.Fatalf("parts = %#v", parts)
	}
	_, emptyErr := promptToHermesParts(t.Context(), nil, ImageLimits{}, "")
	if emptyErr == nil {
		t.Fatal("empty prompt accepted")
	}
	var emptyReqErr *acp.RequestError
	if !errors.As(emptyErr, &emptyReqErr) || emptyReqErr.Code != -32602 {
		t.Fatalf("empty prompt error = %#v, want invalid params", emptyErr)
	}
	if !reflect.DeepEqual(emptyReqErr.Data, map[string]any{jsonFieldError: valUnsupported, keyField: acpFieldPrompt}) {
		t.Fatalf("empty prompt data = %#v, want unsupported/prompt", emptyReqErr.Data)
	}
	if _, err := promptToHermesParts(t.Context(), []acp.ContentBlock{{
		Audio: &acp.ContentBlockAudio{Type: "audio", Data: "AA==", MimeType: "audio/wav"},
	}}, ImageLimits{}, ""); err == nil {
		t.Fatal("audio prompt accepted")
	}
	req, ids := questionElicitationRequest(nativehermes.QuestionRequest{ID: "q", SessionID: "s"})
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
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	messageID := "msg-user"
	client.sendMessage = func(_ context.Context, id string, req nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		if id != "native-1" {
			t.Fatalf("native id = %q", id)
		}
		if len(req.Parts) != 1 || req.Parts[0]["text"] != "/review inspect this" {
			t.Fatalf("plain slash request = %#v", req)
		}

		return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
	}

	resp, err := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"),
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

func TestPromptReloadsMCPOnceInsideFirstAuthorizedTurn(t *testing.T) {
	client := newFakeHermesClient()
	session := testSession(newTestAgent(), client)
	session.mcpServers = []acp.McpServer{HTTPMCPServer("wagie", "http://127.0.0.1/mcp", nil)}

	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		client.mu.Lock()
		reloads := client.reloadCalls
		client.mu.Unlock()
		if reloads != 1 {
			t.Fatalf("SendMessage observed %d MCP reloads, want exactly one", reloads)
		}

		return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
	}

	for _, nonce := range []string{"authorized-1", "authorized-2"} {
		if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, nonce, "reply")); err != nil {
			t.Fatalf("Prompt(%s): %v", nonce, err)
		}
	}

	client.mu.Lock()
	reloads := client.reloadCalls
	client.mu.Unlock()
	if reloads != 1 {
		t.Fatalf("reload calls = %d, want 1", reloads)
	}
}

func TestPromptMCPReloadCancellationRetriesAndFailurePoisons(t *testing.T) {
	t.Run("cancelled reload retries on the next authorized turn", func(t *testing.T) {
		client := newFakeHermesClient()
		started := make(chan struct{})
		client.reloadFunc = func(ctx context.Context, id string) error {
			if id != "native-1" {
				t.Fatalf("reload native id = %q", id)
			}
			close(started)
			<-ctx.Done()

			return ctx.Err()
		}
		agent := newTestAgent(WithScratchDir(t.TempDir()))
		session := testSession(agent, client)
		session.mcpServers = []acp.McpServer{HTTPMCPServer("wagie", "http://127.0.0.1/mcp", nil)}
		if err := session.snapshotToStore(t.Context()); err != nil {
			t.Fatalf("snapshot checkpoint: %v", err)
		}

		replacement := newFakeHermesClient()
		replacement.getSession = testNativeSession("native-1")
		agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			replacement.xdg = opts.ExistingXDG

			return replacement, nil
		}

		result := make(chan acp.PromptResponse, 1)
		errCh := make(chan error, 1)
		go func() {
			resp, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "reload-cancel", "reply"))
			result <- resp
			errCh <- err
		}()
		<-started
		if err := session.cancelRouted(turnRouteMeta("reload-cancel")); err != nil {
			t.Fatalf("cancelRouted: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("cancelled Prompt error = %v", err)
		}
		if resp := <-result; resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("cancelled Prompt response = %#v", resp)
		}

		if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "reload-retry", "reply")); err != nil {
			t.Fatalf("retry Prompt: %v", err)
		}
		client.mu.Lock()
		oldReloads := client.reloadCalls
		client.mu.Unlock()
		replacement.mu.Lock()
		newReloads := replacement.reloadCalls
		replacement.mu.Unlock()
		if oldReloads != 1 || newReloads != 1 {
			t.Fatalf("reload calls old/new = %d/%d, want cancelled attempt plus replacement retry", oldReloads, newReloads)
		}
	})

	t.Run("indeterminate reload failure poisons the session", func(t *testing.T) {
		client := newFakeHermesClient()
		client.reloadErr = errors.New("reload unavailable")
		session := testSession(newTestAgent(), client)
		session.mcpServers = []acp.McpServer{HTTPMCPServer("wagie", "http://127.0.0.1/mcp", nil)}

		_, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "reload-fail", "reply"))
		if err == nil || !strings.Contains(err.Error(), "hermes_mcp_reload_failed") || !strings.Contains(err.Error(), "reload unavailable") {
			t.Fatalf("reload failure = %v", err)
		}
		_, nextErr := session.Prompt(t.Context(), TextPromptRequest(session.id, "reload-after-fail", "reply"))
		if nextErr == nil || !strings.Contains(nextErr.Error(), "session_poisoned") {
			t.Fatalf("post-reload-failure Prompt = %v", nextErr)
		}
	})

	// A gateway that reconnects mid-turn owes the reconnected runtime the MCP
	// reload the turn was admitted under. A reload the gateway refuses fails
	// that turn rather than letting it continue against a runtime whose tool
	// surface is unknown.
	t.Run("reconnect mid-turn fails the turn when the reload is refused", func(t *testing.T) {
		client := newFakeHermesClient()
		started := make(chan struct{})
		client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			close(started)
			<-ctx.Done()

			return nativehermes.NativeMessage{}, ctx.Err()
		}
		session := testSession(newTestAgent(), client)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := session.Prompt(ctx, TextPromptRequest(session.id, "reconnect-turn", "reply"))
			done <- err
		}()
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("Prompt did not start")
		}

		session.mu.Lock()
		session.mcpServers = []acp.McpServer{HTTPMCPServer("wagie", "http://127.0.0.1/mcp", nil)}
		session.mcpReloadComplete = false
		session.mu.Unlock()

		client.reloadErr = errors.New("reload refused")
		client.events <- nativehermes.TurnEvent{Type: "server.connected"}

		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "reload refused") {
				t.Fatalf("reconnect reload failure = %v", err)
			}
		case <-ctx.Done():
			t.Fatal("Prompt did not finish")
		}
	})
}

func TestPromptSuccessCancelAndErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("success through agent", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		agent.mu.Lock()
		agent.sessions[session.id] = session
		agent.mu.Unlock()
		messageID := "user-message"
		client.sendMessage = func(_ context.Context, id string, req nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			if id != "native-1" || len(req.Parts) != 1 {
				t.Fatalf("SendMessage id=%q req=%#v", id, req)
			}
			msg := nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{
				ID:        "assistant-1",
				SessionID: id,
				Role:      "assistant",
				Finish:    "length",
				Tokens:    nativehermes.Tokens{Total: 3, Input: 1, Output: 2},
			}}
			msg.Parts = []nativehermes.Part{{SessionID: id, MessageID: "assistant-1", Type: "text", Text: "hi"}}

			return msg, nil
		}
		request := TextPromptRequest(session.id, "turn-success", "hello")
		request.MessageId = &messageID
		resp, err := agent.Prompt(ctx, request)
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
		client.sendMessage = func(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			return nativehermes.NativeMessage{}, errors.New("send failed")
		}
		session := testSession(newTestAgent(), client)
		if _, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
			t.Fatal("send error prompt succeeded")
		}
	})

	t.Run("snapshot error after final message", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newTestAgent(WithSessionStore(&errorSessionStore{err: errors.New("snapshot failed")}))
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil ||
			!strings.Contains(err.Error(), "snapshot failed") {
			t.Fatalf("snapshot error = %v", err)
		}
	})

	t.Run("prompt validation and turn backpressure", func(t *testing.T) {
		session := testSession(newTestAgent(), newFakeHermesClient())
		session.turnQueue() <- struct{}{}
		if _, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
			t.Fatal("prompt backpressure was ignored")
		}
		<-session.turnQueue()
		if _, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id}); err == nil {
			t.Fatal("empty prompt was accepted")
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{{Audio: &acp.ContentBlockAudio{Type: "audio", Data: "AA==", MimeType: "audio/wav"}}}}); err == nil {
			t.Fatal("unsupported audio prompt was accepted")
		}
	})

	t.Run("turn context cancellation", func(t *testing.T) {
		client := newFakeHermesClient()
		started := make(chan struct{})
		client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			close(started)
			<-ctx.Done()

			return nativehermes.NativeMessage{}, ctx.Err()
		}
		session := testSession(newTestAgent(), client)
		ctx2, cancel := context.WithCancel(context.Background())
		done := make(chan acp.PromptResponse, 1)
		go func() {
			resp, _ := session.Prompt(ctx2, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
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
		agent := newTestAgent()
		_, err := agent.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: "missing"})
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
		agent := newTestAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			return nativehermes.NativeMessage{
				Info:  nativehermes.NativeMessageInfo{ID: "assistant", SessionID: "native-other", Role: "assistant", Finish: "stop"},
				Parts: []nativehermes.Part{{SessionID: "native-1", MessageID: "assistant", Type: "text", Text: "should not emit"}},
			}, nil
		}

		_, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		assertNativeSessionDriftPoison(t, session, conn, store, err, "native-other")
	})

	t.Run("mismatched final message part session id", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		store := newCountingSessionStore()
		agent := newTestAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			return nativehermes.NativeMessage{
				Info:  nativehermes.NativeMessageInfo{ID: "assistant", SessionID: "native-1", Role: "assistant", Finish: "stop"},
				Parts: []nativehermes.Part{{SessionID: "native-other", MessageID: "assistant", Type: "text", Text: "should not emit"}},
			}, nil
		}

		_, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		assertNativeSessionDriftPoison(t, session, conn, store, err, "native-other")
	})

	t.Run("mismatched replay message session id", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		store := newCountingSessionStore()
		agent := newTestAgent(WithSessionStore(store))
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.messages = []nativehermes.NativeMessage{{
			Info: nativehermes.NativeMessageInfo{ID: "user", SessionID: "native-other", Role: "user"},
			Parts: []nativehermes.Part{{
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
	_, nextErr := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("again")}})
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
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		started := make(chan struct{})
		release := make(chan struct{})
		client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			close(started)
			<-release

			return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		done := make(chan error, 1)
		go func() {
			_, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
			done <- err
		}()
		<-started
		client.events <- nativehermes.TurnEvent{Type: "server.connected"}
		client.events <- nativehermes.TurnEvent{
			Type:       "message.part.updated",
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
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		started := make(chan struct{})
		client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			close(started)
			<-ctx.Done()

			return nativehermes.NativeMessage{}, ctx.Err()
		}
		done := make(chan error, 1)
		go func() {
			_, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
			done <- err
		}()
		<-started
		client.events <- nativehermes.TurnEvent{
			Type:       "message.part.updated",
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
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			return nativehermes.NativeMessage{
				Info:  nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
				Parts: []nativehermes.Part{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "done", Raw: json.RawMessage(`{"id":"final"}`)}},
			}, nil
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil ||
			!strings.Contains(err.Error(), "final update failed") {
			t.Fatalf("final emit error = %v", err)
		}
	})
}

func TestReplayAndEventEdgeBranches(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	client.messages = []nativehermes.NativeMessage{{
		Info: nativehermes.NativeMessageInfo{ID: "user-1", SessionID: "native-1", Role: "user"},
		Parts: []nativehermes.Part{{
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
	if err := session.emitMessage(ctx, nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{Role: "user"}}, false); err != nil {
		t.Fatalf("emitMessage skipped user: %v", err)
	}
	if err := session.emitUpdate(ctx, acp.UpdatePlan(acp.PlanEntry{Content: "no client"})); err != nil {
		t.Fatalf("emitUpdate with conn: %v", err)
	}
	nilConnSession := testSession(newTestAgent(), newFakeHermesClient())
	if err := nilConnSession.emitUpdate(ctx, acp.UpdatePlan(acp.PlanEntry{Content: "no client"})); err != nil {
		t.Fatalf("emitUpdate without conn: %v", err)
	}

	noConnClient := newFakeHermesClient()
	noConnSession := testSession(newTestAgent(), noConnClient)
	if err := noConnSession.handlePermission(ctx, nativehermes.PermissionRequest{}); err != nil {
		t.Fatalf("empty permission: %v", err)
	}
	noConnSession.pending = nil
	noConnTurnCtx := noConnSession.beginTurn(t.Context(), "no-connection-turn")
	if err := noConnSession.handlePermission(noConnTurnCtx, testHermesPermissionRequest(t, "p", "tool-p")); err != nil {
		t.Fatalf("nil conn permission: %v", err)
	}
	noConnSession.finishTurn()
	if got := noConnClient.permissionReply(0).message; got != "client unavailable" {
		t.Fatalf("nil conn permission reply = %q", got)
	}
	session.questions = nil
	if err := session.handleQuestion(ctx, nativehermes.QuestionRequest{}); err != nil {
		t.Fatalf("empty question: %v", err)
	}
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	conn.elicitErr = errors.New("elicitation failed")
	if err := session.handleQuestion(ctx, nativehermes.QuestionRequest{ID: "q-error", SessionID: "native-1"}); err == nil {
		t.Fatal("elicitation error was ignored")
	}
	conn.elicitErr = nil
	turnCtx := session.beginTurn(t.Context(), "event-edge-turn")
	testApprovalAndClarifyEventBranches(t, turnCtx, session, client, conn)
	session.finishTurn()
	testForeignEventAndPartHelperBranches(t, ctx, session)
}

func testApprovalAndClarifyEventBranches(t *testing.T, ctx context.Context, session *session, client *fakeHermesClient, conn *recordingAgentClient) {
	t.Helper()

	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "approval.request", Properties: json.RawMessage(`{`)}); err == nil {
		t.Fatal("malformed permission event succeeded")
	}
	conn.permission = acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")}
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type: "approval.request",
		Properties: json.RawMessage(`{
			"id":"p-session",
			"sessionID":"native-1",
			"action":"edit",
			"metadata":{"filepath":"acp-permission-probe.txt"},
			"tool":{"messageID":"m1","callID":"c1"}
		}`),
	}); err != nil {
		t.Fatalf("approval.request event: %v", err)
	}
	reply := client.permissionReply(client.permissionReplyCount() - 1)
	if reply.requestID != "p-session" || reply.reply != "once" {
		t.Fatalf("approval.request reply = %#v", reply)
	}
	permissionReq := conn.permissions[len(conn.permissions)-1]
	if permissionReq.ToolCall.Title == nil || *permissionReq.ToolCall.Title != "edit" {
		t.Fatalf("approval.request ACP request = %#v", permissionReq)
	}
	rawInput, _ := permissionReq.ToolCall.RawInput.(map[string]any)
	metadata, _ := rawInput["metadata"].(map[string]any)
	if rawInput["action"] != "edit" || metadata["filepath"] != "acp-permission-probe.txt" {
		t.Fatalf("approval.request raw input = %#v", permissionReq.ToolCall.RawInput)
	}
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       "clarify.request",
		Properties: json.RawMessage(`{"id":"q-v2","sessionID":"native-1","questions":[{"question":"Continue?"}]}`),
	}); err != nil {
		t.Fatalf("clarify.request event: %v", err)
	}
	questionReply := client.questionReply(client.questionReplyCount() - 1)
	if questionReply.requestID != "q-v2" {
		t.Fatalf("clarify.request reply = %#v", questionReply)
	}
}

func testForeignEventAndPartHelperBranches(t *testing.T, ctx context.Context, session *session) {
	t.Helper()

	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "todo.updated", Properties: json.RawMessage(`{"sessionID":"other","todos":[{"content":"x"}]}`)}); err != nil {
		t.Fatalf("foreign todo event: %v", err)
	}
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "clarify.request", Properties: json.RawMessage(`{"request":{"id":"q","sessionID":"other"}}`)}); err != nil {
		t.Fatalf("foreign question event: %v", err)
	}
	if _, ok := eventPart(json.RawMessage(`{`)); ok {
		t.Fatal("malformed eventPart succeeded")
	}
	rawSession := testSession(newTestAgent(), newFakeHermesClient())
	rawSession.rawMessages = rawMessageConfig{enabled: true}
	if err := rawSession.emitRawHermesEvent(ctx, nativehermes.TurnEvent{Raw: json.RawMessage(`{"type":"x"}`)}); err != nil {
		t.Fatalf("raw event without conn: %v", err)
	}
	if usageFromTokens(nativehermes.Tokens{}) != nil {
		t.Fatal("empty tokens produced usage")
	}
	var emptyResource acp.EmbeddedResourceResource
	if part, err := embeddedResourceHermesPart(emptyResource, &imagePromptBudget{}); err == nil || part != nil {
		t.Fatalf("empty embeddedResourceHermesPart = %#v err=%v", part, err)
	}
	if updates := partUpdates("assistant", nativehermes.Part{Type: "text"}); updates != nil {
		t.Fatalf("empty text updates = %#v", updates)
	}
	if updates := partUpdates("assistant", nativehermes.Part{Type: "reasoning"}); updates != nil {
		t.Fatalf("empty reasoning updates = %#v", updates)
	}
}

func TestPromptRemainingErrorBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("send error after cancelled state", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(newTestAgent(), client)
		client.sendMessage = func(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			session.mu.Lock()
			session.cancelled = true
			session.mu.Unlock()

			return nativehermes.NativeMessage{}, errors.New("cancelled send")
		}
		resp, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		if err != nil || resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("cancelled send resp=%#v err=%v", resp, err)
		}
	})

	t.Run("successful result marked cancelled", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(newTestAgent(), client)
		client.sendMessage = func(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			session.mu.Lock()
			session.cancelled = true
			session.mu.Unlock()

			return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant", SessionID: "native-1", Role: "assistant", Finish: "stop"}}, nil
		}
		resp, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
		if err != nil || resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("cancelled success resp=%#v err=%v", resp, err)
		}
	})

	t.Run("replay and emit update errors", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.updateErr = errors.New("update failed")
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		client.messages = []nativehermes.NativeMessage{{
			Info: nativehermes.NativeMessageInfo{ID: "user", SessionID: "native-1", Role: "user"},
			Parts: []nativehermes.Part{{
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
		agent = newTestAgent()
		agent.setAgentClient(conn)
		session = testSession(agent, client)
		if err := session.emitMessage(ctx, nativehermes.NativeMessage{
			Info: nativehermes.NativeMessageInfo{ID: "assistant", SessionID: "native-1", Role: "assistant"},
			Parts: []nativehermes.Part{{
				ID:        "usage",
				SessionID: "native-1",
				MessageID: "assistant",
				Type:      "step-finish",
				Tokens:    nativehermes.Tokens{Total: 1},
				Raw:       json.RawMessage(`{"id":"usage"}`),
			}},
		}, false); err == nil {
			t.Fatal("emitMessage ignored step-finish update error")
		}
	})

	t.Run("duplicate part and raw notify error", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		part := nativehermes.Part{ID: "dup", SessionID: "native-1", MessageID: "assistant", Type: "text", Text: "hello"}
		if err := session.emitMessage(ctx, nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant", Role: "assistant"}, Parts: []nativehermes.Part{part, part}}, false); err != nil {
			t.Fatalf("duplicate emitMessage: %v", err)
		}
		// A raw-event emit failure is non-authoritative: it is recorded on the
		// observer hook and must NOT abort the turn (handleEvent returns nil).
		conn.notifyErr = errors.New("notify failed")
		session.rawMessages = rawMessageConfig{enabled: true}
		if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "unknown", Raw: json.RawMessage(`{"type":"unknown"}`)}); err != nil {
			t.Fatalf("raw notify error aborted the turn: %v", err)
		}
	})

	t.Run("same-session events and reconcile errors", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(agent, client)
		turnCtx := session.beginTurn(t.Context(), "same-session-turn")
		defer session.finishTurn()
		conn.permErr = errors.New("permission failed")
		if err := session.handleEvent(turnCtx, nativehermes.TurnEvent{
			Type:       "approval.request",
			Properties: json.RawMessage(`{"id":"p","sessionID":"native-1","tool":{"callID":"tool-p"}}`),
		}); err == nil {
			t.Fatal("permission event ignored client error")
		}
		conn.permErr = nil

		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		conn.elicitErr = errors.New("elicitation failed")
		if err := session.handleEvent(ctx, nativehermes.TurnEvent{
			Type:       "clarify.request",
			Properties: json.RawMessage(`{"id":"q","sessionID":"native-1"}`),
		}); err == nil {
			t.Fatal("question event ignored client error")
		}
		if req, ok := eventQuestion(json.RawMessage(`{"id":"direct","sessionID":"native-1"}`)); !ok || req.ID != "direct" {
			t.Fatalf("direct eventQuestion = %#v ok=%v", req, ok)
		}
	})
}

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
	return session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"),
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
	agent := newTestAgent()
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
	if client.closeCount() != 1 || !session.needsRuntimeResume() {
		t.Fatalf("provider failure close=%d needsResume=%v", client.closeCount(), session.needsRuntimeResume())
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
	agent := newTestAgent()
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
	client.sendMessage = func(_ context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{}, nativehermes.NewTurnFailure(nativehermes.CauseTransport, "hermes gateway disconnected: unexpected EOF")
	}

	conn := newRecordingAgentClient()
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(store))
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.cwd = t.TempDir()
	if err := session.snapshotToStore(t.Context()); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
	resumedClient := newFakeHermesClient()
	resumedClient.getSession = testNativeSession("native-1")
	agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
		resumedClient.xdg = opts.ExistingXDG

		return resumedClient, nil
	}

	_, err := promptOnce(context.Background(), session, "first")
	requireTurnFailure(t, err, nativehermes.CauseTransport, "unexpected EOF")
	if client.closeCount() != 1 || !session.needsRuntimeResume() {
		t.Fatalf("transport failure close=%d needsResume=%v", client.closeCount(), session.needsRuntimeResume())
	}

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
	agent := newTestAgent()
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

func TestTurnCancelWaitsForOneProvedRuntimeClose(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	client.closeFunc = func(context.Context) error {
		close(closeEntered)
		<-releaseClose

		return nil
	}

	session := testSession(newTestAgent(), client)
	promptDone := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "cancel-fence", "hang"))
		promptDone <- struct {
			resp acp.PromptResponse
			err  error
		}{resp: resp, err: err}
	}()
	<-started

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- session.cancelRouted(turnRouteMeta("cancel-fence")) }()
	<-closeEntered
	assertNoPromptSettlement(t, promptDone, "Prompt settled before native Close proof")
	select {
	case err := <-cancelDone:
		t.Fatalf("Cancel settled before native Close proof: %v", err)
	default:
	}

	close(releaseClose)
	if err := <-cancelDone; err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	out := <-promptDone
	if out.err != nil || out.resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("cancelled Prompt = %#v err=%v", out.resp, out.err)
	}
	if got := client.closeCount(); got != 1 {
		t.Fatalf("runtime close count = %d, want 1", got)
	}
}

func TestTurnTimeoutWaitsForOneProvedRuntimeClose(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	timeout := make(chan time.Time, 1)
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	client.closeFunc = func(context.Context) error {
		close(closeEntered)
		<-releaseClose

		return nil
	}

	agent := newTestAgent(WithTurnTimeout(time.Hour))
	agent.options.newPromptTimer = func(time.Duration) promptTimer {
		return promptTimer{C: timeout, Stop: func() bool { return true }}
	}
	session := testSession(agent, client)
	promptDone := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "timeout-fence", "hang"))
		promptDone <- struct {
			resp acp.PromptResponse
			err  error
		}{resp: resp, err: err}
	}()
	<-started
	timeout <- time.Now()
	<-closeEntered
	assertNoPromptSettlement(t, promptDone, "timeout settled before native Close proof")

	close(releaseClose)
	out := <-promptDone
	if out.resp.StopReason != "" {
		t.Fatalf("timeout stop reason = %q", out.resp.StopReason)
	}
	requireTurnFailure(t, out.err, nativehermes.CauseTimeout, "deadline")
	if got := client.closeCount(); got != 1 {
		t.Fatalf("runtime close count = %d, want 1", got)
	}
}

func TestTurnCancelCoincidentWithTimeoutClosesOnceAndWins(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	timeout := make(chan time.Time, 1)
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	client.closeFunc = func(context.Context) error {
		close(closeEntered)
		<-releaseClose

		return nil
	}

	agent := newTestAgent(WithTurnTimeout(time.Hour))
	agent.options.newPromptTimer = func(time.Duration) promptTimer {
		return promptTimer{C: timeout, Stop: func() bool { return true }}
	}
	session := testSession(agent, client)
	promptDone := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "coincident-fence", "hang"))
		promptDone <- struct {
			resp acp.PromptResponse
			err  error
		}{resp: resp, err: err}
	}()
	<-started
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- session.cancelRouted(turnRouteMeta("coincident-fence")) }()
	<-closeEntered
	timeout <- time.Now()
	close(releaseClose)
	if err := <-cancelDone; err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	out := <-promptDone
	if out.err != nil || out.resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("coincident Prompt = %#v err=%v", out.resp, out.err)
	}
	if got := client.closeCount(); got != 1 {
		t.Fatalf("runtime close count = %d, want 1", got)
	}
}

func TestTurnFenceProofFailurePoisonsSession(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	client.closeErr = nativehermes.ErrProcessContainmentIncomplete
	session := testSession(newTestAgent(), client)
	promptDone := make(chan error, 1)
	go func() {
		_, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "unproven-fence", "hang"))
		promptDone <- err
	}()
	<-started

	cancelErr := session.cancelRouted(turnRouteMeta("unproven-fence"))
	if !errors.Is(cancelErr, nativehermes.ErrProcessContainmentIncomplete) {
		t.Fatalf("Cancel error = %v, want process-tree proof failure", cancelErr)
	}
	if promptErr := <-promptDone; !errors.Is(promptErr, nativehermes.ErrProcessContainmentIncomplete) {
		t.Fatalf("Prompt error = %v, want process-tree proof failure", promptErr)
	}
	if err := session.ensureNotPoisoned(); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("poisoned session error = %v", err)
	}
	if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "after-unproven", "reply")); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("Prompt after proof failure = %v", err)
	}
	if got := client.closeCount(); got != 1 {
		t.Fatalf("runtime close count = %d, want 1", got)
	}
}

// TestCancelWithoutValidatedNonceHasNoNativeSideEffect pins Cancel Determinism
// rule 2: the native interrupt is downstream of a nonce that authenticates the
// addressed session's current turn, so an unauthenticated cancel — no envelope,
// a stale nonce, or no turn to authorize at all — reaches neither the gateway
// nor the session's pending interactions.
func TestCancelWithoutValidatedNonceHasNoNativeSideEffect(t *testing.T) {
	pendingPermission := nativehermes.PermissionRequest{ID: "approval-1", SessionID: "native-1"}
	pendingQuestion := nativehermes.QuestionRequest{ID: "clarify-1", SessionID: "native-1"}

	for _, testCase := range []struct {
		name   string
		meta   map[string]any
		active bool
	}{
		{name: "no route envelope", meta: nil, active: true},
		{name: "stale nonce", meta: turnRouteMeta("someone-elses-turn"), active: true},
		{name: "no active turn", meta: turnRouteMeta("turn"), active: false},
		{name: "no active turn and no envelope", meta: nil, active: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client := newFakeHermesClient()
			session := testSession(newTestAgent(), client)
			session.mu.Lock()
			session.turnNonce = "turn"
			session.turnEpoch = 1
			session.turnInFlight = testCase.active
			session.cancel = func() { t.Error("unauthenticated cancel fenced the turn") }
			session.pending = map[string]nativehermes.PermissionRequest{pendingPermission.ID: pendingPermission}
			session.questions = map[string]nativehermes.QuestionRequest{pendingQuestion.ID: pendingQuestion}
			session.mu.Unlock()

			err := session.cancelRouted(testCase.meta)

			require.Zero(t, client.abortCount(), "unauthenticated cancel reached native abort")
			require.Zero(t, client.closeCount(), "unauthenticated cancel closed the runtime")

			client.mu.Lock()
			replies := len(client.permissionReplies)
			rejects := len(client.questionRejects)
			client.mu.Unlock()
			require.Zero(t, replies, "unauthenticated cancel cancelled a pending permission")
			require.Zero(t, rejects, "unauthenticated cancel cancelled a pending question")

			require.Error(t, err, "unauthenticated cancel was applied")
		})
	}
}

func TestPromptFenceRemainingFailureBranches(t *testing.T) {
	t.Run("prompt resume admission failure", func(t *testing.T) {
		wantErr := errors.New("resume denied")
		agent := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return nil, wantErr
			},
		}))
		session := testSession(agent, newFakeHermesClient())
		session.runtimeNeedsResume = true
		if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "resume-denied", "reply")); !errors.Is(err, wantErr) {
			t.Fatalf("Prompt resume error = %v", err)
		}
	})

	t.Run("default timer fallback", func(t *testing.T) {
		agent := newTestAgent(WithTurnTimeout(time.Hour))
		agent.options.newPromptTimer = nil
		session := testSession(agent, newFakeHermesClient())
		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "default-timer", "reply"))
		if err != nil || response.StopReason != acp.StopReasonEndTurn {
			t.Fatalf("Prompt with default timer = %#v err=%v", response, err)
		}
	})

	t.Run("timeout fence failure", func(t *testing.T) {
		client := newFakeHermesClient()
		client.closeErr = errors.New("close failed")
		client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			<-ctx.Done()

			return nativehermes.NativeMessage{}, ctx.Err()
		}
		timeout := make(chan time.Time, 1)
		timeout <- time.Now()
		agent := newTestAgent(WithTurnTimeout(time.Hour))
		agent.options.newPromptTimer = func(time.Duration) promptTimer {
			return promptTimer{C: timeout, Stop: func() bool { return true }}
		}
		session := testSession(agent, client)
		if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "timeout-fence-error", "hang")); err == nil || !strings.Contains(err.Error(), "close failed") {
			t.Fatalf("timeout fence error = %v", err)
		}
	})
}

func TestTurnFenceLazyResumePreservesIdentityAndRejectsStaleRoute(t *testing.T) {
	oldClient := newFakeHermesClient()
	oldStarted := make(chan struct{})
	oldClient.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(oldStarted)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}

	scratch := t.TempDir()
	agent := newTestAgent(WithScratchDir(scratch))
	session := testSession(agent, oldClient)
	session.env = map[string]string{"HERMES_REBIND_TEST": "preserved"}
	rebindPathDir := t.TempDir()
	session.extraPathDirs = []string{rebindPathDir}
	session.mcpServers = []acp.McpServer{HTTPMCPServer("wagie", "http://127.0.0.1/mcp", map[string]string{"Authorization": "Bearer test"})}
	if err := session.snapshotToStore(t.Context()); err != nil {
		t.Fatalf("snapshot checkpoint: %v", err)
	}

	replacement := newFakeHermesClient()
	replacement.getSession = testNativeSession("native-1")
	replacementStarted := make(chan struct{})
	releaseReplacement := make(chan struct{})
	replacement.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(replacementStarted)
		<-releaseReplacement

		return nativehermes.NativeMessage{
			Info: nativehermes.NativeMessageInfo{ID: "replacement-message", SessionID: id, Role: valAssistant, Finish: "stop"},
		}, nil
	}

	factoryCalls := 0
	agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
		factoryCalls++
		if opts.ACPSessionID != nativehermes.ACPSessionIDString(session.id) {
			t.Fatalf("replacement ACP session id = %q", opts.ACPSessionID)
		}
		if opts.SessionEnv["HERMES_REBIND_TEST"] != "preserved" {
			t.Fatalf("replacement session env = %#v", opts.SessionEnv)
		}
		if !slices.Equal(opts.ExtraPathDirs, []string{rebindPathDir}) {
			t.Fatalf("replacement extra path dirs = %#v", opts.ExtraPathDirs)
		}
		if len(opts.MCPServers) != 1 {
			t.Fatalf("replacement MCP servers = %#v", opts.MCPServers)
		}
		if opts.ExistingXDG.Root == "" || !strings.HasPrefix(opts.ExistingXDG.Root, agent.homeRoot()) {
			t.Fatalf("replacement XDG = %#v, home=%q", opts.ExistingXDG, agent.homeRoot())
		}
		replacement.xdg = opts.ExistingXDG

		return replacement, nil
	}

	oldDone := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "old-turn", "hang"))
		oldDone <- struct {
			resp acp.PromptResponse
			err  error
		}{resp: resp, err: err}
	}()
	<-oldStarted
	if err := session.cancelRouted(turnRouteMeta("old-turn")); err != nil {
		t.Fatalf("cancel old turn: %v", err)
	}
	oldOut := <-oldDone
	if oldOut.err != nil || oldOut.resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("old Prompt = %#v err=%v", oldOut.resp, oldOut.err)
	}

	newDone := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "replacement-turn", "reply"))
		newDone <- struct {
			resp acp.PromptResponse
			err  error
		}{resp: resp, err: err}
	}()
	select {
	case <-replacementStarted:
	case out := <-newDone:
		t.Fatalf("replacement prompt ended before dispatch: %#v err=%v", out.resp, out.err)
	case <-time.After(time.Second):
		t.Fatal("replacement prompt did not dispatch")
	}

	if err := session.cancelRouted(turnRouteMeta("old-turn")); err == nil || !strings.Contains(err.Error(), "stale route turnNonce") {
		t.Fatalf("stale old route error = %v", err)
	}
	if got := replacement.closeCount(); got != 0 {
		t.Fatalf("stale route closed replacement %d times", got)
	}

	close(releaseReplacement)
	newOut := <-newDone
	if newOut.err != nil || newOut.resp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("replacement Prompt = %#v err=%v", newOut.resp, newOut.err)
	}
	if factoryCalls != 1 {
		t.Fatalf("replacement factory calls = %d, want 1", factoryCalls)
	}
	replacement.mu.Lock()
	getIDs := append([]string(nil), replacement.getSessionIDs...)
	reloads := replacement.reloadCalls
	replacement.mu.Unlock()
	if !slices.Equal(getIDs, []string{"native-1"}) {
		t.Fatalf("replacement GetSession ids = %#v", getIDs)
	}
	if reloads != 1 {
		t.Fatalf("replacement MCP reload calls = %d, want 1", reloads)
	}
	if session.currentTurnEpoch() != 2 {
		t.Fatalf("turn epoch = %d, want monotonic 2", session.currentTurnEpoch())
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatalf("close replacement: %v", err)
	}
}

func TestTurnLazyResumeSerializesWithSessionClose(t *testing.T) {
	oldClient := newFakeHermesClient()
	oldStarted := make(chan struct{})
	oldClient.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(oldStarted)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	agent := newTestAgent(WithScratchDir(t.TempDir()))
	session := testSession(agent, oldClient)
	if err := session.snapshotToStore(t.Context()); err != nil {
		t.Fatalf("snapshot checkpoint: %v", err)
	}

	oldDone := make(chan error, 1)
	go func() {
		_, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "old-close-race", "hang"))
		oldDone <- err
	}()
	<-oldStarted
	if err := session.cancelRouted(turnRouteMeta("old-close-race")); err != nil {
		t.Fatalf("cancel old turn: %v", err)
	}
	if err := <-oldDone; err != nil {
		t.Fatalf("old Prompt: %v", err)
	}

	replacement := newFakeHermesClient()
	replacement.getSession = testNativeSession("native-1")
	replacement.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	factoryEntered := make(chan struct{})
	releaseFactory := make(chan struct{})
	agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
		close(factoryEntered)
		<-releaseFactory
		replacement.xdg = opts.ExistingXDG

		return replacement, nil
	}

	promptDone := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "replacement-close-race", "hang"))
		promptDone <- struct {
			resp acp.PromptResponse
			err  error
		}{resp: resp, err: err}
	}()
	<-factoryEntered

	closeDone := make(chan error, 1)
	go func() { closeDone <- session.Close(context.Background()) }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close crossed an in-flight replacement install: %v", err)
	default:
	}

	close(releaseFactory)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
	out := <-promptDone
	if out.err != nil || out.resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("replacement Prompt during Close = %#v err=%v", out.resp, out.err)
	}
	if got := replacement.closeCount(); got != 1 {
		t.Fatalf("replacement close count = %d, want 1", got)
	}
}

func assertNoPromptSettlement(t *testing.T, done <-chan struct {
	resp acp.PromptResponse
	err  error
}, message string) {
	t.Helper()

	select {
	case out := <-done:
		t.Fatalf("%s: response=%#v error=%v", message, out.resp, out.err)
	default:
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
	agent := newTestAgent(WithTurnTimeout(40 * time.Millisecond))
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
	agent := newTestAgent(WithTurnTimeout(40 * time.Millisecond))
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
	agent := newTestAgent()
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

func TestFailedTurnResultGatewayDisconnectMarksStream(t *testing.T) {
	client := newFakeHermesClient()
	session := testSession(newTestAgent(), client)
	turnCtx := session.beginTurn(t.Context(), "gateway-disconnect")
	turnEpoch := session.currentTurnEpoch()
	defer session.finishTurn()

	sendErr := fmt.Errorf("send frame: %w", nativehermes.ErrGatewayDisconnected)
	_, err := session.failedTurnResult(turnCtx, turnEpoch, sendErr)
	requireTurnFailure(t, err, nativehermes.CauseTransport, "send frame")
	if client.closeCount() != 1 || !session.needsRuntimeResume() {
		t.Fatalf("gateway disconnect close=%d needsResume=%v", client.closeCount(), session.needsRuntimeResume())
	}
	if !session.suppressBacklog() {
		t.Fatal("gateway disconnect did not mark the stream failed for backlog suppression")
	}
}

func TestFailedTurnResultReturnsFenceFailure(t *testing.T) {
	wantErr := errors.New("close proof failed")
	client := newFakeHermesClient()
	client.closeErr = wantErr
	session := testSession(newTestAgent(), client)
	turnCtx := session.beginTurn(t.Context(), "failed-fence")
	turnEpoch := session.currentTurnEpoch()
	defer session.finishTurn()

	_, err := session.failedTurnResult(turnCtx, turnEpoch, nativehermes.NewTurnFailure(nativehermes.CauseProvider, "provider failed"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("failedTurnResult fence error = %v, want %v", err, wantErr)
	}
}

func TestAdmittedUpdateFailureReturnsFenceFailure(t *testing.T) {
	wantErr := errors.New("close proof failed")
	client := newFakeHermesClient()
	client.closeErr = wantErr
	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{
			Info:  nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: valAssistant, Finish: "stop"},
			Parts: []nativehermes.Part{{ID: "part", SessionID: id, MessageID: "assistant", Type: valText, Text: "done"}},
		}, nil
	}
	conn := newRecordingAgentClient()
	conn.updateErr = errors.New("update failed")
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	_, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "update-fence-failure", "reply"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("admitted update fence error = %v, want %v", err, wantErr)
	}
	if poisonErr := session.ensureNotPoisoned(); poisonErr == nil || !strings.Contains(poisonErr.Error(), "session_poisoned") {
		t.Fatalf("admitted update fence poison error = %v", poisonErr)
	}
}

// blobResourceBlock builds an embedded resource whose payload is a blob of an
// arbitrary MIME.
func blobResourceBlock(blob, mimeType, uri string) acp.ContentBlock {
	contents := &acp.BlobResourceContents{Blob: blob, Uri: uri}
	if mimeType != "" {
		contents.MimeType = &mimeType
	}

	return acp.ContentBlock{Resource: &acp.ContentBlockResource{Type: "resource", Resource: acp.EmbeddedResourceResource{
		BlobResourceContents: contents,
	}}}
}

// TestEmbeddedBlobResourceBytesAreGated pins that a blob resource is bounded
// and decodable whatever it declares: the channel carries bytes, so it answers
// to the same decoded-byte budget an image blob does.
func TestEmbeddedBlobResourceBytesAreGated(t *testing.T) {
	limits := applyOptions(nil).ImageLimits

	t.Run("a document blob above the per-image limit is rejected", func(t *testing.T) {
		const oversize = 6_295_951

		blob := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'%'}, oversize))

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
			blobResourceBlock(blob, "application/pdf", "file:///tmp/report.pdf"),
		}, limits, "")
		requireImageInputError(t, err, map[string]any{
			keyField:       acpFieldPromptResource,
			jsonFieldError: imageErrTooLarge,
			keyIndex:       0,
			keySizeBytes:   int64(oversize),
			keyMaxBytes:    limits.MaxInputBytesPerImage,
		})
	})

	t.Run("corrupt base64 in a blob is rejected", func(t *testing.T) {
		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
			blobResourceBlock("not-base64", "application/pdf", "file:///tmp/report.pdf"),
		}, limits, "")
		requireImageInputError(t, err, map[string]any{
			keyField:       acpFieldPromptResource,
			jsonFieldError: imageErrInvalidBase64,
			keyIndex:       0,
		})
	})

	t.Run("blob bytes count toward the per-prompt aggregate", func(t *testing.T) {
		document := bytes.Repeat([]byte{'%'}, 4096)
		png := fixtureBytes(t, "valid.png")
		total := int64(len(document) + len(png))

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
			blobResourceBlock(base64.StdEncoding.EncodeToString(document), "application/pdf", "file:///tmp/report.pdf"),
			{Image: &acp.ContentBlockImage{Type: "image", Data: base64.StdEncoding.EncodeToString(png), MimeType: mimePNG}},
		}, ImageLimits{MaxInputBytesPerPrompt: total - 1}, "")
		requireImageInputError(t, err, map[string]any{
			keyField:       acpFieldPromptImage,
			jsonFieldError: imageErrTooLarge,
			keyIndex:       1,
			keySizeBytes:   total,
			keyMaxBytes:    total - 1,
		})
	})

	t.Run("a conforming blob still degrades to its uri", func(t *testing.T) {
		parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
			blobResourceBlock(base64.StdEncoding.EncodeToString([]byte("plain")), "application/pdf", "file:///tmp/report.pdf"),
		}, limits, "")
		if err != nil {
			t.Fatalf("promptToHermesParts: %v", err)
		}
		if !reflect.DeepEqual(parts, []map[string]any{{keyType: valText, valText: "file:///tmp/report.pdf"}}) {
			t.Fatalf("blob parts = %#v", parts)
		}
	})
}

// TestBlobResourceMediaTypeNormalization pins that a raster declaration is
// routed to the image gates however it is spelled, so no image blob escapes
// validation through a case or parameter difference.
func TestBlobResourceMediaTypeNormalization(t *testing.T) {
	png := base64.StdEncoding.EncodeToString(fixtureBytes(t, "valid.png"))

	for _, mimeType := range []string{"IMAGE/PNG", "Image/Png", " image/png ", "image/png; charset=binary", "IMAGE/PNG;charset=binary"} {
		t.Run(mimeType, func(t *testing.T) {
			parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
				blobResourceBlock(png, mimeType, "file:///tmp/pixels.png"),
			}, applyOptions(nil).ImageLimits, "")
			requireImageInputError(t, err, map[string]any{
				keyField:       acpFieldPromptResource,
				jsonFieldError: imageErrInvalidMediaType,
				keyIndex:       0,
			})
			if parts != nil {
				t.Fatalf("parts built for a rejected raster declaration: %#v", parts)
			}
		})
	}

	parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
		blobResourceBlock(png, mimePNG, "file:///tmp/pixels.png"),
	}, applyOptions(nil).ImageLimits, "")
	if err != nil {
		t.Fatalf("canonical raster blob: %v", err)
	}
	if parts[0][keyType] != valFile {
		t.Fatalf("canonical raster blob part = %#v", parts[0])
	}
	if _, ok := parts[0][keyFilename]; ok {
		t.Fatalf("blob resource derived a native filename from its uri: %#v", parts[0])
	}
}

func TestSharedHomePromptSessionSetLock(t *testing.T) {
	home := t.TempDir()
	agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
	client := newFakeHermesClient()
	session := testSession(agent, client)
	response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "shared-lock", "reply"))
	if err != nil || response.StopReason == "" {
		t.Fatalf("shared-home prompt=%+v err=%v", response, err)
	}

	lock, err := nativehermes.AcquireSharedSessionSetLock(t.Context(), home, nativehermes.SharedSessionSetLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Release() }()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := session.Prompt(ctx, TextPromptRequest(session.id, "blocked-lock", "blocked")); err == nil {
		t.Fatal("contended shared-home turn lock succeeded")
	}
}

func TestPermissionAndQuestionCarryLifecycleActions(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	turnCtx := session.beginTurn(t.Context(), "turn")
	require.NoError(t, session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}))

	require.NoError(t, session.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool")))
	require.NoError(t, session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"}))

	conn.mu.Lock()
	permissionMeta := conn.permissions[0].Meta
	elicitationMeta := conn.elicitations[0].Form.Meta
	conn.mu.Unlock()
	require.Contains(t, permissionMeta, lifecycle.MetaKey)
	require.Contains(t, elicitationMeta, lifecycle.MetaKey)
	require.Empty(t, session.actionRequests)
}

func TestLifecycleActionAdmissionFailuresRejectNativeRequests(t *testing.T) {
	t.Run("permission without owner", func(t *testing.T) {
		session, _, turnCtx := newLifecycleActionSession(t, false)
		err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool"))
		require.ErrorContains(t, err, "outside an accepted lifecycle turn")
	})

	t.Run("question without owner", func(t *testing.T) {
		session, _, turnCtx := newLifecycleActionSession(t, false)
		err := session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
		require.NoError(t, err)
		client, ok := session.client.(*fakeHermesClient)
		require.True(t, ok)
		require.Equal(t, 1, client.questionRejectCount())
	})

	t.Run("permission announcement", func(t *testing.T) {
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		session.agent.setAgentClient(&lifecycleFailingAgentClient{recordingAgentClient: conn, failAt: 1})
		err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool"))
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})

	t.Run("question announcement", func(t *testing.T) {
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		session.agent.setAgentClient(&lifecycleFailingAgentClient{recordingAgentClient: conn, failAt: 1})
		err := session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})
}

func TestLifecycleResolutionFailureDominatesCallbackResult(t *testing.T) {
	runPermission := func(t *testing.T, callbackErr error) {
		t.Helper()
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		conn.permErr = callbackErr
		conn.permissionStarted = make(chan struct{}, 1)
		conn.permissionRelease = make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- session.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool"))
		}()
		<-conn.permissionStarted
		setLifecycleDeliveryError(conn)
		close(conn.permissionRelease)
		require.ErrorContains(t, <-done, "lifecycle delivery failed")
	}

	t.Run("permission callback error", func(t *testing.T) {
		runPermission(t, errors.New("permission callback failed"))
	})
	t.Run("permission answer", func(t *testing.T) {
		runPermission(t, nil)
	})

	runQuestion := func(t *testing.T, response acp.UnstableCreateElicitationResponse, callbackErr error) {
		t.Helper()
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		conn.elicitation = response
		conn.elicitErr = callbackErr
		conn.elicitationStarted = make(chan struct{}, 1)
		conn.elicitationRelease = make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, nativehermes.QuestionRequest{ID: "question", SessionID: "native-1"})
		}()
		<-conn.elicitationStarted
		setLifecycleDeliveryError(conn)
		close(conn.elicitationRelease)
		require.ErrorContains(t, <-done, "lifecycle delivery failed")
	}

	t.Run("question callback error", func(t *testing.T) {
		runQuestion(t, acp.UnstableCreateElicitationResponse{}, errors.New("question callback failed"))
	})
	t.Run("question decline", func(t *testing.T) {
		runQuestion(t, acp.NewUnstableCreateElicitationResponseDecline(), nil)
	})
	t.Run("question answer", func(t *testing.T) {
		runQuestion(t, acp.UnstableCreateElicitationResponse{
			Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{}},
		}, nil)
	})
}

func TestLifecycleCorrelationAndCancelAreValidatedBeforeDispatch(t *testing.T) {
	reserved := map[string]any{lifecycle.MetaKey: map[string]any{}}
	session := testSession(newTestAgent(), newFakeHermesClient())
	require.Error(t, session.cancelRouted(reserved))

	turnCtx := session.beginTurn(t.Context(), "turn")
	_ = turnCtx
	session.mu.Lock()
	session.turnInFlight = true
	session.mu.Unlock()
	activeMeta := turnRouteMeta("turn")
	activeMeta[lifecycle.MetaKey] = map[string]any{}
	require.Error(t, session.cancelRouted(activeMeta))

	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	negotiatedSession := testSession(agent, newFakeHermesClient())
	_, err := negotiatedSession.Prompt(t.Context(), acp.PromptRequest{
		SessionId: negotiatedSession.id,
		Meta:      turnRouteMeta("turn"),
		Prompt:    []acp.ContentBlock{acp.TextBlock("hello")},
	})
	require.Error(t, err)
}

func TestTerminalOutcomeFromHermesMapsStopReasonAndOutcome(t *testing.T) {
	reason, outcome, mapped := terminalOutcomeFromHermes("max_turns")
	require.True(t, mapped)
	require.Equal(t, acp.StopReasonMaxTurnRequests, reason)
	require.Equal(t, lifecycle.OutcomeLimit, outcome)

	// The vocabulary is closed: an unrecognized or empty finish names no stop
	// reason and records the failed outcome, so nothing downstream can read it
	// as a clean end of turn.
	for _, finish := range []string{"", "   ", "tool_calls", "who_knows"} {
		reason, outcome, mapped = terminalOutcomeFromHermes(finish)
		require.False(t, mapped, "finish %q was mapped", finish)
		require.Empty(t, reason, "finish %q named a stop reason", finish)
		require.Equal(t, lifecycle.OutcomeFailed, outcome)
	}
}

// TestPromptFailsOnUnmappedNativeFinish pins the turn-level consequence: a
// native terminal outside the closed finish vocabulary fails the turn with the
// v1 error rather than reporting end_turn over a terminal this adapter cannot
// read.
func TestPromptFailsOnUnmappedNativeFinish(t *testing.T) {
	client := newFakeHermesClient()
	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{
			ID: "assistant-1", SessionID: id, Role: valAssistant, Finish: "tool_calls",
		}}, nil
	}

	session := testSession(newTestAgent(), client)
	resp, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "unknown-finish", "reply"))
	require.Error(t, err)
	require.Empty(t, resp.StopReason, "unmapped finish named an ACP v1 stop reason")

	var reqErr *acp.RequestError

	require.ErrorAs(t, err, &reqErr)

	data, _ := reqErr.Data.(map[string]any)
	require.Equal(t, valHermesTurnFailed, data[jsonFieldError])
	require.Equal(t, string(nativehermes.CauseProvider), data[jsonFieldCause])
	require.Contains(t, data[jsonFieldMessage], "tool_calls")
}
