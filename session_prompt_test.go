package hermesacp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

type publicationFailControlClient struct {
	*recordingAgentClient
	started   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
}

func (c *publicationFailControlClient) SessionUpdate(
	ctx context.Context,
	notification acp.SessionNotification,
) error {
	if _, lifecycleUpdate := notification.Meta[lifecycle.MetaKey]; lifecycleUpdate {
		return errors.New("lifecycle delivery failed")
	}

	return c.recordingAgentClient.SessionUpdate(ctx, notification)
}

func (c *publicationFailControlClient) RequestPermissionRegistered(
	ctx context.Context,
	_ acp.RequestPermissionRequest,
	written chan<- error,
) (acp.RequestPermissionResponse, error) {
	written <- nil
	close(c.started)
	<-ctx.Done()
	close(c.cancelled)
	<-c.release

	return acp.RequestPermissionResponse{}, ctx.Err()
}

func (c *publicationFailControlClient) CreateElicitationRegistered(
	ctx context.Context,
	_ acp.UnstableCreateElicitationRequest,
	_ elicitationScope,
	written chan<- error,
) (acp.UnstableCreateElicitationResponse, error) {
	written <- nil
	close(c.started)
	<-ctx.Done()
	close(c.cancelled)
	<-c.release

	return acp.UnstableCreateElicitationResponse{}, ctx.Err()
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

func (c *strictHermesPermissionClient) RequestPermissionRegistered(
	ctx context.Context,
	request acp.RequestPermissionRequest,
	written chan<- error,
) (acp.RequestPermissionResponse, error) {
	if written != nil {
		written <- nil
	}

	return c.RequestPermission(ctx, request)
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
	req.CycleID = testControlCycleID
	req.TransportGeneration = 1

	return req
}

func TestPermissionPublishesExactNativeToolPendingBeforeCallback(t *testing.T) {
	const turnNonce = "permission-turn"

	client := newFakeHermesClient()
	conn := newStrictHermesPermissionClient(turnNonce)
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(t, agent, client)
	turnCtx := beginTestControlTurn(t, session, t.Context(), turnNonce)
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

func TestPermissionAndNativeStartShareOneToolLifecycle(t *testing.T) {
	const turnNonce = "permission-turn"

	t.Run("native start wins", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newStrictHermesPermissionClient(turnNonce)
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), turnNonce)
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), turnNonce)
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
			name: "stale actor route",
			ctx:  func(ctx context.Context) context.Context { return ctx },
			req: func(t *testing.T) nativehermes.PermissionRequest {
				t.Helper()

				req := testHermesPermissionRequest(t, "request-stale", "tool-stale")
				req.CycleID = "stale-cycle"

				return req
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
			session := testSession(t, agent, client)
			turnCtx := beginTestControlTurn(t, session, t.Context(), turnNonce)
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
		session := testSession(t, agent, client)
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "permission-turn")
		route, active := session.permissionTurnRoute(turnCtx)
		if !active {
			t.Fatal("permission turn was not active")
		}
		req := testHermesPermissionRequest(t, "request", "tool")

		session.pumpMu.Lock()
		delete(session.pumpRoutes, route.cycleID)
		session.pumpMu.Unlock()
		if err := session.ensurePermissionToolPending(turnCtx, req, route); err == nil || !strings.Contains(err.Error(), "crossed") {
			t.Fatalf("stale pending admission error = %v", err)
		}

		session.pumpMu.Lock()
		session.pumpRoutes[route.cycleID] = route.pump
		session.pumpMu.Unlock()
		conn.updateErr = errors.New("pending failed")
		if err := session.ensurePermissionToolPending(turnCtx, req, route); err == nil || !strings.Contains(err.Error(), "pending failed") {
			t.Fatalf("pending publication error = %v", err)
		}
		session.finishTurn()
	})

	t.Run("route changes after pending publication", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newTestAgent()
		session := testSession(t, agent, client)
		base := newRecordingAgentClient()
		conn := &sessionUpdateHookClient{recordingAgentClient: base}
		conn.afterUpdate = func() {
			session.pumpMu.Lock()
			delete(session.pumpRoutes, testControlCycleID)
			session.pumpMu.Unlock()
		}
		agent.setAgentClient(conn)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "permission-turn")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "permission-turn")
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
			session := testSession(t, agent, client)
			turnCtx := beginTestControlTurn(t, session, t.Context(), "permission-turn")
			defer session.finishTurn()
			done := make(chan error, 1)
			go func() {
				done <- session.handlePermission(turnCtx, testHermesPermissionRequest(t, "request", "tool"))
			}()
			<-conn.permissionStarted
			session.pumpMu.Lock()
			delete(session.pumpRoutes, testControlCycleID)
			session.pumpMu.Unlock()
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
		session := testSession(t, newTestAgent(), newFakeHermesClient())
		require.ErrorContains(t, session.handlePermission(t.Context(), nativehermes.PermissionRequest{}), "missing exact ownership")
	})
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, ctx, "turn-question-1")
		defer session.finishTurn()

		req := nativehermes.QuestionRequest{
			ID:                  "question-1",
			SessionID:           "native-1",
			CycleID:             testControlCycleID,
			TransportGeneration: 1,
			Tool:                nativehermes.QuestionTool{MessageID: "message-1", CallID: "call-1"},
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, ctx, "decline-question")
		defer session.finishTurn()
		require.NoError(t, session.handleQuestion(turnCtx, testHermesQuestionRequest("q")))
		if client.questionRejectCount() != 1 {
			t.Fatalf("question rejects = %d, want 1", client.questionRejectCount())
		}
	})

	t.Run("missing capability rejects without ACP request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, ctx, "unsupported-question")
		defer session.finishTurn()
		require.NoError(t, session.handleQuestion(turnCtx, testHermesQuestionRequest("q")))
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
	session := testSession(t, agent, client)

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
	session := testSession(t, agent, client)
	ctx := beginTestControlTurn(t, session, t.Context(), "permission-turn")
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

func TestPermissionQuestionRawMetadataIsRefused(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	agent.setAgentClient(conn)
	session := testSession(t, agent, client)
	ctx := beginTestControlTurn(t, session, t.Context(), "permission-turn")
	defer session.finishTurn()

	require.ErrorContains(t, session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       "approval.request",
		Properties: json.RawMessage(`{"id":"perm-raw","sessionID":"native-1"}`),
	}), "missing typed ownership")
	require.ErrorContains(t, session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       "clarify.request",
		Properties: json.RawMessage(`{"request_id":"question-raw","sessionID":"native-1"}`),
	}), "missing typed ownership")
	require.Zero(t, conn.permissionRequestCount())
	require.Empty(t, conn.elicitations)
	require.Zero(t, client.permissionReplyCount())
	require.Zero(t, client.questionReplyCount())
}

func TestEventMappingMessagePartToolUsageAndRaw(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(t, agent, client)
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
	if len(conn.extensions) != 0 {
		t.Fatalf("prompt mapper duplicated raw projection: %#v", conn.extensions)
	}
}

func TestGatewayToolPartsEmitACPStartAndResult(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(t, agent, client)

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
		{name: "empty completion", streamed: "narration"},
		{name: "fully streamed", complete: "final answer", streamed: "final answer"},
		{name: "completion suffix", complete: "final answer", streamed: "final ", want: "answer"},
		{name: "narrated tool turn", complete: "final answer", streamed: "Reading the file.\n\nfinal answer\n"},
		{name: "answer the stream never carried", complete: "replacement", streamed: "already sent", want: "replacement"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			updates, delivered := partUpdates(valAssistant, nativehermes.Part{
				MessageID:    "message-1",
				Type:         valText,
				Text:         test.complete,
				StreamedText: test.streamed,
			})
			if delivered != test.want {
				t.Fatalf("delivered text = %q, want %q", delivered, test.want)
			}
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

func TestDeliveredCompletionSuffixIsTheRecordedForegroundPrefix(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(t, agent, newFakeHermesClient())
	t.Cleanup(session.stopPump)

	parts := []nativehermes.Part{
		{MessageID: "message-1", Type: valText, Text: "final "},
		{MessageID: "message-1", Type: valText, Text: "final answer", StreamedText: "final "},
		// A repeated full completion has no append-only suffix.
		{MessageID: "message-1", Type: valText, Text: "final answer", StreamedText: "final answer"},
		// A narrated turn ends its delta stream with the final response, so the
		// completion that repeats it has nothing left to deliver.
		{MessageID: "message-1", Type: valText, Text: "final answer", StreamedText: "narration. final answer\n"},
	}
	for _, part := range parts {
		require.NoError(t, session.emitPartUpdates(t.Context(), valAssistant, part))
	}

	conn.mu.Lock()
	updates := append([]acp.SessionNotification(nil), conn.updates...)
	conn.mu.Unlock()
	chunks := make([]string, 0, len(updates))
	for _, update := range updates {
		if chunk := update.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
			chunks = append(chunks, chunk.Content.Text.Text)
		}
	}
	require.Equal(t, []string{"final ", "answer"}, chunks)
	require.Equal(t, "final answer", session.foregroundPrefix())
}

func TestReplayStopsBeforePublishingTheNextMessageAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	base := newRecordingAgentClient()
	var once sync.Once
	conn := &sessionUpdateHookClient{recordingAgentClient: base, afterUpdate: func() { once.Do(cancel) }}
	agent := newTestAgent()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	client.messages = []nativehermes.NativeMessage{
		{
			Info:  nativehermes.NativeMessageInfo{ID: "message-1", SessionID: "native-1", Role: valAssistant},
			Parts: []nativehermes.Part{{MessageID: "message-1", SessionID: "native-1", Type: valText, Text: "first"}},
		},
		{
			Info:  nativehermes.NativeMessageInfo{ID: "message-2", SessionID: "native-1", Role: valAssistant},
			Parts: []nativehermes.Part{{MessageID: "message-2", SessionID: "native-1", Type: valText, Text: "second"}},
		},
	}
	session := testSession(t, agent, client)
	require.ErrorIs(t, session.replayMessages(ctx), context.Canceled)
	require.Equal(t, 1, base.updateCount())
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
		session := testSession(t, agent, newFakeHermesClient())

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
		session := testSession(t, agent, newFakeHermesClient())

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
	session := testSession(t, agent, client)
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
	client.emitError(errors.New("stream closed"))
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
	session := testSession(t, agent, client)

	_, err := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
	requireTurnFailure(t, err, nativehermes.CauseTransport, "connection reset by peer")
	if client.abortCount() != 1 {
		t.Fatalf("abort count = %d, want 1", client.abortCount())
	}
}

func TestPromptIdleGatewayDisconnectRequiresResumeBeforeNextTurn(t *testing.T) {
	client := newFakeHermesClient()
	client.emitError(errors.New("idle stream closed"))
	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{
			Info:  nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
			Parts: []nativehermes.Part{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "ok"}},
		}, nil
	}
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(t, agent, client)
	session.pumpMu.Lock()
	pumpDone := session.pumpDone
	session.pumpMu.Unlock()
	<-pumpDone

	_, err := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}})
	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	data, ok := reqErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, valHermesSessionPoisoned, data[jsonFieldError])
	require.Equal(t, poisonRuntimeResumeFailed, data[jsonFieldCause])
	require.Zero(t, client.abortCount())
}

func TestPromptCleanEOFSentinelDisconnectAbortsTurn(t *testing.T) {
	client := newFakeHermesClient()
	agent := newTestAgent()
	session := testSession(t, agent, client)
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
	client.emitError(errors.New("websocket closed"))
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
				req := testHermesPermissionRequest(t, "perm", "tool-perm")
				req.CycleID = "fake/native-1/cycle-1"
				client.emitEvent(nativehermes.TurnEvent{
					Type: evtApprovalRequest, CycleID: req.CycleID, TransportGeneration: 1,
					Permission: &req,
				})
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
				req := testHermesQuestionRequest("question")
				req.CycleID = "fake/native-1/cycle-1"
				req.Questions = []nativehermes.QuestionInfo{{
					Question: "Pick one", Options: []nativehermes.QuestionOption{{Label: "Yes"}},
				}}
				client.emitEvent(nativehermes.TurnEvent{
					Type: evtClarifyRequest, CycleID: req.CycleID, TransportGeneration: 1,
					Question: &req,
				})
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
			session := testSession(t, agent, client)
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
	session := testSession(t, agent, client)
	ctx := beginTestControlTurn(t, session, t.Context(), "missing-live-turn")
	defer session.finishTurn()

	err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type: evtApprovalRequest,
		Permission: func() *nativehermes.PermissionRequest {
			req := testHermesPermissionRequest(t, "perm-missing-live", "tool-missing-live")

			return &req
		}(),
	})
	if err == nil {
		t.Fatal("missing live mapping did not fail permission handling")
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("missing live mapping error type = %T", err)
	}
	data, _ := reqErr.Data.(map[string]any)
	if data["error"] != valHermesSessionPoisoned || data["cause"] != poisonMissingLiveSessionMapping {
		t.Fatalf("missing live mapping error data = %#v", data)
	}
	if err := session.ensureNotPoisoned(); err == nil {
		t.Fatal("missing live mapping did not poison session")
	}
}

func TestPermissionCancelledReplyBranches(t *testing.T) {
	t.Run("permission without connection rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(t, newTestAgent(), client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "test-turn")
		if err := session.handlePermission(turnCtx, testHermesPermissionRequest(t, "perm", "tool-perm")); err == nil {
			t.Fatal("permission emission without a connection did not fail closed")
		}
		if got := client.permissionReply(0).message; got != "stale or unknown tool call" {
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
		session := testSession(t, agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := beginTestControlTurn(t, session, ctx, "test-turn")
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
		session := testSession(t, agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := beginTestControlTurn(t, session, ctx, "test-turn")
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
		session := testSession(t, agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := beginTestControlTurn(t, session, ctx, "test-turn")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "test-turn")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "test-turn")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, context.Background(), "test-turn")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, context.Background(), "test-turn")
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
		session := testSession(t, newTestAgent(), client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := beginTestControlTurn(t, session, ctx, "test-turn")
		cancel()
		require.ErrorContains(t,
			session.handleQuestion(turnCtx, testHermesQuestionRequest("question")),
			"outside its owning cycle")
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
		session := testSession(t, agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := beginTestControlTurn(t, session, ctx, "test-turn")
		cancel()
		err := session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "outside its owning cycle")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "decline-reply-error")
		defer session.finishTurn()
		require.ErrorContains(t,
			session.handleQuestion(turnCtx, testHermesQuestionRequest("question")),
			"reject failed")
	})

	t.Run("question decline after context cancellation returns cancelled", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		conn.elicitation = acp.NewUnstableCreateElicitationResponseDecline()
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(t, agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := beginTestControlTurn(t, session, ctx, "test-turn")
		cancel()
		err := session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "outside its owning cycle")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, context.Background(), "test-turn")
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "elicitation-error")
		defer session.finishTurn()
		err := session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "elicitation failed")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, t.Context(), "elicitation-and-reject-error")
		defer session.finishTurn()
		err := session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "elicitation failed")
		require.ErrorContains(t, err, "reject failed")
	})

	t.Run("question accept after context cancellation rejects native request", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		agent.setAgentClient(conn)
		session := testSession(t, agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := beginTestControlTurn(t, session, ctx, "test-turn")
		cancel()
		err := session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "outside its owning cycle")
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
		session := testSession(t, agent, client)
		ctx, cancel := context.WithCancel(context.Background())
		turnCtx := beginTestControlTurn(t, session, ctx, "test-turn")
		cancel()
		err := session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "outside its owning cycle")
		require.ErrorContains(t, err, "reject failed")
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, context.Background(), "test-turn")
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
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
		session := testSession(t, agent, client)
		turnCtx := beginTestControlTurn(t, session, context.Background(), "test-turn")
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
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
				Blob: fixtureBase64(t, "valid.png"), MimeType: new("image/png"), Uri: "file:///tmp/image",
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
	if !reflect.DeepEqual(emptyReqErr.Data, map[string]any{jsonFieldError: valUnsupported, jsonFieldField: acpFieldPrompt}) {
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
	session := testSession(t, agent, client)
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
	session := testSession(t, newTestAgent(), client)
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
		agent := newTestAgent(WithScratchDir(durableTempDir(t)))
		session := testSession(t, agent, client)
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
		session := testSession(t, newTestAgent(), client)
		session.mcpServers = []acp.McpServer{HTTPMCPServer("wagie", "http://127.0.0.1/mcp", nil)}

		_, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "reload-fail", "reply"))
		if err == nil || !strings.Contains(err.Error(), poisonMCPReloadFailed) || !strings.Contains(err.Error(), "reload unavailable") {
			t.Fatalf("reload failure = %v", err)
		}
		_, nextErr := session.Prompt(t.Context(), TextPromptRequest(session.id, "reload-after-fail", "reply"))
		if nextErr == nil || !strings.Contains(nextErr.Error(), "session_poisoned") {
			t.Fatalf("post-reload-failure Prompt = %v", nextErr)
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
		session := testSession(t, agent, client)
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
		session := testSession(t, newTestAgent(), client)
		if _, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil {
			t.Fatal("send error prompt succeeded")
		}
	})

	t.Run("snapshot error after final message", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newTestAgent(WithSessionStore(&errorSessionStore{err: errors.New("snapshot failed")}))
		session := testSession(t, agent, client)
		client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
			return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
		}
		if _, err := session.Prompt(ctx, acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("hello")}}); err == nil ||
			!strings.Contains(err.Error(), "snapshot failed") {
			t.Fatalf("snapshot error = %v", err)
		}
	})

	t.Run("prompt validation and turn backpressure", func(t *testing.T) {
		session := testSession(t, newTestAgent(), newFakeHermesClient())
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
		session := testSession(t, newTestAgent(), client)
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
		session := testSession(t, agent, client)
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
		session := testSession(t, agent, client)
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
		session := testSession(t, agent, client)
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
	if err == nil || !strings.Contains(err.Error(), poisonNativeSessionIDDrift) || !strings.Contains(err.Error(), gotNativeID) {
		t.Fatalf("drift error = %v", err)
	}
	if conn.updateCount() != 0 {
		t.Fatalf("updates after poison = %#v", conn.updates)
	}
	if store.replaceCount() != 0 {
		t.Fatalf("store writes after poison = %d, want 0", store.replaceCount())
	}
	_, nextErr := session.Prompt(context.Background(), acp.PromptRequest{Meta: turnRouteMeta("test-turn"), SessionId: session.id, Prompt: []acp.ContentBlock{acp.TextBlock("again")}})
	if nextErr == nil || !strings.Contains(nextErr.Error(), valHermesSessionPoisoned) {
		t.Fatalf("subsequent poison error = %v", nextErr)
	}
	// The latched refusal names the closed cause and nothing else: the native
	// identity that produced it stays off every later answer.
	if strings.Contains(nextErr.Error(), gotNativeID) {
		t.Fatalf("subsequent poison error leaked the native id: %v", nextErr)
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

	t.Run("a mid-turn part update streams before the final message", func(t *testing.T) {
		client := newFakeHermesClient()
		conn := newRecordingAgentClient()
		agent := newTestAgent()
		agent.setAgentClient(conn)
		session := testSession(t, agent, client)
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
		client.emitEvent(nativehermes.TurnEvent{
			Type: "message.part.updated", CycleID: "fake/native-1/cycle-1", TransportGeneration: 1,
			Properties: json.RawMessage(`{"id":"event-part","sessionID":"native-1","messageID":"assistant","type":"text","text":"stream"}`),
		})
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
		session := testSession(t, agent, client)
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
		client.emitEvent(nativehermes.TurnEvent{
			Type: "message.part.updated", CycleID: "fake/native-1/cycle-1", TransportGeneration: 1,
			Properties: json.RawMessage(`{"id":"event-part","sessionID":"native-1","messageID":"assistant","type":"text","text":"stream"}`),
		})
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
		session := testSession(t, agent, client)
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
	session := testSession(t, agent, client)

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
	// Replay carries the row's own text. An empty chunk is what a history
	// decoder reading a key the gateway never sends would produce, so the text
	// is asserted, not merely the update kind.
	if conn.updateCount() != 1 || conn.updates[0].Update.UserMessageChunk == nil {
		t.Fatalf("replay updates = %#v", conn.updates)
	}
	if replayed := conn.updates[0].Update.UserMessageChunk.Content.Text; replayed == nil || replayed.Text != "user text" {
		t.Fatalf("replayed user chunk = %#v", conn.updates[0].Update.UserMessageChunk.Content)
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
	nilConnSession := testSession(t, newTestAgent(), newFakeHermesClient())
	if err := nilConnSession.emitUpdate(ctx, acp.UpdatePlan(acp.PlanEntry{Content: "no client"})); err == nil {
		t.Fatal("emitUpdate without conn claimed success")
	}

	noConnClient := newFakeHermesClient()
	noConnSession := testSession(t, newTestAgent(), noConnClient)
	require.ErrorContains(t, noConnSession.handlePermission(ctx, nativehermes.PermissionRequest{}), "missing exact ownership")
	noConnSession.pending = nil
	noConnTurnCtx := beginTestControlTurn(t, noConnSession, t.Context(), "no-connection-turn")
	if err := noConnSession.handlePermission(noConnTurnCtx, testHermesPermissionRequest(t, "p", "tool-p")); err == nil {
		t.Fatal("nil conn permission claimed success")
	}
	noConnSession.finishTurn()
	if got := noConnClient.permissionReply(0).message; got != "stale or unknown tool call" {
		t.Fatalf("nil conn permission reply = %q", got)
	}
	session.questions = nil
	require.ErrorContains(t, session.handleQuestion(ctx, nativehermes.QuestionRequest{}), "missing exact ownership")
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	conn.elicitErr = errors.New("elicitation failed")
	turnCtx := beginTestControlTurn(t, session, t.Context(), "event-edge-turn")
	require.ErrorContains(t,
		session.handleQuestion(turnCtx, testHermesQuestionRequest("q-error")),
		"elicitation failed")
	conn.elicitErr = nil
	testApprovalAndClarifyEventBranches(t, turnCtx, session, client, conn)
	session.finishTurn()
	testEmptyAndMalformedEventMapperBranches(t, ctx, session)
}

// TestForeignNativeSessionEventsAreRefused pins the guard every turn event
// carries: the gateway stream is per-process, not per-session, so an event
// naming another native session is not this session's business. Each of the
// three turn events is fed in the shape the adapter really receives, with a
// native session id this session does not own.
func TestForeignNativeSessionEventsAreRefused(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}

	session := testSession(t, agent, client)
	ctx := session.beginTurn(t.Context(), "foreign-session-turn")

	defer session.finishTurn()

	for _, test := range []struct {
		name       string
		event      nativehermes.TurnEvent
		wantAction string
	}{
		{
			name: "approval.request",
			event: nativehermes.TurnEvent{
				Type: evtApprovalRequest,
				Permission: &nativehermes.PermissionRequest{
					ID: "p-foreign", SessionID: "native-other", CycleID: testControlCycleID,
					TransportGeneration: 1,
				},
			},
			wantAction: "no permission request",
		},
		{
			name: "clarify.request",
			event: nativehermes.TurnEvent{
				Type: evtClarifyRequest,
				Question: &nativehermes.QuestionRequest{
					ID: "q-foreign", SessionID: "native-other",
					CycleID: testControlCycleID, TransportGeneration: 1,
				},
			},
			wantAction: "no elicitation",
		},
		{
			name: "message.part.updated",
			event: nativehermes.TurnEvent{
				Type:       "message.part.updated",
				Properties: json.RawMessage(`{"id":"part-foreign","sessionID":"native-other","messageID":"assistant","type":"text","text":"not ours"}`),
			},
			wantAction: "no session update",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := session.handleEvent(ctx, test.event); err != nil {
				t.Fatalf("foreign %s: %v", test.name, err)
			}
		})
	}

	if got := conn.permissionRequestCount(); got != 0 {
		t.Fatalf("foreign approval requested permission %d times", got)
	}
	if got := client.permissionReplyCount(); got != 0 {
		t.Fatalf("foreign approval answered the gateway %d times", got)
	}
	if got := len(conn.elicitations); got != 0 {
		t.Fatalf("foreign clarify elicited %d times", got)
	}
	if got := client.questionReplyCount(); got != 0 {
		t.Fatalf("foreign clarify answered the gateway %d times", got)
	}
	if got := conn.updateCount(); got != 0 {
		t.Fatalf("foreign part emitted %d session updates", got)
	}
	// A foreign part is refused before it is marked, so the same part id
	// arriving for this session is still new work.
	if !session.markPart(nativehermes.Part{ID: "part-foreign", SessionID: "native-1", MessageID: "assistant", Type: "text", Text: "not ours"}) {
		t.Fatal("a foreign part was recorded as this session's own")
	}
}

func testApprovalAndClarifyEventBranches(t *testing.T, ctx context.Context, session *session, client *fakeHermesClient, conn *recordingAgentClient) {
	t.Helper()

	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: "approval.request", Properties: json.RawMessage(`{`)}); err == nil {
		t.Fatal("malformed permission event succeeded")
	}
	conn.permission = acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")}
	permission := testHermesPermissionRequest(t, "p-session", "c1")
	permission.Metadata = map[string]any{"filepath": "acp-permission-probe.txt"}
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{
		Type:       evtApprovalRequest,
		Permission: &permission,
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
	question := testHermesQuestionRequest("q-v2")
	question.Questions = []nativehermes.QuestionInfo{{Question: "Continue?"}}
	if err := session.handleEvent(ctx, nativehermes.TurnEvent{Type: evtClarifyRequest, Question: &question}); err != nil {
		t.Fatalf("clarify.request event: %v", err)
	}
	questionReply := client.questionReply(client.questionReplyCount() - 1)
	if questionReply.requestID != "q-v2" {
		t.Fatalf("clarify.request reply = %#v", questionReply)
	}
}

// testEmptyAndMalformedEventMapperBranches covers the mappers' own refusals:
// an event body that decodes to nothing, and inputs that carry no content to
// map. Foreign-session refusal is a separate rule, pinned by
// TestForeignNativeSessionEventsAreRefused.
func testEmptyAndMalformedEventMapperBranches(t *testing.T, ctx context.Context, session *session) {
	t.Helper()

	if _, ok := eventPart(json.RawMessage(`{`)); ok {
		t.Fatal("malformed eventPart succeeded")
	}
	rawSession := testSession(t, newTestAgent(), newFakeHermesClient())
	rawSession.rawMessages = rawMessageConfig{enabled: true}
	if err := rawSession.emitRawHermesEvent(ctx, nativehermes.TurnEvent{Raw: json.RawMessage(`{"type":"x"}`)}); err == nil {
		t.Fatal("raw event without conn claimed success")
	}
	if rawSession.rawSeq != 0 {
		t.Fatalf("raw event without conn consumed sequence %d", rawSession.rawSeq)
	}
	if usageFromTokens(nativehermes.Tokens{}) != nil {
		t.Fatal("empty tokens produced usage")
	}
	var emptyResource acp.EmbeddedResourceResource
	if part, err := embeddedResourceHermesPart(emptyResource, &imagePromptBudget{}); err == nil || part != nil {
		t.Fatalf("empty embeddedResourceHermesPart = %#v err=%v", part, err)
	}
	if updates, delivered := partUpdates("assistant", nativehermes.Part{Type: "text"}); updates != nil || delivered != "" {
		t.Fatalf("empty text updates = %#v", updates)
	}
	if updates, delivered := partUpdates("assistant", nativehermes.Part{Type: "reasoning"}); updates != nil || delivered != "" {
		t.Fatalf("empty reasoning updates = %#v", updates)
	}
}

func TestPromptRemainingErrorBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("send error after cancelled state", func(t *testing.T) {
		client := newFakeHermesClient()
		session := testSession(t, newTestAgent(), client)
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
		session := testSession(t, newTestAgent(), client)
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
		session := testSession(t, agent, client)
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
		session = testSession(t, agent, client)
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
		session := testSession(t, agent, client)
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
		session := testSession(t, agent, client)
		turnCtx := session.beginTurn(t.Context(), "same-session-turn")
		defer session.finishTurn()
		if err := session.handleEvent(turnCtx, nativehermes.TurnEvent{
			Type:       "approval.request",
			Properties: json.RawMessage(`{"id":"p","sessionID":"native-1","tool":{"callID":"tool-p"}}`),
		}); err == nil {
			t.Fatal("metadata-only permission event was accepted")
		}

		agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
		if err := session.handleEvent(turnCtx, nativehermes.TurnEvent{
			Type:       "clarify.request",
			Properties: json.RawMessage(`{"id":"q","sessionID":"native-1"}`),
		}); err == nil {
			t.Fatal("metadata-only question event was accepted")
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

	if wantMsgSubstr != "" {
		require.Contains(t, data[jsonFieldMessage], wantMsgSubstr)
		nativeCause := errors.Unwrap(err)
		if nativeCause == nil || !strings.Contains(nativeCause.Error(), wantMsgSubstr) {
			t.Fatalf("native cause = %v, want substring %q", nativeCause, wantMsgSubstr)
		}
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
		require.NoError(t, os.WriteFile(filepath.Join(client.xdg.Root, fileStateDB), []byte("uncommitted failed native history"), 0o600))
		failure := nativehermes.NewProviderTurnFailure("hermes assistant error: model overloaded", 503, "overloaded")
		client.emitEvent(nativehermes.TurnEvent{
			Type: nativehermes.EventCycleFailed, CycleID: "fake/native-1/cycle-1",
			TransportGeneration: 1, Origin: nativehermes.CycleOriginPrompt, Err: failure,
		})

		return nativehermes.NativeMessage{}, failure
	}

	conn := newRecordingAgentClient()
	agent := newTestAgent()
	agent.setAgentClient(conn)
	session := testSession(t, agent, client)
	require.NoError(t, os.WriteFile(filepath.Join(client.xdg.Root, fileStateDB), []byte("committed native history"), 0o600))
	require.NoError(t, session.snapshotToStore(t.Context()))

	resp, err := promptOnce(context.Background(), session, "hello")
	if resp.StopReason != "" {
		t.Fatalf("failed turn returned a stop reason %q, want none", resp.StopReason)
	}

	data := requireTurnFailure(t, err, nativehermes.CauseProvider, "model overloaded")
	if data[jsonFieldStatusCode] != 503 {
		t.Fatalf("statusCode = %v, want 503", data[jsonFieldStatusCode])
	}

	require.Equal(t, "overloaded", data[jsonFieldProviderCode])
	require.Equal(t, 1, client.closeCount())
	require.True(t, session.needsRuntimeResume())
	replacement := newFakeHermesClient()
	replacement.getSession = testNativeSession("native-1")
	factoryCalls := 0
	agent.options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
		factoryCalls++
		bytes, readErr := os.ReadFile(filepath.Join(start.ExistingXDG.Root, fileStateDB))
		require.NoError(t, readErr)
		require.Equal(t, "committed native history", string(bytes))
		replacement.xdg = start.ExistingXDG

		return replacement, nil
	}
	response, retryErr := promptOnce(t.Context(), session, "next prompt")
	require.NoError(t, retryErr)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Equal(t, 1, factoryCalls)
	require.NoError(t, session.Close(t.Context()))
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
	session := testSession(t, agent, client)

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

	client.emitError(errors.New("stream died mid cancel"))

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
	agent := newTestAgent(WithScratchDir(durableTempDir(t)), WithSessionStore(store))
	agent.setAgentClient(conn)
	session := testSession(t, agent, client)
	session.cwd = durableTempDir(t)
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
	session := testSession(t, agent, client)

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

	session := testSession(t, newTestAgent(), client)
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
	session := testSession(t, agent, client)
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
	session := testSession(t, agent, client)
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
	client.closeErr = ErrContainmentIncomplete
	session := testSession(t, newTestAgent(), client)
	promptDone := make(chan error, 1)
	go func() {
		_, err := session.Prompt(context.Background(), TextPromptRequest(session.id, "unproven-fence", "hang"))
		promptDone <- err
	}()
	<-started

	cancelErr := session.cancelRouted(turnRouteMeta("unproven-fence"))
	if !errors.Is(cancelErr, ErrContainmentIncomplete) {
		t.Fatalf("Cancel error = %v, want process-tree proof failure", cancelErr)
	}
	if promptErr := <-promptDone; !errors.Is(promptErr, ErrContainmentIncomplete) {
		t.Fatalf("Prompt error = %v, want process-tree proof failure", promptErr)
	}
	// An incarnation whose process tree is still alive and un-containable is a
	// runtime this adapter cannot replace, which is its own token rather than an
	// ordinary poisoned session.
	if err := session.ensureNotPoisoned(); err == nil || !strings.Contains(err.Error(), valHermesRuntimeUnavailable) {
		t.Fatalf("poisoned session error = %v", err)
	}
	if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "after-unproven", "reply")); err == nil || !strings.Contains(err.Error(), valHermesRuntimeUnavailable) {
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
			session := testSession(t, newTestAgent(), client)
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
	t.Run("default timer fallback", func(t *testing.T) {
		agent := newTestAgent(WithTurnTimeout(time.Hour))
		agent.options.newPromptTimer = nil
		session := testSession(t, agent, newFakeHermesClient())
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
		session := testSession(t, agent, client)
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

	scratch := durableTempDir(t)
	agent := newTestAgent(WithScratchDir(scratch))
	session := testSession(t, agent, oldClient)
	session.env = map[string]string{"HERMES_REBIND_TEST": "preserved"}
	rebindPathDir := durableTempDir(t)
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
		if filepath.Dir(opts.ExistingXDG.Root) != agent.options.ScratchDir ||
			!strings.HasPrefix(filepath.Base(opts.ExistingXDG.Root), "acp-go-hermes-runtime-") {
			t.Fatalf("replacement XDG = %#v, scratch=%q", opts.ExistingXDG, agent.options.ScratchDir)
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
	agent := newTestAgent(WithScratchDir(durableTempDir(t)))
	session := testSession(t, agent, oldClient)
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
	session := testSession(t, agent, client)

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
	session := testSession(t, agent, client)

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
	session := testSession(t, agent, client)

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

	client.emitError(errors.New("read tcp: unexpected EOF from hermes serve"))
	select {
	case err := <-done:
		requireTurnFailure(t, err, nativehermes.CauseTransport, "unexpected EOF from hermes serve")
	case <-ctx.Done():
		t.Fatal("prompt did not fail")
	}
}

func TestFailedTurnResultGatewayDisconnectFencesRuntime(t *testing.T) {
	client := newFakeHermesClient()
	session := testSession(t, newTestAgent(), client)
	turnCtx := session.beginTurn(t.Context(), "gateway-disconnect")
	turnEpoch := session.currentTurnEpoch()
	defer session.finishTurn()

	nativeCause := errors.New("native websocket EOF")
	sendErr := fmt.Errorf("send frame: %w: %w", nativehermes.ErrGatewayDisconnected, nativeCause)
	_, err := session.failedTurnResult(turnCtx, turnEpoch, sendErr)
	requireTurnFailure(t, err, nativehermes.CauseTransport, "send frame")
	if !errors.Is(err, ErrGatewayDisconnected) || !errors.Is(err, nativeCause) {
		t.Fatalf("embeddable error lost gateway identity or native cause: %v", err)
	}
	if client.closeCount() != 1 || !session.needsRuntimeResume() {
		t.Fatalf("gateway disconnect close=%d needsResume=%v", client.closeCount(), session.needsRuntimeResume())
	}
}

func TestFailedTurnResultReturnsFenceFailure(t *testing.T) {
	wantErr := errors.New("close proof failed")
	client := newFakeHermesClient()
	client.closeErr = wantErr
	session := testSession(t, newTestAgent(), client)
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
	session := testSession(t, agent, client)

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
			jsonFieldField: acpFieldPromptResource,
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
			jsonFieldField: acpFieldPromptResource,
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
			jsonFieldField: acpFieldPromptImage,
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
				jsonFieldField: acpFieldPromptResource,
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

func TestPermissionAndQuestionCarryLifecycleActions(t *testing.T) {
	agent := newTestAgent()
	require.NoError(t, agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	}))
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	session := testSession(t, agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	turnCtx := beginTestControlTurn(t, session, t.Context(), "turn")
	require.NoError(t, session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}))

	require.NoError(t, session.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool")))
	require.NoError(t, session.handleQuestion(turnCtx, testHermesQuestionRequest("question")))

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
		session, _, _ := newLifecycleActionSession(t, true)
		err := session.handlePermission(t.Context(), testHermesPermissionRequest(t, "permission", "tool"))
		require.ErrorContains(t, err, "outside its active turn")
	})

	t.Run("question without owner", func(t *testing.T) {
		session, _, _ := newLifecycleActionSession(t, true)
		err := session.handleQuestion(t.Context(), testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "outside its owning cycle")
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
		err := session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})
}

func TestLifecycleActionPublicationFailureCancelsAndJoinsHostRequest(t *testing.T) {
	for _, test := range []struct {
		name       string
		permission bool
	}{
		{name: "permission", permission: true},
		{name: "elicitation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, base, turnCtx := newLifecycleActionSession(t, true)
			client := &publicationFailControlClient{
				recordingAgentClient: base,
				started:              make(chan struct{}),
				cancelled:            make(chan struct{}),
				release:              make(chan struct{}),
			}
			s.agent.setAgentClient(client)
			done := make(chan error, 1)
			if test.permission {
				go func() {
					done <- s.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool"))
				}()
			} else {
				go func() { done <- s.handleQuestion(turnCtx, testHermesQuestionRequest("question")) }()
			}

			<-client.started
			<-client.cancelled
			select {
			case err := <-done:
				t.Fatalf("handler returned while its host request was live: %v", err)
			default:
			}
			close(client.release)
			require.ErrorContains(t, <-done, "lifecycle delivery failed")
			native, ok := s.client.(*fakeHermesClient)
			require.True(t, ok)
			if test.permission {
				require.Equal(t, 1, native.permissionReplyCount())
			} else {
				require.Equal(t, 1, native.questionRejectCount())
			}
		})
	}
}

func TestLifecycleResolutionFailureDominatesCallbackResult(t *testing.T) {
	runPermission := func(t *testing.T, callbackErr error) {
		t.Helper()
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		pending := lifecyclePendingActionBarrier(session, conn)
		conn.permErr = callbackErr
		conn.permissionStarted = make(chan struct{}, 1)
		conn.permissionRelease = make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- session.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool"))
		}()
		<-conn.permissionStarted
		<-pending
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
		pending := lifecyclePendingActionBarrier(session, conn)
		conn.elicitation = response
		conn.elicitErr = callbackErr
		conn.elicitationStarted = make(chan struct{}, 1)
		conn.elicitationRelease = make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- session.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		}()
		<-conn.elicitationStarted
		<-pending
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

func lifecyclePendingActionBarrier(session *session, conn *recordingAgentClient) <-chan struct{} {
	pending := make(chan struct{}, 1)
	hook := &sessionUpdateHookClient{recordingAgentClient: conn}
	hook.afterUpdate = func() {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		if len(conn.updates) == 0 {
			return
		}
		envelope, _ := conn.updates[len(conn.updates)-1].Meta[lifecycle.MetaKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["state"] == string(lifecycle.ForegroundRequiresAction) {
			select {
			case pending <- struct{}{}:
			default:
			}
		}
	}
	session.agent.setAgentClient(hook)

	return pending
}

func failLifecycleDelivery(t *testing.T, session *session) {
	t.Helper()
	hook, ok := session.agent.connection().(*sessionUpdateHookClient)
	require.True(t, ok)
	setLifecycleDeliveryError(hook.recordingAgentClient)
}

func TestLifecycleControlHardCutFailureBranches(t *testing.T) {
	t.Run("unowned permission", func(t *testing.T) {
		s, _, turnCtx := newLifecycleActionSession(t, false)
		stream := s.lifecycleStream()
		stream.mu.Lock()
		stream.turnID = ""
		stream.mu.Unlock()
		err := s.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool"))
		require.ErrorContains(t, err, "accepted lifecycle turn")
	})
	t.Run("unowned question", func(t *testing.T) {
		s, _, turnCtx := newLifecycleActionSession(t, false)
		stream := s.lifecycleStream()
		stream.mu.Lock()
		stream.turnID = ""
		stream.mu.Unlock()
		err := s.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.NoError(t, err)
	})

	t.Run("permission operation admission", func(t *testing.T) {
		s, _, turnCtx := newLifecycleActionSession(t, true)
		for index := 0; index < cap(s.agent.clientCalls); index++ {
			s.agent.clientCalls <- struct{}{}
		}
		err := s.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool"))
		for index := 0; index < cap(s.agent.clientCalls); index++ {
			<-s.agent.clientCalls
		}
		require.ErrorContains(t, err, valBackpressure)
	})
	t.Run("question operation admission", func(t *testing.T) {
		s, _, turnCtx := newLifecycleActionSession(t, true)
		for index := 0; index < cap(s.agent.clientCalls); index++ {
			s.agent.clientCalls <- struct{}{}
		}
		err := s.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		for index := 0; index < cap(s.agent.clientCalls); index++ {
			<-s.agent.clientCalls
		}
		require.ErrorContains(t, err, valBackpressure)
	})

	t.Run("permission host write", func(t *testing.T) {
		s, conn, turnCtx := newLifecycleActionSession(t, true)
		conn.permissionWriteErr = errors.New("permission write failed")
		err := s.handlePermission(turnCtx, testHermesPermissionRequest(t, "permission", "tool"))
		require.ErrorContains(t, err, "permission write failed")
	})
	t.Run("question host write", func(t *testing.T) {
		s, conn, turnCtx := newLifecycleActionSession(t, true)
		conn.elicitationWriteErr = errors.New("question write failed")
		err := s.handleQuestion(turnCtx, testHermesQuestionRequest("question"))
		require.ErrorContains(t, err, "question write failed")
	})

	runPermissionAfterPending := func(t *testing.T, callbackErr error, beforeReturn func(*session, context.CancelFunc)) error {
		t.Helper()
		s, conn, turnCtx := newLifecycleActionSession(t, true)
		pending := lifecyclePendingActionBarrier(s, conn)
		ctx, cancel := context.WithCancel(turnCtx)
		defer cancel()
		conn.permissionStarted = make(chan struct{}, 1)
		conn.permissionRelease = make(chan struct{})
		conn.permErr = callbackErr
		conn.permissionBeforeReturn = func() { beforeReturn(s, cancel) }
		done := make(chan error, 1)
		go func() {
			done <- s.handlePermission(ctx, testHermesPermissionRequest(t, "permission", "tool"))
		}()
		<-conn.permissionStarted
		<-pending
		close(conn.permissionRelease)

		return <-done
	}
	t.Run("permission missing after result", func(t *testing.T) {
		err := runPermissionAfterPending(t, nil, func(s *session, _ context.CancelFunc) {
			_, _, _ = s.takePendingPermission("permission")
		})
		require.ErrorIs(t, err, errPromptCancelled)
	})
	t.Run("permission cancelled after result", func(t *testing.T) {
		err := runPermissionAfterPending(t, nil, func(_ *session, cancel context.CancelFunc) { cancel() })
		require.ErrorIs(t, err, errPromptCancelled)
	})
	t.Run("permission callback terminalization", func(t *testing.T) {
		err := runPermissionAfterPending(t, errors.New("callback failed"), func(s *session, _ context.CancelFunc) {
			failLifecycleDelivery(t, s)
		})
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})
	t.Run("permission answer terminalization", func(t *testing.T) {
		err := runPermissionAfterPending(t, nil, func(s *session, _ context.CancelFunc) {
			failLifecycleDelivery(t, s)
		})
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})

	runQuestionAfterPending := func(
		t *testing.T,
		response acp.UnstableCreateElicitationResponse,
		callbackErr error,
		beforeReturn func(*session, context.CancelFunc),
	) error {
		t.Helper()
		s, conn, turnCtx := newLifecycleActionSession(t, true)
		pending := lifecyclePendingActionBarrier(s, conn)
		ctx, cancel := context.WithCancel(turnCtx)
		defer cancel()
		conn.elicitation = response
		conn.elicitErr = callbackErr
		conn.elicitationStarted = make(chan struct{}, 1)
		conn.elicitationRelease = make(chan struct{})
		conn.elicitationBeforeReturn = func() { beforeReturn(s, cancel) }
		done := make(chan error, 1)
		go func() { done <- s.handleQuestion(ctx, testHermesQuestionRequest("question")) }()
		<-conn.elicitationStarted
		<-pending
		close(conn.elicitationRelease)

		return <-done
	}
	t.Run("question missing after result", func(t *testing.T) {
		err := runQuestionAfterPending(t, acp.NewUnstableCreateElicitationResponseDecline(), nil,
			func(s *session, _ context.CancelFunc) { _, _, _ = s.takePendingQuestion("question") })
		require.ErrorIs(t, err, errPromptCancelled)
	})
	t.Run("question cancelled after result", func(t *testing.T) {
		err := runQuestionAfterPending(t, acp.NewUnstableCreateElicitationResponseDecline(), nil,
			func(_ *session, cancel context.CancelFunc) { cancel() })
		require.ErrorIs(t, err, errPromptCancelled)
	})
	setDeliveryError := func(s *session, _ context.CancelFunc) {
		failLifecycleDelivery(t, s)
	}
	t.Run("question callback terminalization", func(t *testing.T) {
		err := runQuestionAfterPending(t, acp.UnstableCreateElicitationResponse{}, errors.New("callback failed"), setDeliveryError)
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})
	t.Run("question decline terminalization", func(t *testing.T) {
		err := runQuestionAfterPending(t, acp.NewUnstableCreateElicitationResponseDecline(), nil, setDeliveryError)
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})
	t.Run("question answer terminalization", func(t *testing.T) {
		response := acp.UnstableCreateElicitationResponse{
			Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{}},
		}
		err := runQuestionAfterPending(t, response, nil, setDeliveryError)
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})
}

func TestLifecycleCorrelationAndCancelAreValidatedBeforeDispatch(t *testing.T) {
	reserved := map[string]any{lifecycle.MetaKey: map[string]any{}}
	session := testSession(t, newTestAgent(), newFakeHermesClient())
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
	require.NoError(t, agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	}))
	negotiatedSession := testSession(t, agent, newFakeHermesClient())
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

	session := testSession(t, newTestAgent(), client)
	resp, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "unknown-finish", "reply"))
	require.Error(t, err)
	require.Empty(t, resp.StopReason, "unmapped finish named an ACP v1 stop reason")

	var reqErr *acp.RequestError

	require.ErrorAs(t, err, &reqErr)

	data, _ := reqErr.Data.(map[string]any)
	require.Equal(t, valHermesTurnFailed, data[jsonFieldError])
	require.Equal(t, string(nativehermes.CauseProvider), data[jsonFieldCause])
	require.Contains(t, data[jsonFieldMessage], "tool_calls")
	require.ErrorContains(t, errors.Unwrap(err), "tool_calls")
}
func TestActionOwnershipEdges(t *testing.T) {
	s := testSession(t, newTestAgent(), newFakeHermesClient())
	defer s.stopPump()

	require.NoError(t, s.resolveBlockingAction(t.Context(), announcedAction{}, lifecycle.ActionCancelled))
	err := s.resolveBlockingAction(t.Context(), announcedAction{id: "outside"}, lifecycle.ActionCancelled)
	require.Error(t, err)

	turnCtx := beginTestControlTurn(t, s, t.Context(), "turn")
	require.NoError(t, s.resolveBlockingAction(turnCtx, announcedAction{id: "already-resolved"}, lifecycle.ActionCancelled))

	route, active := s.permissionTurnRoute(turnCtx)
	require.True(t, active)
	action := lifecycle.BlockingAction("action", lifecycle.ActionPermission, "")
	s.actionRequests = nil
	s.registerActionRequest(action, "request", route)
	require.Contains(t, s.actionRequests, action.ActionID)

	wrong := s.actionRequests[action.ActionID]
	wrong.nonce = "wrong"
	s.actionRequests[action.ActionID] = wrong
	taken, current := s.takeActionRequest(turnCtx, action.ActionID)
	require.False(t, taken)
	require.False(t, current)

	s.registerActionRequest(action, "request", route)
	taken, current = s.takeActionRequest(turnCtx, action.ActionID)
	require.True(t, taken)
	require.True(t, current)
	taken, current = s.takeActionRequest(turnCtx, action.ActionID)
	require.False(t, taken)
	require.True(t, current)

	cancelled, cancel := context.WithCancel(turnCtx)
	cancel()
	taken, current = s.takeActionRequest(cancelled, "action")
	require.False(t, taken)
	require.False(t, current)

	s.pending = nil
	permission := nativehermes.PermissionRequest{ID: "permission-after-reset", SessionID: "native-1"}
	s.addPendingPermission(permission)
	pending, found, _ := s.takePendingPermission(permission.ID)
	require.True(t, found)
	require.Equal(t, permission.ID, pending.ID)

	mapped := &mappedWireError{wire: acp.NewInternalError(nil), cause: errors.New("cause")}
	var ordinary error
	require.False(t, mapped.As(&ordinary))
	require.False(t, s.permissionTurnRouteCurrent(t.Context(), permissionTurnRoute{}))
	require.Error(t, s.emitToolPartUpdate(t.Context(), nativehermes.Part{}))
	require.Equal(t, "Hermes permission", permissionHermesToolState(nativehermes.PermissionRequest{}).title)
}
func activeQuestionSession(
	t *testing.T,
	response acp.UnstableCreateElicitationResponse,
	callbackErr error,
) (*session, *fakeHermesClient, *recordingAgentClient, context.Context, context.CancelFunc) {
	t.Helper()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	conn.elicitation = response
	conn.elicitErr = callbackErr
	agent := newTestAgent()
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	agent.setAgentClient(conn)
	s := testSession(t, agent, client)
	t.Cleanup(s.stopPump)
	ctx, cancel := context.WithCancel(t.Context())
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: "question-cycle",
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	require.NoError(t, s.synchronizePump(ctx))
	route := s.routeForEvent(nativehermes.TurnEvent{CycleID: "question-cycle", TransportGeneration: 1})
	require.NotNil(t, route)
	turnCtx := withPumpRoute(ctx, route)

	return s, client, conn, turnCtx, cancel
}

func questionRequest(id string) nativehermes.QuestionRequest {
	return nativehermes.QuestionRequest{
		ID: id, SessionID: "native-1",
		CycleID: "question-cycle", TransportGeneration: 1,
		Questions: []nativehermes.QuestionInfo{{Question: "Continue?"}},
	}
}

func TestQuestionActiveRouteBranches(t *testing.T) {
	t.Run("no connection", func(t *testing.T) {
		client := newFakeHermesClient()
		s := testSession(t, newTestAgent(), client)
		defer s.stopPump()
		turnCtx := beginTestControlTurn(t, s, t.Context(), "turn")
		req := questionRequest("no-connection")
		req.CycleID = testControlCycleID
		require.NoError(t, s.handleQuestion(turnCtx, req))
		require.Equal(t, 1, client.questionRejectCount())
	})

	t.Run("stale identity", func(t *testing.T) {
		s, client, _, turnCtx, _ := activeQuestionSession(t, acp.UnstableCreateElicitationResponse{}, nil)
		route, active := s.permissionTurnRoute(turnCtx)
		require.True(t, active)
		req := questionRequest("stale")
		req.CycleID = route.cycleID + "other"
		require.Error(t, s.handleQuestion(turnCtx, req))
		require.Equal(t, 1, client.questionRejectCount())
	})

	for _, rejectFails := range []bool{false, true} {
		name := "callback error"
		if rejectFails {
			name += " and reject error"
		}
		t.Run(name, func(t *testing.T) {
			callbackErr := errors.New("elicitation failed")
			s, client, _, turnCtx, _ := activeQuestionSession(t, acp.UnstableCreateElicitationResponse{}, callbackErr)
			if rejectFails {
				client.replyErr = errors.New("reject failed")
			}
			err := s.handleQuestion(turnCtx, questionRequest("callback-error"))
			require.ErrorIs(t, err, callbackErr)
			if rejectFails {
				require.ErrorContains(t, err, "reject failed")
			}
		})
	}

	for _, rejectFails := range []bool{false, true} {
		name := "decline"
		if rejectFails {
			name += " reject error"
		}
		t.Run(name, func(t *testing.T) {
			s, client, _, turnCtx, _ := activeQuestionSession(t, acp.NewUnstableCreateElicitationResponseDecline(), nil)
			if rejectFails {
				client.replyErr = errors.New("reject failed")
			}
			err := s.handleQuestion(turnCtx, questionRequest("decline"))
			if rejectFails {
				require.ErrorContains(t, err, "reject failed")
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("accepted", func(t *testing.T) {
		response := acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{
			Action: "accept", Content: map[string]any{"question_1": "yes"},
		}}
		s, client, _, turnCtx, _ := activeQuestionSession(t, response, nil)
		require.NoError(t, s.handleQuestion(turnCtx, questionRequest("accepted")))
		require.Equal(t, 1, client.questionReplyCount())
	})
}

func TestQuestionLateContextCancellationBranches(t *testing.T) {
	responses := []struct {
		name     string
		response acp.UnstableCreateElicitationResponse
		callErr  error
	}{
		{name: "callback error", callErr: errors.New("elicitation failed")},
		{name: "decline", response: acp.NewUnstableCreateElicitationResponseDecline()},
		{name: "accept", response: acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{
			Action: "accept", Content: map[string]any{"question_1": "yes"},
		}}},
	}
	for _, test := range responses {
		for _, rejectFails := range []bool{false, true} {
			name := test.name
			if rejectFails {
				name += " reject error"
			}
			t.Run(name, func(t *testing.T) {
				s, client, conn, turnCtx, cancel := activeQuestionSession(t, test.response, test.callErr)
				conn.elicitationStarted = make(chan struct{}, 1)
				conn.elicitationRelease = make(chan struct{})
				conn.elicitationIgnoreContext = true
				if rejectFails {
					client.replyErr = errors.New("reject failed")
				}
				done := make(chan error, 1)
				go func() { done <- s.handleQuestion(turnCtx, questionRequest("late")) }()
				<-conn.elicitationStarted
				cancel()
				close(conn.elicitationRelease)
				err := <-done
				if rejectFails && test.name != "callback error" {
					require.ErrorContains(t, err, "reject failed")
				} else {
					require.ErrorIs(t, err, errPromptCancelled)
				}
				require.Equal(t, 1, client.questionRejectCount())
			})
		}
	}
}

func TestPermissionStaleIdentityIsRejected(t *testing.T) {
	s := testSession(t, newTestAgent(), newFakeHermesClient())
	defer s.stopPump()
	route := &pumpCycleRoute{
		incarnation: s.pumpIncarnation, generation: 1, cycleID: "permission-cycle",
		nonce: "turn", prompt: true, projected: make(chan error, 1),
	}
	s.pumpMu.Lock()
	s.pumpRoutes[route.cycleID] = route
	s.pumpMu.Unlock()
	turnCtx := withPumpRoute(t.Context(), route)
	req := testHermesPermissionRequest(t, "permission", "tool")
	req.TransportGeneration = 1
	req.CycleID = "stale"
	require.Error(t, s.handlePermission(turnCtx, req))
}

type orderedControlACPClient struct {
	*pipeACPClient

	events            chan string
	wireEvents        chan string
	permissionRelease chan struct{}
	questionRelease   chan struct{}

	mu         sync.Mutex
	permission acp.RequestPermissionRequest
	question   acp.UnstableCreateElicitationRequest
}

func newOrderedControlACPClient() *orderedControlACPClient {
	return &orderedControlACPClient{
		pipeACPClient:     &pipeACPClient{},
		events:            make(chan string, 8),
		wireEvents:        make(chan string, 8),
		permissionRelease: make(chan struct{}),
		questionRelease:   make(chan struct{}),
	}
}

func (c *orderedControlACPClient) RequestPermission(
	_ context.Context,
	request acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.permission = request
	c.mu.Unlock()
	c.events <- "permission"
	<-c.permissionRelease

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(valOnce)}, nil
}

func (c *orderedControlACPClient) UnstableCreateElicitation(
	_ context.Context,
	request acp.UnstableCreateElicitationRequest,
) (acp.UnstableCreateElicitationResponse, error) {
	c.mu.Lock()
	c.question = request
	c.mu.Unlock()
	c.events <- "question"
	<-c.questionRelease

	return acp.UnstableCreateElicitationResponse{
		Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{"question_1": "yes"}},
	}, nil
}

func (c *orderedControlACPClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	if pendingActionNotification(notification) {
		c.events <- "action"
	}

	return nil
}

func pendingActionNotification(notification acp.SessionNotification) bool {
	envelope, _ := notification.Meta[lifecycle.MetaKey].(map[string]any)
	event, _ := envelope["event"].(map[string]any)
	action, _ := event["action"].(map[string]any)

	return event["type"] == string(lifecycle.EventActionUpdate) && action["state"] == string(lifecycle.ActionPending)
}

type orderedControlWireWriter struct {
	target io.Writer
	events chan<- string
}

func (w orderedControlWireWriter) Write(frame []byte) (int, error) {
	written, err := w.target.Write(frame)
	if err != nil || written != len(frame) {
		return written, err
	}

	var envelope struct {
		Method string                  `json:"method"`
		Params acp.SessionNotification `json:"params"`
	}
	_ = json.Unmarshal(frame, &envelope)

	switch envelope.Method {
	case acp.ClientMethodSessionRequestPermission:
		w.events <- "permission"
	case acp.ClientMethodElicitationCreate:
		w.events <- "question"
	case acp.ClientMethodSessionUpdate:
		if pendingActionNotification(envelope.Params) {
			w.events <- "action"
		}
	}

	return written, nil
}

func newOrderedControlSession(t *testing.T) (*session, *fakeHermesClient, *orderedControlACPClient, context.Context) {
	t.Helper()
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	t.Cleanup(func() {
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()
	})

	host := newOrderedControlACPClient()
	_ = acp.NewClientSideConnection(host, c2aW, a2cR)
	agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 2}))
	require.NoError(t, agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	}))
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	connection := newLocalAgentConnection(agent, orderedControlWireWriter{target: a2cW, events: host.wireEvents}, c2aR)
	agent.setAgentClient(connection)
	native := newFakeHermesClient()
	s := testSession(t, agent, native)
	t.Cleanup(s.stopPump)
	require.NoError(t, s.openLifecycleStream())
	turnCtx := beginTestControlTurn(t, s, t.Context(), "wire-control")

	return s, native, host, turnCtx
}

func TestHostControlWritePrecedesPendingActionWithExactIdentity(t *testing.T) {
	t.Run("permission", func(t *testing.T) {
		s, _, host, turnCtx := newOrderedControlSession(t)
		req := testHermesPermissionRequest(t, "native-permission-id", "native-tool-id")
		done := make(chan error, 1)
		go func() { done <- s.handlePermission(turnCtx, req) }()

		require.Equal(t, "permission", <-host.wireEvents)
		require.Equal(t, "action", <-host.wireEvents)
		for event := range host.events {
			if event == "permission" {
				break
			}
		}
		host.mu.Lock()
		requestID := controlRequestID(host.permission.Meta)
		host.mu.Unlock()
		require.Equal(t, req.ID, requestID)
		close(host.permissionRelease)
		require.NoError(t, <-done)
	})

	t.Run("question", func(t *testing.T) {
		s, _, host, turnCtx := newOrderedControlSession(t)
		req := testHermesQuestionRequest("native-question-id")
		done := make(chan error, 1)
		go func() { done <- s.handleQuestion(turnCtx, req) }()

		require.Equal(t, "question", <-host.wireEvents)
		require.Equal(t, "action", <-host.wireEvents)
		for event := range host.events {
			if event == "question" {
				break
			}
		}
		host.mu.Lock()
		route, ok := host.question.Form.Meta[routeMetaKey].(map[string]any)
		host.mu.Unlock()
		require.True(t, ok)
		require.Equal(t, req.ID, route[routeFieldReq])
		close(host.questionRelease)
		require.NoError(t, <-done)
	})
}

func TestHostControlCancellationAfterWriteSuppressesPendingAction(t *testing.T) {
	for _, test := range []struct {
		name       string
		close      bool
		permission bool
	}{
		{name: "permission cancel", permission: true},
		{name: "question close", close: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, native, host, turnCtx := newOrderedControlSession(t)
			atWrite := make(chan struct{})
			releaseWrite := make(chan struct{})
			s.afterHostControlWrite = func() {
				close(atWrite)
				<-releaseWrite
			}
			done := make(chan error, 1)
			if test.permission {
				req := testHermesPermissionRequest(t, "cancel-permission-id", "cancel-tool-id")
				go func() { done <- s.handlePermission(turnCtx, req) }()
				require.Equal(t, "permission", <-host.events)
			} else {
				req := testHermesQuestionRequest("close-question-id")
				go func() { done <- s.handleQuestion(turnCtx, req) }()
				require.Equal(t, "question", <-host.events)
			}
			<-atWrite
			if test.close {
				s.prepareClose()
				s.pumpMu.Lock()
				stopping := s.pumpStopping
				s.pumpMu.Unlock()
				require.True(t, stopping, "close drained a control before establishing pump shutdown")
			} else {
				s.cancelTurn()
			}
			close(releaseWrite)
			if test.permission {
				close(host.permissionRelease)
				require.Equal(t, 1, native.permissionReplyCount())
			} else {
				close(host.questionRelease)
				require.Equal(t, 1, native.questionRejectCount())
			}
			require.ErrorIs(t, <-done, errPromptCancelled)
			if test.close {
				s.mu.Lock()
				resumeNeeded := s.runtimeNeedsResume
				s.mu.Unlock()
				require.False(t, resumeNeeded)
				require.False(t, s.lifecycleStream().fenced(), "close-woken control fenced the stream")
			}
			select {
			case event := <-host.events:
				require.NotEqual(t, "action", event)
			default:
			}
		})
	}
}

func TestPermissionRouteStaleAfterWriteRejectsOwnedPendingRequest(t *testing.T) {
	s, conn, turnCtx := newLifecycleActionSession(t, true)
	defer s.stopPump()
	s.afterHostControlWrite = func() {
		s.pumpMu.Lock()
		s.pumpIncarnation++
		s.pumpMu.Unlock()
	}

	err := s.handlePermission(turnCtx, testHermesPermissionRequest(t, "stale-permission", "stale-tool"))
	require.ErrorIs(t, err, errPromptCancelled)
	native, ok := s.client.(*fakeHermesClient)
	require.True(t, ok)
	require.Equal(t, 1, native.permissionReplyCount())
	require.Empty(t, s.actionRequests)
	require.Len(t, conn.permissions, 1)
}

func runProjectionPrompt(t *testing.T, s *session) promptRun {
	t.Helper()
	release, settlement, err := s.acquireTurn(t.Context())
	require.NoError(t, err)
	defer settlement.complete()
	defer release()
	turnCtx := s.beginTurn(t.Context(), "projection-turn")
	t.Cleanup(s.finishTurn)
	epoch := sessionTurnEpoch(s)

	return s.runPromptTurn(t.Context(), turnCtx, epoch, lifecycle.Submission{}, nativehermes.MessageRequest{}, nil)
}

func TestPromptProjectionRegistrationFailure(t *testing.T) {
	client := newFakeHermesClient()
	s := testSession(t, newTestAgent(), client)
	defer s.stopPump()
	want := errors.New("projection registry failed")
	client.beforePromptDispatch = func() {
		s.pumpMu.Lock()
		s.pumpErr = want
		s.pumpMu.Unlock()
	}
	run := runProjectionPrompt(t, s)
	require.ErrorIs(t, run.err, want)
}

func TestPromptDispatchPromotionRefusals(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*session) *turnSettlement
		want   string
	}{
		{
			name: "lost reservation",
			mutate: func(s *session) *turnSettlement {
				s.promptReservation = nil

				return nil
			},
			want: "prompt reservation is no longer current",
		},
		{
			name: "closing session",
			mutate: func(s *session) *turnSettlement {
				s.lifecycleClosing = true

				return nil
			},
			want: valSessionClosed,
		},
		{
			name: "autonomous foreground won",
			mutate: func(s *session) *turnSettlement {
				return s.claimForegroundLocked(foregroundAutonomous)
			},
			want: valBackpressure,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeHermesClient()
			s := testSession(t, newTestAgent(), client)
			defer s.stopPump()
			var blocker *turnSettlement
			client.beforePromptDispatch = func() {
				s.mu.Lock()
				blocker = test.mutate(s)
				s.mu.Unlock()
			}

			run := runProjectionPrompt(t, s)
			blocker.complete()
			require.ErrorContains(t, run.err, test.want)
		})
	}
}

func TestPromptSynchronizationFailureAbortsBeforeDispatch(t *testing.T) {
	client := newFakeHermesClient()
	s := testSession(t, newTestAgent(), client)
	defer s.stopPump()
	want := errors.New("pump synchronization failed")
	s.pumpMu.Lock()
	s.pumpErr = want
	s.pumpMu.Unlock()

	run := runProjectionPrompt(t, s)
	require.ErrorIs(t, run.err, want)
	require.Equal(t, 1, client.abortCount())
}

func TestPromptLifecycleAcceptanceFailureResolvesProjection(t *testing.T) {
	agent := newTestAgent()
	require.NoError(t, agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation()))
	base := newRecordingAgentClient()
	agent.setAgentClient(&lifecycleFailingAgentClient{recordingAgentClient: base, failAt: 2})
	client := newFakeHermesClient()
	s := testSession(t, agent, client)
	defer s.stopPump()
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
	run := runProjectionPrompt(t, s)
	require.Error(t, run.err)
}

func TestPromptProjectionArrivesBeforeNativeResult(t *testing.T) {
	client := newFakeHermesClient()
	s := testSession(t, newTestAgent(), client)
	defer s.stopPump()
	projectionAcknowledged := make(chan struct{})
	s.afterProjectionAck = func() { close(projectionAcknowledged) }
	client.afterPromptTerminal = func() {
		_ = s.synchronizePump(t.Context())
		<-projectionAcknowledged
	}
	run := runProjectionPrompt(t, s)
	require.NoError(t, run.err)
}

func TestPromptProjectionFailureAfterNativeResult(t *testing.T) {
	client := newFakeHermesClient()
	client.omitPromptTerminal = true
	returned := make(chan struct{})
	client.afterPromptTerminal = func() { close(returned) }
	s := testSession(t, newTestAgent(), client)
	defer s.stopPump()
	release, settlement, err := s.acquireTurn(t.Context())
	require.NoError(t, err)
	defer settlement.complete()
	defer release()
	turnCtx := s.beginTurn(t.Context(), "projection-turn")
	defer s.finishTurn()
	epoch := sessionTurnEpoch(s)
	done := make(chan promptRun, 1)
	go func() {
		done <- s.runPromptTurn(t.Context(), turnCtx, epoch, lifecycle.Submission{}, nativehermes.MessageRequest{}, nil)
	}()
	<-returned
	want := errors.New("projection failed")
	projected := make(chan error, 1)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleFailed, TransportGeneration: 1,
		CycleID: "fake/native-1/cycle-1", Origin: nativehermes.CycleOriginPrompt, Err: want,
		ProjectionDone: func(err error) { projected <- err },
	})
	require.NoError(t, <-projected)
	run := <-done
	require.ErrorIs(t, run.err, want)
}

func TestPromptCancellationWhileAwaitingProjection(t *testing.T) {
	client := newFakeHermesClient()
	client.omitPromptTerminal = true
	returned := make(chan struct{})
	client.afterPromptTerminal = func() { close(returned) }
	s := testSession(t, newTestAgent(), client)
	defer s.stopPump()
	release, settlement, err := s.acquireTurn(t.Context())
	require.NoError(t, err)
	defer settlement.complete()
	defer release()
	turnCtx := s.beginTurn(t.Context(), "projection-turn")
	defer s.finishTurn()
	done := make(chan promptRun, 1)
	go func() {
		done <- s.runPromptTurn(t.Context(), turnCtx, sessionTurnEpoch(s), lifecycle.Submission{}, nativehermes.MessageRequest{}, nil)
	}()
	<-returned
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	cancel()
	run := <-done
	require.True(t, run.cancelled)
}

func TestNativeResultConsumesItsAlreadySentDispatchBeforeSettlement(t *testing.T) {
	results := make(chan promptDispatchResult, 1)
	results <- promptDispatchResult{registered: true}
	var observed promptDispatchResult
	consumePromptDispatchBeforeResult(true, false, results, func(result promptDispatchResult) { observed = result })
	require.True(t, observed.registered)
}

func TestAwaitPromptProjectionReturnsCancellation(t *testing.T) {
	session := testSession(t, newTestAgent(), newFakeHermesClient())
	success := make(chan error, 1)
	success <- nil
	require.Nil(t, session.awaitPromptProjection(t.Context(), success))
	want := errors.New("projection failed")
	failure := make(chan error, 1)
	failure <- want
	require.ErrorIs(t, session.awaitPromptProjection(t.Context(), failure).err, want)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	run := session.awaitPromptProjection(ctx, make(chan error))
	require.NotNil(t, run)
	require.True(t, run.cancelled)
}

func TestPromptSynchronizeCancellationIsUnaccepted(t *testing.T) {
	client := newFakeHermesClient()
	s := testSession(t, newTestAgent(), client)
	defer s.stopPump()
	s.pumpMu.Lock()
	s.pumpErr = errPromptCancelled
	s.pumpMu.Unlock()
	run := runProjectionPrompt(t, s)
	require.Equal(t, acp.StopReasonCancelled, run.response.StopReason)
}

// TestAssistantTextIsAppendOnlyAcrossNativeFixtures is the append-only
// conformance battery. Each case replays one native fixture through the mapper
// and asserts what a client rendering the turn as the in-order concatenation of
// its chunks actually sees: a terminal frame that repeats the streamed text
// contributes only its unstreamed suffix, a harness that streams no deltas
// yields exactly one chunk, and a turn carrying several native messages yields
// each message's text once under its own native identity.
func TestAssistantTextIsAppendOnlyAcrossNativeFixtures(t *testing.T) {
	textPart := func(messageID string, text string, streamed string) nativehermes.Part {
		return nativehermes.Part{
			SessionID: "native-1", MessageID: messageID, Type: valText,
			Text: text, StreamedText: streamed,
		}
	}

	for _, test := range []struct {
		name     string
		fixture  []nativehermes.Part
		want     string
		chunks   int
		messages []string
	}{
		{
			name: "terminal frame repeats the streamed text",
			fixture: []nativehermes.Part{
				textPart("message-1", "The answer ", ""),
				textPart("message-1", "The answer is 42.", "The answer "),
				// The harness's terminal full-message frame restates the whole
				// assembled text; the deltas already carried all of it.
				textPart("message-1", "The answer is 42.", "The answer is 42."),
			},
			want: "The answer is 42.", chunks: 2,
			messages: []string{"message-1", "message-1"},
		},
		{
			name:    "deltas-free harness",
			fixture: []nativehermes.Part{textPart("message-1", "The answer is 42.", "")},
			want:    "The answer is 42.", chunks: 1,
			messages: []string{"message-1"},
		},
		{
			name: "several native messages in one turn",
			fixture: []nativehermes.Part{
				textPart("message-1", "first", ""),
				textPart("message-2", "second", ""),
				// A restated native identity carrying text already streamed for
				// that identity adds nothing.
				textPart("message-2", "second", "second"),
			},
			want: "firstsecond", chunks: 2,
			messages: []string{"message-1", "message-2"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := newRecordingAgentClient()
			agent := newTestAgent()
			agent.setAgentClient(conn)
			session := testSession(t, agent, newFakeHermesClient())
			t.Cleanup(session.stopPump)

			for _, part := range test.fixture {
				require.NoError(t, session.emitPartUpdates(t.Context(), valAssistant, part))
			}

			conn.mu.Lock()
			updates := append([]acp.SessionNotification(nil), conn.updates...)
			conn.mu.Unlock()

			var (
				rendered string
				chunks   int
				messages []string
			)

			for _, update := range updates {
				chunk := update.Update.AgentMessageChunk
				if chunk == nil || chunk.Content.Text == nil {
					continue
				}

				chunks++
				rendered += chunk.Content.Text.Text

				messages = append(messages, *chunk.MessageId)
			}

			require.Equal(t, test.want, rendered, "a client concatenating the chunks renders the final text exactly once")
			require.Equal(t, test.chunks, chunks)
			require.Equal(t, test.messages, messages)
		})
	}
}
