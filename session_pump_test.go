package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func autonomousLifecycleNegotiation() lifecycle.Negotiated {
	return lifecycle.Negotiated{
		Versions:                []int{lifecycle.Version},
		UpdatesOutsidePrompt:    true,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	}
}

func autonomousPromptMeta(id string) map[string]any {
	meta := turnRouteMeta(id)
	meta[lifecycle.MetaKey] = map[string]any{
		"version": 1,
		"submission": map[string]any{
			"submissionId": id,
			"clientNonce":  id + "-nonce",
		},
	}

	return meta
}

func autonomousNativeMessage(text string) nativehermes.NativeMessage {
	return nativehermes.NativeMessage{
		Info: nativehermes.NativeMessageInfo{
			ID: "history-1", SessionID: "native-1", Role: valAssistant, Finish: valStop,
		},
		Parts: []nativehermes.Part{{
			ID: "history-1-text", SessionID: "native-1", MessageID: "history-1",
			Type: valText, Text: text, StreamedText: text,
		}},
	}
}

func sendAutonomousTextCycle(
	t *testing.T,
	client *fakeHermesClient,
	cycleID string,
	text string,
	projected chan<- error,
) {
	t.Helper()
	message := autonomousNativeMessage(text)
	client.mu.Lock()
	client.messages = []nativehermes.NativeMessage{message}
	client.mu.Unlock()

	part := message.Parts[0]
	properties, err := json.Marshal(part)
	require.NoError(t, err)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, TransportGeneration: 1,
		CycleID: cycleID, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: evtMessagePartUpdated, Properties: properties, TransportGeneration: 1,
		CycleID: cycleID, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleComplete, TransportGeneration: 1,
		CycleID: cycleID, Origin: nativehermes.CycleOriginActivity, Message: &message,
		ProjectionDone: func(err error) {
			if projected != nil {
				projected <- err
			}
		},
	})
}

func lifecycleEvents(updates []acp.SessionNotification) []map[string]any {
	events := make([]map[string]any, 0, len(updates))
	for _, update := range updates {
		envelope, ok := update.Meta[lifecycle.MetaKey].(map[string]any)
		if !ok {
			continue
		}
		event, ok := envelope["event"].(map[string]any)
		if ok {
			events = append(events, event)
		}
	}

	return events
}

func TestSessionPumpRetainsPreSnapshotAutonomousOutput(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	t.Cleanup(session.stopPump)

	projected := make(chan error, 1)
	sendAutonomousTextCycle(t, client, "activity-1", "late output", projected)
	select {
	case err := <-projected:
		t.Fatalf("pre-snapshot work projected early: %v", err)
	default:
	}

	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
	select {
	case err := <-projected:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("autonomous cycle did not project")
	}

	conn.mu.Lock()
	updates := append([]acp.SessionNotification(nil), conn.updates...)
	conn.mu.Unlock()
	events := lifecycleEvents(updates)
	require.Len(t, events, 3)
	require.Equal(t, "lifecycle_snapshot", events[0]["type"])
	require.Equal(t, map[string]any{
		"type": "state_update", "state": "running", "cause": "activity",
		"cycleId": events[1]["cycleId"], "turnId": events[1]["turnId"],
	}, events[1])
	require.Equal(t, "state_update", events[2]["type"])
	require.Equal(t, "idle", events[2]["state"])
	require.Equal(t, "activity", events[2]["cause"])
	require.Equal(t, "success", events[2]["outcome"])
	require.Equal(t, string(acp.StopReasonEndTurn), events[2]["stopReason"])

	var chunks []string
	for _, update := range updates {
		if update.Update.AgentMessageChunk != nil && update.Update.AgentMessageChunk.Content.Text != nil {
			chunks = append(chunks, update.Update.AgentMessageChunk.Content.Text.Text)
		}
	}
	require.Equal(t, []string{"late output"}, chunks)

	reducer := lifecycle.NewReducer(lifecycle.Options{Negotiated: autonomousLifecycleNegotiation()})
	for _, update := range updates {
		if update.Meta[lifecycle.MetaKey] == nil {
			continue
		}
		params, err := json.Marshal(update)
		require.NoError(t, err)
		require.NoError(t, reducer.ReduceSessionUpdate(params))
	}
	require.Len(t, reducer.State().Turns, 1)
	require.True(t, reducer.State().Turns[0].Terminal)
	require.Equal(t, lifecycle.OutcomeSuccess, reducer.State().Turns[0].Outcome)
}

func TestSessionPumpHoldsPostPromptActivityUntilForegroundSettlement(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	t.Cleanup(session.stopPump)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))

	release, settlement, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	turnCtx := session.beginTurn(t.Context(), "foreground")
	require.NoError(t, session.promotePromptForeground())
	epoch := sessionTurnEpoch(session)
	require.NoError(t, session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}))

	route, projected, err := session.registerPromptProjection(nativehermes.PromptDispatchInfo{
		CycleID: "prompt-cycle", TransportGeneration: 1,
	}, "foreground", epoch)
	require.NoError(t, err)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleComplete, CycleID: route.cycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginPrompt,
	})
	require.NoError(t, <-projected)

	activity := autonomousNativeMessage("after foreground")
	activity.Info.ID = "history-2"
	activity.Parts[0].ID = "history-2-text"
	activity.Parts[0].MessageID = "history-2"
	part, err := json.Marshal(activity.Parts[0])
	require.NoError(t, err)
	activityProjected := make(chan error, 1)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: "activity-after-prompt",
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: evtMessagePartUpdated, Properties: part, CycleID: "activity-after-prompt",
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleComplete, CycleID: "activity-after-prompt",
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity, Message: &activity,
		ProjectionDone: func(err error) { activityProjected <- err },
	})
	select {
	case result := <-activityProjected:
		t.Fatalf("activity crossed unsettled foreground: %v", result)
	default:
	}

	promptMessage := autonomousNativeMessage("")
	client.mu.Lock()
	client.messages = []nativehermes.NativeMessage{promptMessage}
	client.mu.Unlock()
	_, published, err := session.settlePrompt(
		t.Context(), turnCtx, epoch, SessionStoreTerminalState{},
		promptRun{settle: true, finish: valStop, nativeMessageID: promptMessage.Info.ID}, nil,
	)
	require.NoError(t, err)
	require.True(t, published)

	client.mu.Lock()
	client.messages = []nativehermes.NativeMessage{promptMessage, activity}
	client.mu.Unlock()
	release()
	settlement.complete()

	select {
	case err := <-activityProjected:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("post-prompt activity did not settle")
	}
	require.NoError(t, session.synchronizePump(t.Context()))

	conn.mu.Lock()
	events := lifecycleEvents(append([]acp.SessionNotification(nil), conn.updates...))
	conn.mu.Unlock()
	require.Len(t, events, 6)
	require.Equal(t, "idle", events[3]["state"])
	require.Equal(t, "submission", events[3]["cause"])
	require.Equal(t, "running", events[4]["state"])
	require.Equal(t, "activity", events[4]["cause"])
	require.Equal(t, "idle", events[5]["state"])
	require.Equal(t, "activity", events[5]["cause"])
}

func TestSessionPumpDrainsCompletedAutonomousBeforePromptDispatch(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	connection := newRecordingAgentClient()
	agent.setAgentClient(connection)
	client := newFakeHermesClient()
	s := testSession(agent, client)
	t.Cleanup(s.stopPump)
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
	require.NoError(t, agent.storeStartedSession(s))

	activity := autonomousNativeMessage("older activity")
	client.mu.Lock()
	client.messages = []nativehermes.NativeMessage{activity}
	client.mu.Unlock()
	cycleID := "completed-before-prompt"
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: cycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	projected := make(chan error, 1)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleComplete, CycleID: cycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Message: &activity, ProjectionDone: func(err error) { projected <- err },
	})
	require.NoError(t, <-projected)
	require.NoError(t, s.synchronizePump(t.Context()))

	dispatchSnapshots := make(chan []map[string]any, 1)
	client.beforePromptDispatch = func() {
		connection.mu.Lock()
		updates := append([]acp.SessionNotification(nil), connection.updates...)
		connection.mu.Unlock()
		dispatchSnapshots <- lifecycleEvents(updates)
	}
	_, err := s.Prompt(t.Context(), acp.PromptRequest{
		Meta: autonomousPromptMeta("after-activity"), SessionId: s.id,
		Prompt: []acp.ContentBlock{acp.TextBlock("prompt after activity")},
	})
	require.NoError(t, err)

	events := <-dispatchSnapshots
	activityIdle := -1
	for index, event := range events {
		if event["type"] == string(lifecycle.EventStateUpdate) &&
			event["cause"] == string(lifecycle.CauseActivity) &&
			event["state"] == string(lifecycle.ForegroundIdle) {
			activityIdle = index
		}
		require.NotEqual(t, string(lifecycle.EventPromptAccepted), event["type"])
	}
	require.NotEqual(t, -1, activityIdle)
	require.Equal(t, len(events)-1, activityIdle)

	connection.mu.Lock()
	allEvents := lifecycleEvents(append([]acp.SessionNotification(nil), connection.updates...))
	connection.mu.Unlock()
	promptAccepted := -1
	for index, event := range allEvents {
		if event["type"] == string(lifecycle.EventPromptAccepted) {
			promptAccepted = index

			break
		}
	}
	require.Greater(t, promptAccepted, activityIdle)
	client.mu.Lock()
	require.Equal(t, uint64(1), client.promptCycles)
	client.mu.Unlock()
}

func TestSessionPumpBackgroundControlsResolveExactlyOnce(t *testing.T) {
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	t.Cleanup(session.stopPump)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))

	message := autonomousNativeMessage("")
	client.mu.Lock()
	client.messages = []nativehermes.NativeMessage{message}
	client.mu.Unlock()
	cycleID := "activity-controls"
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, TransportGeneration: 4,
		CycleID: cycleID, Origin: nativehermes.CycleOriginActivity,
	})
	permission := testHermesPermissionRequest(t, "permission-local", "tool-1")
	permission.CycleID = cycleID
	permission.TransportGeneration = 4
	question := nativehermes.QuestionRequest{
		ID: "question-local", SessionID: "native-1",
		Questions: []nativehermes.QuestionInfo{{Question: "Continue?"}},
		CycleID:   cycleID, TransportGeneration: 4,
	}
	for range 2 {
		copyPermission := permission
		client.emitEvent(nativehermes.TurnEvent{
			Type: evtApprovalRequest, TransportGeneration: 4, CycleID: cycleID,
			Origin: nativehermes.CycleOriginActivity, Permission: &copyPermission,
		})
	}
	for range 2 {
		copyQuestion := question
		client.emitEvent(nativehermes.TurnEvent{
			Type: evtClarifyRequest, TransportGeneration: 4, CycleID: cycleID,
			Origin: nativehermes.CycleOriginActivity, Question: &copyQuestion,
		})
	}
	projected := make(chan error, 1)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleComplete, TransportGeneration: 4,
		CycleID: cycleID, Origin: nativehermes.CycleOriginActivity, Message: &message,
		ProjectionDone: func(err error) { projected <- err },
	})
	select {
	case err := <-projected:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("control cycle did not settle")
	}

	require.Equal(t, 1, conn.permissionRequestCount())
	require.Equal(t, 1, client.permissionReplyCount())
	require.Len(t, conn.elicitations, 1)
	require.Equal(t, 1, client.questionReplyCount())
}

type replaceFailStore struct {
	SessionStore
	err error
}

func (s replaceFailStore) Replace(context.Context, SessionKey, []SessionStoreReplacement) error {
	return s.err
}

func TestSessionPumpStoreFailureNeverPublishesIdleSuccess(t *testing.T) {
	wantErr := errors.New("durable store unavailable")
	store := replaceFailStore{SessionStore: NewInMemorySessionStore(), err: wantErr}
	agent := newTestAgent(WithSessionStore(store))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	t.Cleanup(session.stopPump)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))

	projected := make(chan error, 1)
	sendAutonomousTextCycle(t, client, "activity-store-failure", "visible", projected)
	select {
	case err := <-projected:
		require.ErrorIs(t, err, wantErr)
	case <-time.After(2 * time.Second):
		t.Fatal("store failure did not settle the projection")
	}

	conn.mu.Lock()
	events := lifecycleEvents(append([]acp.SessionNotification(nil), conn.updates...))
	conn.mu.Unlock()
	for _, event := range events {
		if event["type"] == "state_update" && event["state"] == "idle" && event["outcome"] == "success" {
			t.Fatalf("store failure published idle success: %#v", event)
		}
	}
	require.True(t, session.lifecycleStream().fenced())
	require.NotEqual(t, string(lifecycle.OutcomeSuccess), session.committedTerminalState().Outcome)
}

func TestSessionPumpContinuesAfterProviderFailureThroughActivityAndPrompt(t *testing.T) {
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	t.Cleanup(session.stopPump)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))

	failedProjection := make(chan error, 1)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: "provider-failure", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleFailed, CycleID: "provider-failure", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Err:            nativehermes.NewTurnFailure(nativehermes.CauseProvider, "provider failed"),
		ProjectionDone: func(err error) { failedProjection <- err },
	})
	require.NoError(t, <-failedProjection)

	session.pumpMu.Lock()
	done := session.pumpDone
	session.pumpMu.Unlock()
	select {
	case <-done:
		t.Fatal("provider failure stopped the permanent pump")
	default:
	}

	activityProjection := make(chan error, 1)
	sendAutonomousTextCycle(t, client, "later-activity", "activity", activityProjection)
	require.NoError(t, <-activityProjection)

	promptMessage := autonomousNativeMessage("prompt")
	promptMessage.Info.ID = "prompt-message"
	promptMessage.Parts[0].ID = "prompt-message-text"
	promptMessage.Parts[0].MessageID = "prompt-message"
	client.sendMessage = func(_ context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return promptMessage, nil
	}

	response, err := session.Prompt(t.Context(), acp.PromptRequest{
		Meta: autonomousPromptMeta("provider-recovery"), SessionId: session.id,
		Prompt: []acp.ContentBlock{acp.TextBlock("continue")},
	})
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)

	conn.mu.Lock()
	events := lifecycleEvents(append([]acp.SessionNotification(nil), conn.updates...))
	conn.mu.Unlock()
	failedIdle := 0
	for _, event := range events {
		if event["type"] == "state_update" && event["state"] == "idle" && event["outcome"] == "failed" {
			failedIdle++
		}
	}
	require.Equal(t, 1, failedIdle)
}

func TestImmediatePromptWaitsForPumpContainmentAndLazilyResumesOnce(t *testing.T) {
	session, agent, _ := newResumeRuntimeTestSession(t)
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
	session.mu.Lock()
	session.runtimeNeedsResume = false
	session.mu.Unlock()

	oldClient, ok := session.client.(*fakeHermesClient)
	require.True(t, ok)
	failedProjection := make(chan error, 1)
	oldClient.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: "failed-before-loss", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	oldClient.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleFailed, CycleID: "failed-before-loss", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Err:            nativehermes.NewTurnFailure(nativehermes.CauseProvider, "provider failed"),
		ProjectionDone: func(err error) { failedProjection <- err },
	})
	require.NoError(t, <-failedProjection)

	containmentReached := make(chan struct{})
	containmentRelease := make(chan struct{})
	oldClient.closeFunc = func(context.Context) error {
		close(containmentReached)
		<-containmentRelease

		return nil
	}
	oldClient.emitError(errors.New("transport lost"))
	<-containmentReached

	replacement := newFakeHermesClient()
	replacement.getSession = testNativeSession("native-1")
	promptMessage := autonomousNativeMessage("fresh prompt")
	promptMessage.Info.ID = "fresh-prompt"
	promptMessage.Parts[0].ID = "fresh-prompt-text"
	promptMessage.Parts[0].MessageID = "fresh-prompt"
	replacement.sendMessage = func(_ context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return promptMessage, nil
	}
	factoryCalled := make(chan struct{}, 2)
	agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		replacement.xdg = options.ExistingXDG
		factoryCalled <- struct{}{}

		return replacement, nil
	}

	type promptResult struct {
		response acp.PromptResponse
		err      error
	}
	result := make(chan promptResult, 1)
	go func() {
		response, err := session.Prompt(t.Context(), acp.PromptRequest{
			Meta: autonomousPromptMeta("resume-after-loss"), SessionId: session.id,
			Prompt: []acp.ContentBlock{acp.TextBlock("continue")},
		})
		result <- promptResult{response: response, err: err}
	}()

	select {
	case <-factoryCalled:
		t.Fatal("replacement started before old containment completed")
	case got := <-result:
		t.Fatalf("prompt returned before old containment completed: %#v", got)
	default:
	}
	close(containmentRelease)
	<-factoryCalled
	got := <-result
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonEndTurn, got.response.StopReason)
	select {
	case <-factoryCalled:
		t.Fatal("prompt installed more than one replacement runtime")
	default:
	}

	require.NoError(t, session.Close(t.Context()))
}

func TestSessionPumpPreSnapshotOverflowFencesIncarnation(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	client := newFakeHermesClient()
	client.deliveries = make(chan nativehermes.TurnDelivery, sessionPumpBacklogCapacity+2)
	session := testSession(agent, client)

	session.pumpMu.Lock()
	done := session.pumpDone
	session.pumpMu.Unlock()
	for index := 0; index <= sessionPumpBacklogCapacity; index++ {
		client.emitEvent(nativehermes.TurnEvent{Type: evtMessagePartUpdated})
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pre-snapshot overflow did not stop the pump")
	}
	session.pumpMu.Lock()
	pumpErr := session.pumpErr
	session.pumpMu.Unlock()
	require.ErrorIs(t, pumpErr, ErrSessionPumpOverflow)
}

func TestSessionCloseJoinsPumpAndEmitsNothingAfterward(t *testing.T) {
	agent := newTestAgent()
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)

	cycleID := "activity-close"
	part := nativehermes.Part{
		ID: "part-1", SessionID: "native-1", MessageID: "history-1", Type: valText, Text: "before close",
	}
	properties, err := json.Marshal(part)
	require.NoError(t, err)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: cycleID, TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: evtMessagePartUpdated, CycleID: cycleID, TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Properties: properties,
	})
	deadline := time.After(2 * time.Second)
	for conn.updateCount() != 1 {
		select {
		case <-deadline:
			t.Fatal("pre-close output was not delivered")
		default:
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, session.Close(ctx))
	wantUpdates := conn.updateCount()
	client.emitEvent(nativehermes.TurnEvent{
		Type: evtMessagePartUpdated, CycleID: cycleID, TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Properties: properties,
	})
	require.Equal(t, wantUpdates, conn.updateCount())

	session.pumpMu.Lock()
	done := session.pumpDone
	session.pumpMu.Unlock()
	select {
	case <-done:
	default:
		t.Fatal("session close returned before the pump joined")
	}
}

func TestSessionCloseTerminalizesOpenAutonomousCycleBeforeReturning(t *testing.T) {
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))

	part := nativehermes.Part{
		ID: "open-text", SessionID: "native-1", MessageID: "open-message", Type: valText, Text: "before close",
	}
	properties, err := json.Marshal(part)
	require.NoError(t, err)
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: "open-activity", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: evtMessagePartUpdated, CycleID: "open-activity", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Properties: properties,
	})
	require.NoError(t, session.synchronizePump(t.Context()))

	closed := make(chan error, 1)
	go func() { closed <- session.Close(t.Context()) }()
	require.NoError(t, <-closed)
	require.Equal(t, 1, client.abortCount())

	conn.mu.Lock()
	updates := append([]acp.SessionNotification(nil), conn.updates...)
	conn.mu.Unlock()
	events := lifecycleEvents(updates)
	cancelledIdle := 0
	for _, event := range events {
		if event["type"] == "state_update" && event["state"] == "idle" {
			require.Equal(t, "cancelled", event["outcome"])
			require.Equal(t, "cancelled", event["stopReason"])
			cancelledIdle++
		}
	}
	require.Equal(t, 1, cancelledIdle)
	require.True(t, session.lifecycleStream().fenced())

	before := conn.updateCount()
	client.emitEvent(nativehermes.TurnEvent{
		Type: evtMessagePartUpdated, CycleID: "open-activity", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Properties: properties,
	})
	require.Equal(t, before, conn.updateCount())

	session.pumpMu.Lock()
	done := session.pumpDone
	session.pumpMu.Unlock()
	select {
	case <-done:
	default:
		t.Fatal("close returned before the sole event consumer joined")
	}
}

func TestSessionCloseCancelsAutonomousPermissionAndElicitationBeforeTerminal(t *testing.T) {
	for _, kind := range []string{"permission", "elicitation"} {
		t.Run(kind, func(t *testing.T) {
			agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
			agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
			agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
			conn := newRecordingAgentClient()
			conn.permissionStarted = make(chan struct{}, 1)
			conn.permissionRelease = make(chan struct{})
			conn.elicitationStarted = make(chan struct{}, 1)
			conn.elicitationRelease = make(chan struct{})
			agent.setAgentClient(conn)
			client := newFakeHermesClient()
			session := testSession(agent, client)
			require.NoError(t, session.openLifecycleStream())
			require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))

			cycleID := "open-control-" + kind
			client.emitEvent(nativehermes.TurnEvent{
				Type: nativehermes.EventCycleStarted, CycleID: cycleID,
				TransportGeneration: 5, Origin: nativehermes.CycleOriginActivity,
			})
			switch kind {
			case "permission":
				req := testHermesPermissionRequest(t, "permission", "tool")
				req.CycleID = cycleID
				req.TransportGeneration = 5
				client.emitEvent(nativehermes.TurnEvent{
					Type: evtApprovalRequest, CycleID: cycleID, TransportGeneration: 5,
					Origin: nativehermes.CycleOriginActivity, Permission: &req,
				})
				<-conn.permissionStarted
			case "elicitation":
				req := nativehermes.QuestionRequest{
					ID: "question", SessionID: "native-1",
					Questions: []nativehermes.QuestionInfo{{Question: "Continue?"}},
					CycleID:   cycleID, TransportGeneration: 5,
				}
				client.emitEvent(nativehermes.TurnEvent{
					Type: evtClarifyRequest, CycleID: cycleID, TransportGeneration: 5,
					Origin: nativehermes.CycleOriginActivity, Question: &req,
				})
				<-conn.elicitationStarted
			}

			require.NoError(t, session.Close(t.Context()))
			require.Equal(t, 1, client.abortCount())
			if kind == "permission" {
				require.Equal(t, 1, client.permissionReplyCount())
			} else {
				require.Equal(t, 1, client.questionRejectCount())
			}

			conn.mu.Lock()
			events := lifecycleEvents(append([]acp.SessionNotification(nil), conn.updates...))
			conn.mu.Unlock()
			cancelledIdle := 0
			for _, event := range events {
				if event["type"] == "state_update" && event["state"] == "idle" {
					require.Equal(t, "cancelled", event["outcome"])
					cancelledIdle++
				}
			}
			require.Equal(t, 1, cancelledIdle)
			require.True(t, session.lifecycleStream().fenced())
		})
	}
}
func TestPumpRouteHelpersAndRegistration(t *testing.T) {
	ctx := t.Context()
	if withPumpRoute(ctx, nil) != ctx {
		t.Fatal("nil route changed context")
	}
	if route, ok := pumpRouteFromContext(nil); ok || route != nil { //nolint:staticcheck // Nil-context behavior is part of this private helper's contract.
		t.Fatalf("nil context route = %#v, %v", route, ok)
	}
	route := &pumpCycleRoute{nonce: "nonce"}
	routed := withPumpRoute(ctx, route)
	if got, ok := pumpRouteFromContext(routed); !ok || got != route {
		t.Fatalf("context route = %#v, %v", got, ok)
	}
	if got, ok := pumpRouteFromContext(ctx); ok || got != nil {
		t.Fatalf("empty context route = %#v, %v", got, ok)
	}

	s := &session{}
	s.startPump(nil)
	if s.pumpDone != nil {
		t.Fatal("nil client started pump")
	}
	if _, _, err := s.registerPromptProjection(nativehermes.PromptDispatchInfo{}, "n", 1); err == nil {
		t.Fatal("registration succeeded without pump")
	}

	client := newFakeHermesClient()
	s = testSession(newTestAgent(), client)
	defer s.stopPump()
	s.startPump(client)
	s.pumpMu.Lock()
	done := s.pumpDone
	s.pumpMu.Unlock()
	if done == nil {
		t.Fatal("pump did not start")
	}

	if _, _, err := s.registerPromptProjection(nativehermes.PromptDispatchInfo{}, "nonce", 4); err == nil {
		t.Fatal("registration accepted missing native identity")
	}
	prompt, projected, err := s.registerPromptProjection(nativehermes.PromptDispatchInfo{
		CycleID: "prompt-cycle", TransportGeneration: 1,
	}, "nonce", 4)
	require.NoError(t, err)
	require.Equal(t, "prompt-cycle", prompt.cycleID)
	if _, _, err := s.registerPromptProjection(nativehermes.PromptDispatchInfo{
		CycleID: prompt.cycleID, TransportGeneration: 1,
	}, "other", 5); err == nil {
		t.Fatal("duplicate registration succeeded")
	}
	s.resolvePromptProjection(nil, nil)
	s.resolvePromptProjection(prompt, nil)
	s.resolvePromptProjection(prompt, errors.New("ignored"))
	require.NoError(t, <-projected)

	s.pumpMu.Lock()
	s.pumpErr = errors.New("pump failed")
	s.pumpMu.Unlock()
	if _, _, err := s.registerPromptProjection(nativehermes.PromptDispatchInfo{CycleID: "later", TransportGeneration: 1}, "n", 6); err == nil {
		t.Fatal("registration ignored pump failure")
	}
}

func TestPumpShutdownJoinsAndRemovesPromptProjection(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	s := testSession(newTestAgent(), newFakeHermesClient())
	route, projected, err := s.registerPromptProjection(nativehermes.PromptDispatchInfo{
		CycleID: "shutdown-prompt", TransportGeneration: 1,
	}, "nonce", 1)
	require.NoError(t, err)
	require.NotNil(t, route)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, s.stopPumpContext(ctx))
	require.ErrorIs(t, <-projected, errPromptCancelled)

	s.pumpMu.Lock()
	_, retained := s.pumpRoutes[route.cycleID]
	pumpErr := s.pumpErr
	s.pumpMu.Unlock()
	require.False(t, retained)
	require.NoError(t, pumpErr)
}

func TestNilCausePumpRouteResolutionCannotLeakRoutes(t *testing.T) {
	prompt := &pumpCycleRoute{cycleID: "prompt", prompt: true, projected: make(chan error, 1)}
	autonomous := &pumpCycleRoute{cycleID: "activity"}
	s := &session{
		pumpRoutes:          map[string]*pumpCycleRoute{prompt.cycleID: prompt, autonomous.cycleID: autonomous},
		pending:             map[string]nativehermes.PermissionRequest{},
		questions:           map[string]nativehermes.QuestionRequest{},
		actionRequests:      map[string]actionRequestOwnership{},
		processedPermission: map[string]struct{}{},
		processedQuestion:   map[string]struct{}{},
	}

	s.resolvePumpRoutes(nil, false)
	require.ErrorIs(t, <-prompt.projected, errPromptCancelled)
	require.Empty(t, s.pumpRoutes)
	require.NoError(t, s.pumpErr)
}

func TestPumpSynchronizationEdges(t *testing.T) {
	want := errors.New("pump failed")
	s := &session{pumpErr: want}
	require.ErrorIs(t, s.synchronizePump(t.Context()), want)
	s = &session{}
	require.Error(t, s.synchronizePump(t.Context()))
	require.EqualError(t, s.stoppedPumpError(), "hermes session pump stopped")
	s.pumpErr = want
	require.ErrorIs(t, s.stoppedPumpError(), want)

	done := make(chan struct{})
	close(done)
	s = &session{pumpDone: done, pumpBarriers: make(chan pumpBarrierRequest)}
	require.EqualError(t, s.synchronizePump(t.Context()), "hermes session pump stopped")

	s = &session{pumpDone: make(chan struct{}), pumpBarriers: make(chan pumpBarrierRequest)}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, s.synchronizePump(ctx), context.Canceled)

	s = &session{pumpDone: make(chan struct{}), pumpBarriers: make(chan pumpBarrierRequest)}
	received := make(chan struct{})
	ctx, cancel = context.WithCancel(t.Context())
	go func() {
		<-s.pumpBarriers
		close(received)
	}()
	go func() {
		<-received
		cancel()
	}()
	require.ErrorIs(t, s.synchronizePump(ctx), context.Canceled)

	s = &session{pumpDone: make(chan struct{}), pumpBarriers: make(chan pumpBarrierRequest)}
	go func() {
		barrier := <-s.pumpBarriers
		barrier.reply <- want
	}()
	require.ErrorIs(t, s.synchronizePump(t.Context()), want)

	s = &session{pumpDone: make(chan struct{}), pumpBarriers: make(chan pumpBarrierRequest)}
	received = make(chan struct{})
	go func() {
		<-s.pumpBarriers
		close(received)
		close(s.pumpDone)
	}()
	require.EqualError(t, s.synchronizePump(t.Context()), "hermes session pump stopped")

	joinCtx, joinCancel := context.WithCancel(t.Context())
	joinCancel()
	s = &session{pumpDone: make(chan struct{}), pumpCancel: func() {}}
	require.ErrorIs(t, s.stopPumpContext(joinCtx), context.Canceled)
}

func TestPumpRouteLookupEdges(t *testing.T) {
	s := &session{pumpIncarnation: 2, pumpRoutes: map[string]*pumpCycleRoute{}}
	if s.routeForEvent(nativehermes.TurnEvent{}) != nil || s.pumpRouteCurrent(nil) {
		t.Fatal("empty route resolved")
	}

	route := &pumpCycleRoute{incarnation: 2, generation: 3, cycleID: "cycle"}
	s.pumpRoutes[route.cycleID] = route
	if got := s.routeForEvent(nativehermes.TurnEvent{CycleID: "cycle", TransportGeneration: 3}); got != route {
		t.Fatalf("exact route = %#v", got)
	}
	if got := s.routeForEvent(nativehermes.TurnEvent{}); got != nil {
		t.Fatalf("missing identity route = %#v", got)
	}
	if got := s.routeForEvent(nativehermes.TurnEvent{CycleID: "cycle", TransportGeneration: 4}); got != nil {
		t.Fatalf("stale generation route = %#v", got)
	}
	route.incarnation = 1
	if got := s.routeForEvent(nativehermes.TurnEvent{CycleID: "cycle", TransportGeneration: 3}); got != nil {
		t.Fatalf("stale incarnation route = %#v", got)
	}
	route.incarnation = 2
	require.True(t, s.pumpRouteCurrent(route))
	delete(s.pumpRoutes, route.cycleID)
	require.False(t, s.pumpRouteCurrent(route))
	s.pumpRoutes[route.cycleID] = route
	s.pumpErr = errors.New("ended")
	require.False(t, s.pumpRouteCurrent(route))
}

func TestHandlePumpItemEdges(t *testing.T) {
	agent := newTestAgent()
	client := newFakeHermesClient()
	s := testSession(agent, client)
	defer s.stopPump()

	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{}))
	require.Error(t, s.handlePumpItem(t.Context(), pumpItem{err: errors.New("wire")}))
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &nativehermes.TurnEvent{Type: nativehermes.EventGatewayRaw}}))
	require.Error(t, s.handlePumpItem(t.Context(), pumpItem{event: &nativehermes.TurnEvent{Type: evtMessagePartUpdated, CycleID: "missing"}}))

	projectionResult := make(chan error, 1)
	s.pumpMu.Lock()
	s.pumpStopping = true
	s.pumpMu.Unlock()
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &nativehermes.TurnEvent{
		Type: evtMessagePartUpdated,
		ProjectionDone: func(err error) {
			projectionResult <- err
		},
	}}))
	require.NoError(t, <-projectionResult)
	s.pumpMu.Lock()
	s.pumpStopping = false
	s.pumpMu.Unlock()

	route := &pumpCycleRoute{
		incarnation: s.pumpIncarnation, generation: 1, cycleID: "prompt", prompt: true,
		projected: make(chan error, 1),
	}
	s.pumpMu.Lock()
	s.pumpRoutes[route.cycleID] = route
	s.pumpMu.Unlock()
	terminal := nativehermes.TurnEvent{Type: nativehermes.EventCycleComplete, CycleID: route.cycleID, TransportGeneration: 1}
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &terminal}))
	require.NoError(t, <-route.projected)

	failed := &pumpCycleRoute{
		incarnation: s.pumpIncarnation, generation: 1, cycleID: "failed-prompt", prompt: true,
		projected: make(chan error, 1),
	}
	s.pumpMu.Lock()
	s.pumpRoutes[failed.cycleID] = failed
	s.pumpMu.Unlock()
	failure := nativehermes.TurnEvent{Type: nativehermes.EventCycleFailed, CycleID: failed.cycleID, TransportGeneration: 1}
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &failure}))
	require.EqualError(t, <-failed.projected, "hermes prompt cycle failed")

	ignored := &pumpCycleRoute{incarnation: s.pumpIncarnation, generation: 1, cycleID: "ignored"}
	s.pumpMu.Lock()
	s.pumpRoutes[ignored.cycleID] = ignored
	s.pumpMu.Unlock()
	event := nativehermes.TurnEvent{Type: "unknown", CycleID: ignored.cycleID, TransportGeneration: 1}
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &event}))

	s.rawMessages = rawMessageConfig{enabled: true}
	event.Raw = []byte(`{"type":"unknown"}`)
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &event}))
}

func TestDrainPumpBarrierEdges(t *testing.T) {
	s := testSession(newTestAgent(), newFakeHermesClient())
	defer s.stopPump()

	deliveries := make(chan nativehermes.TurnDelivery)
	close(deliveries)
	closed, err := s.drainPumpBarrier(t.Context(), deliveries)
	require.True(t, closed)
	require.Error(t, err)

	deliveries = make(chan nativehermes.TurnDelivery, 1)
	missing := nativehermes.TurnEvent{Type: evtMessagePartUpdated, CycleID: "missing"}
	deliveries <- nativehermes.TurnDelivery{Event: &missing}
	closed, err = s.drainPumpBarrier(t.Context(), deliveries)
	require.False(t, closed)
	require.Error(t, err)

	deliveries = make(chan nativehermes.TurnDelivery, 1)
	deliveries <- nativehermes.TurnDelivery{Err: errors.New("wire")}
	closed, err = s.drainPumpBarrier(t.Context(), deliveries)
	require.False(t, closed)
	require.Error(t, err)

	deliveries = make(chan nativehermes.TurnDelivery, 1)
	raw := nativehermes.TurnEvent{Type: nativehermes.EventGatewayRaw}
	deliveries <- nativehermes.TurnDelivery{Event: &raw}
	closed, err = s.drainPumpBarrier(t.Context(), deliveries)
	require.False(t, closed)
	require.NoError(t, err)
}

func TestAutonomousCycleFailureAndValidation(t *testing.T) {
	t.Run("closed session", func(t *testing.T) {
		s := testSession(newTestAgent(), newFakeHermesClient())
		defer s.stopPump()
		s.mu.Lock()
		s.lifecycleClosing = true
		s.mu.Unlock()
		err := s.startAutonomousCycle(t.Context(), nativehermes.TurnEvent{CycleID: "cycle", TransportGeneration: 1})
		var requestErr *acp.RequestError
		require.ErrorAs(t, err, &requestErr)

		projected := make(chan error, 1)
		event := nativehermes.TurnEvent{
			Type: nativehermes.EventCycleStarted, CycleID: "cycle", TransportGeneration: 1,
			Origin: nativehermes.CycleOriginActivity, ProjectionDone: func(err error) { projected <- err },
		}
		require.Error(t, s.handlePumpItem(t.Context(), pumpItem{event: &event}))
		require.Error(t, <-projected)
	})

	t.Run("foreground contention retains structured cause", func(t *testing.T) {
		s := testSession(newTestAgent(), newFakeHermesClient())
		defer s.stopPump()
		_, releaseReuse, err := s.beginReuse(t.Context())
		require.NoError(t, err)
		defer releaseReuse()

		err = s.startAutonomousCycle(t.Context(), nativehermes.TurnEvent{CycleID: "cycle", TransportGeneration: 1})
		var contention *sessionForegroundContentionError
		require.ErrorAs(t, err, &contention)
		require.Equal(t, sessionForegroundBackpressure().Error(), contention.Error())
		requireSessionForegroundBackpressure(t, contention.Unwrap())
	})

	t.Run("nonce failure", func(t *testing.T) {
		s := testSession(newTestAgent(), newFakeHermesClient())
		defer s.stopPump()
		s.newPumpNonce = func() (string, error) { return "", errors.New("entropy failed") }
		err := s.startAutonomousCycle(t.Context(), nativehermes.TurnEvent{CycleID: "cycle", TransportGeneration: 1})
		require.Error(t, err)
	})

	t.Run("duplicate", func(t *testing.T) {
		s := testSession(newTestAgent(), newFakeHermesClient())
		defer s.stopPump()
		s.pumpMu.Lock()
		s.pumpRoutes["cycle"] = &pumpCycleRoute{}
		s.pumpMu.Unlock()
		require.Error(t, s.startAutonomousCycle(t.Context(), nativehermes.TurnEvent{CycleID: "cycle", TransportGeneration: 1}))
	})

	t.Run("opening failure removes route", func(t *testing.T) {
		agent := newTestAgent()
		agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
		s := testSession(agent, newFakeHermesClient())
		defer s.stopPump()
		require.NoError(t, s.openLifecycleStream())
		err := s.startAutonomousCycle(t.Context(), nativehermes.TurnEvent{CycleID: "cycle", TransportGeneration: 1})
		require.Error(t, err)
		s.pumpMu.Lock()
		_, exists := s.pumpRoutes["cycle"]
		s.pumpMu.Unlock()
		require.False(t, exists)
	})

	t.Run("completion validation", func(t *testing.T) {
		s := testSession(newTestAgent(), newFakeHermesClient())
		defer s.stopPump()
		route := &pumpCycleRoute{cycleID: "cycle"}
		s.pumpMu.Lock()
		s.pumpRoutes[route.cycleID] = route
		s.pumpMu.Unlock()
		require.Error(t, s.settleAutonomousCycle(t.Context(), route, nativehermes.TurnEvent{}))

		route = &pumpCycleRoute{cycleID: "cycle-2"}
		s.pumpMu.Lock()
		s.pumpRoutes[route.cycleID] = route
		s.pumpMu.Unlock()
		message := autonomousNativeMessage("")
		message.Info.Finish = "unknown"
		require.Error(t, s.settleAutonomousCycle(t.Context(), route, nativehermes.TurnEvent{Message: &message}))

		route = &pumpCycleRoute{cycleID: "cycle-3"}
		s.pumpMu.Lock()
		s.pumpRoutes[route.cycleID] = route
		s.pumpMu.Unlock()
		message = autonomousNativeMessage("cannot emit")
		message.Info.ID = "cannot-emit"
		message.Parts[0].ID = "cannot-emit-text"
		message.Parts[0].MessageID = "cannot-emit"
		message.Parts[0].StreamedText = ""
		require.Error(t, s.settleAutonomousCycle(t.Context(), route, nativehermes.TurnEvent{Message: &message}))
	})

	t.Run("failed lifecycle", func(t *testing.T) {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
		agent.setAgentClient(newRecordingAgentClient())
		s := testSession(agent, newFakeHermesClient())
		defer s.stopPump()
		require.NoError(t, s.openLifecycleStream())
		require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
		event := nativehermes.TurnEvent{Type: nativehermes.EventCycleStarted, CycleID: "failure", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity}
		require.NoError(t, s.startAutonomousCycle(t.Context(), event))
		route := s.routeForEvent(nativehermes.TurnEvent{CycleID: event.CycleID, TransportGeneration: 1})
		want := errors.New("provider failed")
		require.NoError(t, s.failAutonomousCycle(withPumpRoute(t.Context(), route), route, want))
	})

	t.Run("nil failure and store failure", func(t *testing.T) {
		storeErr := errors.New("store failed")
		agent := newTestAgent(WithSessionStore(replaceFailStore{SessionStore: NewInMemorySessionStore(), err: storeErr}))
		agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
		agent.setAgentClient(newRecordingAgentClient())
		s := testSession(agent, newFakeHermesClient())
		defer s.stopPump()
		require.NoError(t, s.openLifecycleStream())
		require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
		event := nativehermes.TurnEvent{CycleID: "store-failure", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity}
		require.NoError(t, s.startAutonomousCycle(t.Context(), event))
		route := s.routeForEvent(event)
		err := s.failAutonomousCycle(withPumpRoute(t.Context(), route), route, nil)
		require.ErrorIs(t, err, storeErr)
	})

	t.Run("terminal blocker delivery fails", func(t *testing.T) {
		for _, failedCycle := range []bool{false, true} {
			s, conn, turnCtx := newLifecycleActionSession(t, true)
			defer s.stopPump()
			route, active := s.permissionTurnRoute(turnCtx)
			require.True(t, active)
			action, update, owned := s.reserveBlockingAction(lifecycle.ActionPermission, "request", route)
			require.True(t, owned)
			require.NoError(t, s.publishBlockingAction(turnCtx, action, update))
			conn.updateErr = errors.New("action terminal delivery failed")
			var err error
			autonomous := &pumpCycleRoute{cycleID: "autonomous"}
			s.pumpMu.Lock()
			s.pumpRoutes[autonomous.cycleID] = autonomous
			s.pumpMu.Unlock()
			if failedCycle {
				err = s.failAutonomousCycle(withPumpRoute(turnCtx, autonomous), autonomous, errors.New("provider failed"))
			} else {
				message := autonomousNativeMessage("")
				err = s.settleAutonomousCycle(withPumpRoute(turnCtx, autonomous), autonomous, nativehermes.TurnEvent{Message: &message})
			}
			require.ErrorContains(t, err, "action terminal delivery failed")
		}
	})

	t.Run("failed idle delivery", func(t *testing.T) {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
		base := newRecordingAgentClient()
		conn := &lifecycleFailingAgentClient{recordingAgentClient: base, failAt: 3}
		agent.setAgentClient(conn)
		s := testSession(agent, newFakeHermesClient())
		defer s.stopPump()
		require.NoError(t, s.openLifecycleStream())
		require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
		event := nativehermes.TurnEvent{CycleID: "idle-failure", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity}
		require.NoError(t, s.startAutonomousCycle(t.Context(), event))
		route := s.routeForEvent(event)
		err := s.failAutonomousCycle(withPumpRoute(t.Context(), route), route, errors.New("provider failed"))
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})
}

func TestResolveAndFencePumpEdges(t *testing.T) {
	s := testSession(newTestAgent(), newFakeHermesClient())
	defer s.stopPump()
	s.resolvePumpRoutes(nil, true)

	prompt := &pumpCycleRoute{incarnation: s.pumpIncarnation, cycleID: "p", prompt: true, projected: make(chan error, 1)}
	autonomous := &pumpCycleRoute{incarnation: s.pumpIncarnation, cycleID: "a"}
	s.pumpMu.Lock()
	s.pumpRoutes[prompt.cycleID] = prompt
	s.pumpRoutes[autonomous.cycleID] = autonomous
	s.pumpMu.Unlock()
	want := errors.New("stopped")
	s.resolvePumpRoutes(want, true)
	require.ErrorIs(t, <-prompt.projected, want)
	s.resolvePumpRoutes(errors.New("ignored"), false)

	other := newFakeHermesClient()
	s.fencePumpClient(other, s.pumpIncarnation+1, want, make(chan struct{}))
	require.False(t, s.runtimeNeedsResume)

	current := newFakeHermesClient()
	current.closeErr = errors.New("close failed")
	s.pumpMu.Lock()
	s.pumpClient = current
	incarnation := s.pumpIncarnation
	s.pumpMu.Unlock()
	resumeWait := s.publishPumpResumeNeeded(current, incarnation)
	require.NotNil(t, resumeWait)
	s.fencePumpClient(current, incarnation, want, resumeWait)
	require.True(t, s.runtimeNeedsResume)
	require.Contains(t, s.poisonCause, "close failed")
	s.closed = true
	s.runtimeNeedsResume = false
	s.poisonCause = "existing"
	s.fencePumpClient(current, incarnation, want, make(chan struct{}))
	require.False(t, s.runtimeNeedsResume)
	require.Equal(t, "existing", s.poisonCause)

	closed := testSession(newTestAgent(), newFakeHermesClient())
	defer closed.stopPump()
	closed.mu.Lock()
	closed.closed = true
	closed.mu.Unlock()
	closed.pumpMu.Lock()
	closedClient := closed.pumpClient
	closedIncarnation := closed.pumpIncarnation
	closed.pumpMu.Unlock()
	require.Nil(t, closed.publishPumpResumeNeeded(closedClient, closedIncarnation))
	require.False(t, closed.runtimeNeedsResume)
}

func TestSessionPumpDoneWaitsForOwnedFenceClose(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	client := newFakeHermesClient()
	closeEntered := make(chan struct{})
	closeRelease := make(chan struct{})
	client.closeFunc = func(context.Context) error {
		close(closeEntered)
		<-closeRelease

		return nil
	}
	s := testSession(newTestAgent(), client)
	s.releaseProjectionGate()
	s.pumpMu.Lock()
	done := s.pumpDone
	s.pumpMu.Unlock()
	client.deliveries <- nativehermes.TurnDelivery{Err: errors.New("transport ended")}
	<-closeEntered

	select {
	case <-done:
		t.Fatal("pump reported completion while its fence close was live")
	default:
	}
	close(closeRelease)
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("pump did not report completion after its fence close")
	}
	s.detachPump()
}

func TestPumpFenceRecordsAutonomousSettlementFailure(t *testing.T) {
	want := errors.New("terminal snapshot unavailable")
	agent := newTestAgent(WithSessionStore(replaceFailStore{SessionStore: NewInMemorySessionStore(), err: want}))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	agent.setAgentClient(newRecordingAgentClient())
	s := testSession(agent, newFakeHermesClient())
	defer s.stopPump()
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
	started := nativehermes.TurnEvent{Type: nativehermes.EventCycleStarted, CycleID: "autonomous", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity}
	require.NoError(t, s.startAutonomousCycle(t.Context(), started))
	s.resolvePumpRoutes(errors.New("transport ended"), true)
	s.pumpMu.Lock()
	pumpErr := s.pumpErr
	s.pumpMu.Unlock()
	require.ErrorIs(t, pumpErr, want)
}

func TestSessionForegroundAuthorityOrdersAutonomousPromptAndReuse(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	agent := newTestAgent(WithSessionStore(store))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	connection := newRecordingAgentClient()
	agent.setAgentClient(connection)
	s := testSession(agent, client)
	defer s.stopPump()
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
	require.NoError(t, agent.storeStartedSession(s))

	started := nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: "autonomous-a",
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	}
	require.NoError(t, s.startAutonomousCycle(t.Context(), started))
	route := s.routeForEvent(started)
	require.NotNil(t, route)

	s.mu.Lock()
	s.seenParts["preserved-part"] = "preserved-wire"
	s.toolStates["preserved-tool"] = hermesToolState{title: "preserved"}
	s.actionRequests["preserved-action"] = actionRequestOwnership{actionID: "preserved-action"}
	s.foregroundText = []byte("autonomous-prefix")
	s.mu.Unlock()

	updatesBefore := lifecycleUpdateCount(connection)
	replacesBefore := store.replaceCount()

	_, promptErr := s.Prompt(t.Context(), acp.PromptRequest{
		Meta: autonomousPromptMeta("prompt-b"), SessionId: s.id, Prompt: []acp.ContentBlock{acp.TextBlock("must wait")},
	})
	requireSessionForegroundBackpressure(t, promptErr)
	_, _, reuseErr := s.beginReuse(t.Context())
	requireSessionForegroundBackpressure(t, reuseErr)
	_, loadErr := agent.LoadSession(t.Context(), LoadSessionRequest(s.id, s.cwd))
	requireSessionForegroundBackpressure(t, loadErr)
	_, resumeErr := agent.ResumeSession(t.Context(), ResumeSessionRequest(s.id, s.cwd))
	requireSessionForegroundBackpressure(t, resumeErr)

	client.mu.Lock()
	promptSubmits := client.promptCycles
	client.mu.Unlock()
	require.Zero(t, promptSubmits)
	require.Equal(t, updatesBefore, lifecycleUpdateCount(connection))
	require.Equal(t, replacesBefore, store.replaceCount())
	require.Zero(t, client.abortCount())
	require.Zero(t, client.closeCount())
	require.False(t, s.lifecycleStream().fenced())
	s.mu.Lock()
	require.Equal(t, "preserved-wire", s.seenParts["preserved-part"])
	require.Equal(t, "preserved", s.toolStates["preserved-tool"].title)
	require.Equal(t, "preserved-action", s.actionRequests["preserved-action"].actionID)
	require.Equal(t, "autonomous-prefix", string(s.foregroundText))
	s.mu.Unlock()

	require.NoError(t, s.failAutonomousCycle(withPumpRoute(t.Context(), route), route, errors.New("activity failed")))
	_, promptErr = s.Prompt(t.Context(), acp.PromptRequest{
		Meta: autonomousPromptMeta("prompt-c"), SessionId: s.id, Prompt: []acp.ContentBlock{acp.TextBlock("now admitted")},
	})
	require.NoError(t, promptErr)
	_, loadErr = agent.LoadSession(t.Context(), LoadSessionRequest(s.id, s.cwd))
	require.NoError(t, loadErr)
	_, resumeErr = agent.ResumeSession(t.Context(), ResumeSessionRequest(s.id, s.cwd))
	require.NoError(t, resumeErr)
}

type gatedPredispatchRefusal struct {
	*fakeHermesClient
	entered chan struct{}
	release chan struct{}
}

func (s gatedPredispatchRefusal) SendMessage(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
	close(s.entered)
	<-s.release

	return nativehermes.NativeMessage{}, nativehermes.ErrGatewayAmbiguousTurn
}

func TestSessionPumpPromptReservationDoesNotBlockOlderAutonomousProjection(t *testing.T) {
	base := newFakeHermesClient()
	entered := make(chan struct{})
	release := make(chan struct{})
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	connection := newRecordingAgentClient()
	agent.setAgentClient(connection)
	s := testSession(agent, base)
	s.client = gatedPredispatchRefusal{fakeHermesClient: base, entered: entered, release: release}
	defer s.stopPump()
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))

	promptDone := make(chan error, 1)
	go func() {
		_, err := s.Prompt(t.Context(), acp.PromptRequest{
			Meta: autonomousPromptMeta("prompt-first"), SessionId: s.id, Prompt: []acp.ContentBlock{acp.TextBlock("prompt")},
		})
		promptDone <- err
	}()
	<-entered

	started := nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: "activity-after-refusal",
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	}
	projected := make(chan error, 1)
	base.emitEvent(started)
	base.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleFailed, CycleID: started.CycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Err: errors.New("activity failed"), ProjectionDone: func(err error) { projected <- err },
	})
	require.NoError(t, s.synchronizePump(t.Context()))
	s.pumpMu.Lock()
	deferred := len(s.pumpDeferred)
	pumpErr := s.pumpErr
	pumpDone := s.pumpDone
	s.pumpMu.Unlock()
	require.Zero(t, deferred)
	require.NoError(t, pumpErr)
	require.Nil(t, s.routeForEvent(started))
	select {
	case <-pumpDone:
		t.Fatal("ordinary foreground contention terminalized the pump")
	case err := <-projected:
		require.NoError(t, err)
	case <-t.Context().Done():
		t.Fatal("prompt reservation blocked an older autonomous projection")
	}

	close(release)
	require.Error(t, <-promptDone)
	require.NoError(t, s.synchronizePump(t.Context()))
	require.Nil(t, s.routeForEvent(started))
	s.mu.Lock()
	require.Nil(t, s.foreground)
	s.mu.Unlock()
	s.pumpMu.Lock()
	require.Empty(t, s.pumpDeferred)
	require.NoError(t, s.pumpErr)
	s.pumpMu.Unlock()

	connection.mu.Lock()
	events := lifecycleEvents(append([]acp.SessionNotification(nil), connection.updates...))
	connection.mu.Unlock()
	running := 0
	idle := 0
	accepted := 0
	for _, event := range events {
		switch event["type"] {
		case string(lifecycle.EventPromptAccepted):
			accepted++
		case string(lifecycle.EventStateUpdate):
			if event["cause"] != string(lifecycle.CauseActivity) {
				continue
			}
			if event["state"] == string(lifecycle.ForegroundRunning) {
				running++
			}
			if event["state"] == string(lifecycle.ForegroundIdle) {
				idle++
			}
		}
	}
	require.Zero(t, accepted)
	require.Equal(t, 1, running)
	require.Equal(t, 1, idle)
}

func TestSessionPumpDeferredDistinctCyclesPreserveSourceOrder(t *testing.T) {
	base := newFakeHermesClient()
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	connection := newRecordingAgentClient()
	agent.setAgentClient(connection)
	s := testSession(agent, base)
	defer s.stopPump()
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
	_, releaseReuse, err := s.beginReuse(t.Context())
	require.NoError(t, err)

	projectionOrder := make(chan string, 2)
	var projectionA atomic.Int64
	var projectionB atomic.Int64
	emitCycle := func(cycleID string, count *atomic.Int64) {
		base.emitEvent(nativehermes.TurnEvent{
			Type: nativehermes.EventCycleStarted, CycleID: cycleID,
			TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		})
		base.emitEvent(nativehermes.TurnEvent{
			Type: nativehermes.EventCycleFailed, CycleID: cycleID,
			TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
			Err: errors.New("activity failed"), ProjectionDone: func(err error) {
				count.Add(1)
				if err != nil {
					projectionOrder <- cycleID + ": " + err.Error()

					return
				}
				projectionOrder <- cycleID
			},
		})
	}

	emitCycle("activity-a", &projectionA)
	require.NoError(t, s.synchronizePump(t.Context()))

	barrierAccepted := make(chan struct{})
	releaseBarrier := make(chan struct{})
	s.afterPumpBarrierAccept = func() {
		close(barrierAccepted)
		<-releaseBarrier
	}
	barrierDone := make(chan error, 1)
	go func() { barrierDone <- s.synchronizePump(t.Context()) }()
	<-barrierAccepted

	emitCycle("activity-b", &projectionB)
	releaseReuse()
	close(releaseBarrier)
	require.NoError(t, <-barrierDone)
	s.afterPumpBarrierAccept = nil

	require.Equal(t, "activity-a", <-projectionOrder)
	require.Equal(t, "activity-b", <-projectionOrder)
	require.NoError(t, s.synchronizePump(t.Context()))
	require.Equal(t, int64(1), projectionA.Load())
	require.Equal(t, int64(1), projectionB.Load())

	connection.mu.Lock()
	events := lifecycleEvents(append([]acp.SessionNotification(nil), connection.updates...))
	connection.mu.Unlock()
	states := make([]string, 0, 4)
	for _, event := range events {
		if event["type"] == string(lifecycle.EventStateUpdate) && event["cause"] == string(lifecycle.CauseActivity) {
			state, ok := event["state"].(string)
			require.True(t, ok)
			states = append(states, state)
		}
	}
	require.Equal(t, []string{
		string(lifecycle.ForegroundRunning), string(lifecycle.ForegroundIdle),
		string(lifecycle.ForegroundRunning), string(lifecycle.ForegroundIdle),
	}, states)
	s.pumpMu.Lock()
	require.NoError(t, s.pumpErr)
	require.Empty(t, s.pumpDeferred)
	s.pumpMu.Unlock()
	s.mu.Lock()
	require.False(t, s.runtimeNeedsResume)
	s.mu.Unlock()
	require.Zero(t, base.closeCount())
	require.False(t, s.lifecycleStream().fenced())
}

func TestSessionPumpRawDeliveryWaitsBehindDeferredCycle(t *testing.T) {
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	agent.setAgentClient(newRecordingAgentClient())
	client := newFakeHermesClient()
	s := testSession(agent, client)
	defer s.stopPump()
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
	s.rawMessages = rawMessageConfig{enabled: true}
	_, releaseReuse, err := s.beginReuse(t.Context())
	require.NoError(t, err)

	type projectionResult struct {
		name string
		err  error
	}
	projected := make(chan projectionResult, 2)
	cycleID := "activity-before-raw"
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: cycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleFailed, CycleID: cycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Err: errors.New("activity failed"), ProjectionDone: func(err error) {
			projected <- projectionResult{name: "activity", err: err}
		},
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventGatewayRaw, Raw: json.RawMessage(`{"type":"gateway.note"}`),
		ProjectionDone: func(err error) {
			projected <- projectionResult{name: "raw", err: err}
		},
	})
	require.NoError(t, s.synchronizePump(t.Context()))
	select {
	case item := <-projected:
		t.Fatalf("projection crossed held foreground: %#v", item)
	default:
	}

	releaseReuse()
	activityResult := <-projected
	require.Equal(t, "activity", activityResult.name)
	require.NoError(t, activityResult.err)
	rawResult := <-projected
	require.Equal(t, "raw", rawResult.name)
	require.NoError(t, rawResult.err)
	require.NoError(t, s.synchronizePump(t.Context()))
	s.pumpMu.Lock()
	require.NoError(t, s.pumpErr)
	require.Empty(t, s.pumpDeferred)
	s.pumpMu.Unlock()
	require.False(t, s.lifecycleStream().fenced())
}

func TestSessionPumpTerminalSourceItemsWaitBehindDeferredCycle(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		queueTerminal func(*fakeHermesClient, chan error)
		hasProjection bool
	}{
		{
			name: "delivery error",
			queueTerminal: func(client *fakeHermesClient, _ chan error) {
				client.emitError(errors.New("wire ended"))
			},
		},
		{
			name: "source EOF",
			queueTerminal: func(client *fakeHermesClient, _ chan error) {
				close(client.deliveries)
			},
		},
		{
			name: "event without identity",
			queueTerminal: func(client *fakeHermesClient, projected chan error) {
				client.emitEvent(nativehermes.TurnEvent{
					Type:           evtMessagePartUpdated,
					ProjectionDone: func(err error) { projected <- err },
				})
			},
			hasProjection: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
			agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
			agent.setAgentClient(newRecordingAgentClient())
			client := newFakeHermesClient()
			s := testSession(agent, client)
			require.NoError(t, s.openLifecycleStream())
			require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
			_, releaseReuse, err := s.beginReuse(t.Context())
			require.NoError(t, err)

			activityProjected := make(chan error, 1)
			cycleID := "activity-before-terminal-source-item"
			client.emitEvent(nativehermes.TurnEvent{
				Type: nativehermes.EventCycleStarted, CycleID: cycleID,
				TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
			})
			client.emitEvent(nativehermes.TurnEvent{
				Type: nativehermes.EventCycleFailed, CycleID: cycleID,
				TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
				Err: errors.New("activity failed"), ProjectionDone: func(err error) {
					activityProjected <- err
				},
			})
			terminalProjected := make(chan error, 1)
			testCase.queueTerminal(client, terminalProjected)
			require.NoError(t, s.synchronizePump(t.Context()))
			s.pumpMu.Lock()
			done := s.pumpDone
			s.pumpMu.Unlock()
			select {
			case <-done:
				t.Fatal("terminal source item overtook the deferred cycle")
			case err := <-activityProjected:
				t.Fatalf("activity projected across held foreground: %v", err)
			default:
			}

			releaseReuse()
			require.NoError(t, <-activityProjected)
			if testCase.hasProjection {
				require.Error(t, <-terminalProjected)
			}
			select {
			case <-done:
			case <-t.Context().Done():
				t.Fatal("terminal source item did not stop the pump after the deferred prefix")
			}
			s.pumpMu.Lock()
			require.Error(t, s.pumpErr)
			require.Empty(t, s.pumpDeferred)
			s.pumpMu.Unlock()
			require.Equal(t, 1, client.closeCount())
			require.True(t, s.lifecycleStream().fenced())
		})
	}
}

func requireSessionForegroundBackpressure(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, valBackpressure, data[jsonFieldError])
	require.Equal(t, "session_foreground", data[keyLimit])
}

func TestSessionPumpCloseResolvesDeferredAutonomousProjectionOnce(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	connection := newRecordingAgentClient()
	agent.setAgentClient(connection)
	base := newFakeHermesClient()
	s := testSession(agent, base)
	s.client = treeInventoryServer{fakeHermesClient: base, vacant: true}
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))

	reuseCtx, releaseReuse, err := s.beginReuse(t.Context())
	require.NoError(t, err)
	type projectionResult struct {
		cycleID string
		err     error
	}
	projected := make(chan projectionResult, 4)
	var projectionA atomic.Int64
	var projectionB atomic.Int64
	emitCycle := func(cycleID string, count *atomic.Int64) {
		base.emitEvent(nativehermes.TurnEvent{
			Type: nativehermes.EventCycleStarted, CycleID: cycleID,
			TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		})
		base.emitEvent(nativehermes.TurnEvent{
			Type: nativehermes.EventCycleFailed, CycleID: cycleID,
			TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
			Err: errors.New("activity failed"), ProjectionDone: func(err error) {
				count.Add(1)
				projected <- projectionResult{cycleID: cycleID, err: err}
			},
		})
	}
	emitCycle("deferred-a-at-close", &projectionA)
	emitCycle("deferred-b-at-close", &projectionB)
	require.NoError(t, s.synchronizePump(t.Context()))
	s.pumpMu.Lock()
	pumpDone := s.pumpDone
	s.pumpMu.Unlock()

	closed := make(chan error, 1)
	go func() { closed <- s.Close(t.Context()) }()
	<-reuseCtx.Done()
	<-pumpDone
	first := <-projected
	second := <-projected
	require.Equal(t, "deferred-a-at-close", first.cycleID)
	require.Error(t, first.err)
	require.Equal(t, "deferred-b-at-close", second.cycleID)
	require.Error(t, second.err)
	releaseReuse()
	require.NoError(t, <-closed)
	require.Equal(t, int64(1), projectionA.Load())
	require.Equal(t, int64(1), projectionB.Load())
	select {
	case extra := <-projected:
		t.Fatalf("deferred projection resolved twice: %v", extra)
	default:
	}
	require.Equal(t, 1, base.closeCount())

	connection.mu.Lock()
	events := lifecycleEvents(append([]acp.SessionNotification(nil), connection.updates...))
	connection.mu.Unlock()
	for _, event := range events {
		require.False(t,
			event["type"] == string(lifecycle.EventStateUpdate) && event["cause"] == string(lifecycle.CauseActivity),
			"deferred activity projected across close: %#v", event,
		)
	}
}

func TestSessionPumpDeferredQueueOverflowsOnlyAtCapacity(t *testing.T) {
	for _, sameCycle := range []bool{false, true} {
		name := "distinct cycles"
		if sameCycle {
			name = "one cycle"
		}
		t.Run(name, func(t *testing.T) {
			agent := newTestAgent()
			agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
			agent.setAgentClient(newRecordingAgentClient())
			client := newFakeHermesClient()
			client.deliveries = make(chan nativehermes.TurnDelivery, sessionPumpBacklogCapacity+2)
			s := testSession(agent, client)
			require.NoError(t, s.openLifecycleStream())
			require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
			_, releaseReuse, err := s.beginReuse(t.Context())
			require.NoError(t, err)

			projected := make(chan error, 2)
			var projectionCount atomic.Int64
			for index := 0; index <= sessionPumpBacklogCapacity; index++ {
				cycleID := fmt.Sprintf("deferred-%d", index)
				eventType := nativehermes.EventCycleStarted
				if sameCycle {
					cycleID = "deferred-cycle"
					if index > 0 {
						eventType = evtMessagePartUpdated
					}
				}
				event := nativehermes.TurnEvent{
					Type: eventType, CycleID: cycleID,
					TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
				}
				if index == sessionPumpBacklogCapacity {
					event.ProjectionDone = func(err error) {
						projectionCount.Add(1)
						projected <- err
					}
				}
				client.emitEvent(event)
			}
			require.ErrorIs(t, s.synchronizePump(t.Context()), ErrSessionPumpOverflow)
			require.ErrorIs(t, <-projected, ErrSessionPumpOverflow)
			releaseReuse()
			s.pumpMu.Lock()
			done := s.pumpDone
			s.pumpMu.Unlock()
			select {
			case <-done:
			case <-t.Context().Done():
				t.Fatal("overflowed deferred queue did not fence its generation")
			}
			s.pumpMu.Lock()
			require.ErrorIs(t, s.pumpErr, ErrSessionPumpOverflow)
			require.Empty(t, s.pumpDeferred)
			s.pumpMu.Unlock()
			require.Equal(t, int64(1), projectionCount.Load())
			select {
			case extra := <-projected:
				t.Fatalf("overflow projection resolved twice: %v", extra)
			default:
			}
			require.Equal(t, 1, client.closeCount())
			require.True(t, s.lifecycleStream().fenced())
		})
	}
}

func TestSessionPumpDeferredGenerationLossFailsClosed(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	agent.setAgentClient(newRecordingAgentClient())
	client := newFakeHermesClient()
	s := testSession(agent, client)
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
	_, releaseReuse, err := s.beginReuse(t.Context())
	require.NoError(t, err)

	type projectionResult struct {
		cycleID string
		err     error
	}
	projected := make(chan projectionResult, 4)
	var projectionA atomic.Int64
	var projectionB atomic.Int64
	cycleID := "generation-a"
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: cycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleFailed, CycleID: cycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
		Err: errors.New("activity failed"), ProjectionDone: func(err error) {
			projectionA.Add(1)
			projected <- projectionResult{cycleID: "generation-a", err: err}
		},
	})
	cycleID = "generation-b"
	client.emitEvent(nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, CycleID: cycleID,
		TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity,
	})
	client.emitEvent(nativehermes.TurnEvent{
		Type: evtMessagePartUpdated, CycleID: cycleID,
		TransportGeneration: 2, Origin: nativehermes.CycleOriginActivity,
		ProjectionDone: func(err error) {
			projectionB.Add(1)
			projected <- projectionResult{cycleID: "generation-b", err: err}
		},
	})
	require.NoError(t, s.synchronizePump(t.Context()))
	barrierAccepted := make(chan struct{})
	releaseBarrier := make(chan struct{})
	s.afterPumpBarrierAccept = func() {
		close(barrierAccepted)
		<-releaseBarrier
	}
	barrierDone := make(chan error, 1)
	go func() { barrierDone <- s.synchronizePump(t.Context()) }()
	<-barrierAccepted
	releaseReuse()
	close(releaseBarrier)
	require.NoError(t, <-barrierDone)
	s.afterPumpBarrierAccept = nil

	s.pumpMu.Lock()
	done := s.pumpDone
	s.pumpMu.Unlock()
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("deferred generation loss did not stop the pump")
	}
	first := <-projected
	second := <-projected
	require.Equal(t, "generation-a", first.cycleID)
	require.NoError(t, first.err)
	require.Equal(t, "generation-b", second.cycleID)
	require.Error(t, second.err)
	require.Equal(t, int64(1), projectionA.Load())
	require.Equal(t, int64(1), projectionB.Load())
	select {
	case extra := <-projected:
		t.Fatalf("generation-loss projection resolved twice: %v", extra)
	default:
	}
	s.pumpMu.Lock()
	require.Error(t, s.pumpErr)
	require.Empty(t, s.pumpDeferred)
	s.pumpMu.Unlock()
	require.Equal(t, 1, client.closeCount())
	require.True(t, s.lifecycleStream().fenced())
}

func TestPumpContainmentCertifiesVacancyBeforeFencingStream(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	connection := newRecordingAgentClient()
	agent.setAgentClient(connection)
	base := newFakeHermesClient()
	s := testSession(agent, base)
	s.stopPump()
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))

	inventory := treeInventoryServer{fakeHermesClient: base, vacant: true}
	var current nativehermes.Server = inventory
	s.mu.Lock()
	s.client = current
	s.mu.Unlock()
	s.pumpMu.Lock()
	s.pumpClient = current
	s.pumpStopping = false
	incarnation := s.pumpIncarnation
	s.pumpMu.Unlock()
	resumeWait := s.publishPumpResumeNeeded(current, incarnation)
	require.NotNil(t, resumeWait)
	s.fencePumpClient(current, incarnation, errors.New("transport ended"), resumeWait)
	select {
	case <-resumeWait:
	case <-t.Context().Done():
		t.Fatal("containment certification did not publish its result")
	}
	require.NoError(t, s.runtimeResumeErr)
	require.True(t, s.lifecycleStream().fenced())
}

func TestSessionPumpStopsOnClosedStreams(t *testing.T) {
	agent := newTestAgent()
	client := newFakeHermesClient()
	s := testSession(agent, client)
	close(client.deliveries)
	s.pumpMu.Lock()
	done := s.pumpDone
	s.pumpMu.Unlock()
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("pump did not observe closed delivery stream")
	}
	s.detachPump()

	client = newFakeHermesClient()
	s = &session{
		agent:            newTestAgent(),
		projectionGate:   make(chan struct{}),
		pumpRoutes:       map[string]*pumpCycleRoute{},
		pumpBarriers:     make(chan pumpBarrierRequest),
		pumpIncarnation:  1,
		actionRequests:   map[string]actionRequestOwnership{},
		seenParts:        map[string]string{},
		activeMessageIDs: map[string]struct{}{},
		toolStates:       map[string]hermesToolState{},
	}
	done = make(chan struct{})
	go s.runPump(t.Context(), t.Context(), done, client, 1)
	close(client.deliveries)
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("pre-snapshot pump did not stop after delivery EOF")
	}
}

func TestSessionPumpHandlesAutonomousFailureItem(t *testing.T) {
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	agent.setAgentClient(newRecordingAgentClient())
	s := testSession(agent, newFakeHermesClient())
	defer s.stopPump()
	require.NoError(t, s.openLifecycleStream())
	require.NoError(t, s.lifecycleStream().ensureLifecycleOpened(t.Context()))
	started := nativehermes.TurnEvent{Type: nativehermes.EventCycleStarted, CycleID: "failed", TransportGeneration: 1, Origin: nativehermes.CycleOriginActivity}
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &started}))
	failed := nativehermes.TurnEvent{Type: nativehermes.EventCycleFailed, CycleID: started.CycleID, TransportGeneration: 1}
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &failed}))
}

func TestSessionPumpBarrierFailureStopsIncarnation(t *testing.T) {
	gate := make(chan struct{})
	close(gate)
	s := &session{
		agent:            newTestAgent(),
		projectionGate:   gate,
		pumpBarriers:     make(chan pumpBarrierRequest),
		pumpRoutes:       map[string]*pumpCycleRoute{},
		pumpIncarnation:  1,
		pumpStopping:     false,
		actionRequests:   map[string]actionRequestOwnership{},
		seenParts:        map[string]string{},
		activeMessageIDs: map[string]struct{}{},
		toolStates:       map[string]hermesToolState{},
	}
	client := newFakeHermesClient()
	client.deliveries = make(chan nativehermes.TurnDelivery, 2)
	done := make(chan struct{})
	s.pumpDone = done
	go s.runPump(t.Context(), t.Context(), done, client, 1)
	event := nativehermes.TurnEvent{Type: evtMessagePartUpdated, CycleID: "missing", TransportGeneration: 1}
	client.deliveries <- nativehermes.TurnDelivery{Event: &event}
	require.Error(t, s.synchronizePump(t.Context()))
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("barrier failure did not stop pump")
	}
}

type panicDeliveriesServer struct{ nativehermes.Server }

func (panicDeliveriesServer) Deliveries() <-chan nativehermes.TurnDelivery { panic("delivery panic") }

func TestPumpDeferredFailureBarriers(t *testing.T) {
	newBarePump := func() *session {
		gate := make(chan struct{})
		close(gate)

		return &session{
			agent:            newTestAgent(),
			projectionGate:   gate,
			pumpBarriers:     make(chan pumpBarrierRequest),
			pumpRoutes:       map[string]*pumpCycleRoute{},
			pumpIncarnation:  1,
			actionRequests:   map[string]actionRequestOwnership{},
			seenParts:        map[string]string{},
			activeMessageIDs: map[string]struct{}{},
			toolStates:       map[string]hermesToolState{},
		}
	}

	t.Run("reader panic", func(t *testing.T) {
		s := newBarePump()
		done := make(chan struct{})
		s.pumpDone = done
		go s.runPump(t.Context(), t.Context(), done, panicDeliveriesServer{Server: newFakeHermesClient()}, 1)
		<-done
		require.Error(t, s.pumpErr)
	})

	t.Run("active delivery EOF", func(t *testing.T) {
		s := newBarePump()
		client := newFakeHermesClient()
		done := make(chan struct{})
		s.pumpDone = done
		go s.runPump(t.Context(), t.Context(), done, client, 1)
		require.NoError(t, s.synchronizePump(t.Context()))
		close(client.deliveries)
		<-done
		require.Error(t, s.pumpErr)
	})

	for _, test := range []struct {
		name  string
		panic bool
	}{
		{name: "barrier drain error"},
		{name: "accepted barrier panic", panic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newBarePump()
			s.projectionGate = make(chan struct{})
			client := newFakeHermesClient()
			client.deliveries = make(chan nativehermes.TurnDelivery, 1)
			event := nativehermes.TurnEvent{
				Type: "mapped-without-route", CycleID: "missing", TransportGeneration: 1,
			}
			s.afterPumpBarrierAccept = func() {
				if test.panic {
					panic("accepted barrier panic")
				}
				client.deliveries <- nativehermes.TurnDelivery{Event: &event}
			}
			done := make(chan struct{})
			s.pumpDone = done
			barrier := pumpBarrierRequest{reply: make(chan error, 1)}
			barrierSent := make(chan struct{})
			go func() {
				s.pumpBarriers <- barrier
				close(barrierSent)
			}()
			go s.runPump(t.Context(), t.Context(), done, client, 1)
			close(s.projectionGate)
			<-barrierSent
			require.Error(t, <-barrier.reply)
			<-done
		})
	}

	t.Run("pre-open backlog projections", func(t *testing.T) {
		s := newBarePump()
		s.projectionGate = make(chan struct{})
		client := newFakeHermesClient()
		client.deliveries = make(chan nativehermes.TurnDelivery, sessionPumpBacklogCapacity+1)
		projected := make(chan error, sessionPumpBacklogCapacity+1)
		for index := 0; index <= sessionPumpBacklogCapacity; index++ {
			event := nativehermes.TurnEvent{ProjectionDone: func(err error) { projected <- err }}
			client.deliveries <- nativehermes.TurnDelivery{Event: &event}
		}
		done := make(chan struct{})
		s.pumpDone = done
		go s.runPump(t.Context(), t.Context(), done, client, 1)
		<-done
		for index := 0; index <= sessionPumpBacklogCapacity; index++ {
			require.ErrorIs(t, <-projected, ErrSessionPumpOverflow)
		}
	})
}

func TestAutonomousStartDefersWithoutWaitingForForegroundSettlement(t *testing.T) {
	s := testSession(newTestAgent(), newFakeHermesClient())
	defer s.stopPump()
	settlement := &turnSettlement{done: make(chan struct{})}
	s.pumpMu.Lock()
	s.pumpForegroundSettle = settlement
	s.pumpMu.Unlock()
	event := nativehermes.TurnEvent{
		Type: nativehermes.EventCycleStarted, Origin: nativehermes.CycleOriginActivity,
		CycleID: "activity", TransportGeneration: 1,
	}
	projected := make(chan error, 1)
	event.ProjectionDone = func(err error) { projected <- err }
	require.NoError(t, s.handlePumpItem(t.Context(), pumpItem{event: &event}))
	select {
	case err := <-projected:
		t.Fatalf("deferred projection completed early: %v", err)
	default:
	}
	s.pumpMu.Lock()
	require.Len(t, s.pumpDeferred, 1)
	s.pumpMu.Unlock()
	require.Error(t, s.startAutonomousCycle(t.Context(), nativehermes.TurnEvent{}))
}

func TestSessionPumpFailsBufferedPreSnapshotTransportError(t *testing.T) {
	gate := make(chan struct{})
	s := &session{
		agent:            newTestAgent(),
		projectionGate:   gate,
		pumpBarriers:     make(chan pumpBarrierRequest),
		pumpRoutes:       map[string]*pumpCycleRoute{},
		pumpIncarnation:  1,
		actionRequests:   map[string]actionRequestOwnership{},
		seenParts:        map[string]string{},
		activeMessageIDs: map[string]struct{}{},
		toolStates:       map[string]hermesToolState{},
	}
	client := newFakeHermesClient()
	client.deliveries = make(chan nativehermes.TurnDelivery)
	done := make(chan struct{})
	go s.runPump(t.Context(), t.Context(), done, client, 1)
	sent := make(chan struct{})
	go func() {
		client.emitError(errors.New("pre-snapshot transport failed"))
		close(sent)
	}()
	<-sent
	close(gate)
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("pump did not fail the buffered transport error")
	}
	s.pumpMu.Lock()
	pumpErr := s.pumpErr
	s.pumpMu.Unlock()
	require.ErrorContains(t, errors.Unwrap(pumpErr), "pre-snapshot transport failed")
}

func TestSessionPumpContinuesAfterNilMappedError(t *testing.T) {
	s := testSession(newTestAgent(), newFakeHermesClient())
	defer s.stopPump()
	require.NoError(t, s.synchronizePump(t.Context()))
	client, ok := s.client.(*fakeHermesClient)
	require.True(t, ok)
	client.emitError(nil)
	require.NoError(t, s.synchronizePump(t.Context()))
}

func TestCloseWaitsForPublishedPumpContainmentOutcome(t *testing.T) {
	t.Run("caller deadline", func(t *testing.T) {
		s := testSession(newTestAgent(), newFakeHermesClient())
		wait := make(chan struct{})
		s.mu.Lock()
		s.runtimeNeedsResume = true
		s.runtimeResumeWait = wait
		s.mu.Unlock()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, s.closeLocked(ctx, false), context.Canceled)
		close(wait)
		s.stopPump()
	})

	t.Run("containment failure", func(t *testing.T) {
		s := testSession(newTestAgent(), newFakeHermesClient())
		wait := make(chan struct{})
		close(wait)
		want := errors.New("containment failed")
		s.mu.Lock()
		s.runtimeNeedsResume = true
		s.runtimeResumeWait = wait
		s.runtimeResumeErr = want
		s.mu.Unlock()
		require.ErrorIs(t, s.closeLocked(t.Context(), false), want)
		s.stopPump()
	})
}
