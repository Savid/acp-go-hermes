package hermesacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func lifecycleOffer(versions ...any) map[string]any {
	return map[string]any{lifecycle.MetaKey: map[string]any{"versions": versions}}
}

func TestLifecycleNegotiationAndReservedMetadata(t *testing.T) {
	weak := newTestAgent()
	response, err := weak.Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Nil(t, response.Meta)
	require.False(t, weak.negotiatedLifecycle().Present())

	response, err = weak.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(2)})
	require.NoError(t, err)
	require.Nil(t, response.Meta)
	require.False(t, weak.negotiatedLifecycle().Present())

	response, err = weak.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	advertisement, ok := response.Meta[lifecycle.MetaKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, []int{1}, advertisement["versions"])
	require.Equal(t, true, advertisement["updatesOutsidePrompt"])
	require.Equal(t, false, advertisement["authoritativeQuiescence"])
	require.Equal(t, []string{}, advertisement["activityKinds"])

	authoritative := newTestAgent()
	authoritative.containmentMode = RuntimeContainmentAuthoritative
	response, err = authoritative.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	advertisement, ok = response.Meta[lifecycle.MetaKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, advertisement["authoritativeQuiescence"])
	require.Equal(t, string(lifecycle.ProofClassProcessContainment), advertisement["quiescenceSource"])

	_, err = weak.Initialize(t.Context(), acp.InitializeRequest{
		Meta: map[string]any{lifecycle.MetaKey: "v1"},
	})
	require.Error(t, err)

	reserved := map[string]any{lifecycle.MetaKey: map[string]any{}}
	_, err = weak.Authenticate(t.Context(), acp.AuthenticateRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.Logout(t.Context(), acp.LogoutRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.SetSessionMode(t.Context(), acp.SetSessionModeRequest{Meta: reserved})
	require.Error(t, err)

	_, err = weak.HandleExtensionMethod(t.Context(), "unknown", json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`))
	require.Error(t, err)
	_, err = weak.HandleExtensionMethod(t.Context(), "unknown", json.RawMessage(`{`))
	require.Error(t, err)

	_, err = weak.NewSession(t.Context(), acp.NewSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.loadOrResumeSession(t.Context(), "session", "", nil, nil, reserved)
	require.Error(t, err)
	_, err = weak.ListSessions(t.Context(), acp.ListSessionsRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.CloseSession(t.Context(), acp.CloseSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{Meta: reserved})
	require.Error(t, err)
}

func TestSessionLifecycleStreamReducesCompleteTurn(t *testing.T) {
	negotiated := lifecycle.Negotiated{
		Versions:                []int{lifecycle.Version},
		UpdatesOutsidePrompt:    true,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	}
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(negotiated)
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	stream := session.lifecycleStream()
	require.NotEmpty(t, stream.streamID())
	require.Empty(t, stream.turnIdentity())

	require.NoError(t, stream.ensureLifecycleOpened(t.Context()))
	require.NoError(t, stream.ensureLifecycleOpened(t.Context()))
	require.NoError(t, stream.accept(t.Context(), lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce", RunID: "run",
	}))
	require.NotEmpty(t, stream.turnIdentity())

	permission, permissionMeta, ok := stream.reserveAction(lifecycle.ActionPermission)
	require.True(t, ok)
	require.NotEmpty(t, permissionMeta)
	elicitation, _, ok := stream.reserveAction(lifecycle.ActionElicitation)
	require.True(t, ok)
	require.NoError(t, stream.announceAction(t.Context(), permission))
	require.NoError(t, stream.announceAction(t.Context(), elicitation))
	require.NoError(t, stream.resolveAction(t.Context(), permission.ActionID, lifecycle.ActionAccepted))
	require.NoError(t, stream.resolveAction(t.Context(), elicitation.ActionID, lifecycle.ActionDeclined))

	remaining, _, ok := stream.reserveAction(lifecycle.ActionPermission)
	require.True(t, ok)
	require.NoError(t, stream.announceAction(t.Context(), remaining))
	require.NoError(t, stream.terminalizeBlockers(t.Context()))
	require.NoError(t, stream.terminalizeBlockers(t.Context()))
	require.NoError(t, stream.settle(t.Context(), lifecycleTurnOutcome{
		stopReason: string(acp.StopReasonEndTurn), outcome: lifecycle.OutcomeSuccess,
	}))
	require.Empty(t, stream.turnIdentity())
	require.NoError(t, stream.certify(t.Context(), "root-1"))

	reducer := lifecycle.NewReducer(lifecycle.Options{Negotiated: negotiated})
	conn.mu.Lock()
	updates := append([]acp.SessionNotification(nil), conn.updates...)
	conn.mu.Unlock()
	for _, update := range updates {
		params, marshalErr := json.Marshal(update)
		require.NoError(t, marshalErr)
		require.NoError(t, reducer.ReduceSessionUpdate(params))
	}

	state := reducer.State()
	require.Len(t, state.Turns, 1)
	require.True(t, state.Turns[0].Terminal)
	require.Equal(t, lifecycle.OutcomeSuccess, state.Turns[0].Outcome)
	require.Len(t, state.Actions, 3)
	for _, action := range state.Actions {
		require.True(t, action.State.Terminal())
	}
	require.True(t, state.Quiescence.Certified)
	require.Equal(t, "hermes-process-tree/root-1", state.Quiescence.Barrier)

	stream.fence()
	require.True(t, stream.fenced())
	require.Error(t, stream.certify(t.Context(), "after-fence"))
}

func TestSessionLifecycleStreamAbsenceAndFailureFences(t *testing.T) {
	var absent *sessionStream
	require.Empty(t, absent.streamID())
	require.Empty(t, absent.turnIdentity())
	require.NoError(t, absent.ensureLifecycleOpened(t.Context()))
	require.NoError(t, absent.accept(t.Context(), lifecycle.Submission{}))
	_, _, ok := absent.reserveAction(lifecycle.ActionPermission)
	require.False(t, ok)
	require.NoError(t, absent.announceAction(t.Context(), lifecycle.ActionUpdate{}))
	require.NoError(t, absent.resolveAction(t.Context(), "", lifecycle.ActionCancelled))
	require.NoError(t, absent.terminalizeBlockers(t.Context()))
	require.NoError(t, absent.settle(t.Context(), lifecycleTurnOutcome{}))
	require.NoError(t, absent.certify(t.Context(), ""))
	absent.fence()
	require.False(t, absent.fenced())

	agent := newTestAgent()
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	require.Nil(t, session.lifecycleStream())

	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	oldReader := sessionIDRandReader
	t.Cleanup(func() { sessionIDRandReader = oldReader })
	sessionIDRandReader = errorReader{err: errors.New("id failed")}
	require.Error(t, session.openLifecycleStream())
	sessionIDRandReader = oldReader

	require.NoError(t, session.openLifecycleStream())
	stream := session.lifecycleStream()
	_, _, ok = stream.reserveAction(lifecycle.ActionPermission)
	require.False(t, ok)
	require.NoError(t, stream.settle(t.Context(), lifecycleTurnOutcome{}))
	require.NoError(t, stream.certify(t.Context(), "not-authoritative"))
	require.NoError(t, stream.resolveAction(t.Context(), "", lifecycle.ActionCancelled))

	conn := newRecordingAgentClient()
	conn.updateErr = errors.New("delivery failed")
	agent.setAgentClient(conn)
	require.Error(t, stream.ensureLifecycleOpened(t.Context()))
	require.True(t, stream.fenced())
}

func TestLifecycleOpeningIsOrderedAfterResponse(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())

	withoutStream := testSession(newTestAgent(), newFakeHermesClient())
	agent.deferStreamOpen(withoutStream)
	agent.releaseStreamOpens()
	agent.deferStreamOpen(session)
	require.Zero(t, conn.updateCount())
	agent.releaseStreamOpens()
	agent.awaitStreamOpens()
	require.Equal(t, 1, conn.updateCount())

	called := 0
	var output bytes.Buffer
	writer := responseOrderedWriter{writer: &output, written: func() { called++ }}
	written, err := writer.Write([]byte("response"))
	require.NoError(t, err)
	require.Equal(t, len("response"), written)
	require.Equal(t, "response", output.String())
	require.Equal(t, 1, called)

	failing := responseOrderedWriter{writer: failingWriter{}, written: func() { called++ }}
	_, err = failing.Write([]byte("response"))
	require.Error(t, err)
	require.Equal(t, 1, called)
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type lifecycleFailingAgentClient struct {
	*recordingAgentClient
	failAt int
	seen   int
}

func (c *lifecycleFailingAgentClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if _, lifecycleUpdate := notification.Meta[lifecycle.MetaKey]; lifecycleUpdate {
		c.seen++
		if c.seen == c.failAt {
			return errors.New("lifecycle delivery failed")
		}
	}

	return c.recordingAgentClient.SessionUpdate(ctx, notification)
}

func TestLifecycleEmitterViolationFencesStream(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	stream := session.lifecycleStream()
	require.NoError(t, stream.ensureLifecycleOpened(context.Background()))
	require.Error(t, stream.announceAction(context.Background(), lifecycle.ActionUpdate{}))
	require.True(t, stream.fenced())
}

func TestLifecycleStreamDeliveryFailuresStopAtFailedEvent(t *testing.T) {
	negotiated := lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	}

	newStream := func(t *testing.T) (*sessionStream, *recordingAgentClient) {
		t.Helper()
		agent := newTestAgent()
		agent.retainNegotiatedLifecycle(negotiated)
		conn := newRecordingAgentClient()
		agent.setAgentClient(conn)
		session := testSession(agent, newFakeHermesClient())
		require.NoError(t, session.openLifecycleStream())

		return session.lifecycleStream(), conn
	}

	t.Run("opening", func(t *testing.T) {
		stream, conn := newStream(t)
		conn.updateErr = errors.New("delivery failed")
		require.Error(t, stream.accept(t.Context(), lifecycle.Submission{SubmissionID: "submission", ClientNonce: "nonce"}))
		require.True(t, stream.fenced())
	})

	t.Run("acceptance", func(t *testing.T) {
		stream, conn := newStream(t)
		require.NoError(t, stream.ensureLifecycleOpened(t.Context()))
		conn.updateErr = errors.New("delivery failed")
		require.Error(t, stream.accept(t.Context(), lifecycle.Submission{SubmissionID: "submission", ClientNonce: "nonce"}))
		require.True(t, stream.fenced())
	})

	t.Run("resolution", func(t *testing.T) {
		stream, conn := newStream(t)
		require.NoError(t, stream.accept(t.Context(), lifecycle.Submission{SubmissionID: "submission", ClientNonce: "nonce"}))
		action, _, owned := stream.reserveAction(lifecycle.ActionPermission)
		require.True(t, owned)
		require.NoError(t, stream.announceAction(t.Context(), action))
		conn.updateErr = errors.New("delivery failed")
		require.Error(t, stream.resolveAction(t.Context(), action.ActionID, lifecycle.ActionAccepted))
	})

	t.Run("terminalize", func(t *testing.T) {
		stream, conn := newStream(t)
		require.NoError(t, stream.accept(t.Context(), lifecycle.Submission{SubmissionID: "submission", ClientNonce: "nonce"}))
		action, _, owned := stream.reserveAction(lifecycle.ActionPermission)
		require.True(t, owned)
		require.NoError(t, stream.announceAction(t.Context(), action))
		conn.updateErr = errors.New("delivery failed")
		require.Error(t, stream.terminalizeBlockers(t.Context()))
	})

	t.Run("settlement", func(t *testing.T) {
		stream, conn := newStream(t)
		require.NoError(t, stream.accept(t.Context(), lifecycle.Submission{SubmissionID: "submission", ClientNonce: "nonce"}))
		conn.updateErr = errors.New("delivery failed")
		require.Error(t, stream.settle(t.Context(), lifecycleTurnOutcome{
			stopReason: string(acp.StopReasonEndTurn), outcome: lifecycle.OutcomeSuccess,
		}))
	})
}

func TestLifecycleActionRegistrationAndMetadata(t *testing.T) {
	agent := newTestAgent()
	session := testSession(agent, newFakeHermesClient())

	action, owned, err := session.announceBlockingAction(t.Context(), lifecycle.ActionPermission, "request")
	require.NoError(t, err)
	require.True(t, owned)
	require.Empty(t, action.id)
	require.NoError(t, session.resolveBlockingAction(t.Context(), action, lifecycle.ActionAccepted))

	session.registerActionRequest("action", "request")
	require.False(t, session.takeActionRequest("unknown"))
	require.True(t, session.takeActionRequest("action"))
	require.False(t, session.takeActionRequest("action"))
	require.NoError(t, session.resolveBlockingAction(t.Context(), announcedAction{id: "unknown"}, lifecycle.ActionAccepted))

	meta := map[string]any{"vendor": true}
	require.Equal(t, meta, actionMeta(meta, announcedAction{}))
	correlation := map[string]any{"version": 1}
	stamped := actionMeta(nil, announcedAction{correlation: correlation})
	require.Equal(t, correlation, stamped[lifecycle.MetaKey])
	require.Equal(t, lifecycle.ActionCancelled, permissionActionState(acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeCancelled(),
	}, valReject))
	require.Equal(t, lifecycle.ActionDeclined, permissionActionState(acp.RequestPermissionResponse{}, valReject))
	require.Equal(t, lifecycle.ActionAccepted, permissionActionState(acp.RequestPermissionResponse{}, valOnce))
}

func TestLifecycleSettlementValueBoundaries(t *testing.T) {
	var settlement *turnSettlement
	settlement.complete()
	require.NoError(t, settlement.await(t.Context()))

	settlement = &turnSettlement{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, settlement.await(ctx), context.Canceled)
	settlement.complete()
	require.NoError(t, settlement.await(t.Context()))

	native := &stateSnapshotTerminal{MessageID: "message"}
	require.Empty(t, (committedState{}).nativeTerminal().MessageID)
	require.Equal(t, "message", (committedState{native: native}).nativeTerminal().MessageID)

	session := testSession(newTestAgent(), newFakeHermesClient())
	session.recordForegroundPrefix("")
	session.recordForegroundPrefix("prefix")
	session.recordForegroundPrefix(string(bytes.Repeat([]byte("x"), lifecycleForegroundPrefixBytes)))
	session.recordForegroundPrefix("ignored")
	require.Len(t, session.foregroundPrefix(), lifecycleForegroundPrefixBytes)

	require.Nil(t, foregroundOf(nil))
	foreground := &stateSnapshotForeground{TurnID: "turn"}
	require.Same(t, foreground, foregroundOf(&stateSnapshotWrapper{Foreground: foreground}))

	reason, outcome := terminalOutcomeFromHermes("max_turns")
	require.Equal(t, acp.StopReasonMaxTurnRequests, reason)
	require.Equal(t, lifecycle.OutcomeLimit, outcome)
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

type treeInventoryServer struct {
	*fakeHermesClient
	vacant bool
}

func (s treeInventoryServer) ProviderTreeVacant() (bool, bool) { return s.vacant, true }

func TestLifecycleCloseProofAndOpeningFailure(t *testing.T) {
	managed := &managedHermesServer{Server: treeInventoryServer{fakeHermesClient: newFakeHermesClient(), vacant: true}}
	vacant, proved := managed.ProviderTreeVacant()
	require.True(t, vacant)
	require.True(t, proved)

	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	conn.updateErr = errors.New("opening failed")
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	agent.openDeferredStream(session)
	require.True(t, session.lifecycleStream().fenced())
}

func TestStoredLifecycleBoundaryValidation(t *testing.T) {
	valid := func() stateSnapshot {
		return stateSnapshot{
			Archives: map[string]archiveInfo{},
			Terminal: &stateSnapshotTerminal{},
			Wrapper: &stateSnapshotWrapper{Foreground: &stateSnapshotForeground{
				StreamID: "stream", TurnID: "turn", CapturedAtUnixMilli: 1,
				Outcome: string(lifecycle.OutcomeSuccess), StopReason: string(acp.StopReasonEndTurn),
			}},
		}
	}

	for name, mutate := range map[string]func(*stateSnapshot){
		"identity": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.TurnID = ""
		},
		"outcome": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.Outcome = "future"
		},
		"failed stop reason": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.Outcome = string(lifecycle.OutcomeFailed)
		},
		"missing stop reason": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.StopReason = ""
		},
		"unsupported stop reason": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.StopReason = "future"
		},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := valid()
			mutate(&snapshot)
			require.Error(t, validateStateSnapshotRequiredSections(snapshot))
		})
	}

	require.NoError(t, validateStateSnapshotRequiredSections(valid()))
}

func TestLifecycleSnapshotCaptureFailureBoundaries(t *testing.T) {
	t.Run("closed turn", func(t *testing.T) {
		session := testSession(newTestAgent(), newFakeHermesClient())
		session.closed = true
		_, err := session.captureSnapshotLocked(t.Context(), &terminalSnapshotRequirement{})
		require.ErrorContains(t, err, "closed")
	})

	t.Run("todo fallback", func(t *testing.T) {
		client := newFakeHermesClient()
		client.todosErr = errors.New("todos unavailable")
		session := testSession(newTestAgent(), client)
		session.committed.todos = []nativehermes.Todo{{Content: "committed"}}
		commit, err := session.captureSnapshotLocked(t.Context(), nil)
		require.NoError(t, err)
		require.Equal(t, session.committed.todos, commit.todos)
	})

	t.Run("settled archive read", func(t *testing.T) {
		storeErr := errors.New("archive unavailable")
		agent := newTestAgent(WithSessionStore(&errorSessionStore{err: storeErr}))
		session := testSession(agent, newFakeHermesClient())
		requirement := &terminalSnapshotRequirement{
			nativeUnavailable: true,
			settlementCapture: true,
			foreground: stateSnapshotForeground{
				StreamID: "stream", TurnID: "turn", CapturedAtUnixMilli: 1,
				Outcome: string(lifecycle.OutcomeFailed),
			},
		}
		_, err := session.captureSnapshotLocked(t.Context(), requirement)
		require.ErrorIs(t, err, storeErr)
	})

	t.Run("cancel after serialization", func(t *testing.T) {
		session := testSession(newTestAgent(), newFakeHermesClient())
		ctx, cancel := context.WithCancel(t.Context())
		originalMarshal := stateJSONMarshal
		t.Cleanup(func() { stateJSONMarshal = originalMarshal })
		calls := 0
		stateJSONMarshal = func(value any) ([]byte, error) {
			calls++
			encoded, err := json.Marshal(value)
			if calls == 2 {
				cancel()
			}

			return encoded, err
		}

		_, err := session.captureSnapshotLocked(ctx, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func newLifecycleActionSession(t *testing.T, accepted bool) (*session, *recordingAgentClient, context.Context) {
	t.Helper()

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
	session.mu.Lock()
	session.turnInFlight = true
	session.mu.Unlock()
	if accepted {
		require.NoError(t, session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
			SubmissionID: "submission", ClientNonce: "nonce",
		}))
	}

	return session, conn, turnCtx
}

func setLifecycleDeliveryError(conn *recordingAgentClient) {
	conn.mu.Lock()
	conn.updateErr = errors.New("lifecycle delivery failed")
	conn.mu.Unlock()
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

func sessionTurnEpoch(session *session) uint64 {
	session.mu.Lock()
	defer session.mu.Unlock()

	return session.turnEpoch
}

func TestPromptSettlementStopsAtLifecycleDeliveryFailure(t *testing.T) {
	t.Run("blocker terminalization", func(t *testing.T) {
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		action, _, owned := session.lifecycleStream().reserveAction(lifecycle.ActionPermission)
		require.True(t, owned)
		require.NoError(t, session.lifecycleStream().announceAction(turnCtx, action))
		session.agent.setAgentClient(&lifecycleFailingAgentClient{recordingAgentClient: conn, failAt: 1})

		_, published, err := session.settlePrompt(
			t.Context(), turnCtx, sessionTurnEpoch(session), SessionStoreTerminalState{},
			promptRun{settle: true, cancelled: true}, nil,
		)
		require.False(t, published)
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})

	t.Run("terminal idle", func(t *testing.T) {
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		session.agent.setAgentClient(&lifecycleFailingAgentClient{recordingAgentClient: conn, failAt: 1})

		_, published, err := session.settlePrompt(
			t.Context(), turnCtx, sessionTurnEpoch(session), SessionStoreTerminalState{},
			promptRun{settle: true, cancelled: true}, nil,
		)
		require.True(t, published)
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})

	t.Run("quiescence", func(t *testing.T) {
		agent := newTestAgent()
		agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
			Versions:                []int{lifecycle.Version},
			AuthoritativeQuiescence: true,
			QuiescenceSource:        lifecycle.ProofClassProcessContainment,
			ActivityKinds:           []lifecycle.ActivityKind{},
		})
		base := newRecordingAgentClient()
		conn := &lifecycleFailingAgentClient{recordingAgentClient: base, failAt: 5}
		agent.setAgentClient(conn)
		client := newFakeHermesClient()
		session := testSession(agent, client)
		session.client = treeInventoryServer{fakeHermesClient: client, vacant: true}
		require.NoError(t, session.openLifecycleStream())
		turnCtx := session.beginTurn(t.Context(), "turn")
		session.mu.Lock()
		session.turnInFlight = true
		session.mu.Unlock()
		require.NoError(t, session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
			SubmissionID: "submission", ClientNonce: "nonce",
		}))

		_, published, err := session.settlePrompt(
			t.Context(), turnCtx, sessionTurnEpoch(session), SessionStoreTerminalState{},
			promptRun{settle: true, cancelled: true, endsIncarnation: true}, nil,
		)
		require.True(t, published)
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})
}

func TestLifecycleRunFailureClassification(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	acceptErr := errors.New("acceptance failed")
	run := session.nativeRun(t.Context(), nativehermes.NativeMessage{}, nil, acceptErr)
	require.ErrorIs(t, run.err, acceptErr)
	require.True(t, run.endsIncarnation)

	run = session.nativeRun(t.Context(), nativehermes.NativeMessage{}, nativehermes.ErrGatewayDisconnected, nil)
	require.Error(t, run.err)
	require.True(t, run.endsIncarnation)

	client := newFakeHermesClient()
	client.closeErr = errors.New("containment failed")
	session = testSession(newTestAgent(), client)
	session.beginTurn(t.Context(), "turn")
	run = session.unacceptedCancel(sessionTurnEpoch(session), nil)
	require.ErrorContains(t, run.err, "containment failed")
}

func TestClosedBoundaryStopsAtFirstFailedRung(t *testing.T) {
	t.Run("terminalization", func(t *testing.T) {
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		action, _, owned := session.lifecycleStream().reserveAction(lifecycle.ActionPermission)
		require.True(t, owned)
		require.NoError(t, session.lifecycleStream().announceAction(turnCtx, action))
		session.agent.setAgentClient(&lifecycleFailingAgentClient{recordingAgentClient: conn, failAt: 1})
		err := session.publishClosedBoundary(t.Context(), session.lifecycleStream(), nil, containmentProof{})
		require.ErrorContains(t, err, "lifecycle delivery failed")
	})

	t.Run("publication", func(t *testing.T) {
		storeErr := errors.New("publication failed")
		session := testSession(newTestAgent(WithSessionStore(&errorSessionStore{err: storeErr})), newFakeHermesClient())
		commit := &sessionStoreCommit{
			mainKey: SessionKey{SessionID: string(session.id), Subpath: SessionStoreMainSubpath},
			replacements: []SessionStoreReplacement{{
				Key: SessionKey{SessionID: string(session.id), Subpath: SessionStoreMainSubpath},
			}},
			deadline: time.Second,
		}
		err := session.publishClosedBoundary(t.Context(), nil, commit, containmentProof{})
		require.Error(t, err)
	})

	t.Run("unproved vacancy", func(t *testing.T) {
		session := testSession(newTestAgent(), newFakeHermesClient())
		require.NoError(t, session.publishClosedBoundary(t.Context(), nil, nil, containmentProof{}))
	})

	t.Run("proved vacancy", func(t *testing.T) {
		agent := newTestAgent()
		agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
			Versions:                []int{lifecycle.Version},
			AuthoritativeQuiescence: true,
			QuiescenceSource:        lifecycle.ProofClassProcessContainment,
			ActivityKinds:           []lifecycle.ActivityKind{},
		})
		conn := newRecordingAgentClient()
		agent.setAgentClient(conn)
		session := testSession(agent, newFakeHermesClient())
		require.NoError(t, session.openLifecycleStream())
		require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
		require.NoError(t, session.publishClosedBoundary(t.Context(), session.lifecycleStream(), nil, containmentProof{
			vacantProven: true, empty: true, barrier: "root",
		}))
	})
}

type failNthIDReader struct {
	reads  int
	failAt int
}

func (r *failNthIDReader) Read(buffer []byte) (int, error) {
	r.reads++
	if r.reads == r.failAt {
		return 0, errors.New("lifecycle stream id failed")
	}

	for index := range buffer {
		buffer[index] = byte(r.reads + index)
	}

	return len(buffer), nil
}

func installFailingLifecycleIDReader(t *testing.T, failAt int) {
	t.Helper()

	original := sessionIDRandReader
	t.Cleanup(func() { sessionIDRandReader = original })
	sessionIDRandReader = &failNthIDReader{failAt: failAt}
}

func TestSessionConstructionCleansUpWhenLifecycleStreamIDFails(t *testing.T) {
	negotiated := lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	}

	t.Run("new", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-new")
		agent := newTestAgent(WithScratchDir(t.TempDir()), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				xdg, err := nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
				if err != nil {
					return nil, err
				}
				client.xdg = xdg

				return client, nil
			}
		})
		agent.retainNegotiatedLifecycle(negotiated)
		installFailingLifecycleIDReader(t, 2)

		_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		require.ErrorContains(t, err, "lifecycle stream id failed")
		require.True(t, client.closed)
	})

	t.Run("load", func(t *testing.T) {
		store := NewInMemorySessionStore()
		client := newFakeHermesClient()
		client.getSession = testNativeSession("native-1")
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(store), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				client.xdg = opts.ExistingXDG

				return client, nil
			}
		})
		seed := testSession(agent, newFakeHermesClient())
		require.NoError(t, seed.snapshotToStore(t.Context()))
		agent.retainNegotiatedLifecycle(negotiated)
		installFailingLifecycleIDReader(t, 1)

		_, err := agent.loadOrResumeSession(t.Context(), seed.id, seed.cwd, nil, nil, nil)
		require.ErrorContains(t, err, "lifecycle stream id failed")
		require.True(t, client.closed)
	})

	t.Run("fork with incomplete cleanup", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		childClient.closeErr = nativehermes.ErrProcessContainmentIncomplete
		agent := newTestAgent(WithScratchDir(t.TempDir()), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				childClient.xdg = opts.ExistingXDG

				return childClient, nil
			}
		})
		parent := testSession(agent, parentClient)
		agent.sessions[parent.id] = parent
		agent.retainNegotiatedLifecycle(negotiated)
		installFailingLifecycleIDReader(t, 2)

		_, err := agent.forkSession(t.Context(), acp.UnstableForkSessionRequest{
			SessionId: parent.id, Cwd: t.TempDir(),
		})
		require.ErrorContains(t, err, "lifecycle stream id failed")
		require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
	})
}
