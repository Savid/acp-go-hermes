package hermesacp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

// Close after an incarnation-ending settlement on an authoritative-quiescence
// configuration: the settlement already certified and fenced the stream, so the
// close boundary must not try to certify again on the fenced stream.
func TestSettleClosedSessionAfterIncarnationEndingSettlement(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions:                []int{lifecycle.Version},
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	session.client = treeInventoryServer{fakeHermesClient: client, vacant: true}
	if err := session.openLifecycleStream(); err != nil {
		t.Fatal(err)
	}
	turnCtx := session.beginTurn(t.Context(), "turn")
	session.mu.Lock()
	session.turnInFlight = true
	session.mu.Unlock()
	if err := session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}); err != nil {
		t.Fatal(err)
	}

	_, published, err := session.settlePrompt(
		t.Context(), turnCtx, sessionTurnEpoch(session), SessionStoreTerminalState{},
		promptRun{settle: true, cancelled: true, endsIncarnation: true, markCancelled: true}, nil,
	)
	if err != nil {
		t.Fatalf("settlement: published=%v err=%v", published, err)
	}
	if !session.lifecycleStream().fenced() {
		t.Fatal("stream not fenced after incarnation-ending settlement")
	}

	if err := session.settleClosedSession(t.Context()); err != nil {
		t.Errorf("close after a fenced incarnation must not report an error: %v", err)
	}
}

// The same shape through the public CloseSession handler, as a host drives it:
// a cancel settled the turn and ended the incarnation, and the close response
// must not carry a spurious stale_stream violation.
func TestCloseSessionAfterCancelledTurn(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions:                []int{lifecycle.Version},
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	session.client = treeInventoryServer{fakeHermesClient: client, vacant: true}
	if err := session.openLifecycleStream(); err != nil {
		t.Fatal(err)
	}
	turnCtx := session.beginTurn(t.Context(), "turn")
	session.mu.Lock()
	session.turnInFlight = true
	session.mu.Unlock()
	if err := session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := session.settlePrompt(
		t.Context(), turnCtx, sessionTurnEpoch(session), SessionStoreTerminalState{},
		promptRun{settle: true, cancelled: true, endsIncarnation: true, markCancelled: true}, nil,
	); err != nil {
		t.Fatalf("settlement: %v", err)
	}
	agent.sessions[session.id] = session

	if _, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id}); err != nil {
		t.Errorf("CloseSession after a cancelled turn must not report an error: %v", err)
	}
}

// A routed cancel that wins while the pre-claim capture is still reading the
// native history fences the runtime that read depends on, so the capture fails.
// The turn's truthful outcome is cancelled: the commit must be rebuilt in the
// cancelled shape and published, not poison the session.
func TestCancelDuringPreClaimCaptureSettlesCancelled(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()

	messagesBlocked := make(chan struct{})
	messagesRelease := make(chan struct{})
	var once sync.Once
	client.messagesFunc = func(ctx context.Context, _ string) ([]nativehermes.NativeMessage, error) {
		once.Do(func() { close(messagesBlocked) })
		select {
		case <-messagesRelease:
			return nil, errors.New("gateway connection closed")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	session := testSession(agent, client)
	if err := session.openLifecycleStream(); err != nil {
		t.Fatal(err)
	}
	turnCtx := session.beginTurn(t.Context(), "turn")
	session.mu.Lock()
	session.turnInFlight = true
	session.mu.Unlock()
	if err := session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}); err != nil {
		t.Fatal(err)
	}

	type settleResult struct {
		published bool
		err       error
	}
	settled := make(chan settleResult, 1)
	go func() {
		_, published, err := session.settlePrompt(
			t.Context(), turnCtx, sessionTurnEpoch(session), SessionStoreTerminalState{},
			promptRun{settle: true, finish: "stop", nativeMessageID: "assistant-1"}, nil,
		)
		settled <- settleResult{published: published, err: err}
	}()

	select {
	case <-messagesBlocked:
	case <-time.After(5 * time.Second):
		t.Fatal("settlement never reached the native history read")
	}

	// The cancel wins before the commit claim: it fences the native runtime,
	// which is exactly what a real cancel does to a turn still being settled.
	if err := session.cancelRouted(turnRouteMeta("turn")); err != nil {
		t.Fatalf("cancelRouted: %v", err)
	}
	close(messagesRelease)

	select {
	case result := <-settled:
		if result.err != nil {
			t.Errorf("cancel during the pre-claim capture must still settle: %v", result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("settlement hung")
	}

	if err := session.ensureNotPoisoned(); err != nil {
		t.Errorf("session poisoned by a cancel that must be recorded as a cancelled outcome: %v", err)
	}
	if terminal := session.committedTerminalState(); terminal.Outcome != string(lifecycle.OutcomeCancelled) {
		t.Errorf("expected committed cancelled outcome, got %q", terminal.Outcome)
	}
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

func TestTurnSettlementValueBoundaries(t *testing.T) {
	var settlement *turnSettlement
	settlement.complete()
	require.NoError(t, settlement.await(t.Context()))

	settlement = &turnSettlement{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, settlement.await(ctx), context.Canceled)
	settlement.complete()
	require.NoError(t, settlement.await(t.Context()))
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
