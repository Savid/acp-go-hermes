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

	emitted := lifecycleUpdateCount(conn)

	if _, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id}); err != nil {
		t.Errorf("CloseSession after a cancelled turn must not report an error: %v", err)
	}
	if after := lifecycleUpdateCount(conn); after != emitted {
		t.Errorf("close emitted %d event(s) on the stream the cancel already fenced", after-emitted)
	}
}

// A close of a session whose incarnation never opened its stream: the same
// branch reached from the other side. No snapshot was ever delivered, so there
// is no stream to terminalize on and nothing to certify against; the boundary
// still runs its containment proof and answers success, and it emits nothing —
// least of all the quiescence fact its completed proof would otherwise state,
// which on an unopened stream would be a delta before the snapshot.
func TestCloseSessionOnANeverOpenedIncarnationEmitsNothing(t *testing.T) {
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
	require.NoError(t, session.openLifecycleStream())
	agent.sessions[session.id] = session

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.NoError(t, err, "a close whose incarnation never opened must still succeed")
	require.Zero(t, lifecycleUpdateCount(conn), "the boundary emitted on an incarnation that never opened")
	require.Equal(t, 1, client.closeCount(), "the containment proof runs whether or not the stream opened")
}

// The live incarnation is the other half of the same branch: a stream whose
// opening assertion was delivered and which nothing has fenced does get the
// emission rungs. What it states there is whatever its boundary actually
// proved — a completed whole-tree proof yields the quiescence fact, and a
// runtime that enumerates nothing yields none — and either way the stream is
// fenced afterwards.
func TestCloseSessionOnALiveIncarnationStatesWhatItProved(t *testing.T) {
	closeLive := func(t *testing.T, enumerates bool) (*session, *recordingAgentClient) {
		t.Helper()

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

		if enumerates {
			session.client = treeInventoryServer{fakeHermesClient: client, vacant: true}
		}

		require.NoError(t, session.openLifecycleStream())
		require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
		require.Equal(t, 1, lifecycleUpdateCount(conn), "precondition: the incarnation opened")
		agent.sessions[session.id] = session

		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		require.NoError(t, err)
		require.True(t, session.lifecycleStream().fenced(), "a settled close ends the incarnation")

		return session, conn
	}

	t.Run("proved vacancy", func(t *testing.T) {
		_, conn := closeLive(t, true)
		require.Equal(t, 2, lifecycleUpdateCount(conn), "the boundary owes the fact its completed proof produced")
	})

	t.Run("nothing to enumerate", func(t *testing.T) {
		_, conn := closeLive(t, false)
		require.Equal(t, 1, lifecycleUpdateCount(conn),
			"a boundary that enumerated no tree states no quiescence fact")
	})
}

// The durable branch's precision: an entity the incarnation loss already
// terminalized as `failed` stays `failed`. The close terminalizes only what is
// still nonterminal in the store, so a boundary the store already holds is never
// rewritten to the close's own cancelled verdict.
func TestCloseNeverRewritesALossTerminalizedFailureAsCancelled(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	turnCtx := session.beginTurn(t.Context(), "turn")
	session.mu.Lock()
	session.turnInFlight = true
	session.mu.Unlock()
	require.NoError(t, session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}))

	// The incarnation is lost under the turn: the run reports its failure, the
	// settlement records it, and the store holds `failed` from that moment on.
	_, published, err := session.settlePrompt(
		t.Context(), turnCtx, sessionTurnEpoch(session), SessionStoreTerminalState{},
		promptRun{settle: true, err: errors.New("gateway connection closed"), endsIncarnation: true}, nil,
	)
	require.Error(t, err, "a lost incarnation settles as the failure it was")
	require.True(t, published)
	require.Equal(t, string(lifecycle.OutcomeFailed), session.committedTerminalState().Outcome)

	agent.sessions[session.id] = session
	_, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.NoError(t, closeErr)

	entries, loadErr := agent.sessionStore().Load(t.Context(), SessionKey{
		SessionID: string(session.id), Subpath: SessionStoreMainSubpath,
	})
	require.NoError(t, loadErr)

	durable, inspectErr := InspectSessionStoreTerminalState(string(session.id), entries)
	require.NoError(t, inspectErr)
	require.Equal(t, string(lifecycle.OutcomeFailed), durable.Outcome,
		"the close rewrote a loss-terminalized failure as its own cancelled verdict")
	require.Empty(t, durable.StopReason, "no stop reason names a failure")
}

// lifecycleUpdateCount counts the notifications that actually carried a
// lifecycle envelope, which is what "emits nothing on the dead stream" is a
// claim about.
func lifecycleUpdateCount(conn *recordingAgentClient) int {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	count := 0

	for _, notification := range conn.updates {
		if _, carried := notification.Meta[lifecycle.MetaKey]; carried {
			count++
		}
	}

	return count
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

// The fence that ends an incarnation can land while the close boundary is
// running: the owed opening snapshot is delivered off the write barrier, and an
// undeliverable one fences the stream from that background goroutine. The
// generation the boundary captured before its containment proof is durable state,
// not a stream event, so it must still be published rather than dropped because a
// stream the close never needed went terminal underneath it.
func TestCloseSessionPublishesCapturedGenerationWhenDeferredOpenFences(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	conn.updateErr = errors.New("opening failed")
	agent.setAgentClient(conn)

	client := newFakeHermesClient()
	inClose := make(chan struct{})
	releaseClose := make(chan struct{})
	client.closeFunc = func(context.Context) error {
		close(inClose)
		<-releaseClose

		return nil
	}

	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	// A first durable generation, so the assertion reads a store that changed
	// rather than one that was never written.
	require.NoError(t, session.snapshotToStore(t.Context()))
	agent.sessions[session.id] = session
	agent.deferStreamOpen(session)

	session.mu.Lock()
	session.title = "renamed-before-close"
	session.mu.Unlock()

	closed := make(chan error, 1)
	go func() {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		closed <- err
	}()

	select {
	case <-inClose:
	case <-time.After(10 * time.Second):
		t.Fatal("close never reached the native containment boundary")
	}
	// The write barrier fires here: the owed snapshot cannot be delivered, so the
	// deferred open fences the incarnation while the close boundary is mid-flight.
	agent.releaseStreamOpens()
	agent.awaitStreamOpens()
	close(releaseClose)

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("close hung")
	}
	require.True(t, session.lifecycleStream().fenced(), "precondition: the deferred open did not fence the stream")

	entries, err := agent.sessionStore().Load(t.Context(), SessionKey{
		SessionID: string(session.id), Subpath: SessionStoreMainSubpath,
	})
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	require.Contains(t, string(entries[0]), "renamed-before-close",
		"the generation captured before the containment boundary was never published")
}

// The same window with a capture that failed: the close boundary observed the
// failure before the fence landed, so the close must report it rather than answer
// success over a generation it never made durable.
func TestCloseSessionReportsCaptureFailureWhenDeferredOpenFences(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	conn.updateErr = errors.New("opening failed")
	agent.setAgentClient(conn)

	client := newFakeHermesClient()
	client.messagesErr = errors.New("native history read failed")
	inClose := make(chan struct{})
	releaseClose := make(chan struct{})
	client.closeFunc = func(context.Context) error {
		close(inClose)
		<-releaseClose

		return nil
	}

	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	agent.sessions[session.id] = session
	agent.deferStreamOpen(session)

	closed := make(chan error, 1)
	go func() {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		closed <- err
	}()

	select {
	case <-inClose:
	case <-time.After(10 * time.Second):
		t.Fatal("close never reached the native containment boundary")
	}
	agent.releaseStreamOpens()
	agent.awaitStreamOpens()
	close(releaseClose)

	select {
	case err := <-closed:
		require.ErrorContains(t, err, "native history read failed",
			"a capture failure the close boundary observed was swallowed by the fenced path")
	case <-time.After(10 * time.Second):
		t.Fatal("close hung")
	}
	require.True(t, session.lifecycleStream().fenced(), "precondition: the deferred open did not fence the stream")
}
