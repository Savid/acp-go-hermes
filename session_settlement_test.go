package hermesacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestAwaitSettlementHonorsCancellationAcrossTurnAndReuse(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	session.mu.Lock()
	session.foreground = &turnSettlement{done: make(chan struct{})}
	session.mu.Unlock()
	require.ErrorIs(t, session.awaitSettlement(ctx), context.Canceled)

	session.mu.Lock()
	session.foreground = &turnSettlement{done: make(chan struct{})}
	session.mu.Unlock()
	require.ErrorIs(t, session.awaitSettlement(ctx), context.Canceled)
}

func TestTurnSettlementNotifiesAfterCompletionLatch(t *testing.T) {
	settlement := &turnSettlement{done: make(chan struct{})}
	settlement.notify = func() {
		select {
		case <-settlement.done:
		default:
			t.Fatal("settlement notified before closing its completion latch")
		}
	}

	settlement.complete()
}

// Close after an incarnation-ending settlement on an authoritative-quiescence
// configuration: the settlement already certified and fenced the stream, so the
// close boundary must not try to certify again on the fenced stream.
func TestSettleClosedSessionAfterIncarnationEndingSettlement(t *testing.T) {
	agent := newTestAgent(WithHostAuthority(newTestHostAuthority()))
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version:                 lifecycle.Version,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
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

func TestAgentClosePublishesAuthoritativeQuiescenceBeforeConnectionDetach(t *testing.T) {
	agent := newTestAgent(WithHostAuthority(newTestHostAuthority()))
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version:                 lifecycle.Version,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
	agent.sessions[session.id] = session

	require.NoError(t, agent.Close())

	conn.mu.Lock()
	events := lifecycleEvents(append([]acp.SessionNotification(nil), conn.updates...))
	conn.mu.Unlock()
	require.Len(t, events, 2)
	require.Equal(t, string(lifecycle.EventSnapshot), events[0]["type"])
	require.Equal(t, string(lifecycle.EventQuiescenceUpdate), events[1]["type"])
	require.Equal(t, true, events[1]["quiescent"])
	require.Nil(t, agent.connection())
}

// The same shape through the public CloseSession handler, as a host drives it:
// a cancel settled the turn and ended the incarnation, and the close response
// must not carry a spurious stale_stream violation.
func TestCloseSessionAfterCancelledTurn(t *testing.T) {
	agent := newTestAgent(WithHostAuthority(newTestHostAuthority()))
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version:                 lifecycle.Version,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
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
	agent := newTestAgent(WithHostAuthority(newTestHostAuthority()))
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version:                 lifecycle.Version,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	agent.sessions[session.id] = session

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.NoError(t, err, "a close whose incarnation never opened must still succeed")
	require.Zero(t, lifecycleUpdateCount(conn), "the boundary emitted on an incarnation that never opened")
	require.Equal(t, 1, client.closeCount(), "the containment proof runs whether or not the stream opened")
}

// A never-opened incarnation still ends at the close boundary. The owed opening
// snapshot is delivered from a detached goroutine the close never joins, so a
// close that returned success while leaving the stream unfenced would let that
// snapshot reach the host afterwards — a frame on a session the host was told is
// gone. The fence makes the late open a local stale_stream refusal instead.
func TestCloseSessionFencesANeverOpenedIncarnation(t *testing.T) {
	agent := newTestAgent(WithHostAuthority(newTestHostAuthority()))
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version:                 lifecycle.Version,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	agent.sessions[session.id] = session
	// The establishing response queues the owed snapshot; the release goroutine
	// that delivers it is detached, and CloseSession never joins it.
	requireStreamOpenDeferred(t, agent, lifecycleRequestContext(t.Context(), 1), session)

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.NoError(t, err)
	require.Zero(t, lifecycleUpdateCount(conn), "the boundary emitted on an incarnation that never opened")
	require.True(t, session.lifecycleStream().fenced(),
		"the close left a never-opened incarnation able to speak after it answered success")

	// The detached open now runs, after the close already returned.
	agent.openDeferredStream(session)
	require.Zero(t, lifecycleUpdateCount(conn),
		"the owed opening snapshot reached the host after the session was closed")
}

// The live incarnation is the other half of the same branch: a stream whose
// opening assertion was delivered and which nothing has fenced does get the
// emission rungs. Managed execution states the quiescence fact supplied by its
// authority boundary; ordinary execution states none. Either way the stream is
// fenced afterwards.
func TestCloseSessionOnALiveIncarnationStatesWhatItProved(t *testing.T) {
	closeLive := func(t *testing.T, managed bool) (*session, *recordingAgentClient) {
		t.Helper()

		agent := newTestAgent()
		negotiated := lifecycle.Negotiated{Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{}}
		if managed {
			agent = newTestAgent(WithHostAuthority(newTestHostAuthority()))
			negotiated.AuthoritativeQuiescence = true
			negotiated.QuiescenceSource = lifecycle.ProofClassProcessContainment
		}
		agent.retainNegotiatedLifecycle(negotiated)
		conn := newRecordingAgentClient()
		agent.setAgentClient(conn)
		client := newFakeHermesClient()
		session := testSession(agent, client)

		require.NoError(t, session.openLifecycleStream())
		require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
		require.Equal(t, 1, lifecycleUpdateCount(conn), "precondition: the incarnation opened")
		agent.sessions[session.id] = session

		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		require.NoError(t, err)
		require.True(t, session.lifecycleStream().fenced(), "a settled close ends the incarnation")

		return session, conn
	}

	t.Run("managed authority", func(t *testing.T) {
		_, conn := closeLive(t, true)
		require.Equal(t, 2, lifecycleUpdateCount(conn), "the boundary owes the fact its completed proof produced")
	})

	t.Run("ordinary execution", func(t *testing.T) {
		_, conn := closeLive(t, false)
		require.Equal(t, 1, lifecycleUpdateCount(conn),
			"ordinary execution states no quiescence fact")
	})
}

// cancelHonoringAgentClient answers a lifecycle emission the way the wire does:
// a notification written on a cancelled context never reaches the host.
type cancelHonoringAgentClient struct {
	*recordingAgentClient
}

func (c *cancelHonoringAgentClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return c.recordingAgentClient.SessionUpdate(ctx, notification)
}

// TestCloseRunsItsEmissionRungsOnTheDetachedContext pins that a cancelled caller
// cannot buy containment, a durable commit, and a fence while skipping the
// terminal transitions and the quiescence fact the boundary owes. The close
// fences the stream on every exit, so an emission skipped here has nowhere left
// to be made: the settlement response would report a contained session whose
// host projection still holds a pending action and no proof of quiescence,
// permanently.
func TestCloseRunsItsEmissionRungsOnTheDetachedContext(t *testing.T) {
	closeCancelled := func(t *testing.T, session *session, recorder *recordingAgentClient) int {
		t.Helper()

		session.agent.sessions[session.id] = session
		emitted := lifecycleUpdateCount(recorder)

		cancelled, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := session.agent.CloseSession(cancelled, acp.CloseSessionRequest{SessionId: session.id})
		require.NoError(t, err, "a cancelled caller must not fail the boundary it already proved")
		require.True(t, session.lifecycleStream().fenced(), "a settled close ends the incarnation")

		return lifecycleUpdateCount(recorder) - emitted
	}

	t.Run("terminalization", func(t *testing.T) {
		session, recorder, turnCtx := newLifecycleActionSession(t, true)
		session.agent.setAgentClient(&cancelHonoringAgentClient{recordingAgentClient: recorder})

		action, _, owned := session.lifecycleStream().reserveAction(lifecycle.ActionPermission)
		require.True(t, owned)
		require.NoError(t, session.lifecycleStream().announceAction(turnCtx, action))

		route := session.routeForEvent(nativehermes.TurnEvent{
			CycleID: testControlCycleID, TransportGeneration: 1,
		})
		require.NotNil(t, route)
		session.finishAutonomousRoute(route)

		require.Equal(t, 2, closeCancelled(t, session, recorder),
			"the close owes the pending action terminal transition and one cancelled idle")
	})

	t.Run("quiescence", func(t *testing.T) {
		agent := newTestAgent(WithHostAuthority(newTestHostAuthority()))
		agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
			Version:                 lifecycle.Version,
			AuthoritativeQuiescence: true,
			QuiescenceSource:        lifecycle.ProofClassProcessContainment,
			ActivityKinds:           []lifecycle.ActivityKind{},
		})

		recorder := newRecordingAgentClient()
		agent.setAgentClient(&cancelHonoringAgentClient{recordingAgentClient: recorder})

		client := newFakeHermesClient()
		session := testSession(agent, client)
		require.NoError(t, session.openLifecycleStream())
		require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))

		require.Equal(t, 1, closeCancelled(t, session, recorder),
			"the close owes the quiescence fact its completed proof produced")
	})
}

// The durable branch's precision: an entity the incarnation loss already
// terminalized as `failed` stays `failed`. The close terminalizes only what is
// still nonterminal in the store, so a boundary the store already holds is never
// rewritten to the close's own cancelled verdict.
func TestCloseNeverRewritesALossTerminalizedFailureAsCancelled(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
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

// refuseFirstReplaceStore refuses exactly one Replace and then behaves like the
// in-memory store, which is the store a host recovers: the write that failed is
// the write the retry is expected to land.
type refuseFirstReplaceStore struct {
	*InMemorySessionStore
	mu       sync.Mutex
	refusals int
	err      error
}

func (s *refuseFirstReplaceStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	s.mu.Lock()
	first := s.refusals == 0
	s.refusals++
	s.mu.Unlock()

	if first {
		return s.err
	}

	return s.InMemorySessionStore.Replace(ctx, main, replacements)
}

// TestFailedCloseBoundaryKeepsTheIDCloseable pins what a close that did not
// complete its boundary leaves behind. The rungs the boundary owes — a tree
// proved contained, a generation the store accepted — are still owed when it
// fails, and the id is the only name the host has for them. Detaching it would
// answer the retry the failure asks for with unknown_session and strand the work
// with nothing able to reach it, so the session stays addressable and the next
// close runs the boundary again.
func TestFailedCloseBoundaryKeepsTheIDCloseable(t *testing.T) {
	t.Run("containment", func(t *testing.T) {
		client := newFakeHermesClient()

		var closes atomic.Int32

		client.closeFunc = func(context.Context) error {
			if closes.Add(1) == 1 {
				return errors.New("containment failed")
			}

			return nil
		}

		agent := newTestAgent(WithHostAuthority(newTestHostAuthority()))
		agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
			Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
		})
		agent.setAgentClient(newRecordingAgentClient())
		session := testSession(agent, client)
		require.NoError(t, session.openLifecycleStream())

		session.mu.Lock()
		session.title = "renamed-before-close"
		session.mu.Unlock()

		agent.sessions[session.id] = session

		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		require.ErrorContains(t, err, "containment failed")

		resolved, resolveErr := agent.session(session.id)
		require.NoError(t, resolveErr, "the failed close detached the id its retry needs")
		require.Same(t, session, resolved)

		_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		require.NoError(t, err)
		require.EqualValues(t, 2, closes.Load(),
			"the retry answered success without re-running the containment boundary")

		_, resolveErr = agent.session(session.id)
		require.Error(t, resolveErr, "a completed close still detaches the id")

		// The generation the failed boundary captured is the one the retry owed,
		// and it reaches the store rather than dying with the first attempt.
		entries, loadErr := agent.sessionStore().Load(t.Context(), SessionKey{
			SessionID: string(session.id), Subpath: SessionStoreMainSubpath,
		})
		require.NoError(t, loadErr)
		require.NotEmpty(t, entries)
		require.Contains(t, string(entries[len(entries)-1]), "renamed-before-close")
	})

	// The refused commit is the same owed rung whether the incarnation still has
	// a stream to speak on or never opened one: the durable rung is not a stream
	// rung, so both boundaries retain what the store would not take.
	for name, opened := range map[string]bool{"durable commit": true, "durable commit on an unopened stream": false} {
		t.Run(name, func(t *testing.T) {
			store := &refuseFirstReplaceStore{
				InMemorySessionStore: NewInMemorySessionStore(),
				err:                  errors.New("durable commit refused"),
			}
			agent := newTestAgent(WithSessionStore(store))
			agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
				Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
			})
			agent.setAgentClient(newRecordingAgentClient())
			client := newFakeHermesClient()
			session := testSession(agent, client)
			require.NoError(t, session.openLifecycleStream())

			if opened {
				require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
			}

			session.mu.Lock()
			session.title = "renamed-before-close"
			session.mu.Unlock()

			agent.sessions[session.id] = session
			key := SessionKey{SessionID: string(session.id), Subpath: SessionStoreMainSubpath}

			_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
			require.ErrorContains(t, err, "durable commit refused")

			refused, loadErr := store.Load(t.Context(), key)
			require.NoError(t, loadErr)
			require.Empty(t, refused, "precondition: the refused commit reached the store anyway")

			resolved, resolveErr := agent.session(session.id)
			require.NoError(t, resolveErr, "the failed close detached the id its retry needs")
			require.Same(t, session, resolved)

			_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
			require.NoError(t, err)

			entries, loadErr := store.Load(t.Context(), key)
			require.NoError(t, loadErr)
			require.NotEmpty(t, entries, "the retry never made the commit the failed close owed")
			require.Contains(t, string(entries[len(entries)-1]), "renamed-before-close")

			_, resolveErr = agent.session(session.id)
			require.Error(t, resolveErr, "a completed close still detaches the id")
		})
	}
}

// TestQuarantinedContainmentStillLeavesTheIDCloseable reconciles the retained id
// with the generation-root quarantine. The two answer different questions: the
// quarantine says a root this adapter could not prove empty may never back a
// resumed runtime again, while the retained id says the close boundary is still
// owed and still reachable. A quarantined session therefore refuses to resume a
// turn and accepts another close, which is the only operation that can discharge
// what it owes.
func TestQuarantinedContainmentStillLeavesTheIDCloseable(t *testing.T) {
	client := newFakeHermesClient()

	var closes atomic.Int32

	client.closeFunc = func(context.Context) error {
		if closes.Add(1) == 1 {
			return ErrContainmentIncomplete
		}

		return nil
	}

	agent := newTestAgent()
	session := testSession(agent, client)
	agent.sessions[session.id] = session

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.ErrorIs(t, agent.rejectIncompleteHermesSession(session.id), ErrContainmentIncomplete,
		"the incomplete generation root was not quarantined")
	require.NotNil(t, agent.activeSession(session.id), "the quarantine detached the id the retry needs")

	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.NoError(t, err)
	require.EqualValues(t, 2, closes.Load())
	require.Nil(t, agent.activeSession(session.id))

	// The quarantine outlives the completed close: the root it named is still one
	// no resume may build on.
	require.ErrorIs(t, agent.rejectIncompleteHermesSession(session.id), ErrContainmentIncomplete)
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
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()

	messagesBlocked := make(chan struct{})
	messagesRelease := make(chan struct{})
	var once sync.Once

	session := testSession(agent, client)
	if err := session.openLifecycleStream(); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(client.XDGDirs().Root, "state.db")
	require.NoError(t, os.WriteFile(statePath, []byte("prior-complete-native-state"), 0o600))
	require.NoError(t, session.snapshotToStore(t.Context()))
	stateKey := SessionKey{SessionID: string(session.id), Subpath: stateDBSubpath}
	priorComplete, loadErr := agent.sessionStore().Load(t.Context(), stateKey)
	require.NoError(t, loadErr)
	require.NotEmpty(t, priorComplete)
	require.NoError(t, os.WriteFile(statePath, []byte("partial-forced-state"), 0o600))
	client.messagesFunc = func(ctx context.Context, _ string) ([]nativehermes.NativeMessage, error) {
		once.Do(func() { close(messagesBlocked) })
		select {
		case <-messagesRelease:
			return nil, errors.New("gateway connection closed")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
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
	afterForcedRevoke, loadErr := agent.sessionStore().Load(t.Context(), stateKey)
	require.NoError(t, loadErr)
	require.Equal(t, priorComplete, afterForcedRevoke,
		"forced revoke published the partially captured native state")
}

func TestLifecycleActionRegistrationAndMetadata(t *testing.T) {
	agent := newTestAgent()
	session := testSession(agent, newFakeHermesClient())

	action, update, owned := session.reserveBlockingAction(lifecycle.ActionPermission, "request", permissionTurnRoute{})
	require.True(t, owned)
	require.NoError(t, session.publishBlockingAction(t.Context(), action, update))
	require.Empty(t, action.id)
	require.NoError(t, session.resolveBlockingAction(t.Context(), action, lifecycle.ActionAccepted))

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

	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	require.NoError(t, session.openLifecycleStream())
	_, _, owned = session.reserveBlockingAction(lifecycle.ActionPermission, "unowned", permissionTurnRoute{})
	require.False(t, owned)

	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	turnCtx := session.beginTurn(t.Context(), "turn")
	require.NoError(t, session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}))
	conn.updateErr = errors.New("pending action failed")
	action, update, owned = session.reserveBlockingAction(lifecycle.ActionPermission, "failed", permissionTurnRoute{})
	require.True(t, owned)
	err := session.publishBlockingAction(turnCtx, action, update)
	require.ErrorContains(t, err, "pending action failed")
}

func TestPromptContainsPanickingNativeSend(t *testing.T) {
	client := newFakeHermesClient()
	client.sendMessage = func(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		panic("native send panic")
	}
	session := testSession(newTestAgent(), client)
	defer session.stopPump()
	_, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "panic-send", "reply"))
	require.Error(t, err)
	require.ErrorContains(t, errors.Unwrap(err), "source corrupted")
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
		agent := newTestAgent(WithHostAuthority(newTestHostAuthority()))
		agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
			Version:                 lifecycle.Version,
			AuthoritativeQuiescence: true,
			QuiescenceSource:        lifecycle.ProofClassProcessContainment,
			ActivityKinds:           []lifecycle.ActivityKind{},
		})
		base := newRecordingAgentClient()
		conn := &lifecycleFailingAgentClient{recordingAgentClient: base, failAt: 6}
		agent.setAgentClient(conn)
		client := newFakeHermesClient()
		session := testSession(agent, client)
		require.NoError(t, session.openLifecycleStream())
		turnCtx := beginTestControlTurn(t, session, t.Context(), "turn")
		session.mu.Lock()
		session.turnInFlight = true
		session.turnEpoch = 1
		session.turnSettlement = turnSettlementOpen
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
	for _, test := range []struct {
		name            string
		sendErr         error
		acceptErr       error
		dispatched      bool
		wantSettle      bool
		wantIncarnation bool
	}{
		{name: "registration refusal", sendErr: nativehermes.ErrGatewayAmbiguousTurn},
		{name: "agent-origin backpressure", sendErr: nativehermes.ErrGatewayAgentBusy},
		{name: "absorbed by the running turn", sendErr: nativehermes.ErrGatewayTurnAbsorbedPrompt},
		{name: "queued as the next native turn", sendErr: nativehermes.ErrGatewayPromptQueuedAsNextTurn},
		{name: "admission refusal", acceptErr: acceptErr},
		{name: "missing dispatch proof"},
		{name: "post-dispatch admission failure", acceptErr: acceptErr, dispatched: true, wantSettle: true, wantIncarnation: true},
		{name: "post-dispatch transport failure", sendErr: nativehermes.ErrGatewayDisconnected, dispatched: true, wantSettle: true, wantIncarnation: true},
		{name: "post-dispatch provider failure", sendErr: nativehermes.NewTurnFailure(nativehermes.CauseProvider, "provider failed"), dispatched: true, wantSettle: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			run := session.nativeRun(t.Context(), nativehermes.NativeMessage{}, test.sendErr, test.acceptErr, test.dispatched)
			require.Error(t, run.err)
			require.Equal(t, test.wantSettle, run.settle)
			require.Equal(t, test.wantIncarnation, run.endsIncarnation)
		})
	}

	// Hermes running a full autonomous turn between prompts is the contention
	// the ACP foreground already states, so the prompt is refused with the same
	// retryable backpressure rather than mapped to a turn failure.
	busy := session.nativeRun(t.Context(), nativehermes.NativeMessage{}, nativehermes.ErrGatewayAgentBusy, nil, false)
	require.Equal(t, sessionForegroundBackpressure().Error(), busy.err.Error())

	// A prompt Hermes folded into the turn already running is accepted work,
	// not contention: it is stated as its own outcome so no host retries text
	// that is already inside a live turn.
	absorbed := session.nativeRun(t.Context(), nativehermes.NativeMessage{}, nativehermes.ErrGatewayTurnAbsorbedPrompt, nil, false)
	require.Equal(t, sessionPromptAbsorbed().Error(), absorbed.err.Error())
	require.NotEqual(t, sessionForegroundBackpressure().Error(), absorbed.err.Error())

	// A prompt Hermes queued as the session's next native turn, run by a turn
	// that announced no start of its own, is accepted work too: Hermes runs the
	// text, so the outcome is stated rather than offered back for a retry that
	// would run it twice.
	queued := session.nativeRun(t.Context(), nativehermes.NativeMessage{}, nativehermes.ErrGatewayPromptQueuedAsNextTurn, nil, false)
	require.Equal(t, sessionPromptQueuedTurn().Error(), queued.err.Error())
	require.NotEqual(t, sessionForegroundBackpressure().Error(), queued.err.Error())

	client := newFakeHermesClient()
	client.closeErr = errors.New("containment failed")
	session = testSession(newTestAgent(), client)
	session.beginTurn(t.Context(), "turn")
	run := session.unacceptedCancel(sessionTurnEpoch(session), nil)
	require.ErrorContains(t, run.err, "containment failed")
}

type predispatchRefusingServer struct {
	*fakeHermesClient
	err error
}

func (s predispatchRefusingServer) SendMessage(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
	return nativehermes.NativeMessage{}, s.err
}

func TestPredispatchRefusalHasNoTurnSettlementBoundary(t *testing.T) {
	store := newCountingSessionStore()
	base := newFakeHermesClient()
	agent := newTestAgent(WithSessionStore(store))
	agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
	connection := newRecordingAgentClient()
	agent.setAgentClient(connection)
	session := testSession(agent, base)
	defer session.stopPump()
	session.client = predispatchRefusingServer{fakeHermesClient: base, err: nativehermes.ErrGatewayAmbiguousTurn}
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
	updatesBefore := lifecycleUpdateCount(connection)

	_, err := session.Prompt(t.Context(), acp.PromptRequest{
		Meta: autonomousPromptMeta("predispatch-refusal"), SessionId: session.id,
		Prompt: []acp.ContentBlock{acp.TextBlock("refuse")},
	})
	require.Error(t, err)
	require.Equal(t, updatesBefore, lifecycleUpdateCount(connection))
	require.Zero(t, store.replaceCount())
	require.Zero(t, base.abortCount())
	require.Zero(t, base.closeCount())
	require.False(t, session.lifecycleStream().fenced())
	session.mu.Lock()
	require.Nil(t, session.foreground)
	require.Equal(t, turnSettlementIdle, session.turnSettlement)
	session.mu.Unlock()
}

func TestClosedBoundaryStopsAtFirstFailedRung(t *testing.T) {
	t.Run("terminalization", func(t *testing.T) {
		session, conn, turnCtx := newLifecycleActionSession(t, true)
		action, _, owned := session.lifecycleStream().reserveAction(lifecycle.ActionPermission)
		require.True(t, owned)
		require.NoError(t, session.lifecycleStream().announceAction(turnCtx, action))
		session.agent.setAgentClient(&lifecycleFailingAgentClient{recordingAgentClient: conn, failAt: 1})
		published, err := session.publishClosedBoundary(t.Context(), session.lifecycleStream(), nil, containmentProof{})
		require.ErrorContains(t, err, "lifecycle delivery failed")
		require.False(t, published, "a rung that stopped before the durable one discharged it")
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
		published, err := session.publishClosedBoundary(t.Context(), nil, commit, containmentProof{})
		require.Error(t, err)
		require.False(t, published, "a refused commit was reported as durable")
	})

	t.Run("cancelled idle delivery", func(t *testing.T) {
		agent := newTestAgent()
		agent.retainNegotiatedLifecycle(autonomousLifecycleNegotiation())
		connection := newRecordingAgentClient()
		agent.setAgentClient(connection)
		session := testSession(agent, newFakeHermesClient())
		defer session.stopPump()
		require.NoError(t, session.openLifecycleStream())
		stream := session.lifecycleStream()
		require.NoError(t, stream.ensureLifecycleOpened(t.Context()))
		require.NoError(t, stream.startActivity(t.Context()))
		connection.updateErr = errors.New("cancelled idle unavailable")
		published, err := session.publishClosedBoundary(t.Context(), stream, nil, containmentProof{})
		require.ErrorContains(t, err, "cancelled idle unavailable")
		require.True(t, published, "the boundary had no missing durable publication")
	})

	t.Run("unproved vacancy", func(t *testing.T) {
		session := testSession(newTestAgent(), newFakeHermesClient())
		published, err := session.publishClosedBoundary(t.Context(), nil, nil, containmentProof{})
		require.NoError(t, err)
		require.True(t, published, "a boundary with no commit to make still owes none")
	})

	t.Run("proved vacancy", func(t *testing.T) {
		agent := newTestAgent()
		agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
			Version:                 lifecycle.Version,
			AuthoritativeQuiescence: true,
			QuiescenceSource:        lifecycle.ProofClassProcessContainment,
			ActivityKinds:           []lifecycle.ActivityKind{},
		})
		conn := newRecordingAgentClient()
		agent.setAgentClient(conn)
		session := testSession(agent, newFakeHermesClient())
		require.NoError(t, session.openLifecycleStream())
		require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
		published, err := session.publishClosedBoundary(t.Context(), session.lifecycleStream(), nil, containmentProof{
			vacantProven: true, empty: true, barrier: "root",
		})
		require.NoError(t, err)
		require.True(t, published)
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
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
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
	requireStreamOpenDeferred(t, agent, lifecycleRequestContext(t.Context(), 2), session)

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
	releaseLifecycleOpening(t, agent, session, 2)
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

// A mandatory close capture is the last read of the live generation. Failure
// therefore returns before containment or fencing and leaves the exact logical
// session addressable but close-only. A retry must really capture again; it may
// not answer success from the prior failure.
func TestCloseCaptureFailureLeavesExactCloseOnlySessionRetryable(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)

	client := newFakeHermesClient()
	client.messagesErr = errors.New("native history read failed")
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
	session.mu.Lock()
	session.title = "captured-on-successful-retry"
	session.mu.Unlock()
	agent.sessions[session.id] = session

	for attempt := 1; attempt <= 2; attempt++ {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		require.ErrorContains(t, err, "native history read failed")
		require.Equal(t, 0, client.closeCount(), "attempt %d destructively contained the runtime", attempt)
		require.False(t, session.lifecycleStream().fenced(), "attempt %d fenced the live stream", attempt)

		resolved, resolveErr := agent.session(session.id)
		require.NoError(t, resolveErr)
		require.Same(t, session, resolved)
	}

	client.mu.Lock()
	client.messagesErr = nil
	client.mu.Unlock()

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.NoError(t, err)
	require.Equal(t, 1, client.closeCount())
	_, resolveErr := agent.session(session.id)
	require.Error(t, resolveErr, "successful close did not detach the id")

	entries, loadErr := agent.sessionStore().Load(t.Context(), SessionKey{
		SessionID: string(session.id), Subpath: SessionStoreMainSubpath,
	})
	require.NoError(t, loadErr)
	require.NotEmpty(t, entries, "successful retry detached before persistence")
	require.Contains(t, string(entries[len(entries)-1]), "captured-on-successful-retry")
}

// TestCloseOnAFencedIncarnationRetainsTheLastCommittedGeneration pins the other
// half of the fenced branch. A dead incarnation discharges whatever durable
// commit it owes, and where the loss already committed it owes nothing: the last
// committed generation is retained unrewritten rather than overwritten with a
// generation captured from a runtime the fence already ended.
func TestCloseOnAFencedIncarnationRetainsTheLastCommittedGeneration(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)

	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.snapshotToStore(t.Context()))

	key := SessionKey{SessionID: string(session.id), Subpath: SessionStoreMainSubpath}
	committed, err := agent.sessionStore().Load(t.Context(), key)
	require.NoError(t, err)
	require.NotEmpty(t, committed, "precondition: nothing was ever committed")

	before := string(committed[len(committed)-1])

	// The incarnation is lost, and only then does the host close the session.
	session.lifecycleStream().fence()

	session.mu.Lock()
	session.title = "renamed-after-the-fence"
	session.mu.Unlock()

	agent.sessions[session.id] = session

	_, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.NoError(t, closeErr)

	after, err := agent.sessionStore().Load(t.Context(), key)
	require.NoError(t, err)
	require.NotEmpty(t, after, "the close destroyed the generation the loss had committed")
	require.Equal(t, before, string(after[len(after)-1]),
		"the close rewrote a generation the fenced incarnation had already committed")
}

// TestAgentCloseMakesTheDurableCommitAWireCloseWould pins the ladder's durable
// rung on the embedded path. Agent.Close runs the same ladder session/close
// runs, so state a wire close would have committed must not be dropped along
// with the wrapper.
func TestAgentCloseMakesTheDurableCommitAWireCloseWould(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)

	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.snapshotToStore(t.Context()))

	session.mu.Lock()
	session.title = "renamed-before-shutdown"
	session.mu.Unlock()

	agent.sessions[session.id] = session

	require.NoError(t, agent.Close())

	entries, err := agent.sessionStore().Load(t.Context(), SessionKey{
		SessionID: string(session.id), Subpath: SessionStoreMainSubpath,
	})
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	require.Contains(t, string(entries[len(entries)-1]), "renamed-before-shutdown",
		"the embedded shutdown dropped a commit a wire close would have made")
	require.True(t, session.lifecycleStream().fenced(), "the shutdown left the incarnation able to speak")
}

// TestAgentCloseCancelsTheTurnInFlightBeforeItsBoundary pins the ladder's first
// rungs on the embedded path: admission closes, the turn in flight is cancelled,
// and the settlement it owes completes before the shutdown's own boundary runs.
func TestAgentCloseCancelsTheTurnInFlightBeforeItsBoundary(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}

	agent := newTestAgent(WithScratchDir(t.TempDir()))
	session := testSession(agent, client)
	agent.sessions[session.id] = session

	type promptOutcome struct {
		resp acp.PromptResponse
		err  error
	}

	promptDone := make(chan promptOutcome, 1)

	go func() {
		resp, promptErr := session.Prompt(context.Background(), TextPromptRequest(session.id, "shutdown-active", "hang"))
		promptDone <- promptOutcome{resp: resp, err: promptErr}
	}()
	<-started

	require.NoError(t, agent.Close())

	out := <-promptDone
	require.NoError(t, out.err)
	require.Equal(t, acp.StopReasonCancelled, out.resp.StopReason,
		"the shutdown did not cancel the turn in flight")
}

func TestSettlementManagedCompletionResidualBranch(t *testing.T) {
	want := errors.New("managed completion refused")
	managed := &managedHermesServer{closed: true, closeErr: want}
	session := testSession(newTestAgent(), newFakeHermesClient())
	session.owedCloseCommit = &sessionStoreCommit{
		managed: managed, managedReady: []SessionStoreReplacement{{Key: SessionKey{SessionID: "session"}}},
	}
	if err := session.settleClosedSession(t.Context()); !errors.Is(err, want) || session.owedCloseCommit == nil {
		t.Fatalf("managed settlement completion = %v, retained=%v", err, session.owedCloseCommit != nil)
	}
}
