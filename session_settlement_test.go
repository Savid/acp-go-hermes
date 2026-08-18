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
