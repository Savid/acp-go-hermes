package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestSessionLifecycleStreamReducesCompleteTurn(t *testing.T) {
	negotiated := lifecycle.Negotiated{
		Version:                 lifecycle.Version,
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
	require.NoError(t, stream.startActivity(t.Context()))
	require.ErrorContains(t, stream.startActivity(t.Context()), "hermes_lifecycle_overlap")
	require.NoError(t, stream.settle(t.Context(), lifecycleTurnOutcome{
		stopReason: lifecycle.StopReasonCancelled,
		outcome:    lifecycle.OutcomeCancelled,
	}))
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
	require.Len(t, state.Turns, 2)
	require.True(t, state.Turns[0].Terminal)
	require.Equal(t, lifecycle.OutcomeCancelled, state.Turns[0].Outcome)
	require.True(t, state.Turns[1].Terminal)
	require.Equal(t, lifecycle.OutcomeSuccess, state.Turns[1].Outcome)
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
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
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

func TestLifecycleEmitterViolationFencesStream(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	agent.setAgentClient(newRecordingAgentClient())
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	stream := session.lifecycleStream()
	require.NoError(t, stream.ensureLifecycleOpened(context.Background()))
	require.Error(t, stream.announceAction(context.Background(), lifecycle.ActionUpdate{}))
	require.True(t, stream.fenced())
}

func TestLifecycleStreamDeliveryFailuresStopAtFailedEvent(t *testing.T) {
	negotiated := lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
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
