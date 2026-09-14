package hermesacp

import (
	"context"
	"fmt"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
)

// lifecycleState is the session's side of the ordered lifecycle stream: one
// incarnation per hermes process generation, the foreground it currently holds,
// and the blocking actions outstanding on it.
type lifecycleState struct {
	stream   *lifecycle.Stream
	seq      uint64
	cycleID  string
	blockers map[string]struct{}
}

func (s *session) lifecycleNegotiated() lifecycle.Negotiated {
	return s.agent.lifecycleNegotiated()
}

func (s *session) nextLifecycleID(kind string) string {
	s.lc.seq++

	return fmt.Sprintf("%s-%d", kind, s.lc.seq)
}

// openStream opens the incarnation for the live process generation with an
// idle snapshot. Its identity is minted by the agent so no earlier
// incarnation of the session, including one before a close, shares it. It is a no-op while the host negotiated no lifecycle.
func (s *session) openStream(ctx context.Context) error {
	negotiated := s.lifecycleNegotiated()
	if !negotiated.Present() {
		return nil
	}

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream != nil && !s.lc.stream.Fenced() {
		return nil
	}

	s.lc.stream = lifecycle.NewStream(fmt.Sprintf("%s:%d", s.id, s.agent.nextIncarnation()), negotiated)
	s.lc.cycleID = s.nextLifecycleID("cycle")
	s.lc.blockers = make(map[string]struct{})

	return s.emitLifecycleLocked(ctx, lifecycle.SnapshotEvent(s.lc.cycleID))
}

// acceptTurn publishes the prompt's acceptance exactly once: the pump does it
// on the first record hermes produced for the turn, the prompt does it when the
// native response arrives first.
func (s *session) acceptTurn(ctx context.Context, t *turn) {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if t.accepted {
		return
	}

	t.accepted = true

	if s.lc.stream == nil || s.lc.stream.Fenced() {
		return
	}

	t.turnID = s.nextLifecycleID("turn")
	t.cycleID = s.nextLifecycleID("cycle")
	t.origin = lifecycle.CauseSubmission
	s.lc.cycleID = t.cycleID

	if err := s.emitLifecycleLocked(ctx, lifecycle.AcceptedEvent(t.submission, t.turnID)); err != nil {
		t.failure = err

		return
	}

	if err := s.emitLifecycleLocked(ctx, lifecycle.TransitionEvent(lifecycle.ForegroundRunning, t.cycleID, t.turnID)); err != nil {
		t.failure = err
	}
}

// lcOpenAgentCycle opens an agent-origin turn on the stream.
func (s *session) lcOpenAgentCycle(ctx context.Context, c *cycle) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.stream.Fenced() {
		return nil
	}

	c.turnID = s.nextLifecycleID("turn")
	c.cycleID = s.nextLifecycleID("cycle")
	s.lc.cycleID = c.cycleID

	return s.emitLifecycleLocked(ctx, lifecycle.TransitionEventWithCause(
		lifecycle.ForegroundRunning, c.cycleID, c.turnID, lifecycle.CauseActivity,
	))
}

// lcActionPendingWithID announces one blocking action on the cycle and moves
// the foreground to requires_action for its first blocker.
func (s *session) lcActionPendingWithID(ctx context.Context, c *cycle, actionID string, kind lifecycle.ActionKind) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.stream.Fenced() || c.turnID == "" {
		return nil
	}

	owner := lifecycle.Owner{Type: lifecycle.OwnerTurn, ID: c.turnID}

	if err := s.emitLifecycleLocked(ctx, lifecycle.ActionEvent(lifecycle.PendingAction(actionID, kind, owner, true))); err != nil {
		return err
	}

	if len(s.lc.blockers) == 0 {
		if err := s.emitLifecycleLocked(ctx, lifecycle.TransitionEventWithCause(
			lifecycle.ForegroundRequiresAction, c.cycleID, c.turnID, c.origin,
		)); err != nil {
			return err
		}
	}

	s.lc.blockers[actionID] = struct{}{}

	return nil
}

// lcActionResolved terminalizes one action and returns the foreground to
// running once no blocker remains.
func (s *session) lcActionResolved(ctx context.Context, c *cycle, actionID string, state lifecycle.ActionState) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if _, pending := s.lc.blockers[actionID]; !pending || s.lc.stream == nil || s.lc.stream.Fenced() {
		return nil
	}

	delete(s.lc.blockers, actionID)

	if err := s.emitLifecycleLocked(ctx, lifecycle.ActionEvent(lifecycle.ResolvedAction(actionID, state))); err != nil {
		return err
	}

	if len(s.lc.blockers) > 0 {
		return nil
	}

	return s.emitLifecycleLocked(ctx, lifecycle.TransitionEventWithCause(
		lifecycle.ForegroundRunning, c.cycleID, c.turnID, c.origin,
	))
}

// lcIdle ends the cycle. Blockers still pending are terminalized as cancelled
// first, and a failed outcome carries no stop reason.
func (s *session) lcIdle(ctx context.Context, c *cycle, verdict cycleVerdict) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.stream.Fenced() || c.turnID == "" {
		return nil
	}

	for actionID := range s.lc.blockers {
		if err := s.emitLifecycleLocked(ctx, lifecycle.ActionEvent(lifecycle.ResolvedAction(actionID, lifecycle.ActionCancelled))); err != nil {
			return err
		}

		delete(s.lc.blockers, actionID)
	}

	stopReason := verdict.stopReason
	if verdict.outcome == lifecycle.OutcomeFailed {
		stopReason = ""
	}

	return s.emitLifecycleLocked(ctx, lifecycle.IdleEventWithCause(c.cycleID, c.turnID, c.origin, stopReason, verdict.outcome))
}

// lcFence ends the incarnation. Later native process generations open a new
// stream with a fresh snapshot.
func (s *session) lcFence() {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream != nil {
		s.lc.stream.Fence()
	}
}

// actionCorrelation is the value stamped on a permission or elicitation
// request while the lifecycle is negotiated.
func (s *session) actionCorrelation(c *cycle, actionID string) map[string]any {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if actionID == "" || s.lc.stream == nil {
		return nil
	}

	return map[string]any{wire.LifecycleKey: lifecycle.ActionCorrelation{
		StreamID: s.lc.stream.ID(),
		ActionID: actionID,
		Owner:    lifecycle.Owner{Type: lifecycle.OwnerTurn, ID: c.turnID},
	}.Value()}
}

// emitLifecycleLocked claims the next sequence on the stream and delivers the
// envelope on its own identity-only session_info_update. A refused event
// fences the incarnation: the stream cannot be continued truthfully. Delivery
// never rides a request's cancellation: a sequence the stream claimed must
// reach the host even when the request that caused it is gone.
func (s *session) emitLifecycleLocked(ctx context.Context, event lifecycle.Event) error {
	envelope, err := s.lc.stream.Emit(event)
	if err != nil {
		s.lc.stream.Fence()

		return fmt.Errorf("lifecycle stream refused an event: %w", err)
	}

	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	return conn.SessionUpdate(context.WithoutCancel(ctx), acp.SessionNotification{
		Meta:      map[string]any{wire.LifecycleKey: envelope},
		SessionId: s.id,
		Update:    acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}},
	})
}
