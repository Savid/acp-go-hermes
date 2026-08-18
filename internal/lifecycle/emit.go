package lifecycle

import "encoding/json"

// Stream is one incarnation's ordered emitter. It claims a sequence before
// delivery is attempted, so a lost or refused event leaves a detectable gap
// rather than a silently contiguous stream, and it reduces every event through
// the same reducer the canonical fixture battery drives, so a stream this
// adapter could not support fails at the point of emission instead of at its
// consumers.
//
// A Stream is not safe for concurrent use; the session that owns the
// incarnation serializes emission.
type Stream struct {
	id       string
	reducer  *Reducer
	sequence uint64
	fenced   bool
}

// NewStream opens an incarnation identified by id. The identity names one native
// lifecycle source lifetime: it never rotates while that source survives, and it
// never outlives it.
func NewStream(id string, negotiated Negotiated) *Stream {
	return &Stream{id: id, reducer: NewReducer(Options{Negotiated: negotiated})}
}

// ID reports the incarnation this stream speaks for.
func (s *Stream) ID() string { return s.id }

// State returns the projection the emitted stream proves.
func (s *Stream) State() State { return s.reducer.State() }

// Sequence reports the highest sequence this stream has claimed. A quiescence
// watermark reads it to name the work its proof covers.
func (s *Stream) Sequence() uint64 { return s.sequence }

// Fence ends the incarnation. A fenced stream is terminal: nothing may be
// emitted on it again, and the next incarnation opens its own stream with its
// own sequence space.
func (s *Stream) Fence() { s.fenced = true }

// Fenced reports whether the incarnation has ended.
func (s *Stream) Fenced() bool { return s.fenced }

// Emit claims the next sequence, reduces the event, and renders the envelope for
// the notification's `_meta`. A refused event is never rendered and its sequence
// stays consumed, which is exactly the detectable gap the ordering rule wants.
func (s *Stream) Emit(event Event) (map[string]any, error) {
	if s.fenced {
		return nil, violation(ViolationStaleStream, s.id, s.sequence, "the incarnation is fenced")
	}

	s.sequence++

	envelope := map[string]any{
		fieldVersion:  Version,
		fieldStreamID: s.id,
		fieldSequence: s.sequence,
		fieldEvent:    encodeEvent(event),
	}
	params, marshalErr := json.Marshal(map[string]any{
		updateField: map[string]any{sessionUpdateField: string(CarrierSessionInfo)},
		metaField:   map[string]any{MetaKey: envelope},
	})
	if marshalErr != nil {
		return nil, violation(ViolationMalformedEnvelope, s.id, s.sequence, marshalErr.Error())
	}

	delivery, err := DecodeSessionUpdate(params, s.reducer.negotiated)
	if err == nil {
		err = s.reducer.Reduce(delivery)
	}
	if err != nil {
		return nil, err
	}

	return envelope, nil
}

// SnapshotEvent opens a stream from the whole state this adapter can state
// truthfully. A freshly launched native generation holds nothing live, so the
// nonterminal sets are empty and the quiescence fact is whatever the
// configuration's proof class actually established at the last boundary.
func SnapshotEvent(cycleID string, quiescence QuiescenceFact) Event {
	return Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundIdle, CycleID: cycleID},
		Quiescence: quiescence,
	}}
}

// AcceptedEvent records that the native dispatcher took durable ownership of a
// submitted frame. The submission identity is echoed verbatim from the prompt's
// correlation value.
func AcceptedEvent(submission Submission, turnID string) Event {
	return Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: submission.SubmissionID,
		ClientNonce:  submission.ClientNonce,
		TurnID:       turnID,
		RunID:        submission.RunID,
	}}
}

// RunningEvent opens or resumes the foreground cycle a submission caused.
func RunningEvent(cycleID, turnID string) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:   ForegroundRunning,
		CycleID: cycleID,
		TurnID:  turnID,
		Cause:   CauseSubmission,
	}}
}

// RequiresActionEvent reports the foreground cycle a blocking action stopped.
// The action's own record is always emitted first: the resolution is the reason
// the foreground may move, so the two are never derived from one another.
func RequiresActionEvent(cycleID, turnID string) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:   ForegroundRequiresAction,
		CycleID: cycleID,
		TurnID:  turnID,
		Cause:   CauseSubmission,
	}}
}

// IdleEvent ends the cycle a submission caused, carrying the turn's truthful stop
// reason and recorded outcome.
func IdleEvent(cycleID, turnID, stopReason string, outcome Outcome) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:      ForegroundIdle,
		CycleID:    cycleID,
		TurnID:     turnID,
		Cause:      CauseSubmission,
		StopReason: stopReason,
		Outcome:    outcome,
	}}
}

// ActionEvent records one permission or elicitation's state. A first sight
// carries every member that fixes what the action is; a later patch carries the
// identity and the state it resolved to.
func ActionEvent(action ActionUpdate) Event {
	return Event{Type: EventActionUpdate, Action: &action}
}

// BlockingAction builds a blocking action's first sight, owned by the turn it
// stops.
func BlockingAction(actionID string, kind ActionKind, turnID string) ActionUpdate {
	blocks := true

	return ActionUpdate{
		ActionID:         actionID,
		Kind:             kind,
		State:            ActionPending,
		Owner:            Owner{Type: OwnerTurn, ID: turnID},
		BlocksForeground: &blocks,
	}
}

// ResolvedAction builds the later patch that terminalizes an announced action.
func ResolvedAction(actionID string, state ActionState) ActionUpdate {
	return ActionUpdate{ActionID: actionID, State: state}
}

// QuiescenceEvent states the authoritative quiescence fact a completed proof
// produced. It carries the proof class and the watermark that proof covers, never
// a guess, a heuristic, or a confidence.
func QuiescenceEvent(fact QuiescenceFact) Event {
	return Event{Type: EventQuiescenceUpdate, Quiescence: &fact}
}

// encodeEvent renders one event for the wire. Every member of the closed set has
// an encoder here, because the reducer that validates this adapter's own stream
// reduces all six and an event it can reduce but not render would be a rule with
// no expression.
func encodeEvent(event Event) map[string]any {
	switch event.Type {
	case EventSnapshot:
		return encodeSnapshot(*event.Snapshot)
	case EventPromptAccepted:
		return withOptional(map[string]any{
			fieldType:         string(EventPromptAccepted),
			fieldSubmissionID: event.PromptAccepted.SubmissionID,
			fieldClientNonce:  event.PromptAccepted.ClientNonce,
			fieldTurnID:       event.PromptAccepted.TurnID,
		}, fieldRunID, event.PromptAccepted.RunID)
	case EventStateUpdate:
		return encodeTransition(*event.State)
	case EventActivityUpdate:
		return map[string]any{
			fieldType:     string(EventActivityUpdate),
			fieldActivity: encodeActivity(*event.Activity),
		}
	case EventActionUpdate:
		return map[string]any{
			fieldType:   string(EventActionUpdate),
			fieldAction: encodeAction(*event.Action),
		}
	default:
		fact := encodeQuiescence(*event.Quiescence)
		fact[fieldType] = string(EventQuiescenceUpdate)

		return fact
	}
}

func encodeSnapshot(snapshot Snapshot) map[string]any {
	foreground := map[string]any{
		fieldState:   string(snapshot.Foreground.State),
		fieldCycleID: snapshot.Foreground.CycleID,
	}
	withOptional(foreground, fieldTurnID, snapshot.Foreground.TurnID)
	withOptional(foreground, fieldOrigin, string(snapshot.Foreground.Origin))

	activities := make([]any, 0, len(snapshot.Activities))
	for index := range snapshot.Activities {
		activities = append(activities, encodeActivity(snapshot.Activities[index]))
	}

	actions := make([]any, 0, len(snapshot.Actions))
	for index := range snapshot.Actions {
		actions = append(actions, encodeAction(snapshot.Actions[index]))
	}

	return map[string]any{
		fieldType:       string(EventSnapshot),
		fieldForeground: foreground,
		fieldActivities: activities,
		fieldActions:    actions,
		fieldQuiescence: encodeQuiescence(snapshot.Quiescence),
	}
}

func encodeTransition(transition StateTransition) map[string]any {
	encoded := map[string]any{
		fieldType:    string(EventStateUpdate),
		fieldState:   string(transition.State),
		fieldCycleID: transition.CycleID,
		fieldCause:   string(transition.Cause),
	}
	withOptional(encoded, fieldTurnID, transition.TurnID)
	withOptional(encoded, fieldStopReason, transition.StopReason)
	withOptional(encoded, fieldOutcome, string(transition.Outcome))

	return encoded
}

func encodeActivity(activity ActivityUpdate) map[string]any {
	encoded := map[string]any{
		fieldActivityID: activity.ActivityID,
		fieldState:      string(activity.State),
	}
	withOptional(encoded, fieldKind, string(activity.Kind))
	withOptional(encoded, fieldParentID, activity.ParentID)
	withOptional(encoded, fieldToolCallID, activity.ToolCallID)
	withOptional(encoded, fieldCause, string(activity.Cause))
	withOptional(encoded, fieldOriginTurnID, activity.OriginTurnID)
	withOptional(encoded, fieldRunID, activity.RunID)

	if activity.Progress != nil {
		encoded[fieldProgress] = activity.Progress
	}

	return encoded
}

// encodeAction renders one action. A later patch restates no immutable member,
// so the owner and the blocking claim appear only where the first sight stated
// them.
func encodeAction(action ActionUpdate) map[string]any {
	encoded := map[string]any{
		fieldActionID: action.ActionID,
		fieldState:    string(action.State),
	}
	withOptional(encoded, fieldKind, string(action.Kind))
	withOptional(encoded, fieldRunID, action.RunID)

	if action.Owner.ID != "" {
		encoded[fieldOwner] = map[string]any{
			fieldType: string(action.Owner.Type),
			fieldID:   action.Owner.ID,
		}
	}

	if action.BlocksForeground != nil {
		encoded[fieldBlocksForeground] = *action.BlocksForeground
	}

	return encoded
}

// encodeQuiescence renders a fact's members. A negative fact carries no proof at
// all: `source` is present if and only if the fact is positive, and it is never a
// `none` sentinel.
func encodeQuiescence(fact QuiescenceFact) map[string]any {
	if !fact.Quiescent {
		return map[string]any{fieldQuiescent: false}
	}

	encoded := map[string]any{
		fieldQuiescent: true,
		fieldSource:    string(fact.Source),
		fieldWatermark: fact.Watermark,
	}

	return withOptional(encoded, fieldBarrier, fact.Barrier)
}

// withOptional adds a member only when it has a value. An optional member is
// omitted rather than emitted empty, because an empty opaque identifier fails
// closed on the reading side.
func withOptional(encoded map[string]any, key, value string) map[string]any {
	if value != "" {
		encoded[key] = value
	}

	return encoded
}
