package lifecycle

import (
	"encoding/json"
	"slices"
)

// State is the projection a reduced stream proves. It is the only surface a
// lifecycle fact may be read from: nothing in it is inferred from silence,
// timing, transcript text, or a poll.
//
// A projection belongs to one incarnation and never adopts a prior incarnation's
// turns, activities, actions, or watermark. Its encoded shape is the family
// fixture battery's: nine members, every one present.
type State struct {
	StreamID       string `json:"streamId"`
	ReducedThrough uint64 `json:"reducedThrough"`
	// SuppressedRetransmissions counts frames recognized as exact retransmissions
	// of an already-reduced identity.
	SuppressedRetransmissions int              `json:"suppressedRetransmissions"`
	Foreground                *Foreground      `json:"foreground"`
	Turns                     []TurnRecord     `json:"turns"`
	Activities                []ActivityRecord `json:"activities"`
	Actions                   []ActionRecord   `json:"actions"`
	Quiescence                QuiescenceState  `json:"quiescence"`
	// Closed reports that the incarnation is fenced: its descendants are contained
	// and no later event is possible from it.
	Closed bool `json:"closed"`
}

// TurnRecord is one foreground epoch. A terminal turn never reopens. The members
// beyond the projected four are correlation a consumer needs and the wire
// projection deliberately does not pin.
type TurnRecord struct {
	TurnID       string  `json:"turnId"`
	Origin       Cause   `json:"origin"`
	Terminal     bool    `json:"terminal"`
	Outcome      Outcome `json:"outcome,omitempty"`
	SubmissionID string  `json:"-"`
	ClientNonce  string  `json:"-"`
	RunID        string  `json:"-"`
	CycleID      string  `json:"-"`
	StopReason   string  `json:"-"`
}

// ActivityRecord is one activity's projection. Immutable identity is fixed on
// first sight; only state and progress ever change, and progress is rendered
// rather than reduced.
type ActivityRecord struct {
	ActivityID   string          `json:"activityId"`
	Kind         ActivityKind    `json:"kind"`
	State        ActivityState   `json:"state"`
	ParentID     string          `json:"parentId,omitempty"`
	ToolCallID   string          `json:"toolCallId,omitempty"`
	Cause        Cause           `json:"-"`
	OriginTurnID string          `json:"originTurnId"`
	RunID        string          `json:"runId,omitempty"`
	Progress     json.RawMessage `json:"-"`
}

// ActionRecord is one action's projection.
type ActionRecord struct {
	ActionID         string      `json:"actionId"`
	Kind             ActionKind  `json:"kind"`
	State            ActionState `json:"state"`
	Owner            Owner       `json:"owner"`
	RunID            string      `json:"runId,omitempty"`
	BlocksForeground bool        `json:"blocksForeground"`
}

// QuiescenceState is the current certification. Certified is true only while an
// authoritative fact covers every transition reduced so far; InvalidatedAt records
// the sequence that revoked the last certification.
type QuiescenceState struct {
	Certified     bool
	Source        ProofClass
	Watermark     uint64
	Barrier       string
	InvalidatedAt uint64
}

// MarshalJSON projects the proof a certified fact carried, or the sequence that
// revoked the last one. The two sets never appear together.
func (q QuiescenceState) MarshalJSON() ([]byte, error) {
	projected := map[string]any{"certified": q.Certified}

	switch {
	case q.Certified:
		projected[fieldSource] = q.Source
		projected[fieldWatermark] = q.Watermark

		if q.Barrier != "" {
			projected[fieldBarrier] = q.Barrier
		}
	case q.InvalidatedAt > 0:
		projected["invalidatedAt"] = q.InvalidatedAt
	}

	//nolint:wrapcheck // The projected shape is this method's whole subject.
	return json.Marshal(projected)
}

// Vacant reports whether nothing the stream knows about is live. Vacancy is not
// quiescence: it is the precondition an emitter observes before it may claim one.
func (s State) Vacant() bool {
	if s.Foreground != nil && s.Foreground.State != ForegroundIdle {
		return false
	}

	for index := range s.Activities {
		if !s.Activities[index].State.Terminal() {
			return false
		}
	}

	for _, action := range s.Actions {
		if !action.State.Terminal() {
			return false
		}
	}

	return true
}

// Activity returns one activity's projection.
func (s State) Activity(activityID string) (ActivityRecord, bool) {
	if index := indexOf(s.Activities, activityID, ActivityRecord.identity); index >= 0 {
		return s.Activities[index], true
	}

	return ActivityRecord{}, false
}

// Action returns one action's projection.
func (s State) Action(actionID string) (ActionRecord, bool) {
	if index := indexOf(s.Actions, actionID, ActionRecord.identity); index >= 0 {
		return s.Actions[index], true
	}

	return ActionRecord{}, false
}

// Turn returns one turn's projection.
func (s State) Turn(turnID string) (TurnRecord, bool) {
	if index := indexOf(s.Turns, turnID, TurnRecord.identity); index >= 0 {
		return s.Turns[index], true
	}

	return TurnRecord{}, false
}

func (r TurnRecord) identity() string     { return r.TurnID }
func (r ActivityRecord) identity() string { return r.ActivityID }
func (r ActionRecord) identity() string   { return r.ActionID }

// indexOf locates a record by its identity. The projections are small ordered
// slices rather than maps because the fixture battery pins their order.
func indexOf[T any](records []T, id string, identity func(T) string) int {
	for index, record := range records {
		if identity(record) == id {
			return index
		}
	}

	return -1
}

func (s State) clone() State {
	out := s
	if s.Foreground != nil {
		foreground := *s.Foreground
		out.Foreground = &foreground
	}

	out.Turns = cloneRecords(s.Turns)
	out.Activities = cloneRecords(s.Activities)
	out.Actions = cloneRecords(s.Actions)

	return out
}

// cloneRecords keeps a projected collection non-nil so a consumer never has to
// distinguish "no entries" from "not populated".
func cloneRecords[T any](in []T) []T {
	if len(in) == 0 {
		return []T{}
	}

	return slices.Clone(in)
}
