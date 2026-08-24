// Package lifecycle implements the `acp-go.dev/lifecycle` extension: the closed
// event vocabulary, the strict wire decoder, and the reducer that validates one
// ordered session lifecycle stream.
//
// The reducer is deliberately self-contained rather than shared. Independently
// written reducers reaching the same verdict on the canonical fixture battery is
// what proves the wire contract is unambiguous; a shared package would prove
// only that one implementation agrees with itself.
//
// It reduces the whole event set, including events this adapter's own
// configuration never emits, because the same code validates the stream this
// adapter emits: a validator that only handles what the harness happens to
// produce cannot detect the case where the harness produces something else.
package lifecycle

import "slices"

const (
	// MetaKey is the family-reserved ACP `_meta` key every lifecycle value rides
	// under. It is a family literal, not a vendor namespace.
	MetaKey = "acp-go.dev/lifecycle"

	// Version is the only envelope version this package implements.
	Version = 1

	// IdentifierBound is the largest opaque identifier the extension carries. An
	// identifier is a correlation handle rather than a payload, so a longer one
	// fails closed.
	IdentifierBound = 4096
)

// ProofClass names how a configuration proves background quiescence. The set is
// closed: quiet time, prompt return, process silence, queue drainage, a drain
// window, and ACP idle are never members.
type ProofClass string

const (
	// ProofClassProcessContainment proves vacancy by containing the complete
	// native process tree.
	ProofClassProcessContainment ProofClass = "process-containment"
)

// Valid reports whether the proof class is the closed set's sole member.
func (c ProofClass) Valid() bool {
	return c == ProofClassProcessContainment
}

// ForegroundState is one foreground cycle's state. A finished cycle's outcome is
// recorded on its ending transition, never as an extra state.
type ForegroundState string

const (
	ForegroundRunning        ForegroundState = "running"
	ForegroundIdle           ForegroundState = "idle"
	ForegroundRequiresAction ForegroundState = "requires_action"
)

// Valid reports whether the foreground state is part of the closed vocabulary.
func (s ForegroundState) Valid() bool {
	switch s {
	case ForegroundRunning, ForegroundIdle, ForegroundRequiresAction:
		return true
	default:
		return false
	}
}

// Cause names what caused a foreground transition or an activity.
type Cause string

const (
	CauseSubmission Cause = "submission"
	CauseActivity   Cause = "activity"
	CauseSession    Cause = "session"
)

// Valid reports whether the cause is part of the closed vocabulary.
func (c Cause) Valid() bool {
	switch c {
	case CauseSubmission, CauseActivity, CauseSession:
		return true
	default:
		return false
	}
}

// Outcome is a finished cycle's recorded result.
type Outcome string

const (
	OutcomeSuccess   Outcome = "success"
	OutcomeRefused   Outcome = "refused"
	OutcomeCancelled Outcome = "cancelled"
	OutcomeLimit     Outcome = "limit"
	OutcomeFailed    Outcome = "failed"
)

// Valid reports whether the outcome is part of the closed vocabulary.
func (o Outcome) Valid() bool {
	switch o {
	case OutcomeSuccess, OutcomeRefused, OutcomeCancelled, OutcomeLimit, OutcomeFailed:
		return true
	default:
		return false
	}
}

// The standard ACP v1 stop reasons. The extension adds no variant of its own, so
// an ending transition carries exactly one of these.
const (
	StopReasonEndTurn         = "end_turn"
	StopReasonMaxTokens       = "max_tokens"
	StopReasonMaxTurnRequests = "max_turn_requests"
	StopReasonRefusal         = "refusal"
	StopReasonCancelled       = "cancelled"
)

// ValidStopReason reports whether a stop reason is one of the standard five.
func ValidStopReason(reason string) bool {
	switch reason {
	case StopReasonEndTurn, StopReasonMaxTokens, StopReasonMaxTurnRequests,
		StopReasonRefusal, StopReasonCancelled:
		return true
	default:
		return false
	}
}

// ActivityKind classifies one node of a session's owned activity tree.
type ActivityKind string

const (
	ActivityTask     ActivityKind = "task"
	ActivitySubagent ActivityKind = "subagent"
)

// Valid reports whether the kind is part of the closed activity vocabulary.
func (k ActivityKind) Valid() bool {
	switch k {
	case ActivityTask, ActivitySubagent:
		return true
	default:
		return false
	}
}

// ActivityState is one activity's state. The last three are terminal, and
// terminal is immutable.
type ActivityState string

const (
	ActivityPending        ActivityState = "pending"
	ActivityRunning        ActivityState = "running"
	ActivityRequiresAction ActivityState = "requires_action"
	ActivityCompleted      ActivityState = "completed"
	ActivityFailed         ActivityState = "failed"
	ActivityCancelled      ActivityState = "cancelled"
)

// Valid reports whether the state is part of the closed activity vocabulary.
func (s ActivityState) Valid() bool {
	switch s {
	case ActivityPending, ActivityRunning, ActivityRequiresAction,
		ActivityCompleted, ActivityFailed, ActivityCancelled:
		return true
	default:
		return false
	}
}

// Terminal reports whether the activity can never change state again.
func (s ActivityState) Terminal() bool {
	switch s {
	case ActivityCompleted, ActivityFailed, ActivityCancelled:
		return true
	default:
		return false
	}
}

// ActionKind classifies a request that holds work until it is answered.
type ActionKind string

const (
	ActionPermission  ActionKind = "permission"
	ActionElicitation ActionKind = "elicitation"
)

// Valid reports whether the kind is part of the closed action vocabulary.
func (k ActionKind) Valid() bool {
	return k == ActionPermission || k == ActionElicitation
}

// ActionState is one action's state. Everything but pending is terminal.
type ActionState string

const (
	ActionPending   ActionState = "pending"
	ActionAccepted  ActionState = "accepted"
	ActionDeclined  ActionState = "declined"
	ActionCancelled ActionState = "cancelled"
	ActionFailed    ActionState = "failed"
)

// Valid reports whether the state is part of the closed action vocabulary.
func (s ActionState) Valid() bool {
	switch s {
	case ActionPending, ActionAccepted, ActionDeclined, ActionCancelled, ActionFailed:
		return true
	default:
		return false
	}
}

// Terminal reports whether the action is resolved.
func (s ActionState) Terminal() bool {
	return s != ActionPending
}

// OwnerType names what an action belongs to.
type OwnerType string

const (
	OwnerTurn     OwnerType = "turn"
	OwnerActivity OwnerType = "activity"
)

// Valid reports whether the owner type is part of the closed vocabulary.
func (t OwnerType) Valid() bool {
	return t == OwnerTurn || t == OwnerActivity
}

// Owner identifies the turn or activity one action belongs to.
type Owner struct {
	Type OwnerType `json:"type"`
	ID   string    `json:"id"`
}

// EventType discriminates one lifecycle event. The set is closed at six.
type EventType string

const (
	EventSnapshot         EventType = "lifecycle_snapshot"
	EventPromptAccepted   EventType = "prompt_accepted"
	EventStateUpdate      EventType = "state_update"
	EventActivityUpdate   EventType = "activity_update"
	EventActionUpdate     EventType = "action_update"
	EventQuiescenceUpdate EventType = "quiescence_update"
)

// Negotiated carries the lifecycle facts one configuration proved at
// `initialize`. Every field is a proven structured fact: an empty activity-kind
// set and `UpdatesOutsidePrompt` false are truthful answers for a configuration
// where nothing can outlive a contained prompt.
type Negotiated struct {
	// Versions is the non-empty intersection of the host's offer and the
	// versions this configuration implements. The connection speaks the highest
	// member.
	Versions                []int          `json:"versions"`
	UpdatesOutsidePrompt    bool           `json:"updatesOutsidePrompt"`
	AuthoritativeQuiescence bool           `json:"authoritativeQuiescence"`
	QuiescenceSource        ProofClass     `json:"quiescenceSource,omitempty"`
	ActivityKinds           []ActivityKind `json:"activityKinds"`
}

// Present reports whether the configuration answered the lifecycle key at all.
// An absent answer makes every envelope, correlation value, and lifecycle fact
// illegal on the connection.
func (n Negotiated) Present() bool { return len(n.Versions) > 0 }

// SupportsVersion reports whether an envelope version is inside the negotiated
// set.
func (n Negotiated) SupportsVersion(version int) bool {
	return slices.Contains(n.Versions, version)
}

// NegotiatedVersion is the single integer every envelope and correlation value
// on the connection carries: the highest member of the intersection.
func (n Negotiated) NegotiatedVersion() int {
	if !n.Present() {
		return 0
	}

	return slices.Max(n.Versions)
}

// DeclaresActivityKind reports whether the answer advertised an activity kind. A
// kind it never advertised is a kind it cannot prove.
func (n Negotiated) DeclaresActivityKind(kind ActivityKind) bool {
	return slices.Contains(n.ActivityKinds, kind)
}

// Advertisement renders the answer for `InitializeResponse._meta`. The activity
// kinds are always an array and never null, and `quiescenceSource` is present
// exactly when a proof class was proven.
func (n Negotiated) Advertisement() map[string]any {
	kinds := make([]string, 0, len(n.ActivityKinds))
	for _, kind := range n.ActivityKinds {
		kinds = append(kinds, string(kind))
	}

	advertisement := map[string]any{
		fieldVersions:                slices.Clone(n.Versions),
		fieldUpdatesOutsidePrompt:    n.UpdatesOutsidePrompt,
		fieldAuthoritativeQuiescence: n.AuthoritativeQuiescence,
		fieldActivityKinds:           kinds,
	}

	if n.AuthoritativeQuiescence {
		advertisement[fieldQuiescenceSource] = string(n.QuiescenceSource)
	}

	return advertisement
}
