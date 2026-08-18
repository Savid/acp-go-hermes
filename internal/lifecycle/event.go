package lifecycle

import "encoding/json"

// The fixed member names of the envelope, the six events, and the negotiation
// and correlation objects. They are named once so the decoder's unknown-member
// rule and the emitter's construction cannot drift apart.
const (
	fieldVersion  = "version"
	fieldVersions = "versions"
	fieldStreamID = "streamId"
	fieldSequence = "sequence"
	fieldEvent    = "event"
	fieldType     = "type"

	fieldForeground = "foreground"
	fieldActivities = "activities"
	fieldActions    = "actions"
	fieldQuiescence = "quiescence"

	fieldState   = "state"
	fieldCycleID = "cycleId"
	fieldTurnID  = "turnId"
	fieldCause   = "cause"
	fieldOrigin  = "origin"

	fieldSubmissionID = "submissionId"
	fieldClientNonce  = "clientNonce"
	fieldRunID        = "runId"
	fieldStopReason   = "stopReason"
	fieldOutcome      = "outcome"

	fieldActivity     = "activity"
	fieldActivityID   = "activityId"
	fieldKind         = "kind"
	fieldParentID     = "parentId"
	fieldToolCallID   = "toolCallId"
	fieldOriginTurnID = "originTurnId"
	fieldProgress     = "progress"

	fieldAction           = "action"
	fieldActionID         = "actionId"
	fieldOwner            = "owner"
	fieldBlocksForeground = "blocksForeground"
	fieldID               = "id"

	fieldQuiescent = "quiescent"
	fieldSource    = "source"
	fieldWatermark = "watermark"
	fieldBarrier   = "barrier"

	fieldUpdatesOutsidePrompt    = "updatesOutsidePrompt"
	fieldAuthoritativeQuiescence = "authoritativeQuiescence"
	fieldQuiescenceSource        = "quiescenceSource"
	fieldActivityKinds           = "activityKinds"

	fieldSubmission = "submission"
)

// Event is one decoded lifecycle event. Exactly one payload pointer matches Type,
// and the decoder is the only producer, so a reducer never has to defend against
// a discriminant without its payload.
type Event struct {
	Type           EventType
	Snapshot       *Snapshot
	PromptAccepted *PromptAccepted
	State          *StateTransition
	Activity       *ActivityUpdate
	Action         *ActionUpdate
	Quiescence     *QuiescenceFact
}

// Snapshot opens a stream with the whole truth it can state: the foreground state
// and cycle, the complete nonterminal activity and action sets, and the current
// quiescence fact with its proof source.
type Snapshot struct {
	Foreground Foreground
	Activities []ActivityUpdate
	Actions    []ActionUpdate
	Quiescence QuiescenceFact
}

// Foreground is one foreground cycle. TurnID is empty while no turn holds it,
// and Origin names that turn's provenance exactly while it does: a resumed turn
// with no recorded origin would be a turn a consumer could not attribute. Origin
// is the snapshot's own member and is not projected — the turn record it opens
// carries it instead.
type Foreground struct {
	State   ForegroundState `json:"state"`
	CycleID string          `json:"cycleId"`
	TurnID  string          `json:"turnId,omitempty"`
	Origin  Cause           `json:"-"`
}

// PromptAccepted records that the native dispatcher took durable ownership of a
// submitted frame. Acceptance is never inferred from a running transition.
type PromptAccepted struct {
	SubmissionID string
	ClientNonce  string
	TurnID       string
	RunID        string
}

// StateTransition is one foreground state change. Only a transition that ends
// work carries a stop reason and an outcome.
type StateTransition struct {
	State      ForegroundState
	CycleID    string
	TurnID     string
	Cause      Cause
	StopReason string
	Outcome    Outcome
}

// endingIdleDefect reports why an idle transition that settles a turn is
// structurally incomplete, or the empty string when it is not. An idle naming a
// turn ends it, so the outcome is always required; the stop reason is required
// with it except on a failure, where no ACP v1 stop reason names one and the v1
// error carries it instead.
//
// Both the decoder and the reducer consult it, so an event this adapter emits is
// held to the same rule as one it reads.
func endingIdleDefect(transition StateTransition) string {
	if transition.State != ForegroundIdle || transition.TurnID == "" {
		return ""
	}

	switch {
	case transition.Outcome == "":
		return "an idle transition that ends a turn records its outcome"
	case transition.Outcome == OutcomeFailed && transition.StopReason != "":
		return "a failed outcome states no stop reason"
	case transition.Outcome != OutcomeFailed && transition.StopReason == "":
		return "an idle transition that ends a turn records its stop reason"
	default:
		return ""
	}
}

// ActivityUpdate reports one activity. A first sight carries every immutable
// identity field; a later update carries state and progress only.
type ActivityUpdate struct {
	ActivityID   string
	Kind         ActivityKind
	State        ActivityState
	ParentID     string
	ToolCallID   string
	Cause        Cause
	OriginTurnID string
	RunID        string
	// Progress is the one member whose interior this contract does not fix: an
	// opaque object a host renders and never reduces. It still takes part in the
	// duplicate comparison, which the whole-frame fingerprint covers.
	Progress json.RawMessage
}

// ActionUpdate reports one permission or elicitation. Only an action blocking the
// current foreground cycle bears on requires_action.
//
// BlocksForeground is a pointer because absence and false are different facts: it
// is required on a first sight, and a later patch that omits it restates nothing.
type ActionUpdate struct {
	ActionID         string
	Kind             ActionKind
	State            ActionState
	Owner            Owner
	RunID            string
	BlocksForeground *bool
}

// QuiescenceFact is an authoritative quiescence claim with the proof that
// produced it. Watermark is the sequence the proof fences, and zero fences
// nothing.
type QuiescenceFact struct {
	Quiescent bool
	Source    ProofClass
	Watermark uint64
	Barrier   string
}

// CarrierClass reports whether an envelope rode the one carrier the extension
// permits. Every other session update carries per-entity reduction semantics a
// conformant consumer may legally coalesce, which would make the envelope
// unrecoverable.
type CarrierClass string

const (
	// CarrierUnknown is the zero value: a carrier that has not been classified
	// cannot be proven legal.
	CarrierUnknown CarrierClass = ""
	// CarrierSessionInfo names the identity-only session_info_update.
	CarrierSessionInfo CarrierClass = "session_info_update"
	// CarrierIneligible names every other carrier.
	CarrierIneligible CarrierClass = "ineligible"
)

// Delivery is one delivered lifecycle event with its ordering identity. That
// identity is (StreamID, Sequence): StreamID names the native lifecycle source
// incarnation rather than a transient connection.
type Delivery struct {
	StreamID string
	Sequence uint64
	Carrier  CarrierClass
	Event    Event
	// Frame is the whole delivered notification as a decoded value — envelope and
	// carrier together. Comparing decoded values is what distinguishes an exact
	// retransmission from a conflicting reuse of the same identity: key order and
	// insignificant whitespace are never differences, and nothing has to retain
	// raw bytes for the life of a session.
	Frame any
}
