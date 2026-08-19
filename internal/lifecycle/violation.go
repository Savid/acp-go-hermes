package lifecycle

import "strconv"

// ViolationKind names one fail-closed verdict. The vocabulary is closed at the
// twenty tokens below: every rule this package enforces reports one of them,
// and a condition none of them names is not refusable.
type ViolationKind string

const (
	// ViolationUnsupportedVersion refuses an envelope or correlation value whose
	// version is outside the negotiated set.
	ViolationUnsupportedVersion ViolationKind = "unsupported_version"
	// ViolationUnknownField refuses any unknown member of the negotiation object,
	// the envelope, an event, or a correlation value.
	ViolationUnknownField ViolationKind = "unknown_field"
	// ViolationMalformedEnvelope refuses a missing, wrong-typed, or over-bound
	// member. Structural validity is checked before ordering, so a malformed
	// envelope never reports an ordering token.
	ViolationMalformedEnvelope ViolationKind = "malformed_envelope"
	// ViolationUnknownEventType refuses an event type outside the closed six.
	ViolationUnknownEventType ViolationKind = "unknown_event_type"
	// ViolationUnnegotiatedFact refuses an event asserting a fact the negotiated
	// answer did not claim, and any envelope at all when the answer omitted the
	// key.
	ViolationUnnegotiatedFact ViolationKind = "unnegotiated_fact"
	// ViolationIllegalCarrier refuses an envelope on any carrier other than the
	// identity-only session_info_update, and a non-object value at the key.
	ViolationIllegalCarrier ViolationKind = "illegal_carrier"
	// ViolationDeltaBeforeSnapshot refuses any event other than a snapshot as a
	// stream's first event.
	ViolationDeltaBeforeSnapshot ViolationKind = "delta_before_snapshot"
	// ViolationConflictingDuplicate refuses a repeated sequence identity whose
	// content differs from the reduced one.
	ViolationConflictingDuplicate ViolationKind = "conflicting_duplicate"
	// ViolationSequenceGap refuses a sequence that skips the expected next
	// number: contiguity is the loss detector.
	ViolationSequenceGap ViolationKind = "sequence_gap"
	// ViolationSequenceRegression refuses a sequence below the stream's reduced
	// range that was never reduced.
	ViolationSequenceRegression ViolationKind = "sequence_regression"
	// ViolationStreamCycle refuses a second snapshot on a live stream.
	ViolationStreamCycle ViolationKind = "stream_cycle"
	// ViolationStaleStream refuses an event from an incarnation already fenced or
	// superseded, and every event for a session whose close containment completed.
	ViolationStaleStream ViolationKind = "stale_stream"
	// ViolationPostTerminalMutation refuses an update carrying any difference
	// from the reduced terminal record of an activity, action, or turn already
	// terminal — a changed state, a changed carried member such as progress, or
	// a restated immutable at other than its first-sight value. A member-wise
	// no-op restatement is suppressed instead.
	ViolationPostTerminalMutation ViolationKind = "post_terminal_mutation"
	// ViolationImmutableIdentityChange refuses an update that changes an immutable
	// identity field, a first sight missing one, and an acceptance reusing a turn
	// the stream already introduced.
	ViolationImmutableIdentityChange ViolationKind = "immutable_identity_change"
	// ViolationInconsistentForeground refuses a foreground state with no blocker
	// behind it, a blocker with no foreground state beside it, and a transition
	// that releases or ends a cycle an action still blocks.
	ViolationInconsistentForeground ViolationKind = "inconsistent_foreground"
	// ViolationParentTerminalBeforeChild refuses a parent activity that
	// terminalizes while an owned descendant is nonterminal.
	ViolationParentTerminalBeforeChild ViolationKind = "parent_terminal_before_child"
	// ViolationChildAfterParentTerminal refuses a new child activity naming a
	// terminal parent.
	ViolationChildAfterParentTerminal ViolationKind = "child_after_parent_terminal"
	// ViolationLateCausalWork refuses a first-sight activity whose origin turn or
	// parent activity was first seen at or before a certified watermark.
	ViolationLateCausalWork ViolationKind = "late_causal_work"
	// ViolationFalseQuiescence refuses a positive quiescence fact the stream
	// carrying it disproves. A proof the stream disproves is a lie about the
	// boundary rather than a weaker claim, so it fails closed instead of quietly
	// certifying nothing: a swallowed lie is indistinguishable from a loss.
	ViolationFalseQuiescence ViolationKind = "false_quiescence"
	// ViolationUnknownEntity refuses an event naming an entity the stream never
	// introduced. Unknown references would make parentage, ownership, and
	// terminal-ordering rules unenforceable.
	ViolationUnknownEntity ViolationKind = "unknown_entity"
)

// violationVocabulary is the closed set in full. Enumerating it is what lets the
// fixture battery prove every token is pinned by a vector rather than merely
// declared here.
var violationVocabulary = []ViolationKind{
	ViolationUnsupportedVersion,
	ViolationUnknownField,
	ViolationMalformedEnvelope,
	ViolationUnknownEventType,
	ViolationUnnegotiatedFact,
	ViolationIllegalCarrier,
	ViolationDeltaBeforeSnapshot,
	ViolationConflictingDuplicate,
	ViolationSequenceGap,
	ViolationSequenceRegression,
	ViolationStreamCycle,
	ViolationStaleStream,
	ViolationPostTerminalMutation,
	ViolationImmutableIdentityChange,
	ViolationInconsistentForeground,
	ViolationParentTerminalBeforeChild,
	ViolationChildAfterParentTerminal,
	ViolationLateCausalWork,
	ViolationFalseQuiescence,
	ViolationUnknownEntity,
}

// ViolationError is one refusal. It names the offending ordering identity so a
// report can say exactly which frame failed closed.
type ViolationError struct {
	Kind     ViolationKind
	StreamID string
	Sequence uint64
	Detail   string
}

// Error implements error.
func (v *ViolationError) Error() string {
	message := "lifecycle violation " + string(v.Kind) +
		" at " + v.StreamID + "#" + strconv.FormatUint(v.Sequence, 10)
	if v.Detail != "" {
		message += ": " + v.Detail
	}

	return message
}

// Is matches by kind so a caller can assert a class of refusal without knowing
// the offending identity.
func (v *ViolationError) Is(target error) bool {
	other, ok := target.(*ViolationError)
	if !ok {
		return false
	}

	return other.Kind == v.Kind
}

func violation(kind ViolationKind, streamID string, sequence uint64, detail string) *ViolationError {
	return &ViolationError{Kind: kind, StreamID: streamID, Sequence: sequence, Detail: detail}
}
