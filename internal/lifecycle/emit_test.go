package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// containedConfiguration is the answer a session whose containment proves
// whole-tree vacancy gives: a channel that survives between prompts, no activity
// kind, and the process-containment proof class.
func containedConfiguration() Negotiated {
	return Negotiated{
		Versions:                []int{Version},
		UpdatesOutsidePrompt:    true,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        ProofClassProcessContainment,
		ActivityKinds:           []ActivityKind{},
	}
}

// reduceEnvelopes replays emitted envelopes through a reducer over the one legal
// carrier, which is the only measure of wire legality that counts.
func reduceEnvelopes(t *testing.T, reducer *Reducer, envelopes []map[string]any) {
	t.Helper()

	for index, envelope := range envelopes {
		params, err := json.Marshal(map[string]any{
			"sessionId": "sess-1",
			"update":    map[string]any{sessionUpdateField: string(CarrierSessionInfo)},
			metaField:   map[string]any{MetaKey: envelope},
		})
		require.NoError(t, err)
		require.NoError(t, reducer.ReduceSessionUpdate(params), "envelope %d", index)
	}
}

func emitAll(t *testing.T, stream *Stream, events ...Event) []map[string]any {
	t.Helper()

	envelopes := make([]map[string]any, 0, len(events))

	for index, event := range events {
		envelope, err := stream.Emit(event)
		require.NoError(t, err, "event %d", index)

		envelopes = append(envelopes, envelope)
	}

	return envelopes
}

// TestEmittedStreamReducesThroughTheSameReducer proves the emitted bytes are
// wire-legal by decoding them from a session/update notification and reducing
// them through the reducer the family battery drives.
func TestEmittedStreamReducesThroughTheSameReducer(t *testing.T) {
	t.Parallel()

	negotiated := containedConfiguration()
	stream := NewStream("strm-1", negotiated)
	submission := Submission{SubmissionID: "sub-1", ClientNonce: "non-1", RunID: "run-1"}

	envelopes := emitAll(t, stream,
		SnapshotEvent("cyc-0", QuiescenceFact{}),
		AcceptedEvent(submission, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
	)

	settled := QuiescenceFact{
		Quiescent: true,
		Source:    ProofClassProcessContainment,
		Watermark: stream.State().ReducedThrough,
		Barrier:   "contained-exit-1",
	}
	envelopes = append(envelopes, emitAll(t, stream, QuiescenceEvent(settled))...)

	reducer := NewReducer(Options{Negotiated: negotiated})
	reduceEnvelopes(t, reducer, envelopes)

	state := reducer.State()
	require.Equal(t, "strm-1", state.StreamID)
	require.Equal(t, uint64(5), state.ReducedThrough)
	require.Equal(t, []TurnRecord{{
		TurnID:       "turn-1",
		Origin:       CauseSubmission,
		Terminal:     true,
		Outcome:      OutcomeSuccess,
		SubmissionID: "sub-1",
		ClientNonce:  "non-1",
		RunID:        "run-1",
		CycleID:      "cyc-1",
		StopReason:   StopReasonEndTurn,
	}}, state.Turns)
	require.True(t, state.Quiescence.Certified)
	require.Equal(t, uint64(4), state.Quiescence.Watermark)
	require.Equal(t, "contained-exit-1", state.Quiescence.Barrier)
	require.Equal(t, uint64(5), stream.Sequence())
}

// TestEmittedBlockingActionReducesAsRequiresAction proves the action surface this
// adapter really emits: a permission announces its own record first, the
// foreground reports the cycle that record blocked, and the resolution
// terminalizes the blocker before the cycle moves again.
func TestEmittedBlockingActionReducesAsRequiresAction(t *testing.T) {
	t.Parallel()

	negotiated := containedConfiguration()
	stream := NewStream("strm-1", negotiated)

	envelopes := emitAll(t, stream,
		SnapshotEvent("cyc-0", QuiescenceFact{}),
		AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		ActionEvent(BlockingAction("act-1", ActionPermission, "turn-1")),
		RequiresActionEvent("cyc-1", "turn-1"),
		ActionEvent(ResolvedAction("act-1", ActionAccepted)),
		RunningEvent("cyc-1", "turn-1"),
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
	)

	reducer := NewReducer(Options{Negotiated: negotiated})
	reduceEnvelopes(t, reducer, envelopes)

	state := reducer.State()
	require.Equal(t, []ActionRecord{{
		ActionID:         "act-1",
		Kind:             ActionPermission,
		State:            ActionAccepted,
		Owner:            Owner{Type: OwnerTurn, ID: "turn-1"},
		BlocksForeground: true,
	}}, state.Actions)
	require.Equal(t, ForegroundIdle, state.Foreground.State)
	require.True(t, state.Turns[0].Terminal)
}

// TestEmittedActivityPatchCarriesOnlyWhatItStates proves the activity encoder
// renders a first sight's whole identity and a later patch's state alone. This
// adapter's gateway vocabulary has no background activity entity, so nothing
// emits one; the encoder exists because the reducer that validates this stream
// reduces all six events and a rule with no expression could never be tested.
func TestEmittedActivityPatchCarriesOnlyWhatItStates(t *testing.T) {
	t.Parallel()

	progress := json.RawMessage(`{"done":1}`)
	first := encodeActivity(ActivityUpdate{
		ActivityID:   "act-1",
		Kind:         ActivityTask,
		State:        ActivityRunning,
		ParentID:     "act-0",
		ToolCallID:   "tool-1",
		Cause:        CauseSubmission,
		OriginTurnID: "turn-1",
		RunID:        "run-1",
		Progress:     progress,
	})
	require.Equal(t, map[string]any{
		fieldActivityID:   "act-1",
		fieldState:        string(ActivityRunning),
		fieldKind:         string(ActivityTask),
		fieldParentID:     "act-0",
		fieldToolCallID:   "tool-1",
		fieldCause:        string(CauseSubmission),
		fieldOriginTurnID: "turn-1",
		fieldRunID:        "run-1",
		fieldProgress:     progress,
	}, first)

	require.Equal(t, map[string]any{
		fieldActivityID: "act-1",
		fieldState:      string(ActivityCompleted),
	}, encodeActivity(ActivityUpdate{ActivityID: "act-1", State: ActivityCompleted}))

	negotiated := containedConfiguration()
	negotiated.ActivityKinds = []ActivityKind{ActivityTask}
	stream := NewStream("strm-1", negotiated)

	envelopes := emitAll(t, stream,
		SnapshotEvent("cyc-0", QuiescenceFact{}),
		AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		Event{Type: EventActivityUpdate, Activity: &ActivityUpdate{
			ActivityID:   "act-1",
			Kind:         ActivityTask,
			State:        ActivityRunning,
			Cause:        CauseSubmission,
			OriginTurnID: "turn-1",
		}},
		Event{Type: EventActivityUpdate, Activity: &ActivityUpdate{
			ActivityID: "act-1",
			State:      ActivityCompleted,
		}},
	)

	reducer := NewReducer(Options{Negotiated: negotiated})
	reduceEnvelopes(t, reducer, envelopes)

	recorded, known := reducer.State().Activity("act-1")
	require.True(t, known)
	require.Equal(t, ActivityCompleted, recorded.State)

	unopened, err := NewStream("strm-2", negotiated).Emit(Event{
		Type:     EventActivityUpdate,
		Activity: &ActivityUpdate{ActivityID: "act-1", State: ActivityCompleted},
	})
	require.Nil(t, unopened)
	require.ErrorAs(t, err, new(*ViolationError))
}

// TestEmittedSnapshotStatesAResumedTurnWholly proves the snapshot encoder renders
// a mid-turn resumption with the origin the contract requires beside the turn it
// names, and renders its complete nonterminal sets.
func TestEmittedSnapshotStatesAResumedTurnWholly(t *testing.T) {
	t.Parallel()

	blocks := true
	negotiated := containedConfiguration()
	negotiated.ActivityKinds = []ActivityKind{ActivityTask}

	envelope, err := NewStream("strm-1", negotiated).Emit(Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{
			State:   ForegroundRequiresAction,
			CycleID: "cyc-1",
			TurnID:  "turn-1",
			Origin:  CauseSubmission,
		},
		Activities: []ActivityUpdate{{
			ActivityID:   "act-1",
			Kind:         ActivityTask,
			State:        ActivityRunning,
			Cause:        CauseSubmission,
			OriginTurnID: "turn-1",
		}},
		Actions: []ActionUpdate{{
			ActionID:         "ask-1",
			Kind:             ActionElicitation,
			State:            ActionPending,
			Owner:            Owner{Type: OwnerTurn, ID: "turn-1"},
			BlocksForeground: &blocks,
		}},
	}})
	require.NoError(t, err)

	event, ok := envelope[fieldEvent].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{
		fieldState:   string(ForegroundRequiresAction),
		fieldCycleID: "cyc-1",
		fieldTurnID:  "turn-1",
		fieldOrigin:  string(CauseSubmission),
	}, event[fieldForeground])
	require.Len(t, event[fieldActivities], 1)
	require.Len(t, event[fieldActions], 1)
}

// TestEmitClaimsTheSequenceBeforeDelivery proves a refused event consumes its
// sequence: a counter that advanced only on success would make loss invisible,
// which is the exact failure contiguity exists to expose.
func TestEmitClaimsTheSequenceBeforeDelivery(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())

	_, err := stream.Emit(RunningEvent("cyc-1", "turn-1"))
	require.ErrorAs(t, err, new(*ViolationError))
	require.Equal(t, uint64(1), stream.Sequence())
}

// TestAFencedStreamEmitsNothingFurther proves the incarnation boundary is
// terminal on the emitting side too: a fenced stream never writes another event,
// so nothing can escape onto an identity a consumer has already refused.
func TestAFencedStreamEmitsNothingFurther(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())

	_, err := stream.Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)
	require.False(t, stream.Fenced())

	stream.Fence()
	require.True(t, stream.Fenced())

	envelope, err := stream.Emit(RunningEvent("cyc-1", "turn-1"))
	require.Nil(t, envelope)

	var refusal *ViolationError

	require.ErrorAs(t, err, &refusal)
	require.Equal(t, ViolationStaleStream, refusal.Kind)
	require.Equal(t, uint64(1), stream.Sequence())
}

// TestSnapshotStatesAnUnprovenBoundaryAsNotQuiescent proves a configuration with
// no proof class emits a negative fact rather than a `none` sentinel or a
// present-and-empty source.
func TestSnapshotStatesAnUnprovenBoundaryAsNotQuiescent(t *testing.T) {
	t.Parallel()

	degenerate := Negotiated{Versions: []int{Version}, ActivityKinds: []ActivityKind{}}

	envelope, err := NewStream("strm-1", degenerate).Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)

	event, ok := envelope[fieldEvent].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{fieldQuiescent: false}, event[fieldQuiescence])
}

// TestAcceptanceOmitsAnAbsentRunID proves an optional handle is omitted rather
// than emitted empty: an empty opaque identifier fails closed on the reader.
func TestAcceptanceOmitsAnAbsentRunID(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())

	_, err := stream.Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)

	envelope, err := stream.Emit(AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"))
	require.NoError(t, err)

	event, ok := envelope[fieldEvent].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, event, fieldRunID)
}

// TestActionCorrelationNamesTheStreamAndTheAction proves the value stamped on a
// held request carries exactly the emitting stream and the action's lifecycle
// identity, with the optional ownership root omitted rather than emptied.
func TestActionCorrelationNamesTheStreamAndTheAction(t *testing.T) {
	t.Parallel()

	require.Equal(t, map[string]any{
		fieldVersion:  Version,
		fieldStreamID: "strm-1",
		fieldAction: map[string]any{
			fieldActionID: "act-1",
			fieldOwner:    map[string]any{fieldType: string(OwnerTurn), fieldID: "turn-1"},
		},
	}, ActionCorrelation("strm-1", BlockingAction("act-1", ActionPermission, "turn-1")))

	rooted := BlockingAction("act-2", ActionElicitation, "turn-1")
	rooted.RunID = "run-1"

	correlation, ok := ActionCorrelation("strm-1", rooted)[fieldAction].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "run-1", correlation[fieldRunID])
}
