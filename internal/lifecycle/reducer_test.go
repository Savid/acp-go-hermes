package lifecycle

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// deliver builds one delivery on the canonical stream. Reducing a delivery
// directly is how the emitter validates an event it is about to publish, so these
// vectors exercise the same entry point Stream.Emit uses.
func deliver(sequence uint64, event Event) Delivery {
	return Delivery{StreamID: "strm", Sequence: sequence, Carrier: CarrierSessionInfo, Event: event}
}

// openSnapshot is an empty whole-state assertion with no certified boundary.
func openSnapshot() Event {
	return Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundIdle, CycleID: "cyc-0"},
	}}
}

// reduceAll reduces events at contiguous sequences from one and reports the first
// refusal.
func reduceAll(t *testing.T, negotiated Negotiated, events ...Event) (*Reducer, *ViolationError) {
	t.Helper()

	reducer := NewReducer(Options{Negotiated: negotiated})

	for index, event := range events {
		err := reducer.Reduce(deliver(uint64(index+1), event))
		if err == nil {
			continue
		}

		var refusal *ViolationError

		require.ErrorAs(t, err, &refusal)

		return reducer, refusal
	}

	return reducer, nil
}

func requireReduceRefusal(t *testing.T, negotiated Negotiated, kind ViolationKind, events ...Event) {
	t.Helper()

	_, refusal := reduceAll(t, negotiated, events...)
	require.NotNil(t, refusal)
	require.Equal(t, kind, refusal.Kind)
}

// activityEvent builds one activity update.
func activityEvent(update ActivityUpdate) Event {
	return Event{Type: EventActivityUpdate, Activity: &update}
}

// actionEvent builds one action update.
func actionEvent(update ActionUpdate) Event {
	return Event{Type: EventActionUpdate, Action: &update}
}

// stated marks a blocking claim an action's first sight actually made, which is
// the difference an omitted member may not be read as.
func stated(blocks bool) *bool { return &blocks }

// TestReducerRefusesAnEventWithNoPayload pins that a discriminant without its
// payload is malformed rather than reduced as an empty event. Only an emitter can
// produce one: the decoder never yields a discriminant it could not read.
func TestReducerRefusesAnEventWithNoPayload(t *testing.T) {
	t.Parallel()

	requireReduceRefusal(t, richConfiguration(), ViolationMalformedEnvelope, Event{Type: EventSnapshot})

	for _, event := range []Event{
		{Type: EventPromptAccepted},
		{Type: EventStateUpdate},
		{Type: EventActivityUpdate},
		{Type: EventActionUpdate},
		{Type: EventQuiescenceUpdate},
	} {
		requireReduceRefusal(t, richConfiguration(), ViolationMalformedEnvelope, openSnapshot(), event)
	}
}

// TestReducerRefusesAnIncompleteSnapshotForeground pins that a stream's opening
// whole-state assertion states a complete foreground or opens nothing.
func TestReducerRefusesAnIncompleteSnapshotForeground(t *testing.T) {
	t.Parallel()

	for _, foreground := range []Foreground{
		{CycleID: "cyc-0"},
		{State: ForegroundIdle},
	} {
		requireReduceRefusal(t, richConfiguration(), ViolationMalformedEnvelope,
			Event{Type: EventSnapshot, Snapshot: &Snapshot{Foreground: foreground}})
	}
}

// TestSnapshotIntroducesEveryIdentityItNames pins that the state a snapshot
// asserts predates the stream: its foreground turn, its activities' origin turns,
// and its actions' owners are all introduced by the assertion itself.
func TestSnapshotIntroducesEveryIdentityItNames(t *testing.T) {
	t.Parallel()

	reducer, refusal := reduceAll(t, richConfiguration(), Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSubmission},
		Activities: []ActivityUpdate{
			{ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning, Cause: CauseSubmission, OriginTurnID: "turn-1"},
			{ActivityID: "act-2", Kind: ActivitySubagent, State: ActivityRunning, Cause: CauseActivity, OriginTurnID: "turn-1", ParentID: "act-1"},
		},
		Actions: []ActionUpdate{{
			ActionID: "req-1", Kind: ActionPermission, State: ActionPending,
			Owner: Owner{Type: OwnerTurn, ID: "turn-1"}, BlocksForeground: stated(false),
		}},
		Quiescence: QuiescenceFact{},
	}})
	require.Nil(t, refusal)

	state := reducer.State()
	require.Len(t, state.Activities, 2)
	require.Len(t, state.Actions, 1)

	activity, found := state.Activity("act-2")
	require.True(t, found)
	require.Equal(t, "act-1", activity.ParentID)

	action, found := state.Action("req-1")
	require.True(t, found)
	require.Equal(t, Owner{Type: OwnerTurn, ID: "turn-1"}, action.Owner)

	turn, found := state.Turn("turn-1")
	require.True(t, found, "a snapshot resuming mid-turn projects that turn as open")
	require.Equal(t, CauseSubmission, turn.Origin)
	require.False(t, state.Vacant(), "live work is not vacancy")
}

// TestSnapshotRefusesAnIncompleteEntity pins that the sets a snapshot carries are
// held to the same first-sight rules a later one is, and to the asymmetry
// between them: an activity missing an immutable identity field reports that,
// while an action missing a member of its first sight is malformed.
func TestSnapshotRefusesAnIncompleteEntity(t *testing.T) {
	t.Parallel()

	requireReduceRefusal(t, richConfiguration(), ViolationImmutableIdentityChange,
		Event{Type: EventSnapshot, Snapshot: &Snapshot{
			Foreground: Foreground{State: ForegroundIdle, CycleID: "cyc-0"},
			Activities: []ActivityUpdate{{ActivityID: "act-1", State: ActivityRunning}},
		}})

	requireReduceRefusal(t, richConfiguration(), ViolationMalformedEnvelope,
		Event{Type: EventSnapshot, Snapshot: &Snapshot{
			Foreground: Foreground{State: ForegroundIdle, CycleID: "cyc-0"},
			Actions:    []ActionUpdate{{ActionID: "req-1", State: ActionPending}},
		}})
}

// TestReducerRefusesAnUnnegotiatedActivityKind pins that a kind outside the
// answer's set asserts a fact the configuration never claimed.
func TestReducerRefusesAnUnnegotiatedActivityKind(t *testing.T) {
	t.Parallel()

	degenerate := Negotiated{Versions: []int{Version}, ActivityKinds: []ActivityKind{}}

	requireReduceRefusal(t, degenerate, ViolationUnnegotiatedFact,
		Event{Type: EventSnapshot, Snapshot: &Snapshot{
			Foreground: Foreground{State: ForegroundIdle, CycleID: "cyc-0"},
			Activities: []ActivityUpdate{
				{ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning, Cause: CauseSession, OriginTurnID: "turn-1"},
			},
		}})
}

// TestTurnNeverReopens pins that a terminal turn is final on every path that
// names it again.
func TestTurnNeverReopens(t *testing.T) {
	t.Parallel()

	accepted := Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
	}}
	idle := IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess)

	for _, event := range []Event{
		accepted,
		RunningEvent("cyc-1", "turn-1"),
		idle,
	} {
		requireReduceRefusal(t, richConfiguration(), ViolationPostTerminalMutation,
			openSnapshot(), accepted, RunningEvent("cyc-1", "turn-1"), idle, event)
	}
}

// TestForegroundTransitionsResolveTheirTurn pins that a transition naming a turn
// the stream never introduced is an unknown entity, and that a session-caused
// idle with no turn simply closes the cycle.
func TestForegroundTransitionsResolveTheirTurn(t *testing.T) {
	t.Parallel()

	requireReduceRefusal(t, richConfiguration(), ViolationUnknownEntity,
		openSnapshot(), RunningEvent("cyc-1", "turn-ghost"))

	requireReduceRefusal(t, richConfiguration(), ViolationUnknownEntity,
		openSnapshot(), IdleEvent("cyc-1", "turn-ghost", StopReasonEndTurn, OutcomeSuccess))

	reducer, refusal := reduceAll(t, richConfiguration(), openSnapshot(),
		Event{Type: EventStateUpdate, State: &StateTransition{State: ForegroundIdle, CycleID: "cyc-1", Cause: CauseSession}})
	require.Nil(t, refusal)
	require.Equal(t, &Foreground{State: ForegroundIdle, CycleID: "cyc-1"}, reducer.State().Foreground)
}

// TestSnapshotForegroundTurnSettlesWithoutAcceptance pins that a turn the
// snapshot introduced can end even though no acceptance opened it here.
func TestSnapshotForegroundTurnSettlesWithoutAcceptance(t *testing.T) {
	t.Parallel()

	reducer, refusal := reduceAll(t, richConfiguration(),
		Event{Type: EventSnapshot, Snapshot: &Snapshot{
			Foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1"},
		}},
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
	)
	require.Nil(t, refusal)

	turn, found := reducer.State().Turn("turn-1")
	require.True(t, found)
	require.True(t, turn.Terminal)
}

// TestBlockingActionOwesItsForegroundTransition pins the second half of the
// blocking-action rule: a blocked cycle reports requires_action before it reports
// anything else.
func TestBlockingActionOwesItsForegroundTransition(t *testing.T) {
	t.Parallel()

	accepted := Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
	}}
	blocking := actionEvent(ActionUpdate{
		ActionID: "req-1", Kind: ActionPermission, State: ActionPending,
		Owner: Owner{Type: OwnerTurn, ID: "turn-1"}, BlocksForeground: stated(true),
	})

	requireReduceRefusal(t, richConfiguration(), ViolationInconsistentForeground,
		openSnapshot(), accepted, RunningEvent("cyc-1", "turn-1"), blocking,
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess))
}

// TestEmittedEndingIdleRecordsHowItSettled pins the rule on the emitter's own
// path: the reducer an emitted event is validated through holds an ending idle
// to the same structure a decoded one is, so an event this adapter cannot state
// truthfully fails at the point of emission.
func TestEmittedEndingIdleRecordsHowItSettled(t *testing.T) {
	t.Parallel()

	accepted := Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
	}}

	for _, ending := range []StateTransition{
		{State: ForegroundIdle, CycleID: "cyc-1", TurnID: "turn-1", Cause: CauseSubmission, StopReason: StopReasonEndTurn},
		{State: ForegroundIdle, CycleID: "cyc-1", TurnID: "turn-1", Cause: CauseSubmission, Outcome: OutcomeSuccess},
		{
			State: ForegroundIdle, CycleID: "cyc-1", TurnID: "turn-1", Cause: CauseSubmission,
			StopReason: StopReasonEndTurn, Outcome: OutcomeFailed,
		},
	} {
		requireReduceRefusal(t, richConfiguration(), ViolationMalformedEnvelope,
			openSnapshot(), accepted, RunningEvent("cyc-1", "turn-1"),
			Event{Type: EventStateUpdate, State: &ending})
	}
}

// TestActivityIdentityIsImmutable pins every restated identity field.
func TestActivityIdentityIsImmutable(t *testing.T) {
	t.Parallel()

	first := ActivityUpdate{
		ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning,
		Cause: CauseSession, OriginTurnID: "turn-1", ParentID: "", ToolCallID: "tool-1", RunID: "run-1",
	}
	opening := Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundIdle, CycleID: "cyc-0"},
		Activities: []ActivityUpdate{first},
	}}

	for _, patch := range []ActivityUpdate{
		{ActivityID: "act-1", State: ActivityRunning, Kind: ActivityMonitor},
		{ActivityID: "act-1", State: ActivityRunning, ParentID: "act-9"},
		{ActivityID: "act-1", State: ActivityRunning, ToolCallID: "tool-9"},
		{ActivityID: "act-1", State: ActivityRunning, Cause: CauseActivity},
		{ActivityID: "act-1", State: ActivityRunning, OriginTurnID: "turn-9"},
		{ActivityID: "act-1", State: ActivityRunning, RunID: "run-9"},
	} {
		requireReduceRefusal(t, richConfiguration(), ViolationImmutableIdentityChange, opening, activityEvent(patch))
	}

	reducer, refusal := reduceAll(t, richConfiguration(), opening,
		activityEvent(ActivityUpdate{ActivityID: "act-1", State: ActivityRunning, Progress: json.RawMessage(`{"done":1}`)}))
	require.Nil(t, refusal)

	activity, found := reducer.State().Activity("act-1")
	require.True(t, found)
	require.JSONEq(t, `{"done":1}`, string(activity.Progress))
}

// TestActivityReferencesMustExist pins that parentage and origin are resolved
// against entities the stream actually introduced.
func TestActivityReferencesMustExist(t *testing.T) {
	t.Parallel()

	accepted := Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
	}}

	requireReduceRefusal(t, richConfiguration(), ViolationUnknownEntity, openSnapshot(), accepted,
		activityEvent(ActivityUpdate{
			ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning,
			Cause: CauseSubmission, OriginTurnID: "turn-1", ParentID: "act-ghost",
		}))
}

// TestParentTerminalizesAfterEveryOwnedAction pins that an unresolved action an
// activity owns keeps that activity nonterminal.
func TestParentTerminalizesAfterEveryOwnedAction(t *testing.T) {
	t.Parallel()

	opening := Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundIdle, CycleID: "cyc-0"},
		Activities: []ActivityUpdate{{
			ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning,
			Cause: CauseSession, OriginTurnID: "turn-1",
		}},
		Actions: []ActionUpdate{{
			ActionID: "req-1", Kind: ActionElicitation, State: ActionPending,
			Owner: Owner{Type: OwnerActivity, ID: "act-1"}, BlocksForeground: stated(false),
		}},
	}}

	requireReduceRefusal(t, richConfiguration(), ViolationParentTerminalBeforeChild, opening,
		activityEvent(ActivityUpdate{ActivityID: "act-1", State: ActivityCompleted}))
}

// TestActionRules pins first-sight identity, ownership resolution, terminal
// immutability, and the immutable members a later patch may only restate.
func TestActionRules(t *testing.T) {
	t.Parallel()

	accepted := Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
	}}
	opening := []Event{openSnapshot(), accepted, RunningEvent("cyc-1", "turn-1")}
	pending := actionEvent(ActionUpdate{
		ActionID: "req-1", Kind: ActionPermission, State: ActionPending,
		Owner: Owner{Type: OwnerTurn, ID: "turn-1"}, RunID: "run-1", BlocksForeground: stated(false),
	})

	requireReduceRefusal(t, richConfiguration(), ViolationMalformedEnvelope,
		append(append([]Event{}, opening...), actionEvent(ActionUpdate{ActionID: "req-1", State: ActionPending}))...)

	requireReduceRefusal(t, richConfiguration(), ViolationUnknownEntity,
		append(append([]Event{}, opening...), actionEvent(ActionUpdate{
			ActionID: "req-1", Kind: ActionPermission, State: ActionPending,
			Owner: Owner{Type: OwnerActivity, ID: "act-ghost"}, BlocksForeground: stated(false),
		}))...)

	for _, patch := range []ActionUpdate{
		{ActionID: "req-1", State: ActionAccepted, Kind: ActionElicitation},
		{ActionID: "req-1", State: ActionAccepted, Owner: Owner{Type: OwnerTurn, ID: "turn-9"}},
		{ActionID: "req-1", State: ActionAccepted, RunID: "run-9"},
		{ActionID: "req-1", State: ActionAccepted, BlocksForeground: stated(true)},
	} {
		requireReduceRefusal(t, richConfiguration(), ViolationImmutableIdentityChange,
			append(append([]Event{}, opening...), pending, actionEvent(patch))...)
	}

	requireReduceRefusal(t, richConfiguration(), ViolationPostTerminalMutation,
		append(append([]Event{}, opening...), pending,
			actionEvent(ActionUpdate{ActionID: "req-1", State: ActionAccepted}),
			actionEvent(ActionUpdate{ActionID: "req-1", State: ActionDeclined}))...)
}

// TestTerminalActionOnFirstSightNeverBlocks pins that an action already resolved
// when a delta first sees it holds nothing: it is recorded, and the cycle it
// would otherwise have blocked owes no transition.
func TestTerminalActionOnFirstSightNeverBlocks(t *testing.T) {
	t.Parallel()

	accepted := Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
	}}
	resolved := actionEvent(ActionUpdate{
		ActionID: "req-1", Kind: ActionPermission, State: ActionCancelled,
		Owner: Owner{Type: OwnerTurn, ID: "turn-1"}, BlocksForeground: stated(true),
	})

	reducer, refusal := reduceAll(t, richConfiguration(),
		openSnapshot(), accepted, RunningEvent("cyc-1", "turn-1"), resolved,
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess))
	require.Nil(t, refusal)
	require.True(t, reducer.State().Vacant())
}

// TestQuiescenceInvalidationIsExplicit pins that a negative fact revokes the
// certified boundary exactly as acceptance does.
func TestQuiescenceInvalidationIsExplicit(t *testing.T) {
	t.Parallel()

	certified := Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundIdle, CycleID: "cyc-0"},
		Quiescence: QuiescenceFact{Quiescent: true, Source: ProofClassProcessContainment},
	}}

	reducer, refusal := reduceAll(t, richConfiguration(), certified,
		QuiescenceEvent(QuiescenceFact{}))
	require.Nil(t, refusal)

	state := reducer.State()
	require.False(t, state.Quiescence.Certified)
	require.Equal(t, uint64(2), state.Quiescence.InvalidatedAt)
}

// TestQuiescenceRefusesAnUnprovenClass pins that a fact naming a class the answer
// never advertised asserts something the connection did not claim.
func TestQuiescenceRefusesAnUnprovenClass(t *testing.T) {
	t.Parallel()

	degenerate := Negotiated{Versions: []int{Version}, ActivityKinds: []ActivityKind{}}

	requireReduceRefusal(t, degenerate, ViolationUnnegotiatedFact, openSnapshot(),
		QuiescenceEvent(QuiescenceFact{Quiescent: true, Source: ProofClassProcessContainment}))

	requireReduceRefusal(t, richConfiguration(), ViolationUnnegotiatedFact, openSnapshot(),
		QuiescenceEvent(QuiescenceFact{Quiescent: true, Source: ProofClassNativeSettledBarrier}))
}

// TestReducerLatchesOnTheFirstRefusal pins fail-closed on the consumer side: a
// stream that failed closed never reduces again.
func TestReducerLatchesOnTheFirstRefusal(t *testing.T) {
	t.Parallel()

	reducer, refusal := reduceAll(t, richConfiguration(), RunningEvent("cyc-1", "turn-1"))
	require.NotNil(t, refusal)
	require.Equal(t, ViolationDeltaBeforeSnapshot, refusal.Kind)
	require.Same(t, refusal, reducer.Failed())
	require.Equal(t, richConfiguration(), reducer.Negotiated())

	require.ErrorIs(t, reducer.Reduce(deliver(2, openSnapshot())), refusal)
	require.ErrorIs(t, reducer.ReduceSessionUpdate(json.RawMessage(enveloped(`{"type":"prompt_accepted","submissionId":"a","clientNonce":"b","turnId":"c"}`))), refusal)
}

// TestReducerRefusesAnIneligibleCarrier pins that the carrier rule binds the
// opening event and every later one alike.
func TestReducerRefusesAnIneligibleCarrier(t *testing.T) {
	t.Parallel()

	reducer := NewReducer(Options{Negotiated: richConfiguration()})
	opening := deliver(1, openSnapshot())
	opening.Carrier = CarrierIneligible

	err := reducer.Reduce(opening)
	require.ErrorAs(t, err, new(*ViolationError))
	require.Equal(t, ViolationIllegalCarrier, reducer.Failed().Kind)

	reducer = NewReducer(Options{Negotiated: richConfiguration()})
	require.NoError(t, reducer.Reduce(deliver(1, openSnapshot())))

	later := deliver(2, RunningEvent("cyc-1", "turn-1"))
	later.Carrier = CarrierIneligible

	require.Error(t, reducer.Reduce(later))
	require.Equal(t, ViolationIllegalCarrier, reducer.Failed().Kind)
}

// TestReducerRefusesADeltaFromAnUnseenStream pins that only an opening snapshot
// may arrive on a stream identity this reducer has not seen.
func TestReducerRefusesADeltaFromAnUnseenStream(t *testing.T) {
	t.Parallel()

	reducer := NewReducer(Options{Negotiated: richConfiguration()})
	require.NoError(t, reducer.Reduce(deliver(1, openSnapshot())))

	foreign := Delivery{StreamID: "other", Sequence: 2, Carrier: CarrierSessionInfo, Event: RunningEvent("c", "t")}
	require.Error(t, reducer.Reduce(foreign))
	require.Equal(t, ViolationStaleStream, reducer.Failed().Kind)
}

// TestReducerRefusesAnUnopenedStreamsSnapshot pins that a snapshot this reducer
// cannot apply leaves the stream unopened rather than half-open.
func TestReducerRefusesAnUnopenedStreamsSnapshot(t *testing.T) {
	t.Parallel()

	reducer := NewReducer(Options{Negotiated: richConfiguration()})
	require.Error(t, reducer.Reduce(deliver(1, Event{Type: EventSnapshot, Snapshot: &Snapshot{}})))
	require.Equal(t, ViolationMalformedEnvelope, reducer.Failed().Kind)
}

// TestWholesaleIdempotenceHasNoWindow pins that an exact retransmission is
// suppressed however far back its identity was reduced. A bounded window would
// turn the oldest identity in a long stream into a conflicting duplicate, which
// is the one thing wholesale idempotence promises never happens.
func TestWholesaleIdempotenceHasNoWindow(t *testing.T) {
	t.Parallel()

	reducer := NewReducer(Options{Negotiated: richConfiguration()})

	opening := deliver(1, openSnapshot())
	opening.Frame = map[string]any{"seq": 1}
	require.NoError(t, reducer.Reduce(opening))

	for sequence := uint64(2); sequence <= 4096; sequence++ {
		filler := deliver(sequence, Event{Type: EventStateUpdate, State: &StateTransition{
			State: ForegroundIdle, CycleID: "cyc-0", Cause: CauseSession,
		}})
		filler.Frame = map[string]any{"seq": sequence}
		require.NoError(t, reducer.Reduce(filler))
	}

	require.NoError(t, reducer.Reduce(opening))
	require.Equal(t, 1, reducer.State().SuppressedRetransmissions)
	require.Nil(t, reducer.Failed())
}

// TestStateLookupsMissEntitiesTheStreamNeverHeld pins the projection's own
// lookups.
func TestStateLookupsMissEntitiesTheStreamNeverHeld(t *testing.T) {
	t.Parallel()

	state := NewReducer(Options{Negotiated: richConfiguration()}).State()

	_, found := state.Activity("act-1")
	require.False(t, found)

	_, found = state.Action("req-1")
	require.False(t, found)

	_, found = state.Turn("turn-1")
	require.False(t, found)

	require.True(t, state.Vacant(), "a stream with nothing in it holds nothing live")
}

// TestVacancyIsNotForegroundState pins that vacancy is not foreground state: a
// live cycle, an unfinished activity, and an unanswered request each hold the
// session open on their own.
func TestVacancyIsNotForegroundState(t *testing.T) {
	t.Parallel()

	running, refusal := reduceAll(t, richConfiguration(), Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSubmission},
	}})
	require.Nil(t, refusal)
	require.False(t, running.State().Vacant())

	accepted := Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
	}}
	live := activityEvent(ActivityUpdate{
		ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning,
		Cause: CauseSubmission, OriginTurnID: "turn-1",
	})
	working, refusal := reduceAll(t, richConfiguration(), openSnapshot(), accepted,
		RunningEvent("cyc-1", "turn-1"), live,
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess))
	require.Nil(t, refusal)
	require.False(t, working.State().Vacant(), "an unfinished activity holds the session open")

	background := actionEvent(ActionUpdate{
		ActionID: "req-1", Kind: ActionPermission, State: ActionPending,
		Owner: Owner{Type: OwnerTurn, ID: "turn-1"}, BlocksForeground: stated(false),
	})
	held, refusal := reduceAll(t, richConfiguration(), openSnapshot(), accepted,
		RunningEvent("cyc-1", "turn-1"), background,
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess))
	require.Nil(t, refusal)
	require.False(t, held.State().Vacant(), "an unanswered request holds the session open")
}

// TestSnapshotRefusesAnUnresolvableSet pins the two ways a snapshot's own action
// set fails: an entry already terminal asserts a state that is over, and an owner
// the snapshot does not introduce could never be attributed.
func TestSnapshotRefusesAnUnresolvableSet(t *testing.T) {
	t.Parallel()

	requireReduceRefusal(t, richConfiguration(), ViolationMalformedEnvelope,
		Event{Type: EventSnapshot, Snapshot: &Snapshot{
			Foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSubmission},
			Actions: []ActionUpdate{{
				ActionID: "req-1", Kind: ActionPermission, State: ActionAccepted,
				Owner: Owner{Type: OwnerTurn, ID: "turn-1"}, BlocksForeground: stated(false),
			}},
		}})

	requireReduceRefusal(t, richConfiguration(), ViolationUnknownEntity,
		Event{Type: EventSnapshot, Snapshot: &Snapshot{
			Foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSubmission},
			Actions: []ActionUpdate{{
				ActionID: "req-1", Kind: ActionPermission, State: ActionPending,
				Owner: Owner{Type: OwnerActivity, ID: "act-ghost"}, BlocksForeground: stated(false),
			}},
		}})
}

// TestViolationErrorNamesTheFrameItRefused pins the refusal's own reporting and
// its match-by-kind behaviour.
func TestViolationErrorNamesTheFrameItRefused(t *testing.T) {
	t.Parallel()

	refusal := violation(ViolationSequenceGap, "strm", 7, "expected 6")
	require.Equal(t, "lifecycle violation sequence_gap at strm#7: expected 6", refusal.Error())
	require.Equal(t, "lifecycle violation sequence_gap at strm#7",
		violation(ViolationSequenceGap, "strm", 7, "").Error())

	require.ErrorIs(t, refusal, &ViolationError{Kind: ViolationSequenceGap})
	require.NotErrorIs(t, refusal, &ViolationError{Kind: ViolationStaleStream})
	require.NotErrorIs(t, refusal, errors.New("sequence_gap"))
}

// TestNegotiatedVersionIsTheHighestCommonMember pins the single integer every
// envelope on a connection carries.
func TestNegotiatedVersionIsTheHighestCommonMember(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0, Negotiated{}.NegotiatedVersion())
	require.Equal(t, 3, Negotiated{Versions: []int{1, 3}}.NegotiatedVersion())
}

// TestAdvertisementRendersTheAnswer pins the wire shape of the answer, including
// the presence rule that binds quiescenceSource to its claim.
func TestAdvertisementRendersTheAnswer(t *testing.T) {
	t.Parallel()

	degenerate := Negotiated{Versions: []int{Version}, ActivityKinds: []ActivityKind{}}.Advertisement()
	require.Equal(t, []string{}, degenerate["activityKinds"])
	require.NotContains(t, degenerate, "quiescenceSource")

	full := richConfiguration().Advertisement()
	require.Equal(t, "process-containment", full["quiescenceSource"])
	require.Equal(t, []string{"task", "monitor", "subagent", "goal", "other"}, full["activityKinds"])
	require.Equal(t, true, full["updatesOutsidePrompt"])
}

// TestProofClassIsClosedAtTwo pins that nothing outside the two proof classes is
// a class at all.
func TestProofClassIsClosedAtTwo(t *testing.T) {
	t.Parallel()

	require.True(t, ProofClassProcessContainment.Valid())
	require.True(t, ProofClassNativeSettledBarrier.Valid())
	require.False(t, ProofClass("quiet-for-a-while").Valid())
}

// TestClosedVocabulariesRefuseEverythingElse pins each closed set's rejection of
// a value outside it.
func TestClosedVocabulariesRefuseEverythingElse(t *testing.T) {
	t.Parallel()

	require.False(t, ForegroundState("waiting").Valid())
	require.False(t, Cause("whim").Valid())
	require.False(t, Outcome("partial").Valid())
	require.False(t, ActivityKind("chore").Valid())
	require.False(t, ActivityState("stalled").Valid())
	require.False(t, ActionKind("approval").Valid())
	require.False(t, ActionState("held").Valid())
	require.False(t, OwnerType("session").Valid())
	require.False(t, ValidStopReason("stop"))
}

// TestStreamIDNamesTheIncarnation pins the emitter's own identity accessor.
func TestStreamIDNamesTheIncarnation(t *testing.T) {
	t.Parallel()

	require.Equal(t, "strm-1", NewStream("strm-1", containedConfiguration()).ID())
}

// TestFirstSightViaActivityUpdateStatesItsWholeIdentity pins that an activity's
// first sight carries every immutable field wherever it arrives, and that its
// origin turn must be one the stream opened.
func TestFirstSightViaActivityUpdateStatesItsWholeIdentity(t *testing.T) {
	t.Parallel()

	accepted := Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
	}}

	requireReduceRefusal(t, richConfiguration(), ViolationImmutableIdentityChange,
		openSnapshot(), accepted,
		activityEvent(ActivityUpdate{ActivityID: "act-1", State: ActivityRunning, OriginTurnID: "turn-1", Cause: CauseSubmission}))

	requireReduceRefusal(t, richConfiguration(), ViolationUnknownEntity,
		openSnapshot(), accepted,
		activityEvent(ActivityUpdate{
			ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning,
			Cause: CauseSubmission, OriginTurnID: "turn-ghost",
		}))
}

// fencedStream reduces a settled turn whose activity finished before a certified
// boundary, then opens an agent-origin turn after it. Everything the boundary
// fenced is first seen at or before watermark 6; turn-2 is not.
func fencedStream(t *testing.T) *Reducer {
	t.Helper()

	reducer, refusal := reduceAll(t, richConfiguration(),
		openSnapshot(),
		Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
			SubmissionID: "sub-1", ClientNonce: "non-1", TurnID: "turn-1",
		}},
		RunningEvent("cyc-1", "turn-1"),
		activityEvent(ActivityUpdate{
			ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning,
			Cause: CauseSubmission, OriginTurnID: "turn-1",
		}),
		activityEvent(ActivityUpdate{ActivityID: "act-1", State: ActivityCompleted}),
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
		QuiescenceEvent(QuiescenceFact{Quiescent: true, Source: ProofClassProcessContainment, Watermark: 6}),
		Event{Type: EventStateUpdate, State: &StateTransition{
			State: ForegroundRunning, CycleID: "cyc-2", TurnID: "turn-2", Cause: CauseActivity,
		}},
	)
	require.Nil(t, refusal)

	return reducer
}

// TestCausalFenceRefusesAFencedParent pins the parent half of the predicate: an
// activity rooted in a parent first seen at or before the watermark was fenced by
// that proof, even when its origin turn came later.
func TestCausalFenceRefusesAFencedParent(t *testing.T) {
	t.Parallel()

	err := fencedStream(t).Reduce(deliver(9, activityEvent(ActivityUpdate{
		ActivityID: "act-2", Kind: ActivityTask, State: ActivityRunning,
		Cause: CauseActivity, OriginTurnID: "turn-2", ParentID: "act-1",
	})))

	var refusal *ViolationError

	require.ErrorAs(t, err, &refusal)
	require.Equal(t, ViolationLateCausalWork, refusal.Kind)
}

// TestUnrelatedWorkMayBeginAfterASettledBoundary pins the other half: a
// certified boundary fences causal work rooted at or before its watermark and
// nothing else.
func TestUnrelatedWorkMayBeginAfterASettledBoundary(t *testing.T) {
	t.Parallel()

	reducer := fencedStream(t)
	require.NoError(t, reducer.Reduce(deliver(9, activityEvent(ActivityUpdate{
		ActivityID: "act-3", Kind: ActivityTask, State: ActivityRunning,
		Cause: CauseActivity, OriginTurnID: "turn-2",
	}))))

	state := reducer.State()
	require.False(t, state.Quiescence.Certified, "new work invalidates the prior boundary")
	require.False(t, state.Vacant())
}

// TestQuiescenceIsDisprovedByLiveWork pins that a positive fact the stream
// contradicts fails closed rather than quietly certifying nothing: a swallowed
// lie is indistinguishable from a loss.
func TestQuiescenceIsDisprovedByLiveWork(t *testing.T) {
	t.Parallel()

	opening := Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSubmission},
		Activities: []ActivityUpdate{{
			ActivityID: "act-1", Kind: ActivityTask, State: ActivityRunning,
			Cause: CauseSubmission, OriginTurnID: "turn-1",
		}},
	}}

	requireReduceRefusal(t, richConfiguration(), ViolationFalseQuiescence, opening,
		QuiescenceEvent(QuiescenceFact{Quiescent: true, Source: ProofClassProcessContainment, Watermark: 1}))
}

// TestActionRestatementKeepsItPending pins that a patch restating a pending
// action leaves it holding its owner.
func TestActionRestatementKeepsItPending(t *testing.T) {
	t.Parallel()

	pending := actionEvent(ActionUpdate{
		ActionID: "req-1", Kind: ActionElicitation, State: ActionPending,
		Owner: Owner{Type: OwnerTurn, ID: "turn-1"}, BlocksForeground: stated(false),
	})

	reducer, refusal := reduceAll(t, richConfiguration(),
		Event{Type: EventSnapshot, Snapshot: &Snapshot{
			Foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSubmission},
			Quiescence: QuiescenceFact{},
		}},
		pending,
		actionEvent(ActionUpdate{ActionID: "req-1", State: ActionPending}),
	)
	require.Nil(t, refusal)

	action, found := reducer.State().Action("req-1")
	require.True(t, found)
	require.Equal(t, ActionPending, action.State)
	require.False(t, reducer.State().Quiescence.Certified)
}
