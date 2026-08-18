package lifecycle

// applySnapshot opens the stream from a whole-state assertion. A snapshot is
// taken whole or not at all: the entire assertion is judged before any of it is
// projected, so a refused snapshot opens nothing and leaves no half-built
// projection behind.
func (r *Reducer) applySnapshot(delivery Delivery) error {
	snapshot := delivery.Event.Snapshot
	if snapshot == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the snapshot payload is missing")
	}

	if err := r.checkSnapshot(delivery, *snapshot); err != nil {
		return err
	}

	r.projectSnapshot(delivery, *snapshot)

	return nil
}

// checkSnapshot judges the assertion. A snapshot introduces exactly what it
// names — the turn its foreground reports, the turns its activities name as
// origin, and its own activities — and every reference it makes resolves inside
// that set: an action owner is a reference rather than an introduction.
func (r *Reducer) checkSnapshot(delivery Delivery, snapshot Snapshot) error {
	if !snapshot.Foreground.State.Valid() || snapshot.Foreground.CycleID == "" {
		return r.fail(delivery, ViolationMalformedEnvelope, "the snapshot's foreground is incomplete")
	}

	introduced := snapshot.introduces()

	if err := r.checkSnapshotActivities(delivery, snapshot, introduced); err != nil {
		return err
	}

	if err := r.checkSnapshotActions(delivery, snapshot, introduced); err != nil {
		return err
	}

	if snapshot.Foreground.State == ForegroundRequiresAction && !snapshot.carriesBlocker() {
		return r.fail(delivery, ViolationInconsistentForeground,
			"cycle "+snapshot.Foreground.CycleID+" lists no blocking action")
	}

	return r.checkQuiescence(delivery, snapshot.Quiescence, snapshot.vacant())
}

// checkSnapshotActivities validates the asserted activity set. The set is the
// complete nonterminal one, so an entry that is already terminal asserts as
// current a state that is over.
func (r *Reducer) checkSnapshotActivities(delivery Delivery, snapshot Snapshot, introduced introductions) error {
	for index := range snapshot.Activities {
		activity := &snapshot.Activities[index]

		if err := r.checkActivityIdentity(delivery, *activity); err != nil {
			return err
		}

		if activity.State.Terminal() {
			return r.fail(delivery, ViolationMalformedEnvelope, "activity "+activity.ActivityID+" is terminal")
		}

		if activity.ParentID != "" && !introduced.activities[activity.ParentID] {
			return r.fail(delivery, ViolationUnknownEntity, "parent activity "+activity.ParentID+" is not introduced")
		}
	}

	return nil
}

func (r *Reducer) checkSnapshotActions(delivery Delivery, snapshot Snapshot, introduced introductions) error {
	for _, action := range snapshot.Actions {
		if err := r.checkActionIdentity(delivery, action); err != nil {
			return err
		}

		if action.State.Terminal() {
			return r.fail(delivery, ViolationMalformedEnvelope, "action "+action.ActionID+" is terminal")
		}

		if !introduced.holds(action.Owner) {
			return r.fail(delivery, ViolationUnknownEntity,
				"owner "+string(action.Owner.Type)+" "+action.Owner.ID+" is not introduced")
		}
	}

	return nil
}

// projectSnapshot installs the validated assertion. A snapshot resuming mid-turn
// projects that turn as open with the origin it reported, because a resumed
// stream whose foreground names a turn nothing later opened would leave every
// reference to it unresolvable.
func (r *Reducer) projectSnapshot(delivery Delivery, snapshot Snapshot) {
	foreground := snapshot.Foreground
	r.state.Foreground = &foreground

	if foreground.TurnID != "" {
		r.state.Turns = append(r.state.Turns, TurnRecord{
			TurnID:  foreground.TurnID,
			Origin:  foreground.Origin,
			CycleID: foreground.CycleID,
		})
		r.seeTurn(foreground.TurnID, delivery.Sequence)
	}

	for index := range snapshot.Activities {
		r.seeTurn(snapshot.Activities[index].OriginTurnID, delivery.Sequence)
		r.recordActivity(delivery, snapshot.Activities[index])
	}

	for _, action := range snapshot.Actions {
		r.recordAction(delivery, action)
	}

	r.recordQuiescence(delivery, snapshot.Quiescence)
}

// introductions is the identity set a snapshot brings into existence.
type introductions struct {
	turns      map[string]bool
	activities map[string]bool
}

func (s Snapshot) introduces() introductions {
	introduced := introductions{turns: map[string]bool{}, activities: map[string]bool{}}
	if s.Foreground.TurnID != "" {
		introduced.turns[s.Foreground.TurnID] = true
	}

	for index := range s.Activities {
		introduced.turns[s.Activities[index].OriginTurnID] = true
		introduced.activities[s.Activities[index].ActivityID] = true
	}

	return introduced
}

func (i introductions) holds(owner Owner) bool {
	if owner.Type == OwnerActivity {
		return i.activities[owner.ID]
	}

	return i.turns[owner.ID]
}

// carriesBlocker reports whether the snapshot's own action set explains a
// requires_action foreground. The set is complete, so a blocker it does not list
// does not exist.
func (s Snapshot) carriesBlocker() bool {
	for _, action := range s.Actions {
		if blocksForeground(action) {
			return true
		}
	}

	return false
}

// vacant reports whether the asserted state holds nothing live. The sets are the
// complete nonterminal ones, so a non-empty set is live work by construction.
func (s Snapshot) vacant() bool {
	return s.Foreground.State == ForegroundIdle && len(s.Activities) == 0 && len(s.Actions) == 0
}

// applyPromptAccepted opens a prompt-origin turn. Acceptance introduces its turn,
// so it happens once per turn: a second acceptance of a turn the stream already
// introduced changes that turn's identity rather than reporting a new one.
// Acceptance is also the dispatch linearization point, so it invalidates whatever
// boundary was certified before the frame the native dispatcher just took
// ownership of.
func (r *Reducer) applyPromptAccepted(delivery Delivery) error {
	accepted := delivery.Event.PromptAccepted
	if accepted == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the acceptance payload is missing")
	}

	index := r.turnIndex(accepted.TurnID)

	switch {
	case index >= 0 && r.state.Turns[index].Terminal:
		return r.fail(delivery, ViolationPostTerminalMutation, "turn "+accepted.TurnID+" is terminal")
	case r.turnKnown(accepted.TurnID):
		return r.fail(delivery, ViolationImmutableIdentityChange,
			"turn "+accepted.TurnID+" was already introduced")
	}

	r.state.Turns = append(r.state.Turns, TurnRecord{
		TurnID:       accepted.TurnID,
		Origin:       CauseSubmission,
		SubmissionID: accepted.SubmissionID,
		ClientNonce:  accepted.ClientNonce,
		RunID:        accepted.RunID,
	})
	r.seeTurn(accepted.TurnID, delivery.Sequence)
	r.invalidateQuiescence(delivery.Sequence)
	r.lastTransition = delivery.Sequence

	return nil
}

func (r *Reducer) applyStateUpdate(delivery Delivery) error {
	transition := delivery.Event.State
	if transition == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the transition payload is missing")
	}

	if detail := endingIdleDefect(*transition); detail != "" {
		return r.fail(delivery, ViolationMalformedEnvelope, detail)
	}

	if err := r.checkBlockedCycle(delivery, *transition); err != nil {
		return err
	}

	apply := r.applyLive
	if transition.State == ForegroundIdle {
		apply = r.applyIdle
	}

	if err := apply(delivery, *transition); err != nil {
		return err
	}

	r.lastTransition = delivery.Sequence

	return nil
}

// checkBlockedCycle enforces both halves of the blocking-action rule: a
// requires-action transition names a cycle something is actually blocking, and a
// cycle a blocking action stopped neither leaves that state nor ends while any
// action blocking it is still nonterminal. The resolution is the reason the
// foreground may move, so it is always ordered first, and a cycle returns to
// running when the last blocker resolves rather than the first.
func (r *Reducer) checkBlockedCycle(delivery Delivery, transition StateTransition) error {
	if transition.State == ForegroundRequiresAction {
		if !r.blocked(transition.CycleID) {
			return r.fail(delivery, ViolationInconsistentForeground,
				"cycle "+transition.CycleID+" has no outstanding blocking action")
		}

		if r.blockedCycle == transition.CycleID {
			r.blockedCycle = ""
		}

		return nil
	}

	if r.blocked(transition.CycleID) || r.blockedCycle == transition.CycleID {
		return r.fail(delivery, ViolationInconsistentForeground,
			"cycle "+transition.CycleID+" is blocked and reported "+string(transition.State))
	}

	return nil
}

// blocked reports whether an action that stopped this cycle is still nonterminal.
func (r *Reducer) blocked(cycleID string) bool {
	for _, action := range r.state.Actions {
		if action.BlocksForeground && !action.State.Terminal() && r.actionCycle[action.ActionID] == cycleID {
			return true
		}
	}

	return false
}

// applyLive reduces a running or requires-action transition. Exactly two events
// open a turn, and this is the second: an activity-caused running transition
// bearing a turn the stream has not introduced opens an agent-origin turn. Every
// other transition names a turn the stream already opened — a session-caused one
// reports the foreground moving for a reason the host did not cause, which is
// never a reason to invent an owner for it.
func (r *Reducer) applyLive(delivery Delivery, transition StateTransition) error {
	index := r.turnIndex(transition.TurnID)

	switch {
	case index >= 0 && r.state.Turns[index].Terminal:
		return r.fail(delivery, ViolationPostTerminalMutation, "turn "+transition.TurnID+" is terminal")
	case index < 0:
		if transition.Cause != CauseActivity || transition.State != ForegroundRunning {
			return r.fail(delivery, ViolationUnknownEntity, "turn "+transition.TurnID+" was never opened")
		}

		r.state.Turns = append(r.state.Turns, TurnRecord{TurnID: transition.TurnID, Origin: CauseActivity})
		r.seeTurn(transition.TurnID, delivery.Sequence)

		index = len(r.state.Turns) - 1
	}

	r.state.Turns[index].CycleID = transition.CycleID
	r.state.Foreground = &Foreground{
		State:   transition.State,
		CycleID: transition.CycleID,
		TurnID:  transition.TurnID,
	}
	r.invalidateQuiescence(delivery.Sequence)

	return nil
}

// applyIdle ends a foreground cycle. An ending transition never opens a turn, so
// it must name one the stream already introduced.
func (r *Reducer) applyIdle(delivery Delivery, transition StateTransition) error {
	index := r.turnIndex(transition.TurnID)

	switch {
	case transition.TurnID == "":
		r.state.Foreground = &Foreground{State: ForegroundIdle, CycleID: transition.CycleID}

		return nil
	case index >= 0 && r.state.Turns[index].Terminal:
		return r.fail(delivery, ViolationPostTerminalMutation, "turn "+transition.TurnID+" is terminal")
	case index < 0:
		return r.fail(delivery, ViolationUnknownEntity, "turn "+transition.TurnID+" was never opened")
	}

	turn := &r.state.Turns[index]
	turn.Terminal = true
	turn.StopReason = transition.StopReason
	turn.Outcome = transition.Outcome
	turn.CycleID = transition.CycleID
	r.state.Foreground = &Foreground{State: ForegroundIdle, CycleID: transition.CycleID}

	return nil
}

func (r *Reducer) applyActivityUpdate(delivery Delivery) error {
	update := delivery.Event.Activity
	if update == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the activity payload is missing")
	}

	r.lastTransition = delivery.Sequence

	if r.activityIndex(update.ActivityID) >= 0 {
		return r.patchActivity(delivery, *update)
	}

	if err := r.checkActivityIdentity(delivery, *update); err != nil {
		return err
	}

	if err := r.checkActivityReferences(delivery, *update); err != nil {
		return err
	}

	if err := r.checkCausalFence(delivery, *update); err != nil {
		return err
	}

	if err := r.checkActivityParent(delivery, update.ActivityID, update.ParentID); err != nil {
		return err
	}

	r.recordActivity(delivery, *update)

	return nil
}

// checkActivityReferences resolves a first sight's references. A parent with no
// prior first sight or an origin turn the stream never opened would leave
// parentage, ownership, and terminal ordering unenforceable.
func (r *Reducer) checkActivityReferences(delivery Delivery, update ActivityUpdate) error {
	if !r.turnKnown(update.OriginTurnID) {
		return r.fail(delivery, ViolationUnknownEntity, "origin turn "+update.OriginTurnID+" was never opened")
	}

	if update.ParentID != "" && !r.activityKnown(update.ParentID) {
		return r.fail(delivery, ViolationUnknownEntity, "parent activity "+update.ParentID+" was never seen")
	}

	return nil
}

// checkCausalFence refuses old causal work discovered after the boundary that
// settled it. The predicate is mechanical: an activity rooted in a turn or a
// parent first seen at or before a certified watermark was fenced by that proof,
// and a settled boundary never reopens.
func (r *Reducer) checkCausalFence(delivery Delivery, update ActivityUpdate) error {
	if r.fence == 0 {
		return nil
	}

	if seen, known := r.turnSeen[update.OriginTurnID]; known && seen <= r.fence {
		return r.fail(delivery, ViolationLateCausalWork, "origin turn "+update.OriginTurnID+" is fenced")
	}

	if seen, known := r.activitySeen[update.ParentID]; known && seen <= r.fence {
		return r.fail(delivery, ViolationLateCausalWork, "parent activity "+update.ParentID+" is fenced")
	}

	return nil
}

// checkActivityIdentity validates an activity's first sight, when every immutable
// identity field must be present and the kind must be one the answer proved.
func (r *Reducer) checkActivityIdentity(delivery Delivery, update ActivityUpdate) error {
	switch {
	case update.Kind == "" || update.Cause == "" || update.OriginTurnID == "":
		return r.fail(delivery, ViolationImmutableIdentityChange,
			"activity "+update.ActivityID+" states an incomplete identity")
	case !r.negotiated.DeclaresActivityKind(update.Kind):
		return r.fail(delivery, ViolationUnnegotiatedFact, "activity kind "+string(update.Kind))
	}

	return nil
}

func (r *Reducer) recordActivity(delivery Delivery, update ActivityUpdate) {
	r.state.Activities = append(r.state.Activities, ActivityRecord(update))
	r.seeActivity(update.ActivityID, delivery.Sequence)

	if !update.State.Terminal() {
		r.invalidateQuiescence(delivery.Sequence)
	}
}

// checkActivityParent validates parentage: a parent this stream introduced must
// still be nonterminal when it gains a child.
func (r *Reducer) checkActivityParent(delivery Delivery, activityID, parentID string) error {
	index := r.activityIndex(parentID)
	if parentID == "" || index < 0 || activityID == parentID {
		return nil
	}

	if r.state.Activities[index].State.Terminal() {
		return r.fail(delivery, ViolationChildAfterParentTerminal, "parent "+parentID+" is terminal")
	}

	return nil
}

// patchActivity applies a later update, which may change only state and progress.
// A restated immutable field is permitted only with its first-sight value, and
// changes nothing.
func (r *Reducer) patchActivity(delivery Delivery, update ActivityUpdate) error {
	index := r.activityIndex(update.ActivityID)

	existing := r.state.Activities[index]
	if detail := immutableActivityConflict(existing, update); detail != "" {
		return r.fail(delivery, ViolationImmutableIdentityChange, detail)
	}

	if existing.State.Terminal() && update.State != existing.State {
		return r.fail(delivery, ViolationPostTerminalMutation, "activity "+existing.ActivityID+" is terminal")
	}

	if update.State.Terminal() {
		if err := r.checkDescendantsTerminal(delivery, existing.ActivityID); err != nil {
			return err
		}
	}

	r.state.Activities[index].State = update.State

	if update.Progress != nil {
		r.state.Activities[index].Progress = update.Progress
	}

	if !update.State.Terminal() {
		r.invalidateQuiescence(delivery.Sequence)
	}

	return nil
}

// checkDescendantsTerminal refuses a parent that would terminalize while part of
// the subtree it claims to have finished is still live.
func (r *Reducer) checkDescendantsTerminal(delivery Delivery, activityID string) error {
	for index := range r.state.Activities {
		if child := &r.state.Activities[index]; child.ParentID == activityID && !child.State.Terminal() {
			return r.fail(delivery, ViolationParentTerminalBeforeChild, "child "+child.ActivityID+" is live")
		}
	}

	for _, action := range r.state.Actions {
		if action.Owner.Type == OwnerActivity && action.Owner.ID == activityID && !action.State.Terminal() {
			return r.fail(delivery, ViolationParentTerminalBeforeChild, "action "+action.ActionID+" is unresolved")
		}
	}

	return nil
}

func immutableActivityConflict(existing ActivityRecord, update ActivityUpdate) string {
	switch {
	case update.Kind != "" && update.Kind != existing.Kind:
		return "activity " + update.ActivityID + " changed kind"
	case update.ParentID != "" && update.ParentID != existing.ParentID:
		return "activity " + update.ActivityID + " changed parent"
	case update.ToolCallID != "" && update.ToolCallID != existing.ToolCallID:
		return "activity " + update.ActivityID + " changed tool link"
	case update.Cause != "" && update.Cause != existing.Cause:
		return "activity " + update.ActivityID + " changed cause"
	case update.OriginTurnID != "" && update.OriginTurnID != existing.OriginTurnID:
		return "activity " + update.ActivityID + " changed origin turn"
	case update.RunID != "" && update.RunID != existing.RunID:
		return "activity " + update.ActivityID + " changed ownership root"
	default:
		return ""
	}
}

func (r *Reducer) applyActionUpdate(delivery Delivery) error {
	update := delivery.Event.Action
	if update == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the action payload is missing")
	}

	r.lastTransition = delivery.Sequence

	if r.actionIndex(update.ActionID) >= 0 {
		return r.patchAction(delivery, *update)
	}

	if err := r.checkActionIdentity(delivery, *update); err != nil {
		return err
	}

	if err := r.checkActionOwner(delivery, *update); err != nil {
		return err
	}

	r.recordAction(delivery, *update)

	return nil
}

// checkActionIdentity validates an action's first sight, when every member that
// fixes what the action is must be present. blocksForeground read as false by
// default would silently demote a blocking request to a background one, which is
// the difference between a foreground a host renders as waiting and one it
// renders as working.
func (r *Reducer) checkActionIdentity(delivery Delivery, update ActionUpdate) error {
	if update.Kind == "" || update.Owner.ID == "" || update.BlocksForeground == nil {
		return r.fail(delivery, ViolationMalformedEnvelope,
			"action "+update.ActionID+" states an incomplete first sight")
	}

	return nil
}

// blocksForeground reports a stated blocking claim. An omitted member states
// nothing, which is why only a first sight is required to carry one.
func blocksForeground(update ActionUpdate) bool {
	return update.BlocksForeground != nil && *update.BlocksForeground
}

// checkActionOwner resolves the entity an action is owned by. An action hung off
// a turn or activity the stream never introduced could never be attributed.
func (r *Reducer) checkActionOwner(delivery Delivery, update ActionUpdate) error {
	known := r.turnKnown(update.Owner.ID)
	if update.Owner.Type == OwnerActivity {
		known = r.activityKnown(update.Owner.ID)
	}

	if !known {
		return r.fail(delivery, ViolationUnknownEntity,
			"owner "+string(update.Owner.Type)+" "+update.Owner.ID+" was never opened")
	}

	return nil
}

func (r *Reducer) recordAction(delivery Delivery, update ActionUpdate) {
	r.state.Actions = append(r.state.Actions, ActionRecord{
		ActionID:         update.ActionID,
		Kind:             update.Kind,
		State:            update.State,
		Owner:            update.Owner,
		RunID:            update.RunID,
		BlocksForeground: *update.BlocksForeground,
	})

	if update.State.Terminal() {
		return
	}

	r.blockForeground(update)
	r.invalidateQuiescence(delivery.Sequence)
}

// blockForeground records the cycle a blocking action stopped and the transition
// that cycle now owes. A blocker blocks the cycle current at its first sight, and
// it never moves the foreground by itself.
func (r *Reducer) blockForeground(update ActionUpdate) {
	if !blocksForeground(update) || r.state.Foreground == nil {
		return
	}

	r.actionCycle[update.ActionID] = r.state.Foreground.CycleID

	if r.state.Foreground.State != ForegroundRequiresAction {
		r.blockedCycle = r.state.Foreground.CycleID
	}
}

func (r *Reducer) patchAction(delivery Delivery, update ActionUpdate) error {
	index := r.actionIndex(update.ActionID)

	existing := r.state.Actions[index]

	switch {
	case update.Kind != "" && update.Kind != existing.Kind:
		return r.fail(delivery, ViolationImmutableIdentityChange, "action "+update.ActionID+" changed kind")
	case update.Owner.ID != "" && update.Owner != existing.Owner:
		return r.fail(delivery, ViolationImmutableIdentityChange, "action "+update.ActionID+" changed owner")
	case update.RunID != "" && update.RunID != existing.RunID:
		return r.fail(delivery, ViolationImmutableIdentityChange, "action "+update.ActionID+" changed ownership root")
	case update.BlocksForeground != nil && *update.BlocksForeground != existing.BlocksForeground:
		return r.fail(delivery, ViolationImmutableIdentityChange, "action "+update.ActionID+" changed what it blocks")
	case existing.State.Terminal() && update.State != existing.State:
		return r.fail(delivery, ViolationPostTerminalMutation, "action "+update.ActionID+" is terminal")
	}

	r.state.Actions[index].State = update.State

	if !update.State.Terminal() {
		r.invalidateQuiescence(delivery.Sequence)
	}

	return nil
}

// applyQuiescence reduces a standalone quiescence fact. The event asserts the
// proof class whatever its polarity, so a configuration that proved no class
// emits none at all: admitting even a negative one would let a host read
// authoritative absence of background work from a source that cannot observe it.
func (r *Reducer) applyQuiescence(delivery Delivery) error {
	fact := delivery.Event.Quiescence
	if fact == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the quiescence payload is missing")
	}

	if !r.negotiated.AuthoritativeQuiescence {
		return r.fail(delivery, ViolationUnnegotiatedFact, "the answer proved no quiescence class")
	}

	if err := r.checkQuiescence(delivery, *fact, r.state.Vacant()); err != nil {
		return err
	}

	r.recordQuiescence(delivery, *fact)

	return nil
}

// checkQuiescence judges a positive fact against the stream carrying it. A fact
// naming a class the answer never claimed is refused before its content is
// judged. One the stream disproves — work is still live, or the watermark stops
// short of the last event that recorded any — is a lie about the boundary rather
// than a weaker claim, so it fails closed instead of certifying nothing: a
// swallowed lie is indistinguishable from a loss.
func (r *Reducer) checkQuiescence(delivery Delivery, fact QuiescenceFact, vacant bool) error {
	switch {
	case !fact.Quiescent:
		return nil
	case fact.Source != r.negotiated.QuiescenceSource:
		return r.fail(delivery, ViolationUnnegotiatedFact, "quiescence proof "+string(fact.Source))
	case !vacant:
		return r.fail(delivery, ViolationFalseQuiescence, "the stream still holds live work")
	case fact.Watermark < r.lastTransition:
		return r.fail(delivery, ViolationFalseQuiescence, "the watermark stops short of the last recorded work")
	default:
		return nil
	}
}

// recordQuiescence installs a validated fact. A negative one revokes whatever
// boundary stood; only a later positive fact certifies again.
func (r *Reducer) recordQuiescence(delivery Delivery, fact QuiescenceFact) {
	if !fact.Quiescent {
		r.invalidateQuiescence(delivery.Sequence)

		return
	}

	r.state.Quiescence = QuiescenceState{
		Certified: true,
		Source:    fact.Source,
		Watermark: fact.Watermark,
		Barrier:   fact.Barrier,
	}
	r.fence = max(r.fence, fact.Watermark)
}

// seeTurn and seeActivity retain the sequence an identity was first seen at. Only
// the first sight counts: it is what a later watermark fences.
func (r *Reducer) seeTurn(turnID string, sequence uint64) {
	if _, known := r.turnSeen[turnID]; !known {
		r.turnSeen[turnID] = sequence
	}
}

func (r *Reducer) seeActivity(activityID string, sequence uint64) {
	if _, known := r.activitySeen[activityID]; !known {
		r.activitySeen[activityID] = sequence
	}
}

func (r *Reducer) turnKnown(turnID string) bool {
	_, known := r.turnSeen[turnID]

	return known
}

func (r *Reducer) activityKnown(activityID string) bool {
	_, known := r.activitySeen[activityID]

	return known
}

func (r *Reducer) turnIndex(turnID string) int {
	return indexOf(r.state.Turns, turnID, TurnRecord.identity)
}

func (r *Reducer) activityIndex(activityID string) int {
	return indexOf(r.state.Activities, activityID, ActivityRecord.identity)
}

func (r *Reducer) actionIndex(actionID string) int {
	return indexOf(r.state.Actions, actionID, ActionRecord.identity)
}
