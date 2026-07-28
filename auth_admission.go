package hermesacp

import (
	"context"
	"sync"

	"github.com/coder/acp-go-sdk"
)

// authGate is a mutex a leg waits on with its own context, plus the number of
// legs holding or waiting for it. The count is what lets the broker drop the
// gate when the last one leaves: the keys are native home paths and
// (session, provider) pairs, both unbounded over the life of an agent that
// outlives every session it serves, and a gate map that only ever grows is a
// leak. Dropping the gate on the way out is also why nothing else may delete
// one — a gate removed while a leg still holds it is replaced by a fresh gate
// the next leg passes straight through, which is the serialization silently
// not happening.
type authGate struct {
	ch      chan struct{}
	waiters int
}

// authAcquireGate serializes every leg that names the same key, returning the
// release the holder defers. It reports false when the caller's context ended
// first, which is the only way a leg leaves the queue without having held the
// gate.
func authAcquireGate[K comparable](ctx context.Context, mu *sync.Mutex, gates map[K]*authGate, key K) (func(), bool) {
	mu.Lock()

	gate, ok := gates[key]
	if !ok {
		gate = &authGate{ch: make(chan struct{}, 1)}
		gates[key] = gate
	}

	gate.waiters++

	mu.Unlock()

	leave := func() {
		mu.Lock()
		defer mu.Unlock()

		gate.waiters--
		if gate.waiters == 0 {
			delete(gates, key)
		}
	}

	held := func() {
		<-gate.ch

		leave()
	}

	// An uncontended gate is taken even by a request that is already ending, so
	// a leg only ever fails on a queue it actually joined. Selecting straight
	// away would make that a coin toss between the two ready cases, and a leg
	// refused by a gate nobody held would report a wait it never made.
	select {
	case gate.ch <- struct{}{}:
		return held, true
	default:
	}

	select {
	case gate.ch <- struct{}{}:
		return held, true
	case <-ctx.Done():
		leave()

		return nil, false
	}
}

// admit holds one authorize per (session, provider) across its whole admission:
// the replay check, the retired check, the supersede, the ledger intent, the
// publication, and the mint that settles it. Without it two identical requests
// both fail to find each other — neither has published a flow yet when the
// other looks — and both mint a login at the provider, after which the second
// publication supersedes and cancels the flow the first caller was handed. The
// mint is what a waiting request waits for, and every mint here is bounded by
// authNativeCallTimeout.
func (p *providerAuth) admit(ctx context.Context, key authFlowKey) (func(), bool) {
	return authAcquireGate(ctx, &p.mu, p.admissions, key)
}

// lockSlot serializes every mutation of one session's native credential store.
// The gate is keyed on the home rather than on the provider because hermes
// rewrites auth.json whole: two providers' legs that read the same document and
// rename their own complete replacement leave only the last one's slot, so
// per-provider keying would still lose an unrelated provider's credential.
// Keyed on the home, a completion's lineage check, native write, and
// confirmation are one indivisible sequence against a disconnect's read,
// generation bump, removal, absence proof, and removed record.
func (p *providerAuth) lockSlot(ctx context.Context, home string) (func(), bool) {
	return authAcquireGate(ctx, &p.mu, p.slots, home)
}

// lockFlowSlot takes the credential-slot gate for a leg that answers for a
// flow.
func (p *providerAuth) lockFlowSlot(ctx context.Context, flow *authFlow, home string) (func(), error) {
	release, ok := p.lockSlot(ctx, home)
	if !ok {
		return nil, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	}

	return release, nil
}

// lockLedger serializes every read-modify-write of one provider's durable
// ledger entry: authorize's revision bump, disconnect's generation bump, a
// completion's lineage check and confirmation, and injection's record of what
// it installed. Each of those decides what to write from what it just read, and
// an interleaved write in between is read back and overwritten — a disconnect's
// generation bump lost to an authorize's revision bump leaves the removal
// standing in the native store under a record naming a generation the owner
// retired.
//
// The key is the provider id rather than the native home the credential-slot
// gate uses, because that is the identity of the record these legs rewrite: the
// ledger is agent-wide, one file per provider under the host's own root with no
// per-home segment, so two sessions rewriting one provider's record would hold
// two different home gates and serialize nothing. It is always taken inside the
// slot gate, never around it.
func (p *providerAuth) lockLedger(ctx context.Context, providerID string) (func(), bool) {
	return authAcquireGate(ctx, &p.mu, p.ledgers, providerID)
}

// lockFlowLedger takes the ledger gate for a leg that answers for a flow.
func (p *providerAuth) lockFlowLedger(ctx context.Context, flow *authFlow) (func(), error) {
	release, ok := p.lockLedger(ctx, flow.providerID)
	if !ok {
		return nil, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	}

	return release, nil
}

// publishFlow retires and terminalizes the record a new authorize replaces and
// publishes the new one, refusing outright once the session has closed.
// Publication is the authoritative session check: close marks the id and takes
// its cleanup set in one critical section under this same lock, so a flow that
// publishes before it is swept and cancelled with the rest, and a flow that
// would publish after it never exists at all. The alternative — making close
// wait for the legs already in flight — blocks the whole teardown for the
// length of an unbounded native call, and refusing publication holds the same
// invariant without that: no flow escapes close's cleanup set.
//
// It refuses on the lifetime and not merely on the id, because the two stop
// agreeing the moment an id is reinstated. The mark is what orders this check
// against the sweep — both take this lock — but it is cleared when session/load
// hydrates the id again, and the leg being admitted here may have resolved its
// session before any of that and waited out the whole close behind a gate. Such
// a leg still holds the object close tore down, so the session's own flag is
// what names which lifetime it belongs to: close and delete both set it before
// reaching this broker, and nothing ever clears it.
//
// This is the only place the broker mutex and a session mutex are held at once,
// and the order is broker first. Nothing held under a session mutex reaches
// back for this one or for the agent's, so the order cannot close a cycle.
func (p *providerAuth) publishFlow(ctx context.Context, session *session, key authFlowKey, flow *authFlow) error {
	p.mu.Lock()

	_, closed := p.closedSessions[key.sessionID]
	if closed || session.lifetimeEnded() {
		p.mu.Unlock()

		return unknownSessionError()
	}

	if previous, ok := p.retained[key]; ok {
		p.retire(key, previous.authorizeRequestID)
	}

	superseded := p.flows[key]
	if superseded != nil {
		delete(p.byID, superseded.id)

		superseded.state = authStateCancelled
		superseded.reason = authReasonSuperseded

		superseded.stopCompleter()
	}

	p.flows[key] = flow
	p.byID[flow.id] = flow
	p.retained[key] = flow

	p.mu.Unlock()

	if superseded != nil {
		p.cancelNative(ctx, superseded)
	}

	return nil
}

// retire records an idempotency key the broker can no longer answer. Only the
// newest record per key is replayable, so a delayed retry of an older one has
// nothing to replay — and minting in its place would destroy the live flow it
// never named, which is the one thing an idempotency key exists to prevent. The
// caller holds the mutex.
func (p *providerAuth) retire(key authFlowKey, requestID string) {
	keys, ok := p.retired[key]
	if !ok {
		keys = make(map[string]struct{})
		p.retired[key] = keys
	}

	keys[requestID] = struct{}{}
}

func (p *providerAuth) requestRetired(key authFlowKey, requestID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, retired := p.retired[key][requestID]

	return retired
}

// sessionClosed reports whether the session is one close has already swept.
// Refusing a leg on it is the cheap path; publishFlow is the correct one.
func (p *providerAuth) sessionClosed(sessionID acp.SessionId) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, closed := p.closedSessions[sessionID]

	return closed
}

// reopenSession drops the mark when the id becomes live again. The mark exists
// to stop a flow publishing into a session close already swept, and a
// reinstated id has a new session behind it with no such flow: keeping the mark
// would refuse its legs forever. The agent mutex is held by the caller, and
// nothing here reaches back for it.
func (p *providerAuth) reopenSession(sessionID acp.SessionId) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.closedSessions, sessionID)
}

// claimFlow admits the one leg that may drive a pending flow's native mutation
// and holds the claim for the whole attempt.
func (p *providerAuth) claimFlow(flow *authFlow) error {
	if !p.tryClaimFlow(flow) {
		return authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	return nil
}

// tryClaimFlow takes the terminal check and the claim in one critical section
// and reports whether this leg got it. The two must be indivisible because a
// native call sits between them: a second callback passes the same pending
// check while the first is still inside its write, and both then apply a
// different secret to the one reserved slot with no data race for the detector
// to find, since every field access on the record is itself locked. A leg with
// a cached answer — the status probe — skips a busy flow rather than queueing
// behind it, because whoever holds the claim is already driving the same
// completion.
func (p *providerAuth) tryClaimFlow(flow *authFlow) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if authTerminal(flow.state) || flow.claimed {
		return false
	}

	flow.claimed = true

	return true
}

// releaseFlow drops the claim. Every claimant defers it: a flow that
// terminalized rejects a later claim anyway, so releasing after success is
// harmless and keeps the paths uniform.
func (p *providerAuth) releaseFlow(flow *authFlow) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow.claimed = false
}

// claimHarvest admits the one harvest a completed flow allows and holds the
// claim across the whole attempt. The state read and the claim are one critical
// section because four record reads sit between them, and a check-then-set that
// wide lets a second leg pass the check before the first has set it — handing
// two callers the same live access and refresh material, again with nothing for
// the race detector to find.
func (p *providerAuth) claimHarvest(flow *authFlow) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if flow.state != authStateAuthenticated && flow.state != authStateSaved {
		return authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	if flow.harvested {
		return authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	flow.harvested = true

	return nil
}

// failHarvest releases the claim and fails the leg. At-most-once governs the
// credential a harvest hands back, and an attempt that handed back nothing has
// nothing to be once about.
func (p *providerAuth) failHarvest(flow *authFlow, cause string) error {
	p.mu.Lock()
	flow.harvested = false
	p.mu.Unlock()

	return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
}
