package hermesacp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// Closed flow states.
const (
	authStatePending       = "pending"
	authStateAuthenticated = "authenticated"
	authStateSaved         = "saved"
	authStateFailed        = "failed"
	authStateCancelled     = "cancelled"
	authStateExpired       = "expired"
)

// Closed flow reasons, legal only against the state each pairs with.
const (
	authReasonProviderRefused   = "provider_refused"
	authReasonNativeVeto        = "native_veto"
	authReasonTransport         = "transport"
	authReasonProcess           = "process"
	authReasonAcceptanceUnknown = "acceptance_unknown"
	authReasonHarvestFailed     = "harvest_failed"
	authReasonOwnerCancel       = "owner_cancel"
	authReasonSuperseded        = "superseded"
	authReasonSessionClosed     = "session_closed"
	authReasonDeadline          = "deadline"
)

// Closed interaction discriminator.
const (
	authInteractionWait     = "wait"
	authInteractionCallback = "callback"
	authInteractionSecret   = "secret"
)

const authCallbackInputCode = "code"

const (
	// authSafetyDeadline bounds a flow independently of the harness, so an
	// effective deadline exists even where no native expiry is supplied.
	authSafetyDeadline = 15 * time.Minute
	// authPollFloor is the fastest cadence a status call may drive a native
	// read at, so consumer poll cadence never propagates into a provider. It
	// deliberately raises a native interval below it.
	authPollFloor = 5 * time.Second
	// authSlowDownStep is added to the adapter's own interval when a native
	// read answers with a rate-limit refusal.
	authSlowDownStep = 5 * time.Second
	// authNativeCallTimeout bounds one non-blocking native auth call.
	authNativeCallTimeout = 30 * time.Second
)

var (
	authRandRead = rand.Read
	authNow      = time.Now
)

// authFlow is the session-scoped record of one login. The presentation it can
// replay lives here and nowhere else: it carries url, message, and userCode,
// which are code-bearing for the flow's life.
type authFlow struct {
	id                 string
	sessionID          acp.SessionId
	providerID         string
	connectionID       string
	revision           int64
	bindingGeneration  int64
	method             authCatalogMethod
	authorizeRequestID string
	nativeSessionID    string
	presentation       authAuthorizeResult

	poolSnapshot nativehermes.AuthPoolSnapshot

	createdAt           int64
	state               string
	reason              string
	expiresAt           time.Time
	credentialExpiresAt int64
	harvested           bool

	nextProbeAt   time.Time
	probeInterval time.Duration

	// ready is closed once the mint has settled, with either a presentation or
	// the failure that replaced it. A repeat of the idempotency key that arrives
	// before then waits here rather than replaying a presentation nobody minted.
	ready   chan struct{}
	mintErr error

	disarm chan struct{}
}

// authMint is what one native start produces: the wire presentation and the
// mutable flow state it settles. They are committed together, so a concurrent
// status never observes half a mint.
type authMint struct {
	presentation    authAuthorizeResult
	nativeSessionID string
	expiresAt       time.Time
	probeInterval   time.Duration
	snapshot        nativehermes.AuthPoolSnapshot
}

type authAuthorizeResult struct {
	Interaction    string `json:"interaction"`
	URL            string `json:"url,omitempty"`
	Message        string `json:"message"`
	UserCode       string `json:"userCode,omitempty"`
	CallbackInput  string `json:"callbackInput,omitempty"`
	FlowID         string `json:"flowId"`
	FlowExpiresAt  int64  `json:"flowExpiresAt"`
	PollIntervalMs int64  `json:"pollIntervalMs,omitempty"`
}

type authFlowIDResult struct {
	FlowID string `json:"flowId"`
}

type authStatusResult struct {
	FlowID    string `json:"flowId"`
	State     string `json:"state"`
	ExpiresAt int64  `json:"expiresAt,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func authTerminal(state string) bool {
	return state != authStatePending
}

// newAuthToken mints an opaque adapter-owned identifier from 16 CSPRNG bytes,
// encoded unpadded base64url. Native flow handles never cross the boundary.
func newAuthToken() (string, error) {
	var value [16]byte
	if _, err := authRandRead(value[:]); err != nil {
		return "", fmt.Errorf("create provider auth token: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

// authorize starts exactly one flow per (sessionId, providerId). It records the
// idempotency key before any native mint and has persisted the flow's slot
// binding before it returns.
func (p *providerAuth) authorize(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params,
		authFieldSessionID, authFieldProviderID, authFieldConnectionID,
		authFieldMethodsGeneration, authFieldMethod, authFieldAuthorizeRequestID, authFieldInputs)
	if err != nil {
		return nil, err
	}

	request, err := decodeAuthorizeRequest(fields)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(request.sessionID)
	if err != nil {
		return nil, err
	}

	key := authFlowKey{sessionID: session.id, providerID: request.providerID}

	if replay, ok, replayErr := p.replayAuthorize(ctx, key, request.authorizeRequestID); ok {
		if replayErr != nil {
			return nil, replayErr
		}

		return replay, nil
	}

	method, err := p.resolveMethod(request)
	if err != nil {
		return nil, err
	}

	if inputErr := validateAuthInputs(request.inputs); inputErr != nil {
		return nil, inputErr
	}

	flowID, err := newAuthToken()
	if err != nil {
		return nil, authFailed(authCauseProcess, request.providerID, request.method, "")
	}

	now := authNow()
	record := authLedgerRecord{
		ProviderID:         request.providerID,
		ConnectionID:       request.connectionID,
		Revision:           1,
		BindingGeneration:  1,
		FlowID:             flowID,
		AuthorizeRequestID: request.authorizeRequestID,
		State:              authLedgerIntent,
		CreatedAt:          now.UnixMilli(),
		UpdatedAt:          now.UnixMilli(),
	}

	if prior, ok, readErr := p.ledger.read(request.providerID); readErr == nil && ok {
		record.Revision = prior.Revision + 1
		record.BindingGeneration = prior.BindingGeneration
		record.CreatedAt = prior.CreatedAt
	}

	if writeErr := p.ledger.write(record); writeErr != nil {
		return nil, authFailed(authCauseProcess, request.providerID, request.method, "")
	}

	flow := &authFlow{
		id:                 flowID,
		sessionID:          session.id,
		providerID:         request.providerID,
		connectionID:       request.connectionID,
		revision:           record.Revision,
		bindingGeneration:  record.BindingGeneration,
		method:             method,
		authorizeRequestID: request.authorizeRequestID,
		createdAt:          record.CreatedAt,
		state:              authStatePending,
		expiresAt:          now.Add(authSafetyDeadline),
		probeInterval:      authPollFloor,
		ready:              make(chan struct{}),
		disarm:             make(chan struct{}),
	}

	p.registerFlow(ctx, key, flow)

	mint, cause := p.mintPresentation(ctx, session, flow)
	p.commitMint(flow, mint)

	if cause != "" {
		failure := p.fail(ctx, flow, cause, false)
		p.settle(flow, failure)

		return nil, failure
	}

	p.settle(flow, nil)
	p.armCompleter(flow)

	return mint.presentation, nil
}

// registerFlow terminalizes the flow a new authorize replaces and publishes the
// new record in one step, before the mint that fills it in has run: a repeat of
// the idempotency key has something to find from the moment the flow exists,
// and a mint failure addresses a real flow rather than nothing. The retained
// record outlives every terminal transition, so the key answers for as long as
// the session lives.
func (p *providerAuth) registerFlow(ctx context.Context, key authFlowKey, flow *authFlow) {
	p.mu.Lock()

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
}

// commitMint settles what the native start produced onto the flow record.
func (p *providerAuth) commitMint(flow *authFlow, mint authMint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow.presentation = mint.presentation
	flow.nativeSessionID = mint.nativeSessionID
	flow.expiresAt = mint.expiresAt
	flow.probeInterval = mint.probeInterval
	flow.poolSnapshot = mint.snapshot
}

// settle releases every repeat waiting on the mint, with the presentation it
// produced or the failure that replaced it.
func (p *providerAuth) settle(flow *authFlow, mintErr error) {
	p.mu.Lock()
	flow.mintErr = mintErr
	p.mu.Unlock()

	close(flow.ready)
}

type authorizeRequest struct {
	sessionID    string
	providerID   string
	connectionID string
	generation   string
	method       string
	// authorizeRequestID is the caller-minted idempotency key. authorize is the
	// only leg that takes one because it is the most destructive leg here.
	authorizeRequestID string
	inputs             map[string]string
}

func decodeAuthorizeRequest(fields map[string]json.RawMessage) (authorizeRequest, error) {
	request := authorizeRequest{}

	var err error
	if request.sessionID, err = authRequiredString(fields, authFieldSessionID); err != nil {
		return request, err
	}

	if request.providerID, err = authRequiredString(fields, authFieldProviderID); err != nil {
		return request, err
	}

	if request.connectionID, err = authRequiredConnectionID(fields); err != nil {
		return request, err
	}

	if request.generation, err = authRequiredString(fields, authFieldMethodsGeneration); err != nil {
		return request, err
	}

	if request.method, err = authRequiredString(fields, authFieldMethod); err != nil {
		return request, err
	}

	if request.authorizeRequestID, err = authRequiredString(fields, authFieldAuthorizeRequestID); err != nil {
		return request, err
	}

	if raw, ok := fields[authFieldInputs]; ok {
		if err := json.Unmarshal(raw, &request.inputs); err != nil {
			return request, invalidAuthField(authFieldInputs)
		}
	}

	return request, nil
}

// replayAuthorize answers a repeated idempotency key verbatim from memory: no
// supersede, no completer disarm, no destruction of flow state, and no native
// call. It answers from the retained record rather than the pending one, so a
// completed, failed, cancelled, or expired flow still replays; and it waits for
// the mint the first call started rather than replaying a presentation nobody
// has minted yet.
func (p *providerAuth) replayAuthorize(ctx context.Context, key authFlowKey, requestID string) (authAuthorizeResult, bool, error) {
	p.mu.Lock()
	flow, ok := p.retained[key]
	p.mu.Unlock()

	if !ok || flow.authorizeRequestID != requestID {
		return authAuthorizeResult{}, false, nil
	}

	select {
	case <-flow.ready:
	case <-ctx.Done():
		return authAuthorizeResult{}, true, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return flow.presentation, true, flow.mintErr
}

// resolveMethod fences a method id against the generation that produced it.
func (p *providerAuth) resolveMethod(request authorizeRequest) (authCatalogMethod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.generation == "" || p.generation != request.generation {
		return authCatalogMethod{}, invalidAuthField(authFieldMethodsGeneration)
	}

	for _, method := range p.catalog[request.providerID] {
		if method.ID == request.method {
			return method, nil
		}
	}

	return authCatalogMethod{}, invalidAuthField(authFieldMethod)
}

// mintPresentation performs the native start for an oauth method and builds the
// wire presentation. An operator-key method has nothing to mint: its value is
// submitted through callback and applied to the reserved slot there. An empty
// cause is the success answer; every other cause carries the transition its
// caller performs.
func (p *providerAuth) mintPresentation(ctx context.Context, session *session, flow *authFlow) (authMint, string) {
	mint := authMint{
		presentation: authAuthorizeResult{
			FlowID:        flow.id,
			FlowExpiresAt: flow.expiresAt.UnixMilli(),
			// The label already passed its display bound when the catalog
			// published it, and it is the only presentation text hermes gives a
			// login method.
			Message: flow.method.Label,
		},
		expiresAt:     flow.expiresAt,
		probeInterval: flow.probeInterval,
	}

	if flow.method.Type == authMethodTypeAPI {
		mint.presentation.Interaction = authInteractionSecret

		return mint, ""
	}

	client := session.authNativeClient()
	if client == nil {
		return mint, authCauseTransport
	}

	// The pool is fingerprinted before the native flow can append to it, so the
	// entry this flow produces is the only one the reserved slot can claim.
	snapshot, err := authSnapshotPool(client.XDGDirs().Root, flow.providerID)
	if err != nil {
		return mint, authCauseHarvestFailed
	}

	mint.snapshot = snapshot

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	start, err := client.AuthStart(callCtx, flow.providerID)
	if err != nil {
		return mint, authNativeCause(err)
	}

	mint.nativeSessionID = start.SessionID

	if start.Flow != flow.method.Flow {
		return mint, authCauseNativeVeto
	}

	if authLoopbackHost(start.URL) {
		return mint, authCauseUnsupportedVariant
	}

	authorizeURL, ok := authDisplayURL(start.URL)
	if !ok {
		return mint, authCauseNativeVeto
	}

	mint.presentation.URL = authorizeURL

	if start.ExpiresIn > 0 {
		if native := authNow().Add(start.ExpiresIn); native.Before(mint.expiresAt) {
			mint.expiresAt = native
			mint.presentation.FlowExpiresAt = native.UnixMilli()
		}
	}

	if start.Flow == nativehermes.AuthFlowPKCE {
		mint.presentation.Interaction = authInteractionCallback
		mint.presentation.CallbackInput = authCallbackInputCode

		return mint, ""
	}

	mint.presentation.Interaction = authInteractionWait

	if start.UserCode != "" {
		code, ok := authDisplayUserCode(start.UserCode)
		if !ok {
			return mint, authCauseNativeVeto
		}

		mint.presentation.UserCode = code
	}

	if start.PollInterval > 0 {
		mint.presentation.PollIntervalMs = start.PollInterval.Milliseconds()
		mint.probeInterval = max(start.PollInterval, authPollFloor)
	}

	return mint, ""
}

// armCompleter bounds the flow by its effective deadline. It is armed exactly
// once, at authorize, and status never starts, extends, or rearms it.
func (p *providerAuth) armCompleter(flow *authFlow) {
	deadline := time.Until(flow.expiresAt)
	disarm := flow.disarm

	p.goSafe("provider auth completer", func() {
		timer := time.NewTimer(deadline)
		defer timer.Stop()

		select {
		case <-disarm:
			return
		case <-timer.C:
			p.expire(flow)
		}
	})
}

func (p *providerAuth) expire(flow *authFlow) {
	p.mu.Lock()

	if authTerminal(flow.state) {
		p.mu.Unlock()

		return
	}

	flow.state = authStateExpired
	flow.reason = authReasonDeadline

	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	p.cancelNative(ctx, flow)
}

// cancelNative invokes hermes' own flow cancel route. It never claims
// provider-side cancellation: an issued device code stays valid at the provider
// until it expires there.
func (p *providerAuth) cancelNative(ctx context.Context, flow *authFlow) {
	if flow.nativeSessionID == "" {
		return
	}

	session, err := p.agent.session(flow.sessionID)
	if err != nil {
		return
	}

	client := session.authNativeClient()
	if client == nil {
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	_ = client.AuthCancelFlow(callCtx, flow.nativeSessionID)
}

func (f *authFlow) stopCompleter() {
	select {
	case <-f.disarm:
	default:
		close(f.disarm)
	}
}

// callback submits the flow's expected value: the authorization code a pkce
// flow pastes back, or the operator key an interaction:"secret" method applies
// to the reserved slot.
func (p *providerAuth) callback(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldMethod, authFieldFlowID, authFieldInput)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, err
	}

	method, err := authRequiredString(fields, authFieldMethod)
	if err != nil {
		return nil, err
	}

	flowID, err := authRequiredString(fields, authFieldFlowID)
	if err != nil {
		return nil, err
	}

	input, err := authString(fields, authFieldInput)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	flow, err := p.addressFlow(session.id, providerID, flowID)
	if err != nil {
		return nil, err
	}

	if flow.method.ID != method {
		return nil, invalidAuthField(authFieldMethod)
	}

	state, callbackInput := p.flowGate(flow)

	if authTerminal(state) {
		return nil, authFailed(authCauseFlowState, providerID, method, flowID)
	}

	if input == "" || len(input) > authMaxSecretBytes {
		return nil, invalidAuthField(authFieldInput)
	}

	if flow.method.Type == authMethodTypeAPI {
		return p.applySecret(ctx, session, flow, input)
	}

	if callbackInput == "" {
		return nil, invalidAuthField(authFieldInput)
	}

	return p.submitCode(ctx, session, flow, input)
}

// flowGate reads the two record fields a callback is admitted on. Both are
// settled by the mint, which a callback can race.
func (p *providerAuth) flowGate(flow *authFlow) (string, string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return flow.state, flow.presentation.CallbackInput
}

// applySecret writes an operator-supplied key into the reserved slot. No
// harness validates a secret at write time, so the flow reaches saved rather
// than authenticated.
func (p *providerAuth) applySecret(ctx context.Context, session *session, flow *authFlow, input string) (any, error) {
	home := session.authHome()
	if home == "" {
		return nil, p.fail(ctx, flow, authCauseTransport, true)
	}

	material := nativehermes.AuthMaterial{
		AuthType:    nativehermes.AuthTypeAPIKey,
		AccessToken: input,
	}

	if err := authWriteSlot(home, flow.providerID, authSlotLabel(flow.connectionID), material); err != nil {
		return nil, p.fail(ctx, flow, authCauseHarvestFailed, true)
	}

	if err := p.confirm(ctx, flow); err != nil {
		return nil, err
	}

	p.terminalize(flow, authStateSaved, "", 0)

	return authFlowIDResult{FlowID: flow.id}, nil
}

// submitCode hands the pasted authorization code to hermes and then reads the
// flow's own poll route, which is the only completion signal on this surface.
func (p *providerAuth) submitCode(ctx context.Context, session *session, flow *authFlow, input string) (any, error) {
	client := session.authNativeClient()
	if client == nil {
		return nil, p.fail(ctx, flow, authCauseTransport, true)
	}

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	if err := client.AuthSubmit(callCtx, flow.providerID, flow.nativeSessionID, input); err != nil {
		return nil, p.failSettled(ctx, flow, authNativeCause(err), true)
	}

	poll, err := client.AuthPollFlow(callCtx, flow.providerID, flow.nativeSessionID)
	if err != nil {
		return nil, p.failSettled(ctx, flow, authNativeCause(err), true)
	}

	switch poll.State {
	case nativehermes.AuthPollComplete:
		if err := p.completeFlow(ctx, session, flow); err != nil {
			return nil, err
		}
	case nativehermes.AuthPollDenied:
		return nil, p.failSettled(ctx, flow, authCauseProviderRefused, true)
	}

	return authFlowIDResult{FlowID: flow.id}, nil
}

// completeFlow migrates the entry the native flow just wrote into the reserved
// slot before anything reads it: copy the token material, remove the original
// by index, then append the labelled slot. Remove-then-add is load-bearing —
// appending under a label that already exists leaves two live copies of one
// credential — and it is what makes every unlabelled entry unharvestable.
func (p *providerAuth) completeFlow(ctx context.Context, session *session, flow *authFlow) error {
	if cause, abandoned := p.abandonedCause(flow); abandoned {
		return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
	}

	home := session.authHome()
	if home == "" {
		return p.fail(ctx, flow, authCauseTransport, true)
	}

	migrated, migrateErr := authMigrateSlot(home, flow.providerID, authSlotLabel(flow.connectionID), flow.poolSnapshot)
	if migrateErr != nil || !migrated {
		return p.fail(ctx, flow, authCauseHarvestFailed, true)
	}

	if err := p.confirm(ctx, flow); err != nil {
		return err
	}

	expiry, _, expiryErr := authReadFlowExpiry(home, flow.providerID, flow.method.Flow)
	if expiryErr != nil {
		return p.fail(ctx, flow, authCauseHarvestFailed, true)
	}

	p.terminalize(flow, authStateAuthenticated, "", nativehermes.AuthAnchorExpiry(authNow(), expiry))

	return nil
}

// confirm records the post-mutation confirmation, which is what separates a
// residence answer of confirmed_present from not_confirmed.
func (p *providerAuth) confirm(ctx context.Context, flow *authFlow) error {
	record := authLedgerRecord{
		ProviderID:         flow.providerID,
		ConnectionID:       flow.connectionID,
		Revision:           flow.revision,
		BindingGeneration:  flow.bindingGeneration,
		FlowID:             flow.id,
		AuthorizeRequestID: flow.authorizeRequestID,
		State:              authLedgerConfirmed,
		CreatedAt:          flow.createdAt,
		UpdatedAt:          authNow().UnixMilli(),
	}

	if err := p.ledger.write(record); err != nil {
		return p.fail(ctx, flow, authCauseProcess, true)
	}

	return nil
}

// abandonedCause reports the cause a leg answers with when the flow reached a
// terminal state while the native call this leg started was still in flight.
// Such a leg owns no transition and confirms nothing: the record it addressed
// is already closed, and the outcome it carries is no longer the flow's.
func (p *providerAuth) abandonedCause(flow *authFlow) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch {
	case !authTerminal(flow.state):
		return "", false
	case flow.state == authStateCancelled:
		return authCauseFlowCancelled, true
	default:
		return authCauseFlowState, true
	}
}

// failSettled answers a native outcome that could have arrived after the flow
// closed. The transition it would otherwise perform belongs to whoever closed
// the flow first, and a cause naming the provider over a login the owner
// abandoned reports a refusal nobody made.
func (p *providerAuth) failSettled(ctx context.Context, flow *authFlow, cause string, materialInFlight bool) error {
	if abandoned, ok := p.abandonedCause(flow); ok {
		return authFailed(abandoned, flow.providerID, flow.method.ID, flow.id)
	}

	return p.fail(ctx, flow, cause, materialInFlight)
}

// fail returns the leg's closed error and performs the transition its cause
// pairs with. A cause with no transition consumes nothing.
func (p *providerAuth) fail(ctx context.Context, flow *authFlow, cause string, materialInFlight bool) error {
	if state, reason := authFlowTransition(cause, materialInFlight); state != "" {
		p.terminalize(flow, state, reason, 0)
		p.cancelNative(ctx, flow)
	}

	return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
}

// terminalize records the flow's one terminal transition and frees the pending
// slot. A flow that already reached one keeps it: a native answer still in
// flight when the owner cancelled arrives into a record the owner already
// closed, and what it carries is no longer the flow's outcome.
func (p *providerAuth) terminalize(flow *authFlow, state string, reason string, credentialExpiresAt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if authTerminal(flow.state) {
		return
	}

	flow.state = state
	flow.reason = reason
	flow.credentialExpiresAt = credentialExpiresAt

	flow.stopCompleter()
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
}

// addressFlow resolves a flowId a caller supplied. A missing, unknown,
// superseded, or cross-session id is a caller addressing failure and never a
// flow failure.
func (p *providerAuth) addressFlow(sessionID acp.SessionId, providerID string, flowID string) (*authFlow, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow, ok := p.byID[flowID]
	if !ok || flow.sessionID != sessionID || flow.providerID != providerID {
		return nil, invalidAuthField(authFieldFlowID)
	}

	return flow, nil
}

// status reports the flow, not the connection. Its expiresAt is credential
// expiry and never flow expiry.
func (p *providerAuth) status(ctx context.Context, params json.RawMessage) (any, error) {
	session, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.probe(ctx, session, flow)

	p.mu.Lock()
	defer p.mu.Unlock()

	result := authStatusResult{FlowID: flow.id, State: flow.state, Reason: flow.reason}
	if flow.state == authStateAuthenticated {
		result.ExpiresAt = flow.credentialExpiresAt
	}

	return result, nil
}

// probe refreshes a pending flow from the native poll route behind the
// adapter's own interval, serving the cached state in between so a consumer's
// poll cadence never reaches the provider. The interval is the native one
// raised to the floor, and a native slow_down adds five seconds to it, which
// the next status reports through the slower cadence it produces.
func (p *providerAuth) probe(ctx context.Context, session *session, flow *authFlow) {
	p.mu.Lock()

	now := authNow()
	if authTerminal(flow.state) || flow.nativeSessionID == "" || now.Before(flow.nextProbeAt) {
		p.mu.Unlock()

		return
	}

	flow.nextProbeAt = now.Add(flow.probeInterval)
	p.mu.Unlock()

	client := session.authNativeClient()
	if client == nil {
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	poll, err := client.AuthPollFlow(callCtx, flow.providerID, flow.nativeSessionID)
	if err != nil {
		return
	}

	switch poll.State {
	case nativehermes.AuthPollComplete:
		_ = p.completeFlow(ctx, session, flow)
	case nativehermes.AuthPollDenied:
		p.terminalize(flow, authStateFailed, authReasonProviderRefused, 0)
	case nativehermes.AuthPollSlowDown:
		p.mu.Lock()
		flow.probeInterval += authSlowDownStep
		flow.nextProbeAt = now.Add(flow.probeInterval)
		p.mu.Unlock()
	}
}

// cancel disarms the completer, terminalizes the flow record, frees the pending
// slot, and invokes hermes' native cancel route. It never claims provider-side
// cancellation.
func (p *providerAuth) cancel(ctx context.Context, params json.RawMessage) (any, error) {
	_, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()

	if authTerminal(flow.state) {
		p.mu.Unlock()

		return authFlowIDResult{FlowID: flow.id}, nil
	}

	flow.state = authStateCancelled
	flow.reason = authReasonOwnerCancel

	flow.stopCompleter()
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	p.mu.Unlock()

	p.cancelNative(ctx, flow)

	return authFlowIDResult{FlowID: flow.id}, nil
}

func (p *providerAuth) addressedFlowLeg(params json.RawMessage) (*session, *authFlow, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldFlowID)
	if err != nil {
		return nil, nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, nil, err
	}

	flowID, err := authRequiredString(fields, authFieldFlowID)
	if err != nil {
		return nil, nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, nil, err
	}

	flow, err := p.addressFlow(session.id, providerID, flowID)
	if err != nil {
		return nil, nil, err
	}

	return session, flow, nil
}

// closeSession cancels every pending flow the session owns, terminalizing each
// as cancelled/session_closed and invoking native cancel. It runs before the
// native interrupt, so a flow is never abandoned to a process already being
// torn down. Closing the session is also what ends the reach of an idempotency
// key: the retained records go with it.
func (p *providerAuth) closeSession(ctx context.Context, sessionID acp.SessionId) {
	p.mu.Lock()

	pending := make([]*authFlow, 0, len(p.flows))

	for key, flow := range p.retained {
		if key.sessionID != sessionID {
			continue
		}

		delete(p.retained, key)
		delete(p.byID, flow.id)

		if _, live := p.flows[key]; !live {
			continue
		}

		delete(p.flows, key)

		flow.state = authStateCancelled
		flow.reason = authReasonSessionClosed

		flow.stopCompleter()

		pending = append(pending, flow)
	}

	p.mu.Unlock()

	for _, flow := range pending {
		p.cancelNative(ctx, flow)
	}
}
