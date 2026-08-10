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
	authReasonOwnerCancel       = "owner_cancel"
	authReasonSuperseded        = "superseded"
	authReasonSessionClosed     = "session_closed"
	authReasonDeadline          = "deadline"
)

// Closed interaction discriminator.
const (
	authInteractionWait     = "wait"
	authInteractionCallback = "callback"
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
	id        string
	sessionID acp.SessionId
	// session is the exact ACP-session lifetime that minted the native flow.
	// Session IDs are reusable after close/load, so native cleanup must never
	// resolve this lifetime again through the agent's current session map.
	session            *session
	providerID         string
	connectionID       string
	revision           int64
	bindingGeneration  int64
	method             authCatalogMethod
	authorizeRequestID string
	nativeSessionID    string
	presentation       authAuthorizeResult

	createdAt int64
	state     string
	reason    string
	expiresAt time.Time

	// claimed marks the flow as held by the one leg currently driving its
	// native mutation.
	claimed bool

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
// idempotency key before any native mint and persists the flow lineage before
// it returns.
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

	// Everything from the replay check to the settled mint is held against
	// another authorize for the same key, which is what makes the idempotency
	// key mean anything: without it neither request has published a flow when
	// the other looks for one.
	release, admitted := p.admit(ctx, key)
	if !admitted {
		return nil, authFailed(authCauseTimeout, request.providerID, request.method, "")
	}

	defer release()

	releaseProvider, admitted := p.lockProvider(ctx, request.providerID)
	if !admitted {
		return nil, authFailed(authCauseTimeout, request.providerID, request.method, "")
	}
	defer releaseProvider()

	if replay, ok, replayErr := p.replayAuthorize(ctx, key, request.authorizeRequestID); ok {
		if replayErr != nil {
			return nil, replayErr
		}

		return replay, nil
	}

	if p.requestRetired(key, request.authorizeRequestID) {
		return nil, invalidAuthField(authFieldAuthorizeRequestID)
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

	record, err := p.recordAuthorizeIntent(ctx, request, flowID, now)
	if err != nil {
		return nil, err
	}

	p.cancelProviderFlows(ctx, request.providerID)

	flow := &authFlow{
		id:                 flowID,
		sessionID:          session.id,
		session:            session,
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

	// The flow is published before the mint that fills it in has run: a repeat
	// of the idempotency key has something to find from the moment the flow
	// exists, and a mint failure addresses a real flow rather than nothing. The
	// retained record outlives every terminal transition, so the key answers
	// for as long as the session lives.
	if publishErr := p.publishFlow(ctx, session, key, flow); publishErr != nil {
		return nil, publishErr
	}

	mint, cause := p.mintPresentation(ctx, session, flow)

	if cause != "" {
		failure := p.fail(ctx, flow, cause, false)
		p.settle(flow, failure)

		return nil, failure
	}

	// The mint outlived a close that swept this flow: the login it started is
	// stoppable only through the id the commit just recorded, and close read
	// that id before the commit wrote it. Cancelling here is what leaves no
	// login running for a session nobody can address.
	if abandoned, ok := p.abandonedCause(flow); ok {
		p.cancelNativeFlow(ctx, session, flow)

		failure := authFailed(abandoned, flow.providerID, flow.method.ID, flow.id)
		p.settle(flow, failure)

		return nil, failure
	}

	p.settle(flow, nil)
	p.armCompleter(flow)

	return mint.presentation, nil
}

// recordAuthorizeIntent performs the one read-modify-write authorize makes on
// the provider's ledger entry: the record it reads decides the revision this
// flow claims and carries the binding generation forward. The gate is held
// across the read and the write and released before the mint, so no native start
// runs while another session's authorize waits for the same entry.
func (p *providerAuth) recordAuthorizeIntent(ctx context.Context, request authorizeRequest, flowID string, now time.Time) (authLedgerRecord, error) {
	release, acquired := p.lockLedger(ctx, request.providerID)
	if !acquired {
		return authLedgerRecord{}, authFailed(authCauseTimeout, request.providerID, request.method, "")
	}

	defer release()

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

	prior, ok, readErr := p.ledger.read(request.providerID)
	if readErr != nil {
		return authLedgerRecord{}, authFailed(authCauseProcess, request.providerID, request.method, "")
	}

	if ok {
		record.Revision = prior.Revision + 1
		record.BindingGeneration = prior.BindingGeneration
		record.CreatedAt = prior.CreatedAt
	}

	if writeErr := p.ledger.write(record); writeErr != nil {
		return authLedgerRecord{}, authFailed(authCauseProcess, request.providerID, request.method, "")
	}

	return record, nil
}

// commitMint settles what the native start produced onto the flow record.
func (p *providerAuth) commitMint(flow *authFlow, mint authMint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow.presentation = mint.presentation
	flow.nativeSessionID = mint.nativeSessionID
	flow.expiresAt = mint.expiresAt
	flow.probeInterval = mint.probeInterval
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
// wire presentation, then settles whatever it produced onto the flow record
// before its caller sees the cause. Committing on every path is what makes the
// caller's fence reach a login the mint began: the fence cancels by the session
// id on the record, and a refusal that returned before the commit would leave
// the login running at the provider with nothing left to stop it. An empty
// cause is the success answer; every other cause carries the transition its
// caller performs.
func (p *providerAuth) mintPresentation(ctx context.Context, session *session, flow *authFlow) (authMint, string) {
	mint, cause := p.buildMint(ctx, session, flow)
	p.commitMint(flow, mint)

	return mint, cause
}

// buildMint produces the presentation an authorize answers with.
func (p *providerAuth) buildMint(ctx context.Context, session *session, flow *authFlow) (authMint, string) {
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

	client := session.authNativeClient()
	if client == nil {
		return mint, authCauseTransport
	}

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	start, err := client.AuthStart(callCtx, flow.providerID)
	if err != nil {
		return mint, authNativeCause(err)
	}

	// The started login is stoppable only through this id, and every judgement
	// about what was started is made below, by a function that receives the
	// mint already carrying it. A veto cannot be written that returns without
	// it, so a refused login is never one nothing can cancel.
	mint.nativeSessionID = start.SessionID

	return applyNativeStart(mint, start, flow.method)
}

// applyNativeStart judges what the native start produced and folds it into the
// mint. It refuses a flow variant the catalog did not publish, an authorization
// URL only this host could reach, and any display value that fails its bound.
func applyNativeStart(mint authMint, start nativehermes.AuthStart, method authCatalogMethod) (authMint, string) {
	if start.Flow != method.Flow {
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

	p.cancelNativeFlow(ctx, flow.session, flow)
}

// cancelNativeFlow cancels through the exact session lifetime the caller
// already resolved. The native flow id is read under the mutex because the
// mint commits it there, and the leg that cancels is routinely the one racing
// that commit. It never claims provider-side cancellation: an issued device
// code stays valid at the provider until it expires there.
func (p *providerAuth) cancelNativeFlow(ctx context.Context, session *session, flow *authFlow) {
	p.mu.Lock()
	nativeSessionID := flow.nativeSessionID
	p.mu.Unlock()

	if nativeSessionID == "" {
		return
	}

	client := session.authNativeClient()
	if client == nil {
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	_ = client.AuthCancelFlow(callCtx, nativeSessionID)
}

func (f *authFlow) stopCompleter() {
	select {
	case <-f.disarm:
	default:
		close(f.disarm)
	}
}

// callback submits the authorization code a PKCE flow pastes back.
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

	if claimErr := p.claimFlow(flow); claimErr != nil {
		return nil, claimErr
	}

	defer p.releaseFlow(flow)

	if input == "" || len(input) > authMaxCallbackBytes {
		return nil, invalidAuthField(authFieldInput)
	}

	if p.callbackInput(flow) == "" {
		return nil, invalidAuthField(authFieldInput)
	}

	return p.submitCode(ctx, session, flow, input)
}

// callbackInput reads the value the mint published as this flow's expected
// paste-back, which a callback can race the mint for.
func (p *providerAuth) callbackInput(flow *authFlow) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return flow.presentation.CallbackInput
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
	case nativehermes.AuthPollApproved:
		if err := p.completeFlow(ctx, session, flow); err != nil {
			return nil, err
		}
	case nativehermes.AuthPollDenied:
		return nil, p.failSettled(ctx, flow, authCauseProviderRefused, true)
	case nativehermes.AuthPollExpired:
		return nil, p.failSettled(ctx, flow, authCauseFlowExpired, true)
	case nativehermes.AuthPollError:
		return nil, p.failSettled(ctx, flow, authCauseTransport, true)
	}

	return authFlowIDResult{FlowID: flow.id}, nil
}

// completeFlow records the values-free lineage of a native terminal success.
// Hermes has already persisted the credential in its own durable auth home.
func (p *providerAuth) completeFlow(ctx context.Context, session *session, flow *authFlow) error {
	if cause, abandoned := p.abandonedCause(flow); abandoned {
		return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
	}

	if session.authNativeClient() == nil {
		return p.fail(ctx, flow, authCauseTransport, true)
	}

	release, err := p.lockFlowProvider(ctx, flow)
	if err != nil {
		return err
	}

	defer release()

	releaseLedger, err := p.lockFlowLedger(ctx, flow)
	if err != nil {
		return err
	}

	defer releaseLedger()

	if cause := p.lineageCause(flow); cause != "" {
		return p.fail(ctx, flow, cause, false)
	}

	if err := p.confirm(ctx, flow); err != nil {
		return err
	}

	p.terminalize(flow, authStateAuthenticated, "")

	return nil
}

// confirm records the post-mutation confirmation, which is what separates a
// residence answer of confirmed_present from not_confirmed.
func (p *providerAuth) confirm(ctx context.Context, flow *authFlow) error {
	if cause := p.confirmCause(flow); cause != "" {
		return p.fail(ctx, flow, cause, true)
	}

	return nil
}

// terminalize records the flow's one terminal transition and frees its pending
// key. A flow that already reached one keeps it: a native answer still in
// flight when the owner cancelled arrives into a record the owner already
// closed, and what it carries is no longer the flow's outcome.
func (p *providerAuth) terminalize(flow *authFlow, state string, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if authTerminal(flow.state) {
		return
	}

	flow.state = state
	flow.reason = reason

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

// status reports the flow, not the connection.
func (p *providerAuth) status(ctx context.Context, params json.RawMessage) (any, error) {
	session, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.probe(ctx, session, flow)

	p.mu.Lock()
	defer p.mu.Unlock()

	return authStatusResult{FlowID: flow.id, State: flow.state, Reason: flow.reason}, nil
}

// probe refreshes a pending flow from the native poll route behind the
// adapter's own interval, serving the cached state in between so a consumer's
// poll cadence never reaches the provider. The interval is the native one
// raised to the floor.
func (p *providerAuth) probe(ctx context.Context, session *session, flow *authFlow) {
	if !p.tryClaimFlow(flow) {
		return
	}

	defer p.releaseFlow(flow)

	p.mu.Lock()

	now := authNow()
	if flow.nativeSessionID == "" || now.Before(flow.nextProbeAt) {
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
	case nativehermes.AuthPollApproved:
		_ = p.completeFlow(ctx, session, flow)
	case nativehermes.AuthPollDenied:
		p.terminalize(flow, authStateFailed, authReasonProviderRefused)
	case nativehermes.AuthPollExpired:
		p.terminalize(flow, authStateExpired, authReasonDeadline)
	case nativehermes.AuthPollError:
		p.terminalize(flow, authStateFailed, authReasonAcceptanceUnknown)
	}
}

// cancel disarms the completer, terminalizes the flow record, frees its pending
// key, and invokes hermes' native cancel route. It never claims provider-side
// cancellation.
func (p *providerAuth) cancel(ctx context.Context, params json.RawMessage) (any, error) {
	session, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	if p.markOwnerCancelled(flow) {
		p.cancelNativeFlow(ctx, session, flow)
	}

	return authFlowIDResult{FlowID: flow.id}, nil
}

// markOwnerCancelled performs the cancel leg's terminal transition. Keeping
// the transition separate from native cleanup makes the lifetime boundary
// explicit: the session resolved beside the flow remains the cleanup target
// even if close removes it and a load reuses the durable session ID after this
// transition.
func (p *providerAuth) markOwnerCancelled(flow *authFlow) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if authTerminal(flow.state) {
		return false
	}

	flow.state = authStateCancelled
	flow.reason = authReasonOwnerCancel

	flow.stopCompleter()
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})

	return true
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

func (p *providerAuth) cancelProviderFlows(ctx context.Context, providerID string) {
	p.mu.Lock()

	flows := make([]*authFlow, 0)
	for key, flow := range p.flows {
		if key.providerID != providerID {
			continue
		}

		delete(p.flows, key)

		flow.state = authStateCancelled
		flow.reason = authReasonSuperseded
		flow.stopCompleter()
		flows = append(flows, flow)
	}
	p.mu.Unlock()

	for _, flow := range flows {
		p.cancelNativeFlow(ctx, flow.session, flow)
	}
}

// closeSession cancels every pending flow the session owns, terminalizing each
// as cancelled/session_closed and invoking native cancel. It runs before the
// native interrupt, so a flow is never abandoned to a process already being
// torn down. Closing the session is also what ends the reach of an idempotency
// key: the retained and retired records go with it.
//
// The id is marked closed in the same critical section that takes the cleanup
// set, which is what makes the set complete: an authorize that has not yet
// published cannot publish afterwards, so there is no flow left for close to
// have missed. Waiting for the legs already in flight would be the other way to
// get that, and it would block teardown for the length of an unbounded native
// call.
func (p *providerAuth) closeSession(ctx context.Context, sessionID acp.SessionId) {
	p.mu.Lock()

	p.closedSessions[sessionID] = struct{}{}

	pending := make([]*authFlow, 0, len(p.flows))

	for key := range p.retired {
		if key.sessionID == sessionID {
			delete(p.retired, key)
		}
	}

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
		p.cancelNativeFlow(ctx, flow.session, flow)
	}
}
