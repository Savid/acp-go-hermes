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

	// Everything from the replay check to the settled mint is held against
	// another authorize for the same key, which is what makes the idempotency
	// key mean anything: without it neither request has published a flow when
	// the other looks for one.
	release, admitted := p.admit(ctx, key)
	if !admitted {
		return nil, authFailed(authCauseTimeout, request.providerID, request.method, "")
	}

	defer release()

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
// across the read and the write and released before the mint, so a disconnect
// cannot have its generation bump read back and overwritten here, and no native
// start runs while another session's authorize waits for the same entry.
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

	if prior, ok, readErr := p.ledger.read(request.providerID); readErr == nil && ok {
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

// buildMint produces the presentation an authorize answers with. An
// operator-key method has nothing to mint: its value is submitted through
// callback and applied to the reserved slot there.
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

	p.cancelNative(ctx, flow)
}

// cancelNative invokes hermes' own flow cancel route. It never claims
// provider-side cancellation: an issued device code stays valid at the provider
// until it expires there.
func (p *providerAuth) cancelNative(ctx context.Context, flow *authFlow) {
	session, err := p.agent.session(flow.sessionID)
	if err != nil {
		return
	}

	p.cancelNativeFlow(ctx, session, flow)
}

// cancelNativeFlow cancels through a session the caller already resolved. The
// native flow id is read under the mutex because the mint commits it there, and
// the leg that cancels is routinely the one racing that commit.
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

	if claimErr := p.claimFlow(flow); claimErr != nil {
		return nil, claimErr
	}

	defer p.releaseFlow(flow)

	if input == "" || len(input) > authMaxSecretBytes {
		return nil, invalidAuthField(authFieldInput)
	}

	if flow.method.Type == authMethodTypeAPI {
		return p.applySecret(ctx, session, flow, input)
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

// applySecret writes an operator-supplied key into the reserved slot. No
// harness validates a secret at write time, so the flow reaches saved rather
// than authenticated.
func (p *providerAuth) applySecret(ctx context.Context, session *session, flow *authFlow, input string) (any, error) {
	home := session.authHome()
	if home == "" {
		return nil, p.fail(ctx, flow, authCauseTransport, true)
	}

	release, err := p.lockFlowSlot(ctx, flow, home)
	if err != nil {
		return nil, err
	}

	defer release()

	releaseLedger, err := p.lockFlowLedger(ctx, flow)
	if err != nil {
		return nil, err
	}

	defer releaseLedger()

	// The recorded lineage is compared before the write and not only after it.
	// A leg that writes first and compares second leaves the key resident under
	// a ledger entry a disconnect moved past — live at the provider, skipped by
	// inventory, and invisible on every host surface.
	if cause := p.lineageCause(flow); cause != "" {
		return nil, p.fail(ctx, flow, cause, false)
	}

	material := nativehermes.AuthMaterial{
		AuthType:    nativehermes.AuthTypeAPIKey,
		AccessToken: input,
	}

	if err := authWriteSlot(home, flow.providerID, authSlotLabel(flow.connectionID), material); err != nil {
		return nil, p.fail(ctx, flow, authCauseHarvestFailed, true)
	}

	// The write landed, so the key is resident whatever became of the flow
	// while it ran. Its provenance is recorded first — a credential nothing
	// names can be neither removed nor reported — and only then does the leg
	// find out whether the outcome is still its to report.
	confirmCause := p.confirmCause(flow)

	if cause, abandoned := p.abandonedCause(flow); abandoned {
		return nil, authFailed(cause, flow.providerID, flow.method.ID, flow.id)
	}

	if confirmCause != "" {
		return nil, p.fail(ctx, flow, confirmCause, true)
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

	release, err := p.lockFlowSlot(ctx, flow, home)
	if err != nil {
		return err
	}

	defer release()

	releaseLedger, err := p.lockFlowLedger(ctx, flow)
	if err != nil {
		return err
	}

	defer releaseLedger()

	// The lineage is read before the migration for the same reason the secret
	// apply reads it before its write: a disconnect that already proved the
	// slot absent must not have a labelled slot appear behind it.
	if cause := p.lineageCause(flow); cause != "" {
		return p.fail(ctx, flow, cause, false)
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
	if cause := p.confirmCause(flow); cause != "" {
		return p.fail(ctx, flow, cause, true)
	}

	return nil
}

// confirmCause writes the confirmation and answers the cause that stopped it
// rather than performing a transition: whether the flow is still the caller's
// to close is the caller's question. It compares no lineage of its own. The
// provider owns one entry and its gate is held from the check that admitted
// this mutation through this write, so nothing can have moved the entry in
// between — a comparison here would be asking a second time what the caller
// already established it may write.
func (p *providerAuth) confirmCause(flow *authFlow) string {
	if err := p.ledger.write(authConfirmation(flow)); err != nil {
		return authCauseProcess
	}

	return ""
}

// lineageCause reports the cause that stops a mutation this flow no longer owns
// — the provider's recorded lineage has moved past it, or it could not be read
// at all. Both callers hold the credential-slot gate and the provider's ledger
// gate across the check, the mutation it admits, and the confirmation that
// follows, so what it reports cannot go stale under them. That unbroken hold is
// the whole reason the confirmation compares nothing of its own: shorten it and
// the check moves back into confirmCause, or a successor rewrites the entry
// between the two and the credential is left resident under a lineage no
// surface names.
func (p *providerAuth) lineageCause(flow *authFlow) string {
	prior, present, err := p.ledger.read(flow.providerID)
	if err != nil {
		return authCauseProcess
	}

	if present && authLedgerAdvancedPast(prior, authConfirmation(flow)) {
		return authCauseBindingConflict
	}

	return ""
}

// authConfirmation is the record a completed leg writes and compares against.
func authConfirmation(flow *authFlow) authLedgerRecord {
	return authLedgerRecord{
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
}

// authLedgerAdvancedPast reports whether the recorded lineage already belongs
// to something later than the record offered. A removal moves the binding
// generation and every fresh authorize moves the revision, so either one ahead
// means a successor owns the provider's entry.
func authLedgerAdvancedPast(prior authLedgerRecord, record authLedgerRecord) bool {
	if prior.BindingGeneration != record.BindingGeneration {
		return prior.BindingGeneration > record.BindingGeneration
	}

	return prior.Revision > record.Revision
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
		p.cancelNative(ctx, flow)
	}
}
