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

	createdAt           int64
	state               string
	reason              string
	expiresAt           time.Time
	credentialExpiresAt int64
	harvested           bool

	nextProbeAt   time.Time
	probeInterval time.Duration

	disarm chan struct{}
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

	if replay, ok := p.replayAuthorize(key, request.authorizeRequestID); ok {
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

	p.supersede(ctx, key, authReasonSuperseded)

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
		disarm:             make(chan struct{}),
	}

	presentation, err := p.mintPresentation(ctx, session, flow)
	if err != nil {
		return nil, err
	}

	flow.presentation = presentation

	p.mu.Lock()
	p.flows[key] = flow
	p.byID[flowID] = flow
	p.mu.Unlock()

	p.armCompleter(flow)

	return presentation, nil
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

	if request.connectionID, err = authRequiredString(fields, authFieldConnectionID); err != nil {
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
// call.
func (p *providerAuth) replayAuthorize(key authFlowKey, requestID string) (authAuthorizeResult, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow, ok := p.flows[key]
	if !ok || flow.authorizeRequestID != requestID {
		return authAuthorizeResult{}, false
	}

	return flow.presentation, true
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
// submitted through callback and applied to the reserved slot there.
func (p *providerAuth) mintPresentation(ctx context.Context, session *session, flow *authFlow) (authAuthorizeResult, error) {
	result := authAuthorizeResult{
		FlowID:        flow.id,
		FlowExpiresAt: flow.expiresAt.UnixMilli(),
	}

	// The label already passed its display bound when the catalog published it,
	// and it is the only presentation text hermes gives a login method.
	result.Message = flow.method.Label

	if flow.method.Type == authMethodTypeAPI {
		result.Interaction = authInteractionSecret

		return result, nil
	}

	client := session.authNativeClient()
	if client == nil {
		return authAuthorizeResult{}, authFailed(authCauseTransport, flow.providerID, flow.method.ID, flow.id)
	}

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	start, err := client.AuthStart(callCtx, flow.providerID)
	if err != nil {
		return authAuthorizeResult{}, authFailed(authNativeCause(err), flow.providerID, flow.method.ID, flow.id)
	}

	if start.Flow != flow.method.Flow {
		return authAuthorizeResult{}, authFailed(authCauseNativeVeto, flow.providerID, flow.method.ID, flow.id)
	}

	if authLoopbackHost(start.URL) {
		return authAuthorizeResult{}, authFailed(authCauseUnsupportedVariant, flow.providerID, flow.method.ID, flow.id)
	}

	authorizeURL, ok := authDisplayURL(start.URL)
	if !ok {
		return authAuthorizeResult{}, authFailed(authCauseNativeVeto, flow.providerID, flow.method.ID, flow.id)
	}

	flow.nativeSessionID = start.SessionID
	result.URL = authorizeURL

	if start.ExpiresIn > 0 {
		if native := authNow().Add(start.ExpiresIn); native.Before(flow.expiresAt) {
			flow.expiresAt = native
			result.FlowExpiresAt = native.UnixMilli()
		}
	}

	if start.Flow == nativehermes.AuthFlowPKCE {
		result.Interaction = authInteractionCallback
		result.CallbackInput = authCallbackInputCode

		return result, nil
	}

	result.Interaction = authInteractionWait

	if start.UserCode != "" {
		code, ok := authDisplayUserCode(start.UserCode)
		if !ok {
			return authAuthorizeResult{}, authFailed(authCauseNativeVeto, flow.providerID, flow.method.ID, flow.id)
		}

		result.UserCode = code
	}

	if start.PollInterval > 0 {
		result.PollIntervalMs = start.PollInterval.Milliseconds()
		flow.probeInterval = max(start.PollInterval, authPollFloor)
	}

	return result, nil
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

// supersede terminalizes the flow a new authorize replaces and invokes the
// native cancel route alongside the wrapper disarm.
func (p *providerAuth) supersede(ctx context.Context, key authFlowKey, reason string) {
	p.mu.Lock()

	flow, ok := p.flows[key]
	if !ok {
		p.mu.Unlock()

		return
	}

	delete(p.flows, key)
	delete(p.byID, flow.id)

	flow.state = authStateCancelled
	flow.reason = reason

	flow.stopCompleter()
	p.mu.Unlock()

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

	if authTerminal(flow.state) {
		return nil, authFailed(authCauseFlowState, providerID, method, flowID)
	}

	if input == "" || len(input) > authMaxSecretBytes {
		return nil, invalidAuthField(authFieldInput)
	}

	if flow.method.Type == authMethodTypeAPI {
		return p.applySecret(ctx, session, flow, input)
	}

	if flow.presentation.CallbackInput == "" {
		return nil, invalidAuthField(authFieldInput)
	}

	return p.submitCode(ctx, session, flow, input)
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
		return nil, p.fail(ctx, flow, authNativeCause(err), true)
	}

	poll, err := client.AuthPollFlow(callCtx, flow.providerID, flow.nativeSessionID)
	if err != nil {
		return nil, p.fail(ctx, flow, authNativeCause(err), true)
	}

	switch poll.State {
	case nativehermes.AuthPollComplete:
		if err := p.completeFlow(ctx, session, flow); err != nil {
			return nil, err
		}
	case nativehermes.AuthPollDenied:
		return nil, p.fail(ctx, flow, authCauseProviderRefused, true)
	}

	return authFlowIDResult{FlowID: flow.id}, nil
}

// completeFlow migrates the entry the native flow just wrote into the reserved
// slot before anything reads it: copy the token material, remove the original
// by index, then append the labelled slot. Remove-then-add is load-bearing —
// appending under a label that already exists leaves two live copies of one
// credential — and it is what makes every unlabelled entry unharvestable.
func (p *providerAuth) completeFlow(ctx context.Context, session *session, flow *authFlow) error {
	home := session.authHome()
	if home == "" {
		return p.fail(ctx, flow, authCauseTransport, true)
	}

	migrated, migrateErr := authMigrateSlot(home, flow.providerID, authSlotLabel(flow.connectionID))
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

// fail returns the leg's closed error and performs the transition its cause
// pairs with. A cause with no transition consumes nothing.
func (p *providerAuth) fail(ctx context.Context, flow *authFlow, cause string, materialInFlight bool) error {
	if state, reason := authFlowTransition(cause, materialInFlight); state != "" {
		p.terminalize(flow, state, reason, 0)
		p.cancelNative(ctx, flow)
	}

	return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
}

func (p *providerAuth) terminalize(flow *authFlow, state string, reason string, credentialExpiresAt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

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
// torn down.
func (p *providerAuth) closeSession(ctx context.Context, sessionID acp.SessionId) {
	p.mu.Lock()

	pending := make([]*authFlow, 0, len(p.flows))

	for key, flow := range p.flows {
		if key.sessionID != sessionID {
			continue
		}

		delete(p.flows, key)
		delete(p.byID, flow.id)

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
