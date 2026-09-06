package hermesacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// Session-scoped provider-auth extension methods. Hermes owns credential
// material in the configured native auth home; this surface coordinates only
// values-free flow and lineage state.
const (
	AuthMethodsMethod    = "_hermes/auth/methods"
	AuthAuthorizeMethod  = "_hermes/auth/authorize"
	AuthCallbackMethod   = "_hermes/auth/callback"
	AuthStatusMethod     = "_hermes/auth/status"
	AuthCancelMethod     = "_hermes/auth/cancel"
	AuthInventoryMethod  = "_hermes/auth/inventory"
	AuthDisconnectMethod = "_hermes/auth/disconnect"
)

const (
	providerAuthCapabilityKey = "providerAuth"
	providerAuthMethodsField  = "methods"

	authFailedErrorTag = "hermes_auth_failed"

	authFieldSessionID          = "sessionId"
	authFieldProviderID         = "providerId"
	authFieldConnectionID       = "connectionId"
	authFieldMethodsGeneration  = "methodsGeneration"
	authFieldMethod             = "method"
	authFieldAuthorizeRequestID = "authorizeRequestId"
	authFieldInputs             = "inputs"
	authFieldFlowID             = "flowId"
	authFieldInput              = "input"
	authFieldBindingGeneration  = "bindingGeneration"
	authFieldParams             = "params"

	authValueInvalid = "invalid"
)

// Closed cause enum carried by a provider-auth leg failure.
const (
	authCauseNativeVeto         = "native_veto"
	authCauseProviderRefused    = "provider_refused"
	authCauseTransport          = "transport"
	authCauseProcess            = "process"
	authCauseTimeout            = "timeout"
	authCauseUnsupportedVariant = "unsupported_variant"
	authCauseFlowExpired        = "flow_expired"
	authCauseFlowState          = "flow_state"
	authCauseFlowCancelled      = "flow_cancelled"
	authCausePolicy             = "policy"
	authCauseBindingConflict    = "binding_conflict"
)

// authMethodNames lists every advertised leg in the order the capability
// reports them.
func authMethodNames() []string {
	return []string{
		AuthMethodsMethod,
		AuthAuthorizeMethod,
		AuthCallbackMethod,
		AuthStatusMethod,
		AuthCancelMethod,
		AuthInventoryMethod,
		AuthDisconnectMethod,
	}
}

// providerAuth is the agent-scoped broker behind the provider-auth legs. It
// owns the current method catalog, the per-session flow records, and the
// durable values-free ledger.
//
// Every leg here runs concurrently with every other one. The ACP connection
// dispatches each inbound request on its own goroutine and cancels that
// request's context when the handler returns; only notifications are
// serialized. So two legs addressing the same flow, the same session, or the
// same native provider are ordinary, and every read state → native
// call → write state sequence on this surface is a check-then-set whose window
// is the whole native call. Nothing in the field-level locking below closes
// that window: each individual access is already guarded, so the outcome is a
// lost update rather than a data race, and the race detector reports nothing.
// The four admission primitives in auth_admission.go are what make those
// sequences indivisible — session admission, the per-(session, provider)
// authorize gate, the per-flow claim, and the provider and ledger gates.
type providerAuth struct {
	agent  *Agent
	ledger *authLedger

	mu         sync.Mutex
	generation string
	catalog    map[string][]authCatalogMethod
	flows      map[authFlowKey]*authFlow
	byID       map[string]*authFlow
	// retained holds the newest flow per key whatever its state, so a repeated
	// idempotency key is answerable for as long as the session lives.
	retained map[authFlowKey]*authFlow
	// retired holds the idempotency keys a later authorize replaced, per key.
	retired map[authFlowKey]map[string]struct{}
	// closedSessions is the tombstone publication checks against.
	closedSessions map[acp.SessionId]struct{}
	// admissions gates authorize per (session, provider); providers gates native
	// provider mutations; ledgers gates each provider's durable lineage record.
	admissions map[authFlowKey]*authGate
	providers  map[string]*authGate
	ledgers    map[string]*authGate
}

type authFlowKey struct {
	sessionID  acp.SessionId
	providerID string
}

// newProviderAuth builds the broker when a usable durable ledger root is
// configured. A root that was asked for and could not be prepared leaves the
// surface unadvertised, exactly as an unset one does: a leg that cannot record
// what it did must not be offered.
func newProviderAuth(agent *Agent) *providerAuth {
	if !authLedgerRootConfigured(agent.options) {
		return nil
	}

	home, err := prepareProviderAuthResidence(providerAuthResidence(agent.options))
	if err != nil {
		agent.log.WarnContext(context.Background(), "provider auth surface is unavailable", slog.String(jsonFieldError, err.Error()))

		return nil
	}

	agent.options.SharedHermesHome = home

	ledger, err := newAuthLedger(agent.options)
	if err != nil {
		agent.log.WarnContext(context.Background(), "provider auth surface is unavailable", slog.String(jsonFieldError, err.Error()))

		return nil
	}

	return &providerAuth{
		agent:          agent,
		ledger:         ledger,
		flows:          make(map[authFlowKey]*authFlow),
		byID:           make(map[string]*authFlow),
		retained:       make(map[authFlowKey]*authFlow),
		retired:        make(map[authFlowKey]map[string]struct{}),
		closedSessions: make(map[acp.SessionId]struct{}),
		admissions:     make(map[authFlowKey]*authGate),
		providers:      make(map[string]*authGate),
		ledgers:        make(map[string]*authGate),
	}
}

// capability reports the enabled leg names. The array is the host's only
// discovery surface for which legs exist.
func (p *providerAuth) capability() map[string]any {
	return map[string]any{
		providerAuthMethodsField: authMethodNames(),
	}
}

func (a *Agent) handleAuthExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, bool, error) {
	broker := a.providerAuth
	if broker == nil {
		return nil, false, nil
	}

	switch method {
	case AuthMethodsMethod:
		result, err := broker.methods(ctx, params)

		return result, true, err
	case AuthAuthorizeMethod:
		result, err := broker.authorize(ctx, params)

		return result, true, err
	case AuthCallbackMethod:
		result, err := broker.callback(ctx, params)

		return result, true, err
	case AuthStatusMethod:
		result, err := broker.status(ctx, params)

		return result, true, err
	case AuthCancelMethod:
		result, err := broker.cancel(ctx, params)

		return result, true, err
	case AuthInventoryMethod:
		result, err := broker.inventory(ctx, params)

		return result, true, err
	case AuthDisconnectMethod:
		result, err := broker.disconnect(ctx, params)

		return result, true, err
	default:
		return nil, false, nil
	}
}

// authFailedError is the uniform provider-auth leg failure. Native message
// text, native response bodies, and child stderr never reach it: every failure
// becomes this closed shape.
type authFailedError struct {
	cause      string
	providerID string
	method     string
	flowID     string
}

func (f *authFailedError) Error() string {
	return authFailedErrorTag + ": " + f.cause
}

func (f *authFailedError) requestError() *acp.RequestError {
	data := map[string]any{
		jsonFieldError: authFailedErrorTag,
		jsonFieldCause: f.cause,
		"retryable":    authCauseRetryable(f.cause),
	}
	if f.providerID != "" {
		data[authFieldProviderID] = f.providerID
	}

	if f.method != "" {
		data[authFieldMethod] = f.method
	}

	if f.flowID != "" {
		data[authFieldFlowID] = f.flowID
	}

	return acp.NewAuthRequired(data)
}

// authCauseRetryable reports whether the same call could succeed unchanged. The
// three transport-shaped causes can; a refusal, a veto, and every flow-state
// answer cannot, because repeating them changes nothing.
func authCauseRetryable(cause string) bool {
	switch cause {
	case authCauseTransport, authCauseProcess, authCauseTimeout:
		return true
	default:
		return false
	}
}

func authFailed(cause string, providerID string, method string, flowID string) error {
	failure := &authFailedError{cause: cause, providerID: providerID, method: method, flowID: flowID}

	return failure.requestError()
}

// authFlowTransition maps a leg cause to the flow transition it must also
// perform. An empty state means the cause carries no transition: a refusal the
// adapter made itself never consumes the owner's authorization.
func authFlowTransition(cause string, materialInFlight bool) (string, string) {
	switch cause {
	case authCauseNativeVeto, authCauseUnsupportedVariant:
		return authStateFailed, authReasonNativeVeto
	case authCauseProviderRefused:
		return authStateFailed, authReasonProviderRefused
	case authCauseTransport:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonTransport
	case authCauseProcess:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonProcess
	case authCauseTimeout:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonTransport
	case authCauseFlowExpired:
		return authStateExpired, authReasonDeadline
	default:
		return "", ""
	}
}

// authNativeCause classifies a native failure without forwarding any of its
// text. Hermes reports a provider refusal with a body that can carry an entire
// upstream response.
func authNativeCause(err error) string {
	// An uncontained browser launch is this adapter's own refusal, not a native
	// answer: nothing was asked of hermes, so repeating the leg on this platform
	// changes nothing and the owner's authorization is untouched.
	if errors.Is(err, nativehermes.ErrBrowserLaunchUncontained) {
		return authCausePolicy
	}

	if nativehermes.AuthRefused(err) {
		return authCauseProviderRefused
	}

	return authCauseTransport
}

// authSession resolves the session a leg addresses. An unknown, unloaded, or
// tombstoned session gets the uniform unknown-session rejection, and so does
// one this broker has already swept: a leg that arrives after close is answered
// cheaply here rather than deep inside a native call.
func (p *providerAuth) authSession(id string) (*session, error) {
	session, err := p.agent.session(acp.SessionId(id))
	if err != nil {
		return nil, err
	}

	if p.sessionClosed(session.id) {
		return nil, unknownSessionError()
	}

	return session, nil
}

// authNativeClient reports the session's live gateway.
func (s *session) authNativeClient() nativehermes.Server {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.client
}

type providerAuthSupporter interface {
	ProviderAuthSupported() bool
}

func nativeProviderAuthSupported(client nativehermes.Server) bool {
	supported, ok := client.(providerAuthSupporter)

	return ok && supported.ProviderAuthSupported()
}

// authParamFields walks a leg's params object once, rejecting an unknown field,
// a duplicate field, and a non-object body with the offending field path. Every
// request object on this surface is closed, and encoding/json alone would let a
// duplicate key silently win.
func authParamFields(raw json.RawMessage, allowed ...string) (map[string]json.RawMessage, error) {
	permitted := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		permitted[name] = struct{}{}
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))

	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, invalidAuthField(authFieldParams)
	}

	fields := make(map[string]json.RawMessage, len(allowed))

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, invalidAuthField(authFieldParams)
		}

		key, _ := keyToken.(string)
		if _, ok := permitted[key]; !ok {
			return nil, unsupportedField(key)
		}

		if _, duplicate := fields[key]; duplicate {
			return nil, unsupportedField(key)
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, invalidAuthField(key)
		}

		fields[key] = value
	}

	if _, err := decoder.Token(); err != nil {
		return nil, invalidAuthField(authFieldParams)
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, invalidAuthField(authFieldParams)
	}

	return fields, nil
}

// authRequiredString decodes a non-empty string field.
func authRequiredString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", invalidAuthField(name)
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", invalidAuthField(name)
	}

	return value, nil
}

// authConnectionIDMaxBytes bounds the caller-minted connection id, matching the
// bound the contract fixes for the whole family. The accepted shape is the
// opaque ASCII token a consumer mints — a short prefix and a UUID is 40 bytes —
// with room to spare.
const authConnectionIDMaxBytes = 128

// authRequiredConnectionID decodes and validates the host-minted connection id
// where it enters the provider lineage.
func authRequiredConnectionID(fields map[string]json.RawMessage) (string, error) {
	value, err := authRequiredString(fields, authFieldConnectionID)
	if err != nil {
		return "", err
	}

	if !authValidConnectionID(value) {
		return "", invalidAuthField(authFieldConnectionID)
	}

	return value, nil
}

// authValidConnectionID reports whether id is an opaque bounded ASCII token.
// Restricting it to that alphabet is what keeps the derived label free of the
// adapter's own prefix, of separators and control characters, and of two wire
// spellings that decode to one Go string and would alias one connection.
func authValidConnectionID(id string) bool {
	if id == "" || len(id) > authConnectionIDMaxBytes {
		return false
	}

	for index := range len(id) {
		if !authConnectionIDByte(id[index]) {
			return false
		}
	}

	return true
}

func authConnectionIDByte(char byte) bool {
	return (char >= 'A' && char <= 'Z') ||
		(char >= 'a' && char <= 'z') ||
		(char >= '0' && char <= '9') ||
		char == '-' || char == '_'
}

// authString decodes a string field that may be empty but must be present.
func authString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", invalidAuthField(name)
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", invalidAuthField(name)
	}

	return value, nil
}

func authRequiredInt64(fields map[string]json.RawMessage, name string) (int64, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, invalidAuthField(name)
	}

	var value int64
	if err := json.Unmarshal(raw, &value); err != nil || value <= 0 {
		return 0, invalidAuthField(name)
	}

	return value, nil
}

func (p *providerAuth) goSafe(name string, fn func()) {
	go func() {
		defer recoverAgentGoroutine(context.Background(), p.agent.log, name)

		fn()
	}()
}

func invalidAuthField(path string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: authValueInvalid,
		jsonFieldField: path,
	})
}
