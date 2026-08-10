package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	testFieldCredential   = "credential"
	testFieldAccessToken  = "access_token"
	testFieldRefreshToken = "refresh_token"
	testFieldMetadata     = "metadata"
	testProviderID        = "xai-oauth"
	testConnectionID      = "connection-1"
	testSessionID         = acp.SessionId("wrapper-session")
)

// newAuthAgent builds an agent with a usable durable ledger root and one
// registered session whose native home is an isolated temp directory. The same
// fake gateway backs the broker every leg speaks to, because the broker is a
// native launch like any other.
func newAuthAgent(t *testing.T) (*Agent, *fakeHermesClient) {
	t.Helper()

	home := t.TempDir()
	client := newFakeHermesClient()
	client.xdg = nativehermes.XDGDirs{Root: home}

	agent := newTestAgent(
		WithProviderAuthRoot(t.TempDir()),
		WithProviderAuthHome(t.TempDir()),
		WithScratchDir(t.TempDir()),
		withTestClientFactory(client),
	)
	if agent.providerAuth == nil {
		t.Fatal("provider auth surface is unavailable with a usable root")
	}

	t.Cleanup(func() { _ = agent.providerAuth.closeBroker(context.Background()) })

	session := newSession(agent, testSessionID, "/cwd", nil, nil, nativehermes.Session{ID: "native"}, client, sessionMeta{}, idmapRecord{})
	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	return agent, client
}

// stopAuthBroker is how a test says "this agent has no native gateway left":
// the broker refuses every later borrow once it is closed, which is the state a
// leg meets when the harness is gone.
func stopAuthBroker(t *testing.T, agent *Agent) {
	t.Helper()

	if err := agent.providerAuth.closeBroker(context.Background()); err != nil {
		t.Fatalf("close provider auth broker: %v", err)
	}
}

// mustType asserts a leg result's concrete type without discarding the failure.
func mustType[T any](t *testing.T, value any) T {
	t.Helper()

	typed, ok := value.(T)
	if !ok {
		t.Fatalf("result is %T, want %T", value, typed)
	}

	return typed
}

func callLeg(t *testing.T, agent *Agent, method string, params map[string]any) (any, error) {
	t.Helper()

	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	return agent.HandleExtensionMethod(context.Background(), method, encoded)
}

func requireAuthCause(t *testing.T, err error, cause string) {
	t.Helper()

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error is not a request error: %v", err)
	}

	data, ok := requestErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("request error data is not an object: %#v", requestErr.Data)
	}

	if data[jsonFieldError] != authFailedErrorTag {
		t.Fatalf("error tag = %v, want %s", data[jsonFieldError], authFailedErrorTag)
	}

	if data[jsonFieldCause] != cause {
		t.Fatalf("cause = %v, want %s", data[jsonFieldCause], cause)
	}
}

func requireInvalidField(t *testing.T, err error, field string) {
	t.Helper()

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error is not a request error: %v", err)
	}

	data, ok := requestErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("request error data is not an object: %#v", requestErr.Data)
	}

	if data[keyField] != field {
		t.Fatalf("field = %v, want %s", data[keyField], field)
	}
}

func TestAuthSurfaceIsUnadvertisedWithoutAUsableRoot(t *testing.T) {
	t.Parallel()

	if newTestAgent().providerAuth != nil {
		t.Fatal("unset root advertised the provider auth surface")
	}

	if newTestAgent(WithProviderAuthRoot("relative"), WithProviderAuthHome(t.TempDir())).providerAuth != nil {
		t.Fatal("relative root advertised the provider auth surface")
	}

	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	if newTestAgent(WithProviderAuthRoot(file), WithProviderAuthHome(t.TempDir())).providerAuth != nil {
		t.Fatal("root that is not a directory advertised the provider auth surface")
	}

	if newTestAgent(WithProviderAuthRoot(t.TempDir())).providerAuth != nil {
		t.Fatal("ledger without native auth home advertised provider auth")
	}

	if newTestAgent(WithProviderAuthHome(t.TempDir())).providerAuth != nil {
		t.Fatal("native auth home without ledger advertised provider auth")
	}
}

func TestAuthCapabilityListsEveryLeg(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	response, err := agent.Initialize(context.Background(), acp.InitializeRequest{})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	hermesMeta, _ := response.AgentCapabilities.Meta[hermesMetaKey].(map[string]any)

	capability, ok := hermesMeta[providerAuthCapabilityKey].(map[string]any)
	if !ok {
		t.Fatalf("capability missing providerAuth: %#v", hermesMeta)
	}

	names, _ := capability[providerAuthMethodsField].([]string)
	if len(names) != 6 {
		t.Fatalf("advertised %d legs, want 6: %#v", len(names), names)
	}

	unset, err := newTestAgent().Initialize(context.Background(), acp.InitializeRequest{})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	unsetMeta, _ := unset.AgentCapabilities.Meta[hermesMetaKey].(map[string]any)
	if _, present := unsetMeta[providerAuthCapabilityKey]; present {
		t.Fatal("unset root still advertised providerAuth")
	}
}

func TestAuthLegsAnswerOnlyWhileAdvertised(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	for _, method := range authMethodNames() {
		if _, err := callLeg(t, agent, method, map[string]any{}); err == nil {
			t.Fatalf("%s accepted empty params", method)
		}
	}

	bare := newTestAgent()

	for _, method := range authMethodNames() {
		_, err := callLeg(t, bare, method, map[string]any{"sessionId": "x"})

		var requestErr *acp.RequestError
		if !errors.As(err, &requestErr) || requestErr.Code != -32601 {
			t.Fatalf("%s without a root returned %v, want method-not-found", method, err)
		}
	}

	if _, err := callLeg(t, agent, "_hermes/auth/unknown", map[string]any{}); err == nil {
		t.Fatal("unknown auth-shaped method accepted")
	}
}

func TestAuthParamFieldsRejectsMalformedClosedObjects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		raw   string
		field string
	}{
		{"non object", `[]`, authFieldParams},
		{"unknown field", `{"sessionId":"a","extra":1}`, "extra"},
		{"duplicate field", `{"sessionId":"a","sessionId":"b"}`, "sessionId"},
		{"unterminated", `{"sessionId":`, "sessionId"},
		{"trailing content", `{"sessionId":"a"} 1`, authFieldParams},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := authParamFields(json.RawMessage(tt.raw), authFieldSessionID)
			if err == nil {
				t.Fatal("malformed params accepted")
			}

			requireInvalidField(t, err, tt.field)
		})
	}

	if _, err := authParamFields(json.RawMessage(`{"sessionId":1x}`), authFieldSessionID); err == nil {
		t.Fatal("invalid token accepted")
	}
}

func TestAuthFieldDecodersRejectMissingAndMalformedValues(t *testing.T) {
	t.Parallel()

	fields := map[string]json.RawMessage{
		"empty":  json.RawMessage(`""`),
		"number": json.RawMessage(`7`),
		"text":   json.RawMessage(`"value"`),
	}

	if _, err := authRequiredString(fields, "missing"); err == nil {
		t.Fatal("missing required string accepted")
	}

	if _, err := authRequiredString(fields, "empty"); err == nil {
		t.Fatal("empty required string accepted")
	}

	if value, err := authRequiredString(fields, "text"); err != nil || value != "value" {
		t.Fatalf("authRequiredString = %q, %v", value, err)
	}

	if _, err := authString(fields, "missing"); err == nil {
		t.Fatal("missing string accepted")
	}

	if _, err := authString(fields, "number"); err == nil {
		t.Fatal("non-string accepted")
	}

	if value, err := authString(fields, "empty"); err != nil || value != "" {
		t.Fatalf("authString = %q, %v", value, err)
	}
}

func TestAuthFailedErrorCarriesTheClosedShapeOnly(t *testing.T) {
	t.Parallel()

	failure := &authFailedError{cause: authCauseTransport, providerID: "p", method: "m", flowID: "f"}
	if failure.Error() != authFailedErrorTag+": "+authCauseTransport {
		t.Fatalf("Error() = %q", failure.Error())
	}

	data, _ := failure.requestError().Data.(map[string]any)
	if data["retryable"] != true || data[authFieldProviderID] != "p" || data[authFieldMethod] != "m" || data[authFieldFlowID] != "f" {
		t.Fatalf("request error data = %#v", data)
	}

	bare, _ := (&authFailedError{cause: authCausePolicy}).requestError().Data.(map[string]any)
	if _, present := bare[authFieldProviderID]; present {
		t.Fatalf("bare failure carried a provider id: %#v", bare)
	}

	if bare["retryable"] != false {
		t.Fatalf("policy refusal reported retryable: %#v", bare)
	}

	for _, cause := range []string{authCauseTransport, authCauseProcess, authCauseTimeout} {
		if !authCauseRetryable(cause) {
			t.Fatalf("%s is not retryable", cause)
		}
	}

	for _, cause := range []string{
		authCauseNativeVeto, authCauseProviderRefused,
		authCauseUnsupportedVariant, authCauseFlowExpired, authCauseFlowState,
		authCauseFlowCancelled, authCausePolicy, authCauseBindingConflict,
	} {
		if authCauseRetryable(cause) {
			t.Fatalf("%s reported retryable", cause)
		}
	}
}

func TestAuthFlowTransitionIsTotalOverTheCauseEnum(t *testing.T) {
	t.Parallel()

	cases := []struct {
		cause    string
		inFlight bool
		state    string
		reason   string
	}{
		{authCauseNativeVeto, false, authStateFailed, authReasonNativeVeto},
		{authCauseUnsupportedVariant, false, authStateFailed, authReasonNativeVeto},
		{authCauseProviderRefused, false, authStateFailed, authReasonProviderRefused},
		{authCauseTransport, false, authStateFailed, authReasonTransport},
		{authCauseTransport, true, authStateFailed, authReasonAcceptanceUnknown},
		{authCauseProcess, false, authStateFailed, authReasonProcess},
		{authCauseProcess, true, authStateFailed, authReasonAcceptanceUnknown},
		{authCauseTimeout, false, authStateFailed, authReasonTransport},
		{authCauseTimeout, true, authStateFailed, authReasonAcceptanceUnknown},
		{authCauseFlowExpired, false, authStateExpired, authReasonDeadline},
		{authCausePolicy, false, "", ""},
		{authCauseBindingConflict, false, "", ""},
		{authCauseFlowState, false, "", ""},
		{authCauseFlowCancelled, false, "", ""},
	}

	for _, tt := range cases {
		state, reason := authFlowTransition(tt.cause, tt.inFlight)
		if state != tt.state || reason != tt.reason {
			t.Fatalf("%s(inFlight=%v) = %q/%q, want %q/%q", tt.cause, tt.inFlight, state, reason, tt.state, tt.reason)
		}
	}
}

func TestAuthNativeCauseNeverForwardsNativeText(t *testing.T) {
	t.Parallel()

	refusal := &nativehermes.AuthStatusError{StatusCode: 400}
	if authNativeCause(refusal) != authCauseProviderRefused {
		t.Fatal("a native 4xx is not a provider refusal")
	}

	if authNativeCause(nativehermes.ErrBrowserLaunchUncontained) != authCausePolicy {
		t.Fatal("an uncontained browser launch was classified as a native answer")
	}

	if authNativeCause(&nativehermes.AuthStatusError{StatusCode: 503}) != authCauseTransport {
		t.Fatal("a native 5xx is not transport")
	}

	if authNativeCause(errors.New("dial tcp: connection refused")) != authCauseTransport {
		t.Fatal("a dial failure is not transport")
	}
}

func TestAuthSessionResolution(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	if _, err := agent.providerAuth.authSession("missing"); err == nil {
		t.Fatal("unknown session accepted")
	}

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()
}

func TestAuthGoSafeContainsAPanic(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	done := make(chan struct{})

	agent.providerAuth.goSafe("panicking", func() {
		defer close(done)

		panic("boom")
	})

	<-done
}

func TestAuthParamFieldsRejectsATruncatedObject(t *testing.T) {
	t.Parallel()

	_, err := authParamFields(json.RawMessage(`{"sessionId":"a"`), authFieldSessionID)
	if err == nil {
		t.Fatal("a truncated object was accepted")
	}

	requireInvalidField(t, err, authFieldParams)
}

// adversarialConnectionIDs are caller-minted values that must never enter
// durable lineage. The two replacement-rune spellings are one Go string reached
// from different wire encodings and would alias one connection.
func adversarialConnectionIDs() map[string]string {
	return map[string]string{
		"empty":              "",
		"carries the prefix": "acp-go-hermes:connection-1",
		"path separators":    "../../../etc/passwd",
		"windows separators": `..\..\connection`,
		"newline":            "connection\n1",
		"nul":                "connection\x00 1",
		"bidi override":      "connection\u202e1",
		"space":              "connection 1",
		"replacement rune":   "connection-�",
		"non ascii":          "connection-é",
		"unbounded":          strings.Repeat("c", authConnectionIDMaxBytes+1),
	}
}

func TestAuthorizeRejectsInvalidConnectionIDs(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	for name, connectionID := range adversarialConnectionIDs() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := callLeg(t, agent, AuthAuthorizeMethod, map[string]any{
				"sessionId": string(testSessionID), "providerId": testProviderID,
				"connectionId": connectionID, "methodsGeneration": "generation",
				"method": "device_code", "authorizeRequestId": "request-1",
			})
			requireInvalidField(t, err, authFieldConnectionID)
		})
	}
}

func TestConnectionIDAcceptsTheOpaqueTokenAConsumerMints(t *testing.T) {
	t.Parallel()

	accepted := []string{
		"pac_2f1c9b4e-8d3a-4c17-9f21-0b6e5a7c8d90",
		"pac_conformance",
		testConnectionID,
		"C0",
		strings.Repeat("c", authConnectionIDMaxBytes),
	}

	for _, connectionID := range accepted {
		if !authValidConnectionID(connectionID) {
			t.Fatalf("connection id %q was refused", connectionID)
		}
	}
}
