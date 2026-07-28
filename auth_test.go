package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	testFieldCredential  = "credential"
	testFieldAccessToken = "access_token"
	testFieldMetadata    = "metadata"
	testProviderID       = "xai-oauth"
	testConnectionID     = "connection-1"
	testSessionID        = acp.SessionId("wrapper-session")
)

// newAuthAgent builds an agent with a usable durable ledger root and one
// registered session whose native home is an isolated temp directory.
func newAuthAgent(t *testing.T) (*Agent, *fakeHermesClient) {
	t.Helper()

	home := t.TempDir()
	client := newFakeHermesClient()
	client.xdg = nativehermes.XDGDirs{Root: home}

	agent := NewAgent(WithProviderAuthRoot(t.TempDir()))
	if agent.providerAuth == nil {
		t.Fatal("provider auth surface is unavailable with a usable root")
	}

	session := newSession(agent, testSessionID, "/cwd", nil, nil, nativehermes.Session{ID: "native"}, client, sessionMeta{}, idmapRecord{})
	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	return agent, client
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

func authErrorField(t *testing.T, err error, name string) string {
	t.Helper()

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error is not a request error: %v", err)
	}

	data, ok := requestErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("request error data is not an object: %#v", requestErr.Data)
	}

	value, _ := data[name].(string)
	if value == "" {
		t.Fatalf("error data carries no %s: %#v", name, data)
	}

	return value
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

	if NewAgent().providerAuth != nil {
		t.Fatal("unset root advertised the provider auth surface")
	}

	if NewAgent(WithProviderAuthRoot("relative")).providerAuth != nil {
		t.Fatal("relative root advertised the provider auth surface")
	}

	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	if NewAgent(WithProviderAuthRoot(file)).providerAuth != nil {
		t.Fatal("root that is not a directory advertised the provider auth surface")
	}
}

func TestAuthCapabilityListsEveryLegAndTheInjectionKey(t *testing.T) {
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
	if len(names) != 8 {
		t.Fatalf("advertised %d legs, want 8: %#v", len(names), names)
	}

	if capability[providerAuthInjectionKey] != providerAuthOptionPath {
		t.Fatalf("injectionKey = %v", capability[providerAuthInjectionKey])
	}

	unset, err := NewAgent().Initialize(context.Background(), acp.InitializeRequest{})
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

	bare := NewAgent()

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

	if _, err := authRequiredInt64(fields, "missing"); err == nil {
		t.Fatal("missing int accepted")
	}

	if _, err := authRequiredInt64(fields, "text"); err == nil {
		t.Fatal("non-numeric int accepted")
	}

	if value, err := authRequiredInt64(fields, "number"); err != nil || value != 7 {
		t.Fatalf("authRequiredInt64 = %d, %v", value, err)
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
		{authCauseHarvestFailed, false, authStateFailed, authReasonHarvestFailed},
		{authCauseFlowExpired, false, authStateExpired, authReasonDeadline},
		{authCausePolicy, false, "", ""},
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

	if authNativeCause(&nativehermes.AuthStatusError{StatusCode: 503}) != authCauseTransport {
		t.Fatal("a native 5xx is not transport")
	}

	if authNativeCause(errors.New("dial tcp: connection refused")) != authCauseTransport {
		t.Fatal("a dial failure is not transport")
	}
}

func TestAuthSessionAndHomeResolution(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	if _, err := agent.providerAuth.authSession("missing"); err == nil {
		t.Fatal("unknown session accepted")
	}

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	if session.authHome() != client.xdg.Root {
		t.Fatalf("authHome = %q", session.authHome())
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	if session.authHome() != "" {
		t.Fatal("a session with no live gateway reported a home")
	}
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

func TestProviderAuthOptionKeyIsRejectedWithoutTheSurface(t *testing.T) {
	t.Parallel()

	meta := map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
		metaProviderAuthKey: map[string]any{},
	}}}

	if _, err := NewAgent().sessionMetaFromLifecycle(meta); err == nil {
		t.Fatal("injection key accepted without the provider auth surface")
	} else {
		requireInvalidField(t, err, providerAuthOptionPath)
	}

	agent, _ := newAuthAgent(t)
	if _, err := agent.sessionMetaFromLifecycle(meta); err != nil {
		t.Fatalf("injection key rejected with the surface configured: %v", err)
	}
}

func TestProviderAuthBindingsFromMetaDecodeStrictly(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	valid := map[string]any{testProviderID: map[string]any{
		"connectionId":      testConnectionID,
		"revision":          1,
		"bindingGeneration": 1,
		testFieldCredential: map[string]any{
			"type":        string(ProviderCredentialHermesOAuth),
			"authType":    ProviderAuthTypeOAuth,
			"accessToken": "token",
		},
	}}

	meta, err := agent.sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{
		metaOptionsKey: map[string]any{metaProviderAuthKey: valid},
	}})
	if err != nil {
		t.Fatalf("valid binding rejected: %v", err)
	}

	if !meta.ProviderAuthSupplied || meta.injectionOutcome == nil {
		t.Fatal("supplied binding did not allocate an injection cell")
	}

	if meta.ProviderAuth[testProviderID].ConnectionID != testConnectionID {
		t.Fatalf("decoded binding = %#v", meta.ProviderAuth)
	}

	rejected := []any{
		map[string]any{testProviderID: map[string]any{"connectionId": "", "revision": 1, "bindingGeneration": 1}},
		map[string]any{testProviderID: map[string]any{"connectionId": "c", "revision": 0, "bindingGeneration": 1}},
		map[string]any{testProviderID: map[string]any{"connectionId": "c", "revision": 1, "bindingGeneration": 0}},
		"not-an-object",
		map[string]any{testProviderID: map[string]any{
			"connectionId": "c", "revision": 1, "bindingGeneration": 1,
			testFieldCredential: map[string]any{"type": "oauth"},
		}},
	}

	for index, value := range rejected {
		if _, err := providerAuthBindingsFromMeta(value); err == nil {
			t.Fatalf("case %d accepted an invalid binding", index)
		}
	}

	if _, err := providerAuthBindingsFromMeta(func() {}); err == nil {
		t.Fatal("unencodable binding accepted")
	}
}

func TestInjectProviderAuthIsSkippedWhenNothingWasSupplied(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	agent.injectProviderAuth(client.xdg.Root, sessionMeta{})

	bare := NewAgent()
	outcome := new(string)
	bare.injectProviderAuth(client.xdg.Root, sessionMeta{injectionOutcome: outcome})

	if *outcome != "" {
		t.Fatalf("an agent without the surface recorded %q", *outcome)
	}
}

func TestReinjectActiveSessionRecordsTheTriState(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	meta := sessionMeta{
		ProviderAuth:         map[string]ProviderAuthBinding{testProviderID: testBinding()},
		ProviderAuthSupplied: true,
		injectionOutcome:     new(string),
	}

	agent.reinjectActiveSession(session, meta)

	if meta.injection() != authInjectionApplied {
		t.Fatalf("first injection = %q", meta.injection())
	}

	if session.snapshot().providerAuthInjection != authInjectionApplied {
		t.Fatal("session did not record the injection outcome")
	}

	present, err := nativehermes.AuthSlotPresent(client.xdg.Root, testProviderID, nativehermes.AuthSlotLabel(testConnectionID))
	if err != nil || !present {
		t.Fatalf("reserved slot present = %v, %v", present, err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	second := sessionMeta{ProviderAuth: meta.ProviderAuth, ProviderAuthSupplied: true, injectionOutcome: new(string)}
	agent.reinjectActiveSession(session, second)

	if second.injection() != authInjectionConflict {
		t.Fatalf("injection without a live home = %q", second.injection())
	}

	NewAgent().reinjectActiveSession(session, second)
}

func TestLifecycleResponseMetaOmitsAnInjectionThatNeverRan(t *testing.T) {
	t.Parallel()

	plain := lifecycleResponseMeta(sessionSnapshot{})

	hermesMeta, _ := plain[hermesMetaKey].(map[string]any)
	if _, present := hermesMeta[metaProviderAuthKey]; present {
		t.Fatalf("absent injection reported: %#v", hermesMeta)
	}

	carried := lifecycleResponseMeta(sessionSnapshot{providerAuthInjection: authInjectionNoop})

	carriedMeta, _ := carried[hermesMetaKey].(map[string]any)

	injection, _ := carriedMeta[metaProviderAuthKey].(map[string]any)
	if injection[providerAuthInjectionName] != authInjectionNoop {
		t.Fatalf("injection meta = %#v", carriedMeta)
	}
}

func TestProviderAuthDirectHomeIsRejectedFailClosed(t *testing.T) {
	t.Parallel()

	agent := NewAgent(WithProviderAuthDirectHome(filepath.Join(t.TempDir(), "home")))

	_, err := agent.NewSession(context.Background(), acp.NewSessionRequest{Cwd: "/cwd"})
	if err == nil {
		t.Fatal("a configured exact-home consent path was accepted")
	}

	requireInvalidField(t, err, optionFieldProviderAuthDirectHome)

	relative := NewAgent(WithProviderAuthDirectHome("relative"))
	if _, err := relative.Initialize(context.Background(), acp.InitializeRequest{}); err == nil {
		t.Fatal("a relative exact-home consent path was accepted at construction")
	}

	if err := validateProviderAuthRoots(Options{ProviderAuthRoot: "relative"}); err == nil {
		t.Fatal("a relative provider auth root was accepted at construction")
	}

	if err := validateProviderAuthRoots(Options{}); err != nil {
		t.Fatalf("empty roots rejected: %v", err)
	}
}

func testBinding() ProviderAuthBinding {
	return ProviderAuthBinding{
		ConnectionID:      testConnectionID,
		Revision:          1,
		BindingGeneration: 1,
		Credential: ProviderCredential{
			Type:        ProviderCredentialHermesOAuth,
			HermesOAuth: &ProviderHermesOAuthCredential{AuthType: ProviderAuthTypeOAuth, AccessToken: "token"},
		},
	}
}

func TestAuthParamFieldsRejectsATruncatedObject(t *testing.T) {
	t.Parallel()

	_, err := authParamFields(json.RawMessage(`{"sessionId":"a"`), authFieldSessionID)
	if err == nil {
		t.Fatal("a truncated object was accepted")
	}

	requireInvalidField(t, err, authFieldParams)
}

// Every Hermes session gets a throwaway home, so session/new is the injection
// path: the binding is written into the generation root the server is about to
// be started against, before it reads it.
func TestNewSessionInjectsIntoTheGenerationRoot(t *testing.T) {
	t.Parallel()

	agent := NewAgent(WithScratchDir(t.TempDir()), WithProviderAuthRoot(t.TempDir()))

	var root string

	agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		client := newFakeHermesClient()
		client.xdg = options.ExistingXDG
		client.createSession = nativehermes.Session{ID: "native-1"}
		root = options.ExistingXDG.Root

		return client, nil
	}

	response, err := agent.NewSession(context.Background(), acp.NewSessionRequest{
		Cwd: t.TempDir(),
		Meta: map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
			metaProviderAuthKey: map[string]any{testProviderID: map[string]any{
				"connectionId":      testConnectionID,
				"revision":          1,
				"bindingGeneration": 1,
				testFieldCredential: map[string]any{
					"type":        string(ProviderCredentialHermesOAuth),
					"authType":    ProviderAuthTypeOAuth,
					"accessToken": "token",
				},
			}},
		}}},
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	meta, _ := response.Meta[hermesMetaKey].(map[string]any)

	injection, _ := meta[metaProviderAuthKey].(map[string]any)
	if injection[providerAuthInjectionName] != authInjectionApplied {
		t.Fatalf("injection = %#v", meta)
	}

	present, err := nativehermes.AuthSlotPresent(root, testProviderID, nativehermes.AuthSlotLabel(testConnectionID))
	if err != nil || !present {
		t.Fatalf("reserved slot in the generation root = %v, %v", present, err)
	}
}

func TestInjectProviderAuthRecordsTheOutcome(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	meta := sessionMeta{}.withProviderAuth(map[string]ProviderAuthBinding{testProviderID: testBinding()})
	agent.injectProviderAuth(client.xdg.Root, meta)

	if meta.injection() != authInjectionApplied {
		t.Fatalf("injection outcome = %q", meta.injection())
	}

	if empty := (sessionMeta{}).withProviderAuth(nil); empty.injectionOutcome != nil {
		t.Fatal("absent bindings allocated an injection cell")
	}
}

func TestLifecycleMetaRejectsAnInvalidBinding(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	_, err := agent.sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{
		metaOptionsKey: map[string]any{metaProviderAuthKey: map[string]any{
			testProviderID: map[string]any{"connectionId": "", "revision": 1, "bindingGeneration": 1},
		}},
	}})
	if err == nil {
		t.Fatal("an invalid binding was accepted at session start")
	}

	requireInvalidField(t, err, providerAuthOptionPath)
}
