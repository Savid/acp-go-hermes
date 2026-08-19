package hermesacp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func TestMethodsEnumeratesOnlyNativeOAuthCatalog(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	client.authProviders = []nativehermes.AuthProvider{
		{ID: "xai-oauth", Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode},
		{ID: "anthropic", Name: "Anthropic API Key", Flow: nativehermes.AuthFlowPKCE},
		{ID: "claude-code", Name: "Anthropic OAuth: Required Extra Usage Credits to Use Subscription", Flow: "external"},
		{ID: "", Name: "nameless", Flow: nativehermes.AuthFlowDeviceCode},
		{ID: "unlabelled", Name: "bad\u202Elabel", Flow: nativehermes.AuthFlowDeviceCode},
	}
	result, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("methods: %v", err)
	}

	catalog := mustType[authMethodsResult](t, result)
	if catalog.Generation == "" {
		t.Fatal("methods returned no generation")
	}

	if _, present := catalog.Providers["claude-code"]; present {
		t.Fatal("an external-flow entry was offered as a login")
	}

	if _, present := catalog.Providers["unlabelled"]; present {
		t.Fatal("an entry whose label violates its bound was published")
	}

	xai := catalog.Providers["xai-oauth"]
	if len(xai) != 1 || xai[0].ID != nativehermes.AuthFlowDeviceCode {
		t.Fatalf("xai methods = %#v", xai)
	}

	if xai[0].Type != authMethodTypeOAuth {
		t.Fatalf("xai method types = %#v", xai)
	}

	anthropic := catalog.Providers["anthropic"]
	if len(anthropic) != 1 || anthropic[0].ID != nativehermes.AuthFlowPKCE {
		t.Fatalf("anthropic methods = %#v", anthropic)
	}

	if anthropic[0].Label != "Anthropic API Key" {
		t.Fatalf("native label was rewritten: %q", anthropic[0].Label)
	}
}

func TestMethodsPublishesNoBrokerLoginForOfficialHermesWithoutDurableAuthHome(t *testing.T) {
	agent, client := newAuthAgent(t)
	unsupported := false
	client.providerAuthSupported = &unsupported
	client.authProvidersErr = errors.New("native catalog must not be consulted")

	result, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("methods: %v", err)
	}

	catalog := mustType[authMethodsResult](t, result)
	if catalog.Generation == "" || len(catalog.Providers) != 0 {
		t.Fatalf("official Hermes catalog = %#v", catalog)
	}

	_, err = callLeg(t, agent, AuthAuthorizeMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "xai-oauth",
		"connectionId": testConnectionID, "methodsGeneration": catalog.Generation,
		"method": nativehermes.AuthFlowDeviceCode, "authorizeRequestId": "request-1",
		"inputs": map[string]string{},
	})
	requireInvalidField(t, err, authFieldMethod)
}

func TestMethodsFailurePaths(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	if _, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": "unknown"}); err == nil {
		t.Fatal("unknown session accepted")
	}

	if _, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{}); err == nil {
		t.Fatal("missing session id accepted")
	}

	client.authProvidersErr = &nativehermes.AuthStatusError{StatusCode: 403}

	_, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseProviderRefused)

	client.authProvidersErr = nil

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	_, err = callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseTransport)
}

func TestMethodsFailsClosedWhenNoGenerationCanBeMinted(t *testing.T) {
	agent, client := newAuthAgent(t)

	original := authRandRead
	authRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }

	t.Cleanup(func() { authRandRead = original })

	_, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseProcess)

	unsupported := false
	client.providerAuthSupported = &unsupported
	_, err = callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseProcess)
}

func TestAuthDisplayTextNormalisesBeforeItMeasures(t *testing.T) {
	t.Parallel()

	value, ok := authDisplayText("Café", authMaxLabelBytes)
	if !ok || value != "Café" {
		t.Fatalf("authDisplayText = %q, %v", value, ok)
	}

	if len(value) != 5 {
		t.Fatalf("bounds were measured before normalisation: %d bytes", len(value))
	}

	rejected := []string{"", strings.Repeat("a", authMaxLabelBytes+1), "line\nbreak", "bidi\u202Eoverride", "\x00null"}
	for _, input := range rejected {
		if _, ok := authDisplayText(input, authMaxLabelBytes); ok {
			t.Fatalf("accepted %q", input)
		}
	}

	if _, ok := authDisplayText("\xff\xfe", authMaxLabelBytes); ok {
		t.Fatal("accepted invalid utf-8")
	}

	for _, input := range []string{"Provider", "Provider 1", "Provider-1", "Provider ©", "Provider Name"} {
		if _, ok := authDisplayText(input, authMaxLabelBytes); !ok {
			t.Fatalf("rejected %q", input)
		}
	}
}

func TestAuthDisplayURLAndUserCodeBounds(t *testing.T) {
	t.Parallel()

	if value, ok := authDisplayURL("https://accounts.x.ai/oauth2/device?user_code=ABCD"); !ok || value == "" {
		t.Fatalf("authDisplayURL = %q, %v", value, ok)
	}

	rejected := []string{
		"",
		"http://accounts.x.ai/device",
		"https://user:pass@accounts.x.ai/device",
		"https://accounts.x.ai/device#fragment",
		"https:///device",
		"https://accounts.x.ai/" + strings.Repeat("a", authMaxURLBytes),
		"https://accounts.x.ai/%zz",
	}

	for _, input := range rejected {
		if _, ok := authDisplayURL(input); ok {
			t.Fatalf("accepted url %q", input)
		}
	}

	if value, ok := authDisplayUserCode("ABCD-EFGH"); !ok || value != "ABCD-EFGH" {
		t.Fatalf("authDisplayUserCode = %q, %v", value, ok)
	}

	for _, input := range []string{"", "</script>ABCD", "AB CD", strings.Repeat("A", authMaxUserCodeBytes+1)} {
		if _, ok := authDisplayUserCode(input); ok {
			t.Fatalf("accepted user code %q", input)
		}
	}
}

func TestAuthLoopbackHostDetection(t *testing.T) {
	t.Parallel()

	loopback := []string{
		"https://127.0.0.1/callback",
		"https://[::1]/callback",
		"https://localhost/callback",
		"https://app.localhost/callback",
		"https://provider.example/authorize?redirect_uri=http%3A%2F%2F127.0.0.1%3A1455%2Fcb",
	}

	for _, input := range loopback {
		if !authLoopbackHost(input) {
			t.Fatalf("missed loopback in %q", input)
		}
	}

	remote := []string{
		"https://provider.example/authorize",
		"https://provider.example/authorize?redirect_uri=https%3A%2F%2Fprovider.example%2Fcb",
		"://bad",
		"https://provider.example/authorize?redirect_uri=%3A%3Abad",
	}

	for _, input := range remote {
		if authLoopbackHost(input) {
			t.Fatalf("false loopback for %q", input)
		}
	}
}

func TestValidateAuthInputsRejectsEveryAnswerBecauseNoPromptIsPublished(t *testing.T) {
	t.Parallel()

	if err := validateAuthInputs(nil); err != nil {
		t.Fatalf("absent inputs rejected: %v", err)
	}

	if err := validateAuthInputs(map[string]string{}); err != nil {
		t.Fatalf("empty inputs rejected: %v", err)
	}

	err := validateAuthInputs(map[string]string{"instanceUrl": "https://gitlab.example"})
	if err == nil {
		t.Fatal("an unpublished prompt answer was accepted")
	}

	requireInvalidField(t, err, authFieldInputs)
}

func TestBuildAuthCatalogSortsProvidersDeterministically(t *testing.T) {
	t.Parallel()

	_, entries := buildAuthCatalog(
		[]nativehermes.AuthProvider{
			{ID: "zeta", Name: "Zeta", Flow: nativehermes.AuthFlowDeviceCode},
			{ID: "alpha", Name: "Alpha", Flow: nativehermes.AuthFlowDeviceCode},
		},
	)

	if len(entries) != 2 {
		t.Fatalf("entries = %#v", entries)
	}

	if entries["alpha"][0].Label != "Alpha" || entries["zeta"][0].Label != "Zeta" {
		t.Fatalf("entries = %#v", entries)
	}
}

func TestMethodsRejectsAnUnknownParamField(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	_, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID), "extra": 1})
	requireInvalidField(t, err, "extra")
}

// TestProviderAuthLegsLaunchNoNativeProcessOfTheirOwn pins where a leg's
// gateway comes from. An explicit process-isolation policy admits exactly one
// live native process per standalone agent identity — the claim proves the
// identity vacant across every task in the PID namespace — so a leg that starts
// a second harness beside the session it is fenced by cannot claim that
// identity at all while the session holds it. It waits out the whole claim
// budget and then answers with the closed transport cause, which is a login
// surface that never works under the very policy it exists to protect. Every
// leg therefore answers on the runtime its addressed session already owns.
func TestProviderAuthLegsLaunchNoNativeProcessOfTheirOwn(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	launches := 0
	agent.options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
		launches++

		return nil, errors.New("a provider-auth leg launched a native process")
	}

	client.authProviders = []nativehermes.AuthProvider{
		{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode},
	}
	client.authStart = nativehermes.AuthStart{SessionID: "native-flow", Flow: nativehermes.AuthFlowDeviceCode, URL: "https://example.test/device"}

	catalog, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("methods: %v", err)
	}

	methods := mustType[authMethodsResult](t, catalog)

	authorized, err := callLeg(t, agent, AuthAuthorizeMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "methodsGeneration": methods.Generation,
		"method": nativehermes.AuthFlowDeviceCode, "authorizeRequestId": "leg-runtime",
	})
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	flow := mustType[authAuthorizeResult](t, authorized)

	if _, err = callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flow.FlowID,
	}); err != nil {
		t.Fatalf("status: %v", err)
	}

	if _, err = callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)}); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if _, err = callLeg(t, agent, AuthCancelMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flow.FlowID,
	}); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if launches != 0 {
		t.Fatalf("provider-auth legs performed %d native launches, want none", launches)
	}
}

func TestCrossAgentPendingProviderFlowDoesNotBlockInventory(t *testing.T) {
	root := t.TempDir()
	home := t.TempDir()
	newAgent := func(sessionID acp.SessionId) (*Agent, *fakeHermesClient) {
		client := newFakeHermesClient()
		client.xdg = nativehermes.XDGDirs{Root: home}
		client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode, LoggedIn: true}}
		client.authStart = nativehermes.AuthStart{SessionID: "native-flow", Flow: nativehermes.AuthFlowDeviceCode, URL: "https://example.test/device"}
		agent := newTestAgent(WithProviderAuthRoot(root), WithSharedHermesHome(home))
		if agent.providerAuth == nil {
			t.Fatal("provider auth unavailable")
		}
		session := newSession(agent, sessionID, "/cwd", nil, nil, nativehermes.Session{ID: "native"}, client, sessionMeta{}, idmapRecord{})
		if err := agent.storeStartedSession(session); err != nil {
			t.Fatal(err)
		}

		return agent, client
	}
	agentA, _ := newAgent("agent-a-session")
	agentB, _ := newAgent("agent-b-session")

	catalog, err := callLeg(t, agentA, AuthMethodsMethod, map[string]any{"sessionId": "agent-a-session"})
	if err != nil {
		t.Fatal(err)
	}
	methods := mustType[authMethodsResult](t, catalog)
	authorized, err := callLeg(t, agentA, AuthAuthorizeMethod, map[string]any{
		"sessionId": "agent-a-session", "providerId": testProviderID,
		"connectionId": testConnectionID, "methodsGeneration": methods.Generation,
		"method": nativehermes.AuthFlowDeviceCode, "authorizeRequestId": "cross-agent-pending",
	})
	if err != nil {
		t.Fatal(err)
	}
	flow := mustType[authAuthorizeResult](t, authorized)

	started := time.Now()
	result, err := callLeg(t, agentB, AuthInventoryMethod, map[string]any{"sessionId": "agent-b-session"})
	if err != nil {
		t.Fatalf("inventory behind pending cross-agent flow: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("inventory blocked for %s", elapsed)
	}
	if entries := mustType[authInventoryResult](t, result).Entries; len(entries) != 0 {
		t.Fatalf("pending provider was reported as proven: %#v", entries)
	}
	if _, err := callLeg(t, agentA, AuthCancelMethod, map[string]any{
		"sessionId": "agent-a-session", "providerId": testProviderID, "flowId": flow.FlowID,
	}); err != nil {
		t.Fatal(err)
	}
}
