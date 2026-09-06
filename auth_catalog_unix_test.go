//go:build !windows

package hermesacp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

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

func TestMethodsRejectsAnUnknownParamField(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	_, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID), "extra": 1})
	requireInvalidField(t, err, "extra")
}

// TestProviderAuthLegsLaunchNoNativeProcessOfTheirOwn pins each login leg to
// the native runtime already owned by its addressed session. A leg must not
// create a second gateway with a separate session incarnation.
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
	root := durableTempDir(t)
	home := durableTempDir(t)
	newAgent := func(sessionID acp.SessionId) (*Agent, *fakeHermesClient) {
		client := newFakeHermesClient()
		client.xdg = nativehermes.XDGDirs{Root: home}
		client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode, LoggedIn: true}}
		client.authStart = nativehermes.AuthStart{SessionID: "native-flow", Flow: nativehermes.AuthFlowDeviceCode, URL: "https://example.test/device"}
		agent := newTestAgent(WithProviderAuthRoot(root), WithSharedHermesHome(home))
		if agent.providerAuth == nil {
			t.Fatal("provider auth unavailable")
		}
		session := newSession(agent, sessionID, absTestPath("cwd"), nil, nil, nativehermes.Session{ID: "native"}, client, sessionMeta{}, idmapRecord{})
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
