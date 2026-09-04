package hermesacp

import (
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	testDeviceURL = "https://accounts.x.ai/oauth2/device?user_code=ABCD-EFGH"
	testPKCEURL   = "https://claude.ai/oauth/authorize?code_challenge=abc"
)

func seedCatalog(t *testing.T, agent *Agent, client *fakeHermesClient) string {
	t.Helper()

	client.authProviders = []nativehermes.AuthProvider{
		{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode},
		{ID: "anthropic", Name: "Anthropic", Flow: nativehermes.AuthFlowPKCE},
	}

	result, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("methods: %v", err)
	}

	return mustType[authMethodsResult](t, result).Generation
}

func authorizeParams(generation string, providerID string, method string, requestID string) map[string]any {
	return map[string]any{
		"sessionId":          string(testSessionID),
		"providerId":         providerID,
		"connectionId":       testConnectionID,
		"methodsGeneration":  generation,
		"method":             method,
		"authorizeRequestId": requestID,
	}
}

func startDeviceFlow(t *testing.T, agent *Agent, client *fakeHermesClient) authAuthorizeResult {
	t.Helper()

	generation := seedCatalog(t, agent, client)
	client.authStart = nativehermes.AuthStart{
		SessionID:    "native-device",
		Flow:         nativehermes.AuthFlowDeviceCode,
		URL:          testDeviceURL,
		UserCode:     "ABCD-EFGH",
		PollInterval: 3 * time.Second,
		ExpiresIn:    30 * time.Minute,
	}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
		generation,
		testProviderID,
		nativehermes.AuthFlowDeviceCode,
		"request-1",
	))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	return mustType[authAuthorizeResult](t, result)
}

func startPKCEFlow(t *testing.T, agent *Agent, client *fakeHermesClient) authAuthorizeResult {
	t.Helper()

	generation := seedCatalog(t, agent, client)
	client.authStart = nativehermes.AuthStart{
		SessionID: "native-pkce",
		Flow:      nativehermes.AuthFlowPKCE,
		URL:       testPKCEURL,
		ExpiresIn: 5 * time.Minute,
	}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
		generation,
		"anthropic",
		nativehermes.AuthFlowPKCE,
		"request-pkce",
	))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	return mustType[authAuthorizeResult](t, result)
}
