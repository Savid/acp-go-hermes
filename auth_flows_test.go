package hermesacp

import (
	"context"
	"errors"
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

func TestAuthorizePublishesNativeOAuthPresentations(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	device := startDeviceFlow(t, agent, client)

	if device.Interaction != authInteractionWait ||
		device.URL != testDeviceURL ||
		device.UserCode != "ABCD-EFGH" ||
		device.CallbackInput != "" ||
		device.PollIntervalMs != 3000 ||
		device.FlowExpiresAt == 0 {
		t.Fatalf("device presentation = %#v", device)
	}

	record, ok, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil || !ok || record.State != authLedgerIntent || record.FlowID != device.FlowID {
		t.Fatalf("device lineage = %#v, %v, %v", record, ok, err)
	}

	pkce := startPKCEFlow(t, agent, client)
	if pkce.Interaction != authInteractionCallback ||
		pkce.CallbackInput != authCallbackInputCode ||
		pkce.URL != testPKCEURL ||
		pkce.UserCode != "" ||
		pkce.PollIntervalMs != 0 {
		t.Fatalf("PKCE presentation = %#v", pkce)
	}
}

func TestOAuthCompletionTrustsNativeTerminalStateAndConfirmsLineage(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startPKCEFlow(t, agent, client)
	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollApproved}

	result, err := callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": "anthropic",
		"method":     nativehermes.AuthFlowPKCE,
		"flowId":     presentation.FlowID,
		"input":      "code#state",
	})
	if err != nil {
		t.Fatalf("callback: %v", err)
	}

	if mustType[authFlowIDResult](t, result).FlowID != presentation.FlowID {
		t.Fatalf("callback result = %#v", result)
	}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": "anthropic",
		"flowId":     presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if got := mustType[authStatusResult](t, status); got.State != authStateAuthenticated || got.ExpiresAt != 0 {
		t.Fatalf("status = %#v", got)
	}

	record, ok, err := agent.providerAuth.ledger.read("anthropic")
	if err != nil || !ok || record.State != authLedgerConfirmed ||
		record.ConnectionID != testConnectionID {
		t.Fatalf("confirmed lineage = %#v, %v, %v", record, ok, err)
	}

	if len(client.authSubmits) != 1 || client.authSubmits[0] != "code#state" {
		t.Fatalf("native submits = %#v", client.authSubmits)
	}
}

func TestStatusMapsExactNativePollVocabulary(t *testing.T) {
	t.Parallel()

	cases := []struct {
		native string
		state  string
		reason string
	}{
		{nativehermes.AuthPollApproved, authStateAuthenticated, ""},
		{nativehermes.AuthPollDenied, authStateFailed, authReasonProviderRefused},
		{nativehermes.AuthPollExpired, authStateExpired, authReasonDeadline},
		{nativehermes.AuthPollError, authStateFailed, authReasonAcceptanceUnknown},
	}

	for _, testCase := range cases {
		t.Run(testCase.native, func(t *testing.T) {
			t.Parallel()

			agent, client := newAuthAgent(t)
			presentation := startDeviceFlow(t, agent, client)
			client.authPoll = nativehermes.AuthPoll{State: testCase.native}

			result, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
				"sessionId":  string(testSessionID),
				"providerId": testProviderID,
				"flowId":     presentation.FlowID,
			})
			if err != nil {
				t.Fatalf("status: %v", err)
			}

			got := mustType[authStatusResult](t, result)
			if got.State != testCase.state || got.Reason != testCase.reason {
				t.Fatalf("status = %#v", got)
			}
		})
	}
}

func TestStatusCachesPendingPollsBehindTheFloor(t *testing.T) {
	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	now := time.Now()
	originalNow := authNow
	authNow = func() time.Time { return now }
	t.Cleanup(func() { authNow = originalNow })

	polls := 0
	client.authPollFunc = func(context.Context, string, string) (nativehermes.AuthPoll, error) {
		polls++

		return nativehermes.AuthPoll{State: nativehermes.AuthPollPending}, nil
	}

	params := map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": testProviderID,
		"flowId":     presentation.FlowID,
	}

	for range 3 {
		if _, err := callLeg(t, agent, AuthStatusMethod, params); err != nil {
			t.Fatalf("status: %v", err)
		}
	}

	if polls != 1 {
		t.Fatalf("native polls = %d", polls)
	}

	now = now.Add(authPollFloor)
	if _, err := callLeg(t, agent, AuthStatusMethod, params); err != nil {
		t.Fatalf("status after floor: %v", err)
	}

	if polls != 2 {
		t.Fatalf("native polls after floor = %d", polls)
	}
}

func TestAuthorizeReplayAndCancellationAreIdempotent(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	first := startDeviceFlow(t, agent, client)

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
		agent.providerAuth.generation,
		testProviderID,
		nativehermes.AuthFlowDeviceCode,
		"request-1",
	))
	if err != nil || mustType[authAuthorizeResult](t, result) != first {
		t.Fatalf("authorize replay = %#v, %v", result, err)
	}

	params := map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": testProviderID,
		"flowId":     first.FlowID,
	}
	for range 2 {
		if _, cancelErr := callLeg(t, agent, AuthCancelMethod, params); cancelErr != nil {
			t.Fatalf("cancel: %v", cancelErr)
		}
	}

	status, err := callLeg(t, agent, AuthStatusMethod, params)
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	got := mustType[authStatusResult](t, status)
	if got.State != authStateCancelled || got.Reason != authReasonOwnerCancel {
		t.Fatalf("status = %#v", got)
	}

	if len(client.authCancelled) != 1 || client.authCancelled[0] != "native-device" {
		t.Fatalf("native cancellations = %#v", client.authCancelled)
	}
}

func TestAuthorizeAndCallbackRejectAddressingFailures(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	stale := authorizeParams("stale", testProviderID, nativehermes.AuthFlowDeviceCode, "request")
	_, err := callLeg(t, agent, AuthAuthorizeMethod, stale)
	requireInvalidField(t, err, authFieldMethodsGeneration)

	unknown := authorizeParams(generation, testProviderID, "unknown", "request")
	_, err = callLeg(t, agent, AuthAuthorizeMethod, unknown)
	requireInvalidField(t, err, authFieldMethod)

	withInputs := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request")
	withInputs["inputs"] = map[string]string{"unexpected": "value"}
	_, err = callLeg(t, agent, AuthAuthorizeMethod, withInputs)
	requireInvalidField(t, err, authFieldInputs)

	presentation := startDeviceFlow(t, agent, client)
	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": testProviderID,
		"method":     nativehermes.AuthFlowDeviceCode,
		"flowId":     presentation.FlowID,
		"input":      "value",
	})
	requireInvalidField(t, err, authFieldInput)
}

func TestAuthorizeNativeFailuresStayClosed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		start nativehermes.AuthStart
		err   error
		cause string
	}{
		{
			name:  "provider refusal",
			err:   &nativehermes.AuthStatusError{StatusCode: 400},
			cause: authCauseProviderRefused,
		},
		{
			name: "flow mismatch",
			start: nativehermes.AuthStart{
				SessionID: "native",
				Flow:      nativehermes.AuthFlowPKCE,
				URL:       testPKCEURL,
			},
			cause: authCauseNativeVeto,
		},
		{
			name: "loopback",
			start: nativehermes.AuthStart{
				SessionID: "native",
				Flow:      nativehermes.AuthFlowDeviceCode,
				URL:       "https://localhost/callback",
			},
			cause: authCauseUnsupportedVariant,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			agent, client := newAuthAgent(t)
			generation := seedCatalog(t, agent, client)
			client.authStart = testCase.start
			client.authStartErr = testCase.err

			_, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
				generation,
				testProviderID,
				nativehermes.AuthFlowDeviceCode,
				"request",
			))
			requireAuthCause(t, err, testCase.cause)
		})
	}
}

func TestCallbackNativeTerminalFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		poll  nativehermes.AuthPoll
		err   error
		cause string
	}{
		{"submit refusal", nativehermes.AuthPoll{}, &nativehermes.AuthStatusError{StatusCode: 400}, authCauseProviderRefused},
		{"denied", nativehermes.AuthPoll{State: nativehermes.AuthPollDenied}, nil, authCauseProviderRefused},
		{"expired", nativehermes.AuthPoll{State: nativehermes.AuthPollExpired}, nil, authCauseFlowExpired},
		{"error", nativehermes.AuthPoll{State: nativehermes.AuthPollError}, nil, authCauseTransport},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			agent, client := newAuthAgent(t)
			presentation := startPKCEFlow(t, agent, client)
			client.authSubmitErr = testCase.err
			client.authPoll = testCase.poll

			_, err := callLeg(t, agent, AuthCallbackMethod, map[string]any{
				"sessionId":  string(testSessionID),
				"providerId": "anthropic",
				"method":     nativehermes.AuthFlowPKCE,
				"flowId":     presentation.FlowID,
				"input":      "code",
			})
			requireAuthCause(t, err, testCase.cause)
		})
	}
}

func TestStatusKeepsPendingOnNativeTransportFailure(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)
	client.authPollErr = errors.New("transport")

	result, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": testProviderID,
		"flowId":     presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if got := mustType[authStatusResult](t, result); got.State != authStatePending {
		t.Fatalf("status = %#v", got)
	}
}

func TestCloseSessionCancelsItsPendingFlows(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}

	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	if agent.providerAuth.byID[presentation.FlowID] != nil {
		t.Fatal("closed session left its flow addressable")
	}

	if len(client.authCancelled) != 1 {
		t.Fatalf("native cancellations = %#v", client.authCancelled)
	}
}
