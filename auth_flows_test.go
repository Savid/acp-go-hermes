package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
		{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode, Disconnectable: true},
		{ID: "anthropic", Name: "Anthropic API Key", Flow: nativehermes.AuthFlowPKCE, Disconnectable: true},
	}
	client.authKeyProviders = []nativehermes.AuthAPIKeyProvider{{ID: "openai", Name: "OpenAI"}}

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
		SessionID:    "native-flow",
		Flow:         nativehermes.AuthFlowDeviceCode,
		URL:          testDeviceURL,
		UserCode:     "ABCD-EFGH",
		PollInterval: 3 * time.Second,
		ExpiresIn:    30 * time.Minute,
	}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	return mustType[authAuthorizeResult](t, result)
}

func TestAuthorizeDeviceFlowRelaysTheNativePresentation(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	if presentation.Interaction != authInteractionWait {
		t.Fatalf("interaction = %q", presentation.Interaction)
	}

	if presentation.URL != testDeviceURL || presentation.UserCode != "ABCD-EFGH" {
		t.Fatalf("presentation = %#v", presentation)
	}

	if presentation.CallbackInput != "" {
		t.Fatalf("a wait flow published a callback input: %#v", presentation)
	}

	if presentation.PollIntervalMs != 3000 {
		t.Fatalf("pollIntervalMs = %d, want the native hint", presentation.PollIntervalMs)
	}

	if presentation.FlowExpiresAt == 0 {
		t.Fatal("flowExpiresAt is always present")
	}

	// The native expiry exceeds the wrapper deadline, so the effective deadline
	// is the wrapper's.
	if presentation.FlowExpiresAt > time.Now().Add(authSafetyDeadline+time.Minute).UnixMilli() {
		t.Fatalf("flowExpiresAt = %d exceeds the wrapper deadline", presentation.FlowExpiresAt)
	}

	flow := agent.providerAuth.byID[presentation.FlowID]
	if flow.probeInterval != authPollFloor {
		t.Fatalf("probe interval = %v, want the floor raised from the native 3s", flow.probeInterval)
	}

	record, ok, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil || !ok {
		t.Fatalf("ledger entry after authorize: %v, %v", ok, err)
	}

	if record.State != authLedgerIntent || record.FlowID != presentation.FlowID || record.AuthorizeRequestID != "request-1" {
		t.Fatalf("ledger record = %#v", record)
	}
}

func TestAuthorizePKCEFlowUsesTheOtherStartShape(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	client.authStart = nativehermes.AuthStart{
		SessionID: "native-pkce",
		Flow:      nativehermes.AuthFlowPKCE,
		URL:       testPKCEURL,
		ExpiresIn: 5 * time.Minute,
	}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "anthropic", nativehermes.AuthFlowPKCE, "request-pkce"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	presentation := mustType[authAuthorizeResult](t, result)
	if presentation.Interaction != authInteractionCallback || presentation.CallbackInput != authCallbackInputCode {
		t.Fatalf("presentation = %#v", presentation)
	}

	if presentation.UserCode != "" || presentation.PollIntervalMs != 0 {
		t.Fatalf("the pkce start shape carries neither a user code nor a poll interval: %#v", presentation)
	}

	// The native expiry is shorter than the wrapper deadline, so it wins.
	if presentation.FlowExpiresAt > time.Now().Add(6*time.Minute).UnixMilli() {
		t.Fatalf("flowExpiresAt = %d ignored the shorter native expiry", presentation.FlowExpiresAt)
	}
}

func TestAuthorizeSecretMethodMintsNothing(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "request-secret"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	presentation := mustType[authAuthorizeResult](t, result)
	if presentation.Interaction != authInteractionSecret || presentation.URL != "" {
		t.Fatalf("presentation = %#v", presentation)
	}

	if presentation.Message != "OpenAI" {
		t.Fatalf("secret message = %q, want the method's native label", presentation.Message)
	}

	if len(client.authSubmits) != 0 {
		t.Fatal("a secret authorize reached the native start route")
	}
}

func TestAuthorizeFencesGenerationMethodAndInputs(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	stale := authorizeParams("stale-generation", testProviderID, nativehermes.AuthFlowDeviceCode, "r")

	_, err := callLeg(t, agent, AuthAuthorizeMethod, stale)
	requireInvalidField(t, err, authFieldMethodsGeneration)

	_, err = callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, "no-such-method", "r"))
	requireInvalidField(t, err, authFieldMethod)

	withInputs := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r")
	withInputs["inputs"] = map[string]any{"instanceUrl": "https://gitlab.example"}

	_, err = callLeg(t, agent, AuthAuthorizeMethod, withInputs)
	requireInvalidField(t, err, authFieldInputs)

	malformedInputs := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r")
	malformedInputs["inputs"] = 7

	_, err = callLeg(t, agent, AuthAuthorizeMethod, malformedInputs)
	requireInvalidField(t, err, authFieldInputs)

	for _, field := range []string{"sessionId", "providerId", "connectionId", "methodsGeneration", "method", "authorizeRequestId"} {
		params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r")
		delete(params, field)

		_, err := callLeg(t, agent, AuthAuthorizeMethod, params)
		requireInvalidField(t, err, field)
	}

	unknownSession := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r")
	unknownSession["sessionId"] = "unknown"

	if _, err := callLeg(t, agent, AuthAuthorizeMethod, unknownSession); err == nil {
		t.Fatal("unknown session accepted")
	}
}

func TestAuthorizeReplayAnswersFromMemoryWithNoNativeCall(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	first := startDeviceFlow(t, agent, client)

	generation := agent.providerAuth.generation
	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		t.Fatal("a replayed authorizeRequestId reached the native start route")

		return nativehermes.AuthStart{}, nil
	}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if mustType[authAuthorizeResult](t, result) != first {
		t.Fatalf("replay = %#v, want %#v", result, first)
	}
}

func TestAuthorizeFailsClosedWhenTheProviderPoolCannotBeSnapshotted(t *testing.T) {
	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	original := authSnapshotPool
	authSnapshotPool = func(string, string) (nativehermes.AuthPoolSnapshot, error) {
		return nativehermes.AuthPoolSnapshot{}, errors.New("store")
	}

	t.Cleanup(func() { authSnapshotPool = original })

	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		t.Error("the native start ran without a snapshot of the pool it appends to")

		return nativehermes.AuthStart{}, nil
	}

	_, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1"))
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestAuthorizeReplayOutlivesTerminalization(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	first := startDeviceFlow(t, agent, client)

	seedNativeCompletion(t, client.xdg.Root, testProviderID)
	seedDeviceResidence(t, client.xdg.Root, testProviderID, 21600)

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollComplete}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": first.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if state := mustType[authStatusResult](t, status).State; state != authStateAuthenticated {
		t.Fatalf("state = %q, want a terminal flow to replay from", state)
	}

	generation := agent.providerAuth.generation
	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		t.Error("a replayed authorizeRequestId reached the native start route")

		return nativehermes.AuthStart{}, nil
	}

	replay, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1"))
	if err != nil {
		t.Fatalf("replay after terminalization: %v", err)
	}

	if mustType[authAuthorizeResult](t, replay) != first {
		t.Fatalf("replay = %#v, want %#v", replay, first)
	}

	if len(client.authCancelled) != 0 {
		t.Fatal("a replayed idempotency key superseded the flow it should have replayed")
	}

	record, _, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil || record.Revision != 1 || record.FlowID != first.FlowID {
		t.Fatalf("a replay disturbed the ledger: %#v, %v", record, err)
	}
}

func TestAuthorizeReplayWaitsForTheMintToPublish(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	minting := make(chan struct{})
	release := make(chan struct{})
	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		close(minting)
		<-release

		return nativehermes.AuthStart{
			SessionID: "native-flow",
			Flow:      nativehermes.AuthFlowDeviceCode,
			URL:       testDeviceURL,
			UserCode:  "ABCD-EFGH",
		}, nil
	}

	params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1")

	minted := make(chan authAuthorizeResult, 1)
	go func() {
		result, err := callLeg(t, agent, AuthAuthorizeMethod, params)
		if err != nil {
			t.Errorf("authorize: %v", err)
			close(minted)

			return
		}

		minted <- mustType[authAuthorizeResult](t, result)
	}()

	<-minting

	replayed := make(chan authAuthorizeResult, 1)
	go func() {
		result, err := callLeg(t, agent, AuthAuthorizeMethod, params)
		if err != nil {
			t.Errorf("replay during the mint: %v", err)
			close(replayed)

			return
		}

		replayed <- mustType[authAuthorizeResult](t, result)
	}()

	time.Sleep(10 * time.Millisecond)

	select {
	case answered := <-replayed:
		t.Fatalf("a repeat answered before the mint published: %#v", answered)
	default:
	}

	// A caller that gives up while the mint is still running is answered rather
	// than left holding the wait.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	key := authFlowKey{sessionID: testSessionID, providerID: testProviderID}
	if _, ok, err := agent.providerAuth.replayAuthorize(cancelled, key, "request-1"); !ok || err == nil {
		t.Fatalf("an abandoned repeat = %v, %v", ok, err)
	}

	close(release)

	presentation := <-minted
	if presentation != <-replayed {
		t.Fatal("a repeat arriving during the mint replayed a different presentation")
	}

	if presentation.Interaction != authInteractionWait || presentation.FlowID == "" {
		t.Fatalf("presentation = %#v", presentation)
	}
}

func TestAuthorizeReplayRepeatsTheMintFailure(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	starts := 0
	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		starts++

		return nativehermes.AuthStart{}, errors.New("gateway")
	}

	params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1")

	_, err := callLeg(t, agent, AuthAuthorizeMethod, params)
	requireAuthCause(t, err, authCauseTransport)

	flowID := authErrorField(t, err, authFieldFlowID)

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID,
	})
	if err != nil {
		t.Fatalf("status on a failed mint: %v", err)
	}

	terminal := mustType[authStatusResult](t, status)
	if terminal.State != authStateFailed || terminal.Reason != authReasonTransport {
		t.Fatalf("failed mint left %#v", terminal)
	}

	_, replayErr := callLeg(t, agent, AuthAuthorizeMethod, params)
	requireAuthCause(t, replayErr, authCauseTransport)

	if starts != 1 {
		t.Fatalf("native start invocations = %d, want the failure replayed", starts)
	}
}

// TestAuthorizeRefusesAnUncontainedBrowserLaunch covers the leg's answer where
// the session's process cannot shadow the launchers a login would exec: the
// closed refusal, not a browser tab and not a native call the owner authorized.
func TestAuthorizeRefusesAnUncontainedBrowserLaunch(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		return nativehermes.AuthStart{}, nativehermes.ErrBrowserLaunchUncontained
	}

	params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1")

	_, err := callLeg(t, agent, AuthAuthorizeMethod, params)
	requireAuthCause(t, err, authCausePolicy)

	if authErrorRetryable(t, err) {
		t.Fatal("a refusal the adapter made itself was reported as retryable")
	}
}

func TestAuthorizeSupersedesTheOlderFlow(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	first := startDeviceFlow(t, agent, client)

	generation := agent.providerAuth.generation
	client.authStartFunc = nil

	second, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-2"))
	if err != nil {
		t.Fatalf("superseding authorize: %v", err)
	}

	if mustType[authAuthorizeResult](t, second).FlowID == first.FlowID {
		t.Fatal("superseding authorize reused the flow id")
	}

	if len(client.authCancelled) != 1 {
		t.Fatalf("native cancel invocations = %d, want one alongside the wrapper disarm", len(client.authCancelled))
	}

	_, err = callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": first.FlowID,
	})
	requireInvalidField(t, err, authFieldFlowID)

	record, _, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}

	if record.Revision != 2 {
		t.Fatalf("revision = %d, want the lineage counter advanced", record.Revision)
	}
}

func TestAuthorizeNativeFailurePaths(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	client.authStartErr = &nativehermes.AuthStatusError{StatusCode: 429}

	_, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r1"))
	requireAuthCause(t, err, authCauseProviderRefused)

	client.authStartErr = nil
	client.authStart = nativehermes.AuthStart{SessionID: "s", Flow: nativehermes.AuthFlowPKCE, URL: testPKCEURL}

	_, err = callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r2"))
	requireAuthCause(t, err, authCauseNativeVeto)

	client.authStart = nativehermes.AuthStart{SessionID: "s", Flow: nativehermes.AuthFlowDeviceCode, URL: "https://127.0.0.1/device"}

	_, err = callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r3"))
	requireAuthCause(t, err, authCauseUnsupportedVariant)

	client.authStart = nativehermes.AuthStart{SessionID: "s", Flow: nativehermes.AuthFlowDeviceCode, URL: "http://accounts.x.ai/device"}

	_, err = callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r4"))
	requireAuthCause(t, err, authCauseNativeVeto)

	client.authStart = nativehermes.AuthStart{
		SessionID: "s", Flow: nativehermes.AuthFlowDeviceCode, URL: testDeviceURL, UserCode: "</script>ABCD",
	}

	_, err = callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r5"))
	requireAuthCause(t, err, authCauseNativeVeto)
}

func TestAuthorizeFailsClosedWhenTheGatewayIsGone(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	_, err = callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r"))
	requireAuthCause(t, err, authCauseTransport)
}

func TestAuthorizeFailsClosedWhenTheLedgerCannotRecord(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	ledgerRename = func(string, string) error { return errors.New("rename") }

	_, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r"))
	requireAuthCause(t, err, authCauseProcess)
}

func TestAuthorizeFailsClosedWhenNoFlowIDCanBeMinted(t *testing.T) {
	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	original := authRandRead
	authRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }

	t.Cleanup(func() { authRandRead = original })

	_, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r"))
	requireAuthCause(t, err, authCauseProcess)
}

func TestCallbackAppliesASecretAndReachesSaved(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	flowID := mustType[authAuthorizeResult](t, result).FlowID

	if _, errLocal := callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai",
		"method": authAPIKeyMethodID, "flowId": flowID, "input": "sk-operator-key",
	}); errLocal != nil {
		t.Fatalf("callback: %v", errLocal)
	}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai", "flowId": flowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if state := mustType[authStatusResult](t, status).State; state != authStateSaved {
		t.Fatalf("state = %q, want saved for a value nobody validated", state)
	}

	material, present, err := nativehermes.AuthReadSlot(client.xdg.Root, "openai", nativehermes.AuthSlotLabel(testConnectionID))
	if err != nil || !present {
		t.Fatalf("reserved slot = %v, %v", present, err)
	}

	if material.AccessToken != "sk-operator-key" || material.AuthType != nativehermes.AuthTypeAPIKey {
		t.Fatalf("reserved slot material = %#v", material)
	}
}

func TestCallbackSubmitsAPKCECodeAndMigratesTheReservedSlot(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	seedAmbientPool(t, client.xdg.Root, "anthropic")

	client.authStart = nativehermes.AuthStart{SessionID: "native-pkce", Flow: nativehermes.AuthFlowPKCE, URL: testPKCEURL}
	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollComplete}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "anthropic", nativehermes.AuthFlowPKCE, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	flowID := mustType[authAuthorizeResult](t, result).FlowID

	seedNativeCompletion(t, client.xdg.Root, "anthropic")
	seedPKCEResidence(t, client.xdg.Root, 1750000000000)

	if _, errLocal := callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic",
		"method": nativehermes.AuthFlowPKCE, "flowId": flowID, "input": "code#state",
	}); errLocal != nil {
		t.Fatalf("callback: %v", errLocal)
	}

	if len(client.authSubmits) != 1 || client.authSubmits[0] != "code#state" {
		t.Fatalf("native submits = %#v", client.authSubmits)
	}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic", "flowId": flowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if mustType[authStatusResult](t, status).State != authStateAuthenticated {
		t.Fatalf("status = %#v", status)
	}

	if mustType[authStatusResult](t, status).ExpiresAt != 1750000000000 {
		t.Fatalf("credential expiry = %d, want the pkce residence value", mustType[authStatusResult](t, status).ExpiresAt)
	}

	assertReservedSlotOnly(t, client.xdg.Root, "anthropic", "native-access")
}

func TestCallbackRejectsMalformedAndTerminalFlows(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	base := map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"method": nativehermes.AuthFlowDeviceCode, "flowId": presentation.FlowID, "input": "value",
	}

	for _, field := range []string{"sessionId", "providerId", "method", "flowId", "input"} {
		params := map[string]any{}
		for key, value := range base {
			params[key] = value
		}

		delete(params, field)

		_, err := callLeg(t, agent, AuthCallbackMethod, params)
		requireInvalidField(t, err, field)
	}

	mismatch := map[string]any{}
	for key, value := range base {
		mismatch[key] = value
	}

	mismatch["method"] = authAPIKeyMethodID

	_, err := callLeg(t, agent, AuthCallbackMethod, mismatch)
	requireInvalidField(t, err, authFieldMethod)

	empty := map[string]any{}
	for key, value := range base {
		empty[key] = value
	}

	empty["input"] = ""

	_, err = callLeg(t, agent, AuthCallbackMethod, empty)
	requireInvalidField(t, err, authFieldInput)

	// A wait flow publishes no callback input, so submitting one is an
	// addressing failure rather than a native call.
	_, err = callLeg(t, agent, AuthCallbackMethod, base)
	requireInvalidField(t, err, authFieldInput)

	if _, errLocal := callLeg(t, agent, AuthCancelMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	}); errLocal != nil {
		t.Fatalf("cancel: %v", errLocal)
	}

	_, err = callLeg(t, agent, AuthCallbackMethod, base)
	requireAuthCause(t, err, authCauseFlowState)

	unknownSession := map[string]any{}
	for key, value := range base {
		unknownSession[key] = value
	}

	unknownSession["sessionId"] = "unknown"

	if _, err := callLeg(t, agent, AuthCallbackMethod, unknownSession); err == nil {
		t.Fatal("unknown session accepted")
	}
}

func TestCallbackNativeFailurePaths(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	client.authStart = nativehermes.AuthStart{SessionID: "native-pkce", Flow: nativehermes.AuthFlowPKCE, URL: testPKCEURL}

	start := func(requestID string) string {
		t.Helper()

		result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "anthropic", nativehermes.AuthFlowPKCE, requestID))
		if err != nil {
			t.Fatalf("authorize: %v", err)
		}

		return mustType[authAuthorizeResult](t, result).FlowID
	}

	callback := func(flowID string) error {
		_, err := callLeg(t, agent, AuthCallbackMethod, map[string]any{
			"sessionId": string(testSessionID), "providerId": "anthropic",
			"method": nativehermes.AuthFlowPKCE, "flowId": flowID, "input": "code",
		})

		return err
	}

	client.authSubmitErr = &nativehermes.AuthStatusError{StatusCode: 400}
	requireAuthCause(t, callback(start("r1")), authCauseProviderRefused)

	client.authSubmitErr = nil
	client.authPollErr = errors.New("transport")
	requireAuthCause(t, callback(start("r2")), authCauseTransport)

	client.authPollErr = nil
	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollDenied}
	requireAuthCause(t, callback(start("r3")), authCauseProviderRefused)

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollComplete}
	requireAuthCause(t, callback(start("r4")), authCauseHarvestFailed)

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollPending}

	flowID := start("r5")
	if err := callback(flowID); err != nil {
		t.Fatalf("pending callback: %v", err)
	}
}

func TestCallbackFailsClosedWhenTheHomeIsGone(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	flowID := mustType[authAuthorizeResult](t, result).FlowID

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai",
		"method": authAPIKeyMethodID, "flowId": flowID, "input": "key",
	})
	requireAuthCause(t, err, authCauseTransport)

	session.mu.Lock()
	session.client = client
	session.mu.Unlock()

	second, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r2"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	if errLocal := os.WriteFile(filepath.Join(client.xdg.Root, "auth.json"), []byte("{"), 0o600); errLocal != nil {
		t.Fatalf("corrupt store: %v", err)
	}

	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai",
		"method": authAPIKeyMethodID, "flowId": mustType[authAuthorizeResult](t, second).FlowID, "input": "key",
	})
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestCallbackFailsClosedWhenTheConfirmationCannotBeWritten(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	ledgerRename = func(string, string) error { return errors.New("rename") }

	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai",
		"method": authAPIKeyMethodID, "flowId": mustType[authAuthorizeResult](t, result).FlowID, "input": "key",
	})
	requireAuthCause(t, err, authCauseProcess)
}

func TestStatusPollsBehindTheEnforcedFloor(t *testing.T) {
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

	params := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID}

	for range 5 {
		if _, err := callLeg(t, agent, AuthStatusMethod, params); err != nil {
			t.Fatalf("status: %v", err)
		}
	}

	if polls != 1 {
		t.Fatalf("native polls = %d, want one behind the enforced interval", polls)
	}

	now = now.Add(authPollFloor)

	if _, err := callLeg(t, agent, AuthStatusMethod, params); err != nil {
		t.Fatalf("status: %v", err)
	}

	if polls != 2 {
		t.Fatalf("native polls after the interval elapsed = %d", polls)
	}
}

func TestStatusHonoursANativeSlowDown(t *testing.T) {
	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	now := time.Now()
	originalNow := authNow
	authNow = func() time.Time { return now }

	t.Cleanup(func() { authNow = originalNow })

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollSlowDown}

	params := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID}

	if _, err := callLeg(t, agent, AuthStatusMethod, params); err != nil {
		t.Fatalf("status: %v", err)
	}

	flow := agent.providerAuth.byID[presentation.FlowID]
	if flow.probeInterval != authPollFloor+authSlowDownStep {
		t.Fatalf("interval after slow_down = %v", flow.probeInterval)
	}
}

func TestStatusCompletesADeviceFlowAndAnchorsTheExpiry(t *testing.T) {
	agent, client := newAuthAgent(t)

	seedAmbientPool(t, client.xdg.Root, testProviderID)

	presentation := startDeviceFlow(t, agent, client)

	anchor := time.Unix(1_700_000_000, 0)
	originalNow := authNow
	authNow = func() time.Time { return anchor }

	t.Cleanup(func() { authNow = originalNow })

	seedNativeCompletion(t, client.xdg.Root, testProviderID)
	seedDeviceResidence(t, client.xdg.Root, testProviderID, 21600)

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollComplete}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	result := mustType[authStatusResult](t, status)
	if result.State != authStateAuthenticated {
		t.Fatalf("status = %#v", result)
	}

	want := anchor.Add(21600 * time.Second).UnixMilli()
	if result.ExpiresAt != want {
		t.Fatalf("expiresAt = %d, want the harvest-time anchor %d", result.ExpiresAt, want)
	}

	assertReservedSlotOnly(t, client.xdg.Root, testProviderID, "native-access")
}

func TestStatusTerminalisesADeniedFlow(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollDenied}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	result := mustType[authStatusResult](t, status)
	if result.State != authStateFailed || result.Reason != authReasonProviderRefused {
		t.Fatalf("status = %#v", result)
	}

	if result.ExpiresAt != 0 {
		t.Fatalf("a terminal-negative state reported a credential expiry: %#v", result)
	}
}

func TestStatusServesACachedStateWhenTheNativePollFails(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	client.authPollErr = errors.New("transport")

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if mustType[authStatusResult](t, status).State != authStatePending {
		t.Fatalf("status = %#v", status)
	}
}

func TestStatusOnASecretFlowNeverPolls(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	client.authPollFunc = func(context.Context, string, string) (nativehermes.AuthPoll, error) {
		t.Fatal("a secret flow reached the native poll route")

		return nativehermes.AuthPoll{}, nil
	}

	if _, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai", "flowId": mustType[authAuthorizeResult](t, result).FlowID,
	}); err != nil {
		t.Fatalf("status: %v", err)
	}
}

func TestStatusFailsClosedWhenTheGatewayIsGone(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	if _, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	}); err != nil {
		t.Fatalf("status: %v", err)
	}
}

func TestCancelIsIdempotentAndInvokesNativeCancel(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	params := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID}

	result, err := callLeg(t, agent, AuthCancelMethod, params)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if mustType[authFlowIDResult](t, result).FlowID != presentation.FlowID {
		t.Fatalf("cancel = %#v", result)
	}

	if len(client.authCancelled) != 1 {
		t.Fatalf("native cancel invocations = %d", len(client.authCancelled))
	}

	if _, errLocal := callLeg(t, agent, AuthCancelMethod, params); errLocal != nil {
		t.Fatalf("second cancel: %v", err)
	}

	if len(client.authCancelled) != 1 {
		t.Fatal("cancel on a terminal flow invoked native cancel again")
	}

	status, err := callLeg(t, agent, AuthStatusMethod, params)
	if err != nil {
		t.Fatalf("status after cancel: %v", err)
	}

	terminal := mustType[authStatusResult](t, status)
	if terminal.State != authStateCancelled || terminal.Reason != authReasonOwnerCancel {
		t.Fatalf("status = %#v", terminal)
	}
}

func TestAddressedFlowLegRejectsEveryAddressingFailure(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	base := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID}

	for _, field := range []string{"sessionId", "providerId", "flowId"} {
		params := map[string]any{}
		for key, value := range base {
			params[key] = value
		}

		delete(params, field)

		_, err := callLeg(t, agent, AuthStatusMethod, params)
		requireInvalidField(t, err, field)
	}

	crossProvider := map[string]any{}
	for key, value := range base {
		crossProvider[key] = value
	}

	crossProvider["providerId"] = "anthropic"

	_, err := callLeg(t, agent, AuthStatusMethod, crossProvider)
	requireInvalidField(t, err, authFieldFlowID)

	unknownSession := map[string]any{}
	for key, value := range base {
		unknownSession[key] = value
	}

	unknownSession["sessionId"] = "unknown"

	if _, err := callLeg(t, agent, AuthStatusMethod, unknownSession); err == nil {
		t.Fatal("unknown session accepted")
	}
}

func TestFlowExpiresOnTheEffectiveDeadline(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	client.authStart = nativehermes.AuthStart{
		SessionID: "native-flow", Flow: nativehermes.AuthFlowDeviceCode,
		URL: testDeviceURL, ExpiresIn: time.Millisecond,
	}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	flowID := mustType[authAuthorizeResult](t, result).FlowID
	params := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID}

	deadline := time.Now().Add(2 * time.Second)

	for {
		status, err := callLeg(t, agent, AuthStatusMethod, params)
		if err != nil {
			t.Fatalf("status: %v", err)
		}

		terminal := mustType[authStatusResult](t, status)
		if terminal.State == authStateExpired {
			if terminal.Reason != authReasonDeadline {
				t.Fatalf("expired reason = %q", terminal.Reason)
			}

			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("flow did not expire: %#v", terminal)
		}

		time.Sleep(5 * time.Millisecond)
	}

	// A completer that already fired is idempotent.
	agent.providerAuth.expire(agent.providerAuth.byID[flowID])
}

func TestCloseSessionCancelsPendingFlows(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	flow := agent.providerAuth.byID[presentation.FlowID]
	if flow != nil {
		t.Fatal("a closed session left its flow addressable")
	}

	if len(client.authCancelled) != 1 {
		t.Fatalf("native cancel invocations = %d", len(client.authCancelled))
	}

	agent.providerAuth.closeSession(context.Background(), testSessionID)
}

func TestCompletionNeverMigratesAnAmbientPoolEntry(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	writeStoreFixture(t, client.xdg.Root, map[string]any{
		"credential_pool": map[string]any{
			testProviderID: []any{map[string]any{"auth_type": "oauth", testFieldAccessToken: "ambient", "source": "gh"}},
		},
	})

	presentation := startDeviceFlow(t, agent, client)

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollComplete}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	terminal := mustType[authStatusResult](t, status)
	if terminal.State != authStateFailed || terminal.Reason != authReasonHarvestFailed {
		t.Fatalf("a flow that produced nothing settled as %#v", terminal)
	}

	present, err := nativehermes.AuthSlotPresent(client.xdg.Root, testProviderID, nativehermes.AuthSlotLabel(testConnectionID))
	if err != nil || present {
		t.Fatalf("an entry resident before the flow was handed a reserved slot: %v, %v", present, err)
	}
}

func TestCloseSessionEndsTheReachOfARetainedFlow(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	seedNativeCompletion(t, client.xdg.Root, testProviderID)
	seedDeviceResidence(t, client.xdg.Root, testProviderID, 21600)

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollComplete}

	if _, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	}); err != nil {
		t.Fatalf("status: %v", err)
	}

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	broker := agent.providerAuth
	if len(broker.retained) != 0 || len(broker.byID) != 0 || len(broker.flows) != 0 {
		t.Fatalf("a closed session left records behind: %d retained, %d addressable, %d pending", len(broker.retained), len(broker.byID), len(broker.flows))
	}

	if len(client.authCancelled) != 0 {
		t.Fatalf("a terminal flow was cancelled natively at close: %v", client.authCancelled)
	}
}

func TestCancelNativeIsSkippedWithoutANativeFlow(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	agent.providerAuth.cancelNative(context.Background(), &authFlow{sessionID: testSessionID})

	agent.providerAuth.cancelNative(context.Background(), &authFlow{sessionID: "missing", nativeSessionID: "n"})

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	agent.providerAuth.cancelNative(context.Background(), &authFlow{sessionID: testSessionID, nativeSessionID: "n"})

	if len(client.authCancelled) != 0 {
		t.Fatalf("native cancel invocations = %d", len(client.authCancelled))
	}
}

func TestNewAuthTokenIsOpaqueAndUnpadded(t *testing.T) {
	t.Parallel()

	token, err := newAuthToken()
	if err != nil {
		t.Fatalf("newAuthToken: %v", err)
	}

	if len(token) != 22 {
		t.Fatalf("token %q is not 16 CSPRNG bytes in unpadded base64url", token)
	}

	if !authTerminal(authStateFailed) || authTerminal(authStatePending) {
		t.Fatal("terminal classification is wrong")
	}
}

// seedAmbientPool writes the gh-derived entry a fresh Hermes home already
// carries. It is resident before any flow starts and must never be migrated.
func seedAmbientPool(t *testing.T, home string, providerID string) {
	t.Helper()

	seedPoolEntry(t, home, providerID, map[string]any{
		"auth_type": "oauth", testFieldAccessToken: "ambient", "source": "gh", "label": "",
	})
}

// seedNativeCompletion writes the unlabelled pool entry a completed native flow
// leaves behind. A completed flow takes the first position, so the entry it
// produced is not the newest one.
func seedNativeCompletion(t *testing.T, home string, providerID string) {
	t.Helper()

	seedPoolEntry(t, home, providerID, map[string]any{
		"auth_type": "oauth", testFieldAccessToken: "native-access", "refresh_token": "native-refresh",
		"expires_at": nil, "source": "device_code", "request_count": 0, "secret_fingerprint": "abc",
	})
}

func seedPoolEntry(t *testing.T, home string, providerID string, entry map[string]any) {
	t.Helper()

	store := readStoreFixture(t, home)

	pool, _ := store["credential_pool"].(map[string]any)
	if pool == nil {
		pool = map[string]any{}
	}

	entries, _ := pool[providerID].([]any)
	pool[providerID] = append([]any{entry}, entries...)
	store["credential_pool"] = pool

	writeStoreFixture(t, home, store)
}

func seedDeviceResidence(t *testing.T, home string, providerID string, expiresIn int) {
	t.Helper()

	store := readStoreFixture(t, home)

	providers, _ := store["providers"].(map[string]any)
	if providers == nil {
		providers = map[string]any{}
	}

	providers[providerID] = map[string]any{"tokens": map[string]any{
		testFieldAccessToken: "native-access", "expires_in": expiresIn, "token_type": "Bearer", "id_token": "ignored",
	}}
	store["providers"] = providers

	writeStoreFixture(t, home, store)
}

func seedPKCEResidence(t *testing.T, home string, expiresAt int64) {
	t.Helper()

	contents, err := json.Marshal(map[string]any{
		"accessToken": "native-access", "refreshToken": "native-refresh", "expiresAt": expiresAt,
	})
	if err != nil {
		t.Fatalf("marshal pkce residence: %v", err)
	}

	if err := os.WriteFile(filepath.Join(home, ".anthropic_oauth.json"), contents, 0o600); err != nil {
		t.Fatalf("write pkce residence: %v", err)
	}
}

func writeStoreFixture(t *testing.T, home string, store map[string]any) {
	t.Helper()

	contents, err := json.Marshal(store)
	if err != nil {
		t.Fatalf("marshal store: %v", err)
	}

	if err := os.WriteFile(filepath.Join(home, "auth.json"), contents, 0o600); err != nil {
		t.Fatalf("write store: %v", err)
	}
}

func readStoreFixture(t *testing.T, home string) map[string]any {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join(home, "auth.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{}
		}

		t.Fatalf("read store: %v", err)
	}

	var store map[string]any
	if err := json.Unmarshal(contents, &store); err != nil {
		t.Fatalf("decode store: %v", err)
	}

	return store
}

// assertReservedSlotOnly proves the remove-then-add migration: exactly one entry
// carries the reserved label, the ambient entry survives untouched, and the
// original unlabelled entry is gone rather than duplicated.
func assertReservedSlotOnly(t *testing.T, home string, providerID string, token string) {
	t.Helper()

	store := readStoreFixture(t, home)

	pool, _ := store["credential_pool"].(map[string]any)

	entries, _ := pool[providerID].([]any)
	if len(entries) != 2 {
		t.Fatalf("pool entries = %#v", entries)
	}

	label := nativehermes.AuthSlotLabel(testConnectionID)
	labelled := 0

	for _, raw := range entries {
		entry, _ := raw.(map[string]any)
		if entry["label"] == label {
			labelled++

			if entry[testFieldAccessToken] != token {
				t.Fatalf("reserved slot token = %v", entry[testFieldAccessToken])
			}
		}
	}

	if labelled != 1 {
		t.Fatalf("%d entries carry the reserved label; remove-then-add must leave exactly one", labelled)
	}

	first, _ := entries[0].(map[string]any)
	if first[testFieldAccessToken] != "ambient" {
		t.Fatalf("the ambient entry was disturbed: %#v", first)
	}
}

func TestFlowLegsRejectUnknownParamFields(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	for _, method := range []string{AuthAuthorizeMethod, AuthCallbackMethod, AuthStatusMethod, AuthCancelMethod} {
		_, err := callLeg(t, agent, method, map[string]any{"sessionId": string(testSessionID), "extra": 1})
		requireInvalidField(t, err, "extra")
	}
}

func TestCallbackRejectsAnUnknownFlowID(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	startDeviceFlow(t, agent, client)

	_, err := callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"method": nativehermes.AuthFlowDeviceCode, "flowId": "unknown", "input": "value",
	})
	requireInvalidField(t, err, authFieldFlowID)
}

func TestSubmitCodeFailsClosedWithoutAGateway(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	client.authStart = nativehermes.AuthStart{SessionID: "native-pkce", Flow: nativehermes.AuthFlowPKCE, URL: testPKCEURL}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "anthropic", nativehermes.AuthFlowPKCE, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic",
		"method": nativehermes.AuthFlowPKCE, "flowId": mustType[authAuthorizeResult](t, result).FlowID, "input": "code",
	})
	requireAuthCause(t, err, authCauseTransport)
}

func TestCompleteFlowFailurePaths(t *testing.T) {
	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	flow := agent.providerAuth.byID[presentation.FlowID]

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	originalExpiry := authReadFlowExpiry
	authReadFlowExpiry = func(string, string, string) (nativehermes.AuthMaterial, bool, error) {
		return nativehermes.AuthMaterial{}, false, errors.New("residence")
	}

	t.Cleanup(func() { authReadFlowExpiry = originalExpiry })

	seedNativeCompletion(t, client.xdg.Root, testProviderID)

	requireAuthCause(t, agent.providerAuth.completeFlow(context.Background(), session, flow), authCauseHarvestFailed)

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	requireAuthCause(t, agent.providerAuth.completeFlow(context.Background(), session, flow), authCauseTransport)
}

func TestCompleteFlowFailsClosedWhenTheConfirmationCannotBeWritten(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	flow := agent.providerAuth.byID[presentation.FlowID]

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	seedNativeCompletion(t, client.xdg.Root, testProviderID)

	ledgerRename = func(string, string) error { return errors.New("rename") }

	requireAuthCause(t, agent.providerAuth.completeFlow(context.Background(), session, flow), authCauseProcess)
}

func TestCloseSessionLeavesPeerFlowsAlone(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	peer := newSession(agent, "peer-session", "/cwd", nil, nil, nativehermes.Session{ID: "peer"}, client, sessionMeta{}, idmapRecord{})
	if err := agent.storeStartedSession(peer); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	agent.providerAuth.closeSession(context.Background(), "peer-session")

	if agent.providerAuth.byID[presentation.FlowID] == nil {
		t.Fatal("closing a peer session cancelled another session's flow")
	}
}
