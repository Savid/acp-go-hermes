package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func authRawParams(t *testing.T, params map[string]any) json.RawMessage {
	t.Helper()

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal auth params: %v", err)
	}

	return raw
}

func endedAuthContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return ctx
}

func TestAuthGateQueuedWaiterAcquiresAfterRelease(t *testing.T) {
	t.Parallel()

	gates := map[string]*authGate{}
	var mu sync.Mutex

	first, ok := authAcquireGate(context.Background(), &mu, gates, "provider")
	if !ok {
		t.Fatal("first acquisition failed")
	}

	acquired := make(chan func(), 1)
	go func() {
		release, queued := authAcquireGate(context.Background(), &mu, gates, "provider")
		if queued {
			acquired <- release
		}
	}()

	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		waiters := gates["provider"].waiters
		mu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter did not join the gate")
		}
	}

	first()

	select {
	case release := <-acquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("queued waiter did not acquire after release")
	}
}

func TestFlowGatesFailClosedAfterContextCancellation(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	flow := &authFlow{id: "flow", providerID: testProviderID, method: authCatalogMethod{ID: "oauth"}}

	providerRelease, ok := agent.providerAuth.lockProvider(context.Background(), testProviderID)
	if !ok {
		t.Fatal("hold provider gate")
	}
	if release, err := agent.providerAuth.lockFlowProvider(endedAuthContext(), flow); err == nil || release != nil {
		t.Fatal("cancelled provider waiter acquired")
	}
	providerRelease()

	ledgerRelease, ok := agent.providerAuth.lockLedger(context.Background(), testProviderID)
	if !ok {
		t.Fatal("hold ledger gate")
	}
	if release, err := agent.providerAuth.lockFlowLedger(endedAuthContext(), flow); err == nil || release != nil {
		t.Fatal("cancelled ledger waiter acquired")
	}
	ledgerRelease()
}

func TestPublishFlowSupersedesAndRetiresItsPredecessor(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}

	key := authFlowKey{sessionID: testSessionID, providerID: testProviderID}
	first := &authFlow{
		id:                 "first",
		sessionID:          testSessionID,
		session:            session,
		providerID:         testProviderID,
		authorizeRequestID: "request-1",
		nativeSessionID:    "native-first",
		state:              authStatePending,
		disarm:             make(chan struct{}),
	}
	second := &authFlow{
		id:                 "second",
		sessionID:          testSessionID,
		providerID:         testProviderID,
		authorizeRequestID: "request-2",
		state:              authStatePending,
		disarm:             make(chan struct{}),
	}

	agent.providerAuth.flows[key] = first
	agent.providerAuth.byID[first.id] = first
	agent.providerAuth.retained[key] = first

	if err := agent.providerAuth.publishFlow(context.Background(), session, key, second); err != nil {
		t.Fatalf("publish successor: %v", err)
	}

	if !agent.providerAuth.requestRetired(key, first.authorizeRequestID) {
		t.Fatal("predecessor request was not retired")
	}
	if first.state != authStateCancelled || first.reason != authReasonSuperseded {
		t.Fatalf("predecessor = %s/%s", first.state, first.reason)
	}
	if len(client.authCancelled) != 1 || client.authCancelled[0] != "native-first" {
		t.Fatalf("native cancellations = %#v", client.authCancelled)
	}

	agent.providerAuth.closeSession(context.Background(), testSessionID)
	third := &authFlow{disarm: make(chan struct{})}
	if err := agent.providerAuth.publishFlow(context.Background(), session, key, third); err == nil {
		t.Fatal("closed session accepted a flow")
	}
}

func TestClaimFlowRejectsBusyAndTerminalRecords(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	busy := &authFlow{
		id:         "busy",
		providerID: testProviderID,
		method:     authCatalogMethod{ID: nativehermes.AuthFlowPKCE},
		state:      authStatePending,
		claimed:    true,
	}
	requireAuthCause(t, agent.providerAuth.claimFlow(busy), authCauseFlowState)

	busy.claimed = false
	busy.state = authStateCancelled
	requireAuthCause(t, agent.providerAuth.claimFlow(busy), authCauseFlowState)
}

func TestAuthorizeFailsClosedAcrossAdmissionAndPersistenceBoundaries(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)
	params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request")
	raw := authRawParams(t, params)
	key := authFlowKey{sessionID: testSessionID, providerID: testProviderID}

	admissionRelease, ok := agent.providerAuth.admit(context.Background(), key)
	if !ok {
		t.Fatal("hold admission gate")
	}
	_, err := agent.providerAuth.authorize(endedAuthContext(), raw)
	requireAuthCause(t, err, authCauseTimeout)
	admissionRelease()

	providerRelease, ok := agent.providerAuth.lockProvider(context.Background(), testProviderID)
	if !ok {
		t.Fatal("hold provider gate")
	}
	_, err = agent.providerAuth.authorize(endedAuthContext(), raw)
	requireAuthCause(t, err, authCauseTimeout)
	providerRelease()

	originalRand := authRandRead
	authRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	_, err = agent.providerAuth.authorize(context.Background(), raw)
	requireAuthCause(t, err, authCauseProcess)
	authRandRead = originalRand

	ledgerRename = func(string, string) error { return errors.New("rename") }
	_, err = agent.providerAuth.authorize(context.Background(), raw)
	requireAuthCause(t, err, authCauseProcess)
}

func TestAuthorizeRejectsMalformedAndRetiredRequests(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	for _, field := range []string{
		authFieldSessionID,
		authFieldProviderID,
		authFieldConnectionID,
		authFieldMethodsGeneration,
		authFieldMethod,
		authFieldAuthorizeRequestID,
	} {
		params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request")
		delete(params, field)

		_, err := callLeg(t, agent, AuthAuthorizeMethod, params)
		requireInvalidField(t, err, field)
	}

	params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "retired")
	key := authFlowKey{sessionID: testSessionID, providerID: testProviderID}
	agent.providerAuth.mu.Lock()
	agent.providerAuth.retire(key, "retired")
	agent.providerAuth.mu.Unlock()

	_, err := callLeg(t, agent, AuthAuthorizeMethod, params)
	requireInvalidField(t, err, authFieldAuthorizeRequestID)
}

func TestAuthorizeReplaysTerminalMintFailure(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)
	client.authStartErr = errors.New("transport")
	params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request")

	_, firstErr := callLeg(t, agent, AuthAuthorizeMethod, params)
	requireAuthCause(t, firstErr, authCauseTransport)

	_, replayErr := callLeg(t, agent, AuthAuthorizeMethod, params)
	requireAuthCause(t, replayErr, authCauseTransport)
}

func TestRecordAuthorizeIntentRejectsUnreadableLedgerAndCanceledGate(t *testing.T) {
	restoreLedgerHooks(t)

	agent, _ := newAuthAgent(t)
	request := authorizeRequest{
		providerID:         testProviderID,
		connectionID:       testConnectionID,
		method:             nativehermes.AuthFlowDeviceCode,
		authorizeRequestID: "request",
	}

	ledgerRelease, ok := agent.providerAuth.lockLedger(context.Background(), testProviderID)
	if !ok {
		t.Fatal("hold ledger gate")
	}
	_, err := agent.providerAuth.recordAuthorizeIntent(endedAuthContext(), request, "flow", time.Now())
	requireAuthCause(t, err, authCauseTimeout)
	ledgerRelease()

	path := agent.providerAuth.ledger.path(testProviderID)
	if writeErr := os.WriteFile(path, []byte("{"), 0o600); writeErr != nil {
		t.Fatalf("write corrupt ledger: %v", writeErr)
	}

	_, err = agent.providerAuth.recordAuthorizeIntent(context.Background(), request, "flow", time.Now())
	requireAuthCause(t, err, authCauseProcess)
}

func TestFlowExpiryAndCancellationNoGatewayPaths(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)
	flow := agent.providerAuth.byID[presentation.FlowID]

	agent.providerAuth.expire(flow)
	agent.providerAuth.expire(flow)
	if flow.state != authStateExpired || flow.reason != authReasonDeadline {
		t.Fatalf("expired flow = %s/%s", flow.state, flow.reason)
	}

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}
	agent.providerAuth.cancelNativeFlow(context.Background(), session, &authFlow{})

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()
	agent.providerAuth.cancelNativeFlow(context.Background(), session, &authFlow{nativeSessionID: "native"})
}

func TestCallbackRejectsMalformedAddressingAndUnavailableGateway(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startPKCEFlow(t, agent, client)
	base := map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": "anthropic",
		"method":     nativehermes.AuthFlowPKCE,
		"flowId":     presentation.FlowID,
		"input":      "code",
	}

	for _, field := range []string{
		authFieldSessionID,
		authFieldProviderID,
		authFieldMethod,
		authFieldFlowID,
		authFieldInput,
	} {
		params := make(map[string]any, len(base))
		for key, value := range base {
			params[key] = value
		}
		delete(params, field)

		_, err := callLeg(t, agent, AuthCallbackMethod, params)
		requireInvalidField(t, err, field)
	}

	wrongMethod := make(map[string]any, len(base))
	for key, value := range base {
		wrongMethod[key] = value
	}
	wrongMethod["method"] = nativehermes.AuthFlowDeviceCode
	_, err := callLeg(t, agent, AuthCallbackMethod, wrongMethod)
	requireInvalidField(t, err, authFieldMethod)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}
	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	_, err = callLeg(t, agent, AuthCallbackMethod, base)
	requireAuthCause(t, err, authCauseTransport)
}

func TestAddressedFlowLegRejectsEveryAddressingFailure(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)
	base := map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": testProviderID,
		"flowId":     presentation.FlowID,
	}

	for _, field := range []string{authFieldSessionID, authFieldProviderID, authFieldFlowID} {
		params := make(map[string]any, len(base))
		for key, value := range base {
			params[key] = value
		}
		delete(params, field)

		_, err := callLeg(t, agent, AuthStatusMethod, params)
		requireInvalidField(t, err, field)
	}

	crossProvider := map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic", "flowId": presentation.FlowID,
	}
	_, err := callLeg(t, agent, AuthStatusMethod, crossProvider)
	requireInvalidField(t, err, authFieldFlowID)

	_, err = callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": "missing", "providerId": testProviderID, "flowId": presentation.FlowID,
	})
	if err == nil {
		t.Fatal("unknown session accepted")
	}
}

func TestCompletionFailurePathsRemainValuesFree(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)
	flow := agent.providerAuth.byID[presentation.FlowID]
	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()
	requireAuthCause(t, agent.providerAuth.completeFlow(context.Background(), session, flow), authCauseTransport)

	agent, client = newAuthAgent(t)
	presentation = startDeviceFlow(t, agent, client)
	flow = agent.providerAuth.byID[presentation.FlowID]
	session, err = agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}

	ledgerRename = func(string, string) error { return errors.New("rename") }
	requireAuthCause(t, agent.providerAuth.completeFlow(context.Background(), session, flow), authCauseProcess)
}

func TestTerminalAndAbandonedFlowDecisions(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	flow := &authFlow{
		id:         "flow",
		sessionID:  testSessionID,
		providerID: testProviderID,
		method:     authCatalogMethod{ID: nativehermes.AuthFlowDeviceCode},
		state:      authStatePending,
		disarm:     make(chan struct{}),
	}

	if cause, abandoned := agent.providerAuth.abandonedCause(flow); cause != "" || abandoned {
		t.Fatalf("pending flow abandoned as %q/%v", cause, abandoned)
	}

	agent.providerAuth.terminalize(flow, authStateCancelled, authReasonOwnerCancel)
	agent.providerAuth.terminalize(flow, authStateAuthenticated, "")

	if cause, abandoned := agent.providerAuth.abandonedCause(flow); cause != authCauseFlowCancelled || !abandoned {
		t.Fatalf("cancelled flow = %q/%v", cause, abandoned)
	}
	requireAuthCause(t, agent.providerAuth.failSettled(context.Background(), flow, authCauseTransport, true), authCauseFlowCancelled)

	failed := *flow
	failed.state = authStateFailed
	if cause, abandoned := agent.providerAuth.abandonedCause(&failed); cause != authCauseFlowState || !abandoned {
		t.Fatalf("failed flow = %q/%v", cause, abandoned)
	}
}

func TestLedgerLineageComparisonAndReadFailure(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)
	flow := agent.providerAuth.byID[presentation.FlowID]

	if authLedgerAdvancedPast(
		authLedgerRecord{BindingGeneration: 1, Revision: 1},
		authLedgerRecord{BindingGeneration: 2, Revision: 1},
	) {
		t.Fatal("older binding generation advanced past newer")
	}

	path := agent.providerAuth.ledger.path(testProviderID)
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatalf("write corrupt ledger: %v", err)
	}
	if cause := agent.providerAuth.lineageCause(flow); cause != authCauseProcess {
		t.Fatalf("lineage cause = %q", cause)
	}
}

func TestProviderAuthResidenceValidationAndPreparationFailures(t *testing.T) {
	if err := validateProviderAuthRoots(Options{
		ProviderAuthRoot: t.TempDir(),
		SharedHermesHome: "relative",
	}); err == nil {
		t.Fatal("relative provider auth residence accepted")
	}

	if _, err := prepareProviderAuthResidence("relative"); err == nil {
		t.Fatal("relative provider auth residence prepared")
	}

	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := prepareProviderAuthResidence(filepath.Join(file, "child")); err == nil {
		t.Fatal("provider auth residence beneath a file prepared")
	}

	agent := newTestAgent(
		WithProviderAuthRoot(t.TempDir()),
		WithSharedHermesHome(filepath.Join(file, "child")),
	)
	if agent.providerAuth != nil {
		t.Fatal("unusable provider auth residence advertised")
	}

	if _, err := newAuthLedger(Options{
		ProviderAuthRoot: t.TempDir(),
		SharedHermesHome: "relative",
	}); err == nil {
		t.Fatal("ledger accepted relative provider auth residence")
	}
}

func TestAuthMintRequiresLiveNativeClientAndCompleterDisarmsOnce(t *testing.T) {
	flow := &authFlow{
		id:            "flow",
		expiresAt:     time.Now().Add(time.Minute),
		probeInterval: time.Second,
		method:        authCatalogMethod{Label: "Login"},
		disarm:        make(chan struct{}),
	}

	_, cause := (&providerAuth{}).buildMint(t.Context(), &session{}, flow)
	if cause != authCauseTransport {
		t.Fatalf("mint cause = %q", cause)
	}

	flow.stopCompleter()
	flow.stopCompleter()
}

func TestAuthSessionRejectsBrokerTombstone(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	agent.providerAuth.mu.Lock()
	agent.providerAuth.closedSessions[testSessionID] = struct{}{}
	agent.providerAuth.mu.Unlock()

	if _, err := agent.providerAuth.authSession(string(testSessionID)); err == nil {
		t.Fatal("tombstoned session accepted")
	}
}

func TestPublishFlowRejectsEndedSessionLifetime(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}

	session.mu.Lock()
	session.closed = true
	session.mu.Unlock()

	flow := &authFlow{id: "flow", state: authStatePending, disarm: make(chan struct{})}
	err = agent.providerAuth.publishFlow(
		context.Background(),
		session,
		authFlowKey{sessionID: testSessionID, providerID: testProviderID},
		flow,
	)
	if err == nil {
		t.Fatal("ended session lifetime accepted a flow")
	}
}

func TestStatusProbeWithoutGatewayLeavesPending(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)
	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}
	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	result, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if mustType[authStatusResult](t, result).State != authStatePending {
		t.Fatal("gateway loss terminalized pending status")
	}
}

func TestAuthFailureRetriabilityIsClosed(t *testing.T) {
	t.Parallel()

	for cause, want := range map[string]bool{
		authCauseTransport:       true,
		authCauseProcess:         true,
		authCauseTimeout:         true,
		authCauseProviderRefused: false,
	} {
		if got := authCauseRetryable(cause); got != want {
			t.Fatalf("%s retryable = %v", cause, got)
		}
	}

	var requestErr *acp.RequestError
	if !errors.As(authFailed(authCauseTransport, "", "", ""), &requestErr) {
		t.Fatal("auth failure did not produce request error")
	}
}

func TestAuthorizeOuterValidationAndCloseRaces(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	_, err := callLeg(t, agent, AuthAuthorizeMethod, map[string]any{
		"sessionId": string(testSessionID), "extra": true,
	})
	requireInvalidField(t, err, "extra")

	_, err = callLeg(t, agent, AuthAuthorizeMethod, map[string]any{
		"sessionId": "missing", "providerId": testProviderID,
		"connectionId": testConnectionID, "methodsGeneration": generation,
		"method": nativehermes.AuthFlowDeviceCode, "authorizeRequestId": "request",
	})
	if err == nil {
		t.Fatal("unknown session accepted")
	}

	invalidInputs := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "invalid-inputs")
	invalidInputs["inputs"] = "bad"
	_, err = callLeg(t, agent, AuthAuthorizeMethod, invalidInputs)
	requireInvalidField(t, err, authFieldInputs)

	// Hold the ledger rename after session lookup and close before publication.
	recording := make(chan struct{})
	resume := make(chan struct{})
	originalRename := ledgerRename
	ledgerRename = func(from string, to string) error {
		close(recording)
		<-resume

		return originalRename(from, to)
	}

	settled := make(chan error, 1)
	go func() {
		_, authorizeErr := agent.providerAuth.authorize(
			context.Background(),
			authRawParams(t, authorizeParams(
				generation,
				testProviderID,
				nativehermes.AuthFlowDeviceCode,
				"close-before-publish",
			)),
		)
		settled <- authorizeErr
	}()

	<-recording
	agent.providerAuth.closeSession(context.Background(), testSessionID)
	close(resume)
	if err := <-settled; err == nil {
		t.Fatal("flow published after session close")
	}
}

func TestAuthorizeMintOutlivingCloseCancelsNativeFlow(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)
	starting := make(chan struct{})
	resume := make(chan struct{})
	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		close(starting)
		<-resume

		return nativehermes.AuthStart{
			SessionID: "native-late",
			Flow:      nativehermes.AuthFlowDeviceCode,
			URL:       testDeviceURL,
		}, nil
	}

	settled := make(chan error, 1)
	go func() {
		_, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
			generation,
			testProviderID,
			nativehermes.AuthFlowDeviceCode,
			"request",
		))
		settled <- err
	}()

	<-starting
	agent.providerAuth.closeSession(context.Background(), testSessionID)
	close(resume)
	requireAuthCause(t, <-settled, authCauseFlowCancelled)

	if len(client.authCancelled) != 1 || client.authCancelled[0] != "native-late" {
		t.Fatalf("late native flow was not cancelled: %#v", client.authCancelled)
	}
}

func TestAuthorizeSuccessorAdvancesRevision(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	first := startDeviceFlow(t, agent, client)

	client.authStart.SessionID = "native-second"
	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
		agent.providerAuth.generation,
		testProviderID,
		nativehermes.AuthFlowDeviceCode,
		"request-2",
	))
	if err != nil {
		t.Fatalf("authorize successor: %v", err)
	}

	second := mustType[authAuthorizeResult](t, result)
	if second.FlowID == first.FlowID {
		t.Fatal("successor reused flow id")
	}

	record, ok, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil || !ok || record.Revision != 2 {
		t.Fatalf("successor lineage = %#v/%v/%v", record, ok, err)
	}
}

func TestReplayAuthorizeHonorsCanceledWaiter(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	key := authFlowKey{sessionID: testSessionID, providerID: testProviderID}
	agent.providerAuth.retained[key] = &authFlow{
		id:                 "flow",
		providerID:         testProviderID,
		authorizeRequestID: "request",
		method:             authCatalogMethod{ID: nativehermes.AuthFlowDeviceCode},
		ready:              make(chan struct{}),
	}

	if _, ok, err := agent.providerAuth.replayAuthorize(endedAuthContext(), key, "request"); !ok || err == nil {
		t.Fatalf("canceled replay = %v/%v", ok, err)
	}
}

func TestNativePresentationValidationBranches(t *testing.T) {
	t.Parallel()

	mint := authMint{expiresAt: time.Now().Add(time.Minute)}
	method := authCatalogMethod{Flow: nativehermes.AuthFlowDeviceCode}

	if _, cause := applyNativeStart(mint, nativehermes.AuthStart{
		Flow: nativehermes.AuthFlowDeviceCode,
		URL:  "not-a-url",
	}, method); cause != authCauseNativeVeto {
		t.Fatalf("invalid URL cause = %q", cause)
	}

	if _, cause := applyNativeStart(mint, nativehermes.AuthStart{
		Flow:     nativehermes.AuthFlowDeviceCode,
		URL:      testDeviceURL,
		UserCode: "bad code",
	}, method); cause != authCauseNativeVeto {
		t.Fatalf("invalid user code cause = %q", cause)
	}
}

func TestFlowCompleterExpiresAtDeadline(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)
	client.authStart = nativehermes.AuthStart{
		SessionID: "native-short",
		Flow:      nativehermes.AuthFlowDeviceCode,
		URL:       testDeviceURL,
		ExpiresIn: time.Millisecond,
	}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
		generation,
		testProviderID,
		nativehermes.AuthFlowDeviceCode,
		"short",
	))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	flow := agent.providerAuth.byID[mustType[authAuthorizeResult](t, result).FlowID]
	deadline := time.Now().Add(time.Second)
	state := authStatePending
	for state == authStatePending && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)

		agent.providerAuth.mu.Lock()
		state = flow.state
		agent.providerAuth.mu.Unlock()
	}
	if state != authStateExpired {
		t.Fatalf("deadline left flow %q", state)
	}
}

func TestCallbackAdditionalClosedBranches(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startPKCEFlow(t, agent, client)

	_, err := callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic",
		"method": nativehermes.AuthFlowPKCE, "flowId": presentation.FlowID,
		"input": "code", "extra": true,
	})
	requireInvalidField(t, err, "extra")

	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": "missing", "providerId": "anthropic",
		"method": nativehermes.AuthFlowPKCE, "flowId": presentation.FlowID, "input": "code",
	})
	if err == nil {
		t.Fatal("unknown callback session accepted")
	}

	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic",
		"method": nativehermes.AuthFlowPKCE, "flowId": "missing", "input": "code",
	})
	requireInvalidField(t, err, authFieldFlowID)

	flow := agent.providerAuth.byID[presentation.FlowID]
	flow.claimed = true
	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic",
		"method": nativehermes.AuthFlowPKCE, "flowId": presentation.FlowID, "input": "code",
	})
	requireAuthCause(t, err, authCauseFlowState)
	flow.claimed = false

	_, err = callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic",
		"method": nativehermes.AuthFlowPKCE, "flowId": presentation.FlowID,
		"input": string(make([]byte, authMaxCallbackBytes+1)),
	})
	requireInvalidField(t, err, authFieldInput)
}

func TestCallbackPollAndCompletionErrorsStayClosed(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startPKCEFlow(t, agent, client)
	client.authPollErr = errors.New("poll")
	_, err := callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic",
		"method": nativehermes.AuthFlowPKCE, "flowId": presentation.FlowID, "input": "code",
	})
	requireAuthCause(t, err, authCauseTransport)

	agent, client = newAuthAgent(t)
	presentation = startPKCEFlow(t, agent, client)
	flow := agent.providerAuth.byID[presentation.FlowID]
	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollApproved}
	agent.providerAuth.mu.Lock()
	flow.state = authStateFailed
	agent.providerAuth.mu.Unlock()

	_, err = agent.providerAuth.submitCode(
		context.Background(),
		mustSession(t, agent),
		flow,
		"code",
	)
	requireAuthCause(t, err, authCauseFlowState)
}

func mustSession(t *testing.T, agent *Agent) *session {
	t.Helper()

	session, err := agent.session(testSessionID)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	return session
}

func TestCompleteFlowGateAndLineageFailures(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)
	flow := agent.providerAuth.byID[presentation.FlowID]
	session := mustSession(t, agent)

	providerRelease, ok := agent.providerAuth.lockProvider(context.Background(), testProviderID)
	if !ok {
		t.Fatal("hold provider gate")
	}
	err := agent.providerAuth.completeFlow(endedAuthContext(), session, flow)
	requireAuthCause(t, err, authCauseTimeout)
	providerRelease()

	ledgerRelease, ok := agent.providerAuth.lockLedger(context.Background(), testProviderID)
	if !ok {
		t.Fatal("hold ledger gate")
	}
	err = agent.providerAuth.completeFlow(endedAuthContext(), session, flow)
	requireAuthCause(t, err, authCauseTimeout)
	ledgerRelease()

	record, present, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil || !present {
		t.Fatalf("read lineage: %v/%v", present, err)
	}
	record.Revision++
	if err := agent.providerAuth.ledger.write(record); err != nil {
		t.Fatalf("advance lineage: %v", err)
	}

	requireAuthCause(
		t,
		agent.providerAuth.completeFlow(context.Background(), session, flow),
		authCauseBindingConflict,
	)
}

func TestAddressedFlowLegRejectsUnknownField(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	_, _, err := agent.providerAuth.addressedFlowLeg(authRawParams(t, map[string]any{
		"sessionId": string(testSessionID), "extra": true,
	}))
	requireInvalidField(t, err, "extra")
}

func TestCloseSessionSkipsPeerAndTerminalFlows(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	live := startDeviceFlow(t, agent, client)
	liveFlow := agent.providerAuth.byID[live.FlowID]

	peerKey := authFlowKey{sessionID: "peer", providerID: "peer-provider"}
	peer := &authFlow{id: "peer-flow", state: authStatePending, disarm: make(chan struct{})}
	agent.providerAuth.flows[peerKey] = peer
	agent.providerAuth.byID[peer.id] = peer
	agent.providerAuth.retained[peerKey] = peer

	agent.providerAuth.terminalize(liveFlow, authStateAuthenticated, "")
	agent.providerAuth.closeSession(context.Background(), testSessionID)

	if agent.providerAuth.byID[peer.id] == nil {
		t.Fatal("peer flow removed")
	}
}

func TestProviderAuthResidenceHookFailures(t *testing.T) {
	restoreLedgerHooks(t)

	home := filepath.Join(t.TempDir(), "auth")
	ledgerChmod = func(string, os.FileMode) error { return errors.New("chmod") }
	if _, err := prepareProviderAuthResidence(home); err == nil {
		t.Fatal("provider auth residence chmod failure accepted")
	}

	restoreLedgerHooks(t)
	ledgerEvalPath = func(string) (string, error) { return "", errors.New("resolve") }
	if _, err := prepareProviderAuthResidence(home); err == nil {
		t.Fatal("provider auth residence resolution failure accepted")
	}
}
