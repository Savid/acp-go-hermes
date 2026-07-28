package hermesacp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// admissionRendezvous bounds the wait a blocked leg makes for the leg it is
// racing. Every test here pins an interleaving that the admission discipline
// makes impossible, so the leg that is meant to arrive second may never arrive
// at all: once it is queued behind the gate, the bound is what lets the holder
// finish and hand the gate over. No assertion depends on it — it only decides
// how long the serialized ordering takes.
const admissionRendezvous = time.Second

// authLeg calls one provider-auth leg off the test goroutine, where t.Fatalf is
// not usable.
func authLeg(agent *Agent, method string, params map[string]any) (any, error) {
	encoded, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	return agent.HandleExtensionMethod(context.Background(), method, encoded)
}

// authRawParams encodes a leg's params for a broker call that needs its own
// context.
func authRawParams(t *testing.T, params map[string]any) json.RawMessage {
	t.Helper()

	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	return encoded
}

// endedRequest is the context of a leg whose caller went away, which is how a
// leg queued behind a gate leaves without ever holding it.
func endedRequest(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return ctx
}

// TestTwoCredentialLegsHarvestTheSlotOnce runs the two harvests a host can
// issue for one flow at the same time. The leg reads four records between
// admitting itself and recording that it harvested, and a check-then-set that
// wide hands both legs the same live access and refresh material — with no data
// race to find, because every field access on the record is itself locked.
func TestTwoCredentialLegsHarvestTheSlotOnce(t *testing.T) {
	allowRefreshHarvest(t, testProviderID)

	agent, client := newAuthAgent(t)
	flowID := completedFlow(t, agent, client)

	params := map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID,
	}

	inside := make(chan struct{})
	release := make(chan struct{})

	var reads atomic.Int32

	original := authReadSlot
	authReadSlot = func(home string, providerID string, label string) (nativehermes.AuthMaterial, bool, error) {
		if reads.Add(1) == 1 {
			close(inside)
			<-release
		}

		return original(home, providerID, label)
	}

	t.Cleanup(func() { authReadSlot = original })

	settled := make(chan error, 1)

	go func() {
		_, err := authLeg(agent, AuthCredentialMethod, params)
		settled <- err
	}()

	<-inside

	_, secondErr := callLeg(t, agent, AuthCredentialMethod, params)

	close(release)

	firstErr := <-settled

	if firstErr == nil && secondErr == nil {
		t.Fatal("two concurrent credential legs both harvested the same reserved slot")
	}

	if firstErr != nil && secondErr != nil {
		t.Fatalf("neither credential leg harvested: %v / %v", firstErr, secondErr)
	}

	if got := reads.Load(); got != 1 {
		t.Fatalf("reserved slot reads = %d, want the refused leg to stop before the slot", got)
	}
}

// TestDisconnectSurvivesACompletionAdmittedBeforeIt runs the disconnect a host
// issues while an operator-key callback is still inside its native write. The
// removal proves absence and records it, and the write then refills the slot:
// the ledger says removed, inventory skips removed records, and the credential
// is live at the provider and invisible on every host surface.
func TestDisconnectSurvivesACompletionAdmittedBeforeIt(t *testing.T) {
	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	writing := make(chan struct{})
	verified := make(chan struct{})

	var writes atomic.Int32

	originalWrite := authWriteSlot
	authWriteSlot = func(home string, providerID string, label string, material nativehermes.AuthMaterial) error {
		if writes.Add(1) == 1 {
			close(writing)

			select {
			case <-verified:
			case <-time.After(admissionRendezvous):
			}
		}

		return originalWrite(home, providerID, label, material)
	}

	originalPresent := authSlotPresent
	authSlotPresent = func(home string, providerID string, label string) (bool, error) {
		present, presentErr := originalPresent(home, providerID, label)

		close(verified)

		return present, presentErr
	}

	t.Cleanup(func() {
		authWriteSlot = originalWrite
		authSlotPresent = originalPresent
	})

	flowID := mustType[authAuthorizeResult](t, result).FlowID

	settled := make(chan error, 1)

	go func() {
		_, callbackErr := authLeg(agent, AuthCallbackMethod, map[string]any{
			"sessionId": string(testSessionID), "providerId": "openai",
			"method": authAPIKeyMethodID, "flowId": flowID, "input": "sk-operator-key",
		})
		settled <- callbackErr
	}()

	<-writing

	if _, disconnectErr := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai",
		"connectionId": testConnectionID, "bindingGeneration": 1,
	}); disconnectErr != nil {
		t.Fatalf("disconnect: %v", disconnectErr)
	}

	<-settled

	present, err := nativehermes.AuthSlotPresent(client.xdg.Root, "openai", nativehermes.AuthSlotLabel(testConnectionID))
	if err != nil {
		t.Fatalf("slot present: %v", err)
	}

	record, ok, err := agent.providerAuth.ledger.read("openai")
	if err != nil || !ok {
		t.Fatalf("ledger read: %v present=%v", err, ok)
	}

	if present && record.State == authLedgerRemoved {
		t.Fatal("a successful disconnect left the credential resident under a removed ledger entry")
	}

	if present {
		t.Fatalf("the reserved slot outlived its disconnect: ledger = %#v", record)
	}
}

// TestTwoCallbacksApplyOneSecret runs the two callbacks a host can issue for
// one flow. Both are admitted on the same pending read, both write a different
// operator key into the one reserved slot, and the flow reports the first one
// saved while the last write is what is resident.
func TestTwoCallbacksApplyOneSecret(t *testing.T) {
	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	flowID := mustType[authAuthorizeResult](t, result).FlowID

	writing := make(chan struct{})
	release := make(chan struct{})

	var writes atomic.Int32

	originalWrite := authWriteSlot
	authWriteSlot = func(home string, providerID string, label string, material nativehermes.AuthMaterial) error {
		if writes.Add(1) == 1 {
			close(writing)
			<-release
		}

		return originalWrite(home, providerID, label, material)
	}

	t.Cleanup(func() { authWriteSlot = originalWrite })

	settled := make(chan error, 1)

	go func() {
		_, callbackErr := authLeg(agent, AuthCallbackMethod, map[string]any{
			"sessionId": string(testSessionID), "providerId": "openai",
			"method": authAPIKeyMethodID, "flowId": flowID, "input": "sk-first",
		})
		settled <- callbackErr
	}()

	<-writing

	_, secondErr := callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai",
		"method": authAPIKeyMethodID, "flowId": flowID, "input": "sk-second",
	})

	close(release)

	firstErr := <-settled

	if firstErr == nil && secondErr == nil {
		t.Fatal("two callbacks for one flow both reported the secret saved")
	}

	if got := writes.Load(); got != 1 {
		t.Fatalf("native slot writes = %d, want only the admitted callback to write", got)
	}

	material, present, err := nativehermes.AuthReadSlot(client.xdg.Root, "openai", nativehermes.AuthSlotLabel(testConnectionID))
	if err != nil || !present {
		t.Fatalf("reserved slot after both callbacks: %v present=%v", err, present)
	}

	saved := "sk-first"
	if firstErr != nil {
		saved = "sk-second"
	}

	if material.AccessToken != saved {
		t.Fatalf("resident key = %q, want the one the surface reported saved (%q)", material.AccessToken, saved)
	}
}

// TestTwoProvidersKeepEachOthersCredentialSlot runs two providers' callbacks
// against the one native store a session owns. Every mutation of that store is
// a read-modify-write of the whole document, so two legs that read the same
// original and rename their own complete replacement leave only the last one's
// slot — while both report the key saved.
func TestTwoProvidersKeepEachOthersCredentialSlot(t *testing.T) {
	agent, client := newAuthAgent(t)

	writeStoreFixture(t, client.xdg.Root, map[string]any{})

	client.authKeyProviders = []nativehermes.AuthAPIKeyProvider{{ID: "openai", Name: "OpenAI"}, {ID: "mistral", Name: "Mistral"}}

	methods, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("methods: %v", err)
	}

	generation := mustType[authMethodsResult](t, methods).Generation

	flows := map[string]string{}

	for _, providerID := range []string{"openai", "mistral"} {
		result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, providerID, authAPIKeyMethodID, "r-"+providerID))
		if err != nil {
			t.Fatalf("authorize %s: %v", providerID, err)
		}

		flows[providerID] = mustType[authAuthorizeResult](t, result).FlowID
	}

	writing := make(chan struct{})
	peerSettled := make(chan struct{})
	settled := make(chan error, 1)

	var writes atomic.Int32

	originalWrite := authWriteSlot
	authWriteSlot = func(home string, providerID string, label string, material nativehermes.AuthMaterial) error {
		// The native write is a read-modify-write of the whole document. Hold
		// this leg's pre-image across the window and commit it, which is what
		// two unserialized calls into the native write do to each other.
		path := filepath.Join(home, "auth.json")

		preimage, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		if writes.Add(1) == 1 {
			close(writing)

			select {
			case <-peerSettled:
			case <-time.After(admissionRendezvous):
			}
		}

		if err := os.WriteFile(path, preimage, 0o600); err != nil {
			return err
		}

		return originalWrite(home, providerID, label, material)
	}

	t.Cleanup(func() { authWriteSlot = originalWrite })

	go func() {
		_, err := authLeg(agent, AuthCallbackMethod, map[string]any{
			"sessionId": string(testSessionID), "providerId": "openai",
			"method": authAPIKeyMethodID, "flowId": flows["openai"], "input": "sk-openai",
		})
		settled <- err
	}()

	<-writing

	if _, err := callLeg(t, agent, AuthCallbackMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "mistral",
		"method": authAPIKeyMethodID, "flowId": flows["mistral"], "input": "sk-mistral",
	}); err != nil {
		t.Fatalf("mistral callback: %v", err)
	}

	close(peerSettled)

	if err := <-settled; err != nil {
		t.Fatalf("openai callback: %v", err)
	}

	for _, providerID := range []string{"openai", "mistral"} {
		present, err := nativehermes.AuthSlotPresent(client.xdg.Root, providerID, nativehermes.AuthSlotLabel(testConnectionID))
		if err != nil {
			t.Fatalf("slot present %s: %v", providerID, err)
		}

		if !present {
			t.Fatalf("%s reported its key saved and lost the slot to the other provider's write", providerID)
		}
	}
}

// TestConcurrentIdenticalAuthorizesMintOneFlow runs the same idempotency key
// twice at once. The replay check runs before either request has published its
// flow, so both miss each other, both mint a login at the provider, and the
// loser's flow is superseded and cancelled under the caller it was handed to.
func TestConcurrentIdenticalAuthorizesMintOneFlow(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	var starts atomic.Int32

	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		starts.Add(1)

		return nativehermes.AuthStart{
			SessionID: "native-flow", Flow: nativehermes.AuthFlowDeviceCode,
			URL: testDeviceURL, UserCode: "ABCD-EFGH",
			PollInterval: 3 * time.Second, ExpiresIn: 30 * time.Minute,
		}, nil
	}

	recording := make(chan struct{})
	settled := make(chan struct{})

	var renames atomic.Int32

	originalRename := ledgerRename
	ledgerRename = func(from string, to string) error {
		if renames.Add(1) == 1 {
			close(recording)

			select {
			case <-settled:
			case <-time.After(admissionRendezvous):
			}
		}

		return originalRename(from, to)
	}

	params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1")

	type outcome struct {
		result any
		err    error
	}

	first := make(chan outcome, 1)

	go func() {
		result, err := authLeg(agent, AuthAuthorizeMethod, params)
		first <- outcome{result, err}
	}()

	<-recording

	second, secondErr := callLeg(t, agent, AuthAuthorizeMethod, params)

	close(settled)

	admitted := <-first

	if admitted.err != nil || secondErr != nil {
		t.Fatalf("concurrent identical authorizes failed: %v / %v", admitted.err, secondErr)
	}

	if got := starts.Load(); got != 1 {
		t.Fatalf("native logins started = %d, want one per idempotency key", got)
	}

	if mustType[authAuthorizeResult](t, admitted.result).FlowID != mustType[authAuthorizeResult](t, second).FlowID {
		t.Fatalf("one idempotency key minted two flows: %#v / %#v", admitted.result, second)
	}
}

// TestAuthorizeAdmittedBeforeCloseMintsNothingAfterIt runs an authorize that
// passed session lookup and has not yet published its flow when the session
// closes. Close takes its cleanup set and cancels what it finds; a flow that
// publishes afterwards is in no such set, and the login it then starts outlives
// the session that asked for it.
func TestAuthorizeAdmittedBeforeCloseMintsNothingAfterIt(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	var starts atomic.Int32

	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		starts.Add(1)

		return nativehermes.AuthStart{
			SessionID: "native-flow", Flow: nativehermes.AuthFlowDeviceCode, URL: testDeviceURL,
		}, nil
	}

	recording := make(chan struct{})
	closed := make(chan struct{})

	var renames atomic.Int32

	originalRename := ledgerRename
	ledgerRename = func(from string, to string) error {
		if renames.Add(1) == 1 {
			close(recording)
			<-closed
		}

		return originalRename(from, to)
	}

	settled := make(chan error, 1)

	go func() {
		_, err := authLeg(agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1"))
		settled <- err
	}()

	<-recording

	agent.providerAuth.closeSession(context.Background(), testSessionID)

	close(closed)

	if err := <-settled; err == nil {
		t.Fatal("an authorize published a flow into a session that had already closed")
	}

	if got := starts.Load(); got != 0 {
		t.Fatalf("native logins started after close = %d", got)
	}

	_, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	requireInvalidField(t, err, jsonFieldSessionID)

	broker := agent.providerAuth

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if len(broker.flows) != 0 || len(broker.byID) != 0 || len(broker.retained) != 0 {
		t.Fatalf("a closed session kept records: %d pending, %d addressable, %d retained", len(broker.flows), len(broker.byID), len(broker.retained))
	}
}

// TestARetiredAuthorizeRequestIDLeavesItsSuccessorAlone replays an idempotency
// key a later authorize already replaced. Only the newest key is answerable, so
// the retry has nothing to replay — and minting in its place destroys the live
// flow it never named, which is the one thing an idempotency key exists to
// prevent.
func TestARetiredAuthorizeRequestIDLeavesItsSuccessorAlone(t *testing.T) {
	agent, client := newAuthAgent(t)

	startDeviceFlow(t, agent, client)

	generation := agent.providerAuth.generation

	second, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-2"))
	if err != nil {
		t.Fatalf("second authorize: %v", err)
	}

	successor := mustType[authAuthorizeResult](t, second).FlowID

	if _, retryErr := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1")); retryErr == nil {
		t.Fatal("a retired authorizeRequestId was answered with a fresh login")
	}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": successor,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if state := mustType[authStatusResult](t, status).State; state != authStatePending {
		t.Fatalf("the successor flow is %q, want the retired retry to have left it pending", state)
	}

	// A retired key reaches no further than the session that minted it.
	agent.providerAuth.closeSession(context.Background(), testSessionID)

	broker := agent.providerAuth

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if len(broker.retired) != 0 {
		t.Fatalf("a closed session kept %d retired key sets", len(broker.retired))
	}
}

// TestStatusSkipsTheProbeWhileACallbackHoldsTheFlow runs the host's status poll
// against a flow whose callback is already inside the native poll route. Both
// legs complete the same flow, and an unserialized completion migrates the one
// native entry twice: the second migration finds nothing left to move and
// settles a flow the first one had already completed.
func TestStatusSkipsTheProbeWhileACallbackHoldsTheFlow(t *testing.T) {
	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	client.authStart = nativehermes.AuthStart{SessionID: "native-pkce", Flow: nativehermes.AuthFlowPKCE, URL: testPKCEURL}

	result, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "anthropic", nativehermes.AuthFlowPKCE, "r"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	flowID := mustType[authAuthorizeResult](t, result).FlowID

	seedNativeCompletion(t, client.xdg.Root, "anthropic")
	seedPKCEResidence(t, client.xdg.Root, 1750000000000)

	polling := make(chan struct{})
	release := make(chan struct{})

	var polls atomic.Int32

	client.authPollFunc = func(context.Context, string, string) (nativehermes.AuthPoll, error) {
		if polls.Add(1) == 1 {
			close(polling)
			<-release
		}

		return nativehermes.AuthPoll{State: nativehermes.AuthPollComplete}, nil
	}

	settled := make(chan error, 1)

	go func() {
		_, err := authLeg(agent, AuthCallbackMethod, map[string]any{
			"sessionId": string(testSessionID), "providerId": "anthropic",
			"method": nativehermes.AuthFlowPKCE, "flowId": flowID, "input": "code#state",
		})
		settled <- err
	}()

	<-polling

	if _, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": "anthropic", "flowId": flowID,
	}); err != nil {
		t.Fatalf("status: %v", err)
	}

	close(release)

	callbackErr := <-settled

	if got := polls.Load(); got != 1 {
		t.Fatalf("native poll reads = %d, want the status probe to skip a flow another leg holds", got)
	}

	if callbackErr != nil {
		t.Fatalf("callback: %v", callbackErr)
	}
}

// TestGatedLegsFailClosedWhenTheRequestEndedFirst pins what a leg does when the
// caller that asked for it is gone before the gate reaches it. Every gate here
// is waited on with the request's own context, so the queue never outlives the
// request that joined it.
func TestGatedLegsFailClosedWhenTheRequestEndedFirst(t *testing.T) {
	agent, client := newAuthAgent(t)

	device := startDeviceFlow(t, agent, client)

	client.authKeyProviders = []nativehermes.AuthAPIKeyProvider{{ID: "openai", Name: "OpenAI"}, {ID: "mistral", Name: "Mistral"}}

	methods, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("methods: %v", err)
	}

	generation := mustType[authMethodsResult](t, methods).Generation

	parked, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r-openai"))
	if err != nil {
		t.Fatalf("authorize openai: %v", err)
	}

	queued, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "mistral", authAPIKeyMethodID, "r-mistral"))
	if err != nil {
		t.Fatalf("authorize mistral: %v", err)
	}

	writing := make(chan struct{})
	release := make(chan struct{})

	original := authWriteSlot
	authWriteSlot = func(home string, providerID string, label string, material nativehermes.AuthMaterial) error {
		close(writing)
		<-release

		return original(home, providerID, label, material)
	}

	t.Cleanup(func() { authWriteSlot = original })

	held := make(chan error, 1)

	go func() {
		_, callbackErr := authLeg(agent, AuthCallbackMethod, map[string]any{
			"sessionId": string(testSessionID), "providerId": "openai",
			"method": authAPIKeyMethodID, "flowId": mustType[authAuthorizeResult](t, parked).FlowID,
			"input": "sk-openai",
		})
		held <- callbackErr
	}()

	<-writing

	broker := agent.providerAuth
	ended := endedRequest(t)

	_, err = broker.disconnect(ended, authRawParams(t, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai",
		"connectionId": testConnectionID, "bindingGeneration": 1,
	}))
	requireAuthCause(t, err, authCauseTimeout)

	if outcome := broker.inject(ended, client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: testBinding()}); outcome != authInjectionConflict {
		t.Fatalf("injection into a store it never reached = %q", outcome)
	}

	_, err = broker.callback(ended, authRawParams(t, map[string]any{
		"sessionId": string(testSessionID), "providerId": "mistral",
		"method": authAPIKeyMethodID, "flowId": mustType[authAuthorizeResult](t, queued).FlowID,
		"input": "sk-mistral",
	}))
	requireAuthCause(t, err, authCauseTimeout)

	session, err := broker.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	requireAuthCause(t, broker.completeFlow(ended, session, broker.byID[device.FlowID]), authCauseTimeout)

	close(release)

	if err := <-held; err != nil {
		t.Fatalf("the leg that held the gate: %v", err)
	}
}

// TestASecondAuthorizeFailsClosedWhenItsRequestEndedInTheQueue pins the same
// answer for the authorize gate, which a second request for one key waits on
// for the length of the first request's mint.
func TestASecondAuthorizeFailsClosedWhenItsRequestEndedInTheQueue(t *testing.T) {
	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	starting := make(chan struct{})
	release := make(chan struct{})

	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		close(starting)
		<-release

		return nativehermes.AuthStart{SessionID: "native-flow", Flow: nativehermes.AuthFlowDeviceCode, URL: testDeviceURL}, nil
	}

	params := authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1")

	held := make(chan error, 1)

	go func() {
		_, err := authLeg(agent, AuthAuthorizeMethod, params)
		held <- err
	}()

	<-starting

	_, err := agent.providerAuth.authorize(endedRequest(t), authRawParams(t, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-2")))
	requireAuthCause(t, err, authCauseTimeout)

	close(release)

	if err := <-held; err != nil {
		t.Fatalf("the authorize that held the gate: %v", err)
	}
}

// TestAMintThatOutlivedItsCloseCancelsTheLoginItStarted pins the other half of
// session admission. A flow published before the close is in the cleanup set,
// but close read its native flow id before the mint had committed one, so the
// login it started is stoppable only by the authorize that started it.
func TestAMintThatOutlivedItsCloseCancelsTheLoginItStarted(t *testing.T) {
	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	starting := make(chan struct{})
	release := make(chan struct{})

	client.authStartFunc = func(context.Context, string) (nativehermes.AuthStart, error) {
		close(starting)
		<-release

		return nativehermes.AuthStart{
			SessionID: "native-flow", Flow: nativehermes.AuthFlowDeviceCode, URL: testDeviceURL,
		}, nil
	}

	settled := make(chan error, 1)

	go func() {
		_, err := authLeg(agent, AuthAuthorizeMethod, authorizeParams(generation, testProviderID, nativehermes.AuthFlowDeviceCode, "request-1"))
		settled <- err
	}()

	<-starting

	agent.providerAuth.closeSession(context.Background(), testSessionID)

	close(release)

	requireAuthCause(t, <-settled, authCauseFlowCancelled)

	client.mu.Lock()
	defer client.mu.Unlock()

	if len(client.authCancelled) != 1 || client.authCancelled[0] != "native-flow" {
		t.Fatalf("native cancels = %#v, want the login the mint committed after close", client.authCancelled)
	}
}

// TestACompletionRefusesToRefillASlotItsDisconnectRemoved runs the oauth arm of
// the same race. The native login has already produced its pool entry when the
// owner disconnects, and the migration that would label it runs afterwards: a
// confirmation refused after the fact leaves the credential resident under a
// ledger entry that says removed, which is live at the provider and reported
// nowhere.
func TestACompletionRefusesToRefillASlotItsDisconnectRemoved(t *testing.T) {
	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	seedNativeCompletion(t, client.xdg.Root, testProviderID)
	seedDeviceResidence(t, client.xdg.Root, testProviderID, 21600)

	if _, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "bindingGeneration": 1,
	}); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	requireAuthCause(t,
		agent.providerAuth.completeFlow(context.Background(), session, agent.providerAuth.byID[presentation.FlowID]),
		authCauseBindingConflict)

	present, err := nativehermes.AuthSlotPresent(client.xdg.Root, testProviderID, nativehermes.AuthSlotLabel(testConnectionID))
	if err != nil {
		t.Fatalf("slot present: %v", err)
	}

	if present {
		t.Fatal("a completion refused after its write left the credential resident under a removed ledger entry")
	}
}

// TestAuthorizeDoesNotClobberADisconnectsGenerationBump runs an authorize's one
// ledger read-modify-write against a disconnect's. The revision an authorize
// claims is derived from the record it read, so a disconnect that bumps the
// binding generation between that read and the write back has its bump read
// back and overwritten — the removal stands in the native store and the record
// names a generation the owner already retired. The two legs run in different
// sessions, which is where the credential-slot gate cannot help: the ledger
// entry is agent-wide, one file per provider under the host's own root, so two
// sessions rewriting it hold two different home gates.
func TestAuthorizeDoesNotClobberADisconnectsGenerationBump(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	if _, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r-first")); err != nil {
		t.Fatalf("authorize: %v", err)
	}

	peerClient := newFakeHermesClient()
	peerClient.xdg = nativehermes.XDGDirs{Root: t.TempDir()}

	peer := newSession(agent, "peer-session", "/cwd", nil, nil, nativehermes.Session{ID: "peer"}, peerClient, sessionMeta{}, idmapRecord{})
	if err := agent.storeStartedSession(peer); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	recording := make(chan struct{})
	disconnected := make(chan struct{})

	var renames atomic.Int32

	originalRename := ledgerRename
	ledgerRename = func(from string, to string) error {
		if renames.Add(1) == 1 {
			close(recording)

			select {
			case <-disconnected:
			case <-time.After(admissionRendezvous):
			}
		}

		return originalRename(from, to)
	}

	settled := make(chan error, 1)

	go func() {
		_, authorizeErr := authLeg(agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r-second"))
		settled <- authorizeErr
	}()

	<-recording

	if _, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
		"sessionId": "peer-session", "providerId": "openai",
		"connectionId": testConnectionID, "bindingGeneration": 1,
	}); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	close(disconnected)

	if err := <-settled; err != nil {
		t.Fatalf("authorize: %v", err)
	}

	record, ok, err := agent.providerAuth.ledger.read("openai")
	if err != nil || !ok {
		t.Fatalf("ledger read: %v present=%v", err, ok)
	}

	if record.BindingGeneration < 2 {
		t.Fatalf("ledger record = %#v, want the disconnect's generation bump to have survived", record)
	}
}

// TestAReopenedSessionAnswersItsAuthLegsAgain pins the one thing the closed-set
// must not outlive: the id. session/close leaves the durable snapshot in place,
// so a later session/load hydrates the same id and it is live again — and a
// tombstone that survived the reopen would refuse every provider-auth leg on
// that id for the rest of the agent's life.
func TestAReopenedSessionAnswersItsAuthLegsAgain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()

	agent := NewAgent(WithScratchDir(root), WithSessionStore(NewInMemorySessionStore()), WithProviderAuthRoot(t.TempDir()))

	xdg, err := nativehermes.CreateXDGDirs(root, "session-1")
	if err != nil {
		t.Fatalf("CreateXDGDirs: %v", err)
	}

	client := newFakeHermesClient()
	client.xdg = xdg

	session := testSession(agent, client)
	session.cwd = root

	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	if err := session.snapshotToStore(ctx); err != nil {
		t.Fatalf("snapshotToStore: %v", err)
	}

	if _, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.id}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}

	loaded := newFakeHermesClient()
	loaded.getSession = testNativeSession("native-1")
	loaded.providers = testProviders()

	agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		loaded.xdg = options.ExistingXDG

		return loaded, nil
	}

	agent.setAgentClient(newRecordingAgentClient())

	if _, err := agent.LoadSession(ctx, LoadSessionRequest(session.id, root)); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}

	if _, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(session.id)}); err != nil {
		t.Fatalf("a reopened session was refused its provider-auth legs: %v", err)
	}
}

// parkLedgerWrite starts an authorize and blocks it inside the one ledger write
// it makes, so the provider's ledger entry is held and no other gate is. The
// returned function releases it.
func parkLedgerWrite(t *testing.T, agent *Agent, providerID string, method string, requestID string) func() {
	t.Helper()

	generation := agent.providerAuth.generation

	recording := make(chan struct{})
	release := make(chan struct{})
	settled := make(chan error, 1)

	var renames atomic.Int32

	original := ledgerRename
	ledgerRename = func(from string, to string) error {
		if renames.Add(1) == 1 {
			close(recording)
			<-release
		}

		return original(from, to)
	}

	go func() {
		_, err := authLeg(agent, AuthAuthorizeMethod, authorizeParams(generation, providerID, method, requestID))
		settled <- err
	}()

	<-recording

	return func() {
		close(release)

		if err := <-settled; err != nil {
			t.Errorf("the authorize that held the ledger entry: %v", err)
		}

		ledgerRename = original
	}
}

// peerAuthSession registers a second session with its own native home, which is
// how a leg reaches a provider's ledger entry without holding the first
// session's credential-slot gate.
func peerAuthSession(t *testing.T, agent *Agent) *fakeHermesClient {
	t.Helper()

	client := newFakeHermesClient()
	client.xdg = nativehermes.XDGDirs{Root: t.TempDir()}

	peer := newSession(agent, "peer-session", "/cwd", nil, nil, nativehermes.Session{ID: "peer"}, client, sessionMeta{}, idmapRecord{})
	if err := agent.storeStartedSession(peer); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	return client
}

// TestLedgerGatedLegsFailClosedWhenTheRequestEndedFirst pins the answer every
// leg gives when the record it must rewrite is held and its own caller is
// already gone. The entry is agent-wide, so these legs reach it from a session
// whose credential-slot gate is free — the queue they join is the provider's,
// not the home's.
func TestLedgerGatedLegsFailClosedWhenTheRequestEndedFirst(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	generation := seedCatalog(t, agent, client)

	first, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(generation, "openai", authAPIKeyMethodID, "r-first"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	peerClient := peerAuthSession(t, agent)

	releaseParked := parkLedgerWrite(t, agent, "openai", authAPIKeyMethodID, "r-second")

	broker := agent.providerAuth
	ended := endedRequest(t)

	_, err = broker.callback(ended, authRawParams(t, map[string]any{
		"sessionId": string(testSessionID), "providerId": "openai",
		"method": authAPIKeyMethodID, "flowId": mustType[authAuthorizeResult](t, first).FlowID,
		"input": "sk-operator-key",
	}))
	requireAuthCause(t, err, authCauseTimeout)

	_, err = broker.disconnect(ended, authRawParams(t, map[string]any{
		"sessionId": "peer-session", "providerId": "openai",
		"connectionId": testConnectionID, "bindingGeneration": 1,
	}))
	requireAuthCause(t, err, authCauseTimeout)

	binding := ProviderAuthBinding{
		ConnectionID: testConnectionID, Revision: 1, BindingGeneration: 1,
		Credential: ProviderCredential{Type: ProviderCredentialHermesOAuth, HermesOAuth: &ProviderHermesOAuthCredential{
			AuthType: ProviderAuthTypeAPIKey, AccessToken: "sk-injected",
		}},
	}

	if outcome := broker.inject(ended, peerClient.xdg.Root, map[string]ProviderAuthBinding{"openai": binding}); outcome != authInjectionConflict {
		t.Fatalf("injection into a record it never reached = %q", outcome)
	}

	peerAuthorize := authorizeParams(generation, "openai", authAPIKeyMethodID, "r-peer")
	peerAuthorize["sessionId"] = "peer-session"

	_, err = broker.authorize(ended, authRawParams(t, peerAuthorize))
	requireAuthCause(t, err, authCauseTimeout)

	releaseParked()
}

// TestACompletionFailsClosedWhenItsRequestEndedAtTheLedger is the same answer
// for the oauth completion arm, which reaches the entry after it already holds
// the credential-slot gate.
func TestACompletionFailsClosedWhenItsRequestEndedAtTheLedger(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	device := startDeviceFlow(t, agent, client)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	releaseParked := parkLedgerWrite(t, agent, testProviderID, nativehermes.AuthFlowDeviceCode, "request-2")

	requireAuthCause(t,
		agent.providerAuth.completeFlow(endedRequest(t), session, agent.providerAuth.byID[device.FlowID]),
		authCauseTimeout)

	releaseParked()
}
