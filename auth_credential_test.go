package hermesacp

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func TestProviderCredentialMarshalsFlat(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		credential ProviderCredential
		want       string
	}{
		{
			name: "oauth",
			credential: ProviderCredential{Type: ProviderCredentialOAuth, OAuth: &ProviderOAuthCredential{
				Refresh: "r", Access: "a", AccessExpiresAt: 1,
			}},
			want: `{"type":"oauth","refresh":"r","access":"a","accessExpiresAt":1}`,
		},
		{
			name:       "api",
			credential: ProviderCredential{Type: ProviderCredentialAPI, API: &ProviderAPICredential{Key: "k"}},
			want:       `{"type":"api","key":"k"}`,
		},
		{
			name: "hermesOauth",
			credential: ProviderCredential{Type: ProviderCredentialHermesOAuth, HermesOAuth: &ProviderHermesOAuthCredential{
				AuthType: ProviderAuthTypeOAuth, AccessToken: "t",
			}},
			want: `{"type":"hermesOauth","authType":"oauth","accessToken":"t"}`,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := json.Marshal(tt.credential)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			if string(encoded) != tt.want {
				t.Fatalf("marshal = %s, want %s", encoded, tt.want)
			}

			var round ProviderCredential
			if err := json.Unmarshal(encoded, &round); err != nil {
				t.Fatalf("round trip: %v", err)
			}
		})
	}

	invalid := []ProviderCredential{
		{Type: ProviderCredentialOAuth},
		{Type: ProviderCredentialAPI},
		{Type: ProviderCredentialHermesOAuth},
		{Type: "wellknown"},
	}

	for _, credential := range invalid {
		if _, err := json.Marshal(credential); err == nil {
			t.Fatalf("marshalled an invalid union value: %#v", credential)
		}
	}
}

func TestProviderCredentialDecodesStrictly(t *testing.T) {
	t.Parallel()

	rejected := []string{
		`[]`,
		`{}`,
		`{"type":7}`,
		`{"type":"wellknown","key":"k","token":"t"}`,
		`{"type":"hermesOauth","accessToken":"t","authType":"oauth","extra":1}`,
		`{"type":"hermesOauth","accessToken":"t","accessToken":"u","authType":"oauth"}`,
		`{"type":"hermesOauth","accessToken":"","authType":"oauth"}`,
		`{"type":"hermesOauth","accessToken":"t","authType":"bearer"}`,
		`{"type":"hermesOauth","accessToken":1,"authType":"oauth"}`,
		`{"type":"oauth","refresh":"","access":"a","accessExpiresAt":1}`,
		`{"type":"oauth","refresh":"r","access":"a","accessExpiresAt":0}`,
		`{"type":"api","key":""}`,
		`{"type":"hermesOauth","accessToken":"t","authType":"oauth"} 1`,
		`{"type":"hermesOauth","accessToken":"t","authType":"oauth"`,
	}

	for _, input := range rejected {
		var credential ProviderCredential
		if err := json.Unmarshal([]byte(input), &credential); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}

	var accepted ProviderCredential
	if err := json.Unmarshal([]byte(`{"type":"api","key":"k","metadata":{"instanceUrl":"https://gitlab.example"}}`), &accepted); err != nil {
		t.Fatalf("valid api credential rejected: %v", err)
	}

	if accepted.API.Metadata["instanceUrl"] != "https://gitlab.example" {
		t.Fatalf("metadata = %#v", accepted.API.Metadata)
	}
}

func TestProviderMetadataBounds(t *testing.T) {
	t.Parallel()

	if !validProviderMetadata(nil) {
		t.Fatal("absent metadata rejected")
	}

	tooManyKeys := map[string]string{}
	for i := range providerMetadataMaxKeys + 1 {
		tooManyKeys[string(rune('a'+i))] = "v"
	}

	if validProviderMetadata(tooManyKeys) {
		t.Fatal("accepted more than sixteen keys")
	}

	if validProviderMetadata(map[string]string{"k": strings.Repeat("v", providerMetadataMaxValueBytes+1)}) {
		t.Fatal("accepted an over-long value")
	}

	oversized := map[string]string{}
	for i := range providerMetadataMaxKeys {
		oversized[string(rune('a'+i))] = strings.Repeat("v", providerMetadataMaxValueBytes)
	}

	if validProviderMetadata(oversized) {
		t.Fatal("accepted an over-long metadata object")
	}
}

// completedFlow drives a device flow to authenticated so the credential leg has
// a real reserved slot and ledger confirmation to work from.
func completedFlow(t *testing.T, agent *Agent, client *fakeHermesClient) string {
	t.Helper()

	presentation := startDeviceFlow(t, agent, client)

	seedNativeCompletion(t, client.xdg.Root, testProviderID)
	seedDeviceResidence(t, client.xdg.Root, testProviderID, 21600)

	client.authPoll = nativehermes.AuthPoll{State: nativehermes.AuthPollComplete}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if mustType[authStatusResult](t, status).State != authStateAuthenticated {
		t.Fatalf("flow did not complete: %#v", status)
	}

	return presentation.FlowID
}

func TestCredentialHarvestsOnlyTheReservedSlotAndOnlyOnce(t *testing.T) {
	agent, client := newAuthAgent(t)
	flowID := completedFlow(t, agent, client)

	anchor := time.Unix(1_700_000_000, 0)
	originalNow := authNow
	authNow = func() time.Time { return anchor }

	t.Cleanup(func() { authNow = originalNow })

	params := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID}

	result, err := callLeg(t, agent, AuthCredentialMethod, params)
	if err != nil {
		t.Fatalf("credential: %v", err)
	}

	harvest := mustType[authCredentialResult](t, result)
	if harvest.ConnectionID != testConnectionID || harvest.Revision != 1 || harvest.BindingGeneration != 1 {
		t.Fatalf("harvest binding = %#v", harvest)
	}

	if harvest.Credential.Type != ProviderCredentialHermesOAuth {
		t.Fatalf("harvest variant = %#v", harvest.Credential)
	}

	variant := harvest.Credential.HermesOAuth
	if variant.AccessToken != "native-access" || variant.RefreshToken != "native-refresh" {
		t.Fatalf("harvest material = %#v", variant)
	}

	if variant.AccessExpiresAt != anchor.Add(21600*time.Second).UnixMilli() {
		t.Fatalf("accessExpiresAt = %d, want the harvest-time anchor", variant.AccessExpiresAt)
	}

	encoded, err := json.Marshal(harvest)
	if err != nil {
		t.Fatalf("marshal harvest: %v", err)
	}

	for _, leaked := range []string{"ambient", "secret_fingerprint", "request_count", "source", "id_token"} {
		if strings.Contains(string(encoded), leaked) {
			t.Fatalf("harvest forwarded native bookkeeping %q: %s", leaked, encoded)
		}
	}

	_, err = callLeg(t, agent, AuthCredentialMethod, params)
	requireAuthCause(t, err, authCauseFlowState)
}

func TestCredentialRefusesAnIncompleteFlow(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	params := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": presentation.FlowID}

	_, err := callLeg(t, agent, AuthCredentialMethod, params)
	requireAuthCause(t, err, authCauseFlowState)

	// A flow-state refusal consumes nothing.
	status, err := callLeg(t, agent, AuthStatusMethod, params)
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if mustType[authStatusResult](t, status).State != authStatePending {
		t.Fatalf("a flow-state refusal terminalized the flow: %#v", status)
	}
}

func TestCredentialFencesAgainstTheLedger(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	flowID := completedFlow(t, agent, client)

	params := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID}

	record, _, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}

	record.ConnectionID = "another-connection"
	if errLocal := agent.providerAuth.ledger.write(record); errLocal != nil {
		t.Fatalf("ledger write: %v", err)
	}

	_, err = callLeg(t, agent, AuthCredentialMethod, params)
	requireAuthCause(t, err, authCauseHarvestFailed)

	if errLocal := os.Remove(agent.providerAuth.ledger.path(testProviderID)); errLocal != nil {
		t.Fatalf("remove ledger entry: %v", err)
	}

	_, err = callLeg(t, agent, AuthCredentialMethod, params)
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestCredentialFailsClosedOnAnUnreadableSlot(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	flowID := completedFlow(t, agent, client)

	params := map[string]any{"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID}

	label := nativehermes.AuthSlotLabel(testConnectionID)
	if _, err := nativehermes.AuthRemoveSlot(client.xdg.Root, testProviderID, label); err != nil {
		t.Fatalf("remove reserved slot: %v", err)
	}

	_, err := callLeg(t, agent, AuthCredentialMethod, params)
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestCredentialFailsClosedWithoutALiveHome(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	flowID := completedFlow(t, agent, client)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	_, err = callLeg(t, agent, AuthCredentialMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID,
	})
	requireAuthCause(t, err, authCauseTransport)
}

func TestCredentialFailsClosedOnAnUndecodableVariant(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	flowID := completedFlow(t, agent, client)

	label := nativehermes.AuthSlotLabel(testConnectionID)
	if err := nativehermes.AuthWriteSlot(client.xdg.Root, testProviderID, label, nativehermes.AuthMaterial{
		AuthType: "unrecognised", AccessToken: "token",
	}); err != nil {
		t.Fatalf("write reserved slot: %v", err)
	}

	_, err := callLeg(t, agent, AuthCredentialMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID,
	})
	requireAuthCause(t, err, authCauseHarvestFailed)

	if _, err := hermesOAuthCredential(nativehermes.AuthMaterial{AuthType: ProviderAuthTypeOAuth}); err == nil {
		t.Fatal("an empty access token round-tripped")
	}
}

func TestCredentialFailsClosedOnACorruptSecondResidence(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	flowID := completedFlow(t, agent, client)

	if err := os.WriteFile(filepath.Join(client.xdg.Root, "auth.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("corrupt store: %v", err)
	}

	_, err := callLeg(t, agent, AuthCredentialMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID,
	})
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestDisconnectRemovesOnlyTheFencedReservedSlot(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	completedFlow(t, agent, client)

	params := map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "bindingGeneration": 1,
	}

	if _, err := callLeg(t, agent, AuthDisconnectMethod, params); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	present, err := nativehermes.AuthSlotPresent(client.xdg.Root, testProviderID, nativehermes.AuthSlotLabel(testConnectionID))
	if err != nil || present {
		t.Fatalf("reserved slot present after disconnect = %v, %v", present, err)
	}

	store := readStoreFixture(t, client.xdg.Root)

	pool, _ := store["credential_pool"].(map[string]any)

	entries, _ := pool[testProviderID].([]any)
	if len(entries) != 1 {
		t.Fatalf("disconnect disturbed the ambient entry: %#v", entries)
	}

	record, _, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}

	if record.State != authLedgerRemoved || record.BindingGeneration != 2 {
		t.Fatalf("ledger record = %#v", record)
	}

	// The same request now fences against the bumped generation.
	_, err = callLeg(t, agent, AuthDisconnectMethod, params)
	requireAuthCause(t, err, authCausePolicy)
}

func TestDisconnectRejectsEveryAddressingAndFencingFailure(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	completedFlow(t, agent, client)

	base := map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "bindingGeneration": 1,
	}

	for _, field := range []string{"sessionId", "providerId", "connectionId", "bindingGeneration"} {
		params := map[string]any{}
		for key, value := range base {
			params[key] = value
		}

		delete(params, field)

		_, err := callLeg(t, agent, AuthDisconnectMethod, params)
		requireInvalidField(t, err, field)
	}

	unknownSession := map[string]any{}
	for key, value := range base {
		unknownSession[key] = value
	}

	unknownSession["sessionId"] = "unknown"

	if _, err := callLeg(t, agent, AuthDisconnectMethod, unknownSession); err == nil {
		t.Fatal("unknown session accepted")
	}

	wrongConnection := map[string]any{}
	for key, value := range base {
		wrongConnection[key] = value
	}

	wrongConnection["connectionId"] = "other"

	_, err := callLeg(t, agent, AuthDisconnectMethod, wrongConnection)
	requireAuthCause(t, err, authCausePolicy)

	unknownProvider := map[string]any{}
	for key, value := range base {
		unknownProvider[key] = value
	}

	unknownProvider["providerId"] = "unrecorded"

	_, err = callLeg(t, agent, AuthDisconnectMethod, unknownProvider)
	requireAuthCause(t, err, authCausePolicy)
}

func TestDisconnectFailurePaths(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	completedFlow(t, agent, client)

	params := map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "bindingGeneration": 1,
	}

	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }

	_, err := callLeg(t, agent, AuthDisconnectMethod, params)
	requireAuthCause(t, err, authCauseHarvestFailed)

	restoreLedgerHooks(t)

	ledgerRename = func(string, string) error { return errors.New("rename") }

	_, err = callLeg(t, agent, AuthDisconnectMethod, params)
	requireAuthCause(t, err, authCauseProcess)

	restoreLedgerHooks(t)

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	record, _, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}

	generationParams := map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "bindingGeneration": record.BindingGeneration,
	}

	_, err = callLeg(t, agent, AuthDisconnectMethod, generationParams)
	requireAuthCause(t, err, authCauseTransport)

	session.mu.Lock()
	session.client = client
	session.mu.Unlock()

	if errLocal := os.WriteFile(filepath.Join(client.xdg.Root, "auth.json"), []byte("{"), 0o600); errLocal != nil {
		t.Fatalf("corrupt store: %v", err)
	}

	record, _, err = agent.providerAuth.ledger.read(testProviderID)
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}

	corruptParams := map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "bindingGeneration": record.BindingGeneration,
	}

	_, err = callLeg(t, agent, AuthDisconnectMethod, corruptParams)
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestDisconnectFailsClosedWhenTheRemovalCannotBeConfirmed(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	completedFlow(t, agent, client)

	record, _, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}

	writes := 0
	originalRename := ledgerRename
	ledgerRename = func(from string, to string) error {
		writes++
		if writes > 1 {
			return errors.New("rename")
		}

		return originalRename(from, to)
	}

	_, err = callLeg(t, agent, AuthDisconnectMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "bindingGeneration": record.BindingGeneration,
	})
	requireAuthCause(t, err, authCauseProcess)

	present, presentErr := nativehermes.AuthSlotPresent(client.xdg.Root, testProviderID, nativehermes.AuthSlotLabel(testConnectionID))
	if presentErr != nil || present {
		t.Fatalf("reserved slot survived a failed confirmation write: %v, %v", present, presentErr)
	}
}

func TestInjectionOutcomesAreTheFourFixedCases(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	broker := agent.providerAuth
	home := client.xdg.Root

	bindings := map[string]ProviderAuthBinding{testProviderID: testBinding()}

	if outcome := broker.inject(home, bindings); outcome != authInjectionApplied {
		t.Fatalf("empty entry outcome = %q", outcome)
	}

	if outcome := broker.inject(home, bindings); outcome != authInjectionNoop {
		t.Fatalf("equal live entry outcome = %q", outcome)
	}

	differing := testBinding()
	differing.Credential.HermesOAuth = &ProviderHermesOAuthCredential{AuthType: ProviderAuthTypeOAuth, AccessToken: "other"}

	if outcome := broker.inject(home, map[string]ProviderAuthBinding{testProviderID: differing}); outcome != authInjectionConflict {
		t.Fatalf("differing live entry outcome = %q", outcome)
	}

	foreign := testBinding()
	foreign.ConnectionID = "unknown-connection"

	if outcome := broker.inject(home, map[string]ProviderAuthBinding{testProviderID: foreign}); outcome != authInjectionConflict {
		t.Fatalf("unknown connection outcome = %q", outcome)
	}

	if outcome := broker.inject(home, map[string]ProviderAuthBinding{}); outcome != authInjectionNoop {
		t.Fatalf("empty binding map outcome = %q", outcome)
	}
}

func TestInjectionAcceptsOnlyTheHermesVariant(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)

	binding := testBinding()
	binding.Credential = ProviderCredential{Type: ProviderCredentialAPI, API: &ProviderAPICredential{Key: "k"}}

	outcome := agent.providerAuth.inject(client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: binding})
	if outcome != authInjectionConflict {
		t.Fatalf("a foreign variant produced %q", outcome)
	}

	empty := testBinding()
	empty.Credential = ProviderCredential{Type: ProviderCredentialHermesOAuth}

	if agent.providerAuth.injectOne(client.xdg.Root, testProviderID, empty) != authInjectionConflict {
		t.Fatal("a type without its variant pointer was accepted")
	}
}

func TestInjectionConflictsWhenTheLineageDiffers(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	broker := agent.providerAuth

	if outcome := broker.inject(client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: testBinding()}); outcome != authInjectionApplied {
		t.Fatalf("first injection = %q", outcome)
	}

	advanced := testBinding()
	advanced.Revision = 2

	if outcome := broker.inject(client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: advanced}); outcome != authInjectionConflict {
		t.Fatalf("a revision mismatch produced %q", outcome)
	}

	restoreLedgerHooks(t)

	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }

	if outcome := broker.inject(client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: testBinding()}); outcome != authInjectionConflict {
		t.Fatalf("an unreadable ledger produced %q", outcome)
	}
}

func TestInjectionConflictsWhenTheStoreCannotBeReadOrWritten(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	broker := agent.providerAuth

	if err := os.WriteFile(filepath.Join(client.xdg.Root, "auth.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("corrupt store: %v", err)
	}

	if outcome := broker.inject(client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: testBinding()}); outcome != authInjectionConflict {
		t.Fatalf("a corrupt store produced %q", outcome)
	}

	if err := os.Remove(filepath.Join(client.xdg.Root, "auth.json")); err != nil {
		t.Fatalf("remove store: %v", err)
	}

	ledgerRename = func(string, string) error { return errors.New("rename") }

	if outcome := broker.inject(client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: testBinding()}); outcome != authInjectionConflict {
		t.Fatalf("an unwritable ledger produced %q", outcome)
	}

	restoreLedgerHooks(t)

	if outcome := broker.inject(filepath.Join(client.xdg.Root, "auth.json", "nested"), map[string]ProviderAuthBinding{testProviderID: testBinding()}); outcome != authInjectionConflict {
		t.Fatalf("an unwritable home produced %q", outcome)
	}
}

func TestInjectionSurvivesADisconnectedLineage(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	broker := agent.providerAuth

	if err := broker.ledger.write(authLedgerRecord{
		ProviderID: testProviderID, ConnectionID: "old-connection",
		Revision: 1, BindingGeneration: 1, State: authLedgerRemoved,
	}); err != nil {
		t.Fatalf("seed removed record: %v", err)
	}

	if outcome := broker.inject(client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: testBinding()}); outcome != authInjectionApplied {
		t.Fatalf("injection after a disconnect = %q", outcome)
	}
}

func TestSortedBindingKeysIsDeterministic(t *testing.T) {
	t.Parallel()

	keys := sortedBindingKeys(map[string]ProviderAuthBinding{"zeta": {}, "alpha": {}, "mid": {}})
	if len(keys) != 3 || keys[0] != "alpha" || keys[1] != "mid" || keys[2] != "zeta" {
		t.Fatalf("keys = %#v", keys)
	}
}

func TestStrictCredentialFieldsWalksTheObjectOnce(t *testing.T) {
	t.Parallel()

	rejected := []string{
		`{"a":1,}`,
		`{"a":}`,
		`{"a":1`,
		`{"a":1} 2`,
	}

	for _, input := range rejected {
		if _, err := strictCredentialFields([]byte(input)); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}

	fields, err := strictCredentialFields([]byte(`{"a":1,"b":"x"}`))
	if err != nil || len(fields) != 2 {
		t.Fatalf("strictCredentialFields = %#v, %v", fields, err)
	}
}

func TestDecodeCredentialVariantRejectsAnUnencodableField(t *testing.T) {
	t.Parallel()

	var variant ProviderAPICredential

	err := decodeCredentialVariant(map[string]json.RawMessage{"key": json.RawMessage("{")}, []string{"key"}, &variant)
	if err == nil {
		t.Fatal("an unencodable field was accepted")
	}
}

func TestDecodeVariantsRejectMalformedValues(t *testing.T) {
	t.Parallel()

	var credential ProviderCredential

	if err := credential.decodeOAuth(map[string]json.RawMessage{"refresh": json.RawMessage(`7`)}); err == nil {
		t.Fatal("a non-string refresh was accepted")
	}

	if err := credential.decodeAPI(map[string]json.RawMessage{"key": json.RawMessage(`7`)}); err == nil {
		t.Fatal("a non-string key was accepted")
	}
}

func TestCredentialFailsClosedOnAnUnreadableSecondResidence(t *testing.T) {
	agent, client := newAuthAgent(t)
	flowID := completedFlow(t, agent, client)

	original := authReadFlowExpiry
	authReadFlowExpiry = func(string, string, string) (nativehermes.AuthMaterial, bool, error) {
		return nativehermes.AuthMaterial{}, false, errors.New("residence")
	}

	t.Cleanup(func() { authReadFlowExpiry = original })

	_, err := callLeg(t, agent, AuthCredentialMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID, "flowId": flowID,
	})
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestDisconnectRejectsAnUnknownParamField(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	_, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{"sessionId": string(testSessionID), "extra": 1})
	requireInvalidField(t, err, "extra")
}

func TestDisconnectFailsClosedWhenAbsenceCannotBeVerified(t *testing.T) {
	agent, client := newAuthAgent(t)
	completedFlow(t, agent, client)

	original := authSlotPresent
	authSlotPresent = func(string, string, string) (bool, error) { return true, nil }

	t.Cleanup(func() { authSlotPresent = original })

	_, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
		"sessionId": string(testSessionID), "providerId": testProviderID,
		"connectionId": testConnectionID, "bindingGeneration": 1,
	})
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestInjectionConflictsWhenTheSlotCannotBeWritten(t *testing.T) {
	agent, client := newAuthAgent(t)

	original := authWriteSlot
	authWriteSlot = func(string, string, string, nativehermes.AuthMaterial) error { return errors.New("write") }

	t.Cleanup(func() { authWriteSlot = original })

	outcome := agent.providerAuth.inject(client.xdg.Root, map[string]ProviderAuthBinding{testProviderID: testBinding()})
	if outcome != authInjectionConflict {
		t.Fatalf("an unwritable slot produced %q", outcome)
	}
}
