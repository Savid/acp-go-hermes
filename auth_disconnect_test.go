package hermesacp

import (
	"errors"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func seedConfirmedLineage(t *testing.T, agent *Agent, providerID string) authLedgerRecord {
	t.Helper()

	record := authLedgerRecord{
		ProviderID:        providerID,
		ConnectionID:      testConnectionID,
		Revision:          1,
		BindingGeneration: 1,
		State:             authLedgerConfirmed,
		CreatedAt:         1,
		UpdatedAt:         1,
	}
	if err := agent.providerAuth.ledger.write(record); err != nil {
		t.Fatalf("seed confirmed lineage: %v", err)
	}

	return record
}

func TestDisconnectUsesNativeDeleteAndVerifiesLoggedOut(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	seedConfirmedLineage(t, agent, testProviderID)
	client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, LoggedIn: false}}

	result, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
		"sessionId":         string(testSessionID),
		"providerId":        testProviderID,
		"connectionId":      testConnectionID,
		"bindingGeneration": 1,
	})
	if err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if _, ok := result.(struct{}); !ok {
		t.Fatalf("disconnect result = %T", result)
	}

	if len(client.authDisconnected) != 1 || client.authDisconnected[0] != testProviderID {
		t.Fatalf("native disconnects = %#v", client.authDisconnected)
	}

	record, ok, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil || !ok || record.State != authLedgerRemoved || record.BindingGeneration != 2 {
		t.Fatalf("removed lineage = %#v, %v, %v", record, ok, err)
	}
}

func TestDisconnectCancelsPendingProviderFlowBeforeNativeDelete(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	presentation := startDeviceFlow(t, agent, client)

	record, ok, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil || !ok {
		t.Fatalf("read flow lineage: %v, %v", ok, err)
	}

	record.State = authLedgerConfirmed
	if writeErr := agent.providerAuth.ledger.write(record); writeErr != nil {
		t.Fatalf("confirm flow lineage: %v", writeErr)
	}

	client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, LoggedIn: false}}

	if _, disconnectErr := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
		"sessionId":         string(testSessionID),
		"providerId":        testProviderID,
		"connectionId":      testConnectionID,
		"bindingGeneration": record.BindingGeneration,
	}); disconnectErr != nil {
		t.Fatalf("disconnect: %v", disconnectErr)
	}

	status, err := callLeg(t, agent, AuthStatusMethod, map[string]any{
		"sessionId":  string(testSessionID),
		"providerId": testProviderID,
		"flowId":     presentation.FlowID,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	got := mustType[authStatusResult](t, status)
	if got.State != authStateCancelled || got.Reason != authReasonSuperseded {
		t.Fatalf("cancelled flow = %#v", got)
	}
	if len(client.authCancelled) != 1 {
		t.Fatalf("native flow cancellations = %#v", client.authCancelled)
	}
}

func TestDisconnectFailsClosedOnLineageNativeAndVerificationFailures(t *testing.T) {
	t.Parallel()

	t.Run("lineage", func(t *testing.T) {
		t.Parallel()

		agent, _ := newAuthAgent(t)
		_, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
			"sessionId":         string(testSessionID),
			"providerId":        testProviderID,
			"connectionId":      testConnectionID,
			"bindingGeneration": 1,
		})
		requireAuthCause(t, err, authCauseBindingConflict)
	})

	t.Run("native refusal", func(t *testing.T) {
		t.Parallel()

		agent, client := newAuthAgent(t)
		seedConfirmedLineage(t, agent, testProviderID)
		client.authDisconnectErr = &nativehermes.AuthStatusError{StatusCode: 400}

		_, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
			"sessionId":         string(testSessionID),
			"providerId":        testProviderID,
			"connectionId":      testConnectionID,
			"bindingGeneration": 1,
		})
		requireAuthCause(t, err, authCauseProviderRefused)
	})

	t.Run("still logged in", func(t *testing.T) {
		t.Parallel()

		agent, client := newAuthAgent(t)
		seedConfirmedLineage(t, agent, testProviderID)
		client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, LoggedIn: true}}

		_, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
			"sessionId":         string(testSessionID),
			"providerId":        testProviderID,
			"connectionId":      testConnectionID,
			"bindingGeneration": 1,
		})
		requireAuthCause(t, err, authCauseNativeVeto)
	})

	t.Run("catalog transport", func(t *testing.T) {
		t.Parallel()

		agent, client := newAuthAgent(t)
		seedConfirmedLineage(t, agent, testProviderID)
		client.authProvidersErr = errors.New("transport")

		_, err := callLeg(t, agent, AuthDisconnectMethod, map[string]any{
			"sessionId":         string(testSessionID),
			"providerId":        testProviderID,
			"connectionId":      testConnectionID,
			"bindingGeneration": 1,
		})
		requireAuthCause(t, err, authCauseTransport)
	})
}

func TestDisconnectRejectsMalformedAddressing(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	base := map[string]any{
		"sessionId":         string(testSessionID),
		"providerId":        testProviderID,
		"connectionId":      testConnectionID,
		"bindingGeneration": 1,
	}

	for _, field := range []string{"sessionId", "providerId", "connectionId", "bindingGeneration"} {
		params := make(map[string]any, len(base))
		for key, value := range base {
			params[key] = value
		}
		delete(params, field)

		_, err := callLeg(t, agent, AuthDisconnectMethod, params)
		requireInvalidField(t, err, field)
	}

	invalid := make(map[string]any, len(base))
	for key, value := range base {
		invalid[key] = value
	}
	invalid["bindingGeneration"] = 0

	_, err := callLeg(t, agent, AuthDisconnectMethod, invalid)
	requireInvalidField(t, err, authFieldBindingGeneration)
}
