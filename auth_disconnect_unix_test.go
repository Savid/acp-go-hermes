//go:build !windows

package hermesacp

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestDisconnectRemovesExactLineageAndPermitsAReplacement(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	seedConfirmedLineage(t, agent, testProviderID)

	result, err := callLeg(t, agent, AuthDisconnectMethod, disconnectParams(testConnectionID, 1))
	if err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if _, ok := result.(struct{}); !ok {
		t.Fatalf("disconnect result = %#v", result)
	}

	client.mu.Lock()
	disconnected := append([]string(nil), client.authDisconnected...)
	client.mu.Unlock()
	if len(disconnected) != 1 || disconnected[0] != testProviderID {
		t.Fatalf("native disconnects = %#v", disconnected)
	}

	record, present, err := agent.providerAuth.ledger.read(testProviderID)
	if err != nil || !present || record.State != authLedgerRemoved || record.BindingGeneration != 2 {
		t.Fatalf("removed lineage = %#v, %v, %v", record, present, err)
	}
	inventory, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{
		authFieldSessionID: string(testSessionID),
	})
	if err != nil {
		t.Fatalf("inventory after disconnect: %v", err)
	}
	if entries := mustType[authInventoryResult](t, inventory).Entries; len(entries) != 0 {
		t.Fatalf("removed lineage remained in inventory: %#v", entries)
	}

	generation := seedCatalog(t, agent, client)
	client.authStart = nativehermes.AuthStart{
		SessionID:    "replacement-native",
		Flow:         nativehermes.AuthFlowDeviceCode,
		URL:          testDeviceURL,
		UserCode:     "REPLACEMENT",
		PollInterval: time.Second,
		ExpiresIn:    time.Minute,
	}
	_, err = callLeg(t, agent, AuthAuthorizeMethod, map[string]any{
		authFieldSessionID:          string(testSessionID),
		authFieldProviderID:         testProviderID,
		authFieldConnectionID:       "connection-2",
		authFieldMethodsGeneration:  generation,
		authFieldMethod:             nativehermes.AuthFlowDeviceCode,
		authFieldAuthorizeRequestID: "replacement-request",
	})
	if err != nil {
		t.Fatalf("replacement authorize: %v", err)
	}

	record, present, err = agent.providerAuth.ledger.read(testProviderID)
	if err != nil || !present || record.ConnectionID != "connection-2" || record.BindingGeneration != 2 {
		t.Fatalf("replacement lineage = %#v, %v, %v", record, present, err)
	}
}

func TestDisconnectFencesTheExactLineage(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	seedConfirmedLineage(t, agent, testProviderID)

	for _, params := range []map[string]any{
		disconnectParams("other-connection", 1),
		disconnectParams(testConnectionID, 2),
	} {
		_, err := callLeg(t, agent, AuthDisconnectMethod, params)
		requireAuthCause(t, err, authCauseBindingConflict)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.authDisconnected) != 0 {
		t.Fatalf("a mismatched lineage reached native removal: %#v", client.authDisconnected)
	}
}

func TestDisconnectNativeFailureRetainsConfirmedLineage(t *testing.T) {
	t.Parallel()

	agent, client := newAuthAgent(t)
	seedConfirmedLineage(t, agent, testProviderID)
	client.authDisconnectErr = errors.New("native failure")

	_, err := callLeg(t, agent, AuthDisconnectMethod, disconnectParams(testConnectionID, 1))
	requireAuthCause(t, err, authCauseTransport)

	record, present, readErr := agent.providerAuth.ledger.read(testProviderID)
	if readErr != nil || !present || record.State != authLedgerConfirmed || record.BindingGeneration != 1 {
		t.Fatalf("failed disconnect changed lineage = %#v, %v, %v", record, present, readErr)
	}
}

func TestDisconnectFailureBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("unknown session", func(t *testing.T) {
		t.Parallel()

		agent, _ := newAuthAgent(t)
		params := disconnectParams(testConnectionID, 1)
		params[authFieldSessionID] = "unknown"

		if _, err := callLeg(t, agent, AuthDisconnectMethod, params); err == nil {
			t.Fatal("disconnect accepted an unknown session")
		}
	})

	t.Run("provider gate timeout", func(t *testing.T) {
		t.Parallel()

		agent, _ := newAuthAgent(t)
		seedConfirmedLineage(t, agent, testProviderID)
		release, ok := agent.providerAuth.lockProvider(context.Background(), testProviderID)
		if !ok {
			t.Fatal("hold provider gate")
		}
		defer release()

		_, err := disconnectWithContext(t, agent, endedAuthContext(), disconnectParams(testConnectionID, 1))
		requireAuthCause(t, err, authCauseTimeout)
	})

	t.Run("ledger gate timeout", func(t *testing.T) {
		t.Parallel()

		agent, _ := newAuthAgent(t)
		seedConfirmedLineage(t, agent, testProviderID)
		release, ok := agent.providerAuth.lockLedger(context.Background(), testProviderID)
		if !ok {
			t.Fatal("hold ledger gate")
		}
		defer release()

		_, err := disconnectWithContext(t, agent, endedAuthContext(), disconnectParams(testConnectionID, 1))
		requireAuthCause(t, err, authCauseTimeout)
	})

	t.Run("missing native client", func(t *testing.T) {
		t.Parallel()

		agent, _ := newAuthAgent(t)
		seedConfirmedLineage(t, agent, testProviderID)
		session, err := agent.providerAuth.authSession(string(testSessionID))
		if err != nil {
			t.Fatalf("authSession: %v", err)
		}
		session.mu.Lock()
		session.client = nil
		session.mu.Unlock()

		_, err = callLeg(t, agent, AuthDisconnectMethod, disconnectParams(testConnectionID, 1))
		requireAuthCause(t, err, authCauseTransport)
	})
}

func TestDisconnectLedgerReadFailure(t *testing.T) {
	restoreLedgerHooks(t)

	agent, _ := newAuthAgent(t)
	seedConfirmedLineage(t, agent, testProviderID)
	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }

	_, err := callLeg(t, agent, AuthDisconnectMethod, disconnectParams(testConnectionID, 1))
	requireAuthCause(t, err, authCauseProcess)
}

func TestDisconnectFinalWriteFailureCanRetryTheIdempotentNativeDelete(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	seedConfirmedLineage(t, agent, testProviderID)

	realRename := os.Rename
	ledgerRename = func(string, string) error { return errors.New("rename") }

	_, err := callLeg(t, agent, AuthDisconnectMethod, disconnectParams(testConnectionID, 1))
	requireAuthCause(t, err, authCauseProcess)

	record, present, readErr := agent.providerAuth.ledger.read(testProviderID)
	if readErr != nil || !present || record.State != authLedgerConfirmed || record.BindingGeneration != 1 {
		t.Fatalf("failed tombstone changed lineage = %#v, %v, %v", record, present, readErr)
	}

	ledgerRename = realRename
	if _, err = callLeg(t, agent, AuthDisconnectMethod, disconnectParams(testConnectionID, 1)); err != nil {
		t.Fatalf("retry disconnect: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.authDisconnected) != 2 {
		t.Fatalf("native DELETE was not safely retried: %#v", client.authDisconnected)
	}
}

func TestDisconnectRejectsMalformedAddressing(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	base := disconnectParams(testConnectionID, 1)

	for _, field := range []string{
		authFieldSessionID, authFieldProviderID, authFieldConnectionID,
		authFieldBindingGeneration,
	} {
		params := make(map[string]any, len(base))
		for key, value := range base {
			params[key] = value
		}
		delete(params, field)

		if _, err := callLeg(t, agent, AuthDisconnectMethod, params); err == nil {
			t.Fatalf("disconnect accepted missing %s", field)
		}
	}

	for _, generation := range []any{0, -1, 1.5, "1"} {
		params := disconnectParams(testConnectionID, 1)
		params[authFieldBindingGeneration] = generation
		if _, err := callLeg(t, agent, AuthDisconnectMethod, params); err == nil {
			t.Fatalf("disconnect accepted generation %#v", generation)
		}
	}

	params := disconnectParams(testConnectionID, 1)
	params["extra"] = true
	if _, err := callLeg(t, agent, AuthDisconnectMethod, params); err == nil {
		t.Fatal("disconnect accepted an unknown field")
	}
}
