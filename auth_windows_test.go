//go:build windows

package hermesacp

import "testing"

// TestProviderAuthStaysUnadvertisedOnWindows pins the documented deviation at
// the surface a host sees: the ledger and residence pair that advertises
// provider auth everywhere else advertises nothing here, because the shared
// Hermes home the ledger binds is refused. The agent itself keeps working.
func TestProviderAuthStaysUnadvertisedOnWindows(t *testing.T) {
	agent := newTestAgent(WithProviderAuthRoot(t.TempDir()), WithSharedHermesHome(t.TempDir()))
	if agent.providerAuth != nil {
		t.Fatal("windows advertised the provider auth surface")
	}

	if agent.optionsErr != nil {
		t.Fatalf("unusable provider auth disabled the agent: %v", agent.optionsErr)
	}
}
