//go:build windows

package hermesacp

import (
	"strings"
	"testing"
)

// TestAuthLedgerRefusesTheSharedHomeOnWindows pins where that refusal is made:
// the ledger binds a per-residence lock root inside the adapter control
// directory, and preparing that directory is what Windows declines.
func TestAuthLedgerRefusesTheSharedHomeOnWindows(t *testing.T) {
	_, err := newAuthLedger(Options{ProviderAuthRoot: durableTempDir(t), SharedHermesHome: durableTempDir(t)})
	if err == nil {
		t.Fatal("windows built a provider auth ledger")
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("ledger refusal = %v", err)
	}
}
