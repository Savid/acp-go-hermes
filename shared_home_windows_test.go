//go:build windows

package hermesacp

import (
	"strings"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const windowsSharedHomeRefusal = "shared Hermes home is unsupported on windows"

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

// TestAuthLedgerRefusesTheSharedHomeOnWindows pins where that refusal is made:
// the ledger binds a per-residence lock root inside the adapter control
// directory, and preparing that directory is what Windows declines.
func TestAuthLedgerRefusesTheSharedHomeOnWindows(t *testing.T) {
	_, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), SharedHermesHome: t.TempDir()})
	if err == nil {
		t.Fatal("windows built a provider auth ledger")
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("ledger refusal = %v", err)
	}
}

// TestSessionOperationJournalRefusesTheSharedHomeOnWindows pins that the
// shared session-operation journal is refused for the same reason, so a
// shared-home session mutation cannot begin here rather than half-beginning.
func TestSessionOperationJournalRefusesTheSharedHomeOnWindows(t *testing.T) {
	journal, err := beginSessionOperationJournal(t.TempDir(), sessionOperationJournalFields{
		OperationID: "operation", Kind: sessionOperationKindNew, Mode: sessionOperationModeShared,
		LogicalSessionID: "session-1",
	})
	if err == nil {
		t.Fatal("windows began a shared session-operation journal")
	}

	if journal != nil {
		t.Fatal("refused journal was returned anyway")
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("journal refusal = %v", err)
	}
}

// TestSharedSessionSetLockRefusesOnWindows pins the fence a shared-home turn
// takes before touching the native session set: it is unavailable here, so no
// shared-home turn can claim one and proceed unfenced.
func TestSharedSessionSetLockRefusesOnWindows(t *testing.T) {
	lock, err := nativehermes.AcquireSharedSessionSetLock(
		t.Context(), t.TempDir(), nativehermes.SharedSessionSetLockExclusive)
	if err == nil {
		t.Fatal("windows acquired a shared session-set lock")
	}

	if lock != nil {
		t.Fatal("refused session-set lock was returned anyway")
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("session-set lock refusal = %v", err)
	}
}
