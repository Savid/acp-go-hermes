//go:build !windows

package hermesacp

import (
	"errors"
	"os"
	"testing"
)

// TestAuthLedgerDirectorySyncFailureFailsTheCommit pins that a ledger commit is
// not reported as durable when the root it renamed into could not be flushed.
func TestAuthLedgerDirectorySyncFailureFailsTheCommit(t *testing.T) {
	restoreLedgerHooks(t)

	ledger := newTestLedger(t)

	ledgerOpen = func(string) (*os.File, error) { return nil, errors.New("open") }
	if err := ledger.write(authLedgerRecord{ProviderID: testProviderID}); err == nil {
		t.Fatal("directory sync failure ignored")
	}
}
