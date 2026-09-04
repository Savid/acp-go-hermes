//go:build windows

package hermesacp

import (
	"errors"
	"os"
	"testing"
)

// TestAuthLedgerCommitNeverOpensTheLedgerRoot pins the documented Windows
// behaviour: FlushFileBuffers refuses a directory handle opened through
// os.Open, so the commit path never opens the ledger root at all and a fault
// planted on that open cannot reach a ledger write. The ledger is built
// directly because Windows refuses the shared home a constructed one binds.
func TestAuthLedgerCommitNeverOpensTheLedgerRoot(t *testing.T) {
	restoreLedgerHooks(t)

	ledger := &authLedger{dir: t.TempDir()}

	ledgerOpen = func(string) (*os.File, error) { return nil, errors.New("open") }
	if err := ledger.write(authLedgerRecord{ProviderID: testProviderID}); err != nil {
		t.Fatalf("ledger commit: %v", err)
	}
}
