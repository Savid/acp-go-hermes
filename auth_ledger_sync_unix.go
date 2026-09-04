//go:build !windows

package hermesacp

import (
	"errors"
	"fmt"
)

// syncAuthLedgerDirectory flushes the ledger root so the rename that commits an
// entry reaches the disk in the order the commit path assumes.
func syncAuthLedgerDirectory(path string) error {
	dir, err := ledgerOpen(path)
	if err != nil {
		return fmt.Errorf("open provider auth ledger root: %w", err)
	}

	return errors.Join(dir.Sync(), dir.Close())
}
