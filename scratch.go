package hermesacp

import (
	"fmt"
	"os"
)

// scratchParent resolves the parent directory for all ephemeral on-disk
// materialization: dir when set, else the system temp directory. This is the
// only place in the module that consults the system temp directory.
func scratchParent(dir string) string {
	if dir != "" {
		return dir
	}

	return os.TempDir()
}

// ensureScratchParent resolves the scratch parent and creates it 0700 when
// missing.
func ensureScratchParent(dir string) (string, error) {
	parent := scratchParent(dir)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("create scratch parent: %w", err)
	}

	return parent, nil
}
