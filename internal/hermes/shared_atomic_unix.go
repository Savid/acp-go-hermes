//go:build !windows

package hermes

import (
	"errors"
	"os"
)

func syncSharedHermesDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}

	return errors.Join(dir.Sync(), dir.Close())
}
