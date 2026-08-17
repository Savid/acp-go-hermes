//go:build !windows

package hermesacp

import (
	"errors"
	"os"
)

func syncSessionOperationDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}

	return errors.Join(directory.Sync(), directory.Close())
}
