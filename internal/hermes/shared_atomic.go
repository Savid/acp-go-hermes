package hermes

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	sharedAtomicCreateTemp = os.CreateTemp
	sharedAtomicRename     = os.Rename
	sharedAtomicWriteFile  = atomicSharedHermesWriteFile
	sharedAtomicFileSync   = (*os.File).Sync
	sharedAtomicFileClose  = (*os.File).Close
)

// atomicSharedHermesWriteFile commits one complete same-directory replacement.
// The file is durable before rename and the containing directory is synced
// afterwards on platforms that expose directory fsync.
//
//nolint:govet,gosec // Narrow setup scope keeps the directory error adjacent to its operation; every caller confines path beneath an already-validated adapter-owned root.
func atomicSharedHermesWriteFile(path string, data []byte, mode os.FileMode) (err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	temp, err := sharedAtomicCreateTemp(dir, ".acp-go-hermes-write-*")
	if err != nil {
		return fmt.Errorf("create atomic shared Hermes file: %w", err)
	}

	tempPath := temp.Name()

	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, sharedAtomicFileClose(temp))
		}

		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}()

	if err := temp.Chmod(mode); err != nil {
		return err
	}

	if _, err := temp.Write(data); err != nil {
		return err
	}

	if err := sharedAtomicFileSync(temp); err != nil {
		return err
	}

	if err := sharedAtomicFileClose(temp); err != nil {
		return err
	}

	closed = true

	if err := sharedAtomicRename(tempPath, path); err != nil {
		return fmt.Errorf("rename atomic shared Hermes file: %w", err)
	}

	return syncSharedHermesDirectory(dir)
}
