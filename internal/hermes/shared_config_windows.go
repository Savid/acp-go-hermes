//go:build windows

package hermes

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func tryLockHermesFile(file *os.File) (func() error, bool, error) {
	overlapped := new(windows.Overlapped)
	err := windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
	if err == nil {
		return func() error {
			if unlockErr := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped); unlockErr != nil {
				return fmt.Errorf("unlock Hermes file: %w", unlockErr)
			}

			return nil
		}, true, nil
	}
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return nil, false, nil
	}

	return nil, false, fmt.Errorf("lock Hermes file: %w", err)
}
