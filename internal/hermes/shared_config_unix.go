//go:build !windows

package hermes

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func tryLockHermesFile(file *os.File) (func() error, bool, error) {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err == nil {
		return func() error {
			if unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN); unlockErr != nil {
				return fmt.Errorf("unlock Hermes file: %w", unlockErr)
			}

			return nil
		}, true, nil
	}

	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return nil, false, nil
	}

	return nil, false, fmt.Errorf("lock Hermes file: %w", err)
}
