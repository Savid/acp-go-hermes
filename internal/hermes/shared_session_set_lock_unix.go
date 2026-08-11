//go:build unix

package hermes

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func tryLockSharedSessionSetFile(file *os.File, mode SharedSessionSetLockMode) (func() error, bool, error) {
	how := unix.LOCK_SH | unix.LOCK_NB
	if mode == SharedSessionSetLockExclusive {
		how = unix.LOCK_EX | unix.LOCK_NB
	}

	err := unix.Flock(int(file.Fd()), how)
	if err == nil {
		return func() error {
			if unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN); unlockErr != nil {
				return fmt.Errorf("unlock shared Hermes session set: %w", unlockErr)
			}

			return nil
		}, true, nil
	}

	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return nil, false, nil
	}

	return nil, false, fmt.Errorf("lock shared Hermes session set: %w", err)
}
