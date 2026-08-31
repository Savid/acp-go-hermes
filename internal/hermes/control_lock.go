package hermes

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func acquireServerControlLock(ctx context.Context, dir string) (*SharedSessionSetLock, error) {
	file, err := os.OpenFile(filepath.Join(dir, "server.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Hermes server control lock: %w", err)
	}

	if err := file.Chmod(0o600); err != nil {
		return nil, errors.Join(err, file.Close())
	}

	for {
		unlock, acquired, lockErr := tryLockSharedSessionSetFile(file, SharedSessionSetLockExclusive)
		if lockErr != nil {
			return nil, errors.Join(lockErr, file.Close())
		}

		if acquired {
			return &SharedSessionSetLock{file: file, unlock: unlock}, nil
		}

		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			_ = timer.Stop()

			return nil, errors.Join(ctx.Err(), file.Close())
		case <-timer.C:
		}
	}
}
