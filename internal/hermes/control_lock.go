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
	return acquireServerControlLockWithOps(
		ctx,
		dir,
		os.OpenFile,
		(*os.File).Chmod,
		tryLockServerControlFile,
	)
}

// tryLockServerControlFile fences one Hermes server generation's control
// directory. The claim is always exclusive and the OS releases it when the
// adapter exits, so it needs only the single-writer file lock every supported
// platform provides. It deliberately does not reuse the session-set backend:
// that one also has to offer a shared claim, which is why Windows refuses it,
// and a server generation never asks for one.
func tryLockServerControlFile(file *os.File, _ SharedSessionSetLockMode) (func() error, bool, error) {
	return tryLockHermesFile(file)
}

func acquireServerControlLockWithOps(
	ctx context.Context,
	dir string,
	openFile func(string, int, os.FileMode) (*os.File, error),
	chmodFile func(*os.File, os.FileMode) error,
	tryLock func(*os.File, SharedSessionSetLockMode) (func() error, bool, error),
) (*SharedSessionSetLock, error) {
	file, err := openFile(filepath.Join(dir, "server.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Hermes server control lock: %w", err)
	}

	if err := chmodFile(file, 0o600); err != nil {
		return nil, errors.Join(err, file.Close())
	}

	for {
		unlock, acquired, lockErr := tryLock(file, SharedSessionSetLockExclusive)
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
