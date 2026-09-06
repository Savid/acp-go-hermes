package hermes

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	sharedHermesControlSuffix = ".acp-go-hermes-control"
	sharedSessionSetLockName  = "session-set.lock"
)

// SharedSessionSetLockMode describes the cross-process native-session-set
// fence. Turns hold a shared claim because Hermes may rotate/compress a
// session while handling a prompt. Session creation, fork and recovery hold an
// exclusive claim so a failed native mutation can be reconciled against a
// stable persisted-session inventory.
type SharedSessionSetLockMode uint8

const (
	SharedSessionSetLockShared SharedSessionSetLockMode = iota + 1
	SharedSessionSetLockExclusive
)

// SharedSessionSetLock is an OS-held advisory lock. The claim is released by
// the kernel if the adapter process exits without calling Release.
type SharedSessionSetLock struct {
	file   *os.File
	unlock func() error
	once   sync.Once
	err    error
}

var (
	sharedHomeLocalValidator  = EnsureLocalSharedHermesHome
	sharedSessionSetFileChmod = (*os.File).Chmod
	sharedSessionSetTryLock   = tryLockSharedSessionSetFile
	sharedSessionSetLstat     = os.Lstat
	sharedSessionSetChmod     = os.Chmod
	sharedSessionSetSyncDir   = syncSharedHermesDirectory
)

// SharedHermesAdapterControlDir returns the adapter-owned durable control root
// paired with, but deliberately outside, an official HERMES_HOME.
func SharedHermesAdapterControlDir(home string) (string, error) {
	if home == "" || !filepath.IsAbs(home) || filepath.Clean(home) != home || home == string(filepath.Separator) {
		return "", errors.New("shared Hermes home must be a clean non-root absolute path")
	}

	resolved, err := filepath.EvalSymlinks(home)
	if err != nil {
		return "", fmt.Errorf("resolve shared Hermes home for adapter control: %w", err)
	}

	return resolved + sharedHermesControlSuffix, nil
}

// EnsureSharedHermesAdapterControlDir creates and validates the adapter-owned
// control root without ever placing control material beneath HERMES_HOME.
func EnsureSharedHermesAdapterControlDir(home string) (string, error) {
	control, err := SharedHermesAdapterControlDir(home)
	if err != nil {
		return "", err
	}

	if err := ensureSharedHermesAdapterControlDir(control); err != nil {
		return "", err
	}

	if err := sharedHomeLocalValidator(control); err != nil {
		return "", fmt.Errorf("validate shared Hermes adapter control filesystem: %w", err)
	}

	return control, nil
}

// AcquireSharedSessionSetLock waits, subject to ctx, for one shared or
// exclusive session-set claim. Windows refuses shared-home use until its lock
// backend can prove equivalent shared/exclusive and crash-release semantics.
func AcquireSharedSessionSetLock(ctx context.Context, home string, mode SharedSessionSetLockMode) (*SharedSessionSetLock, error) {
	if ctx == nil {
		return nil, errors.New("shared Hermes session-set lock requires a context")
	}

	if mode != SharedSessionSetLockShared && mode != SharedSessionSetLockExclusive {
		return nil, errors.New("shared Hermes session-set lock mode is invalid")
	}

	control, err := EnsureSharedHermesAdapterControlDir(home)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filepath.Join(control, sharedSessionSetLockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open shared Hermes session-set lock: %w", err)
	}

	if err := sharedSessionSetFileChmod(file, 0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("protect shared Hermes session-set lock: %w", err), file.Close())
	}

	for {
		unlock, acquired, lockErr := sharedSessionSetTryLock(file, mode)
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

			return nil, errors.Join(fmt.Errorf("lock shared Hermes session set: %w", context.Cause(ctx)), file.Close())
		case <-timer.C:
		}
	}
}

func ensureSharedHermesAdapterControlDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create shared Hermes adapter control directory: %w", err)
	}

	info, err := sharedSessionSetLstat(path)
	if err != nil {
		return fmt.Errorf("inspect shared Hermes adapter control directory: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("shared Hermes adapter control path is not a directory")
	}

	if err := sharedSessionSetChmod(path, 0o700); err != nil {
		return fmt.Errorf("protect shared Hermes adapter control directory: %w", err)
	}

	if err := sharedSessionSetSyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync shared Hermes adapter control parent: %w", err)
	}

	return nil
}

// Release drops the session-set claim exactly once.
func (l *SharedSessionSetLock) Release() error {
	if l == nil {
		return nil
	}

	l.once.Do(func() {
		l.err = errors.Join(l.unlock(), l.file.Close())
	})

	return l.err
}
