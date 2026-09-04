package hermes

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	sharedSessionOwnersDir  = ".acp-go-hermes-session-owners"
	sharedOwnerLockAttempts = 3
)

var errSharedOwnerLockReplaced = errors.New("shared Hermes owner lock was replaced during acquisition")

type sharedOwnerLockKind struct {
	label  string
	active string
}

var (
	sharedSessionOwnerKind = sharedOwnerLockKind{
		label:  "session-owner",
		active: "shared Hermes session is already active",
	}
	sharedHomeOwnerKind = sharedOwnerLockKind{
		label:  "home-root",
		active: "shared Hermes home root is already claimed by a live writer",
	}
)

type SharedSessionOwner struct {
	lockPath string
	file     *os.File
	unlock   func() error
	once     sync.Once
	err      error
}

var (
	sharedOwnerChmod     = os.Chmod
	sharedOwnerFileChmod = (*os.File).Chmod
	sharedOwnerFileStat  = (*os.File).Stat
	sharedOwnerLstat     = os.Lstat
	sharedOwnerTryLock   = tryLockHermesFile
)

func acquireSharedSessionOwner(home string, kind string, id string) (*SharedSessionOwner, error) {
	if id == "" {
		return nil, fmt.Errorf("shared Hermes home requires a non-empty %s session id", kind)
	}

	control, err := EnsureSharedHermesAdapterControlDir(home)
	if err != nil {
		return nil, err
	}

	directory := filepath.Join(control, sharedSessionOwnersDir)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create shared Hermes session-owner directory: %w", err)
	}

	if err := sharedOwnerChmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("protect shared Hermes session-owner directory: %w", err)
	}

	digest := sha256.Sum256([]byte(kind + "\x00" + id))

	return acquireSharedOwnerLock(filepath.Join(directory, hex.EncodeToString(digest[:]))+".lock", sharedSessionOwnerKind)
}

func acquireSharedOwnerLock(lockPath string, kind sharedOwnerLockKind) (*SharedSessionOwner, error) {
	for range sharedOwnerLockAttempts {
		owner, err := tryAcquireSharedOwnerLock(lockPath, kind)
		if !errors.Is(err, errSharedOwnerLockReplaced) {
			return owner, err
		}
	}

	return nil, fmt.Errorf("shared Hermes %s lock was replaced during every acquisition attempt", kind.label)
}

func tryAcquireSharedOwnerLock(lockPath string, kind sharedOwnerLockKind) (*SharedSessionOwner, error) {
	file, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open shared Hermes %s lock: %w", kind.label, err)
	}

	if chmodErr := sharedOwnerFileChmod(file, 0o600); chmodErr != nil {
		return nil, errors.Join(fmt.Errorf("protect shared Hermes %s lock: %w", kind.label, chmodErr), file.Close())
	}

	unlock, acquired, err := sharedOwnerTryLock(file)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}

	if !acquired {
		return nil, errors.Join(errors.New(kind.active), file.Close())
	}

	if err := verifySharedOwnerLockPath(file, lockPath, kind); err != nil {
		return nil, errors.Join(err, unlock(), file.Close())
	}

	return &SharedSessionOwner{lockPath: lockPath, file: file, unlock: unlock}, nil
}

func verifySharedOwnerLockPath(file *os.File, lockPath string, kind sharedOwnerLockKind) error {
	locked, err := sharedOwnerFileStat(file)
	if err != nil {
		return fmt.Errorf("inspect shared Hermes %s lock: %w", kind.label, err)
	}

	named, err := sharedOwnerLstat(lockPath)
	if err != nil || !os.SameFile(locked, named) {
		return errSharedOwnerLockReplaced
	}

	return nil
}

func acquireSharedACPSessionOwner(home string, id ACPSessionIDString) (*SharedSessionOwner, error) {
	return acquireSharedSessionOwner(home, "ACP", string(id))
}

func AcquireSharedACPSessionOwner(home string, id ACPSessionIDString) (*SharedSessionOwner, error) {
	return acquireSharedACPSessionOwner(home, id)
}

func AcquireSharedNativeSessionOwner(home string, id string) (*SharedSessionOwner, error) {
	return acquireSharedSessionOwner(home, "native", id)
}

func (o *SharedSessionOwner) Release() error {
	if o == nil {
		return nil
	}

	o.once.Do(func() {
		if o.file == nil || o.unlock == nil {
			o.err = errors.New("shared Hermes owner lock is unavailable")

			return
		}

		o.err = errors.Join(os.Remove(o.lockPath), o.unlock(), o.file.Close())
		o.file = nil
	})

	return o.err
}
