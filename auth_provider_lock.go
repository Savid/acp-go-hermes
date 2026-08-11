package hermesacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const authProviderLockDir = ".provider-locks"

var authTryProviderFileLock = tryAuthProviderFileLock

// authProviderLease is the cross-Agent/process mutation fence for one provider
// in one provider-auth residence. It lives under the host-owned ledger root,
// never the native-writable Hermes home. OAuth flows retain it until their
// terminal native mutation and ledger transition have both settled.
type authProviderLease struct {
	file   *os.File
	unlock func() error
	once   sync.Once
	err    error
}

func (l *authProviderLease) Release() error {
	if l == nil {
		return nil
	}

	l.once.Do(func() {
		l.err = errors.Join(l.unlock(), l.file.Close())
	})

	return l.err
}

func (l *authLedger) acquireProviderLease(ctx context.Context, providerID string) (*authProviderLease, error) {
	if l == nil || l.dir == "" {
		return nil, errors.New("provider auth lock root is unavailable")
	}

	lockDir := l.providerLockDir
	if lockDir == "" {
		// Test and embedded ledgers constructed before this field existed still
		// derive the same host-owned child. Production construction creates and
		// protects it eagerly.
		lockDir = filepath.Join(l.dir, authProviderLockDir)
		if err := os.MkdirAll(lockDir, authLedgerDirMode); err != nil {
			return nil, fmt.Errorf("create provider auth lock root: %w", err)
		}
	}

	digest := sha256.Sum256([]byte(providerID))
	path := filepath.Join(lockDir, hex.EncodeToString(digest[:])+".lock")

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, authLedgerFileMode)
	if err != nil {
		return nil, fmt.Errorf("open provider auth lock: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	for {
		unlock, acquired, lockErr := authTryProviderFileLock(file)
		if lockErr != nil {
			return nil, errors.Join(fmt.Errorf("lock provider auth residence: %w", lockErr), file.Close())
		}

		if acquired {
			return &authProviderLease{file: file, unlock: unlock}, nil
		}

		select {
		case <-waitCtx.Done():
			return nil, errors.Join(fmt.Errorf("lock provider auth residence: %w", context.Cause(waitCtx)), file.Close())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (p *providerAuth) releaseFlowProviderLease(flow *authFlow) {
	if flow != nil && flow.providerLease != nil {
		_ = flow.providerLease.Release()
	}
}
