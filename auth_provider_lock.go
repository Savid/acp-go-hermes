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

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const authProviderLockDir = ".provider-locks"

var authTryProviderFileLock = tryAuthProviderFileLock

// authProviderLease is the cross-Agent/process mutation fence for one provider
// in one provider-auth residence. It is keyed by the residence it protects
// rather than by the ledger root that records the outcome: the native auth.json
// lives in the shared home, so two Agents naming that home queue behind one
// lease however many distinct ledger roots they were configured with. The lock
// sits in the adapter-owned control root beside the home, never inside the
// native-writable home itself. OAuth flows retain it until their terminal
// native mutation and ledger transition have both settled.
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

// providerAuthLockRoot prepares the per-residence lock root. The control root
// derivation resolves symlinks, so two spellings of one home converge on one
// directory and therefore on one lease per provider.
func providerAuthLockRoot(residence string) (string, error) {
	control, err := nativehermes.EnsureSharedHermesAdapterControlDir(residence)
	if err != nil {
		return "", fmt.Errorf("prepare provider auth lock control root: %w", err)
	}

	dir := filepath.Join(control, authProviderLockDir)
	if err := ledgerMkdirAll(dir, authLedgerDirMode); err != nil {
		return "", fmt.Errorf("create provider auth lock root: %w", err)
	}

	if err := ledgerChmod(dir, authLedgerDirMode); err != nil {
		return "", fmt.Errorf("restrict provider auth lock root: %w", err)
	}

	return dir, nil
}

func (l *authLedger) acquireProviderLease(ctx context.Context, providerID string) (*authProviderLease, error) {
	if l == nil || l.providerLockDir == "" {
		return nil, errors.New("provider auth lock root is unavailable")
	}

	digest := sha256.Sum256([]byte(providerID))
	path := filepath.Join(l.providerLockDir, hex.EncodeToString(digest[:])+".lock")

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
