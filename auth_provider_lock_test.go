package hermesacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthProviderLeaseEdges(t *testing.T) {
	var nilLease *authProviderLease
	if err := nilLease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := (*authLedger)(nil).acquireProviderLease(t.Context(), "provider"); err == nil {
		t.Fatal("nil ledger acquired provider lease")
	}

	t.Run("open lock", func(t *testing.T) {
		ledger := &authLedger{dir: durableTempDir(t), providerLockDir: filepath.Join(durableTempDir(t), "missing")}
		if _, err := ledger.acquireProviderLease(t.Context(), "provider"); err == nil {
			t.Fatal("missing lock directory accepted")
		}
	})

	t.Run("contention timeout", func(t *testing.T) {
		ledger := &authLedger{dir: durableTempDir(t), providerLockDir: durableTempDir(t)}
		first, err := ledger.acquireProviderLease(t.Context(), "provider")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = first.Release() }()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := ledger.acquireProviderLease(ctx, "provider"); err == nil {
			t.Fatal("contended canceled acquisition succeeded")
		}
	})

	t.Run("unlock and close errors join", func(t *testing.T) {
		file, err := os.CreateTemp(durableTempDir(t), "lease")
		if err != nil {
			t.Fatal(err)
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		unlockErr := errors.New("unlock")
		lease := &authProviderLease{file: file, unlock: func() error { return unlockErr }}
		err = lease.Release()
		if !errors.Is(err, unlockErr) || !strings.Contains(err.Error(), "file already closed") {
			t.Fatalf("release error=%v", err)
		}
		if second := lease.Release(); !errors.Is(second, unlockErr) {
			t.Fatalf("idempotent release=%v", second)
		}
	})
}

func TestAuthLedgerProviderLockRootFailures(t *testing.T) {
	for name, apply := range map[string]func(){
		"mkdir": func() {
			ledgerMkdirAll = func(path string, mode os.FileMode) error {
				if filepath.Base(path) == authProviderLockDir {
					return errors.New("lock mkdir")
				}

				return os.MkdirAll(path, mode)
			}
		},
		"chmod": func() {
			ledgerChmod = func(path string, mode os.FileMode) error {
				if filepath.Base(path) == authProviderLockDir {
					return errors.New("lock chmod")
				}

				return os.Chmod(path, mode)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			restoreLedgerHooks(t)
			apply()
			if _, err := newAuthLedger(Options{ProviderAuthRoot: durableTempDir(t), SharedHermesHome: durableTempDir(t)}); err == nil {
				t.Fatalf("provider lock root %s failure ignored", name)
			}
		})
	}

	t.Run("control root", func(t *testing.T) {
		if _, err := newAuthLedger(Options{
			ProviderAuthRoot: durableTempDir(t), SharedHermesHome: filepath.Join(durableTempDir(t), "missing"),
		}); err == nil {
			t.Fatal("unresolvable residence control root accepted")
		}
	})
}

// TestAcquireProviderLeaseSurfacesLockSyscallFailure separates a failed lock
// syscall from ordinary contention: contention retries until the deadline,
// while a syscall failure must abort the acquisition immediately.
func TestAcquireProviderLeaseSurfacesLockSyscallFailure(t *testing.T) {
	wantErr := errors.New("lock syscall")
	previous := authTryProviderFileLock
	authTryProviderFileLock = func(*os.File) (func() error, bool, error) {
		return nil, false, wantErr
	}
	t.Cleanup(func() { authTryProviderFileLock = previous })

	ledger := &authLedger{dir: durableTempDir(t), providerLockDir: durableTempDir(t)}
	if _, err := ledger.acquireProviderLease(t.Context(), "provider"); !errors.Is(err, wantErr) {
		t.Fatalf("provider lock error = %v", err)
	}
}
