package hermesacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
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
		ledger := &authLedger{dir: t.TempDir(), providerLockDir: filepath.Join(t.TempDir(), "missing")}
		if _, err := ledger.acquireProviderLease(t.Context(), "provider"); err == nil {
			t.Fatal("missing lock directory accepted")
		}
	})

	t.Run("contention timeout", func(t *testing.T) {
		ledger := &authLedger{dir: t.TempDir(), providerLockDir: t.TempDir()}
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
		file, err := os.CreateTemp(t.TempDir(), "lease")
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
			if _, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), SharedHermesHome: t.TempDir()}); err == nil {
				t.Fatalf("provider lock root %s failure ignored", name)
			}
		})
	}

	t.Run("control root", func(t *testing.T) {
		if _, err := newAuthLedger(Options{
			ProviderAuthRoot: t.TempDir(), SharedHermesHome: filepath.Join(t.TempDir(), "missing"),
		}); err == nil {
			t.Fatal("unresolvable residence control root accepted")
		}
	})
}

// TestAuthProviderLeaseFollowsResidence pins the lease to the thing it fences:
// the native credential file in the shared home, not the ledger root that only
// records the outcome.
func TestAuthProviderLeaseFollowsResidence(t *testing.T) {
	t.Run("one residence under separate ledger roots contends", func(t *testing.T) {
		home := t.TempDir()
		first, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), SharedHermesHome: home})
		if err != nil {
			t.Fatal(err)
		}
		second, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), SharedHermesHome: home})
		if err != nil {
			t.Fatal(err)
		}
		if first.dir == second.dir {
			t.Fatal("separate ledger roots shared one record root")
		}
		if first.providerLockDir != second.providerLockDir {
			t.Fatalf("lock roots %q and %q diverged for one residence", first.providerLockDir, second.providerLockDir)
		}

		lease, err := first.acquireProviderLease(t.Context(), testProviderID)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lease.Release() }()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := second.acquireProviderLease(ctx, testProviderID); err == nil {
			t.Fatal("second ledger acquired a lease on a residence already fenced")
		}
	})

	t.Run("separate residences under one ledger root do not contend", func(t *testing.T) {
		root := t.TempDir()
		first, err := newAuthLedger(Options{ProviderAuthRoot: root, SharedHermesHome: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		second, err := newAuthLedger(Options{ProviderAuthRoot: root, SharedHermesHome: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		if first.providerLockDir == second.providerLockDir {
			t.Fatal("separate residences shared one lock root")
		}

		held, err := first.acquireProviderLease(t.Context(), testProviderID)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Release() }()

		other, err := second.acquireProviderLease(t.Context(), testProviderID)
		if err != nil {
			t.Fatalf("unrelated residence blocked by another residence's lease: %v", err)
		}
		if err := other.Release(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProviderAuthCrossProcessLeaseFailures(t *testing.T) {
	t.Run("disconnect", func(t *testing.T) {
		agent, _ := newAuthAgent(t)
		agent.providerAuth.ledger = &authLedger{}
		if _, err := callLeg(t, agent, AuthDisconnectMethod, disconnectParams(testConnectionID, 1)); err == nil {
			t.Fatal("disconnect provider-lease failure ignored")
		}
	})

	t.Run("authorize", func(t *testing.T) {
		agent, client := newAuthAgent(t)
		generation := seedCatalog(t, agent, client)
		agent.providerAuth.ledger = &authLedger{}
		if _, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
			generation, testProviderID, nativehermes.AuthFlowDeviceCode, "lease-failure",
		)); err == nil {
			t.Fatal("authorize provider-lease failure ignored")
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

	ledger := &authLedger{dir: t.TempDir(), providerLockDir: t.TempDir()}
	if _, err := ledger.acquireProviderLease(t.Context(), "provider"); !errors.Is(err, wantErr) {
		t.Fatalf("provider lock error = %v", err)
	}
}
