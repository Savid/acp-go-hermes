//go:build !windows

package hermesacp

import (
	"context"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

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
