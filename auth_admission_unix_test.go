//go:build !windows

package hermesacp

import (
	"context"
	"testing"
	"time"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestProviderAndLedgerGatesAreProviderScoped(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	broker := agent.providerAuth

	releaseProvider, ok := broker.lockProvider(context.Background(), testProviderID)
	if !ok {
		t.Fatal("provider gate acquisition failed")
	}
	defer releaseProvider()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	if release, acquired := broker.lockProvider(ctx, testProviderID); acquired || release != nil {
		t.Fatal("same-provider mutation crossed the provider gate")
	}

	other, acquired := broker.lockProvider(context.Background(), "other")
	if !acquired {
		t.Fatal("different provider was unnecessarily blocked")
	}
	other()

	releaseLedger, ok := broker.lockLedger(context.Background(), testProviderID)
	if !ok {
		t.Fatal("ledger gate acquisition failed")
	}
	defer releaseLedger()

	if release, acquired := broker.lockLedger(ctx, testProviderID); acquired || release != nil {
		t.Fatal("same-provider lineage crossed the ledger gate")
	}
}
