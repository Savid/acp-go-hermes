package hermesacp

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestAuthAcquireGateSerializesAndHonorsCancellation(t *testing.T) {
	t.Parallel()

	var gates = map[string]*authGate{}
	var brokerMutex sync.Mutex

	firstRelease, ok := authAcquireGate(context.Background(), &brokerMutex, gates, "provider")
	if !ok {
		t.Fatal("first gate acquisition failed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if release, acquired := authAcquireGate(ctx, &brokerMutex, gates, "provider"); acquired || release != nil {
		t.Fatal("cancelled waiter acquired a held gate")
	}

	firstRelease()

	release, ok := authAcquireGate(context.Background(), &brokerMutex, gates, "provider")
	if !ok {
		t.Fatal("gate did not reopen")
	}
	release()

	if len(gates) != 0 {
		t.Fatalf("idle gate leaked: %#v", gates)
	}
}

func TestWaitForAuthGateAcquiresAfterRelease(t *testing.T) {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	acquired := make(chan bool, 1)
	go func() {
		acquired <- waitForAuthGate(context.Background(), ch)
	}()
	<-ch
	if !<-acquired {
		t.Fatal("waiter did not acquire the released gate")
	}
	<-ch
}

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
