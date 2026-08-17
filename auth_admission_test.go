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

// TestAuthAcquireGateAdmitsALegThatActuallyQueued proves a leg the fast path
// refused is admitted through the queue once the holder leaves, and that the
// handle it gets back is a real hold on the gate rather than a bare "true".
//
// The comment above the non-blocking send states the invariant this covers: an
// uncontended gate is taken immediately, so a leg only ever waits on a queue it
// actually joined. That makes the queued admission — refused by the fast path,
// then granted by waitForAuthGate — a distinct outcome from the fast-path
// admission, and the only one that proves the queue hands the gate over rather
// than merely letting the leg retry.
//
// Reaching it is a scheduling question, so the case does not guess. It fills the
// gate first, which makes the fast-path send impossible rather than unlikely,
// then detects that the leg is genuinely parked in waitForAuthGate: receiving
// from a full buffered channel that has a blocked sender refills the buffer as
// part of the receive, so len(ch) == 1 immediately after the receive is a direct
// observation that a sender was already queued, not a timing guess. An attempt
// that did not park is retried rather than asserted on, and the case fails if it
// never observes the queued admission at all — so it can never pass by silently
// exercising the fast path instead.
func TestAuthAcquireGateAdmitsALegThatActuallyQueued(t *testing.T) {
	t.Parallel()

	for attempt := range 200 {
		gates := map[string]*authGate{}

		var brokerMutex sync.Mutex

		holderRelease, ok := authAcquireGate(context.Background(), &brokerMutex, gates, "provider")
		if !ok {
			t.Fatal("holder did not take the uncontended gate")
		}

		gate := gates["provider"]
		admitted := make(chan func(), 1)

		go func() {
			release, acquired := authAcquireGate(context.Background(), &brokerMutex, gates, "provider")
			if !acquired {
				admitted <- nil

				return
			}
			admitted <- release
		}()

		for {
			brokerMutex.Lock()
			waiters := gate.waiters
			brokerMutex.Unlock()

			if waiters == 2 {
				break
			}
		}

		// Drain the holder's slot by hand so the refill is observable. A parked
		// sender refills the buffer as part of this receive; a leg that has not
		// reached waitForAuthGate yet leaves it empty.
		<-gate.ch
		parked := len(gate.ch) == 1

		if !parked {
			// The leg took the fast path this time. Let it finish, hand the
			// drained slot back so the holder's own release can complete, and
			// retry.
			if release := <-admitted; release != nil {
				release()
			}

			gate.ch <- struct{}{}
			holderRelease()

			if attempt == 199 {
				t.Fatal("never observed a leg admitted through the queue")
			}

			continue
		}

		release := <-admitted
		if release == nil {
			t.Fatal("queued leg was refused by a gate that had just been released")
		}

		// The queued leg holds the gate: its slot is occupied, and the handle it
		// was given is what frees it. A "true" that did not carry the hold would
		// leave the gate takeable by a third leg right now.
		if len(gate.ch) != 1 {
			t.Fatal("queued leg reported admission without holding the gate")
		}

		release()

		if len(gate.ch) != 0 {
			t.Fatal("the queued leg's release did not free the gate")
		}

		// Settle the holder's own bookkeeping: its slot was drained by hand
		// above, so hand one back for the release it still owes.
		gate.ch <- struct{}{}
		holderRelease()

		if len(gates) != 0 {
			t.Fatalf("gate leaked after the last leg left: %#v", gates)
		}

		return
	}
}
