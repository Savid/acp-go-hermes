//go:build !windows

package hermesacp

import (
	"errors"
	"os"
	"strings"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestManagedHermesServerSnapshotOwnerResidualBranches(t *testing.T) {
	successOwner, err := nativehermes.AcquireSharedNativeSessionOwner(t.TempDir(), "success")
	if err != nil {
		t.Fatal(err)
	}
	success := &managedHermesServer{
		Server: newFakeHermesClient(), managed: true, root: t.TempDir(), nativeSessionOwner: successOwner,
	}
	if _, reclaimErr := success.reclaimForSnapshot(t.Context()); reclaimErr != nil || !success.ownerReleased {
		t.Fatalf("successful owner reclaim = %v, released=%v", reclaimErr, success.ownerReleased)
	}

	home := t.TempDir()
	failingOwner, err := nativehermes.AcquireSharedNativeSessionOwner(home, "failure")
	if err != nil {
		t.Fatal(err)
	}
	control, err := nativehermes.SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(control); err != nil {
		t.Fatal(err)
	}
	failing := &managedHermesServer{
		Server: newFakeHermesClient(), managed: true, root: t.TempDir(), nativeSessionOwner: failingOwner,
	}
	if _, err := failing.reclaimForSnapshot(t.Context()); err == nil || failing.ownerReleased {
		t.Fatalf("failed owner reclaim = %v, released=%v", err, failing.ownerReleased)
	}
}

func TestManagedHermesOwnershipAndScratchResidualBranches(t *testing.T) {
	agent := NewAgent()
	agent.retainIncompleteHermesRoot("session", "/retained")
	if err := agent.rejectIncompleteHermesSession("session"); !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("retained session rejection = %v", err)
	}
	if root := hermesServerRoot(nil); root != "" {
		t.Fatalf("nil server root = %q", root)
	}

	home := t.TempDir()
	shared := NewAgent(WithSharedHermesHome(home))
	if shared.optionsErr != nil {
		t.Fatal(shared.optionsErr)
	}
	if err := shared.claimSharedNativeSession(newFakeHermesClient(), "native"); err == nil || !strings.Contains(err.Error(), "requires a managed server") {
		t.Fatalf("non-managed native claim = %v", err)
	}

	released := false
	if err := deleteHermesScratchRoot("", func() { released = true }); err != nil || released {
		t.Fatalf("empty scratch deletion = %v, released=%v", err, released)
	}
	originalRemoveAll := managedRemoveAll
	t.Cleanup(func() { managedRemoveAll = originalRemoveAll })
	managedRemoveAll = func(string) error { return errors.New("remove refused") }
	if err := deleteHermesScratchRoot(t.TempDir(), func() { released = true }); err == nil || released {
		t.Fatalf("failed scratch deletion = %v, released=%v", err, released)
	}
}
