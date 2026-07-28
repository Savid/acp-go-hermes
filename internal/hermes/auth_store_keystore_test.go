//go:build integration

package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const (
	envRunIntegration = "ACP_GO_HERMES_RUN_INTEGRATION"
	envRunKeystore    = "ACP_GO_HERMES_RUN_KEYSTORE"

	// envSessionBus reaches the Secret Service. Whether the fixture exported one
	// is the whole difference between the two Linux configurations.
	envSessionBus = "DBUS_SESSION_BUS_ADDRESS"

	// keystoreFixtureMarker is written by the credential-residence fixture's
	// entrypoint. Seeding a live Secret Service is only safe inside that
	// container, so the Linux configurations run nowhere else.
	keystoreFixtureMarker = "/run/acp-go-hermes-keystore/marker"

	// keystoreDarwinService is a service name this test owns end to end, so the
	// macOS third never reads, overwrites, or deletes a real login item.
	keystoreDarwinService = "acp-go-hermes-residence-canary"
)

const (
	keystoreProvider    = "anthropic"
	keystoreSlotLabel   = "acp-go-hermes-canary"
	keystoreFileCanary  = "canary-file-store-token"
	keystoreStoreCanary = "canary-keystore-token"
)

// TestKeystoreResidenceMatrix proves the identity hermes' residence answer rests
// on: the store under HERMES_HOME is the sole authority, and an item sitting in
// the platform keystore under this provider's name is neither preferred while
// that store answers nor fallen back to once it is gone. The same body runs in
// all three configurations — Linux with a session bus, Linux without one, and
// macOS against the login keychain — so they are asserted to agree rather than
// assumed to.
func TestKeystoreResidenceMatrix(t *testing.T) {
	requireResidenceTier(t)
	seedResidenceKeystore(t)

	home := t.TempDir()

	err := AuthWriteSlot(home, keystoreProvider, keystoreSlotLabel, AuthMaterial{
		AuthType:    "oauth",
		AccessToken: keystoreFileCanary,
	})
	if err != nil {
		t.Fatalf("write the reserved slot: %v", err)
	}

	material, present, err := AuthReadSlot(home, keystoreProvider, keystoreSlotLabel)
	if err != nil || !present {
		t.Fatalf("the store under HERMES_HOME answered nothing: %v/%v", present, err)
	}

	if material.AccessToken != keystoreFileCanary {
		t.Fatalf("the read path answered %q, want the store's %q", material.AccessToken, keystoreFileCanary)
	}

	// Removing the store leaves the keystore item alone and the slot absent. A
	// read path with a keystore branch would answer the seeded value here.
	if err := os.Remove(filepath.Join(home, authStoreFile)); err != nil {
		t.Fatal(err)
	}

	material, present, err = AuthReadSlot(home, keystoreProvider, keystoreSlotLabel)
	if err != nil || present {
		t.Fatalf("an absent store did not answer absence: %v/%v/%v", material.AccessToken, present, err)
	}

	if !residenceKeystoreSeeded() {
		return
	}

	// The seeded item is still there and still readable, which is what makes the
	// absence above an answer about the read path rather than about the keystore.
	if seeded := lookupResidenceCanary(t); seeded != keystoreStoreCanary {
		t.Fatalf("the seeded keystore item read back %q, want %q", seeded, keystoreStoreCanary)
	}
}

// requireResidenceTier answers to both tier gates. On Linux it additionally
// requires the fixture container: planting a canary in a developer's live
// Secret Service is not something a test may do, and the container is where the
// driver runs this binary once per Linux configuration.
func requireResidenceTier(t *testing.T) {
	t.Helper()

	if os.Getenv(envRunIntegration) != "1" || os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 and %s=1 to run the credential-residence matrix",
			envRunIntegration, envRunKeystore)
	}

	if runtime.GOOS == "darwin" {
		return
	}

	if _, err := os.Stat(keystoreFixtureMarker); err != nil {
		t.Skipf("the Linux configurations run inside the keystore fixture container: %v", err)
	}
}

// residenceKeystoreSeeded reports whether this configuration has a platform
// keystore holding a canary. Only the keystore-absent Linux configuration has
// none, which is the configuration it exists to describe.
func residenceKeystoreSeeded() bool {
	return runtime.GOOS == "darwin" || os.Getenv(envSessionBus) != ""
}

// seedResidenceKeystore plants canary material through the platform tool rather
// than through the read path, so the assertion is not a round trip of one
// library against itself.
func seedResidenceKeystore(t *testing.T) {
	t.Helper()

	if runtime.GOOS == "darwin" {
		// The login keychain is the only keychain a writing harness may be
		// pointed at: a write under a scratch HOME blocks forever on an
		// interactive modal. The service name belongs to this test, so the item
		// is one it created and one it deletes.
		seed := exec.Command("security", "add-generic-password",
			"-U", "-s", keystoreDarwinService, "-a", keystoreProvider, "-w", keystoreStoreCanary)

		if output, err := seed.CombinedOutput(); err != nil {
			t.Fatalf("seed the login keychain canary: %v: %s", err, output)
		}

		t.Cleanup(func() {
			remove := exec.Command("security", "delete-generic-password",
				"-s", keystoreDarwinService, "-a", keystoreProvider)

			if output, err := remove.CombinedOutput(); err != nil {
				t.Errorf("delete the login keychain canary: %v: %s", err, output)
			}
		})

		return
	}

	if os.Getenv(envSessionBus) == "" {
		t.Logf("%s is unset: this is the keystore-absent configuration", envSessionBus)

		return
	}

	command := exec.Command("secret-tool", "store", "--label=hermes-canary",
		"service", "hermes", "account", keystoreProvider)
	command.Stdin = strings.NewReader(keystoreStoreCanary)

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("seed keystore canary: %v: %s", err, output)
	}
}

// lookupResidenceCanary reads the seeded item back through the platform tool.
func lookupResidenceCanary(t *testing.T) string {
	t.Helper()

	command := exec.Command("secret-tool", "lookup", "service", "hermes", "account", keystoreProvider)
	if runtime.GOOS == "darwin" {
		command = exec.Command("security", "find-generic-password",
			"-w", "-s", keystoreDarwinService, "-a", keystoreProvider)
	}

	output, err := command.Output()
	if err != nil {
		t.Fatalf("look up keystore canary: %v", err)
	}

	return strings.TrimSpace(string(output))
}
