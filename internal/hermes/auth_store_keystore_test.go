//go:build integration

package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// keystoreFixtureMarker is written by the credential-residence fixture's
// entrypoint. The matrix answers only inside that container, where a live
// Secret Service and its session bus are.
const keystoreFixtureMarker = "/run/acp-go-hermes-keystore/marker"

const (
	keystoreProvider    = "anthropic"
	keystoreSlotLabel   = "acp-go-hermes-canary"
	keystoreFileCanary  = "canary-file-store-token"
	keystoreStoreCanary = "canary-keystore-token"
)

// TestKeystoreResidenceMatrix proves the identity hermes' residence answer rests
// on: the store under HERMES_HOME is the sole authority, and an item sitting in
// a live Secret Service under this provider's name is neither preferred while
// that store answers nor fallen back to once it is gone. The same body runs with
// the session bus present and with it absent, so the two Linux configurations
// are asserted to agree rather than assumed to.
func TestKeystoreResidenceMatrix(t *testing.T) {
	if _, err := os.Stat(keystoreFixtureMarker); err != nil {
		t.Skip("the credential-residence matrix runs inside the keystore fixture container")
	}

	secretService := os.Getenv("DBUS_SESSION_BUS_ADDRESS") != ""
	if secretService {
		seedKeystoreCanary(t, keystoreStoreCanary)
	}

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

	if !secretService {
		return
	}

	// The seeded item is still there and still readable, which is what makes the
	// absence above an answer about the read path rather than about the service.
	if seeded := lookupKeystoreCanary(t); seeded != keystoreStoreCanary {
		t.Fatalf("the seeded keystore item read back %q, want %q", seeded, keystoreStoreCanary)
	}
}

// seedKeystoreCanary plants canary material through the platform tool rather
// than through the read path, so the assertion is not a round trip of one
// library against itself.
func seedKeystoreCanary(t *testing.T, contents string) {
	t.Helper()

	command := exec.Command("secret-tool", "store", "--label=hermes-canary",
		"service", "hermes", "account", keystoreProvider)
	command.Stdin = strings.NewReader(contents)

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("seed keystore canary: %v: %s", err, output)
	}
}

func lookupKeystoreCanary(t *testing.T) string {
	t.Helper()

	output, err := exec.Command("secret-tool", "lookup", "service", "hermes", "account", keystoreProvider).Output()
	if err != nil {
		t.Fatalf("look up keystore canary: %v", err)
	}

	return string(output)
}
