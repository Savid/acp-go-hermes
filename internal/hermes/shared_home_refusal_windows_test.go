//go:build windows

package hermes

import (
	"strings"
	"testing"
)

const windowsSharedHomeRefusal = "shared Hermes home is unsupported on windows"

// TestSharedAdapterControlDirRefusesOnWindows pins the one gate every
// shared-home path passes through. Preparing the adapter control root beside a
// shared HERMES_HOME is refused here, which is why no shared-home config
// transaction, session-set fence, or owner claim can be taken on Windows.
func TestSharedAdapterControlDirRefusesOnWindows(t *testing.T) {
	control, err := EnsureSharedHermesAdapterControlDir(durableTempDir(t))
	if err == nil {
		t.Fatal("windows prepared a shared adapter control root")
	}

	if control != "" {
		t.Fatalf("refused control root was returned anyway: %s", control)
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("control root refusal = %v", err)
	}
}

// TestSharedHomeOwnerRefusesOnWindows pins that the exclusive claim on a
// durable HERMES_HOME root is refused before any lock file is created, so a
// second adapter cannot mistake an absent claim for a free root.
func TestSharedHomeOwnerRefusesOnWindows(t *testing.T) {
	owner, err := AcquireSharedHomeOwner(durableTempDir(t))
	if err == nil {
		t.Fatal("windows claimed a shared Hermes home root")
	}

	if owner != nil {
		t.Fatal("refused home owner was returned anyway")
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("home owner refusal = %v", err)
	}
}

// TestStartServerRefusesASharedHomeOnWindows pins the refusal at the launch
// boundary a host reaches: a server asked for a shared home never starts here,
// and says so, rather than quietly starting an isolated one instead.
func TestStartServerRefusesASharedHomeOnWindows(t *testing.T) {
	scratch := durableTempDir(t)

	server, err := StartServer(t.Context(), StartOptions{
		ACPSessionID:     "windows-shared-home",
		ScratchParent:    scratch,
		Cwd:              scratch,
		ExecutablePath:   "hermes",
		SharedHermesHome: durableTempDir(t),
	})
	if err == nil {
		t.Fatal("windows started a shared-home Hermes server")
	}

	if server != nil {
		t.Fatal("refused server was returned anyway")
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("shared-home start refusal = %v", err)
	}
}
