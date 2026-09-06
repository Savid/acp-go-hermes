//go:build !windows

package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestResidualStartServerOwnershipAndLockFailures(t *testing.T) {
	home := durableTempDir(t)
	if _, err := EnsureSharedHermesAdapterControlDir(home); err != nil {
		t.Fatal(err)
	}
	file, unlock, acquired := rawSharedHomeLock(t, home)
	if !acquired {
		t.Fatal("fixture did not acquire shared home lock")
	}
	options := StartOptions{
		ACPSessionID: "owner-refusal", Cwd: durableTempDir(t), ExistingXDG: testXDGDirs(t),
		SharedHermesHome: home, ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
	}
	if server, err := StartServer(t.Context(), options); err == nil || server != nil {
		t.Fatalf("claimed home server start = %#v, %v", server, err)
	}
	if err := errors.Join(unlock(), file.Close()); err != nil {
		t.Fatal(err)
	}

	controlDir := durableTempDir(t)
	if err := os.Mkdir(filepath.Join(controlDir, "server.lock"), 0o700); err != nil {
		t.Fatal(err)
	}
	options = StartOptions{
		ACPSessionID: "control-refusal", Cwd: durableTempDir(t), ExistingXDG: testXDGDirs(t),
		ControlDir: controlDir, ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
	}
	if server, err := StartServer(t.Context(), options); err == nil || server != nil {
		t.Fatalf("blocked control lock server start = %#v, %v", server, err)
	}
}
