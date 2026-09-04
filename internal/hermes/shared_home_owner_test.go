//go:build !windows

// Every test in this file drives the shared Hermes home. Windows refuses that
// home — its inherited session-owner lock handles are unavailable — so the
// surface these tests exercise does not exist there; the Windows expectation is
// the refusal itself, proven once beside the code that makes it.

package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func rawSharedHomeLock(t *testing.T, home string) (*os.File, func() error, bool) {
	t.Helper()

	control, err := SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(filepath.Join(control, sharedHomeOwnerBaseName+".lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	unlock, acquired, err := tryLockHermesFile(file)
	if err != nil {
		t.Fatal(err)
	}

	return file, unlock, acquired
}

func TestSharedHomeOwnerIsOneExclusiveClaimAcrossEveryNativeWriter(t *testing.T) {
	home := t.TempDir()
	first, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatal(err)
	}
	second, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("same process acquired different shared-home owners")
	}

	file, _, acquired := rawSharedHomeLock(t, home)
	if acquired {
		t.Fatal("external writer acquired an actively owned home")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}

	file, _, acquired = rawSharedHomeLock(t, home)
	if acquired {
		t.Fatal("first reference release dropped a multiply-held home")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}

	file, unlock, acquired := rawSharedHomeLock(t, home)
	if !acquired {
		t.Fatal("last release did not return the home lock")
	}
	if err := errors.Join(unlock(), file.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestSharedHomeOwnerRefusesAConcurrentWriter(t *testing.T) {
	home := t.TempDir()
	if _, err := EnsureSharedHermesAdapterControlDir(home); err != nil {
		t.Fatal(err)
	}
	file, unlock, acquired := rawSharedHomeLock(t, home)
	if !acquired {
		t.Fatal("fixture could not take raw home lock")
	}
	if _, err := AcquireSharedHomeOwner(home); err == nil || !strings.Contains(err.Error(), "already claimed") {
		t.Fatalf("concurrent writer error = %v", err)
	}
	if err := errors.Join(unlock(), file.Close()); err != nil {
		t.Fatal(err)
	}

	owner, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStartServerRefusesAClaimedSessionAndGivesTheHomeRootBack(t *testing.T) {
	home := t.TempDir()
	claimed, err := acquireSharedACPSessionOwner(home, "claimed-session")
	if err != nil {
		t.Fatal(err)
	}

	options := darwinTestStartOptions(t, StartOptions{
		ACPSessionID:     "claimed-session",
		Cwd:              t.TempDir(),
		ExecutablePath:   fakeHermesExecutable(t, fakeProcessModeOK),
		SharedHermesHome: home,
		ExistingXDG:      testXDGDirs(t),
	})
	if _, startErr := StartServer(t.Context(), options); startErr == nil || !strings.Contains(startErr.Error(), "already active") {
		t.Fatalf("claimed session refusal = %v", startErr)
	}
	if releaseErr := claimed.Release(); releaseErr != nil {
		t.Fatal(releaseErr)
	}

	owner, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatalf("refused session start kept the home root: %v", err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}
