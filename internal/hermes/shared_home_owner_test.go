package hermes

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// rawSharedHomeLock takes the home-root lock the way an unrelated process
// would: a fresh open description, which flock refuses to share even inside one
// process. It is what proves the claim is really held rather than merely
// recorded.
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
		t.Fatalf("acquire home root: %v", err)
	}

	second, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatalf("second native writer against the same root: %v", err)
	}
	if second != first {
		t.Fatal("one adapter process took two claims on one home root")
	}

	control := sharedTestControlDir(t, home)
	claim, err := os.ReadFile(filepath.Join(control, sharedHomeOwnerBaseName+".claim"))
	if err != nil {
		t.Fatalf("read home-root claim: %v", err)
	}
	if !strings.Contains(string(claim), `"pid":`) || !strings.Contains(string(claim), `"kernelStartTime":`) {
		t.Fatalf("home-root claim omits the liveness identity: %q", claim)
	}

	file, _, acquired := rawSharedHomeLock(t, home)
	if acquired {
		t.Fatal("home root admitted a second lock while a native writer held it")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := first.Release(); err != nil {
		t.Fatalf("release first reference: %v", err)
	}

	file, _, acquired = rawSharedHomeLock(t, home)
	if acquired {
		t.Fatal("home root was released while a native writer still referenced it")
	}
	if err := errors.Join(file.Close(), second.Release()); err != nil {
		t.Fatalf("release last reference: %v", err)
	}

	for _, suffix := range []string{".lock", ".claim"} {
		if _, err := os.Stat(filepath.Join(control, sharedHomeOwnerBaseName+suffix)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("home-root %s survived the last release: %v", suffix, err)
		}
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release after the claim was given up: %v", err)
	}
}

func TestSharedHomeOwnerRefusesAConcurrentWriter(t *testing.T) {
	home := t.TempDir()
	if _, err := EnsureSharedHermesAdapterControlDir(home); err != nil {
		t.Fatal(err)
	}

	file, unlock, acquired := rawSharedHomeLock(t, home)
	if !acquired {
		t.Fatal("fixture could not take the home-root lock")
	}

	_, err := AcquireSharedHomeOwner(home)
	if err == nil || !strings.Contains(err.Error(), "home root is already claimed by a live writer") {
		t.Fatalf("concurrent writer refusal = %v", err)
	}

	if releaseErr := errors.Join(unlock(), file.Close()); releaseErr != nil {
		t.Fatal(releaseErr)
	}

	owner, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatalf("acquire after the concurrent writer left: %v", err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestSharedHomeOwnerRefusesALiveClaimantAndAdmitsAProvenDeadOne(t *testing.T) {
	home := t.TempDir()
	control := sharedTestControlDir(t, home)
	claimPath := filepath.Join(control, sharedHomeOwnerBaseName+".claim")

	identity, err := CurrentDurableProcessIdentity()
	if err != nil {
		t.Fatal(err)
	}

	live := `{"pid":` + strconv.Itoa(identity.PID) + `,"kernelStartTime":"` + identity.KernelStartTime + `"}`
	if writeErr := atomicSharedHermesWriteFile(claimPath, []byte(live), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, liveErr := AcquireSharedHomeOwner(home); liveErr == nil || !strings.Contains(liveErr.Error(), "home-root claimant process is still live") {
		t.Fatalf("live claimant refusal = %v", liveErr)
	}

	stale := `{"pid":` + strconv.Itoa(identity.PID) + `,"kernelStartTime":"reused-pid-start"}`
	if writeErr := atomicSharedHermesWriteFile(claimPath, []byte(stale), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	owner, staleErr := AcquireSharedHomeOwner(home)
	if staleErr != nil {
		t.Fatalf("start-time mismatch was not recoverable: %v", staleErr)
	}
	if releaseErr := owner.Release(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
}

func TestSharedHomeOwnerAcquisitionFaults(t *testing.T) {
	if _, err := AcquireSharedHomeOwner("relative"); err == nil {
		t.Fatal("relative home root was accepted")
	}

	originalIdentity := sharedHomeOwnerIdentity
	originalMarshal := sharedOwnerJSONMarshal
	t.Cleanup(func() {
		sharedHomeOwnerIdentity = originalIdentity
		sharedOwnerJSONMarshal = originalMarshal
	})

	identityHome := t.TempDir()
	sharedHomeOwnerIdentity = func() (DurableProcessIdentity, error) {
		return DurableProcessIdentity{}, errors.New("identity fault")
	}
	if _, err := AcquireSharedHomeOwner(identityHome); err == nil || !strings.Contains(err.Error(), "identity fault") {
		t.Fatalf("home-root identity fault = %v", err)
	}
	sharedHomeOwnerIdentity = originalIdentity

	bindHome := t.TempDir()
	sharedOwnerJSONMarshal = func(any) ([]byte, error) { return nil, errors.New("claim marshal fault") }
	if _, err := AcquireSharedHomeOwner(bindHome); err == nil || !strings.Contains(err.Error(), "claim marshal fault") {
		t.Fatalf("home-root bind fault = %v", err)
	}
	sharedOwnerJSONMarshal = originalMarshal

	// A failed acquisition must leave the root free rather than half-claimed.
	for _, home := range []string{identityHome, bindHome} {
		owner, err := AcquireSharedHomeOwner(home)
		if err != nil {
			t.Fatalf("acquire after a failed claim: %v", err)
		}
		if err := owner.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSharedHomeOwnerRetainHoldsTheRootAfterUnprovenContainment(t *testing.T) {
	home := t.TempDir()
	t.Cleanup(func() { releaseRetainedSharedOwnersUnder(t, home) })

	owner, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireSharedHomeOwner(home); err != nil {
		t.Fatal(err)
	}

	owner.Retain()
	owner.Retain()

	if err := owner.Release(); err != nil {
		t.Fatalf("release after retention: %v", err)
	}
	if _, err := AcquireSharedHomeOwner(home); err == nil || !strings.Contains(err.Error(), "home root is already claimed") {
		t.Fatalf("acquire after retention = %v", err)
	}

	(*SharedHomeOwner)(nil).Retain()
	if err := (*SharedHomeOwner)(nil).Release(); err != nil {
		t.Fatal(err)
	}
}

func TestSharedHomeOwnerLockDescriptorIsHandedToTheNativeChild(t *testing.T) {
	files, err := (*SharedHomeOwner)(nil).appendLockFile(nil)
	if err != nil || files != nil {
		t.Fatalf("nil home owner appended %v err=%v", files, err)
	}

	if _, unavailable := (&SharedHomeOwner{claim: &SharedSessionOwner{}}).appendLockFile(nil); unavailable == nil {
		t.Fatal("home owner without a lock descriptor was accepted")
	}

	home := t.TempDir()
	owner, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release() })

	files, err = owner.appendLockFile([]*os.File{os.Stdin})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0] != os.Stdin || files[1] != owner.claim.file {
		t.Fatalf("inherited descriptors = %v", files)
	}
}

func TestSharedOwnerClaimRefusesADetachedLockInode(t *testing.T) {
	originalStat := sharedOwnerFileStat
	originalLstat := sharedOwnerLstat
	t.Cleanup(func() {
		sharedOwnerFileStat = originalStat
		sharedOwnerLstat = originalLstat
	})

	sharedOwnerFileStat = func(*os.File) (os.FileInfo, error) { return nil, errors.New("stat fault") }
	if _, statErr := AcquireSharedHomeOwner(t.TempDir()); statErr == nil || !strings.Contains(statErr.Error(), "inspect shared Hermes home-root lock") {
		t.Fatalf("home-root lock stat fault = %v", statErr)
	}
	sharedOwnerFileStat = originalStat

	replaced := t.TempDir()
	sharedOwnerLstat = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
	if _, detachedErr := AcquireSharedHomeOwner(replaced); detachedErr == nil || !strings.Contains(detachedErr.Error(), "replaced during every acquisition attempt") {
		t.Fatalf("detached home-root lock inode = %v", detachedErr)
	}
	sharedOwnerLstat = originalLstat

	owner, err := AcquireSharedHomeOwner(replaced)
	if err != nil {
		t.Fatalf("acquire after every attempt was retired: %v", err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestSharedSessionOwnerReleaseClearsBothArtifacts(t *testing.T) {
	home := t.TempDir()
	owner, err := AcquireSharedNativeSessionOwner(home, "cleared")
	if err != nil {
		t.Fatal(err)
	}
	if bindErr := owner.BindProcessIdentity(os.Getpid(), "start"); bindErr != nil {
		t.Fatal(bindErr)
	}
	if releaseErr := owner.Release(); releaseErr != nil {
		t.Fatalf("release: %v", releaseErr)
	}

	entries, err := os.ReadDir(filepath.Join(sharedTestControlDir(t, home), sharedSessionOwnersDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("session-owner directory retained %d entries after release", len(entries))
	}

	blocked, err := AcquireSharedNativeSessionOwner(home, "blocked")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(blocked.claimPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked.claimPath, "occupant"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := blocked.Release(); err == nil {
		t.Fatal("release accepted an unremovable claim artifact")
	}
}

func TestStartRefusesAHomeOwnerWithoutALockDescriptor(t *testing.T) {
	restoreProcessSeams(t)
	home := t.TempDir()
	executable := fakeHermesExecutable(t, fakeProcessModeOK)
	markExecutableProbed(executable)

	_, err := Start(t.Context(), ProcessOptions{
		ExecutablePath:     executable,
		Home:               home,
		Cwd:                home,
		ScratchParent:      home,
		AmbientEnvironment: testAmbientEnvironment(),
		SharedHomeOwner:    &SharedHomeOwner{claim: &SharedSessionOwner{}},
		Timeout:            10 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "lock is unavailable") {
		t.Fatalf("launch without an inheritable home-root descriptor = %v", err)
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

func TestSharedHomeRootIsRetainedWhenCloseContainmentIsUnproven(t *testing.T) {
	restoreProcessSeams(t)
	home := t.TempDir()
	t.Cleanup(func() { releaseRetainedSharedOwnersUnder(t, home) })

	originalStart := startHermesContainedProcess
	startHermesContainedProcess = func(cmd *exec.Cmd, specs ...ContainmentSpec) (*processContainment, error) {
		tree, err := originalStart(cmd, specs...)
		if err != nil || len(specs) == 0 || specs[0].LifecycleKind != containmentSessionKind {
			return tree, err
		}
		tree.completeFn = func(time.Duration) error { return errors.New("quiescence unproven") }

		return tree, nil
	}

	server, err := StartServer(t.Context(), darwinTestStartOptions(t, StartOptions{
		ACPSessionID:     "unproven-close",
		Cwd:              t.TempDir(),
		ExecutablePath:   fakeHermesGatewayExecutable(t, fakeGatewayModeOK),
		SharedHermesHome: home,
		ExistingXDG:      testXDGDirs(t),
	}))
	if err != nil {
		t.Fatalf("start shared server: %v", err)
	}
	if closeErr := server.Close(context.Background()); !errors.Is(closeErr, ErrProcessContainmentIncomplete) {
		t.Fatalf("close error = %v", closeErr)
	}

	if _, err := AcquireSharedHomeOwner(home); err == nil || !strings.Contains(err.Error(), "home root is already claimed") {
		t.Fatalf("home root after an unproven close = %v", err)
	}
	if _, err := acquireSharedACPSessionOwner(home, "unproven-close"); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("session claim after an unproven close = %v", err)
	}
}
