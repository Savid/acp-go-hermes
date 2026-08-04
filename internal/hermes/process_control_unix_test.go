//go:build unix

package hermes

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestProcessCloseKillsProcessGroupGrandchild(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	cmd := exec.Command("sh", "-c", "trap '' TERM; (trap '' TERM; sleep 30) & echo $! > "+strconv.Quote(pidFile)+"; wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start grouped process: %v", err)
	}
	t.Cleanup(func() {
		_ = killProcess(cmd)
		_, _ = cmd.Process.Wait()
	})
	childPID := waitForPIDFile(t, pidFile)

	oldAfter := after
	t.Cleanup(func() { after = oldAfter })
	after = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()

		return ch
	}

	err := (&Process{Cmd: cmd, tree: &processContainment{processGroupID: cmd.Process.Pid}}).Close(context.Background())
	if err == nil || !strings.Contains(err.Error(), "did not exit") {
		t.Fatalf("Close error = %v", err)
	}
	deadline := time.After(time.Second)
	for processAlive(childPID) {
		select {
		case <-deadline:
			t.Fatalf("grandchild pid %d survived Close timeout", childPID)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestProcessCloseCompletesContainmentAfterRootExit(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	cmd := exec.Command("sh", "-c", "(trap '' TERM; sleep 30) & echo $! > "+strconv.Quote(pidFile)+"; exit 0")
	configureHermesProcess(cmd)
	tree, err := startContainedProcess(cmd, darwinTestContainmentSpec(t))
	if err != nil {
		t.Fatalf("start contained process: %v", err)
	}
	t.Cleanup(func() {
		_ = killProcess(cmd)
		_, _ = cmd.Process.Wait()
	})
	childPID := waitForPIDFile(t, pidFile)

	if err := (&Process{Cmd: cmd, tree: tree}).Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if processAlive(childPID) {
		t.Fatalf("Close returned before post-root descendant %d exited", childPID)
	}
}

func TestSignalProcessBranches(t *testing.T) {
	oldGetpgid := processGetpgid
	oldKill := processKill
	t.Cleanup(func() {
		processGetpgid = oldGetpgid
		processKill = oldKill
	})

	if err := signalProcess(nil, syscall.SIGTERM); err != nil {
		t.Fatalf("nil signalProcess: %v", err)
	}
	cmd := &exec.Cmd{Process: &os.Process{Pid: 123}}
	processGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
	processKill = func(int, syscall.Signal) error {
		t.Fatal("processKill called after ESRCH getpgid")

		return nil
	}
	if err := signalProcess(cmd, syscall.SIGTERM); err != nil {
		t.Fatalf("ESRCH getpgid signalProcess: %v", err)
	}

	target := 0
	processGetpgid = func(int) (int, error) { return 0, errors.New("pgid failed") }
	processKill = func(pid int, _ syscall.Signal) error {
		target = pid

		return nil
	}
	if err := signalProcess(cmd, syscall.SIGTERM); err != nil || target != 123 {
		t.Fatalf("parent fallback target=%d err=%v", target, err)
	}

	processGetpgid = func(pid int) (int, error) { return pid, nil }
	for _, killErr := range []error{syscall.ESRCH, os.ErrProcessDone} {
		processKill = func(int, syscall.Signal) error { return killErr }
		if err := signalProcess(cmd, syscall.SIGTERM); err != nil {
			t.Fatalf("ignored kill error %v returned %v", killErr, err)
		}
	}
	processKill = func(int, syscall.Signal) error { return errors.New("kill failed") }
	if err := signalProcess(cmd, syscall.SIGTERM); err == nil {
		t.Fatal("signalProcess ignored kill failure")
	}
}

func TestUnixProcessContainmentCompletionBranches(t *testing.T) {
	if _, err := startContainedProcess(exec.Command("sh", "-c", "exit 0")); err == nil {
		t.Fatal("startContainedProcess accepted a command without a process group")
	}

	missing := exec.Command(filepath.Join(t.TempDir(), "missing"))
	configureHermesProcess(missing)
	if _, err := startContainedProcess(missing, darwinTestContainmentSpec(t)); err == nil {
		t.Fatal("startContainedProcess accepted a missing executable")
	}

	if err := (*processContainment)(nil).complete(time.Millisecond); err == nil {
		t.Fatal("nil containment completed")
	}
	if err := (&processContainment{}).complete(time.Millisecond); err == nil {
		t.Fatal("zero containment completed")
	}

	oldKill := processKill
	t.Cleanup(func() { processKill = oldKill })
	tree := &processContainment{processGroupID: 123}

	for _, tc := range []struct {
		name  string
		err   error
		alive bool
		bad   bool
	}{
		{name: "alive", alive: true},
		{name: "permission", err: syscall.EPERM, alive: true},
		{name: "gone", err: syscall.ESRCH},
		{name: "failure", err: errors.New("probe failed"), bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			processKill = func(int, syscall.Signal) error { return tc.err }
			alive, err := tree.alive()
			if alive != tc.alive || (err != nil) != tc.bad {
				t.Fatalf("alive=%v err=%v, want alive=%v bad=%v", alive, err, tc.alive, tc.bad)
			}
		})
	}

	processKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	if err := tree.signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal gone tree: %v", err)
	}
	if err := tree.complete(0); err != nil {
		t.Fatalf("complete gone tree: %v", err)
	}

	processKill = func(int, syscall.Signal) error { return errors.New("signal failed") }
	if err := tree.signal(syscall.SIGTERM); err == nil {
		t.Fatal("signal failure ignored")
	}
	if err := tree.waitUntilEmpty(time.Now().Add(time.Second)); err == nil {
		t.Fatal("probe failure ignored")
	}

	processKill = func(int, syscall.Signal) error { return nil }
	if err := tree.waitUntilEmpty(time.Now().Add(-time.Second)); err == nil {
		t.Fatal("expired deadline ignored")
	}
	if err := tree.complete(time.Nanosecond); err == nil {
		t.Fatal("incomplete tree reported complete")
	}

	probeCalls := 0
	processKill = func(_ int, signal syscall.Signal) error {
		if signal != 0 {
			return nil
		}

		probeCalls++
		if probeCalls == 1 {
			return errors.New("first probe failed")
		}

		return syscall.ESRCH
	}
	if err := tree.complete(time.Second); err != nil {
		t.Fatalf("fallback completion: %v", err)
	}
}

func darwinTestContainmentSpec(t *testing.T) ContainmentSpec {
	t.Helper()
	isolation := testProcessIsolation()
	if runtime.GOOS != "darwin" {
		return ContainmentSpec{Isolation: isolation}
	}
	parent := t.TempDir()
	dirs, err := CreateGenerationXDGDirs(parent)
	if err != nil {
		t.Fatalf("CreateGenerationXDGDirs: %v", err)
	}

	return ContainmentSpec{DarwinBestEffort: true, ScratchParent: parent, GenerationRoot: dirs.Root, LifecycleKind: "session", Isolation: isolation}
}

func testProcessIsolation() *ProcessIsolation {
	return &ProcessIsolation{
		UID: 11, GID: 22,
		BaseEnvironment:      map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")},
		TestOnlyNoCredential: true,
	}
}

func provedProcessContainment() *processContainment {
	return &processContainment{
		terminateFn: func() error { return nil },
		killFn:      func() error { return nil },
		completeFn:  func(time.Duration) error { return nil },
	}
}

func setTestIsolationBootstrapEnv(t *testing.T) {
	t.Helper()
	t.Setenv(envIsolationUID, strconv.Itoa(os.Geteuid()))
	t.Setenv(envIsolationGID, strconv.Itoa(os.Getegid()))
	t.Setenv(envIsolationTest, "true")
}

func TestProcessContainmentCompletionFailures(t *testing.T) {
	oldKill := processKill
	oldClose := processTreeClose
	t.Cleanup(func() {
		processKill = oldKill
		processTreeClose = oldClose
	})
	if err := (&Process{}).completeProcessContainment(); err != nil {
		t.Fatalf("nil process tree: %v", err)
	}

	process := &Process{tree: &processContainment{processGroupID: 123}}
	processKill = func(int, syscall.Signal) error { return errors.New("probe failed") }
	if err := process.completeProcessContainment(); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("completion error = %v", err)
	}

	processKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	processTreeClose = func(*processContainment) error { return errors.New("close failed") }
	if err := process.completeProcessContainment(); err == nil || !strings.Contains(err.Error(), "close Hermes process containment") {
		t.Fatalf("close containment error = %v", err)
	}
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr != nil {
				t.Fatalf("parse child pid %q: %v", data, parseErr)
			}

			return pid
		}
		select {
		case <-deadline:
			t.Fatalf("child pid file was not written: %v", err)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)

	return err == nil || errors.Is(err, syscall.EPERM)
}
