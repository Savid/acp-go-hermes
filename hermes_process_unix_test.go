//go:build unix

package hermesacp

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestReapLeaseLadderSignalBranches(t *testing.T) {
	restoreHermesClientSeams(t)
	restoreLeaseReapSeams(t)
	leaseReapTimeout = 5 * time.Millisecond
	leaseReapPollInterval = time.Millisecond
	lease := serverLease{PID: 4242, ProcessStartTime: "start"}
	log := slog.New(slog.DiscardHandler)

	oldGetpgid := hermesSyscallGetpgid
	oldKill := hermesSyscallKill
	t.Cleanup(func() {
		hermesSyscallGetpgid = oldGetpgid
		hermesSyscallKill = oldKill
	})
	hermesSyscallGetpgid = func(int) (int, error) { return 4242, nil }

	// Terminate signal fails and the process stays alive: keep the lease.
	hermesSyscallKill = func(int, syscall.Signal) error { return errors.New("boom") }
	hermesInspectProcess = func(int) (processIdentity, error) {
		return processIdentity{StartTime: "start"}, nil
	}
	if reapLeaseProcess(lease, log) {
		t.Fatal("expected keep when terminate fails and process is alive")
	}

	// Terminate is ignored, SIGKILL fails, but the process is then gone.
	killed := false
	hermesSyscallKill = func(_ int, sig syscall.Signal) error {
		if sig == syscall.SIGKILL {
			killed = true
			return errors.New("kill boom")
		}
		return nil
	}
	hermesInspectProcess = func(int) (processIdentity, error) {
		if killed {
			return processIdentity{}, os.ErrNotExist
		}
		return processIdentity{StartTime: "start"}, nil
	}
	if !reapLeaseProcess(lease, log) {
		t.Fatal("expected success once process is gone after kill-error branch")
	}

	// A reused PID (start-time mismatch) counts as gone.
	hermesInspectProcess = func(int) (processIdentity, error) {
		return processIdentity{StartTime: "other"}, nil
	}
	if !leaseProcessGone(lease) {
		t.Fatal("start-time mismatch not treated as gone")
	}
}

// TestReapLeaseKillsSigtermIgnoringChild proves HW5: the lease reap ladder
// escalates to SIGKILL, verifies the process is dead, and only then removes the
// lease, against a real child that ignores SIGTERM. The child re-execs this
// test binary rather than /bin/sh: macOS strips the environment from
// kern.procargs2 for SIP-protected platform binaries, so lease verification
// only sees env vars of user-built processes like the real hermes server.
func TestReapLeaseKillsSigtermIgnoringChild(t *testing.T) {
	if os.Getenv("HERMES_TEST_LEASE_CHILD") == "1" {
		signal.Ignore(syscall.SIGTERM)
		_, _ = os.Stdout.WriteString("ready\n")
		for {
			time.Sleep(50 * time.Millisecond)
		}
	}
	root := t.TempDir()
	xdg, err := createXDGDirs(root, "lease-child")
	if err != nil {
		t.Fatal(err)
	}
	const token = "child-token"
	cmd := exec.Command(os.Args[0], "-test.run", "^TestReapLeaseKillsSigtermIgnoringChild$", "serve")
	cmd.Env = append(os.Environ(),
		"HERMES_TEST_LEASE_CHILD=1",
		"HERMES_HOME="+xdg.Root,
		"HERMES_DASHBOARD_SESSION_TOKEN="+token,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	buf := make([]byte, 16)
	_, _ = stdout.Read(buf) // wait until SIGTERM is ignored

	identity, err := inspectHermesProcess(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("inspect child: %v", err)
	}
	lease := serverLease{
		PID:              cmd.Process.Pid,
		TokenHash:        passwordHash(token),
		XDGRoot:          xdg.Root,
		ProcessStartTime: identity.StartTime,
	}
	leasePath := filepath.Join(xdg.State, leaseFileName)
	if err := writeLease(xdg.State, lease); err != nil {
		_ = cmd.Process.Kill()
		t.Fatal(err)
	}
	if !leaseMatchesProcess(leasePath, lease) {
		_ = cmd.Process.Kill()
		t.Fatal("child lease did not match live process")
	}

	restoreLeaseReapSeams(t)
	leaseReapTimeout = 100 * time.Millisecond
	leaseReapPollInterval = 5 * time.Millisecond

	reapLeaseFile(leasePath, slog.New(slog.DiscardHandler))

	select {
	case <-waitErr:
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("SIGTERM-ignoring child not killed by reap ladder")
	}
	if _, err := os.Stat(leasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease not removed after killing child: %v", err)
	}
}

func TestDeleteCleanupKeepsSurvivingLeaseRecord(t *testing.T) {
	restoreHermesClientSeams(t)
	restoreLeaseReapSeams(t)
	oldGetpgid := hermesSyscallGetpgid
	oldKill := hermesSyscallKill
	t.Cleanup(func() {
		hermesSyscallGetpgid = oldGetpgid
		hermesSyscallKill = oldKill
	})

	root := t.TempDir()
	xdg, err := createXDGDirs(root, "cleanup-survivor")
	if err != nil {
		t.Fatal(err)
	}
	leasePath := filepath.Join(xdg.State, leaseFileName)
	const token = "cleanup-token"
	const pid = 5151
	identity := processIdentity{
		StartTime: "cleanup-start",
		Cmdline:   []string{"hermes", "serve"},
		Env: map[string]string{
			"HERMES_HOME":                    xdg.Root,
			"HERMES_DASHBOARD_SESSION_TOKEN": token,
		},
	}
	hermesInspectProcess = func(int) (processIdentity, error) { return identity, nil }
	hermesSyscallGetpgid = func(int) (int, error) { return pid, nil }
	hermesSyscallKill = func(int, syscall.Signal) error { return errors.New("signal failed") }
	if err := writeLease(xdg.State, serverLease{
		PID:              pid,
		TokenHash:        passwordHash(token),
		XDGRoot:          xdg.Root,
		ProcessStartTime: identity.StartTime,
	}); err != nil {
		t.Fatal(err)
	}

	agent := NewAgent(WithHome(root))
	agent.deleteCleanup["cleanup-survivor"] = deleteCleanupRecord{SessionID: "cleanup-survivor", XDGRoot: xdg.Root}
	err = agent.retryDeletedSessionCleanup(context.Background())
	if err == nil || !strings.Contains(err.Error(), "kept live lease") {
		t.Fatalf("retry cleanup error = %v", err)
	}
	if _, err := os.Stat(leasePath); err != nil {
		t.Fatalf("surviving lease was removed: %v", err)
	}
	if _, err := os.Stat(xdg.Root); err != nil {
		t.Fatalf("XDG root was removed: %v", err)
	}
	if _, ok := agent.deleteCleanup["cleanup-survivor"]; !ok {
		t.Fatal("cleanup record was forgotten")
	}
}

func TestHermesProcessSignalBranches(t *testing.T) {
	oldGetpgid := hermesSyscallGetpgid
	oldKill := hermesSyscallKill
	oldInspect := hermesInspectProcess
	t.Cleanup(func() {
		hermesSyscallGetpgid = oldGetpgid
		hermesSyscallKill = oldKill
		hermesInspectProcess = oldInspect
	})

	if err := terminateHermesProcess(nil); err != nil {
		t.Fatalf("terminate nil: %v", err)
	}
	if err := killHermesProcess(nil); err != nil {
		t.Fatalf("kill nil: %v", err)
	}
	cmd := &exec.Cmd{Process: &os.Process{Pid: 123}}

	hermesSyscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err != nil {
		t.Fatalf("ESRCH getpgid: %v", err)
	}
	hermesSyscallGetpgid = func(int) (int, error) { return 0, errors.New("getpgid failed") }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err == nil {
		t.Fatal("getpgid error ignored")
	}
	hermesSyscallGetpgid = func(int) (int, error) { return 123, nil }
	hermesSyscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err != nil {
		t.Fatalf("ESRCH kill: %v", err)
	}
	hermesSyscallKill = func(int, syscall.Signal) error { return errors.New("kill failed") }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err == nil {
		t.Fatal("kill error ignored")
	}
	hermesSyscallKill = func(int, syscall.Signal) error { return nil }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err != nil {
		t.Fatalf("signal success: %v", err)
	}
	if err := killHermesProcess(cmd); err != nil {
		t.Fatalf("killHermesProcess: %v", err)
	}

	if err := terminateProcessGroupID(0); err != nil {
		t.Fatalf("kill zero pid: %v", err)
	}
	hermesSyscallGetpgid = func(int) (int, error) { return 0, errors.New("no group") }
	hermesSyscallKill = func(pid int, signal syscall.Signal) error {
		if pid != 123 || signal != syscall.SIGTERM {
			t.Fatalf("kill pid=%d signal=%v", pid, signal)
		}
		return nil
	}
	if err := terminateProcessGroupID(123); err != nil {
		t.Fatalf("terminateProcessGroupID fallback: %v", err)
	}
	hermesSyscallGetpgid = func(int) (int, error) { return 123, nil }
	hermesSyscallKill = func(int, syscall.Signal) error { return errors.New("kill failed") }
	if err := terminateProcessGroupID(123); err == nil {
		t.Fatal("terminateProcessGroupID error ignored")
	}

	root := t.TempDir()
	xdg, err := createXDGDirs(root, "lease-log")
	if err != nil {
		t.Fatal(err)
	}
	leasePath := filepath.Join(xdg.State, leaseFileName)
	hermesInspectProcess = func(int) (processIdentity, error) {
		return processIdentity{
			StartTime: "start",
			Cmdline:   []string{"hermes", "serve"},
			Env: map[string]string{
				"HERMES_HOME":                    xdg.Root,
				"HERMES_DASHBOARD_SESSION_TOKEN": "secret",
			},
		}, nil
	}
	restoreLeaseReapSeams(t)
	leaseReapTimeout = 20 * time.Millisecond
	leaseReapPollInterval = time.Millisecond
	hermesSyscallGetpgid = func(int) (int, error) { return 123, nil }
	hermesSyscallKill = func(int, syscall.Signal) error { return errors.New("kill failed") }
	if err := writeLease(xdg.State, serverLease{
		PID:              123,
		TokenHash:        passwordHash("secret"),
		XDGRoot:          xdg.Root,
		ProcessStartTime: "start",
	}); err != nil {
		t.Fatal(err)
	}
	// Signaling fails and the process stays identity-matched (alive): the
	// lease is KEPT so a later startup retries the reap.
	reapLeaseFile(leasePath, slog.New(slog.DiscardHandler))
	if _, err := os.Stat(leasePath); err != nil {
		t.Fatalf("lease of unkillable process was removed: %v", err)
	}
	_ = os.Remove(leasePath)
}
