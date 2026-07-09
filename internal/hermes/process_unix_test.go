//go:build unix

package hermes

import (
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReapLeaseLadderSignalBranches(t *testing.T) {
	restoreHermesClientSeams(t)
	restoreLeaseReapSeams(t)
	LeaseReapTimeout = 5 * time.Millisecond
	LeaseReapPollInterval = time.Millisecond
	lease := ServerLease{PID: 4242, ProcessStartTime: "start"}
	log := slog.New(slog.DiscardHandler)

	oldGetpgid := SyscallGetpgid
	oldKill := SyscallKill
	t.Cleanup(func() {
		SyscallGetpgid = oldGetpgid
		SyscallKill = oldKill
	})
	SyscallGetpgid = func(int) (int, error) { return 4242, nil }

	// Terminate signal fails and the process stays alive: keep the lease.
	SyscallKill = func(int, syscall.Signal) error { return errors.New("boom") }
	InspectProcess = func(int) (ProcessIdentity, error) {
		return ProcessIdentity{StartTime: "start"}, nil
	}
	if reapLeaseProcess(lease, log) {
		t.Fatal("expected keep when terminate fails and process is alive")
	}

	// Terminate is ignored, SIGKILL fails, but the process is then gone.
	killed := false
	SyscallKill = func(_ int, sig syscall.Signal) error {
		if sig == syscall.SIGKILL {
			killed = true

			return errors.New("kill boom")
		}

		return nil
	}
	InspectProcess = func(int) (ProcessIdentity, error) {
		if killed {
			return ProcessIdentity{}, os.ErrNotExist
		}

		return ProcessIdentity{StartTime: "start"}, nil
	}
	if !reapLeaseProcess(lease, log) {
		t.Fatal("expected success once process is gone after kill-error branch")
	}

	// A reused PID (start-time mismatch) counts as gone.
	InspectProcess = func(int) (ProcessIdentity, error) {
		return ProcessIdentity{StartTime: "other"}, nil
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
	xdg, err := CreateXDGDirs(root, "lease-child")
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
	if err2 := cmd.Start(); err2 != nil {
		t.Fatal(err2)
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
	lease := ServerLease{
		PID:              cmd.Process.Pid,
		TokenHash:        PasswordHash(token),
		XDGRoot:          xdg.Root,
		ProcessStartTime: identity.StartTime,
	}
	leasePath := filepath.Join(xdg.State, LeaseFileName)
	if err := WriteLease(xdg.State, lease); err != nil {
		_ = cmd.Process.Kill()
		t.Fatal(err)
	}
	if !leaseMatchesProcess(leasePath, lease) {
		_ = cmd.Process.Kill()
		t.Fatal("child lease did not match live process")
	}

	restoreLeaseReapSeams(t)
	LeaseReapTimeout = 100 * time.Millisecond
	LeaseReapPollInterval = 5 * time.Millisecond

	ReapLeaseFile(leasePath, slog.New(slog.DiscardHandler))

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

func TestHermesProcessSignalBranches(t *testing.T) {
	oldGetpgid := SyscallGetpgid
	oldKill := SyscallKill
	oldInspect := InspectProcess
	t.Cleanup(func() {
		SyscallGetpgid = oldGetpgid
		SyscallKill = oldKill
		InspectProcess = oldInspect
	})

	if err := terminateHermesProcess(nil); err != nil {
		t.Fatalf("terminate nil: %v", err)
	}
	if err := killHermesProcess(nil); err != nil {
		t.Fatalf("kill nil: %v", err)
	}
	cmd := &exec.Cmd{Process: &os.Process{Pid: 123}}

	SyscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err != nil {
		t.Fatalf("ESRCH getpgid: %v", err)
	}
	SyscallGetpgid = func(int) (int, error) { return 0, errors.New("getpgid failed") }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err == nil {
		t.Fatal("getpgid error ignored")
	}
	SyscallGetpgid = func(int) (int, error) { return 123, nil }
	SyscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err != nil {
		t.Fatalf("ESRCH kill: %v", err)
	}
	SyscallKill = func(int, syscall.Signal) error { return errors.New("kill failed") }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err == nil {
		t.Fatal("kill error ignored")
	}
	SyscallKill = func(int, syscall.Signal) error { return nil }
	if err := signalHermesProcessGroup(cmd, syscall.SIGTERM); err != nil {
		t.Fatalf("signal success: %v", err)
	}
	if err := killHermesProcess(cmd); err != nil {
		t.Fatalf("killHermesProcess: %v", err)
	}

	if err := terminateProcessGroupID(0); err != nil {
		t.Fatalf("kill zero pid: %v", err)
	}
	SyscallGetpgid = func(int) (int, error) { return 0, errors.New("no group") }
	SyscallKill = func(pid int, signal syscall.Signal) error {
		if pid != 123 || signal != syscall.SIGTERM {
			t.Fatalf("kill pid=%d signal=%v", pid, signal)
		}

		return nil
	}
	if err := terminateProcessGroupID(123); err != nil {
		t.Fatalf("terminateProcessGroupID fallback: %v", err)
	}
	SyscallGetpgid = func(int) (int, error) { return 123, nil }
	SyscallKill = func(int, syscall.Signal) error { return errors.New("kill failed") }
	if err := terminateProcessGroupID(123); err == nil {
		t.Fatal("terminateProcessGroupID error ignored")
	}

	root := t.TempDir()
	xdg, err := CreateXDGDirs(root, "lease-log")
	if err != nil {
		t.Fatal(err)
	}
	leasePath := filepath.Join(xdg.State, LeaseFileName)
	InspectProcess = func(int) (ProcessIdentity, error) {
		return ProcessIdentity{
			StartTime: "start",
			Cmdline:   []string{"hermes", "serve"},
			Env: map[string]string{
				"HERMES_HOME":                    xdg.Root,
				"HERMES_DASHBOARD_SESSION_TOKEN": "secret",
			},
		}, nil
	}
	restoreLeaseReapSeams(t)
	LeaseReapTimeout = 20 * time.Millisecond
	LeaseReapPollInterval = time.Millisecond
	SyscallGetpgid = func(int) (int, error) { return 123, nil }
	SyscallKill = func(int, syscall.Signal) error { return errors.New("kill failed") }
	if err := WriteLease(xdg.State, ServerLease{
		PID:              123,
		TokenHash:        PasswordHash("secret"),
		XDGRoot:          xdg.Root,
		ProcessStartTime: "start",
	}); err != nil {
		t.Fatal(err)
	}
	// Signaling fails and the process stays identity-matched (alive): the
	// lease is KEPT so a later startup retries the reap.
	ReapLeaseFile(leasePath, slog.New(slog.DiscardHandler))
	if _, err := os.Stat(leasePath); err != nil {
		t.Fatalf("lease of unkillable process was removed: %v", err)
	}
	_ = os.Remove(leasePath)
}
