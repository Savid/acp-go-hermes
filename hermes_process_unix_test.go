//go:build unix

package hermesacp

import (
	"errors"
	"log/slog"
	"os"
	"os/exec"
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
// lease, against a real child that ignores SIGTERM.
func TestReapLeaseKillsSigtermIgnoringChild(t *testing.T) {
	root := t.TempDir()
	xdg, err := createXDGDirs(root, "lease-child")
	if err != nil {
		t.Fatal(err)
	}
	const token = "child-token"
	cmd := exec.Command("/bin/sh", "-c", "trap '' TERM; echo ready; while true; do sleep 0.05; done", "serve")
	cmd.Env = append(os.Environ(), "HERMES_HOME="+xdg.Root, "HERMES_DASHBOARD_SESSION_TOKEN="+token)
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
	_, _ = stdout.Read(buf) // wait until the TERM trap is installed

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

func TestInspectHermesProcessReadBranches(t *testing.T) {
	oldReadFile := procReadFile
	t.Cleanup(func() { procReadFile = oldReadFile })

	if _, err := inspectHermesProcess(0); err == nil {
		t.Fatal("zero pid inspected successfully")
	}
	if _, err := procStartTime("1 (hermes"); err == nil {
		t.Fatal("malformed proc stat accepted")
	}
	if _, err := procStartTime("1 (hermes) S 0"); err == nil {
		t.Fatal("short proc stat accepted")
	}

	validStat := procStatWithStart("123")
	for _, tt := range []struct {
		name string
		read func(string) ([]byte, error)
		err  bool
	}{
		{
			name: "stat read error",
			read: func(path string) ([]byte, error) {
				if strings.HasSuffix(path, "/stat") {
					return nil, errors.New("stat failed")
				}
				return nil, nil
			},
			err: true,
		},
		{
			name: "stat parse error",
			read: func(path string) ([]byte, error) {
				if strings.HasSuffix(path, "/stat") {
					return []byte("malformed"), nil
				}
				return nil, nil
			},
			err: true,
		},
		{
			name: "cmdline read error",
			read: func(path string) ([]byte, error) {
				switch {
				case strings.HasSuffix(path, "/stat"):
					return []byte(validStat), nil
				case strings.HasSuffix(path, "/cmdline"):
					return nil, errors.New("cmdline failed")
				default:
					return nil, nil
				}
			},
			err: true,
		},
		{
			name: "env read error",
			read: func(path string) ([]byte, error) {
				switch {
				case strings.HasSuffix(path, "/stat"):
					return []byte(validStat), nil
				case strings.HasSuffix(path, "/cmdline"):
					return []byte("hermes\x00serve\x00"), nil
				case strings.HasSuffix(path, "/environ"):
					return nil, errors.New("env failed")
				default:
					return nil, nil
				}
			},
			err: true,
		},
		{
			name: "success",
			read: func(path string) ([]byte, error) {
				switch {
				case strings.HasSuffix(path, "/stat"):
					return []byte(validStat), nil
				case strings.HasSuffix(path, "/cmdline"):
					return []byte("hermes\x00serve\x00"), nil
				case strings.HasSuffix(path, "/environ"):
					return []byte("HERMES_HOME=/tmp/home\x00"), nil
				default:
					return nil, nil
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			procReadFile = tt.read
			identity, err := inspectHermesProcess(123)
			if tt.err {
				if err == nil {
					t.Fatal("inspect succeeded unexpectedly")
				}
				return
			}
			if err != nil || identity.StartTime != "123" || len(identity.Cmdline) != 2 || identity.Env["HERMES_HOME"] != "/tmp/home" {
				t.Fatalf("identity=%#v err=%v", identity, err)
			}
		})
	}
}

func procStatWithStart(start string) string {
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	fields[19] = start
	return "1 (hermes) " + strings.Join(fields, " ")
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
