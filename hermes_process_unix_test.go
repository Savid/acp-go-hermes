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
)

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
					return []byte("XDG_STATE_HOME=/tmp/state\x00"), nil
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
			if err != nil || identity.StartTime != "123" || len(identity.Cmdline) != 2 || identity.Env["XDG_STATE_HOME"] != "/tmp/state" {
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

	if err := killProcessID(0); err != nil {
		t.Fatalf("kill zero pid: %v", err)
	}
	hermesSyscallGetpgid = func(int) (int, error) { return 0, errors.New("no group") }
	hermesSyscallKill = func(pid int, signal syscall.Signal) error {
		if pid != 123 || signal != syscall.SIGTERM {
			t.Fatalf("kill pid=%d signal=%v", pid, signal)
		}
		return nil
	}
	if err := killProcessID(123); err != nil {
		t.Fatalf("killProcessID fallback: %v", err)
	}
	hermesSyscallGetpgid = func(int) (int, error) { return 123, nil }
	hermesSyscallKill = func(int, syscall.Signal) error { return errors.New("kill failed") }
	if err := killProcessID(123); err == nil {
		t.Fatal("killProcessID error ignored")
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
				"XDG_STATE_HOME":                 xdg.State,
				"HERMES_DASHBOARD_SESSION_TOKEN": "secret",
			},
		}, nil
	}
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
	reapLeaseFile(leasePath, slog.New(slog.DiscardHandler))
	if _, err := os.Stat(leasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease after logged reap = %v", err)
	}
}
