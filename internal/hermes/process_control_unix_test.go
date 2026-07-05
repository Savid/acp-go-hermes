//go:build unix

package hermes

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
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

	err := (&Process{Cmd: cmd}).Close(context.Background())
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
