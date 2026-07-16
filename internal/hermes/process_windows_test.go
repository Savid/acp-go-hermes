//go:build windows

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

	"golang.org/x/sys/windows"
)

const (
	envWindowsContainmentHelper = "ACP_GO_HERMES_WINDOWS_CONTAINMENT_HELPER"
	envWindowsContainmentPID    = "ACP_GO_HERMES_WINDOWS_CONTAINMENT_PID"
)

func TestWindowsJobCloseKillsNativeGrandchild(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestWindowsContainmentHelper$")
	cmd.Env = append(os.Environ(),
		envWindowsContainmentHelper+"=root",
		envWindowsContainmentPID+"="+pidFile,
	)
	tree, err := startContainedProcess(cmd)
	if err != nil {
		t.Fatalf("start Windows Job Object root: %v", err)
	}
	process := &Process{Cmd: cmd, tree: tree}
	process.beginWait()
	t.Cleanup(func() { _ = process.Close(context.Background()) })

	grandchildPID := waitWindowsPIDFile(t, pidFile)
	if err := process.Close(context.Background()); err != nil {
		t.Fatalf("close Windows Job Object: %v", err)
	}
	if windowsProcessAlive(uint32(grandchildPID)) {
		t.Fatalf("Windows Job Object grandchild %d survived proved Close", grandchildPID)
	}
}

func TestWindowsContainmentHelper(t *testing.T) {
	switch os.Getenv(envWindowsContainmentHelper) {
	case "root":
		child := exec.Command(os.Args[0], "-test.run", "^TestWindowsContainmentHelper$")
		child.Env = append(os.Environ(), envWindowsContainmentHelper+"=child")
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(os.Getenv(envWindowsContainmentPID), []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
			os.Exit(3)
		}
		for {
			time.Sleep(time.Minute)
		}
	case "child":
		for {
			time.Sleep(time.Minute)
		}
	}
}

func waitWindowsPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr != nil {
				t.Fatalf("parse child pid: %v", parseErr)
			}

			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("Windows containment helper did not write its child pid")

	return 0
}

func windowsProcessAlive(pid uint32) bool {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return !errors.Is(err, windows.ERROR_INVALID_PARAMETER)
	}
	_ = windows.CloseHandle(handle)

	return true
}
