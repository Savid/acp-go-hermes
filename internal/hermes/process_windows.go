//go:build windows

package hermes

import (
	"errors"
	"os"
	"os/exec"
	"strconv"

	"golang.org/x/sys/windows"
)

func terminateHermesProcess(cmd *exec.Cmd) error {
	return killHermesProcess(cmd)
}

func killHermesProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}

		return err
	}

	return nil
}

func terminateProcessGroupID(pid int) error {
	return killProcessGroupID(pid)
}

func killProcessGroupID(pid int) error {
	if pid <= 0 {
		return nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	return proc.Kill()
}

func inspectHermesProcess(pid int) (ProcessIdentity, error) {
	startTime, err := inspectHermesProcessStartTime(pid)
	if err != nil {
		return ProcessIdentity{}, err
	}

	return ProcessIdentity{StartTime: startTime}, nil
}

func inspectHermesProcessStartTime(pid int) (string, error) {
	if pid <= 0 {
		return "", windows.ERROR_INVALID_PARAMETER
	}
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(handle)

	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return "", err
	}

	return strconv.FormatInt(creation.Nanoseconds(), 10), nil
}
