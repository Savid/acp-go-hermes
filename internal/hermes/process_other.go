//go:build !unix && !windows

package hermes

import (
	"errors"
	"os"
	"os/exec"
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

func inspectHermesProcess(int) (ProcessIdentity, error) {
	return ProcessIdentity{}, os.ErrNotExist
}

func inspectHermesProcessStartTime(int) (string, error) {
	return "", os.ErrNotExist
}
