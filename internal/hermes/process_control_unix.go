//go:build unix

package hermes

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

var (
	processGetpgid = syscall.Getpgid
	processKill    = syscall.Kill
)

func terminateProcess(cmd *exec.Cmd) error {
	return signalProcess(cmd, syscall.SIGTERM)
}

func killProcess(cmd *exec.Cmd) error {
	return signalProcess(cmd, syscall.SIGKILL)
}

func signalProcess(cmd *exec.Cmd, signal syscall.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	pid := cmd.Process.Pid

	target := pid
	if pgid, err := processGetpgid(pid); err == nil && pgid == pid {
		target = -pgid
	} else if err != nil && errors.Is(err, syscall.ESRCH) {
		return nil
	}

	if err := processKill(target, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) || errors.Is(err, os.ErrProcessDone) {
			return nil
		}

		return err
	}

	return nil
}
