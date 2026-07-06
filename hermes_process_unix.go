//go:build unix

package hermesacp

import (
	"errors"
	"os/exec"
	"syscall"
)

var (
	hermesSyscallGetpgid = syscall.Getpgid
	hermesSyscallKill    = syscall.Kill
)

func terminateHermesProcess(cmd *exec.Cmd) error {
	return signalHermesProcessGroup(cmd, syscall.SIGTERM)
}

func killHermesProcess(cmd *exec.Cmd) error {
	return signalHermesProcessGroup(cmd, syscall.SIGKILL)
}

func signalHermesProcessGroup(cmd *exec.Cmd, signal syscall.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	pgid, err := hermesSyscallGetpgid(cmd.Process.Pid)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}

	if err := hermesSyscallKill(-pgid, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}

	return nil
}

func terminateProcessGroupID(pid int) error {
	return signalLeaseGroup(pid, syscall.SIGTERM)
}

func killProcessGroupID(pid int) error {
	return signalLeaseGroup(pid, syscall.SIGKILL)
}

func signalLeaseGroup(pid int, signal syscall.Signal) error {
	if pid <= 0 {
		return nil
	}

	target := pid
	if pgid, err := hermesSyscallGetpgid(pid); err == nil {
		target = -pgid
	}

	if err := hermesSyscallKill(target, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}

	return nil
}
