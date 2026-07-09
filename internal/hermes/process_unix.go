//go:build unix

package hermes

import (
	"errors"
	"os/exec"
	"syscall"
)

var (
	SyscallGetpgid = syscall.Getpgid
	SyscallKill    = syscall.Kill
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

	pgid, err := SyscallGetpgid(cmd.Process.Pid)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}

	if err := SyscallKill(-pgid, signal); err != nil {
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
	if pgid, err := SyscallGetpgid(pid); err == nil {
		target = -pgid
	}

	if err := SyscallKill(target, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}

	return nil
}
