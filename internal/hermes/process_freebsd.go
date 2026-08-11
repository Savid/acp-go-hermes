//go:build freebsd

package hermes

import (
	"errors"
	"os/exec"
	"syscall"
)

func configureHermesProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

func inspectHermesProcess(int) (ProcessIdentity, error) {
	return ProcessIdentity{}, errors.ErrUnsupported
}

func inspectHermesProcessStartTime(int) (string, error) {
	return "", errors.ErrUnsupported
}
