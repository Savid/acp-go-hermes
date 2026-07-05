//go:build freebsd

package hermesacp

import (
	"errors"
	"os/exec"
	"syscall"
)

func configureHermesProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

func inspectHermesProcess(int) (processIdentity, error) {
	return processIdentity{}, errors.ErrUnsupported
}
