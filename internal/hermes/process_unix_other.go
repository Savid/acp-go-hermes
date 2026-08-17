//go:build unix && !linux && !freebsd && !darwin

package hermes

import (
	"errors"
	"os/exec"
	"syscall"
)

func configureHermesProcess(cmd *exec.Cmd) {
	// The remaining unix platforms have no Pdeathsig equivalent; parent-death
	// cleanup is best-effort via process-group signalling and stale-lease
	// reaping.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func inspectHermesProcess(int) (ProcessIdentity, error) {
	return ProcessIdentity{}, errors.ErrUnsupported
}

func inspectHermesProcessStartTime(int) (string, error) {
	return "", errors.ErrUnsupported
}
