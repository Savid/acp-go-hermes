//go:build unix

package hermes

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

var (
	processGetpgid = syscall.Getpgid
	processKill    = syscall.Kill
)

type processContainment struct {
	processGroupID int
}

func startContainedProcess(cmd *exec.Cmd) (*processContainment, error) {
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		return nil, errors.New("hermes Unix process-group containment is not configured")
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return &processContainment{processGroupID: cmd.Process.Pid}, nil
}

func (c *processContainment) quiesce(timeout time.Duration) error {
	if c == nil || c.processGroupID <= 0 {
		return errors.New("hermes process-group identity is unavailable")
	}

	if timeout <= 0 {
		timeout = time.Second
	}

	deadline := time.Now().Add(timeout)
	_ = c.signal(syscall.SIGTERM)

	termDeadline := time.Now().Add(500 * time.Millisecond)
	if termDeadline.After(deadline) {
		termDeadline = deadline
	}

	if err := c.waitUntilEmpty(termDeadline); err == nil {
		return nil
	}

	_ = c.signal(syscall.SIGKILL)

	if err := c.waitUntilEmpty(deadline); err != nil {
		return fmt.Errorf("hermes process group %d did not become quiescent: %w", c.processGroupID, err)
	}

	return nil
}

func (c *processContainment) waitUntilEmpty(deadline time.Time) error {
	for {
		alive, err := c.alive()
		if err != nil {
			return err
		}

		if !alive {
			return nil
		}

		if !time.Now().Before(deadline) {
			return errors.New("deadline exceeded")
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (c *processContainment) alive() (bool, error) {
	err := processKill(-c.processGroupID, 0)
	switch {
	case err == nil, errors.Is(err, syscall.EPERM):
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	default:
		return false, fmt.Errorf("probe Hermes process group %d: %w", c.processGroupID, err)
	}
}

func (c *processContainment) signal(signal syscall.Signal) error {
	err := processKill(-c.processGroupID, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}

	return err
}

func (*processContainment) close() error { return nil }

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
