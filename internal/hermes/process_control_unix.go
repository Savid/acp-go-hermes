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

func startContainedProcess(cmd *exec.Cmd, specs ...ContainmentSpec) (*processContainment, error) {
	var spec ContainmentSpec
	if len(specs) > 0 {
		spec = specs[0]
	}

	// An omitted policy selects ordinary same-identity execution, and it selects
	// it here rather than inside a platform backend, so no supervisor, authority,
	// or credential machinery is entered on the way. Darwin best effort is the
	// one stronger boundary a host can layer onto an omitted policy, and it is
	// explicitly opted into.
	if spec.Isolation == nil && !spec.DarwinBestEffort {
		if len(spec.SharedSessionOwnerFiles) != 0 {
			if cmd == nil {
				return nil, errors.New("hermes command is unavailable for inherited session-owner locks")
			}

			cmd.ExtraFiles = append(cmd.ExtraFiles, spec.SharedSessionOwnerFiles...)
		}

		return startOrdinaryProcess(cmd)
	}

	return startUnixContainedProcess(cmd, spec)
}

func (c *processContainment) directChild(cmd *exec.Cmd) *directChildWait {
	if c.direct == nil {
		c.direct = installDirectChildWait(cmd, false)
	}

	c.direct.begin()

	return c.direct
}

func (c *processContainment) complete(timeout time.Duration) error {
	if c != nil && c.completeFn != nil {
		return c.completeFn(timeout)
	}

	return c.completeAuthoritative(timeout)
}

func (c *processContainment) completeAuthoritative(timeout time.Duration) error {
	if c == nil || c.processGroupID <= 0 {
		return errors.New("hermes process-group identity is unavailable")
	}

	if timeout <= 0 {
		timeout = time.Second
	}

	if c.proof != nil {
		deadline := time.Now().Add(timeout)
		select {
		case proved := <-c.proof:
			if !proved {
				return errors.New("hermes supervisor exited without proving descendant quiescence")
			}
		case <-time.After(time.Until(deadline)):
			return errors.New("hermes supervisor did not prove descendant quiescence")
		}

		if err := c.waitUntilEmpty(deadline); err != nil {
			return fmt.Errorf("hermes supervisor process group %d did not become quiescent: %w", c.processGroupID, err)
		}

		return nil
	}

	return c.completeProcessGroup(timeout)
}

// completeProcessGroup runs the bounded SIGTERM-then-SIGKILL ladder against the
// process group this containment leads and waits for that group to become
// unobservable. It is the teardown primitive both the authoritative backend and
// ordinary same-identity execution use; what differs is the claim each makes of
// the result, not the signalling.
func (c *processContainment) completeProcessGroup(timeout time.Duration) error {
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

func (c *processContainment) descendantCount() (int, bool) {
	if c != nil && c.descendantCountFn != nil {
		return c.descendantCountFn()
	}

	// A process-group existence probe proves quiescence, but it cannot
	// enumerate an authoritative nonzero membership count.
	return 0, false
}

func (c *processContainment) terminate(cmd *exec.Cmd) error {
	if c != nil && c.terminateFn != nil {
		return c.terminateFn()
	}

	return terminateProcess(cmd)
}

func (c *processContainment) kill(cmd *exec.Cmd) error {
	if c != nil && c.killFn != nil {
		return c.killFn()
	}

	return killProcess(cmd)
}

func (c *processContainment) close() error {
	if c != nil && c.closeFn != nil {
		return c.closeFn()
	}

	return nil
}

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
