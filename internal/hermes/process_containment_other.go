//go:build !unix

package hermes

import (
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"time"
)

func configureHermesProcess(*exec.Cmd) {}

// processContainment on this platform exists only for ordinary same-identity
// execution. There is no subreaper and no process group to probe, so the
// boundary is exactly the direct child this wrapper started.
type processContainment struct {
	direct            *directChildWait
	terminateFn       func() error
	killFn            func() error
	completeFn        func(time.Duration) error
	descendantCountFn func() (int, bool)
	closeFn           func() error
}

// startContainedProcess refuses every boundary this platform cannot build. An
// omitted policy is served by the ordinary launch path, which this platform
// supports on the same terms as every other; a spec that asked for anything
// stronger is refused here rather than quietly downgraded to it, so this
// backend and the unix one answer a requested boundary the same way.
func startContainedProcess(cmd *exec.Cmd, specs ...ContainmentSpec) (*processContainment, error) {
	var spec ContainmentSpec
	if len(specs) > 0 {
		spec = specs[0]
	}

	if spec.Isolation != nil {
		return nil, fmt.Errorf("explicit process isolation is supported only on linux, not %s", runtime.GOOS)
	}

	if spec.DarwinBestEffort {
		return nil, fmt.Errorf("darwin best-effort containment is supported only on darwin, not %s", runtime.GOOS)
	}

	return startOrdinaryProcess(cmd)
}

func newOrdinaryContainment(process ordinaryChild, direct *directChildWait) *processContainment {
	containment := &processContainment{direct: direct}

	containment.completeFn = func(timeout time.Duration) error {
		return completeOrdinaryDirectChild(process, direct, timeout)
	}
	containment.terminateFn = func() error { return process.terminate() }
	containment.killFn = func() error { return process.kill() }
	containment.descendantCountFn = func() (int, bool) { return 0, false }
	containment.closeFn = func() error { return nil }

	return containment
}

func (c *processContainment) complete(timeout time.Duration) error {
	if c == nil || c.completeFn == nil {
		return errors.New("hermes runtime containment is unavailable")
	}

	return c.completeFn(timeout)
}

func (c *processContainment) descendantCount() (int, bool) {
	if c == nil || c.descendantCountFn == nil {
		return 0, false
	}

	return c.descendantCountFn()
}

func (c *processContainment) terminate(cmd *exec.Cmd) error {
	if c == nil || c.terminateFn == nil {
		return terminateProcess(cmd)
	}

	return c.terminateFn()
}

func (c *processContainment) kill(cmd *exec.Cmd) error {
	if c == nil || c.killFn == nil {
		return killProcess(cmd)
	}

	return c.killFn()
}

func (c *processContainment) close() error {
	if c == nil || c.closeFn == nil {
		return nil
	}

	return c.closeFn()
}

func (c *processContainment) directChild(cmd *exec.Cmd) *directChildWait {
	if c == nil {
		return nil
	}

	if c.direct == nil {
		c.direct = installDirectChildWait(cmd, false)
	}

	c.direct.begin()

	return c.direct
}
