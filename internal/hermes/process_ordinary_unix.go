//go:build unix

package hermes

import (
	"errors"
	"time"
)

// newOrdinaryContainment wraps an already-started native child in the only
// boundary ordinary same-identity execution owns: that direct child and the
// process group it leads. No subreaper is installed, no credential is dropped,
// and no authority is held, so this boundary can say when the work it started
// directly has gone and nothing more.
//
// descendantCountFn is wired to an explicit "unavailable" rather than left nil
// so that a descendant which left the group can never be reported as a zero
// inventory. Ordinary mode publishes no provider-descendant sample at all.
func newOrdinaryContainment(process ordinaryChild, direct *directChildWait) *processContainment {
	containment := &processContainment{processGroupID: process.pid, direct: direct}

	containment.completeFn = func(timeout time.Duration) error {
		return completeOrdinaryBoundary(containment, direct, timeout)
	}
	containment.terminateFn = func() error { return process.terminate() }
	containment.killFn = func() error { return process.kill() }
	containment.descendantCountFn = func() (int, bool) { return 0, false }
	containment.closeFn = func() error { return nil }

	return containment
}

// completeOrdinaryBoundary tears down the started process group and then waits
// for the direct child to be reaped. Both halves are required: the group probe
// says the processes are gone, and the reap says this wrapper has collected the
// child it is responsible for.
func completeOrdinaryBoundary(containment *processContainment, direct *directChildWait, timeout time.Duration) error {
	if containment == nil || containment.processGroupID <= 0 {
		return errors.New("hermes ordinary process identity is unavailable")
	}

	if timeout <= 0 {
		timeout = ordinaryContainmentDeadline
	}

	deadline := time.Now().Add(timeout)

	if err := containment.completeProcessGroup(timeout); err != nil {
		return err
	}

	return direct.awaitReaped(max(time.Until(deadline), 0))
}
