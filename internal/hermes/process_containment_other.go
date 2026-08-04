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

type processContainment struct{}

func startContainedProcess(*exec.Cmd, ...ContainmentSpec) (*processContainment, error) {
	return nil, fmt.Errorf("hermes runtime containment is unsupported on %s", runtime.GOOS)
}

func (*processContainment) complete(time.Duration) error {
	return errors.New("hermes runtime containment is unavailable")
}

func (*processContainment) descendantCount() (int, bool) { return 0, false }

func (*processContainment) terminate(cmd *exec.Cmd) error { return terminateProcess(cmd) }

func (*processContainment) kill(cmd *exec.Cmd) error { return killProcess(cmd) }

func (*processContainment) close() error { return nil }

func (*processContainment) directChild(*exec.Cmd) *directChildWait { return nil }
