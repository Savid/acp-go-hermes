//go:build !unix && !windows

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

func startContainedProcess(*exec.Cmd) (*processContainment, error) {
	return nil, fmt.Errorf("hermes runtime containment is unsupported on %s", runtime.GOOS)
}

func (*processContainment) quiesce(time.Duration) error {
	return errors.New("hermes runtime containment is unavailable")
}

func (*processContainment) descendantCount() (int, bool) { return 0, false }

func (*processContainment) close() error { return nil }
