//go:build unix && !linux

package hermes

import (
	"fmt"
	"os/exec"
	"runtime"
)

func startUnixContainedProcess(*exec.Cmd) (*processContainment, error) {
	return nil, fmt.Errorf("%w: detached native descendants cannot be proved on %s", ErrProcessTreeUnproven, runtime.GOOS)
}
