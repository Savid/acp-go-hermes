//go:build unix && !linux && !darwin

package hermes

import (
	"fmt"
	"os/exec"
	"runtime"
)

func startUnixContainedProcess(*exec.Cmd, ContainmentSpec) (*processContainment, error) {
	return nil, fmt.Errorf("%w: detached native descendants cannot be proved on %s", ErrProcessContainmentIncomplete, runtime.GOOS)
}
