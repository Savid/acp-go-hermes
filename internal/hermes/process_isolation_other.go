//go:build !unix

package hermes

import (
	"errors"
	"os/exec"
)

func validateProcessIsolationPlatform() error {
	return errors.New("process isolation is unsupported on this platform")
}
func applyProcessIsolation(_ *exec.Cmd, isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
func verifyProcessIsolation(isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
