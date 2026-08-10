//go:build !unix

package hermes

import (
	"errors"
	"os/exec"
)

// validateProcessIsolationPlatform refuses an explicit policy here. Ordinary
// same-identity execution remains supported on this platform and never reaches
// this function.
func validateProcessIsolationPlatform() error {
	return errors.New("explicit process isolation is supported only on linux")
}
func applyProcessIsolation(_ *exec.Cmd, isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
func verifyProcessIsolation(isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
