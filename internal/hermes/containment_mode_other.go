//go:build !darwin

package hermes

import (
	"errors"
	"runtime"
)

func validateProcessContainment(bestEffort bool) error {
	if bestEffort {
		return errors.New("darwin best-effort containment is supported only on darwin")
	}

	if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		return ErrProcessContainmentIncomplete
	}

	return nil
}
