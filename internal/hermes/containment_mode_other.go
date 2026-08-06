//go:build !darwin

package hermes

import (
	"errors"
	"runtime"
)

var processRuntimeGOOS = runtime.GOOS

func validateProcessContainment(bestEffort bool) error {
	if bestEffort {
		return errors.New("darwin best-effort containment is supported only on darwin")
	}

	if processRuntimeGOOS != processPlatformLinux {
		return ErrProcessContainmentIncomplete
	}

	return nil
}
