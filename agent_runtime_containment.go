package hermesacp

import (
	"errors"
	"runtime"
)

var agentRuntimePlatform = runtime.GOOS

const agentRuntimeDarwin = "darwin"

func containmentMode(options Options) RuntimeContainmentMode {
	if options.DarwinBestEffortContainment && agentRuntimePlatform != agentRuntimeDarwin {
		return RuntimeContainmentUnavailable
	}

	switch agentRuntimePlatform {
	case "linux", "windows":
		return RuntimeContainmentAuthoritative
	case agentRuntimeDarwin:
		if options.DarwinBestEffortContainment {
			return RuntimeContainmentBestEffort
		}
	}

	return RuntimeContainmentUnavailable
}

func validateContainmentOptions(options Options) error {
	if options.DarwinBestEffortContainment && agentRuntimePlatform != agentRuntimeDarwin {
		return errors.New("darwin best-effort containment is supported only on darwin")
	}

	return nil
}
