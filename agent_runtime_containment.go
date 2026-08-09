package hermesacp

import (
	"errors"
	"os"
	"runtime"
)

var agentRuntimePlatform = runtime.GOOS

const (
	agentRuntimeDarwin  = "darwin"
	agentRuntimeLinux   = "linux"
	agentRuntimeWindows = "windows"
)

// containmentEffectiveUID is the seam the shared-identity report is derived
// through. The mode is selected from a faked platform in tests, so the identity
// it is compared against has to be selectable there too.
var containmentEffectiveUID = os.Geteuid

// sharedProcessIdentity reports whether the configured native identity is the
// identity this process already runs as. Root never qualifies: a zero effective
// uid is the trusted supervisor identity, and the native uid is required to be
// nonzero.
func sharedProcessIdentity(isolation *ProcessIsolation) bool {
	if isolation == nil || agentRuntimePlatform != agentRuntimeLinux {
		return false
	}

	effectiveUID := containmentEffectiveUID()

	return effectiveUID > 0 && uint64(isolation.UID) == uint64(effectiveUID)
}

// provesWholeTreeLifecycle reports whether the selected boundary can prove that
// every process it started has exited. Both Linux boundaries can: they differ
// in whether the agent runs under its own credentials, not in what the
// subreaper observes.
func (mode RuntimeContainmentMode) provesWholeTreeLifecycle() bool {
	return mode == RuntimeContainmentAuthoritative || mode == RuntimeContainmentSharedIdentity
}

func containmentMode(options Options) RuntimeContainmentMode {
	if options.DarwinBestEffortContainment && agentRuntimePlatform != agentRuntimeDarwin {
		return RuntimeContainmentUnavailable
	}

	switch agentRuntimePlatform {
	case agentRuntimeLinux:
		if sharedProcessIdentity(options.ProcessIsolation) {
			return RuntimeContainmentSharedIdentity
		}

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
