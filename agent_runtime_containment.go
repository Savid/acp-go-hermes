package hermesacp

import (
	"errors"
	"runtime"
)

var agentRuntimePlatform = runtime.GOOS

const (
	agentRuntimeDarwin = "darwin"
	agentRuntimeLinux  = "linux"
)

// provesWholeTreeLifecycle reports whether the selected boundary can prove that
// every process it started has exited. Only the explicit hardened Linux
// backend can: it interposes a subreaper that remains the parent of every
// orphaned descendant and reaps that complete tree.
//
// Ordinary same-identity execution cannot, and neither can Darwin best effort.
// Both complete exactly the boundary they directly own, and a descendant that
// left it is outside what either one observed. Returning false here is what
// keeps the wrapper from publishing a provider-descendant inventory — including
// a terminal zero — for a boundary that never enumerated one.
func (mode RuntimeContainmentMode) provesWholeTreeLifecycle() bool {
	return mode == RuntimeContainmentAuthoritative
}

// containmentMode reports the boundary this Agent's options select. An omitted
// ProcessIsolation is the ordinary default and reports shared_identity on every
// platform the adapter otherwise supports: shared_identity is a non-authoritative
// posture, not a Linux containment achievement, so a platform that cannot host
// the hardened backend still runs ordinary work and still reports it honestly.
func containmentMode(options Options) RuntimeContainmentMode {
	if err := validateContainmentOptions(options); err != nil {
		return RuntimeContainmentUnavailable
	}

	// A supplied policy is the strict Linux boundary or nothing. It never
	// degrades to shared_identity or best effort.
	if options.ProcessIsolation != nil {
		if agentRuntimePlatform == agentRuntimeLinux {
			return RuntimeContainmentAuthoritative
		}

		return RuntimeContainmentUnavailable
	}

	if options.DarwinBestEffortContainment {
		return RuntimeContainmentBestEffort
	}

	return RuntimeContainmentSharedIdentity
}

func validateContainmentOptions(options Options) error {
	if options.DarwinBestEffortContainment && agentRuntimePlatform != agentRuntimeDarwin {
		return errors.New("darwin best-effort containment is supported only on darwin")
	}

	// The two explicit options name incompatible boundaries: an explicit
	// hardened identity policy cannot be downgraded to a process-group
	// approximation, so asking for both is a configuration error rather than a
	// precedence question.
	if options.DarwinBestEffortContainment && options.ProcessIsolation != nil {
		return errors.New("darwin best-effort containment cannot be combined with explicit process isolation")
	}

	return nil
}
