package hermes

import (
	"errors"
)

// validateProcessContainment answers only the containment-backend question:
// whether the requested backend can exist on this platform. The Darwin
// best-effort process-group backend is the sole opt-in, and it exists nowhere
// else.
//
// Whether an explicit hardened policy can be honoured is a separate question
// answered by validateProcessIsolation, which carries the Linux-only gate. An
// omitted policy asks for neither: ordinary same-identity execution needs no
// containment backend and is supported on every platform.
func validateProcessContainment(bestEffort bool) error {
	if bestEffort && processRuntimePlatform != processPlatformDarwin {
		return errors.New("darwin best-effort containment is supported only on darwin")
	}

	return nil
}
