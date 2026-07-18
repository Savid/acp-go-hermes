//go:build darwin

package hermes

import "fmt"

func validateProcessContainment(bestEffort bool) error {
	if !bestEffort {
		return fmt.Errorf("%w: Darwin containment is unavailable without explicit best-effort opt-in", ErrProcessContainmentIncomplete)
	}

	return nil
}
