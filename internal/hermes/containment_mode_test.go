package hermes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateProcessContainmentGatesOnlyTheDarwinBackend proves the two
// questions stay separate: this check governs the opt-in Darwin backend, and an
// omitted policy — which needs no backend at all — is accepted on every
// platform rather than being reported as unavailable there.
func TestValidateProcessContainmentGatesOnlyTheDarwinBackend(t *testing.T) {
	original := processRuntimePlatform
	t.Cleanup(func() { processRuntimePlatform = original })

	for _, goos := range []string{processPlatformDarwin, processPlatformLinux, "windows", "freebsd", "openbsd", "plan9"} {
		t.Run(goos, func(t *testing.T) {
			processRuntimePlatform = goos

			require.NoError(t, validateProcessContainment(false))

			if goos == processPlatformDarwin {
				require.NoError(t, validateProcessContainment(true))

				return
			}

			require.ErrorContains(t, validateProcessContainment(true), "supported only on darwin")
		})
	}
}
