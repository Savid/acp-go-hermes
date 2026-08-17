//go:build !darwin

package hermes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestContainmentOperationsAreDarwinOnly proves the two operator-facing
// containment operations refuse everywhere the Darwin backend does not exist.
// They are compiled on these platforms — the CLI links them — so an unexercised
// refusal would be a launch-time surprise rather than a build-time one.
func TestContainmentOperationsAreDarwinOnly(t *testing.T) {
	t.Parallel()

	diagnostics, err := DiagnoseContainment("scratch")
	require.ErrorContains(t, err, "available only on darwin")
	require.Nil(t, diagnostics)

	cleanup, err := CleanupContainment("scratch", "runtime", true)
	require.ErrorContains(t, err, "available only on darwin")
	require.Equal(t, ContainmentCleanupResult{}, cleanup)
}
