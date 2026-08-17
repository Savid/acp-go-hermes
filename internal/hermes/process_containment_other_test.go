//go:build !unix

package hermes

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNonUnixContainmentRefusesEveryStrongerBoundary proves this backend
// refuses a boundary it cannot build rather than starting the process under a
// weaker one. Start validates both requests before reaching here, so these are
// the defence-in-depth refusals; the spawn assertion is that no child exists to
// clean up after either.
func TestNonUnixContainmentRefusesEveryStrongerBoundary(t *testing.T) {
	t.Parallel()

	for name, spec := range map[string]ContainmentSpec{
		"explicit policy":    {Isolation: &ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{}}},
		"darwin best effort": {DarwinBestEffort: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			command := exec.Command("cmd")

			containment, err := startContainedProcess(command, spec)
			require.ErrorContains(t, err, "supported only on")
			require.Nil(t, containment)
			require.Nil(t, command.Process, "a refused boundary must start no process")
		})
	}
}
