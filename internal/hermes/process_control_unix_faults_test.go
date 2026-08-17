//go:build unix

package hermes

import (
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestContainmentRefusesASupervisorThatExitedWithoutProof proves an explicit
// negative proof is a containment failure rather than a completion. The
// supervisor writes its proof byte only after it has killed and reaped every
// descendant, so a supervisor that answers "not proved" means native processes
// may still be running. Completion must not fall back to signalling the process
// group either: the group probe cannot see a descendant that called setsid, so
// treating a negative proof as "just check the group" would report quiescence
// that was never established.
func TestContainmentRefusesASupervisorThatExitedWithoutProof(t *testing.T) {
	signals := 0
	original := processKill
	processKill = func(int, syscall.Signal) error {
		signals++

		return syscall.ESRCH
	}
	t.Cleanup(func() { processKill = original })

	proof := make(chan bool, 1)
	proof <- false

	err := (&processContainment{processGroupID: 4242, proof: proof}).complete(time.Second)
	require.ErrorContains(t, err, "exited without proving descendant quiescence")
	require.Zero(t, signals)
}
