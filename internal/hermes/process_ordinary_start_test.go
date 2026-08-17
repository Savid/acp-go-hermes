//go:build unix

package hermes

import (
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartOrdinaryProcessRefusesAnIncompleteTarget(t *testing.T) {
	for name, target := range map[string]*exec.Cmd{
		"nil":       nil,
		"no path":   {Args: []string{"hermes"}},
		"no args":   {Path: "/bin/true"},
		"empty cmd": {},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := startOrdinaryProcess(target)
			require.ErrorContains(t, err, "ordinary launch target is unavailable")
		})
	}
}

func TestStartOrdinaryProcessReportsAFailedStart(t *testing.T) {
	missing := exec.Command(filepath.Join(t.TempDir(), "missing"))

	_, err := startOrdinaryProcess(missing)
	require.Error(t, err)
}

// TestOrdinaryContainmentSurfacesReportNoInventory exercises the control
// surfaces an ordinary boundary exposes. Terminate and kill act on the started
// child, close is a no-op because nothing durable was taken, and the descendant
// inventory is permanently unavailable rather than zero.
func TestOrdinaryContainmentSurfacesReportNoInventory(t *testing.T) {
	terminated, killed := 0, 0
	wantTerminate, wantKill := errors.New("terminate"), errors.New("kill")

	direct := installDirectChildWait(exec.Command("/bin/true"), true)
	containment := newOrdinaryContainment(ordinaryChild{
		pid: 1,
		terminate: func() error {
			terminated++

			return wantTerminate
		},
		kill: func() error {
			killed++

			return wantKill
		},
	}, direct)

	require.ErrorIs(t, containment.terminate(nil), wantTerminate)
	require.ErrorIs(t, containment.kill(nil), wantKill)
	require.Equal(t, 1, terminated)
	require.Equal(t, 1, killed)
	require.NoError(t, containment.close())

	count, available := containment.descendantCount()
	require.False(t, available, "ordinary execution must publish no inventory")
	require.Zero(t, count)
}

// TestOrdinaryBoundaryCompletionRequiresAStartedChild proves completion refuses
// when no identity was ever established, rather than reporting a boundary it
// never had.
func TestOrdinaryBoundaryCompletionRequiresAStartedChild(t *testing.T) {
	require.ErrorContains(t,
		completeOrdinaryBoundary(nil, nil, time.Second),
		"ordinary process identity is unavailable",
	)

	require.ErrorContains(t,
		completeOrdinaryBoundary(&processContainment{}, nil, time.Second),
		"ordinary process identity is unavailable",
	)
}

// TestStartOrdinaryProcessControlCallbacksUseSeparateChildren executes the
// terminate and kill closures installed by the real launch path. Each signal
// gets its own process group and the direct child is reaped before the next
// case, avoiding a second signal against an identity the kernel may already
// have recycled.
func TestStartOrdinaryProcessControlCallbacksUseSeparateChildren(t *testing.T) {
	for _, control := range []struct {
		name string
		run  func(*processContainment) error
	}{
		{name: "terminate", run: func(containment *processContainment) error { return containment.terminate(nil) }},
		{name: "kill", run: func(containment *processContainment) error { return containment.kill(nil) }},
	} {
		t.Run(control.name, func(t *testing.T) {
			command := exec.Command("/bin/sh", "-c", "sleep 30")
			configureHermesProcess(command)

			containment, err := startOrdinaryProcess(command)
			require.NoError(t, err)
			wait := containment.directChild(command)

			require.NoError(t, control.run(containment))
			select {
			case <-wait.done:
			case <-time.After(10 * time.Second):
				t.Fatal("ordinary child was not reaped after control signal")
			}
			require.NoError(t, containment.close())
		})
	}
}

// TestOrdinaryBoundaryCompletionRunsTheFullLadder drives a real child through
// the default deadline and the group teardown, then proves a group that never
// becomes unobservable is reported as a completion failure.
func TestOrdinaryBoundaryCompletionRunsTheFullLadder(t *testing.T) {
	command := exec.Command("/bin/sh", "-c", "sleep 30")
	configureHermesProcess(command)

	containment, err := startOrdinaryProcess(command)
	require.NoError(t, err)

	// A zero timeout selects the default ordinary deadline and drives the real
	// child through the boundary's kill-and-reap ladder. The terminate and kill
	// callbacks are exercised independently above; signalling this real group
	// twice would introduce a PID/PGID-reuse race into the test itself.
	require.NoError(t, containment.complete(0))

	// A group that stays observable exhausts the ladder and fails closed.
	previousKill := processKill
	t.Cleanup(func() { processKill = previousKill })

	processKill = func(int, syscall.Signal) error { return nil }

	stuck := newOrdinaryContainment(ordinaryChild{
		pid:       1,
		terminate: func() error { return nil },
		kill:      func() error { return nil },
	}, installDirectChildWait(exec.Command("/bin/true"), true))

	require.ErrorContains(t, stuck.complete(20*time.Millisecond), "did not become quiescent")
}
