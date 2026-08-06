package hermes

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestProcessStartRefusesAnUnusableIsolationPolicy proves Start validates the
// isolation policy before it resolves an executable, creates a home, probes a
// version or spawns anything. The policy is what keeps the native process off
// the wrapper's identity, so a session must not reach a launch with a policy
// that was never accepted.
func TestProcessStartRefusesAnUnusableIsolationPolicy(t *testing.T) {
	restoreProcessSeams(t)
	processCovRefuseLaunch(t)
	commandContext = func(context.Context, string, ...string) *exec.Cmd {
		t.Error("refused start resolved a native command")

		return exec.Command(os.DevNull)
	}

	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
	})
	options.Isolation = &ProcessIsolation{UID: 0, GID: 0, BaseEnvironment: map[string]string{}}

	_, err := Start(t.Context(), options)
	require.ErrorContains(t, err, "validate Hermes process isolation")
	require.ErrorContains(t, err, "UID and GID must be nonzero")
}

// TestProcessStartReportsContainmentRefusalAndRemovesTheShim proves a native
// launch that is refused by containment is reported as a spawn-stage failure
// and leaves nothing behind. The browser shim is created before the spawn, so
// a refusal that forgot to remove it would leak a directory that shadows the
// operator's launchers for every later process that inherits the same PATH.
func TestProcessStartReportsContainmentRefusalAndRemovesTheShim(t *testing.T) {
	restoreProcessSeams(t)
	processCovRestoreShimSeam(t)

	refused := errors.New("containment refused")
	startHermesContainedProcess = func(*exec.Cmd, ...ContainmentSpec) (*processContainment, error) {
		return nil, refused
	}

	shimDirs := []string{}
	newProcessShim := newProcessBrowserShim
	newProcessBrowserShim = func(parent string) (*browserShim, error) {
		shim, err := newProcessShim(parent)
		if shim != nil {
			shimDirs = append(shimDirs, shim.dir)
		}

		return shim, err
	}

	stages := map[string]error{}
	executable := fakeHermesExecutable(t, fakeProcessModeOK)
	markExecutableProbed(executable)
	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: executable,
		Home:           t.TempDir(),
		ObserveStartupStage: func(_ context.Context, _ string, stage string, _ time.Duration, err error) {
			stages[stage] = err
		},
	})

	_, err := Start(t.Context(), options)
	require.ErrorIs(t, err, refused)
	require.Equal(t, map[string]error{"spawn": refused}, stages)
	require.Len(t, shimDirs, 1)
	require.NoDirExists(t, shimDirs[0])
}

// TestProcessVersionProbeReleasesEverythingItTookForAnUnusableEnvironment
// proves the version probe hands back every resource it acquired when the
// process environment cannot be composed. The probe holds a native admission, a
// scratch admission and a generation directory before it builds that
// environment; failing to release them would strand an admission slot for the
// lifetime of the wrapper and leave the generation on disk. The generation is
// released rather than retained because nothing was ever launched into it.
func TestProcessVersionProbeReleasesEverythingItTookForAnUnusableEnvironment(t *testing.T) {
	restoreProcessSeams(t)
	processCovRefuseLaunch(t)

	native, scratch, retained := 0, 0, 0
	probeRoots := []string{}
	realMkdirTemp := mkdirTemp
	mkdirTemp = func(parent string, pattern string) (string, error) {
		root, err := realMkdirTemp(parent, pattern)
		if err == nil {
			probeRoots = append(probeRoots, root)
		}

		return root, err
	}

	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
		Env:            map[string]string{"HERMES=INVALID": "1"},
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) {
			return func() { native++ }, func() { scratch++ }, nil
		},
		RetainDiscoveryRoot: func(string, error) { retained++ },
	})

	_, err := Start(t.Context(), options)
	require.ErrorContains(t, err, `process environment contains invalid key "HERMES=INVALID"`)
	require.Equal(t, []int{1, 1, 0}, []int{native, scratch, retained})
	require.Len(t, probeRoots, 1)
	require.NoDirExists(t, probeRoots[0])
}

// processCovRefuseLaunch fails the test if a refusal still reaches a native
// launch, so a refusal that is supposed to happen first cannot pass by
// launching and then failing later.
func processCovRefuseLaunch(t *testing.T) {
	t.Helper()
	startHermesContainedProcess = func(*exec.Cmd, ...ContainmentSpec) (*processContainment, error) {
		t.Error("refused start reached native containment")

		return nil, errors.New("unreachable")
	}
}

func processCovRestoreShimSeam(t *testing.T) {
	t.Helper()
	shim := newProcessBrowserShim
	t.Cleanup(func() { newProcessBrowserShim = shim })
}
