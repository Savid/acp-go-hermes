//go:build linux

package hermes

import (
	"errors"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneCovDirEntry is a synthetic /proc listing entry. A real
// directory read can never hand the same name back twice, so a spoofed or
// replayed listing can only be modelled with a hand-built entry.
type agentStandaloneCovDirEntry struct{ name string }

func (entry agentStandaloneCovDirEntry) Name() string { return entry.name }

func (agentStandaloneCovDirEntry) IsDir() bool { return true }

func (agentStandaloneCovDirEntry) Type() os.FileMode { return os.ModeDir }

func (agentStandaloneCovDirEntry) Info() (os.FileInfo, error) {
	return nil, errors.New("synthetic entry has no metadata")
}

// agentStandaloneCovProcSeams redirects the two procfs readers the vacancy
// proof uses and restores them when the case ends.
func agentStandaloneCovProcSeams(
	t *testing.T,
	readDir func(string) ([]os.DirEntry, error),
	readFile func(string) ([]byte, error),
) {
	t.Helper()
	previousReadDir, previousReadFile := agentStandaloneReadDir, agentStandaloneReadFile
	t.Cleanup(func() {
		agentStandaloneReadDir, agentStandaloneReadFile = previousReadDir, previousReadFile
	})
	if readDir != nil {
		agentStandaloneReadDir = readDir
	}
	if readFile != nil {
		agentStandaloneReadFile = readFile
	}
}

// TestAgentStandaloneCovVacancyAbortsBeforeAndDuringProcessEnumeration proves
// the vacancy proof honours its deadline and cancellation before it looks at
// /proc at all, and again between processes, and that it refuses outright when
// /proc itself cannot be listed. A scan that ran on regardless would report an
// identity vacant on the strength of a partial or aborted enumeration.
func TestAgentStandaloneCovVacancyAbortsBeforeAndDuringProcessEnumeration(t *testing.T) {
	t.Run("expired deadline", func(t *testing.T) {
		scanned := false
		agentStandaloneCovProcSeams(t, func(string) ([]os.DirEntry, error) {
			scanned = true

			return nil, nil
		}, nil)

		err := proveAgentStandaloneIdentityVacant(62401, 62402, time.Now().Add(-time.Second), nil, nil)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.False(t, scanned, "an expired claim must not enumerate /proc")
	})

	t.Run("unreadable proc", func(t *testing.T) {
		wantErr := errors.New("injected /proc listing failure")
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			require.Equal(t, "/proc", path)

			return nil, wantErr
		}, nil)

		err := proveAgentStandaloneIdentityVacant(62403, 62404, time.Now().Add(time.Second), nil, nil)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("canceled between processes", func(t *testing.T) {
		processes := agentStandaloneTestDirEntries(t, "101")
		canceled := make(chan struct{})
		inspected := false
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				close(canceled)

				return processes, nil
			}
			inspected = true

			return nil, os.ErrNotExist
		}, nil)

		err := proveAgentStandaloneIdentityVacant(62405, 62406, time.Now().Add(time.Second), canceled, nil)
		require.ErrorIs(t, err, errAgentStandaloneCanceled)
		require.False(t, inspected, "cancellation must stop the scan before the next process")
	})
}

// TestAgentStandaloneCovProcessTaskVacancyFailsClosed proves each way a task
// enumeration can go wrong ends the proof rather than being read as "no
// matching task". Only a complete, twice-agreeing enumeration may conclude a
// process holds none of the claimed credentials.
func TestAgentStandaloneCovProcessTaskVacancyFailsClosed(t *testing.T) {
	const uid, gid = uint32(62411), uint32(62412)
	processes := agentStandaloneTestDirEntries(t, "101")
	tasks := agentStandaloneTestDirEntries(t, "101")

	t.Run("task listing fails", func(t *testing.T) {
		wantErr := errors.New("injected task listing failure")
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				return processes, nil
			}

			return nil, wantErr
		}, nil)

		err := proveAgentStandaloneIdentityVacant(uid, gid, time.Now().Add(time.Second), nil, nil)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "inspect process 101 tasks")
	})

	t.Run("canceled between tasks", func(t *testing.T) {
		canceled := make(chan struct{})
		reads := 0
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				return processes, nil
			}
			close(canceled)

			return tasks, nil
		}, func(string) ([]byte, error) {
			reads++

			return agentStandaloneTestStatus(1, 1, nil), nil
		})

		err := proveAgentStandaloneIdentityVacant(uid, gid, time.Now().Add(time.Second), canceled, nil)
		require.ErrorIs(t, err, errAgentStandaloneCanceled)
		require.Zero(t, reads, "cancellation must stop before reading task credentials")
	})

	t.Run("non-numeric task entry", func(t *testing.T) {
		spoofed := agentStandaloneTestDirEntries(t, "not-a-task")
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				return processes, nil
			}

			return spoofed, nil
		}, nil)

		err := proveAgentStandaloneIdentityVacant(uid, gid, time.Now().Add(time.Second), nil, nil)
		require.ErrorContains(t, err, `process 101 task directory contains invalid entry "not-a-task"`)
	})

	t.Run("unreadable task credentials", func(t *testing.T) {
		wantErr := errors.New("injected task status failure")
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				return processes, nil
			}

			return tasks, nil
		}, func(string) ([]byte, error) { return nil, wantErr })

		err := proveAgentStandaloneIdentityVacant(uid, gid, time.Now().Add(time.Second), nil, nil)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "inspect task credentials /proc/101/task/101/status")
	})

	t.Run("deadline expires while retrying an unstable process", func(t *testing.T) {
		canceled := make(chan struct{})
		listings := 0
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				return processes, nil
			}
			listings++

			return tasks, nil
		}, func(string) ([]byte, error) {
			close(canceled)

			return nil, os.ErrNotExist
		})

		err := proveAgentStandaloneIdentityVacant(uid, gid, time.Now().Add(time.Second), canceled, nil)
		require.ErrorIs(t, err, errAgentStandaloneCanceled)
		require.Equal(t, 1, listings, "the retry must recheck cancellation before re-listing")
	})

	t.Run("process exits between the two listings", func(t *testing.T) {
		listings := 0
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				return processes, nil
			}
			listings++
			if listings == 1 {
				return tasks, nil
			}

			return nil, os.ErrNotExist
		}, func(string) ([]byte, error) { return agentStandaloneTestStatus(1, 1, nil), nil })

		require.NoError(t, proveAgentStandaloneIdentityVacant(uid, gid, time.Now().Add(time.Second), nil, nil))
		require.Equal(t, 2, listings)
	})

	t.Run("second listing fails", func(t *testing.T) {
		wantErr := errors.New("injected re-listing failure")
		listings := 0
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				return processes, nil
			}
			listings++
			if listings == 1 {
				return tasks, nil
			}

			return nil, wantErr
		}, func(string) ([]byte, error) { return agentStandaloneTestStatus(1, 1, nil), nil })

		err := proveAgentStandaloneIdentityVacant(uid, gid, time.Now().Add(time.Second), nil, nil)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "reinspect process 101 tasks")
	})

	t.Run("task set changes size", func(t *testing.T) {
		short := agentStandaloneTestDirEntries(t, "101")
		long := agentStandaloneTestDirEntries(t, "101", "102")
		listings := 0
		agentStandaloneCovProcSeams(t, func(path string) ([]os.DirEntry, error) {
			if path == "/proc" {
				return processes, nil
			}
			listings++
			if listings%2 == 1 {
				return short, nil
			}

			return long, nil
		}, func(string) ([]byte, error) { return agentStandaloneTestStatus(1, 1, nil), nil })

		err := proveAgentStandaloneIdentityVacant(uid, gid, time.Now().Add(time.Second), nil, nil)
		require.ErrorContains(t, err, "did not stabilize within 64 attempts")
		require.Equal(t, 128, listings)
	})
}

// TestAgentStandaloneCovTaskSetsEqualRejectsRepeatedNames proves a listing that
// repeats a task name is never treated as equal to another listing, even when
// the two have the same length. Accepting it would let a duplicated name mask a
// task that appeared between the two enumerations.
func TestAgentStandaloneCovTaskSetsEqualRejectsRepeatedNames(t *testing.T) {
	repeated := []os.DirEntry{
		agentStandaloneCovDirEntry{name: "101"},
		agentStandaloneCovDirEntry{name: "101"},
	}
	distinct := []os.DirEntry{
		agentStandaloneCovDirEntry{name: "101"},
		agentStandaloneCovDirEntry{name: "102"},
	}
	require.False(t, agentStandaloneTaskSetsEqual(repeated, distinct))
	require.False(t, agentStandaloneTaskSetsEqual(repeated, repeated))
	require.False(t, agentStandaloneTaskSetsEqual(distinct, repeated))
	require.True(t, agentStandaloneTaskSetsEqual(distinct, distinct))
}

// TestAgentStandaloneCovDoubleVacancyProofRequiresBothPasses proves the
// two-pass vacancy proof refuses when the second pass disagrees with the first,
// and names the pass that failed. A single successful pass is not enough to
// rebind an identity that a task could re-enter between the two.
func TestAgentStandaloneCovDoubleVacancyProofRequiresBothPasses(t *testing.T) {
	wantErr := errors.New("task reappeared between passes")
	previous := agentStandaloneVacancyScan
	scans := 0
	agentStandaloneVacancyScan = func(
		uid, gid uint32, _ time.Time, _ <-chan struct{}, _ <-chan os.Signal,
	) error {
		require.Equal(t, uint32(62421), uid)
		require.Equal(t, uint32(62422), gid)
		scans++
		if scans == 2 {
			return wantErr
		}

		return nil
	}
	t.Cleanup(func() { agentStandaloneVacancyScan = previous })

	err := proveAgentStandaloneIdentityVacantTwice(62421, 62422, time.Now().Add(time.Second), nil, nil)
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "second standalone task vacancy proof")
	require.Equal(t, 2, scans)
}

// TestAgentStandaloneCovAcquisitionDeadlineAndSignalsAreRefusals proves the
// acquisition gate refuses once the 30-second budget is gone and refuses when a
// termination signal is pending, naming the signal. Treating either as "keep
// going" would let a claim outlive the supervisor that asked for it.
func TestAgentStandaloneCovAcquisitionDeadlineAndSignalsAreRefusals(t *testing.T) {
	require.ErrorContains(t,
		checkAgentStandaloneAcquisition(time.Now().Add(-time.Millisecond), nil, nil),
		"exceeded 30 seconds",
	)
	require.NoError(t, checkAgentStandaloneAcquisition(time.Now().Add(time.Minute), nil, nil))
	require.NoError(t, checkAgentStandaloneAcquisition(time.Time{}, nil, nil))

	signals := make(chan os.Signal, 1)
	signals <- unix.SIGTERM
	require.ErrorContains(t,
		checkAgentStandaloneAcquisition(time.Now().Add(time.Minute), nil, signals),
		"interrupted by terminated",
	)
}

// TestAgentStandaloneCovRetryNeverSleepsPastTheDeadline proves the retry helper
// refuses immediately once the budget is gone, clamps its final sleep to the
// remaining budget instead of overshooting it, and refuses on a pending signal.
func TestAgentStandaloneCovRetryNeverSleepsPastTheDeadline(t *testing.T) {
	require.ErrorContains(t,
		waitAgentStandaloneRetry(time.Now().Add(-time.Millisecond), nil, nil),
		"exceeded 30 seconds",
	)

	started := time.Now()
	err := waitAgentStandaloneRetry(started.Add(time.Millisecond), nil, nil)
	elapsed := time.Since(started)
	require.ErrorContains(t, err, "exceeded 30 seconds")
	require.Less(t, elapsed, agentStandaloneRetry, "the final wait must be clamped to the remaining budget")

	signals := make(chan os.Signal, 1)
	signals <- unix.SIGINT
	require.ErrorContains(t,
		waitAgentStandaloneRetry(time.Now().Add(time.Minute), nil, signals),
		"interrupted by interrupt",
	)
}

// TestAgentStandaloneCovNilIdentityCloseIsANoOp proves closing an identity that
// was never acquired reports success without touching descriptors, so a failed
// acquisition path may always defer Close.
func TestAgentStandaloneCovNilIdentityCloseIsANoOp(t *testing.T) {
	var identity *agentStandaloneIdentity
	require.NoError(t, identity.Close())
}
