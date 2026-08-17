package hermes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAmbientEnvironmentSnapshotFoldsWindowsSpellings is the portable half of
// the inherited-block evidence, run with the platform seam pinned so every host
// exercises it. A parent that hand-builds a block for CreateProcess can write
// both spellings of one name into it; Go's own launcher keeps the last, and the
// ambient phase is the last seam that still holds the order needed to say which
// one that is. Folding it there rather than carrying two live keys is what
// keeps an inherited block from refusing every launch at the phase merge.
func TestAmbientEnvironmentSnapshotFoldsWindowsSpellings(t *testing.T) {
	originalPlatform := processRuntimePlatform
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	block := []string{
		"Path=C:\\inherited",
		"PATHEXT=.COM",
		"KEPT=yes",
		// A bare entry names no variable, and neither does the leading-'='
		// spelling Windows uses for per-drive working directories.
		"NOEQUALS",
		"=C:=C:\\work",
		"WITH=EQUALS=SIGNS",
		"PATH=C:\\rewritten",
		"PathExt=.BAT",
	}

	processRuntimePlatform = processPlatformWindows
	require.Equal(t, map[string]string{
		"KEPT":    "yes",
		"WITH":    "EQUALS=SIGNS",
		"PATH":    "C:\\rewritten",
		"PathExt": ".BAT",
	}, AmbientEnvironmentSnapshot(block))

	// The folded snapshot is a phase the merge accepts. An unfolded one carries
	// both spellings into a single phase, which has no order to read and is
	// refused — so before the fold an inherited block of this shape failed
	// every session start rather than one launch.
	environment, err := ordinaryEnvironment(AmbientEnvironmentSnapshot(block))
	require.NoError(t, err)
	require.Equal(t, []string{"KEPT=yes", "PATH=C:\\rewritten", "PathExt=.BAT", "WITH=EQUALS=SIGNS"}, environment)

	_, err = ordinaryEnvironment(map[string]string{"Path": "C:\\inherited", "PATH": "C:\\rewritten"})
	require.ErrorContains(t, err, `process environment names PATH twice, as "PATH" and "Path"`)

	// An exact repeat is one variable on every platform, so the block's own
	// order decides that case without any folding.
	require.Equal(t, map[string]string{"PATH": "second"},
		AmbientEnvironmentSnapshot([]string{"PATH=first", "PATH=second"}))

	// Off Windows the two spellings are genuinely two variables. Nothing is
	// folded away, and the adapter-managed keys are still the merge's business
	// rather than the snapshot's.
	processRuntimePlatform = processPlatformLinux
	require.Equal(t, map[string]string{
		"Path":    "C:\\inherited",
		"PATHEXT": ".COM",
		"KEPT":    "yes",
		"WITH":    "EQUALS=SIGNS",
		"PATH":    "C:\\rewritten",
		"PathExt": ".BAT",
	}, AmbientEnvironmentSnapshot(block))
}
