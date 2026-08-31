//go:build windows

package hermes

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWindowsChildResolvesTheConflictingCaseEnvironmentThisAdapterBuilt is the
// native half of the conflicting-case evidence. Nothing here is simulated: the
// adapter builds a launch environment whose ambient phase spells the search
// path and extension list one way and whose later phase spells them another,
// resolves the harness out of that block, and then hands the same block to a
// real cmd.exe. Windows' own resolver has to reach exactly the file this
// adapter resolved — that agreement, not a string comparison against a mirror,
// is what says the search order the wrapper reasoned about is the one the
// harness process actually gets.
func TestWindowsChildResolvesTheConflictingCaseEnvironmentThisAdapterBuilt(t *testing.T) {
	shell, err := resolveHarnessExecutable("cmd", os.Environ())
	require.NoError(t, err, "ordinary resolution must find cmd on the ambient Path")

	decoyDir := t.TempDir()
	harnessDir := t.TempDir()
	operationDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(decoyDir, "hermes.exe"), []byte("MZ"), 0o600))
	harness := writeWindowsCaseFixture(t, harnessDir, "hermes.bat")
	carrier := writeWindowsCaseFixture(t, operationDir, "carrier.bat")

	// cmd.exe needs the system roots to start at all, so the ambient phase is a
	// real inherited block with the conflicting spellings written into it.
	ambient := map[string]string{
		"Path":       decoyDir,
		"PATHEXT":    ".COM;.EXE",
		"SystemRoot": os.Getenv("SystemRoot"),
		"windir":     os.Getenv("windir"),
	}
	environment, err := ordinaryEnvironment(ambient, map[string]string{"PATH": harnessDir, "PathExt": ".BAT"})
	require.NoError(t, err)

	resolved, err := resolveHarnessExecutable("hermes", environment)
	require.NoError(t, err)
	require.Equal(t, harness, resolved)

	for name, native := range map[string]struct {
		env  []string
		call string
		want string
	}{
		// The harness leg proves the folded base: had either spelling been read
		// first-match, cmd.exe would have reached decoyDir\hermes.exe instead.
		"harness resolves from the folded base": {
			env:  environment,
			call: "hermes",
			want: harness,
		},
		// The carrier leg proves the session-owned directories still lead the
		// single rewritten entry, rather than being spliced behind a surviving
		// second spelling of the base search path.
		"operation directory leads the rewritten path": {
			env:  prependPathDirs(environment, []string{operationDir}),
			call: "carrier",
			want: carrier,
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, 1, windowsSearchPathOwners(native.env),
				"the child must receive exactly one search path")

			reported := windowsFixtureResolvedByChild(t, shell, native.env, native.call)
			require.True(t, strings.EqualFold(native.want, reported),
				"cmd.exe ran %q, this adapter resolved %q", reported, native.want)
		})
	}
}

// TestWindowsInheritedBlockWithTwoPathSpellingsResolvesLastWins is the native
// half of the inherited-block evidence. A parent that hand-builds a block for
// CreateProcess — anything that is not itself a Go program — can write both
// spellings of the search path into the block this adapter inherits. That block
// is ordered, so the ambient phase resolves it the way Windows itself does
// instead of carrying two live variables into a phase that has no order to read
// and would refuse the launch outright.
//
// Nothing here is simulated: the adapter folds a real inherited block, resolves
// the harness out of the phase it becomes, and a real cmd.exe handed the same
// block has to reach exactly that file.
func TestWindowsInheritedBlockWithTwoPathSpellingsResolvesLastWins(t *testing.T) {
	shell, err := resolveHarnessExecutable("cmd", os.Environ())
	require.NoError(t, err, "ordinary resolution must find cmd on the ambient Path")

	decoyDir := t.TempDir()
	harnessDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(decoyDir, "hermes.exe"), []byte("MZ"), 0o600))
	harness := writeWindowsCaseFixture(t, harnessDir, "hermes.bat")

	// The block in the order a parent wrote it, system roots included so
	// cmd.exe can start at all. The second spelling of each name is the
	// current one.
	ambient := AmbientEnvironmentSnapshot([]string{
		"SystemRoot=" + os.Getenv("SystemRoot"),
		"windir=" + os.Getenv("windir"),
		"Path=" + decoyDir,
		"PATHEXT=.COM;.EXE",
		"PATH=" + harnessDir,
		"PathExt=.BAT",
	})

	environment, err := ordinaryEnvironment(ambient)
	require.NoError(t, err, "a folded inherited block must not refuse the launch it is the base of")
	require.Equal(t, 1, windowsSearchPathOwners(environment),
		"the child must receive exactly one search path")

	resolved, err := resolveHarnessExecutable("hermes", environment)
	require.NoError(t, err)
	require.Equal(t, harness, resolved,
		"the decoy the superseded spelling pointed at must be unreachable")

	reported := windowsFixtureResolvedByChild(t, shell, environment, "hermes")
	require.True(t, strings.EqualFold(harness, reported),
		"cmd.exe ran %q, this adapter resolved %q", reported, harness)
}

// windowsFixtureResolvedByChild hands a block to a real cmd.exe and reports the
// fully qualified path Windows' own resolver reached for the bare name. That
// answer, rather than a comparison against a mirror of the rules, is what makes
// the child's search order comparable to this adapter's.
func windowsFixtureResolvedByChild(t *testing.T, shell string, env []string, call string) string {
	t.Helper()

	output := filepath.Join(t.TempDir(), "resolved.txt")
	file, err := os.Create(output)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	// /d skips autorun scripts and an empty working directory keeps the
	// implicit current-directory search from answering for PATH.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, shell, "/d", "/c", call)
	command.Env = env
	command.Dir = t.TempDir()
	command.Stdout = file
	require.NoError(t, command.Run())
	require.NoError(t, context.Cause(ctx))
	require.NoError(t, file.Close())

	contents, err := os.ReadFile(output)
	require.NoError(t, err)

	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(contents)), "hermes-case-fixture"))
}

// writeWindowsCaseFixture plants a batch fixture that reports the fully
// qualified path Windows resolved it to, which is what makes the child's own
// search result comparable to this adapter's.
func writeWindowsCaseFixture(t *testing.T, dir string, name string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("@echo hermes-case-fixture %~f0\r\n"), 0o600))

	return path
}

// windowsSearchPathOwners counts the spellings of the search path a block
// carries. More than one would leave the effective order to the child's own
// deduplication, which is the whole failure this lane exists to exclude.
func windowsSearchPathOwners(env []string) int {
	owners := 0

	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(key, "PATH") {
			owners++
		}
	}

	return owners
}
