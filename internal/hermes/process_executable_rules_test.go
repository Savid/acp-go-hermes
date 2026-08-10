package hermes

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The tests here execute the Windows resolution rules on whatever host runs
// them. Windows is the one supported platform whose ordinary launch no unix CI
// machine ever performs, and a rule set that is only compiled is not evidence
// that it resolves anything — so the rules are data, and these cases drive the
// real resolver with them.

func TestExecutableExtensionListFollowsPathext(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		pathext string
		want    []string
	}{
		"absent falls back to the Windows default": {
			pathext: "",
			want:    []string{".com", ".exe", ".bat", ".cmd"},
		},
		"case and spacing are normalized": {
			pathext: " .EXE ; .Cmd ",
			want:    []string{".exe", ".cmd"},
		},
		"a missing leading dot is supplied": {
			pathext: "EXE;BAT",
			want:    []string{".exe", ".bat"},
		},
		"present but naming nothing yields nothing": {
			pathext: ";; ;",
			want:    []string{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, executableExtensionList(test.pathext))
		})
	}
}

// TestWindowsOrdinaryRulesResolveAHarnessWithoutAnExecuteBit is the defect this
// rule set exists for. Go's os.Stat never sets an execute bit on a Windows
// regular file, so a mode-gated resolver rejects every real hermes.exe; the
// unix assertion below proves the fixture would indeed be refused under the
// other platform's rule.
func TestWindowsOrdinaryRulesResolveAHarnessWithoutAnExecuteBit(t *testing.T) {
	t.Parallel()

	binDir := t.TempDir()
	harness := filepath.Join(binDir, "hermes.exe")
	require.NoError(t, os.WriteFile(harness, []byte("MZ"), 0o600))

	// "Path" rather than "PATH", which is how a Windows environment block
	// spells it. PATHEXT is deliberately omitted to exercise the Windows
	// default extension list.
	environment := []string{"Path=" + binDir}
	rules := windowsExecutableRules(environment)

	resolved, err := lookOrdinaryPathWithRules("hermes", environment, rules)
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(resolved), "resolved path %q must be absolute", resolved)
	require.Equal(t, "hermes.exe", filepath.Base(resolved))

	// The same fixture under the other platform's rules: an exact-match PATH
	// lookup finds nothing, which is what made this a hard block rather than a
	// cosmetic difference.
	_, err = lookOrdinaryPathWithRules("hermes", environment, unixExecutableRules())
	require.ErrorContains(t, err, "not found in PATH")
}

func TestWindowsOrdinaryRulesHonourAConfiguredPathext(t *testing.T) {
	t.Parallel()

	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "hermes.exe"), []byte("MZ"), 0o600))

	environment := []string{"Path=" + binDir, "PathExt=.CMD"}
	rules := windowsExecutableRules(environment)

	// PATHEXT names only .cmd, so the .exe beside it is not a candidate.
	_, err := lookOrdinaryPathWithRules("hermes", environment, rules)
	require.ErrorContains(t, err, "not found in PATH")

	require.NoError(t, os.WriteFile(filepath.Join(binDir, "hermes.cmd"), []byte("@echo off"), 0o600))

	resolved, err := lookOrdinaryPathWithRules("hermes", environment, rules)
	require.NoError(t, err)
	require.Equal(t, "hermes.cmd", filepath.Base(resolved))
}

// TestWindowsOrdinaryRulesResolveQualifiedPaths covers the configured-path arm:
// WithExecutablePath is the other way a Windows host names the harness, and a
// drive letter or a forward slash has to count as a path there.
func TestWindowsOrdinaryRulesResolveQualifiedPaths(t *testing.T) {
	t.Parallel()

	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "hermes.exe"), []byte("MZ"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "probe.bat.exe"), []byte("MZ"), 0o600))

	rules := windowsExecutableRules(nil)

	// A qualified extensionless path picks up its extension.
	resolved, err := lookOrdinaryPathWithRules(binDir+"/hermes", nil, rules)
	require.NoError(t, err)
	require.Equal(t, "hermes.exe", filepath.Base(resolved))

	// A qualified path that already carries a listed extension is taken as it
	// stands.
	resolved, err = lookOrdinaryPathWithRules(binDir+"/hermes.exe", nil, rules)
	require.NoError(t, err)
	require.Equal(t, "hermes.exe", filepath.Base(resolved))

	// A name whose own extension is listed but which does not exist still falls
	// through to the appended candidates, so "probe.bat" reaches
	// "probe.bat.exe" the way Windows resolution does.
	resolved, err = lookOrdinaryPathWithRules(binDir+"/probe.bat", nil, rules)
	require.NoError(t, err)
	require.Equal(t, "probe.bat.exe", filepath.Base(resolved))

	_, err = lookOrdinaryPathWithRules(binDir+"/missing", nil, rules)
	require.ErrorContains(t, err, "no executable extension")

	// A directory is not a launch target under either rule set.
	_, err = lookOrdinaryPathWithRules(binDir, nil, unixExecutableRules())
	require.ErrorContains(t, err, "is not executable")
}

// TestWindowsOrdinaryRulesWithoutExtensionsResolveVerbatim covers a PATHEXT set
// to a nonempty list with no extensions: there is then nothing to append, and
// only the name as written can resolve.
func TestWindowsOrdinaryRulesWithoutExtensionsResolveVerbatim(t *testing.T) {
	t.Parallel()

	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "hermes.exe"), []byte("MZ"), 0o600))

	environment := []string{"Path=" + binDir, "PATHEXT=;"}
	rules := windowsExecutableRules(environment)
	require.Empty(t, rules.extensions)

	_, err := lookOrdinaryPathWithRules("hermes", environment, rules)
	require.ErrorContains(t, err, "not found in PATH")

	resolved, err := lookOrdinaryPathWithRules("hermes.exe", environment, rules)
	require.NoError(t, err)
	require.Equal(t, "hermes.exe", filepath.Base(resolved))
}

// TestEnvValueFoldReadsAnInheritedEnvironmentBlock separates the two lookups:
// a closed policy environment was written by its author and is matched exactly,
// while an inherited one is matched case-insensitively because the host chose
// the spelling.
func TestEnvValueFoldReadsAnInheritedEnvironmentBlock(t *testing.T) {
	t.Parallel()

	block := []string{"malformed", "Path=C:\\hermes", "PathExt=.EXE"}

	require.Empty(t, envValue(block, "PATH"))
	require.Equal(t, "C:\\hermes", envValueFold(block, "PATH", true))
	require.Equal(t, ".EXE", envValueFold(block, "PATHEXT", true))
	require.Empty(t, envValueFold(block, "PATH", false))
	require.Empty(t, envValueFold(block, "MISSING", true))
}

// TestOrdinaryExecutableRulesMatchThisPlatform pins the build-tagged selector to
// the rule set the platform's tests above describe, so the mirrored cases stay
// evidence about what this host actually runs rather than about a spare copy.
func TestOrdinaryExecutableRulesMatchThisPlatform(t *testing.T) {
	t.Parallel()

	environment := []string{"PathExt=.EXE"}
	selected := ordinaryExecutableRules(environment)

	if selected.requireExecuteBit {
		require.Equal(t, unixExecutableRules(), selected)

		return
	}

	require.Equal(t, windowsExecutableRules(environment), selected)
}
