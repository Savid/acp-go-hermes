package hermes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// testAmbientEnvironment is the adapter environment an ordinary launch
// inherits. Tests that exercise process lifecycle rather than the isolation
// policy use it with a nil Isolation, which is what the ordinary default
// actually selects.
func testAmbientEnvironment() map[string]string {
	return map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")}
}

func TestOrdinaryEnvironmentScrubsPrivateAndManagedState(t *testing.T) {
	ambient := map[string]string{
		"PATH":                           "/usr/bin",
		"HOME":                           "/home/operator",
		"HERMES_WEB_DIST":                "/opt/hermes/web",
		privateSupervisorEnvPrefix + "X": "spoofed",
		"acp_go_hermes_internal_lower":   "spoofed",
		processSupervisorEnvPrefix:       "guardian",
		envRuntimeID:                     "forged-runtime",
		envScratchRoot:                   "/forged/scratch",
		"HERMES_HOME":                    "/operator/real/home",
		"HERMES_AUTH_HOME":               "/operator/real/credentials",
		"HERMES_DASHBOARD_SESSION_TOKEN": "forged-token",
		"hermes_auth_home":               "/operator/real/credentials",
		"":                               "empty key",
		"BAD=KEY":                        "invalid",
		"NUL\x00KEY":                     "invalid",
	}

	environment, err := ordinaryEnvironment(ambient)
	require.NoError(t, err)

	require.Equal(t, []string{
		"HERMES_WEB_DIST=/opt/hermes/web",
		"HOME=/home/operator",
		"PATH=/usr/bin",
	}, environment)
}

func TestOrdinaryEnvironmentOverlayCannotReintroduceScrubbedState(t *testing.T) {
	environment, err := ordinaryEnvironment(
		map[string]string{"PATH": "/usr/bin"},
		map[string]string{
			"MODEL":                          "sonnet",
			"PATH":                           "/opt/bin",
			"HERMES_AUTH_HOME":               "/operator/real/credentials",
			privateSupervisorEnvPrefix + "X": "spoofed",
		},
	)
	require.NoError(t, err)

	// The overlay wins for ordinary keys and is scrubbed on the same terms as
	// the ambient base for adapter-private and adapter-managed ones.
	require.Equal(t, []string{"MODEL=sonnet", "PATH=/opt/bin"}, environment)

	_, err = ordinaryEnvironment(nil, map[string]string{"BAD=KEY": "value"})
	require.ErrorContains(t, err, "invalid key")
}

// writeTestHarness writes a harness image under dir under the name this
// platform's ordinary resolution of a bare "hermes" will actually find, and
// returns it. Windows resolves a bare name through PATHEXT, so the fixture has
// to carry an extension those rules list.
func writeTestHarness(t *testing.T, dir string) string {
	t.Helper()

	name := "hermes"
	if extensions := ordinaryExecutableRules(nil).extensions; len(extensions) > 0 {
		name += extensions[0]
	}

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"), 0o700))

	return path
}

// TestOrdinaryExecutableResolutionAcceptsAnOrdinaryShellEnvironment pins the
// split from strict policy resolution: a relative PATH entry and a relative
// configured executable are ordinary, and refusing them would turn policy
// omission into an app-start blocker.
func TestOrdinaryExecutableResolutionAcceptsAnOrdinaryShellEnvironment(t *testing.T) {
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o700))

	harness := writeTestHarness(t, binDir)

	// t.Chdir rather than a manual save-and-restore: it restores the directory
	// itself and panics if this test or any parent is parallel, which is the
	// guard a process-wide mutation in a package full of parallel tests needs.
	t.Chdir(root)

	separator := string(os.PathListSeparator)

	for name, search := range map[string]string{
		"relative entry": "bin" + separator + binDir,
		"absolute entry": binDir,
		"empty entry":    separator + "bin",
	} {
		t.Run(name, func(t *testing.T) {
			resolved, lookErr := lookOrdinaryPathInEnvironment("hermes", []string{"PATH=" + search})
			require.NoError(t, lookErr)
			require.True(t, filepath.IsAbs(resolved), "resolved path %q must be absolute", resolved)
			require.Equal(t, filepath.Base(harness), filepath.Base(resolved))
		})
	}

	// A relative configured executable resolves against the working directory
	// and is returned absolute, because exec.Cmd would otherwise evaluate it
	// against the session cwd instead.
	resolved, err := lookOrdinaryPathInEnvironment(filepath.Join("bin", "hermes"), nil)
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(resolved))

	_, err = lookOrdinaryPathInEnvironment("", nil)
	require.ErrorContains(t, err, "empty")

	_, err = lookOrdinaryPathInEnvironment("hermes", []string{"PATH=" + filepath.Join(root, "nonexistent")})
	require.ErrorContains(t, err, "not found in PATH")

	_, err = lookOrdinaryPathInEnvironment(filepath.Join("bin", "missing"), nil)
	require.Error(t, err)
}

// TestResolveHarnessExecutableSplitsByMode proves the resolver picks the strict
// resolver for a supplied policy and the ordinary one for an omitted policy,
// which is what keeps an ordinary relative PATH from being judged by
// closed-policy rules and vice versa.
func TestResolveHarnessExecutableSplitsByMode(t *testing.T) {
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "hermes"), []byte("#!/bin/sh\n"), 0o700))

	policy := &ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{}}

	resolved, err := resolveHarnessExecutable(policy, "hermes", []string{"PATH=" + binDir})
	require.NoError(t, err)
	require.Equal(t, filepath.Join(binDir, "hermes"), resolved)

	// The strict resolver refuses a relative PATH entry even though the same
	// entry is ordinary for an omitted policy.
	_, err = resolveHarnessExecutable(policy, "hermes", []string{"PATH=bin"})
	require.ErrorContains(t, err, "resolve Hermes executable")
	require.ErrorContains(t, err, "not absolute")

	t.Chdir(root)

	resolved, err = resolveHarnessExecutable(nil, "hermes", []string{"PATH=bin"})
	require.NoError(t, err)
	require.Equal(t, "hermes", filepath.Base(resolved))

	_, err = resolveHarnessExecutable(nil, "hermes", []string{"PATH=/nonexistent"})
	require.ErrorContains(t, err, "resolve Hermes executable")
}

// TestStrictPolicyResolutionKeepsItsAbsolutePathRule proves the ordinary
// relaxation above did not leak into the closed-policy resolver, whose author
// writes the whole environment and is held to absolute entries.
func TestStrictPolicyResolutionKeepsItsAbsolutePathRule(t *testing.T) {
	_, err := lookPathInEnvironment("hermes", []string{"PATH=bin:/usr/bin"})
	require.ErrorContains(t, err, "not absolute")

	_, err = lookPathInEnvironment("hermes", nil)
	require.ErrorContains(t, err, "without policy PATH")

	_, err = lookPathInEnvironment(filepath.Join("bin", "hermes"), nil)
	require.ErrorContains(t, err, "not absolute")

	_, err = lookPathInEnvironment("", nil)
	require.ErrorContains(t, err, "empty")
}

func TestScrubOrdinaryEnvironmentKeyIsCaseInsensitive(t *testing.T) {
	for _, key := range []string{
		privateSupervisorEnvPrefix + "MODE",
		strings.ToLower(privateSupervisorEnvPrefix) + "mode",
		processSupervisorEnvPrefix,
		strings.ToLower(envRuntimeID),
		"Hermes_Home",
	} {
		require.True(t, scrubOrdinaryEnvironmentKey(key), "key %q must be scrubbed", key)
	}

	for _, key := range []string{"PATH", "HOME", "HERMES_WEB_DIST", "ACP_GO_HERMES_MODEL"} {
		require.False(t, scrubOrdinaryEnvironmentKey(key), "key %q must be inherited", key)
	}
}
