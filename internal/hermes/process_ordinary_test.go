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

func TestProcessPATHOrdersOperationBeforeBrowserShimAndNativePATH(t *testing.T) {
	separator := string(os.PathListSeparator)
	operationOne := filepath.Join(t.TempDir(), "operation-one")
	operationTwo := filepath.Join(t.TempDir(), "operation-two")
	shimDir := filepath.Join(t.TempDir(), "browser-shim")
	nativeOne := filepath.Join(t.TempDir(), "native-one")
	nativeTwo := filepath.Join(t.TempDir(), "native-two")

	base := []string{
		"STATIC=1",
		"PATH=" + separator + nativeOne + separator + separator + nativeTwo + separator,
	}
	withShim := browserShimEnviron(base, shimDir)
	actual := prependPathDirs(withShim, []string{operationOne, operationTwo, operationOne})
	require.Equal(t,
		strings.Join([]string{operationOne, operationTwo, operationOne, shimDir, nativeOne, nativeTwo}, separator),
		envValue(actual, "PATH"),
	)
	require.NotContains(t, strings.Split(envValue(actual, "PATH"), separator), "")

	absent := prependPathDirs([]string{"STATIC=1"}, []string{operationOne, operationTwo})
	require.Equal(t, strings.Join([]string{operationOne, operationTwo}, separator), envValue(absent, "PATH"))

	noPath := prependPathDirs([]string{"STATIC=1", "PATH=" + separator + separator}, nil)
	require.Empty(t, envValue(noPath, "PATH"))
	require.Equal(t, []string{"STATIC=1"}, noPath)
}

func TestProcessCarrierValidation(t *testing.T) {
	absolute := t.TempDir()
	separator := string(os.PathListSeparator)

	for name, dirs := range map[string][]string{
		"empty":     {absolute, ""},
		"relative":  {"relative"},
		"separator": {absolute + separator + t.TempDir()},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := cloneAndValidateExtraPathDirs(dirs)
			require.Error(t, err)
		})
	}

	input := []string{absolute, absolute}
	cloned, err := cloneAndValidateExtraPathDirs(input)
	require.NoError(t, err)
	input[0] = t.TempDir()
	require.Equal(t, []string{absolute, absolute}, cloned)

	require.Error(t, validateSessionEnvironmentNoPath(map[string]string{"PATH": "/bad"}))
	require.Error(t, validatePathCarrierEnvironment(map[string]string{"BASH_ENV": "/bad"}))
	require.NoError(t, validateSessionEnvironmentNoPath(map[string]string{"TOKEN": "good"}))
	require.True(t, processEnvironmentKeyMatchesForPlatform("Path", "PATH", "windows"))
	require.False(t, processEnvironmentKeyMatchesForPlatform("Path", "PATH", "linux"))

	_, err = validatedProcessCarrier(ProcessOptions{ExtraPathDirs: []string{"relative"}})
	require.Error(t, err)
	_, err = validatedProcessCarrier(ProcessOptions{SessionEnv: map[string]string{"PATH": "/bad"}})
	require.Error(t, err)
	_, err = validatedProcessCarrier(ProcessOptions{Env: map[string]string{"BASH_ENV": "/bad"}})
	require.Error(t, err)
	_, err = validatedProcessCarrier(ProcessOptions{Isolation: &ProcessIsolation{BaseEnvironment: map[string]string{
		hermesPathInitCountEnv: "1",
	}}})
	require.Error(t, err)
	carrier, err := validatedProcessCarrier(ProcessOptions{ExtraPathDirs: []string{absolute}})
	require.NoError(t, err)
	require.Equal(t, []string{absolute}, carrier)

	_, err = Start(t.Context(), ProcessOptions{ExtraPathDirs: []string{"relative"}})
	require.Error(t, err)
	invalidSessionEnvironment := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
		SessionEnv:     map[string]string{"BAD=KEY": "value"},
	})
	_, err = Start(t.Context(), invalidSessionEnvironment)
	require.ErrorContains(t, err, "invalid key")
}

// TestProcessEnvironmentPhasesFoldWindowsNames is the portable half of the
// conflicting-case evidence, run with the platform seam pinned so every host
// exercises it. Windows names environment variables case-insensitively, so an
// inherited "Path" and a "PATH" written by a later phase are one variable. The
// rule has to be decided here rather than left to the child, because this
// adapter resolves the harness executable out of the same block it is about to
// hand over: a first-match read against a block the child deduplicates to the
// last value would search a PATH the harness never sees.
func TestProcessEnvironmentPhasesFoldWindowsNames(t *testing.T) {
	originalPlatform := processRuntimePlatform
	processRuntimePlatform = processPlatformWindows
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	decoyDir := t.TempDir()
	harnessDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(decoyDir, "hermes.exe"), []byte("MZ"), 0o600))
	harness := filepath.Join(harnessDir, "hermes.bat")
	require.NoError(t, os.WriteFile(harness, []byte("@echo fixture\n"), 0o600))

	// The ambient block spells both names the way an inherited Windows block
	// does; the Agent-scoped overlay is a later phase and spells them
	// differently. Only one spelling of each may survive, carrying the later
	// phase's value.
	environment, err := ordinaryEnvironment(
		map[string]string{"Path": decoyDir, "PATHEXT": ".COM;.EXE", "KEPT": "yes"},
		map[string]string{"PATH": harnessDir, "PathExt": ".BAT"},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"KEPT=yes", "PATH=" + harnessDir, "PathExt=.BAT"}, environment)

	// Resolution reads the surviving values, so the decoy image the ambient
	// phase pointed at is unreachable under both the search path and the
	// extension list the last phase installed. The Windows rules are driven
	// directly because the selector that reaches them, ordinaryExecutableRules,
	// is build-tagged even though windowsExecutableRules itself compiles
	// everywhere; the native lane proves the same block against a real Windows
	// child.
	resolved, err := lookOrdinaryPathWithRules("hermes", environment, windowsExecutableRules(environment))
	require.NoError(t, err)
	require.Equal(t, harness, resolved)

	// Two spellings inside one phase have no order at all, so they are refused
	// rather than settled by map iteration.
	_, err = ordinaryEnvironment(nil, map[string]string{"Path": decoyDir, "PATH": harnessDir})
	require.ErrorContains(t, err, `process environment names PATH twice, as "PATH" and "Path"`)

	// A folded read reports the value the child keeps, which is the last one.
	block := []string{"Path=" + decoyDir, "PATH=" + harnessDir}
	require.Equal(t, harnessDir, envValueFold(block, "PATH", true))

	// The carrier rewrite collapses every spelling into the single entry it
	// emits, so no second owner of the search path reaches the child.
	separator := string(os.PathListSeparator)
	operationDir := t.TempDir()
	rewritten := prependPathDirs(append([]string{"KEPT=yes"}, block...), []string{operationDir})
	require.Equal(t, []string{"KEPT=yes", "PATH=" + operationDir + separator + harnessDir}, rewritten)

	// A Hermes variable an operator wrote in another case is the same variable
	// to Hermes itself, so the serve arguments derived from one are too.
	require.Contains(t,
		processServeArgs(ProcessOptions{SessionEnv: map[string]string{"Hermes_Web_Dist": "/opt/web"}}, 1),
		"--skip-build",
	)
	require.Empty(t, processEnvironmentValue(map[string]string{"OTHER": "x"}, envHermesWebDist))

	// Off Windows the two spellings are genuinely different variables and both
	// survive untouched, and only the exact name is read.
	processRuntimePlatform = processPlatformLinux
	unfolded, err := ordinaryEnvironment(map[string]string{"Path": decoyDir}, map[string]string{"PATH": harnessDir})
	require.NoError(t, err)
	require.Equal(t, []string{"PATH=" + harnessDir, "Path=" + decoyDir}, unfolded)
	require.Empty(t, processEnvironmentValue(map[string]string{"Hermes_Web_Dist": "/opt/web"}, envHermesWebDist))
	require.Equal(t, "/opt/web", processEnvironmentValue(map[string]string{envHermesWebDist: "/opt/web"}, envHermesWebDist))
}

func TestExecutableResolutionIgnoresSessionExtraPathDirs(t *testing.T) {
	staticDir := t.TempDir()
	operationDir := t.TempDir()
	want := writeTestHarness(t, staticDir)
	_ = writeTestHarness(t, operationDir)

	options := ProcessOptions{
		ExecutablePath:     "hermes",
		AmbientEnvironment: map[string]string{"PATH": staticDir},
		SessionEnv:         map[string]string{"WAGIE_API_TOKEN": "session"},
		ExtraPathDirs:      []string{operationDir},
	}
	base, err := processLaunchEnvironment(options)
	require.NoError(t, err)
	resolved, err := resolveHarnessExecutable(nil, options.ExecutablePath, base)
	require.NoError(t, err)
	require.Equal(t, want, resolved)
	require.NotContains(t, base, "WAGIE_API_TOKEN=session")
	require.Equal(t, staticDir, envValue(base, "PATH"))
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
