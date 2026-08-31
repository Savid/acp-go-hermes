package hermes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrdinaryEnvironmentScrubsManagedState(t *testing.T) {
	environment, err := ordinaryEnvironment(map[string]string{
		"PATH":                                 "/usr/bin",
		"HOME":                                 "/home/operator",
		"HERMES_WEB_DIST":                      "/opt/hermes/web",
		envHermesHome:                          "/foreign/home",
		envHermesSessionToken:                  "foreign-token",
		strings.ToLower(envHermesHome):         "/foreign/lower-home",
		strings.ToLower(envHermesSessionToken): "foreign-lower-token",
		"":                                     "empty key",
		"BAD=KEY":                              "invalid",
		"NUL\x00KEY":                           "invalid",
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"HERMES_WEB_DIST=/opt/hermes/web",
		"HOME=/home/operator",
		"PATH=/usr/bin",
	}, environment)

	environment, err = ordinaryEnvironment(
		map[string]string{"PATH": "/usr/bin"},
		map[string]string{
			"MODEL":       "sonnet",
			"PATH":        "/opt/bin",
			envHermesHome: "/overlay/home",
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"MODEL=sonnet", "PATH=/opt/bin"}, environment)

	_, err = ordinaryEnvironment(nil, map[string]string{"BAD=KEY": "value"})
	require.ErrorContains(t, err, "invalid key")
}

func TestProcessPATHOrdersOperationBeforeBrowserShimAndBase(t *testing.T) {
	separator := string(os.PathListSeparator)
	operationOne := filepath.Join(t.TempDir(), "operation-one")
	operationTwo := filepath.Join(t.TempDir(), "operation-two")
	shimDir := filepath.Join(t.TempDir(), "browser-shim")
	baseOne := filepath.Join(t.TempDir(), "base-one")
	baseTwo := filepath.Join(t.TempDir(), "base-two")

	base := []string{
		"STATIC=1",
		"PATH=" + separator + baseOne + separator + separator + baseTwo + separator,
	}
	withShim := browserShimEnviron(base, shimDir)
	actual := prependPathDirs(withShim, []string{operationOne, operationTwo, operationOne})
	require.Equal(t,
		strings.Join([]string{operationOne, operationTwo, operationOne, shimDir, baseOne, baseTwo}, separator),
		envValueFold(actual, "PATH", false),
	)
	require.NotContains(t, strings.Split(envValueFold(actual, "PATH", false), separator), "")
	require.Equal(t, browserShimCommand(shimDir), envValueFold(actual, browserShimBrowserEnv, false))

	absent := prependPathDirs([]string{"STATIC=1"}, []string{operationOne, operationTwo})
	require.Equal(t, strings.Join([]string{operationOne, operationTwo}, separator), envValueFold(absent, "PATH", false))

	noPath := prependPathDirs([]string{"STATIC=1", "PATH=" + separator + separator}, nil)
	require.Empty(t, envValueFold(noPath, "PATH", false))
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

	for _, options := range []ProcessOptions{
		{ExtraPathDirs: []string{"relative"}},
		{SessionEnv: map[string]string{"PATH": "/session"}},
		{Env: map[string]string{hermesBashEnvKey: "/untrusted"}},
		{
			StartNative:       func(context.Context, NativeRequest) (NativeProcess, error) { return nil, errors.New("unused") },
			NativeEnvironment: map[string]string{hermesBashEnvKey: "/untrusted"},
		},
	} {
		_, carrierErr := validatedProcessCarrier(options)
		require.Error(t, carrierErr)
	}

	carrier, err := validatedProcessCarrier(ProcessOptions{ExtraPathDirs: []string{absolute}})
	require.NoError(t, err)
	require.Equal(t, []string{absolute}, carrier)

	_, err = processSessionLaunchEnvironment(ProcessOptions{SessionEnv: map[string]string{"BAD=KEY": "value"}})
	require.ErrorContains(t, err, "invalid key")
}

func TestProcessEnvironmentPhasesFoldWindowsNames(t *testing.T) {
	originalPlatform := processRuntimePlatform
	processRuntimePlatform = processPlatformWindows
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	decoyDir := t.TempDir()
	harnessDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(decoyDir, "hermes.exe"), []byte("MZ"), 0o600))
	harness := filepath.Join(harnessDir, "hermes.bat")
	require.NoError(t, os.WriteFile(harness, []byte("@echo fixture\n"), 0o600))

	environment, err := ordinaryEnvironment(
		map[string]string{"Path": decoyDir, "PATHEXT": ".COM;.EXE", "KEPT": "yes"},
		map[string]string{"PATH": harnessDir, "PathExt": ".BAT"},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"KEPT=yes", "PATH=" + harnessDir, "PathExt=.BAT"}, environment)

	resolved, err := lookOrdinaryPathWithRules("hermes", environment, windowsExecutableRules(environment))
	require.NoError(t, err)
	require.Equal(t, harness, resolved)

	_, err = ordinaryEnvironment(nil, map[string]string{"Path": decoyDir, "PATH": harnessDir})
	require.ErrorContains(t, err, `process environment names PATH twice, as "PATH" and "Path"`)

	block := []string{"Path=" + decoyDir, "PATH=" + harnessDir}
	require.Equal(t, harnessDir, envValueFold(block, "PATH", true))

	operationDir := t.TempDir()
	rewritten := prependPathDirs(append([]string{"KEPT=yes"}, block...), []string{operationDir})
	require.Equal(t, []string{"KEPT=yes", "PATH=" + operationDir + string(os.PathListSeparator) + harnessDir}, rewritten)

	require.Contains(t,
		processServeArgs(ProcessOptions{SessionEnv: map[string]string{"Hermes_Web_Dist": "/opt/web"}}, 1),
		"--skip-build",
	)
	require.Empty(t, processEnvironmentValue(map[string]string{"OTHER": "x"}, envHermesWebDist))

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
	resolved, err := resolveHarnessExecutable(options.ExecutablePath, base)
	require.NoError(t, err)
	require.Equal(t, want, resolved)
	require.NotContains(t, base, "WAGIE_API_TOKEN=session")
	require.Equal(t, staticDir, envValueFold(base, "PATH", processRuntimePlatform == processPlatformWindows))
}

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

func TestOrdinaryExecutableResolutionAcceptsShellEnvironment(t *testing.T) {
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o700))
	harness := writeTestHarness(t, binDir)
	t.Chdir(root)

	separator := string(os.PathListSeparator)
	for name, search := range map[string]string{
		"relative entry": "bin" + separator + binDir,
		"absolute entry": binDir,
		"empty entry":    separator + "bin",
	} {
		t.Run(name, func(t *testing.T) {
			resolved, err := lookOrdinaryPathInEnvironment("hermes", []string{"PATH=" + search})
			require.NoError(t, err)
			require.True(t, filepath.IsAbs(resolved))
			require.Equal(t, filepath.Base(harness), filepath.Base(resolved))
		})
	}

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

func TestScrubOrdinaryEnvironmentKeyIsCaseInsensitive(t *testing.T) {
	for _, key := range []string{
		envHermesHome,
		strings.ToLower(envHermesHome),
		envHermesSessionToken,
		strings.ToLower(envHermesSessionToken),
	} {
		require.True(t, scrubOrdinaryEnvironmentKey(key), "key %q must be scrubbed", key)
	}

	for _, key := range []string{"PATH", "HOME", envHermesWebDist, "ACP_GO_HERMES_MODEL"} {
		require.False(t, scrubOrdinaryEnvironmentKey(key), "key %q must be inherited", key)
	}
}

func TestManagedEnvironmentStartsFromAuthorityBase(t *testing.T) {
	environment, err := managedEnvironment(
		map[string]string{"PATH": "/native/bin", "BASE": "base"},
		map[string]string{"BASE": "agent", "AGENT": "yes"},
		map[string]string{"SESSION": "yes"},
	)
	require.NoError(t, err)
	require.Equal(t, "/native/bin", envValueFold(environment, "PATH", false))
	require.Equal(t, "agent", envValueFold(environment, "BASE", false))
	require.Equal(t, "yes", envValueFold(environment, "AGENT", false))
	require.Equal(t, "yes", envValueFold(environment, "SESSION", false))
}

func TestManagedEnvironmentFoldsWindowsNamesAndScrubsAdapterState(t *testing.T) {
	originalPlatform := processRuntimePlatform
	processRuntimePlatform = processPlatformWindows
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	environment, err := managedEnvironment(
		map[string]string{
			"Path":                                 `C:\\native`,
			"BASE":                                 "base",
			strings.ToLower(envHermesHome):         `C:\\foreign-home`,
			strings.ToLower(envHermesSessionToken): "foreign-token",
		},
		map[string]string{"PATH": `C:\\agent`, "base": "agent"},
		map[string]string{"SESSION": "yes", envHermesHome: `C:\\session-home`},
	)
	require.NoError(t, err)
	require.Equal(t, `C:\\agent`, envValueFold(environment, "PATH", true))
	require.Equal(t, "agent", envValueFold(environment, "BASE", true))
	require.Equal(t, "yes", envValueFold(environment, "SESSION", true))
	require.Empty(t, envValueFold(environment, envHermesHome, true))
	require.Empty(t, envValueFold(environment, envHermesSessionToken, true))

	upserted := upsertProcessEnv([]string{
		"Path=old-path",
		"PATH=other-old-path",
		"KEPT=yes",
	}, "PATH", `C:\\final`)
	require.Equal(t, []string{"KEPT=yes", `PATH=C:\\final`}, upserted)
}

func TestProcessScalarHelpers(t *testing.T) {
	port, err := freePort()
	require.NoError(t, err)
	require.Positive(t, port)

	token, err := randomToken()
	require.NoError(t, err)
	require.NotEmpty(t, token)

	require.Positive(t, compareVersions("1.2.3", "1.2.2"))
	require.Zero(t, compareVersions("1.2.3", "1.2.3"))

	executableProbeMu.Lock()
	executableProbed[t.Name()] = true
	executableProbeMu.Unlock()
	t.Cleanup(func() {
		executableProbeMu.Lock()
		delete(executableProbed, t.Name())
		executableProbeMu.Unlock()
	})
	require.NoError(t, ensureExecutableVersion(t.Context(), t.Name(), ProcessOptions{}))
	require.NoError(t, methodPresent("domain.method", &RPCError{Code: 4001, Message: "domain"}))
	require.Error(t, methodPresent("missing.method", &RPCError{Code: -32601, Message: "missing"}))
	require.Error(t, methodPresent("broken.method", errors.New("broken")))
}
