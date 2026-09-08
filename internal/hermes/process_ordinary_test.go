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
			"MODEL":                    "sonnet",
			"PATH":                     "/opt/bin",
			envHermesHome:              "/overlay/home",
			envPrivatePrefix + "TOKEN": "private",
			"acp_go_hermes_internal_x": "private-lowercase",
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"MODEL=sonnet", "PATH=/opt/bin"}, environment)

	environment, err = managedEnvironment(map[string]string{
		"PATH":                     "/usr/bin",
		envPrivatePrefix + "TOKEN": "private",
		"acp_go_hermes_internal_x": "private-lowercase",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"PATH=/usr/bin"}, environment)

	_, err = ordinaryEnvironment(nil, map[string]string{"BAD=KEY": "value"})
	require.ErrorContains(t, err, "invalid key")
}

func TestProcessPATHOrdersOperationBeforeBrowserShimAndBase(t *testing.T) {
	separator := string(os.PathListSeparator)
	operationOne := filepath.Join(durableTempDir(t), "operation-one")
	operationTwo := filepath.Join(durableTempDir(t), "operation-two")
	shimDir := filepath.Join(durableTempDir(t), "browser-shim")
	baseOne := filepath.Join(durableTempDir(t), "base-one")
	baseTwo := filepath.Join(durableTempDir(t), "base-two")

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
	absolute := durableTempDir(t)
	separator := string(os.PathListSeparator)

	for name, dirs := range map[string][]string{
		"empty":     {absolute, ""},
		"relative":  {"relative"},
		"separator": {absolute + separator + durableTempDir(t)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := cloneAndValidateExtraPathDirs(dirs)
			require.Error(t, err)
		})
	}

	input := []string{absolute, absolute}
	cloned, err := cloneAndValidateExtraPathDirs(input)
	require.NoError(t, err)
	input[0] = durableTempDir(t)
	require.Equal(t, []string{absolute, absolute}, cloned)

	require.Error(t, validateSessionEnvironmentNoPath(map[string]string{"PATH": "/bad"}))
	require.Error(t, validatePathCarrierEnvironment(map[string]string{"BASH_ENV": "/bad"}))
	require.Error(t, validatePathCarrierEnvironment(map[string]string{"ENV": "/bad"}))
	require.NoError(t, validateSessionEnvironmentNoPath(map[string]string{"TOKEN": "good"}))
	require.True(t, processEnvironmentKeyMatchesForPlatform("Path", "PATH", "windows"))
	require.False(t, processEnvironmentKeyMatchesForPlatform("Path", "PATH", "linux"))

	for _, options := range []ProcessOptions{
		{ExtraPathDirs: []string{"relative"}},
		{SessionEnv: map[string]string{"PATH": "/session"}},
		{Env: map[string]string{hermesBashEnvKey: "/untrusted"}},
		{Env: map[string]string{hermesShellEnvKey: "/untrusted"}},
		{
			StartNative:       func(context.Context, NativeRequest) (NativeProcess, error) { return nil, errors.New("unused") },
			NativeEnvironment: map[string]string{hermesBashEnvKey: "/untrusted"},
		},
		{
			StartNative:       func(context.Context, NativeRequest) (NativeProcess, error) { return nil, errors.New("unused") },
			NativeEnvironment: map[string]string{hermesShellEnvKey: "/untrusted"},
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
	originalPlatform := Platform
	Platform = processPlatformWindows
	t.Cleanup(func() { Platform = originalPlatform })

	decoyDir := durableTempDir(t)
	harnessDir := durableTempDir(t)
	require.NoError(t, os.WriteFile(filepath.Join(decoyDir, "hermes.exe"), []byte("MZ"), 0o600))
	harness := filepath.Join(harnessDir, "hermes.bat")
	require.NoError(t, os.WriteFile(harness, []byte("@echo fixture\n"), 0o600))

	environment, err := ordinaryEnvironment(
		map[string]string{"Path": decoyDir, "PATHEXT": ".COM;.EXE", "KEPT": "yes"},
		map[string]string{"PATH": harnessDir, "PathExt": ".BAT"},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"PATH=" + harnessDir, "PathExt=.BAT"}, environment, "KEPT is not an inherited name")

	resolved, err := lookOrdinaryPathWithRules("hermes", environment, windowsExecutableRules(environment))
	require.NoError(t, err)
	require.Equal(t, harness, resolved)

	_, err = ordinaryEnvironment(nil, map[string]string{"Path": decoyDir, "PATH": harnessDir})
	require.ErrorContains(t, err, `process environment names PATH twice, as "PATH" and "Path"`)

	block := []string{"Path=" + decoyDir, "PATH=" + harnessDir}
	require.Equal(t, harnessDir, envValueFold(block, "PATH", true))

	operationDir := durableTempDir(t)
	rewritten := prependPathDirs(append([]string{"KEPT=yes"}, block...), []string{operationDir})
	require.Equal(t, []string{"KEPT=yes", "PATH=" + operationDir + string(os.PathListSeparator) + harnessDir}, rewritten)

	require.Contains(t,
		processServeArgs(ProcessOptions{SessionEnv: map[string]string{"Hermes_Web_Dist": "/opt/web"}}, 1),
		"--skip-build",
	)
	require.Empty(t, processEnvironmentValue(map[string]string{"OTHER": "x"}, envHermesWebDist))

	Platform = processPlatformLinux
	unfolded, err := ordinaryEnvironment(map[string]string{"Path": decoyDir, "PATH": "/ambient/bin"}, map[string]string{"PATH": harnessDir})
	require.NoError(t, err)
	require.Equal(t, []string{"PATH=" + harnessDir}, unfolded, "off Windows an inherited Path is a different variable and not an inherited name")
	require.Empty(t, processEnvironmentValue(map[string]string{"Hermes_Web_Dist": "/opt/web"}, envHermesWebDist))
	require.Equal(t, "/opt/web", processEnvironmentValue(map[string]string{envHermesWebDist: "/opt/web"}, envHermesWebDist))
}

func TestExecutableResolutionIgnoresSessionExtraPathDirs(t *testing.T) {
	staticDir := durableTempDir(t)
	operationDir := durableTempDir(t)
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
	require.Equal(t, staticDir, envValueFold(base, "PATH", Platform == processPlatformWindows))
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
	root := durableTempDir(t)
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
	originalPlatform := Platform
	Platform = processPlatformWindows
	t.Cleanup(func() { Platform = originalPlatform })

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

func TestOrdinaryEnvironmentInheritsOnlyTheAllowlist(t *testing.T) {
	originalPlatform := Platform
	t.Cleanup(func() { Platform = originalPlatform })

	ambient := map[string]string{
		// Inherited: what a process needs to run, reach the network, and serve.
		"PATH": "/usr/bin", "HOME": "/home/operator", "TMPDIR": "/tmp/op", "LANG": "en_AU.UTF-8",
		"LC_ALL": "C.UTF-8", "TERM": "xterm-256color", "HTTPS_PROXY": "http://proxy:3128",
		"https_proxy": "http://proxy:3128", "NO_PROXY": "localhost", "SSL_CERT_FILE": "/etc/ssl/ca.pem",
		"REQUESTS_CA_BUNDLE": "/etc/ssl/ca.pem", envHermesWebDist: "/opt/hermes/web",
		// Not inherited: every credential Hermes would seed its pool from, and
		// everything else the operator happens to have exported.
		"OPENAI_API_KEY": "sk-openai", "ANTHROPIC_API_KEY": "sk-ant", "CLAUDE_CODE_OAUTH_TOKEN": "oauth",
		"OPENROUTER_API_KEY": "sk-or", "GITHUB_TOKEN": "ghp", "GH_TOKEN": "gho", "XAI_API_KEY": "xai",
		"SSH_AUTH_SOCK": "/run/agent.sock", "DBUS_SESSION_BUS_ADDRESS": "unix:path=/run/bus",
		"XDG_CONFIG_HOME": "/home/operator/.config", "EDITOR": "vim", "TZ": "Australia/Brisbane",
		// Off Windows a differently-cased spelling is a different variable, and
		// the allowlist names none of them.
		"path": "/lower/bin", "Home": "/mixed/home", "lc_all": "lower",
		envHermesHome: "/foreign/home",
	}

	Platform = processPlatformLinux
	environment, err := ordinaryEnvironment(ambient)
	require.NoError(t, err)
	require.Equal(t, []string{
		"HERMES_WEB_DIST=/opt/hermes/web",
		"HOME=/home/operator",
		"HTTPS_PROXY=http://proxy:3128",
		"LANG=en_AU.UTF-8",
		"LC_ALL=C.UTF-8",
		"NO_PROXY=localhost",
		"PATH=/usr/bin",
		"REQUESTS_CA_BUNDLE=/etc/ssl/ca.pem",
		"SSL_CERT_FILE=/etc/ssl/ca.pem",
		"TERM=xterm-256color",
		"TMPDIR=/tmp/op",
		"https_proxy=http://proxy:3128",
	}, environment)

	// A credential still reaches the harness when the caller hands it over
	// explicitly: the allowlist governs inheritance, not WithEnv.
	explicit, err := ordinaryEnvironment(ambient, map[string]string{"OPENAI_API_KEY": "sk-explicit"})
	require.NoError(t, err)
	require.Contains(t, explicit, "OPENAI_API_KEY=sk-explicit")
	require.NotContains(t, explicit, "ANTHROPIC_API_KEY=sk-ant")

	// Where the platform folds names, the allowlist folds with it: "Path" and
	// "lc_all" are the search path and a locale there, and the leftover
	// spellings are one variable each rather than two.
	Platform = processPlatformWindows
	folded, err := ordinaryEnvironment(map[string]string{
		"Path": `C:\bin`, "lc_all": "lower", "PathExt": ".EXE", "SystemRoot": `C:\Windows`,
		"OPENAI_API_KEY": "sk-openai", "Openai_Api_Key": "sk-mixed", "GITHUB_TOKEN": "ghp",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"Path=C:\\bin", "PathExt=.EXE", "SystemRoot=C:\\Windows", "lc_all=lower"}, folded)

	for _, name := range []string{"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "OPENROUTER_API_KEY", "GITHUB_TOKEN", "GH_TOKEN", "HERMES_HOME", "SSH_AUTH_SOCK", "TZ"} {
		require.False(t, inheritOrdinaryEnvironmentKey(name), name)
	}

	for _, name := range []string{"PATH", "HOME", "LC_MESSAGES", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "SSL_CERT_DIR", "CURL_CA_BUNDLE", "PATHEXT", "COMSPEC", "USERPROFILE", "__CF_USER_TEXT_ENCODING", envHermesWebDist} {
		require.True(t, inheritOrdinaryEnvironmentKey(name), name)
	}
}
