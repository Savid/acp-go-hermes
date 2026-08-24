package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// standaloneTestOwnerID and standaloneTestStateRoot satisfy the standalone
// identity disposition Linux requires of every policy that neither borrows a
// process identity nor opts out of credentials. Without them a Linux run is
// rejected before it reaches the behaviour under test, and the case proves
// nothing it names.
const (
	standaloneTestOwnerID   = "acp-go-hermes-tests"
	standaloneTestStateRoot = "/var/lib/acp-go-hermes-tests"
)

type processIsolationTestCapability struct{}

func (processIsolationTestCapability) Duplicate() (*os.File, error) {
	return nil, errors.New("process isolation test capability is not duplicable")
}

func TestProcessIsolationEnvironmentIdentityAndLookup(t *testing.T) {
	// An explicit policy is Linux-only, so the platform is pinned here to keep
	// these strict-policy branches reachable whichever host runs the suite.
	originalPlatform := processRuntimePlatform
	processRuntimePlatform = processPlatformLinux
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	dir := t.TempDir()
	executable := filepath.Join(dir, "hermes")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700))
	isolation := &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{"PATH": dir, "BASE": "one"},
		StandaloneOwnerID: standaloneTestOwnerID, StandaloneStateRoot: standaloneTestStateRoot,
	}
	// Entering through the mode selector is what proves the explicit arm: an
	// ambient snapshot is supplied alongside the policy and must be ignored
	// entirely, because a closed policy carries its own replacement environment.
	environment, err := processLaunchEnvironment(ProcessOptions{
		Isolation:          isolation,
		AmbientEnvironment: map[string]string{"AMBIENT_ISOLATION_CANARY": "must-not-leak"},
		Env:                map[string]string{"BASE": "two", "EXPLICIT": "yes"},
	})
	require.NoError(t, err)
	require.Contains(t, environment, "BASE=two")
	require.Contains(t, environment, "EXPLICIT=yes")
	require.NotContains(t, environment, "AMBIENT_ISOLATION_CANARY=must-not-leak")
	sessionEnvironment, err := processSessionLaunchEnvironment(ProcessOptions{
		Isolation:  isolation,
		Env:        map[string]string{"STATIC": "base"},
		SessionEnv: map[string]string{"SESSION": "carrier"},
	})
	require.NoError(t, err)
	require.Contains(t, sessionEnvironment, "STATIC=base")
	require.Contains(t, sessionEnvironment, "SESSION=carrier")
	resolved, err := lookPathInEnvironment("hermes", environment)
	require.NoError(t, err)
	require.Equal(t, executable, resolved)
	_, err = lookPathInEnvironment("relative/hermes", environment)
	require.Error(t, err)
	_, err = isolationEnvironment(&ProcessIsolation{
		UID: 1, GID: 1, BaseEnvironment: map[string]string{"BAD=KEY": "x"},
		StandaloneOwnerID: standaloneTestOwnerID, StandaloneStateRoot: standaloneTestStateRoot,
	})
	require.Error(t, err)
	for _, invalid := range []*ProcessIsolation{
		nil,
		{UID: 1, GID: 1},
		{UID: 0, GID: 1},
		{UID: 1, GID: 0},
		{
			UID: 1, GID: 1, BaseEnvironment: map[string]string{privateSupervisorEnvPrefix + "ISOLATION_UID": "1"},
			StandaloneOwnerID: standaloneTestOwnerID, StandaloneStateRoot: standaloneTestStateRoot,
		},
		{
			UID: 1, GID: 1, BaseEnvironment: map[string]string{"ACP_GO_HERMES_PROCESS_SUPERVISOR_TARGET": "/tmp/x"},
			StandaloneOwnerID: standaloneTestOwnerID, StandaloneStateRoot: standaloneTestStateRoot,
		},
	} {
		require.Error(t, validateProcessIsolation(invalid))
	}
	_, err = isolationEnvironment(isolation, map[string]string{"BAD=KEY": "x"})
	require.Error(t, err)
	require.Equal(t, "B", envValue([]string{"invalid", "A=B"}, "A"))
	require.Empty(t, envValue([]string{"A=B"}, "missing"))
	for _, lookup := range []struct {
		file string
		env  []string
	}{
		{"", environment},
		{"hermes", nil},
		{"hermes", []string{"PATH=relative"}},
		{"missing", environment},
		{dir, environment},
	} {
		_, lookupErr := lookPathInEnvironment(lookup.file, lookup.env)
		require.Error(t, lookupErr, "lookup %#v", lookup)
	}
	nonExecutable := filepath.Join(dir, "non-executable")
	require.NoError(t, os.WriteFile(nonExecutable, []byte("x"), 0o600))
	_, err = executableFile(nonExecutable)
	require.Error(t, err)
}

func TestProcessIsolationStandaloneDisposition(t *testing.T) {
	originalPlatform := processRuntimePlatform
	processRuntimePlatform = "linux"
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	borrowed := &ProcessIsolation{
		UID: 1, GID: 2, BaseEnvironment: map[string]string{},
		IdentityLock: processIsolationTestCapability{}, AuthorityDomain: processIsolationTestCapability{},
	}
	require.NoError(t, validateProcessIsolation(borrowed))
	require.Error(t, validateProcessIsolation(&ProcessIsolation{
		UID: 1, GID: 2, BaseEnvironment: map[string]string{}, IdentityLock: processIsolationTestCapability{},
	}))
	require.Error(t, validateStandaloneIdentityDisposition(&ProcessIsolation{
		IdentityLock: processIsolationTestCapability{},
	}))
	require.Error(t, validateStandaloneIdentityDisposition(&ProcessIsolation{
		IdentityLock: processIsolationTestCapability{}, AuthorityDomain: processIsolationTestCapability{},
		StandaloneOwnerID: "mixed",
	}))
	require.Error(t, validateStandaloneIdentityDisposition(&ProcessIsolation{
		StandaloneOwnerID: "-deployment", StandaloneStateRoot: "/var/lib/hermes",
	}))
	require.Error(t, validateStandaloneIdentityDisposition(&ProcessIsolation{
		StandaloneOwnerID: "deployment-1", StandaloneStateRoot: "relative",
	}))
	require.NoError(t, validateStandaloneIdentityDisposition(&ProcessIsolation{
		StandaloneOwnerID: "deployment-1", StandaloneStateRoot: "/var/lib/hermes",
	}))

	// Off Linux the standalone disposition is never even consulted: the
	// platform gate refuses the explicit policy first.
	processRuntimePlatform = "darwin"
	require.ErrorContains(t, validateProcessIsolation(&ProcessIsolation{
		UID: 1, GID: 2, BaseEnvironment: map[string]string{},
	}), "only on linux")
}

func TestProcessIsolationStandaloneFieldGrammar(t *testing.T) {
	for _, value := range []string{
		"", strings.Repeat("a", 257), "-deployment", "deployment space",
	} {
		require.False(t, validStandaloneOwnerID(value), value)
	}
	for _, value := range []string{"A", "deployment-1", "org.example:worker/1"} {
		require.True(t, validStandaloneOwnerID(value), value)
	}

	for _, value := range []string{
		"", strings.Repeat("/a", 2049), string([]byte{0xff}), "relative", "/tmp/../tmp/native",
		"/", "/tmp/native\x00", "/tmp/native\n", "/var/lib/acp-go/agent-identities",
		"/var/lib/acp-go/agent-identities/provider",
	} {
		require.False(t, validStandaloneStateRootPath(value), value)
	}
	require.True(t, validStandaloneStateRootPath("/var/lib/hermes"))
}
