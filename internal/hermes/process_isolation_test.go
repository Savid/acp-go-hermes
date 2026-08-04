package hermes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessIsolationEnvironmentIdentityAndLookup(t *testing.T) {
	t.Setenv("AMBIENT_ISOLATION_CANARY", "must-not-leak")
	dir := t.TempDir()
	executable := filepath.Join(dir, "hermes")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700))
	isolation := &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{"PATH": dir, "BASE": "one"}}
	environment, err := isolationEnvironment(isolation, map[string]string{"BASE": "two", "EXPLICIT": "yes"})
	require.NoError(t, err)
	require.Contains(t, environment, "BASE=two")
	require.Contains(t, environment, "EXPLICIT=yes")
	require.NotContains(t, environment, "AMBIENT_ISOLATION_CANARY=must-not-leak")
	resolved, err := lookPathInEnvironment("hermes", environment)
	require.NoError(t, err)
	require.Equal(t, executable, resolved)
	_, err = lookPathInEnvironment("relative/hermes", environment)
	require.Error(t, err)
	_, err = isolationEnvironment(&ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"BAD=KEY": "x"}})
	require.Error(t, err)
	for _, invalid := range []*ProcessIsolation{
		nil,
		{UID: 1, GID: 1},
		{UID: 0, GID: 1},
		{UID: 1, GID: 0},
		{UID: 1, GID: 1, BaseEnvironment: map[string]string{envIsolationUID: "1"}},
		{UID: 1, GID: 1, BaseEnvironment: map[string]string{"ACP_GO_HERMES_PROCESS_SUPERVISOR_TARGET": "/tmp/x"}},
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
	_, err = supervisorEnvironment(nil, nil, "MODE=1")
	require.Error(t, err)
	supervisorEnv, err := supervisorEnvironment(
		[]string{"A=B", "MODE=old", envIsolationUID + "=old", envIsolationGID + "=old", envIsolationTest + "=old"},
		&ProcessIsolation{UID: 1, GID: 2, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}, "MODE=1",
	)
	require.NoError(t, err)
	values := envSliceMap(supervisorEnv)
	require.Equal(t, "B", values["A"])
	require.Equal(t, "1", values["MODE"])
	require.Equal(t, "true", values[envIsolationTest])
}

func envSliceMap(environment []string) map[string]string {
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}

	return values
}
