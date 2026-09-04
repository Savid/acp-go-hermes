//go:build !windows

package hermes

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeTestBinaryLauncher writes an executable named name in dir that runs the
// test binary with env set and args ahead of whatever it is itself given, and
// returns the path a launcher of it must name. POSIX resolves an executable by
// its execute bit, so the name is used as given and a shell script is enough.
func writeTestBinaryLauncher(t *testing.T, dir, name, testBinary string, env map[string]string, args []string) string {
	t.Helper()

	assignments := ""
	owned := launcherOwnerEnv(env)
	for _, key := range sortedLauncherEnvKeys(owned) {
		assignments += fmt.Sprintf("%s=%s ", key, owned[key])
	}

	quoted := ""
	for _, arg := range args {
		quoted += fmt.Sprintf("%q ", arg)
	}

	path := filepath.Join(dir, name)
	body := fmt.Sprintf("#!/bin/sh\n%sexec %q %s\"$@\"\n", assignments, testBinary, quoted)

	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}

	return path
}
