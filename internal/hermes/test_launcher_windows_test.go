//go:build windows

package hermes

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeTestBinaryLauncher writes an executable named name in dir that runs the
// test binary with env set and args ahead of whatever it is itself given, and
// returns the path a launcher of it must name. Windows resolves an executable
// by extension rather than by an execute bit, and it cannot run a shebang
// script at all, so the launcher is a batch file and the returned path carries
// the ".cmd" the resolver has to find.
func writeTestBinaryLauncher(t *testing.T, dir, name, testBinary string, env map[string]string, args []string) string {
	t.Helper()

	// cmd.exe cannot replace itself the way a POSIX shell execs, so the launcher
	// stays alive as the parent of the process it starts. Move that pair out of
	// whatever directory the launch chose first: Windows will not delete a
	// directory any live process has as its own, and the fake outlives the
	// launch it answers by design.
	body := "@echo off\r\ncd /d \"%SystemRoot%\"\r\n"
	owned := launcherOwnerEnv(env)
	for _, key := range sortedLauncherEnvKeys(owned) {
		body += fmt.Sprintf("set \"%s=%s\"\r\n", key, owned[key])
	}

	quoted := ""
	for _, arg := range args {
		quoted += fmt.Sprintf("%q ", arg)
	}

	path := filepath.Join(dir, name+".cmd")
	body += fmt.Sprintf("%q %s%%*\r\n", testBinary, quoted)

	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}

	return path
}
