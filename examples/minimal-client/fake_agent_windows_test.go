//go:build windows

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFakeAgent writes an executable that drains its stdin and exits, and
// returns the path a launcher must name. Windows resolves an executable by
// extension and cannot run a shebang script, so the fake is a batch file whose
// name carries the ".cmd" the resolver has to find.
func writeFakeAgent(t *testing.T, dir string) string {
	t.Helper()

	path := filepath.Join(dir, "fake-agent.cmd")
	body := "@echo off\r\nmore >nul\r\n"

	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	return path
}
