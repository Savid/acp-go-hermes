//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFakeAgent writes an executable that drains its stdin and exits, and
// returns the path a launcher must name. POSIX resolves an executable by its
// execute bit, so a shell script under a bare name is enough.
func writeFakeAgent(t *testing.T, dir string) string {
	t.Helper()

	path := filepath.Join(dir, "fake-agent")
	if err := os.WriteFile(path, []byte("#!/bin/sh\ncat >/dev/null\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	return path
}
