//go:build integration

package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHermesTerminalCoreSessionCWDIsolation(t *testing.T) {
	root := filepath.Join(t.TempDir(), "terminal-core")
	command := exec.CommandContext(t.Context(), "python3", "terminal_core_probe.py", root)
	command.Env = os.Environ()

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("deterministic Hermes terminal-core characterization: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "TERMINAL_CORE_CWD_ISOLATED=true") {
		t.Fatalf("terminal-core characterization did not prove cwd crossing:\n%s", output)
	}
}
