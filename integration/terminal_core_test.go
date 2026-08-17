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
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("resolve home for official Hermes install: %v", err)
	}
	agentRoot := filepath.Join(home, ".hermes", "hermes-agent")
	python := filepath.Join(agentRoot, "venv", "bin", "python")
	info, err := os.Stat(python)
	if err != nil {
		t.Fatalf("official Hermes venv interpreter %q: %v", python, err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatalf("official Hermes venv interpreter is not executable: %q", python)
	}

	root := filepath.Join(t.TempDir(), "terminal-core")
	// The probe's roots ride in argv: test-only state must not claim a name in
	// the adapter's governed environment namespace, which is reserved for real
	// options.
	command := exec.CommandContext(t.Context(), python, "terminal_core_probe.py", root, agentRoot) // #nosec G204,G702 -- exact local official-Hermes venv selected above.

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("deterministic Hermes terminal-core characterization: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "TERMINAL_CORE_CWD_ISOLATED=true") {
		t.Fatalf("terminal-core characterization did not prove cwd crossing:\n%s", output)
	}
}
