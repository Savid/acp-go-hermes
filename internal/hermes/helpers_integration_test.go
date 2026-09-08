//go:build integration

package hermes

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func integrationHermesCLI(t *testing.T) string {
	t.Helper()
	if os.Getenv("ACP_GO_HERMES_RUN_INTEGRATION") != "1" {
		t.Skip("set ACP_GO_HERMES_RUN_INTEGRATION=1")
	}
	path := os.Getenv("ACP_GO_HERMES_HARNESS_PATH")
	if path == "" {
		path = "hermes"
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		if os.Getenv("ACP_GO_HERMES_RUN_LIVE_TOKENS") == "1" || os.Getenv("ACP_GO_HERMES_RUN_ATTENDED") == "1" || os.Getenv("ACP_GO_HERMES_RUN_KEYSTORE") == "1" {
			t.Fatalf("requested Hermes integration tier requires the CLI: %v", err)
		}
		t.Skipf("Hermes CLI absent for smoke: %v; set ACP_GO_HERMES_HARNESS_PATH", err)
	}
	return resolved
}

func TestIntegrationHarnessPrerequisites(t *testing.T) {
	if os.Args[len(os.Args)-1] == "harness-prerequisite-child" {
		path := integrationHermesCLI(t)
		t.Log("resolved harness " + path)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, integration, tier, value, outcome string
		available                               bool
	}{
		{name: "ungated", outcome: "SKIP"},
		{name: "disabled", integration: "0", outcome: "SKIP"},
		{name: "invalid_gate", integration: "true", outcome: "SKIP"},
		{name: "missing_smoke", integration: "1", outcome: "SKIP"},
		{name: "disabled_live", integration: "1", tier: "RUN_LIVE_TOKENS", value: "0", outcome: "SKIP"},
		{name: "missing_live", integration: "1", tier: "RUN_LIVE_TOKENS", value: "1", outcome: "FAIL"},
		{name: "missing_attended", integration: "1", tier: "RUN_ATTENDED", value: "1", outcome: "FAIL"},
		{name: "missing_keystore", integration: "1", tier: "RUN_KEYSTORE", value: "1", outcome: "FAIL"},
		{name: "fake_path", integration: "1", outcome: "PASS", available: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, suffix := range []string{"RUN_INTEGRATION", "RUN_LIVE_TOKENS", "RUN_ATTENDED", "RUN_KEYSTORE"} {
				t.Setenv("ACP_GO_HERMES_"+suffix, "0")
			}
			t.Setenv("ACP_GO_HERMES_RUN_INTEGRATION", tc.integration)
			if tc.tier != "" {
				t.Setenv("ACP_GO_HERMES_"+tc.tier, tc.value)
			}
			dir := t.TempDir()
			harness := filepath.Join(dir, "hermes")
			if runtime.GOOS == "windows" {
				harness += ".exe"
			}
			if tc.available {
				// Resolution only: this file is never executed.
				if err := os.WriteFile(harness, []byte("fake harness path"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("ACP_GO_HERMES_HARNESS_PATH", harness)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestIntegrationHarnessPrerequisites$", "-test.v", "--", "harness-prerequisite-child")
			cmd.WaitDelay = time.Second
			output, runErr := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			if (runErr != nil) != (tc.outcome == "FAIL") {
				t.Fatalf("unexpected child result: %v\n%s", runErr, output)
			}
			if !strings.Contains(string(output), "--- "+tc.outcome+": TestIntegrationHarnessPrerequisites") {
				t.Fatalf("want child %s:\n%s", tc.outcome, output)
			}
			if tc.available && !strings.Contains(string(output), "resolved harness "+harness) {
				t.Fatalf("fake harness selection was lost:\n%s", output)
			}
		})
	}
}
