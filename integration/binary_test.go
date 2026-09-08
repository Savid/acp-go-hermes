//go:build integration

package integration

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	envRunIntegration = "ACP_GO_HERMES_RUN_INTEGRATION"
	envRunLiveTokens  = "ACP_GO_HERMES_RUN_LIVE_TOKENS"
	envHermesPath     = "ACP_GO_HERMES_HARNESS_PATH"
	envAgentBinary    = "ACP_GO_HERMES_AGENT_BINARY"
)

func TestHermesACPAgentBinaryClosedInput(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := agentCommand(t, ctx, integrationAgentArgs(integrationHermesPath(t), t.TempDir())...)
	cmd.Stdin = strings.NewReader("")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("run acp-go-hermes: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	if stdout.String() != "" {
		t.Fatalf("stdout = %q, want empty ACP stream for closed input", stdout.String())
	}
}

func requireRunIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(envRunIntegration) != "1" {
		t.Skipf("set %s=1 to run live Hermes integration tests", envRunIntegration)
	}
}

func requireRunLiveTokens(t *testing.T) {
	t.Helper()
	requireRunIntegration(t)
	if os.Getenv(envRunLiveTokens) != "1" {
		t.Skipf("set %s=1 to run live Hermes integration tests that spend model tokens", envRunLiveTokens)
	}
}

func integrationHermesPath(t *testing.T) string {
	t.Helper()
	if os.Getenv(envRunIntegration) != "1" {
		t.Skipf("set %s=1 to run hermes integration tests", envRunIntegration)
	}
	path := os.Getenv(envHermesPath)
	if path == "" {
		path = "hermes"
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		if os.Getenv(envRunLiveTokens) == "1" || os.Getenv("ACP_GO_HERMES_RUN_ATTENDED") == "1" || os.Getenv("ACP_GO_HERMES_RUN_KEYSTORE") == "1" {
			t.Fatalf("requested hermes integration tier requires the CLI: %v", err)
		}
		t.Skipf("hermes CLI absent for smoke: %v; set ACP_GO_HERMES_HARNESS_PATH", err)
	}
	return resolved
}

func agentCommand(t *testing.T, ctx context.Context, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, integrationBinaryPath(t), args...)
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ".."
	}
	return filepath.Dir(filepath.Dir(file))
}
