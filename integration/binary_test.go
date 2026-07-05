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
	envHermesPath     = "ACP_GO_HERMES_HERMES_PATH"
	envAgentBinary    = "ACP_GO_HERMES_AGENT_BINARY"
)

func TestHermesACPAgentBinaryClosedInput(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := agentCommand(ctx,
		"-path", integrationHermesPath(t),
		"-home", t.TempDir(),
	)
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
	path := os.Getenv(envHermesPath)
	if path == "" {
		path = "hermes"
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		t.Fatalf("find hermes CLI: %v", err)
	}
	return resolved
}

func agentCommand(ctx context.Context, args ...string) *exec.Cmd {
	if binary := os.Getenv(envAgentBinary); binary != "" {
		return exec.CommandContext(ctx, binary, args...) // #nosec G204,G702 -- opt-in integration test command.
	}
	commandArgs := make([]string, 0, 2+len(args))
	commandArgs = append(commandArgs, "run", "./cmd/acp-go-hermes")
	commandArgs = append(commandArgs, args...)
	cmd := exec.CommandContext(ctx, "go", commandArgs...) // #nosec G204,G702 -- test runs the local wrapper command.
	cmd.Dir = repoRoot()
	return cmd
}

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ".."
	}
	return filepath.Dir(filepath.Dir(file))
}
