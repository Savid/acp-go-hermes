//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

var binaryBuildDir string
var buildIntegrationBinary = sync.OnceValues(func() (string, error) {
	dir, err := os.MkdirTemp("", "acp-go-hermes-integration-")
	if err != nil {
		return "", err
	}
	binaryBuildDir = dir
	name := "acp-go-hermes"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary := filepath.Join(dir, name)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/acp-go-hermes")
	cmd.Dir = integrationRepositoryRoot()
	cmd.WaitDelay = time.Second
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build adapter: %w\n%s", err, output)
	}
	return binary, nil
})

func integrationRepositoryRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("integration source location unavailable")
	}
	return filepath.Dir(filepath.Dir(file))
}

func integrationBinaryPath(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("ACP_GO_HERMES_AGENT_BINARY"); binary != "" {
		absolute, err := filepath.Abs(binary)
		if err != nil {
			t.Fatal(err)
		}
		return absolute
	}
	binary, err := buildIntegrationBinary()
	if err != nil {
		t.Fatal(err)
	}
	return binary
}

func cleanupIntegrationBinary() {
	if binaryBuildDir != "" {
		_ = os.RemoveAll(binaryBuildDir)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	cleanupIntegrationBinary()
	os.Exit(code)
}
