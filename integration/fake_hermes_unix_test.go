//go:build integration && unix

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

func TestHermesACPAgentFakeExecutableLeaseReaperKillsMatchingHermesProcess(t *testing.T) {
	requireRunIntegration(t)
	if runtime.GOOS != "linux" {
		t.Skip("Hermes lease process identity reads /proc on Linux")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	home := t.TempDir()
	sessionRoot := filepath.Join(home, "acp-go-hermes", "orphan")
	stateDir := filepath.Join(sessionRoot, "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}

	processPath := filepath.Join(t.TempDir(), "fake-hermes-orphan")
	if err := os.WriteFile(processPath, []byte("#!/bin/sh\nwhile :; do sleep 10; done\n"), 0o700); err != nil {
		t.Fatalf("write orphan executable: %v", err)
	}
	token := "lease-reaper-token"
	orphan := exec.CommandContext(ctx, processPath, "serve") // #nosec G204 -- integration test starts a local script.
	orphan.Env = append(os.Environ(),
		"HERMES_HOME="+sessionRoot,
		"HERMES_DASHBOARD_SESSION_TOKEN="+token,
	)
	orphan.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := orphan.Start(); err != nil {
		t.Fatalf("start matching orphan process: %v", err)
	}
	waitOrphan := make(chan error, 1)
	go func() { waitOrphan <- orphan.Wait() }()
	orphanExited := false
	t.Cleanup(func() {
		if orphanExited {
			return
		}
		_ = orphan.Process.Kill()
		<-waitOrphan
	})

	startTime := procStartTimeForPID(t, orphan.Process.Pid)
	lease := map[string]any{
		"pid":                orphan.Process.Pid,
		"port":               0,
		"startedAtUnixMilli": time.Now().UnixMilli(),
		"tokenHash":          hexSHA256(token),
		"xdgRoot":            sessionRoot,
		"processStartTime":   startTime,
	}
	data, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	leasePath := filepath.Join(stateDir, "server.lease")
	if err := os.WriteFile(leasePath, data, 0o600); err != nil {
		t.Fatalf("write lease: %v", err)
	}

	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeOK), home)
	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if _, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir())); err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	select {
	case err := <-waitOrphan:
		orphanExited = true
		if err == nil {
			t.Fatal("matching orphan exited cleanly, want signal termination")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("matching stale-lease process was not reaped")
	}
	if _, err := os.Stat(leasePath); !os.IsNotExist(err) {
		t.Fatalf("lease file stat after reaping err=%v", err)
	}
}

func hexSHA256(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func procStartTimeForPID(t *testing.T, pid int) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		t.Fatalf("read proc stat: %v", err)
	}
	stat := string(data)
	closeParen := strings.LastIndex(stat, ")")
	if closeParen < 0 || closeParen+2 >= len(stat) {
		t.Fatalf("malformed proc stat: %q", stat)
	}
	fields := strings.Fields(stat[closeParen+2:])
	if len(fields) < 20 {
		t.Fatalf("short proc stat: %q", stat)
	}
	return fields[19]
}
