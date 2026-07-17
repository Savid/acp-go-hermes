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

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func TestHermesFakeExecutableLeaseReaperKillsMatchingHermesProcess(t *testing.T) {
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

	server, err := nativehermes.StartServer(ctx, nativehermes.StartOptions{
		ACPSessionID:   "orphan",
		Root:           filepath.Join(home, "acp-go-hermes"),
		ScratchParent:  home,
		Cwd:            t.TempDir(),
		ExecutablePath: fakeHermesExecutable(t, fakeModeOK),
		HealthTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("start replacement Hermes server: %v", err)
	}
	defer func() { _ = server.Close(context.Background()) }()

	select {
	case err := <-waitOrphan:
		orphanExited = true
		if err == nil {
			t.Fatal("matching orphan exited cleanly, want signal termination")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("matching stale-lease process was not reaped")
	}
	leaseData, err := os.ReadFile(leasePath)
	if err != nil {
		t.Fatalf("read replacement lease: %v", err)
	}
	var replacementLease struct {
		PID int `json:"pid"`
	}
	if err := json.Unmarshal(leaseData, &replacementLease); err != nil {
		t.Fatalf("decode replacement lease: %v", err)
	}
	if replacementLease.PID <= 0 || replacementLease.PID == orphan.Process.Pid {
		t.Fatalf("replacement lease pid = %d, predecessor pid = %d", replacementLease.PID, orphan.Process.Pid)
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
