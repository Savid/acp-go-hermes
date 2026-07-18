//go:build integration && linux

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

func TestHermesCancelKillsDetachedNativeDescendantBeforePromptSettlesAndRebinds(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	home := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "detached.pid")
	t.Setenv(envFakeHermesDescendantPID, pidFile)
	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeDetachedDescendant), home)
	defer agent.close()

	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	promptDone := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "detached-turn", "spawn detached"))
		promptDone <- struct {
			resp acp.PromptResponse
			err  error
		}{resp: resp, err: err}
	}()

	detachedPID := waitIntegrationPIDFile(t, pidFile)
	t.Cleanup(func() {
		if integrationProcessAlive(detachedPID) {
			_ = syscall.Kill(-detachedPID, syscall.SIGKILL)
		}
	})

	leasePID := waitSessionLeasePID(t, home)
	detachedPGID, err := syscall.Getpgid(detachedPID)
	if err != nil {
		t.Fatalf("detached pgid: %v", err)
	}
	if detachedPGID == leasePID {
		t.Fatalf("fake descendant %d remained in supervisor pgid %d", detachedPID, leasePID)
	}

	if err := conn.Cancel(ctx, hermesacp.CancelRequest(session.SessionId, "detached-turn")); err != nil {
		t.Fatalf("cancel notification: %v", err)
	}
	out := <-promptDone
	if out.err != nil || out.resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("cancelled Prompt = %#v err=%v\nstderr:\n%s", out.resp, out.err, agent.stderrString())
	}
	if integrationProcessAlive(detachedPID) {
		t.Fatalf("Prompt settled while detached descendant %d was still alive", detachedPID)
	}

	resp, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "replacement-turn", "reply"))
	if err != nil {
		t.Fatalf("replacement Prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if resp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("replacement stop reason = %q", resp.StopReason)
	}
}

func waitIntegrationPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr != nil {
				t.Fatalf("parse pid file %q: %v", data, parseErr)
			}

			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("pid file %q was not written", path)

	return 0
}

func waitSessionLeasePID(t *testing.T, home string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(filepath.Join(home, "acp-go-hermes-runtime-*", "state", "server.lease"))
		for _, match := range matches {
			data, err := os.ReadFile(match)
			if err != nil {
				continue
			}
			var lease struct {
				PID int `json:"pid"`
			}
			if json.Unmarshal(data, &lease) == nil && lease.PID > 0 {
				return lease.PID
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("Hermes session lease was not written")

	return 0
}

func integrationProcessAlive(pid int) bool {
	err := syscall.Kill(pid, 0)

	return err == nil || errors.Is(err, syscall.EPERM)
}
