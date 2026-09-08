//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

type liveSessionCLIProbe struct {
	mu      sync.Mutex
	allowed map[string]string
	calls   []liveSessionCLICall
}

type liveSessionCLICall struct {
	Token     string
	Operation string
	Accepted  bool
}

func (p *liveSessionCLIProbe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	operation := r.Header.Get("X-Wagie-Operation")

	p.mu.Lock()
	accepted := p.allowed[token] == operation && operation != ""
	p.calls = append(p.calls, liveSessionCLICall{Token: token, Operation: operation, Accepted: accepted})
	p.mu.Unlock()

	if !accepted {
		http.Error(w, "unauthorized", http.StatusUnauthorized)

		return
	}

	_, _ = fmt.Fprintf(w, "WAGIE_OK_%s", operation)
}

func (p *liveSessionCLIProbe) rotate(oldToken string, newToken string, operation string) {
	p.mu.Lock()
	delete(p.allowed, oldToken)
	p.allowed[newToken] = operation
	p.mu.Unlock()
}

func (p *liveSessionCLIProbe) acceptedCalls(operation string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := 0
	for _, call := range p.calls {
		if call.Accepted && call.Operation == operation {
			count++
		}
	}

	return count
}

func (p *liveSessionCLIProbe) snapshot() []liveSessionCLICall {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]liveSessionCLICall(nil), p.calls...)
}

type liveSessionCLICarrier struct {
	cwd       string
	dir       string
	token     string
	operation string
}

func TestHermesACPAgentLiveSessionCLICarrierRotation(t *testing.T) {
	requireRunLiveTokens(t)
	if runtime.GOOS == "windows" {
		t.Skip("the live proof drives the native POSIX terminal tool")
	}
	curlPath, err := exec.LookPath("curl")
	if err != nil {
		t.Skipf("curl is required for the loopback CLI probe: %v", err)
	}

	first := liveSessionCLICarrier{cwd: t.TempDir(), dir: t.TempDir(), token: "live-bearer-a", operation: "live-operation-a"}
	second := liveSessionCLICarrier{cwd: t.TempDir(), dir: t.TempDir(), token: "live-bearer-b", operation: "live-operation-b"}
	probe := &liveSessionCLIProbe{allowed: map[string]string{
		first.token:  first.operation,
		second.token: second.operation,
	}}
	server := httptest.NewServer(probe)
	defer server.Close()
	writeLiveWagie(t, first.dir, curlPath, server.URL)
	writeLiveWagie(t, second.dir, curlPath, server.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if writeErr := os.WriteFile(configPath, []byte(liveToolTokenHermesConfig()), 0o600); writeErr != nil {
		t.Fatalf("write live-test Hermes config: %v", writeErr)
	}
	args := []string{
		"-seed-file", "config.yaml=" + configPath,
	}
	if model := os.Getenv("ACP_GO_HERMES_MODEL"); model != "" {
		args = append(args, "-model", model)
	}
	agent := startLiveAgent(t, ctx, t.TempDir(), args...)
	defer agent.close()
	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, initializeErr := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); initializeErr != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", initializeErr, agent.stderrString())
	}

	firstSession, err := conn.NewSession(ctx, liveSessionCLIRequest(first))
	if err != nil {
		t.Fatalf("new first session: %v\nstderr:\n%s", err, agent.stderrString())
	}
	secondSession, err := conn.NewSession(ctx, liveSessionCLIRequest(second))
	if err != nil {
		t.Fatalf("new second session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	type promptResult struct {
		operation string
		err       error
	}
	results := make(chan promptResult, 2)
	for _, turn := range []struct {
		session acp.SessionId
		carrier liveSessionCLICarrier
		nonce   string
	}{
		{session: firstSession.SessionId, carrier: first, nonce: "session-cli-a"},
		{session: secondSession.SessionId, carrier: second, nonce: "session-cli-b"},
	} {
		go func() {
			_, promptErr := conn.Prompt(ctx, hermesacp.TextPromptRequest(
				turn.session,
				turn.nonce,
				liveSessionCLIPrompt(turn.carrier),
			))
			results <- promptResult{operation: turn.carrier.operation, err: promptErr}
		}()
	}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("prompt %s: %v\nstderr:\n%s", result.operation, result.err, agent.stderrString())
		}
	}
	if probe.acceptedCalls(first.operation) != 1 || probe.acceptedCalls(second.operation) != 1 {
		t.Fatalf("concurrent native CLI calls did not preserve both carriers: calls=%#v text=%q stderr=%q",
			probe.snapshot(), client.agentText(), agent.stderrString())
	}

	rotated := liveSessionCLICarrier{cwd: first.cwd, dir: t.TempDir(), token: "live-bearer-a-rotated", operation: "live-operation-a-rotated"}
	writeLiveWagie(t, rotated.dir, curlPath, server.URL)
	if _, closeErr := conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: firstSession.SessionId}); closeErr != nil {
		t.Fatalf("close first session: %v\nstderr:\n%s", closeErr, agent.stderrString())
	}
	probe.rotate(first.token, rotated.token, rotated.operation)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+first.token)
	request.Header.Set("X-Wagie-Operation", first.operation)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("old-bearer control request: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old bearer status = %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}

	if _, err := conn.ResumeSession(ctx, hermesacp.ResumeSessionRequest(
		firstSession.SessionId,
		rotated.cwd,
		hermesacp.WithSessionHermesOptions(hermesacp.HermesOptions{
			Env: liveTokenEnv(map[string]string{
				"WAGIE_API_TOKEN":    rotated.token,
				"WAGIE_OPERATION_ID": rotated.operation,
			}),
			ExtraPathDirs: []string{rotated.dir},
		}),
	)); err != nil {
		t.Fatalf("resume rotated session: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(
		firstSession.SessionId,
		"session-cli-a-rotated",
		liveSessionCLIPrompt(rotated),
	)); err != nil {
		t.Fatalf("rotated prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if probe.acceptedCalls(rotated.operation) != 1 || probe.acceptedCalls(second.operation) != 1 {
		t.Fatalf("rotation changed the wrong session carrier: calls=%#v text=%q stderr=%q",
			probe.snapshot(), client.agentText(), agent.stderrString())
	}
}

func liveSessionCLIRequest(carrier liveSessionCLICarrier) acp.NewSessionRequest {
	return hermesacp.NewSessionRequest(carrier.cwd, hermesacp.WithSessionHermesOptions(hermesacp.HermesOptions{
		Env: liveTokenEnv(map[string]string{
			"WAGIE_API_TOKEN":    carrier.token,
			"WAGIE_OPERATION_ID": carrier.operation,
		}),
		ExtraPathDirs: []string{carrier.dir},
	}))
}

func liveSessionCLIPrompt(carrier liveSessionCLICarrier) string {
	wagie := filepath.Join(carrier.dir, "wagie")
	command := fmt.Sprintf(
		`test "$(command -v wagie)" = %q && wagie`,
		wagie,
	)

	return "Use the terminal tool exactly once to run this exact command: " + command +
		". After it succeeds, reply with exactly SESSION_CLI_OK."
}

func writeLiveWagie(t *testing.T, dir string, curlPath string, endpoint string) {
	t.Helper()
	body := fmt.Sprintf(
		"#!/bin/sh\nexec %q -fsS -H \"Authorization: Bearer $WAGIE_API_TOKEN\" -H \"X-Wagie-Operation: $WAGIE_OPERATION_ID\" %q\n",
		curlPath,
		endpoint,
	)
	if err := os.WriteFile(filepath.Join(dir, "wagie"), []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
}
