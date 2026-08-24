package hermesacp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestRecoverAgentGoroutineLogsPanic(t *testing.T) {
	const secret = "panic-secret-sentinel"
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	func() {
		defer recoverAgentGoroutine(context.Background(), logger, "test goroutine")
		panic(secret)
	}()

	if !strings.Contains(buf.String(), "test goroutine") || !strings.Contains(buf.String(), "panic_recovered") || strings.Contains(buf.String(), secret) {
		t.Fatalf("panic log = %q", buf.String())
	}
}

func TestHandleAgentGoroutinePanicBranches(t *testing.T) {
	handleAgentGoroutinePanic(context.Background(), nil, "none", nil, nil)

	var recovered any
	handleAgentGoroutinePanic(context.Background(), nil, "with shutdown", func(value any) {
		recovered = value
	}, "panic-secret-sentinel")
	recoveredErr, ok := recovered.(error)
	if !ok || !errors.Is(recoveredErr, errAgentGoroutinePanic) {
		t.Fatalf("shutdown recovered = %#v", recovered)
	}
	if agentLogger(nil) != nil {
		t.Fatal("nil agent logger returned non-nil")
	}
	agent := newTestAgent()
	if agentLogger(agent) != agent.log {
		t.Fatal("agent logger mismatch")
	}
}
