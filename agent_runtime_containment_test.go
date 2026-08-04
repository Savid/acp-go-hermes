package hermesacp

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestAgentContainmentModeAndObservation(t *testing.T) {
	oldPlatform := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = oldPlatform })
	if got := (*Agent)(nil).ContainmentMode(); got != RuntimeContainmentUnavailable {
		t.Fatalf("nil agent mode = %q", got)
	}

	agentRuntimePlatform = "linux"
	var observed []RuntimeContainmentMode
	defaultAgent := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
			observed = append(observed, mode)
		},
	}))
	want := RuntimeContainmentAuthoritative
	if got := defaultAgent.ContainmentMode(); got != want {
		t.Fatalf("default mode = %q, want %q", got, want)
	}
	if len(observed) != 1 || observed[0] != want {
		t.Fatalf("containment observations = %v", observed)
	}

	agentRuntimePlatform = "darwin"
	if got := newTestAgent().ContainmentMode(); got != RuntimeContainmentUnavailable {
		t.Fatalf("Darwin default mode = %q", got)
	}
	var logs bytes.Buffer
	var snapshots int
	opted := newTestAgent(
		WithDarwinBestEffortContainment(),
		WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { snapshots++ },
		}),
	)
	if opted.ContainmentMode() != RuntimeContainmentBestEffort {
		t.Fatalf("opted mode = %q", opted.ContainmentMode())
	}
	if !strings.Contains(logs.String(), `"containment":"best_effort"`) || !strings.Contains(logs.String(), "escaped descendants may survive") {
		t.Fatalf("structured best-effort warning = %q", logs.String())
	}
	root := opted.processes.register()
	root.observe(t.Context(), testProviderInventory{count: 7, available: true})
	root.retire(t.Context(), true)
	if snapshots != 0 {
		t.Fatalf("best-effort provider snapshots = %d", snapshots)
	}

	agentRuntimePlatform = "freebsd"
	if got := newTestAgent().ContainmentMode(); got != RuntimeContainmentUnavailable {
		t.Fatalf("unsupported mode = %q", got)
	}
	opted = newTestAgent(WithDarwinBestEffortContainment())
	if opted.ContainmentMode() != RuntimeContainmentUnavailable {
		t.Fatalf("off-Darwin opted mode = %q", opted.ContainmentMode())
	}
	if _, err := opted.Initialize(t.Context(), acp.InitializeRequest{}); err == nil || !strings.Contains(err.Error(), "supported only on darwin") {
		t.Fatalf("off-Darwin opt-in initialization error = %v", err)
	}
}
