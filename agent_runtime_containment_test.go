package hermesacp

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestAgentContainmentModeAndObservation(t *testing.T) {
	oldPlatform, oldEffectiveUID := agentRuntimePlatform, containmentEffectiveUID
	t.Cleanup(func() { agentRuntimePlatform, containmentEffectiveUID = oldPlatform, oldEffectiveUID })
	// This test selects the boundary from the platform, so it holds the identity
	// fixed at the trusted supervisor's; the shared-identity selection has its
	// own matrix below.
	containmentEffectiveUID = func() int { return 0 }
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

	agentRuntimePlatform = agentRuntimeWindows
	if got := newTestAgent().ContainmentMode(); got != RuntimeContainmentUnavailable {
		t.Fatalf("Windows mode = %q", got)
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

func TestContainmentModeReportsASharedAgentIdentity(t *testing.T) {
	oldPlatform, oldEffectiveUID := agentRuntimePlatform, containmentEffectiveUID
	t.Cleanup(func() { agentRuntimePlatform, containmentEffectiveUID = oldPlatform, oldEffectiveUID })

	isolation := &ProcessIsolation{UID: 1000, GID: 1000}
	tests := []struct {
		name      string
		platform  string
		effective int
		isolation *ProcessIsolation
		want      RuntimeContainmentMode
	}{
		{name: "own identity", platform: agentRuntimeLinux, effective: 1000, isolation: isolation, want: RuntimeContainmentSharedIdentity},
		{name: "distinct identity", platform: agentRuntimeLinux, effective: 1001, isolation: isolation, want: RuntimeContainmentAuthoritative},
		{name: "trusted root", platform: agentRuntimeLinux, effective: 0, isolation: isolation, want: RuntimeContainmentAuthoritative},
		{name: "unconfigured", platform: agentRuntimeLinux, effective: 1000, want: RuntimeContainmentAuthoritative},
		{name: "darwin", platform: agentRuntimeDarwin, effective: 1000, isolation: isolation, want: RuntimeContainmentUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agentRuntimePlatform = test.platform
			containmentEffectiveUID = func() int { return test.effective }
			if got := containmentMode(Options{ProcessIsolation: test.isolation}); got != test.want {
				t.Fatalf("containment mode = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSharedIdentityAgentKeepsItsLifecycleSurfaces(t *testing.T) {
	oldPlatform, oldEffectiveUID := agentRuntimePlatform, containmentEffectiveUID
	t.Cleanup(func() { agentRuntimePlatform, containmentEffectiveUID = oldPlatform, oldEffectiveUID })
	agentRuntimePlatform = agentRuntimeLinux
	containmentEffectiveUID = os.Geteuid
	if os.Geteuid() == 0 {
		t.Skip("the shared arm is unreachable from a root process")
	}

	var (
		observed  []RuntimeContainmentMode
		snapshots int
	)
	agent := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
			observed = append(observed, mode)
		},
		ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { snapshots++ },
	}))
	if got := agent.ContainmentMode(); got != RuntimeContainmentSharedIdentity {
		t.Fatalf("shared identity mode = %q", got)
	}
	if len(observed) != 1 || observed[0] != RuntimeContainmentSharedIdentity {
		t.Fatalf("containment observations = %v", observed)
	}

	// Whole-tree lifecycle is still proven, so the descendant inventory the
	// authoritative boundary publishes stays on.
	root := agent.processes.register()
	root.observe(t.Context(), testProviderInventory{count: 7, available: true})
	root.retire(t.Context(), true)
	if snapshots == 0 {
		t.Fatal("shared identity retired the provider descendant inventory")
	}
}
