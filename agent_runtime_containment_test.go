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

	agentRuntimePlatform = agentRuntimeLinux

	var observed []RuntimeContainmentMode

	isolated := newIsolatedTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
			observed = append(observed, mode)
		},
	}))
	if got := isolated.ContainmentMode(); got != RuntimeContainmentAuthoritative {
		t.Fatalf("explicit Linux policy mode = %q", got)
	}

	if len(observed) != 1 || observed[0] != RuntimeContainmentAuthoritative {
		t.Fatalf("containment observations = %v", observed)
	}

	agentRuntimePlatform = agentRuntimeDarwin

	var (
		logs      bytes.Buffer
		snapshots int
	)

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

	if !strings.Contains(logs.String(), `"containment":"best_effort"`) ||
		!strings.Contains(logs.String(), "escaped descendants may survive") {
		t.Fatalf("structured best-effort warning = %q", logs.String())
	}

	root := opted.processes.register()
	root.observe(t.Context(), testProviderInventory{count: 7, available: true})
	root.retire(t.Context(), true)

	if snapshots != 0 {
		t.Fatalf("best-effort provider snapshots = %d", snapshots)
	}

	agentRuntimePlatform = "freebsd"

	offDarwin := newTestAgent(WithDarwinBestEffortContainment())
	if offDarwin.ContainmentMode() != RuntimeContainmentUnavailable {
		t.Fatalf("off-Darwin opted mode = %q", offDarwin.ContainmentMode())
	}

	if _, err := offDarwin.Initialize(t.Context(), acp.InitializeRequest{}); err == nil ||
		!strings.Contains(err.Error(), "supported only on darwin") {
		t.Fatalf("off-Darwin opt-in initialization error = %v", err)
	}
}

// TestContainmentModeReportsASharedAgentIdentity pins the selection matrix.
// shared_identity is a non-authoritative posture rather than a Linux
// containment achievement, so an omitted policy reports it on every platform
// the adapter otherwise supports — and a supplied policy never degrades into
// it, whatever identity the adapter happens to be running as.
func TestContainmentModeReportsASharedAgentIdentity(t *testing.T) {
	oldPlatform := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = oldPlatform })

	explicit := &ProcessIsolation{UID: 1000, GID: 1000}

	tests := []struct {
		name       string
		platform   string
		options    Options
		want       RuntimeContainmentMode
		authorized bool
	}{
		{name: "omitted linux", platform: agentRuntimeLinux, want: RuntimeContainmentSharedIdentity},
		{name: "omitted darwin", platform: agentRuntimeDarwin, want: RuntimeContainmentSharedIdentity},
		{name: "omitted windows", platform: "windows", want: RuntimeContainmentSharedIdentity},
		{name: "omitted freebsd", platform: "freebsd", want: RuntimeContainmentSharedIdentity},
		{name: "omitted openbsd", platform: "openbsd", want: RuntimeContainmentSharedIdentity},
		{
			name: "explicit linux", platform: agentRuntimeLinux,
			options: Options{ProcessIsolation: explicit},
			want:    RuntimeContainmentAuthoritative, authorized: true,
		},
		{
			name: "explicit darwin", platform: agentRuntimeDarwin,
			options: Options{ProcessIsolation: explicit}, want: RuntimeContainmentUnavailable,
		},
		{
			name: "explicit windows", platform: "windows",
			options: Options{ProcessIsolation: explicit}, want: RuntimeContainmentUnavailable,
		},
		{
			name: "darwin best effort", platform: agentRuntimeDarwin,
			options: Options{DarwinBestEffortContainment: true}, want: RuntimeContainmentBestEffort,
		},
		{
			name: "best effort off darwin", platform: agentRuntimeLinux,
			options: Options{DarwinBestEffortContainment: true}, want: RuntimeContainmentUnavailable,
		},
		{
			name: "best effort with explicit policy", platform: agentRuntimeDarwin,
			options: Options{DarwinBestEffortContainment: true, ProcessIsolation: explicit},
			want:    RuntimeContainmentUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agentRuntimePlatform = test.platform

			got := containmentMode(test.options)
			if got != test.want {
				t.Fatalf("containment mode = %q, want %q", got, test.want)
			}

			if got.provesWholeTreeLifecycle() != test.authorized {
				t.Fatalf("%q proves whole-tree lifecycle = %v", got, !test.authorized)
			}
		})
	}
}

// TestSharedIdentityAgentKeepsItsLifecycleSurfaces proves ordinary mode keeps
// every reporting surface an Agent has, while publishing none of the evidence
// it cannot produce: the mode is observed once, and the provider-descendant
// inventory is suppressed for the Agent's whole lifetime — including the
// terminal zero a retiring root would otherwise emit.
func TestSharedIdentityAgentKeepsItsLifecycleSurfaces(t *testing.T) {
	oldPlatform := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = oldPlatform })

	for _, platform := range []string{agentRuntimeLinux, agentRuntimeDarwin, "windows", "freebsd"} {
		t.Run(platform, func(t *testing.T) {
			agentRuntimePlatform = platform

			var (
				observed  []RuntimeContainmentMode
				snapshots []int
			)

			agent := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
				ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
					observed = append(observed, mode)
				},
				ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
					snapshots = append(snapshots, count)
				},
			}))

			if got := agent.ContainmentMode(); got != RuntimeContainmentSharedIdentity {
				t.Fatalf("ordinary mode = %q", got)
			}

			if len(observed) != 1 || observed[0] != RuntimeContainmentSharedIdentity {
				t.Fatalf("containment observations = %v", observed)
			}

			root := agent.processes.register()
			root.observe(t.Context(), testProviderInventory{count: 7, available: true})
			root.retire(t.Context(), true)

			if len(snapshots) != 0 {
				t.Fatalf("ordinary execution published provider snapshots %v", snapshots)
			}
		})
	}
}

// TestAgentSessionDefaultsToOrdinaryExecution is the canonical default-mode
// class: an Agent built with no process options manufactures no
// ProcessIsolation value, keeps the public option nil all the way to the launch
// boundary, treats the omission as valid configuration, and reports the
// non-authoritative shared posture.
func TestAgentSessionDefaultsToOrdinaryExecution(t *testing.T) {
	agent := newTestAgent()

	if agent.options.ProcessIsolation != nil {
		t.Fatalf("omission manufactured a policy %+v", agent.options.ProcessIsolation)
	}

	if err := agent.rejectInvalidConfiguration(); err != nil {
		t.Fatalf("omitted policy rejected the configuration: %v", err)
	}

	if got := agent.ContainmentMode(); got != RuntimeContainmentSharedIdentity {
		t.Fatalf("default containment mode = %q", got)
	}

	// The internal launch option is nil too, so no supervisor, authority, or
	// credential machinery is selected on the way to the native process.
	if native := nativeProcessIsolation(agent.options.ProcessIsolation, false, ""); native != nil {
		t.Fatalf("omission produced a native policy %+v", native)
	}

	// The ambient snapshot travels beside the policy rather than inside it.
	if agent.ambientEnv == nil {
		t.Fatal("ordinary execution captured no ambient environment")
	}
}

// TestExplicitProcessIsolationPreservesPolicy is the canonical explicit-mode
// class: a supplied policy is cloned rather than aliased, and an invalid or
// unavailable one refuses without degrading to ordinary or best-effort
// execution.
func TestExplicitProcessIsolationPreservesPolicy(t *testing.T) {
	oldPlatform := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = oldPlatform })

	agentRuntimePlatform = agentRuntimeLinux

	base := map[string]string{"PATH": "/usr/bin"}
	agent := NewAgent(WithProcessIsolation(ProcessIsolation{
		UID: 4242, GID: 4242, BaseEnvironment: base,
		StandaloneOwnerID: "deployment-1", StandaloneStateRoot: "/var/lib/hermes",
	}))

	base["PATH"] = "/mutated"

	if got := agent.options.ProcessIsolation.BaseEnvironment["PATH"]; got != "/usr/bin" {
		t.Fatalf("policy base environment aliased the caller map: %q", got)
	}

	if got := agent.ContainmentMode(); got != RuntimeContainmentAuthoritative {
		t.Fatalf("valid explicit Linux policy mode = %q", got)
	}

	// A structurally valid policy on a platform that cannot host the boundary
	// refuses, and the reported mode never becomes shared or best effort.
	for _, platform := range []string{agentRuntimeDarwin, "windows", "freebsd"} {
		t.Run(platform, func(t *testing.T) {
			agentRuntimePlatform = platform

			refused := NewAgent(WithProcessIsolation(ProcessIsolation{
				UID: 4242, GID: 4242, BaseEnvironment: map[string]string{"PATH": "/usr/bin"},
				StandaloneOwnerID: "deployment-1", StandaloneStateRoot: "/var/lib/hermes",
			}))
			if got := refused.ContainmentMode(); got != RuntimeContainmentUnavailable {
				t.Fatalf("%s explicit policy mode = %q", platform, got)
			}

			err := refused.rejectInvalidConfiguration()
			if err == nil || !strings.Contains(err.Error(), "only on linux") {
				t.Fatalf("%s explicit policy configuration = %v", platform, err)
			}
		})
	}

	agentRuntimePlatform = agentRuntimeLinux

	// An incomplete authority disposition refuses on Linux too.
	incomplete := NewAgent(WithProcessIsolation(ProcessIsolation{
		UID: 4242, GID: 4242, BaseEnvironment: map[string]string{"PATH": "/usr/bin"},
	}))
	if err := incomplete.rejectInvalidConfiguration(); err == nil {
		t.Fatal("explicit policy without an authority disposition was accepted")
	}
}
