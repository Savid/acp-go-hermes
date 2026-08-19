//go:build unix

package hermes

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testVersionScript writes a shell harness that prints an acceptable version
// and then blocks until gate exists, so a test can hold one probe open while
// other callers arrive.
func testVersionScript(t *testing.T, gate string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "hermes")
	body := "#!/bin/sh\nprintf 'Hermes Agent v0.20.0\\n'\nwhile [ ! -f " + gate + " ]; do sleep 0.01; done\n"

	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write version script: %v", err)
	}

	return path
}

func writeImmediateVersionScript(t *testing.T, path string, version string) {
	t.Helper()
	body := "#!/bin/sh\nprintf 'Hermes Agent v" + version + "\\n'\n"
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write immediate version script: %v", err)
	}
}

func TestSharedHomeVersionProbeIsFreshAfterSamePathReplacement(t *testing.T) {
	restoreProcessSeams(t)
	executable := filepath.Join(t.TempDir(), "hermes")
	options := darwinTestProcessOptions(t, ProcessOptions{SharedHome: true, ScratchParent: t.TempDir()})

	writeImmediateVersionScript(t, executable, "0.20.0")
	if err := ensureExecutableVersion(t.Context(), executable, options); err != nil {
		t.Fatalf("first shared version probe: %v", err)
	}
	writeImmediateVersionScript(t, executable, "0.21.0")
	if err := ensureExecutableVersion(t.Context(), executable, options); err != nil {
		t.Fatalf("replacement shared version probe: %v", err)
	}
	if got := executableVersion(executable); got != "0.21.0" {
		t.Fatalf("replacement version = %q, want 0.21.0", got)
	}
}

func TestSharedHomeSkipsMutatingGatewayCompatibilityProbe(t *testing.T) {
	restoreProcessSeams(t)
	executable := filepath.Join(t.TempDir(), "hermes-unprobed")
	if !gatewayMethodProbeNeeded(ProcessOptions{}, executable) {
		t.Fatal("ordinary process unexpectedly skipped the compatibility probe")
	}
	if gatewayMethodProbeNeeded(ProcessOptions{SharedHome: true}, executable) {
		t.Fatal("shared-home process would run the mutating compatibility probe")
	}

	process, err := Start(t.Context(), darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, "probe-error:session.create"),
		Home:           t.TempDir(),
		SharedHome:     true,
		Timeout:        10 * time.Second,
	}))
	if err != nil {
		t.Fatalf("shared-home Start invoked the mutating compatibility probe: %v", err)
	}
	if err := process.Close(t.Context()); err != nil {
		t.Fatalf("close shared-home process: %v", err)
	}
}

func TestSharedHomeVersionBindingPrecedesConfigPreparation(t *testing.T) {
	restoreProcessSeams(t)
	home := t.TempDir()
	if err := bindSharedHermesVersion(t.Context(), home, "0.20.0"); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(home, hermesConfigFileName)
	if err := os.WriteFile(configPath, []byte("operator: unchanged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(t.TempDir(), "hermes")
	writeImmediateVersionScript(t, executable, "0.21.0")
	prepared := false
	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: executable,
		Home:           home,
		SharedHome:     true,
		ScratchParent:  t.TempDir(),
		PrepareSharedHome: func(context.Context, string) error {
			prepared = true

			return os.WriteFile(configPath, []byte("mutated\n"), 0o600)
		},
	})
	// darwinTestProcessOptions normally substitutes a disposable Home to keep
	// unrelated tests isolated; this test intentionally exercises the exact
	// pre-bound durable residence.
	options.Home = home
	if _, err := Start(t.Context(), options); err == nil || !strings.Contains(err.Error(), "bound to Hermes 0.20.0") {
		t.Fatalf("mixed-version start error = %v", err)
	}
	if prepared {
		t.Fatal("config preparation ran before mixed-version rejection")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "operator: unchanged\n" {
		t.Fatalf("operator config changed: %q", data)
	}
	if _, err := os.Stat(filepath.Join(sharedTestControlDir(t, home), sharedConfigFingerprintName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("config fingerprint appeared before version rejection: %v", err)
	}
}

// TestVersionProbeIsSingleflightedAcrossConcurrentStarts proves concurrent
// callers share one --version process. Each probe is a whole second native
// process holding a discovery native root and a scratch generation, so two
// sessions opening at once must not each pay for one; the joiners also honour
// their own contexts, because a caller that leaves must not be able to leave by
// spawning a probe of its own.
func TestVersionProbeIsSingleflightedAcrossConcurrentStarts(t *testing.T) {
	restoreProcessSeams(t)

	gate := filepath.Join(t.TempDir(), "release")
	executable := testVersionScript(t, gate)

	var (
		spawns  int
		spawnMu sync.Mutex
	)

	entered := make(chan struct{}, 1)
	realCommand := command
	command = func(name string, args ...string) *exec.Cmd {
		spawnMu.Lock()
		spawns++
		spawnMu.Unlock()

		select {
		case entered <- struct{}{}:
		default:
		}

		return realCommand(name, args...)
	}

	options := darwinTestProcessOptions(t, ProcessOptions{ScratchParent: t.TempDir()})

	holder := make(chan error, 1)
	go func() { holder <- ensureExecutableVersion(context.Background(), executable, options) }()

	<-entered

	// The probe is registered before its process is spawned, so every caller
	// from here on joins it rather than racing to register another. A joiner
	// that abandons leaves on its own context, and leaving must not be a way to
	// acquire a probe process of its own.
	leaving, cancelLeaving := context.WithCancel(context.Background())
	left := make(chan error, 1)

	go func() { left <- ensureExecutableVersion(leaving, executable, options) }()

	cancelLeaving()

	if err := <-left; !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned joiner error = %v, want context canceled", err)
	}

	joined := make(chan error, 3)
	for range cap(joined) {
		go func() { joined <- ensureExecutableVersion(context.Background(), executable, options) }()
	}

	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatalf("release version probe: %v", err)
	}

	if err := <-holder; err != nil {
		t.Fatalf("version probe: %v", err)
	}

	for range cap(joined) {
		if err := <-joined; err != nil {
			t.Fatalf("joined version probe: %v", err)
		}
	}

	spawnMu.Lock()
	defer spawnMu.Unlock()

	if spawns != 1 {
		t.Fatalf("spawned %d version probes, want 1", spawns)
	}

	if !executableVersionProven(executable) {
		t.Fatal("a passing version left the executable unproven")
	}

	if gatewayMethodsProbed(executable) {
		t.Fatal("the version probe marked the gateway method sweep")
	}
}

// TestAbandonedVersionProbeDoesNotFailTheStartsWaitingOnIt keeps one caller's
// cancellation from becoming everyone's verdict. Sharing a probe is an
// efficiency, not a coupling: the caller that happens to run it can be the one
// whose session/new the host abandoned, and the starts still waiting must reach
// their own answer about the executable rather than inherit a context error
// that says nothing about it.
func TestAbandonedVersionProbeDoesNotFailTheStartsWaitingOnIt(t *testing.T) {
	restoreProcessSeams(t)

	gate := filepath.Join(t.TempDir(), "release")
	executable := testVersionScript(t, gate)

	var (
		spawnMu sync.Mutex
		spawns  int
	)

	realCommand := command
	command = func(name string, args ...string) *exec.Cmd {
		spawnMu.Lock()
		spawns++
		spawnMu.Unlock()

		return realCommand(name, args...)
	}

	// awaitSpawns polls rather than signalling, so the seam never blocks the
	// probe it is observing.
	awaitSpawns := func(want int) {
		t.Helper()

		deadline := time.Now().Add(30 * time.Second)

		for {
			spawnMu.Lock()
			got := spawns
			spawnMu.Unlock()

			if got >= want {
				return
			}

			if time.Now().After(deadline) {
				t.Fatalf("spawned %d version probes, want %d", got, want)
			}

			time.Sleep(time.Millisecond)
		}
	}

	options := darwinTestProcessOptions(t, ProcessOptions{ScratchParent: t.TempDir()})

	abandoning, abandon := context.WithCancel(context.Background())
	abandoned := make(chan error, 1)

	go func() { abandoned <- ensureExecutableVersion(abandoning, executable, options) }()

	awaitSpawns(1)

	waiting := make(chan error, 1)
	go func() { waiting <- ensureExecutableVersion(context.Background(), executable, options) }()

	abandon()

	if err := <-abandoned; !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned probe error = %v, want context canceled", err)
	}

	// The waiter now runs a probe of its own; releasing the gate lets it finish.
	awaitSpawns(2)

	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatalf("release version probe: %v", err)
	}

	if err := <-waiting; err != nil {
		t.Fatalf("a live start inherited the abandoned probe's failure: %v", err)
	}

	if !executableVersionProven(executable) {
		t.Fatal("the retried probe left the executable unproven")
	}
}

// TestVersionProbeSurvivesAStartThatFailsAfterIt proves the version marker
// records what the probe proved rather than whether the whole start succeeded.
// A start refused at spawn keeps the proof, so a retry on an already contended
// host spawns no second --version process in front of it.
func TestVersionProbeSurvivesAStartThatFailsAfterIt(t *testing.T) {
	restoreProcessSeams(t)

	gate := filepath.Join(t.TempDir(), "release")
	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatalf("release gate: %v", err)
	}

	executable := testVersionScript(t, gate)

	spawns := 0
	realCommand := command
	command = func(name string, args ...string) *exec.Cmd {
		spawns++

		return realCommand(name, args...)
	}

	refused := errors.New("containment refused")
	realStart := startHermesContainedProcess
	startHermesContainedProcess = func(cmd *exec.Cmd, specs ...ContainmentSpec) (*processContainment, error) {
		if len(specs) > 0 && specs[0].LifecycleKind == containmentSessionKind {
			return nil, refused
		}

		return realStart(cmd, specs...)
	}

	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: executable,
		Home:           t.TempDir(),
		ScratchParent:  t.TempDir(),
	})

	for attempt := range 2 {
		if _, err := Start(t.Context(), options); !errors.Is(err, refused) {
			t.Fatalf("start attempt %d error = %v, want the containment refusal", attempt, err)
		}

		if spawns != 1 {
			t.Fatalf("start attempt %d spawned %d version probes, want 1", attempt, spawns)
		}
	}

	if !executableVersionProven(executable) {
		t.Fatal("a start that failed after the version probe discarded the proof")
	}

	if gatewayMethodsProbed(executable) {
		t.Fatal("a start that never reached the gateway marked the method sweep")
	}
}
