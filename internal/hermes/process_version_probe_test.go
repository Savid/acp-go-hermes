//go:build unix

package hermes

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
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
	body := "#!/bin/sh\nprintf 'Hermes Agent v0.19.0\\n'\nwhile [ ! -f " + gate + " ]; do sleep 0.01; done\n"

	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write version script: %v", err)
	}

	return path
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
// A start refused at spawn used to discard the proof and re-spawn a second
// --version process on the next attempt, which is an extra native process sat
// in front of every retry on an already contended host.
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
