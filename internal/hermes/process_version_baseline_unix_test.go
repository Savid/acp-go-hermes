//go:build unix

package hermes

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeVersionHarness(t *testing.T, path, version, countPath, gatePath string) {
	t.Helper()
	body := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n"
	if countPath != "" {
		body += fmt.Sprintf("  printf 'probe\\n' >> %q\n", countPath)
	}
	body += fmt.Sprintf("  printf 'Hermes Agent v%s\\n'\n", version)
	if gatePath != "" {
		body += fmt.Sprintf("  while [ ! -f %q ]; do sleep 0.01; done\n", gatePath)
	}
	body += "fi\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o700))
}

func awaitProbeCount(t *testing.T, path string, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read probe count: %v", err)
		}

		return strings.Count(string(data), "probe\n") >= want
	}, 10*time.Second, time.Millisecond)
}

func TestSharedHomeVersionProbeIsFreshAfterSamePathReplacement(t *testing.T) {
	executable := filepath.Join(durableTempDir(t), "hermes")
	options := darwinTestProcessOptions(t, ProcessOptions{SharedHome: true, ScratchParent: durableTempDir(t)})

	writeVersionHarness(t, executable, "0.20.0", "", "")
	require.NoError(t, ensureExecutableVersion(t.Context(), executable, options))
	writeVersionHarness(t, executable, "0.21.0", "", "")
	require.NoError(t, ensureExecutableVersion(t.Context(), executable, options))
	writeVersionHarness(t, executable, "0.19.0", "", "")
	require.ErrorContains(t, ensureExecutableVersion(t.Context(), executable, options), "below minimum")
}

func TestSharedHomeSkipsMutatingGatewayMethodProbe(t *testing.T) {
	executable := filepath.Join(durableTempDir(t), "unprobed-hermes")
	require.True(t, gatewayMethodProbeNeeded(ProcessOptions{}, executable))
	require.False(t, gatewayMethodProbeNeeded(ProcessOptions{SharedHome: true}, executable))

	process, err := Start(t.Context(), darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, "probe-error:session.create"),
		Home:           durableTempDir(t),
		SharedHome:     true,
		Timeout:        10 * time.Second,
	}))
	require.NoError(t, err)
	require.NoError(t, process.Close(t.Context()))
}

func TestVersionProbeIsSingleflightedAcrossConcurrentStarts(t *testing.T) {
	directory := durableTempDir(t)
	executable := filepath.Join(directory, "hermes")
	countPath := filepath.Join(directory, "count")
	gatePath := filepath.Join(directory, "gate")
	writeVersionHarness(t, executable, "0.20.0", countPath, gatePath)
	options := darwinTestProcessOptions(t, ProcessOptions{ScratchParent: durableTempDir(t)})

	holder := make(chan error, 1)
	go func() { holder <- ensureExecutableVersion(context.Background(), executable, options) }()
	awaitProbeCount(t, countPath, 1)

	leavingCtx, cancel := context.WithCancel(context.Background())
	leaving := make(chan error, 1)
	go func() { leaving <- ensureExecutableVersion(leavingCtx, executable, options) }()
	cancel()
	require.ErrorIs(t, <-leaving, context.Canceled)

	joined := make(chan error, 3)
	for range cap(joined) {
		go func() { joined <- ensureExecutableVersion(context.Background(), executable, options) }()
	}
	require.NoError(t, os.WriteFile(gatePath, []byte("go"), 0o600))
	require.NoError(t, <-holder)
	for range cap(joined) {
		require.NoError(t, <-joined)
	}
	data, err := os.ReadFile(countPath)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(data), "probe\n"))
	require.False(t, gatewayMethodsProbed(executable))
}

func TestAbandonedVersionProbeDoesNotFailTheStartsWaitingOnIt(t *testing.T) {
	directory := durableTempDir(t)
	executable := filepath.Join(directory, "hermes")
	countPath := filepath.Join(directory, "count")
	gatePath := filepath.Join(directory, "gate")
	writeVersionHarness(t, executable, "0.20.0", countPath, gatePath)
	options := darwinTestProcessOptions(t, ProcessOptions{ScratchParent: durableTempDir(t)})

	abandoningCtx, abandon := context.WithCancel(context.Background())
	abandoned := make(chan error, 1)
	go func() { abandoned <- ensureExecutableVersion(abandoningCtx, executable, options) }()
	awaitProbeCount(t, countPath, 1)

	waiting := make(chan error, 1)
	go func() { waiting <- ensureExecutableVersion(context.Background(), executable, options) }()
	abandon()
	require.ErrorIs(t, <-abandoned, context.Canceled)
	awaitProbeCount(t, countPath, 2)
	require.NoError(t, os.WriteFile(gatePath, []byte("go"), 0o600))
	require.NoError(t, <-waiting)
}

func TestVersionProbeSurvivesAStartThatFailsAfterIt(t *testing.T) {
	directory := durableTempDir(t)
	executable := filepath.Join(directory, "hermes")
	countPath := filepath.Join(directory, "count")
	writeVersionHarness(t, executable, "0.20.0", countPath, "")
	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: executable,
		ScratchParent:  durableTempDir(t),
		Timeout:        250 * time.Millisecond,
	})

	for range 2 {
		_, err := Start(t.Context(), options)
		require.Error(t, err)
	}
	data, err := os.ReadFile(countPath)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(data), "probe\n"))
	require.False(t, gatewayMethodsProbed(executable))
}

func TestStartRefusesUnsupportedVersionAndStartupMethods(t *testing.T) {
	for _, test := range []struct {
		name string
		mode string
		want string
	}{
		{name: "below minimum", mode: fakeProcessModeOldVersion, want: "below minimum"},
		{name: "malformed version", mode: fakeProcessModeBadVersion, want: "missing semantic version"},
		{name: "missing required method", mode: fakeProcessModeMissingMethod, want: "method not found"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Start(t.Context(), darwinTestProcessOptions(t, ProcessOptions{
				ExecutablePath: fakeHermesExecutable(t, test.mode),
				ScratchParent:  durableTempDir(t),
				Timeout:        10 * time.Second,
			}))
			require.ErrorContains(t, err, test.want)
		})
	}
}
