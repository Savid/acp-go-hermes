package hermes

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type probeTestProcess struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
	result NativeResult
	err    error
}

func (p *probeTestProcess) Stdin() io.WriteCloser                      { return p.stdin }
func (p *probeTestProcess) Stdout() io.ReadCloser                      { return p.stdout }
func (p *probeTestProcess) Stderr() io.ReadCloser                      { return p.stderr }
func (p *probeTestProcess) Wait(context.Context) (NativeResult, error) { return p.result, p.err }
func (p *probeTestProcess) Revoke(context.Context) error               { return nil }

type nopWriteCloser struct{ bytes.Buffer }

func (*nopWriteCloser) Close() error { return nil }

func TestVersionProbeUsesAuthorityAndReclaimsBeforeRemoval(t *testing.T) {
	var mu sync.Mutex
	events := make([]string, 0, 4)
	root := ""
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			mu.Lock()
			defer mu.Unlock()
			root = path
			events = append(events, "prepare")

			return nil
		},
		StartNative: func(_ context.Context, request NativeRequest) (NativeProcess, error) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, "start")
			require.Equal(t, "hermes", request.Executable)
			require.Equal(t, []string{"--version"}, request.Arguments)
			require.Contains(t, request.Environment, "HERMES_HOME="+root)

			return &probeTestProcess{
				stdin: &nopWriteCloser{}, stdout: io.NopCloser(strings.NewReader("hermes 0.20.0\n")),
				stderr: io.NopCloser(strings.NewReader("")),
			}, nil
		},
		ReclaimNativeTree: func(_ context.Context, path string) error {
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, root, path)
			events = append(events, "reclaim")

			return nil
		},
	}

	require.NoError(t, probeExecutableVersion(t.Context(), "hermes", opts))
	require.Equal(t, []string{"prepare", "start", "reclaim"}, events)
}

func TestVersionProbeRejectsUnusableAuthorityProcess(t *testing.T) {
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(context.Context, string) error { return nil },
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &probeTestProcess{}, nil
		},
		ReclaimNativeTree: func(context.Context, string) error { return nil },
	}

	require.ErrorContains(t, probeExecutableVersion(t.Context(), "hermes", opts), "unusable host stdio")
}
