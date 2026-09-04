//go:build !windows

package main

import (
	"context"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"

	hermesacp "github.com/savid/acp-go-hermes"
)

// TestRunReportsAForwardedSignalExitCode pins that a signal arriving while
// serve runs ends the process with that signal's conventional code rather than
// a generic failure. It lives on the POSIX side because Windows cannot deliver
// SIGTERM to a process at all — os.Process.Signal refuses everything but a
// kill there — so there is no Windows behaviour to state.
func TestRunReportsAForwardedSignalExitCode(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	serve = func(ctx context.Context, _ io.Reader, _ io.Writer, _ ...hermesacp.Option) error {
		proc, err := os.FindProcess(os.Getpid())
		if err != nil {
			return err
		}

		if err := proc.Signal(syscall.SIGTERM); err != nil {
			return err
		}

		<-ctx.Done()

		return ctx.Err()
	}
	if code := run(context.Background(), nil, strings.NewReader(""), io.Discard, io.Discard); code != 143 {
		t.Fatalf("signalled serve code = %d", code)
	}
}
