//go:build !windows

package hermes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const browserProbeURL = "https://example.invalid/"

// TestLoginNeverExecsABrowserLauncher proves the launch never reaches a browser
// launcher on PATH. The harness it starts execs every launcher name by bare
// name, and a probe directory ahead of the inherited PATH records every such
// exec in a marker file.
func TestLoginNeverExecsABrowserLauncher(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "launched")
	probe := browserProbeDirOnPath(t, marker)

	for _, name := range browserLauncherNames {
		resolved, lookErr := exec.LookPath(name)
		if lookErr != nil || resolved != filepath.Join(probe, name) {
			t.Fatalf("LookPath(%s) = %q err=%v, want the probe launcher", name, resolved, lookErr)
		}

		if runErr := exec.Command(name, browserProbeURL).Run(); runErr != nil {
			t.Fatalf("probe launcher %s: %v", name, runErr)
		}

		control, controlErr := os.ReadFile(marker)
		if controlErr != nil || !strings.Contains(string(control), name+" "+browserProbeURL) {
			t.Fatalf("probe launcher %s recorded %q err=%v, want the opened URL", name, control, controlErr)
		}
	}

	if removeErr := os.Remove(marker); removeErr != nil {
		t.Fatalf("truncate marker: %v", removeErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	proc, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: browserLaunchingHermesExecutable(t),
		Home:           t.TempDir(),
		Timeout:        10 * time.Second,
		LogWriter:      io.Discard,
	}))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	shimDir := proc.shim.dir
	if err := proc.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if launched, err := os.ReadFile(marker); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a browser launcher on PATH was executed: %q err=%v", launched, err)
	}

	if _, err := os.Stat(shimDir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("shim directory %q outlived the process: %v", shimDir, err)
	}
}

// browserProbeDirOnPath returns a directory of launchers that record their
// arguments in marker, placed ahead of every other PATH entry.
func browserProbeDirOnPath(t *testing.T, marker string) string {
	t.Helper()

	probe := t.TempDir()
	body := fmt.Appendf(nil, "#!/bin/sh\necho \"$0 $*\" >> %q\nexit 0\n", marker)

	for _, name := range browserLauncherNames {
		if err := os.WriteFile(filepath.Join(probe, name), body, 0o700); err != nil {
			t.Fatalf("write probe launcher %s: %v", name, err)
		}
	}

	t.Setenv("PATH", probe+string(os.PathListSeparator)+os.Getenv("PATH"))

	return probe
}

// browserLaunchingHermesExecutable is a harness that opens a URL the way a
// login leg does before serving, resolving the launcher through PATH.
func browserLaunchingHermesExecutable(t *testing.T) string {
	t.Helper()

	serve := fakeHermesExecutable(t, fakeProcessModeOK)
	path := filepath.Join(t.TempDir(), "hermes")

	launches := ""
	for _, name := range browserLauncherNames {
		launches += fmt.Sprintf("%s %q\n", name, browserProbeURL)
	}

	body := fmt.Appendf(nil, `#!/bin/sh
for arg in "$@"; do
	if [ "$arg" = "--version" ]; then
		exec %q "$@"
	fi
done
%sexec %q "$@"
`, serve, launches, serve)

	if err := os.WriteFile(path, body, 0o700); err != nil {
		t.Fatalf("write browser-launching harness: %v", err)
	}

	return path
}

func TestNewBrowserShimWritesExecutableNoOps(t *testing.T) {
	t.Parallel()

	shim, err := newBrowserShim(t.TempDir())
	if err != nil {
		t.Fatalf("newBrowserShim: %v", err)
	}

	info, err := os.Stat(shim.dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("shim directory mode = %v err=%v", info, err)
	}

	for _, name := range browserLauncherNames {
		entry, statErr := os.Stat(filepath.Join(shim.dir, name))
		if statErr != nil || entry.Mode().Perm()&0o100 == 0 {
			t.Fatalf("shim launcher %s = %v err=%v", name, entry, statErr)
		}
	}

	if err := shim.remove(); err != nil {
		t.Fatalf("remove: %v", err)
	}

	if _, err := os.Stat(shim.dir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removed shim directory still present: %v", err)
	}
}

func TestNewBrowserShimReportsMaterialisationFailures(t *testing.T) {
	restoreBrowserShimSeams(t)

	browserShimMkdirTemp = func(string, string) (string, error) {
		return "", errors.New("no scratch")
	}

	if _, err := newBrowserShim(t.TempDir()); err == nil || !strings.Contains(err.Error(), "create browser shim directory") {
		t.Fatalf("newBrowserShim with an unusable scratch parent = %v", err)
	}

	browserShimMkdirTemp = os.MkdirTemp
	browserShimWriteFile = func(string, []byte, os.FileMode) error {
		return errors.New("no launcher")
	}

	if _, err := newBrowserShim(t.TempDir()); err == nil || !strings.Contains(err.Error(), "write browser shim open") {
		t.Fatalf("newBrowserShim with an unwritable launcher = %v", err)
	}
}

func TestProcessStartFailsWhenTheBrowserShimCannotBeBuilt(t *testing.T) {
	restoreBrowserShimSeams(t)

	browserShimMkdirTemp = func(string, string) (string, error) {
		return "", errors.New("no scratch")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
		Timeout:        10 * time.Second,
		LogWriter:      io.Discard,
	}))
	if err == nil || !strings.Contains(err.Error(), "create browser shim directory") {
		t.Fatalf("Start without a browser shim = %v", err)
	}
}

// TestSessionStartsWhereNoShimExistsAndOnlyTheLoginRefuses reproduces the
// platform that has no shim to install: the session starts and serves, and the
// login leg is the only thing that refuses.
func TestSessionStartsWhereNoShimExistsAndOnlyTheLoginRefuses(t *testing.T) {
	restoreBrowserShimSeams(t)

	newProcessBrowserShim = func(string) (*browserShim, error) {
		//nolint:nilnil // The absent shim is exactly the platform under test.
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	proc, err := Start(ctx, darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath: fakeHermesExecutable(t, fakeProcessModeOK),
		Home:           t.TempDir(),
		Timeout:        10 * time.Second,
		LogWriter:      io.Discard,
	}))
	if err != nil {
		t.Fatalf("Start without a shim: %v", err)
	}

	t.Cleanup(func() { _ = proc.Close(context.Background()) })

	if proc.Client == nil {
		t.Fatal("a session without a shim did not reach its gateway")
	}

	if proc.BrowserLaunchContained() {
		t.Fatal("a process started without a shim reported a contained browser launch")
	}

	if _, err := (&hermesServer{process: proc}).AuthStart(ctx, "anthropic"); !errors.Is(err, ErrBrowserLaunchUncontained) {
		t.Fatalf("login leg on an uncontained session = %v", err)
	}

	if err := proc.Close(ctx); err != nil {
		t.Fatalf("Close without a shim: %v", err)
	}
}

func restoreBrowserShimSeams(t *testing.T) {
	t.Helper()

	oldMkdirTemp := browserShimMkdirTemp
	oldWriteFile := browserShimWriteFile
	oldNewShim := newProcessBrowserShim

	t.Cleanup(func() {
		browserShimMkdirTemp = oldMkdirTemp
		browserShimWriteFile = oldWriteFile
		newProcessBrowserShim = oldNewShim
	})
}
