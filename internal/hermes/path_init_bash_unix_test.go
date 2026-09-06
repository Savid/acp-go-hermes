//go:build !windows

package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The proofs below run a real POSIX bash and assert the exact PATH it ends up
// with. Git Bash is the only bash a Windows host has, and its MSYS runtime
// rewrites PATH — and every path-shaped variable — on the way into the shell
// and prepends its own directories, so an exact-PATH assertion states nothing
// about this script there. The Windows conversion proof is among them because
// it is a simulation: it hands the script Windows-form directories under a
// POSIX bash precisely so the script's own conversion, rather than the
// interpreter's, is what runs.

func TestHermesBashEnvRestoresActualCommandPathAfterLoginAndCleansCarrier(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash is required: %v", err)
	}

	first := filepath.Join(durableTempDir(t), "first with spaces")
	second := filepath.Join(durableTempDir(t), "second")
	base := durableTempDir(t)
	for _, dir := range []string{first, second} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	commandName := "acp-go-hermes-path-probe"
	wantCommand := filepath.Join(first, commandName)
	if err := os.WriteFile(wantCommand, []byte("#!/bin/sh\nprintf owned"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, commandName), []byte("#!/bin/sh\nprintf stale"), 0o700); err != nil {
		t.Fatal(err)
	}

	reset := filepath.Join(durableTempDir(t), "profile-reset.sh")
	if err := os.WriteFile(reset, []byte("PATH="+shellSingleQuotePathInit(base)+"\nexport PATH\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	hermesHome := durableTempDir(t)
	initPath := filepath.Join(hermesHome, hermesPathInitFileName)
	if err := os.WriteFile(initPath, hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("bash", "-l", "-c", `. "$2"; printf '%s\n%s\n%s\n%s\n%s' "${PATH%%:*}" "$(command -v "$1")" "$("$1")" "${BASH_ENV-unset}" "${ACP_GO_HERMES_PATH_DIR_COUNT-unset}:${ACP_GO_HERMES_PATH_DIR_1-unset}:${ACP_GO_HERMES_PATH_DIR_WINDOWS-unset}"`, "bash", commandName, reset)
	command.Env = installHermesPathCarrier([]string{
		"HERMES_HOME=" + hermesHome,
		"PATH=" + os.Getenv("PATH"),
	}, hermesHome, []string{first, second})
	output, err := command.Output()
	if err != nil {
		t.Fatalf("run managed login shell: %v", err)
	}
	want := strings.Join([]string{first, wantCommand, "owned", "unset", "unset:unset:unset"}, "\n")
	if string(output) != want {
		t.Fatalf("terminal proof = %q, want %q", output, want)
	}
}

func TestHermesPathInitPassesCarrierThroughBashExecutableWrapper(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash is required: %v", err)
	}
	home := durableTempDir(t)
	if err := os.WriteFile(filepath.Join(home, hermesPathInitFileName), hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(durableTempDir(t), "hermes-wrapper")
	if err := os.WriteFile(wrapper, []byte("#!/bin/bash\nprintf '%s\\n%s' \"$BASH_ENV\" \"$ACP_GO_HERMES_PATH_DIR_COUNT\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(wrapper)
	command.Env = installHermesPathCarrier([]string{"PATH=" + os.Getenv("PATH")}, home, []string{durableTempDir(t)})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Bash Hermes wrapper: %v: %s", err, output)
	}
	want := filepath.ToSlash(filepath.Join(home, hermesPathInitFileName)) + "\n1"
	if string(output) != want {
		t.Fatalf("wrapper carrier = %q, want %q", output, want)
	}
}

func TestHermesPathInitKeepsCompleteRequestedPrefixIdempotent(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash is required: %v", err)
	}
	home := durableTempDir(t)
	if err := os.WriteFile(filepath.Join(home, hermesPathInitFileName), hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(durableTempDir(t), "first[owned]*")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatal(err)
	}
	second := durableTempDir(t)
	base := durableTempDir(t)
	profile := durableTempDir(t)
	dirs := []string{first, second, first}
	initialPath := strings.Join([]string{first, second, first, base}, string(os.PathListSeparator))
	wantPath := strings.Join([]string{first, second, first, profile, base}, string(os.PathListSeparator))
	profileInit := filepath.Join(durableTempDir(t), "preserve-inherited-path.sh")
	if err := os.WriteFile(profileInit, []byte("PATH="+shellSingleQuotePathInit(profile)+":$PATH\nexport PATH\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "-c", `. "$1"; printf '%s' "$PATH"`, "bash", profileInit)
	command.Env = installHermesPathCarrier([]string{"PATH=" + initialPath}, home, dirs)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run idempotent PATH hook: %v: %s", err, output)
	}
	if string(output) != wantPath {
		t.Fatalf("idempotent PATH = %q, want %q", output, wantPath)
	}
}

func TestHermesPathInitConvertsWindowsNativeDirsWithoutExecutingPath(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash is required: %v", err)
	}

	originalPlatform := Platform
	Platform = processPlatformWindows
	t.Cleanup(func() { Platform = originalPlatform })

	hermesHome := durableTempDir(t)
	if err := os.WriteFile(filepath.Join(hermesHome, hermesPathInitFileName), hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}

	maliciousBin := durableTempDir(t)
	marker := filepath.Join(durableTempDir(t), "cygpath-ran")
	if err := os.WriteFile(filepath.Join(maliciousBin, "cygpath"), []byte("#!/bin/sh\nprintf ran > "+shellSingleQuotePathInit(marker)+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "-c", `printf '%s\n%s\n%s' "$PATH" "${BASH_ENV-unset}" "${ACP_GO_HERMES_PATH_DIR_WINDOWS-unset}"`)
	dirs := []string{`C:\tools\one`, `\\server\share\bin`, `C:\tools\one`}
	prefix := []string{"/c/tools/one", "//server/share/bin", "/c/tools/one"}
	basePath := strings.Join([]string{maliciousBin, "/usr/bin", "/bin"}, string(os.PathListSeparator))
	productionPath := strings.Join(append(slices.Clone(prefix), basePath), string(os.PathListSeparator))
	command.Env = installHermesPathCarrier([]string{
		"HERMES_HOME=" + hermesHome,
		"PATH=" + productionPath,
	}, hermesHome, dirs)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Windows carrier simulation: %v: %s", err, output)
	}
	want := productionPath + "\nunset\nunset"
	if string(output) != want {
		t.Fatalf("Windows terminal proof = %q, want %q", output, want)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("Windows conversion executed PATH-owned cygpath: %v", err)
	}
}

func TestHermesPathInitIsIdempotentWhenSourcedAgain(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash is required: %v", err)
	}
	home := durableTempDir(t)
	initPath := filepath.Join(home, hermesPathInitFileName)
	if err := os.WriteFile(initPath, hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}
	operation := durableTempDir(t)
	base := durableTempDir(t)
	command := exec.Command("bash", "-c", `. "$1"; . "$1"; printf '%s\n%s' "$PATH" "${BASH_ENV-unset}"`, "bash", initPath)
	command.Env = installHermesPathCarrier([]string{"PATH=" + base}, home, []string{operation})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("repeat source managed init: %v: %s", err, output)
	}
	want := operation + string(os.PathListSeparator) + base + "\nunset"
	if string(output) != want {
		t.Fatalf("repeated PATH init = %q, want %q", output, want)
	}
}
