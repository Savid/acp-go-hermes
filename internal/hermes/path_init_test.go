package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestInstallHermesPathCarrierOwnsNamespaceBashEnvAndOrder(t *testing.T) {
	originalPlatform := processRuntimePlatform
	processRuntimePlatform = processPlatformLinux
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	first := filepath.Join(t.TempDir(), "first")
	second := filepath.Join(t.TempDir(), "second")
	environment := []string{
		"A=1",
		hermesBashEnvKey + "=/untrusted/init",
		hermesPathInitCountEnv + "=99",
		strings.ToLower(hermesPathInitEnvironment) + "1=untrusted",
	}

	home := t.TempDir()
	got := installHermesPathCarrier(environment, home, []string{first, second})
	want := []string{
		"A=1",
		hermesBashEnvKey + "=" + filepath.Join(home, hermesPathInitFileName),
		hermesPathInitCountEnv + "=2",
		hermesPathInitEnvironment + "1=" + first,
		hermesPathInitEnvironment + "2=" + second,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("managed environment = %#v, want %#v", got, want)
	}
	if got := installHermesPathCarrier(environment, home, nil); !slices.Equal(got, []string{"A=1"}) {
		t.Fatalf("empty carrier environment = %#v", got)
	}
}

func TestHermesBashEnvRestoresActualCommandPathAfterLoginAndCleansCarrier(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash is required: %v", err)
	}

	first := filepath.Join(t.TempDir(), "first with spaces")
	second := filepath.Join(t.TempDir(), "second")
	base := t.TempDir()
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

	reset := filepath.Join(t.TempDir(), "profile-reset.sh")
	if err := os.WriteFile(reset, []byte("PATH="+shellSingleQuotePathInit(base)+"\nexport PATH\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	hermesHome := t.TempDir()
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
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, hermesPathInitFileName), hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(t.TempDir(), "hermes-wrapper")
	if err := os.WriteFile(wrapper, []byte("#!/bin/bash\nprintf '%s\\n%s' \"$BASH_ENV\" \"$ACP_GO_HERMES_PATH_DIR_COUNT\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(wrapper)
	command.Env = installHermesPathCarrier([]string{"PATH=" + os.Getenv("PATH")}, home, []string{t.TempDir()})
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
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, hermesPathInitFileName), hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(t.TempDir(), "first[owned]*")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatal(err)
	}
	second := t.TempDir()
	base := t.TempDir()
	profile := t.TempDir()
	dirs := []string{first, second, first}
	initialPath := strings.Join([]string{first, second, first, base}, string(os.PathListSeparator))
	wantPath := strings.Join([]string{first, second, first, profile, base}, string(os.PathListSeparator))
	profileInit := filepath.Join(t.TempDir(), "preserve-inherited-path.sh")
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

	originalPlatform := processRuntimePlatform
	processRuntimePlatform = processPlatformWindows
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	hermesHome := t.TempDir()
	if err := os.WriteFile(filepath.Join(hermesHome, hermesPathInitFileName), hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}

	maliciousBin := t.TempDir()
	marker := filepath.Join(t.TempDir(), "cygpath-ran")
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
	home := t.TempDir()
	initPath := filepath.Join(home, hermesPathInitFileName)
	if err := os.WriteFile(initPath, hermesPathInitScript, 0o600); err != nil {
		t.Fatal(err)
	}
	operation := t.TempDir()
	base := t.TempDir()
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

func TestValidateSessionEnvironmentRejectsManagedPathCarrierAndBashEnv(t *testing.T) {
	for key := range map[string]struct{}{
		hermesPathInitCountEnv: {},
		hermesBashEnvKey:       {},
	} {
		if err := validateSessionEnvironmentNoPath(map[string]string{key: "untrusted"}); err == nil {
			t.Fatalf("reserved environment key %q was accepted", key)
		}
	}
	if err := validateSessionEnvironmentNoPath(map[string]string{"path": "/bad"}); processRuntimePlatform == processPlatformWindows && err == nil {
		t.Fatal("case-folded Windows PATH was accepted")
	}
}

func shellSingleQuotePathInit(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
