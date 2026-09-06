package hermes

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestInstallHermesPathCarrierOwnsNamespaceBashEnvAndOrder(t *testing.T) {
	originalPlatform := Platform
	Platform = processPlatformLinux
	t.Cleanup(func() { Platform = originalPlatform })

	first := filepath.Join(durableTempDir(t), "first")
	second := filepath.Join(durableTempDir(t), "second")
	environment := []string{
		"A=1",
		hermesBashEnvKey + "=/untrusted/init",
		hermesShellEnvKey + "=/untrusted/sh-init",
		hermesPathInitCountEnv + "=99",
		strings.ToLower(hermesPathInitEnvironment) + "1=untrusted",
	}

	home := durableTempDir(t)
	// The hook is read by Bash, which names files with forward slashes whatever
	// the host spells them with, so the carrier always publishes the slash form.
	initScript := filepath.ToSlash(filepath.Join(home, hermesPathInitFileName))
	got := installHermesPathCarrier(environment, home, []string{first, second})
	want := []string{
		"A=1",
		hermesBashEnvKey + "=" + initScript,
		hermesPathInitCountEnv + "=2",
		hermesPathInitEnvironment + "1=" + first,
		hermesPathInitEnvironment + "2=" + second,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("carrier environment = %#v, want %#v", got, want)
	}
	if got := installHermesPathCarrier(environment, home, nil); !slices.Equal(got, []string{"A=1"}) {
		t.Fatalf("empty carrier environment = %#v", got)
	}

	// A Windows launch adds the one marker that tells the init script to convert
	// native directory spellings itself rather than trust the shell to.
	Platform = processPlatformWindows
	windowsWant := []string{
		"A=1",
		hermesBashEnvKey + "=" + initScript,
		hermesPathInitCountEnv + "=2",
		hermesPathInitWindowsEnv + "=1",
		hermesPathInitEnvironment + "1=" + first,
		hermesPathInitEnvironment + "2=" + second,
	}
	if got := installHermesPathCarrier(environment, home, []string{first, second}); !slices.Equal(got, windowsWant) {
		t.Fatalf("windows carrier environment = %#v, want %#v", got, windowsWant)
	}
}

func TestValidateSessionEnvironmentRejectsManagedPathCarrierAndBashEnv(t *testing.T) {
	for key := range map[string]struct{}{
		hermesPathInitCountEnv: {},
		hermesBashEnvKey:       {},
		hermesShellEnvKey:      {},
	} {
		if err := validateSessionEnvironmentNoPath(map[string]string{key: "untrusted"}); err == nil {
			t.Fatalf("reserved environment key %q was accepted", key)
		}
	}
	if err := validateSessionEnvironmentNoPath(map[string]string{"path": "/bad"}); Platform == processPlatformWindows && err == nil {
		t.Fatal("case-folded Windows PATH was accepted")
	}
}

func shellSingleQuotePathInit(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
