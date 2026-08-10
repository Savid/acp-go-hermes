package hermes

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Ordinary same-identity execution is what an omitted policy selects. It is a
// genuinely separate strategy rather than a relaxed policy: nothing here reads
// a ProcessIsolation, requests a credential change, or consults an authority
// root. What it does own is the sanitized ambient environment the native
// harness inherits, and the executable resolution rules that go with an
// ordinary shell environment rather than a closed policy one.

// The adapter-managed Hermes state keys. Each is written by the adapter from a
// value it generated, so these are the names an inherited environment must
// never be allowed to supply.
const (
	envHermesHome         = "HERMES_HOME"
	envHermesAuthHome     = "HERMES_AUTH_HOME"
	envHermesSessionToken = "HERMES_DASHBOARD_SESSION_TOKEN"
)

// ordinaryManagedEnvironmentKeys names the adapter-managed Hermes state an
// ambient environment must never carry into a native launch. Each is written by
// the adapter itself from a value it generated, so an inherited one can only
// redirect native state at a root the wrapper does not own — the durable
// credential residence most of all.
var ordinaryManagedEnvironmentKeys = []string{envHermesHome, envHermesAuthHome, envHermesSessionToken}

// ordinaryPrivateEnvironmentKeys names the adapter-private markers that live
// outside the private prefix. They travel between the wrapper and its own
// containment bootstrap, so an ambient copy is a forged one.
var ordinaryPrivateEnvironmentKeys = []string{envRuntimeID, envScratchRoot}

// scrubOrdinaryEnvironmentKey reports whether an ambient key is adapter-private
// or adapter-managed state. Matching is case-insensitive because the comparison
// exists to stop a spoofed variable, and a case variant is exactly how one
// would be spelled.
func scrubOrdinaryEnvironmentKey(key string) bool {
	upper := strings.ToUpper(key)

	if strings.HasPrefix(upper, privateSupervisorEnvPrefix) || strings.HasPrefix(upper, processSupervisorEnvPrefix) {
		return true
	}

	return slices.Contains(ordinaryPrivateEnvironmentKeys, upper) || slices.Contains(ordinaryManagedEnvironmentKeys, upper)
}

// ordinaryEnvironment builds the native environment for an omitted policy: the
// adapter's own ambient environment minus its private and managed state, with
// the caller overlay applied on top. The overlay is scrubbed on the same terms
// as the base, so a caller cannot reintroduce through WithEnv what the ambient
// scrub just removed.
func ordinaryEnvironment(ambient map[string]string, overlays ...map[string]string) ([]string, error) {
	env := make(map[string]string, len(ambient))

	for key, value := range ambient {
		if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 || scrubOrdinaryEnvironmentKey(key) {
			continue
		}

		env[key] = value
	}

	for _, overlay := range overlays {
		for key, value := range overlay {
			if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
				return nil, fmt.Errorf("process environment contains invalid key %q", key)
			}

			if scrubOrdinaryEnvironmentKey(key) {
				continue
			}

			env[key] = value
		}
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+env[key])
	}

	return out, nil
}

// lookOrdinaryPathInEnvironment resolves the harness executable for ordinary
// execution. A closed policy may require every PATH entry to be absolute
// because the policy author wrote the whole environment; an ordinary launch
// inherits whatever shell environment the operator already has, where
// "PATH=bin:/usr/bin" and a relative configured executable are both ordinary.
// Refusing those would turn policy omission into an app-start blocker, so the
// rule here is only that the result must exist and be executable.
//
// The resolved path is made absolute because exec.Cmd evaluates a relative Path
// against Cmd.Dir, which is the session cwd rather than the directory this
// resolution ran in.
func lookOrdinaryPathInEnvironment(file string, environment []string) (string, error) {
	if file == "" {
		return "", errors.New("executable name is empty")
	}

	if strings.ContainsRune(file, os.PathSeparator) {
		return absoluteExecutableFile(file)
	}

	for _, dir := range filepath.SplitList(envValue(environment, "PATH")) {
		// An empty PATH entry means the current directory, matching the
		// resolution an ordinary shell would perform.
		if dir == "" {
			dir = "."
		}

		if path, err := absoluteExecutableFile(filepath.Join(dir, file)); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in PATH", file)
}

func absoluteExecutableFile(path string) (string, error) {
	resolved, err := executableFile(path)
	if err != nil {
		return "", err
	}

	return filepath.Abs(resolved)
}
