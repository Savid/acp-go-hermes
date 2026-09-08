package hermesacp

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	valAmbiguous = "ambiguous"

	sessionPathEnvironmentKey    = "PATH"
	sessionNodeOptionsEnvKey     = "NODE_OPTIONS"
	sessionBashEnvironmentKey    = "BASH_ENV"
	sessionShellEnvironmentKey   = "ENV"
	sessionHermesHomeEnvKey      = "HERMES_HOME"
	sessionHermesSessionTokenKey = "HERMES_DASHBOARD_SESSION_TOKEN"
	sessionManagedPathEnvPrefix  = "ACP_GO_HERMES_PATH_DIR_"
	sessionPrivateEnvPrefix      = "ACP_GO_HERMES_INTERNAL_"
)

func validEnvName(key string) bool {
	return key != "" && !strings.ContainsAny(key, "=\x00")
}

// carrierOwnedEnvKey reports whether a key belongs to the adapter's native
// terminal PATH carrier. The managed-directory namespace is the adapter's own
// and is refused under every spelling; the shell startup names are read by
// the shell under an exact platform spelling, so they compare through the
// platform identity.
func carrierOwnedEnvKey(key string) bool {
	if strings.HasPrefix(strings.ToUpper(key), sessionManagedPathEnvPrefix) {
		return true
	}

	switch nativehermes.EnvironmentKey(key) {
	case sessionBashEnvironmentKey, sessionShellEnvironmentKey:
		return true
	default:
		return false
	}
}

// injectionEnvKey reports whether a key names a loader or node injection
// vector. Each is read under an exact platform spelling, so the comparison
// goes through the platform identity.
func injectionEnvKey(key string) bool {
	name := nativehermes.EnvironmentKey(key)

	return name == sessionNodeOptionsEnvKey || strings.HasPrefix(name, "LD_") || strings.HasPrefix(name, "DYLD_")
}

func privateEnvKey(key string) bool {
	return strings.HasPrefix(strings.ToUpper(key), sessionPrivateEnvPrefix)
}

// blockedSessionEnvKey reports whether a session env key names a variable the
// adapter refuses to install on that session's Hermes process: the carrier's
// own names, the injection vectors, the search path that extraPathDirs alone
// owns, and the state roots the adapter writes itself.
func blockedSessionEnvKey(key string) bool {
	if privateEnvKey(key) || carrierOwnedEnvKey(key) || injectionEnvKey(key) {
		return true
	}

	switch nativehermes.EnvironmentKey(key) {
	case sessionPathEnvironmentKey, sessionHermesHomeEnvKey, sessionHermesSessionTokenKey:
		return true
	default:
		return false
	}
}

// validateAgentEnv applies the session name rule to the static Agent-scoped
// environment, with PATH allowed because that surface establishes the native
// base search path. A refusal fails Agent construction.
func validateAgentEnv(env map[string]string) error {
	seen := make(map[string]string, len(env))

	for _, key := range slices.Sorted(maps.Keys(env)) {
		switch {
		case !validEnvName(key) || strings.ContainsRune(env[key], '\x00'):
			return fmt.Errorf("environment key %q is not a variable name", key)
		case carrierOwnedEnvKey(key):
			return fmt.Errorf("environment key %q is reserved for the session PATH carrier", key)
		case privateEnvKey(key):
			return fmt.Errorf("environment key %q is adapter-private", key)
		case injectionEnvKey(key):
			return fmt.Errorf("environment key %q is an injection vector", key)
		}

		identity := nativehermes.EnvironmentKey(key)
		if previous, duplicate := seen[identity]; duplicate {
			return fmt.Errorf("environment keys %q and %q name the same variable", previous, key)
		}

		seen[identity] = key
	}

	return nil
}

// validateSessionEnv checks a session environment in sorted key order, so the
// first refusal is the same on every call. A key that cannot be a variable
// name, a value carrying a NUL, and a blocked name each fail as unsupported at
// the key exactly as the host sent it. Two keys that name one variable under
// the platform identity fail as ambiguous at the later key: a Go map carries
// no order, so the value such a map would deliver is unknowable.
func validateSessionEnv(env map[string]string, path string) error {
	seen := make(map[string]struct{}, len(env))

	for _, key := range slices.Sorted(maps.Keys(env)) {
		if !validEnvName(key) || strings.ContainsRune(env[key], '\x00') || blockedSessionEnvKey(key) {
			return unsupportedField(path + "." + key)
		}

		identity := nativehermes.EnvironmentKey(key)
		if _, duplicate := seen[identity]; duplicate {
			return ambiguousField(path + "." + key)
		}

		seen[identity] = struct{}{}
	}

	return nil
}

func ambiguousField(path string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valAmbiguous,
		jsonFieldField: path,
	})
}

// ValidateHermesSessionMeta reports the refusal a session/new, session/load,
// session/resume, or fork request carrying meta receives from this package's
// _meta.hermes parsing, or nil when the vendor namespace is accepted.
func ValidateHermesSessionMeta(meta map[string]any) error {
	if err := validateLifecycleMeta(meta); err != nil {
		return err
	}

	_, err := hermesOptionsFromMeta(meta)

	return err
}
