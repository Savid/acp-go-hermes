package hermesacp

import (
	"maps"
	"runtime"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
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
)

var sessionEnvPlatform = runtime.GOOS

// sessionEnvIdentity is the name the target platform resolves an environment
// key by: the exact bytes on Unix, where PATH and path are two variables, and
// the upper-cased spelling on Windows, where they are one.
func sessionEnvIdentity(key string) string {
	if sessionEnvPlatform == runtimePlatformWindows {
		return strings.ToUpper(key)
	}

	return key
}

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

	switch sessionEnvIdentity(key) {
	case sessionBashEnvironmentKey, sessionShellEnvironmentKey:
		return true
	default:
		return false
	}
}

// blockedSessionEnvKey reports whether a session env key names a variable the
// adapter refuses to install on that session's Hermes process: the carrier's
// own names, the search path that extraPathDirs alone owns, the state roots
// the adapter writes itself, and the loader and node injection names. All but
// the adapter namespace compare through the platform identity.
func blockedSessionEnvKey(key string) bool {
	if carrierOwnedEnvKey(key) {
		return true
	}

	switch name := sessionEnvIdentity(key); name {
	case sessionPathEnvironmentKey, sessionNodeOptionsEnvKey, sessionHermesHomeEnvKey, sessionHermesSessionTokenKey:
		return true
	default:
		return strings.HasPrefix(name, "LD_") || strings.HasPrefix(name, "DYLD_")
	}
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

		identity := sessionEnvIdentity(key)
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
		keyField:       path,
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
