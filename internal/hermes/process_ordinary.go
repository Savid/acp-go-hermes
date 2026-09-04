package hermes

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

// Ordinary same-identity execution is what an omitted authority selects. It is
// a genuinely separate strategy: nothing here
// requests a credential change or consults an authority. What it does own is
// the sanitized ambient environment the native
// harness inherits, and the executable resolution rules that go with an
// ordinary shell environment.

// The adapter-managed Hermes state keys. Each is written by the adapter from a
// value it generated, so these are the names an inherited environment must
// never be allowed to supply.
const (
	envHermesHome         = "HERMES_HOME"
	envHermesSessionToken = "HERMES_DASHBOARD_SESSION_TOKEN"
	// envHermesWebDist is operator-supplied rather than adapter-managed, so it
	// is read out of a phase map rather than scrubbed out of one.
	envHermesWebDist = "HERMES_WEB_DIST"
	// envPath, envHome, and envPathExt are named once for the allowlist and
	// the places executable resolution reads them.
	envPath    = "PATH"
	envHome    = "HOME"
	envPathExt = "PATHEXT"
)

// ordinaryManagedEnvironmentKeys names the adapter-managed Hermes state an
// ambient environment must never carry into a native launch. Each is written by
// the adapter itself from a value it generated, so an inherited one can only
// redirect native state at a root the wrapper does not own — the durable
// credential residence most of all.
var ordinaryManagedEnvironmentKeys = []string{envHermesHome, envHermesSessionToken}

var processRuntimePlatform = runtime.GOOS

const (
	processPlatformLinux   = "linux"
	processPlatformWindows = "windows"
)

// scrubOrdinaryEnvironmentKey reports whether an ambient key is adapter-managed
// state. Matching is case-insensitive because a case variant must not bypass
// the boundary.
func scrubOrdinaryEnvironmentKey(key string) bool {
	upper := strings.ToUpper(key)

	return slices.Contains(ordinaryManagedEnvironmentKeys, upper)
}

// ordinaryInheritedEnvironmentKeys is the closed set of ambient names an
// ordinary launch inherits. Hermes reads more than fifty environment names as
// provider credentials — its own, and those of every other harness in the
// family, OAuth tokens included — copies each one it finds into the session
// home's auth store, and lists models at every provider it seeded on every
// fresh-home start. An inherited environment therefore hands the harness every
// key the operator happens to have exported, so the base is an allowlist: the
// names a process needs to find its executable, its home, a temp directory,
// a locale, a terminal, and a network, and nothing that carries a credential.
// A credential reaches the harness only through WithEnv, session env, seed
// files, or a shared home's provider auth, each of them explicit.
//
// The first nineteen names and the LC_ prefix are the family's base; the
// proxy, TLS bundle, and web-dist names are what a Python harness reaches the
// network and serves its dashboard with. Both spellings of each proxy name are
// listed because Unix treats them as distinct variables and Python honours
// either.
var ordinaryInheritedEnvironmentKeys = []string{
	envPath, envHome, "USER", "LOGNAME", "SHELL", "TMPDIR", "TMP", "TEMP", "LANG", "TERM",
	"COLORTERM", "NO_COLOR", "FORCE_COLOR", "SYSTEMROOT", "WINDIR", "COMSPEC", envPathExt,
	"USERPROFILE", "__CF_USER_TEXT_ENCODING",
	"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY",
	"http_proxy", "https_proxy", "no_proxy", "all_proxy",
	"SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE",
	envHermesWebDist,
}

// ordinaryInheritedEnvironmentPrefix admits the locale family as a whole.
const ordinaryInheritedEnvironmentPrefix = "LC_"

// inheritOrdinaryEnvironmentKey reports whether an ambient name is one an
// ordinary launch inherits. Names are compared the way the platform resolves
// them: folded where the platform folds, so an inherited "Path" is the search
// path on Windows, and exactly elsewhere, where "Path" is a different variable
// from "PATH" and inheriting it would carry a name the allowlist never named.
func inheritOrdinaryEnvironmentKey(key string) bool {
	if processEnvironmentKeysFold() {
		return strings.HasPrefix(strings.ToUpper(key), ordinaryInheritedEnvironmentPrefix) ||
			slices.ContainsFunc(ordinaryInheritedEnvironmentKeys, func(name string) bool { return strings.EqualFold(name, key) })
	}

	return strings.HasPrefix(key, ordinaryInheritedEnvironmentPrefix) ||
		slices.Contains(ordinaryInheritedEnvironmentKeys, key)
}

func envValueFold(env []string, name string, fold bool) string {
	matched := ""

	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && (key == name || fold && strings.EqualFold(key, name)) {
			matched = value
		}
	}

	return matched
}

// ordinaryEnvironment builds the native environment for an omitted authority:
// the inherited subset of the adapter's own ambient environment, with the
// caller overlay applied on top. The base admits only the names in
// ordinaryInheritedEnvironmentKeys, which is what keeps every ambient
// credential out of the harness; the overlay is scrubbed of the adapter's
// managed state, so a caller cannot redirect through WithEnv the native roots
// the adapter itself sets. Each overlay is a later phase, so where names fold
// it replaces the spelling an earlier phase installed rather than joining it.
func ordinaryEnvironment(ambient map[string]string, overlays ...map[string]string) ([]string, error) {
	phases := make([]map[string]string, 0, len(overlays)+1)

	base := make(map[string]string, len(ambient))
	for key, value := range ambient {
		if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 || !inheritOrdinaryEnvironmentKey(key) {
			continue
		}

		base[key] = value
	}

	phases = append(phases, base)

	for _, overlay := range overlays {
		phase := make(map[string]string, len(overlay))
		for key, value := range overlay {
			if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
				return nil, fmt.Errorf("process environment contains invalid key %q", key)
			}

			if scrubOrdinaryEnvironmentKey(key) {
				continue
			}

			phase[key] = value
		}

		phases = append(phases, phase)
	}

	env, err := mergeProcessEnvironmentPhases(phases...)
	if err != nil {
		return nil, err
	}

	return sortedProcessEnvironment(env), nil
}

// executableSearchRules carries the platform facts ordinary executable
// resolution depends on. They are data rather than build-tagged branches so
// that each platform's rules can be exercised on every host, alongside the
// native Windows runtime lane that proves those rules drive a real process.
type executableSearchRules struct {
	// pathSeparators are the characters whose presence makes a configured
	// executable a path to resolve directly rather than a bare name to search
	// PATH for. Windows counts a drive-letter colon and both slashes.
	pathSeparators string
	// extensions is the ordered PATHEXT list appended to a bare name. It is
	// empty off Windows, where the execute bit rather than the file name says
	// what may run.
	extensions []string
	// requireExecuteBit gates a candidate on Mode()&0o111. Windows regular
	// files never carry one — os.Stat synthesizes 0666 or 0444 there — so
	// requiring it would reject every real hermes.exe.
	requireExecuteBit bool
	// foldEnvironmentKeys matches environment names case-insensitively.
	// Windows environment blocks spell the search path "Path", and an exact
	// compare against "PATH" would silently search nothing.
	foldEnvironmentKeys bool
}

// unixExecutableRules is what every platform but Windows resolves by: the
// execute bit decides, and a name is a path only when it contains a slash.
func unixExecutableRules() executableSearchRules {
	return executableSearchRules{pathSeparators: "/", requireExecuteBit: true}
}

// windowsExecutableRules is what Windows resolves by. It lives here rather than
// in the build-tagged file so the Windows rules are exercised by tests on every
// host; the Windows file is a one-line call to it.
func windowsExecutableRules(environment []string) executableSearchRules {
	return executableSearchRules{
		pathSeparators:      `:\/`,
		extensions:          executableExtensionList(envValueFold(environment, envPathExt, true)),
		foldEnvironmentKeys: true,
	}
}

// defaultWindowsExecutableExtensions is what Windows itself falls back to when
// PATHEXT is absent, and what a bare "hermes" must be able to reach.
const defaultWindowsExecutableExtensions = ".com;.exe;.bat;.cmd"

// executableExtensionList parses a PATHEXT value into the ordered, lowercased,
// dot-prefixed extensions a bare name is tried with. An empty value receives
// Windows' default list; a nonempty separator-only value names no extensions.
func executableExtensionList(pathext string) []string {
	if pathext == "" {
		pathext = defaultWindowsExecutableExtensions
	}

	extensions := make([]string, 0, 4)

	for entry := range strings.SplitSeq(pathext, ";") {
		entry = strings.ToLower(strings.TrimSpace(entry))
		if entry == "" {
			continue
		}

		if entry[0] != '.' {
			entry = "." + entry
		}

		extensions = append(extensions, entry)
	}

	return extensions
}

// lookOrdinaryPathInEnvironment resolves the harness executable for ordinary
// execution. An ordinary launch inherits whatever shell environment the
// operator already has, where
// "PATH=bin:/usr/bin" and a relative configured executable are both ordinary.
// Refusing those would turn authority omission into an app-start blocker, so the
// rule here is only that the result must exist and be runnable on this platform.
func lookOrdinaryPathInEnvironment(file string, environment []string) (string, error) {
	return lookOrdinaryPathWithRules(file, environment, ordinaryExecutableRules(environment))
}

// lookOrdinaryPathWithRules is the resolution itself, separated from the
// platform it runs on so both rule sets are executable everywhere.
//
// The resolved path is made absolute because exec.Cmd evaluates a relative Path
// against Cmd.Dir, which is the session cwd rather than the directory this
// resolution ran in.
func lookOrdinaryPathWithRules(file string, environment []string, rules executableSearchRules) (string, error) {
	if file == "" {
		return "", errors.New("executable name is empty")
	}

	if strings.ContainsAny(file, rules.pathSeparators) {
		return absoluteOrdinaryExecutable(file, rules)
	}

	for _, dir := range filepath.SplitList(envValueFold(environment, envPath, rules.foldEnvironmentKeys)) {
		// An empty PATH entry means the current directory, matching the
		// resolution an ordinary shell would perform.
		if dir == "" {
			dir = "."
		}

		if path, err := absoluteOrdinaryExecutable(filepath.Join(dir, file), rules); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in PATH", file)
}

func absoluteOrdinaryExecutable(path string, rules executableSearchRules) (string, error) {
	resolved, err := findOrdinaryExecutable(path, rules)
	if err != nil {
		return "", err
	}

	return filepath.Abs(resolved)
}

// findOrdinaryExecutable applies the platform's extension rules to one
// candidate. Where there are no extensions the candidate is the answer; where
// there are, a name that already carries one is tried verbatim first and still
// falls through, so a literal "hermes.bat.exe" resolves rather than failing.
func findOrdinaryExecutable(path string, rules executableSearchRules) (string, error) {
	if len(rules.extensions) == 0 {
		return matchExecutableFile(path, rules)
	}

	if hasFileExtension(path, rules.pathSeparators) {
		if resolved, err := matchExecutableFile(path, rules); err == nil {
			return resolved, nil
		}
	}

	for _, extension := range rules.extensions {
		if resolved, err := matchExecutableFile(path+extension, rules); err == nil {
			return resolved, nil
		}
	}

	return "", fmt.Errorf("no executable extension of %q exists", path)
}

// hasFileExtension reports whether the final path element carries any
// extension, which is the question that decides whether a name is tried
// verbatim before extensions are appended to it.
func hasFileExtension(path string, separators string) bool {
	dot := strings.LastIndex(path, ".")

	return dot >= 0 && strings.LastIndexAny(path, separators) < dot
}

// matchExecutableFile accepts one concrete candidate. The execute bit is
// consulted only where the platform actually maintains one.
func matchExecutableFile(path string, rules executableSearchRules) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if !info.Mode().IsRegular() || (rules.requireExecuteBit && info.Mode()&0o111 == 0) {
		return "", fmt.Errorf("%q is not executable", path)
	}

	return path, nil
}
